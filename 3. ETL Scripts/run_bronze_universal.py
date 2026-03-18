"""
FintastIQ Universal Bronze ETL Pipeline
========================================
Works for ANY client folder — no client-specific code needed.

Usage:
  python run_bronze_universal.py --folder "/path/to/Client Data"
  python run_bronze_universal.py --folder "/path/to/Client Data" --client "Acme" --prefix "ACM"
  python run_bronze_universal.py --folder "/path/to/Client Data" --priority "orders.xlsx,pricing.csv"
  python run_bronze_universal.py --folder "/path/to/Client Data" --max-sheets 20

What it does (5 stages):
  1. Discover & profile all spreadsheet files in the folder
  2. LLM classifies every sheet: EXTRACT / CONTEXT / SKIP
  3. LLM maps each EXTRACT sheet's columns to canonical schema
  4. Transforms data into canonical tables
  5. LLM validates output quality + saves Excel + metadata JSON
"""

import argparse
import glob
import json
import os
import re
import sys
import time
import traceback
import warnings
from datetime import datetime

import httpx
import anthropic
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ═══════════════════════════════════════════════════════════════════
# CANONICAL SCHEMA — shared across all clients
# ═══════════════════════════════════════════════════════════════════

CANONICAL_SCHEMA = """
Our canonical pricing diagnostic schema has these target tables:

1. transaction_fact: Core revenue transactions
   Fields: transaction_id, transaction_date, customer_name, customer_id, product_name,
   product_sku, service_type, revenue, list_price, invoice_price, pocket_price, discount_pct,
   discount_amount, quantity, unit_price, cost_of_goods, gross_margin, currency,
   sales_rep_name, sales_rep_id, channel, region, deal_size_bucket, order_id, invoice_id

2. customer_dim: Customer master data
   Fields: customer_id, customer_name, segment, industry, vertical, region, tier,
   company_type, company_size, geography, state, city, lead_source, annual_revenue,
   lifetime_value, churn_risk, churn_status, contract_status

3. product_dim: Products/services/SKUs
   Fields: product_sku, product_name, category, subcategory, pricing_method,
   list_price, cost, margin_pct, feature_summary, launch_date, status

4. sales_rep_dim: Sales team
   Fields: sales_rep_id, sales_rep_name, email, region, territory, team, manager,
   hire_date, quota, commission_rate

5. deal_opportunity: Pipeline and deals
   Fields: deal_id, customer_name, opportunity_name, deal_type, deal_stage,
   deal_amount, probability, expected_revenue, account_owner, close_date, source

6. contract: Contract details
   Fields: contract_id, customer_name, start_date, end_date, term_months,
   contract_value, annual_value, renewal_status, payment_terms

7. list_price_history: Price changes over time
   Fields: product_sku, product_name, effective_date, list_price, previous_price, change_pct

8. monthly_financials: P&L / financial schedules
   Fields: period, line_item, category, amount, budget_amount, variance

These tables feed 20 standard pricing analyses including Pocket Price Waterfall,
Price Band Analysis, Customer Profitability, Win/Loss, Discount Governance,
Contract Leakage, SKU Pareto, Rep Behaviour, Churn Sensitivity, etc.
"""


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════

def get_llm_client(api_key):
    return anthropic.Anthropic(
        api_key=api_key,
        http_client=httpx.Client(verify=False, timeout=120.0),
    )


def call_llm(client, prompt, system=None, max_tokens=4096, model="claude-sonnet-4-20250514"):
    messages = [{"role": "user", "content": prompt}]
    for attempt in range(3):
        try:
            kwargs = {"model": model, "max_tokens": max_tokens, "messages": messages}
            if system:
                kwargs["system"] = system
            resp = client.messages.create(**kwargs)
            return resp.content[0].text, resp.usage
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                raise


def extract_json(text, prefer_array=False):
    match = re.search(r"```(?:json)?\s*\n?([\s\S]*?)\n?```", text)
    if match:
        text = match.group(1)
    order = [("[", "]"), ("{", "}")] if prefer_array else [("{", "}"), ("[", "]")]
    for sc, ec in order:
        start = text.find(sc)
        if start >= 0:
            depth = 0
            for i in range(start, len(text)):
                if text[i] == sc:
                    depth += 1
                elif text[i] == ec:
                    depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(text[start : i + 1])
                        except Exception:
                            break
    try:
        return json.loads(text.strip())
    except Exception:
        return None


def load_client_context(folder_path):
    """Load and flatten client_context.json from the client folder."""
    ctx_path = os.path.join(folder_path, "client_context.json")
    if not os.path.exists(ctx_path):
        return ""
    with open(ctx_path) as f:
        ctx = json.load(f)

    parts = []
    # Flat structure
    if "company_name" in ctx:
        parts.append(
            f"Company: {ctx.get('company_name','?')} | "
            f"Industry: {ctx.get('industry','?')} | "
            f"Model: {ctx.get('business_model','?')}"
        )
        parts.append(
            f"Revenue: {ctx.get('revenue_range','?')} | "
            f"Products: {ctx.get('product_type','?')}"
        )
        parts.append(f"Goal: {ctx.get('engagement_goal','?')}")
        for k in [
            "primary_pricing_model", "customer_segmentation",
            "professional_services_structure", "product_modules",
            "pricing_variables", "current_arr_estimate",
        ]:
            v = ctx.get(k)
            if v and str(v).strip():
                parts.append(f"{k}: {str(v)[:150]}")
    # Nested structure (tier1 / tier3_open)
    elif "tier1" in ctx:
        t1 = ctx["tier1"]
        parts.append(
            f"Company: {t1.get('company_name','?')} | "
            f"Industry: {t1.get('industry','?')} | "
            f"Model: {t1.get('business_model','?')}"
        )
        parts.append(
            f"Revenue: {t1.get('revenue_range','?')} | "
            f"Products: {t1.get('product_type','?')}"
        )
        parts.append(f"Goal: {t1.get('engagement_goal','?')}")
        for k, v in ctx.get("tier3_open", {}).items():
            if v and str(v).strip():
                parts.append(f"{k}: {str(v)[:150]}")

    return "\n".join(parts)


def derive_prefix(client_name):
    """Derive a 3-letter prefix from client name, e.g. 'Gravitate Energy' -> 'GRV'"""
    words = re.sub(r"[^a-zA-Z0-9 ]", "", client_name).split()
    if not words:
        return "CLI"
    if len(words) == 1:
        return words[0][:3].upper()
    return (words[0][0] + words[1][:2]).upper() if len(words) >= 2 else words[0][:3].upper()


# ═══════════════════════════════════════════════════════════════════
# STAGE 1: DISCOVER & PROFILE
# ═══════════════════════════════════════════════════════════════════

def discover_files(folder_path, priority_files=None):
    """Find all spreadsheet files. If priority_files given, only return those."""
    extensions = ["*.xlsx", "*.xlsb", "*.csv", "*.tsv", "*.xls"]

    if priority_files:
        # Match priority file names anywhere in the folder tree
        found = []
        for pf in priority_files:
            matches = glob.glob(
                os.path.join(folder_path, "**", pf), recursive=True
            )
            if not matches:
                # Try partial match
                all_files = []
                for ext in extensions:
                    all_files.extend(
                        glob.glob(os.path.join(folder_path, "**", ext), recursive=True)
                    )
                for f in all_files:
                    if pf.lower() in os.path.basename(f).lower():
                        matches.append(f)
            found.extend(matches)
        files = sorted(set(found))
    else:
        files = []
        for ext in extensions:
            files.extend(
                glob.glob(os.path.join(folder_path, "**", ext), recursive=True)
            )
        files = sorted(set(files))

    # Filter temp / hidden files
    return [
        f for f in files
        if not os.path.basename(f).startswith(("~", "."))
    ]


def profile_single_df(df, name):
    cols = [str(c) for c in df.columns if not str(c).startswith("Unnamed")][:20]
    samples = {}
    for c in cols[:10]:
        if c in df.columns:
            s = df[c].dropna().head(3).tolist()
            samples[c] = [str(v)[:50] for v in s]
    return {"rows": len(df), "cols": len(df.columns), "named_cols": cols, "samples": samples}


def profile_file(filepath, folder_path, max_sheets=12):
    ext = os.path.splitext(filepath)[1].lower()
    filename = os.path.basename(filepath)
    rel_path = os.path.relpath(filepath, folder_path)
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    profile_nrows = 20 if file_size_mb > 20 else None

    # ── CSV / TSV ──
    if ext in (".csv", ".tsv"):
        try:
            sep = "\t" if ext == ".tsv" else ","
            df = pd.read_csv(filepath, nrows=10, sep=sep)
            return {
                "filename": filename,
                "rel_path": rel_path,
                "sheets": {"Sheet1": profile_single_df(df, filename)},
            }
        except Exception as e:
            return {"filename": filename, "rel_path": rel_path, "error": str(e)[:80], "sheets": {}}

    # ── Excel ──
    engine = "pyxlsb" if ext == ".xlsb" else None
    try:
        xl = pd.ExcelFile(filepath, engine=engine)
        all_sheets = xl.sheet_names
    except Exception as e:
        return {"filename": filename, "rel_path": rel_path, "error": str(e)[:80], "sheets": {}}

    # Sample sheets for large files
    if len(all_sheets) > max_sheets:
        sampled = all_sheets[:4] + all_sheets[-2:]
        step = max(1, len(all_sheets) // (max_sheets - 6))
        for i in range(4, len(all_sheets) - 2, step):
            if all_sheets[i] not in sampled and len(sampled) < max_sheets:
                sampled.append(all_sheets[i])
        skipped = len(all_sheets) - len(sampled)
    else:
        sampled = all_sheets
        skipped = 0

    profiles = {}
    for sheet in sampled:
        try:
            df_raw = pd.read_excel(
                filepath, sheet_name=sheet, header=None, engine=engine, nrows=25
            )
            if df_raw.empty:
                profiles[sheet] = None
                continue
            # Detect header row
            best_row, best_score = 0, 0
            for i in range(min(15, len(df_raw))):
                score = sum(
                    1 for v in df_raw.iloc[i]
                    if isinstance(v, str) and len(str(v).strip()) > 2
                )
                if score > best_score:
                    best_score = score
                    best_row = i
            df = pd.read_excel(
                filepath, sheet_name=sheet, header=best_row,
                engine=engine, nrows=profile_nrows,
            )
            df.columns = [str(c).strip() for c in df.columns]
            profiles[sheet] = profile_single_df(df, sheet)
        except Exception as e:
            profiles[sheet] = {"error": str(e)[:80]}

    result = {
        "filename": filename,
        "rel_path": rel_path,
        "filepath": filepath,
        "sheets": profiles,
        "total_sheets": len(all_sheets),
        "file_size_mb": round(file_size_mb, 1),
    }
    if skipped > 0:
        result["skipped_sheets"] = skipped
        result["all_sheet_names"] = all_sheets
    return result


# ═══════════════════════════════════════════════════════════════════
# STAGE 2: LLM CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════

def llm_classify_folder(llm_client, file_profiles, client_name, client_context, model):
    summaries = []
    for fp in file_profiles:
        fname = fp["rel_path"]
        if fp.get("error"):
            summaries.append(f"FILE: {fname} — ERROR: {fp['error']}")
            continue
        total = fp.get("total_sheets", len(fp["sheets"]))
        skipped = fp.get("skipped_sheets", 0)
        size = fp.get("file_size_mb", "?")
        line = f"FILE: {fname} ({size}MB)"
        if skipped > 0:
            line += f" [{total} sheets total, showing {total - skipped} representative]"
        for sname, sp in fp["sheets"].items():
            if sp is None:
                line += f"\n  Sheet '{sname}': EMPTY"
            elif sp.get("error"):
                line += f"\n  Sheet '{sname}': ERROR {sp['error']}"
            else:
                line += f"\n  Sheet '{sname}': {sp['rows']}r x {sp['cols']}c"
                line += f"\n    Columns: {sp['named_cols'][:12]}"
                for col, samps in list(sp["samples"].items())[:4]:
                    line += f"\n    {col}: {samps}"
        if skipped > 0:
            line += f"\n  NOTE: {skipped} more sheets follow same distributor/customer pattern"
        summaries.append(line)

    ctx_block = f"\nCLIENT CONTEXT:\n{client_context}\n" if client_context else ""

    system = f"""You are an expert data analyst for a pricing diagnostic platform scanning {client_name} client data.
{ctx_block}
{CANONICAL_SCHEMA}

CLASSIFICATION RULES:
- "EXTRACT": contains actual structured data loadable into our schema — transactions, customers, products, pricing, contracts, deals, commissions, financials, ARR/MRR
- "CONTEXT": useful reference but already aggregated or not directly loadable
- "SKIP": working documents, presentations, surveys, trackers, strategy, roadmaps, meeting notes, org charts
- Be aggressive about extracting — customer names + revenue/ARR = useful
- Wide-format monthly columns need unpivoting (data_format = wide_dates/wide_months)
- ARR/MRR/subscription → transaction_fact and/or contract
- Pipeline/funnel data → deal_opportunity
- Commission models → sales_rep_dim
- P&L / budget schedules → monthly_financials
- For large per-distributor/customer files: use sheet="ALL_DISTRIBUTOR_SHEETS" for the pattern
"""

    prompt = f"""Classify every file and sheet in this {client_name} client data folder.

{chr(10).join(summaries)}

Return a JSON array. Each item:
- "file": filename (exact)
- "sheet": sheet name (or "ALL_DISTRIBUTOR_SHEETS" for per-distributor patterns)
- "action": "EXTRACT" | "CONTEXT" | "SKIP"
- "target_table": (EXTRACT only) canonical table name(s)
- "reason": one sentence
- "mapping_hints": (EXTRACT only) brief column mapping notes
"""

    text, usage = call_llm(llm_client, prompt, system=system, max_tokens=16000, model=model)
    result = extract_json(text, prefer_array=True)
    if isinstance(result, list):
        result = [r for r in result if isinstance(r, dict)]
    return result, usage


# ═══════════════════════════════════════════════════════════════════
# STAGE 3: LLM COLUMN MAPPING
# ═══════════════════════════════════════════════════════════════════

def llm_map_sheet(llm_client, filepath, sheet_name, classification, client_name,
                  client_context, model, engine=None):
    ext = os.path.splitext(filepath)[1].lower()
    is_large = os.path.getsize(filepath) / (1024 * 1024) > 20
    sample_nrows = 30 if is_large else None

    # ── Read the sheet ──
    if ext in (".csv", ".tsv"):
        sep = "\t" if ext == ".tsv" else ","
        try:
            df = pd.read_csv(filepath, sep=sep, nrows=sample_nrows or 10000)
        except Exception as e:
            return None, None, None
        df.columns = [str(c).strip() for c in df.columns]
    else:
        try:
            df_raw = pd.read_excel(
                filepath, sheet_name=sheet_name, header=None,
                engine=engine, nrows=(sample_nrows or 100),
            )
        except Exception:
            return None, None, None

        if df_raw.empty:
            return None, None, None
        if df_raw.shape[1] > 500:
            df_raw = df_raw.iloc[:, :500]

        # Detect header
        best_row, best_score = 0, 0
        for i in range(min(15, len(df_raw))):
            score = sum(
                1 for v in df_raw.iloc[i]
                if isinstance(v, str) and len(str(v).strip()) > 2
            )
            if score > best_score:
                best_score = score
                best_row = i

        df = pd.read_excel(
            filepath, sheet_name=sheet_name, header=best_row, engine=engine
        )
        if df.shape[1] > 500:
            df = df.iloc[:, :500]
        df.columns = [str(c).strip() for c in df.columns]

    # Deduplicate column names
    seen = {}
    new_cols = []
    for c in df.columns:
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    df.columns = new_cols

    # Build column description
    cols_detail = []
    for c in df.columns[:25]:
        if c.startswith("Unnamed"):
            continue
        samples = df[c].dropna().head(3).tolist()
        cols_detail.append(
            f"  - '{c}' ({df[c].dtype}, samples: {[str(s)[:50] for s in samples]})"
        )

    preview = df.head(3).to_dict("records")
    preview_clean = [
        {k: str(v)[:50] for k, v in row.items()
         if pd.notna(v) and not k.startswith("Unnamed")}
        for row in preview
    ]

    # Detect wide date columns
    date_cols = {}
    for col in df.columns:
        try:
            dt = pd.to_datetime(str(col).strip())
            if 2018 <= dt.year <= 2030:
                date_cols[col] = dt
        except Exception:
            pass
    has_date_cols = len(date_cols) >= 3

    target = classification.get("target_table", "transaction_fact")
    hints = classification.get("mapping_hints", "")
    ctx_snippet = client_context[:300] if client_context else ""

    prompt = f"""Map columns from this sheet to our canonical schema.

Client: {client_name}
{f"Context: {ctx_snippet}" if ctx_snippet else ""}
Target table: {target}
Hints: {hints}

Sheet: "{sheet_name}" ({len(df)} rows x {len(df.columns)} cols)
{"⚠ This sheet has " + str(len(date_cols)) + " date-formatted column headers — needs wide-to-long unpivoting." if has_date_cols else ""}

Columns:
{chr(10).join(cols_detail)}

Sample rows (first 3):
{json.dumps(preview_clean, indent=2, default=str)}

Return a JSON object with:
1. "column_mapping": {{source_col: canonical_field}} — use null to skip a column
2. "data_format": "wide_dates" | "wide_months" | "long"
3. "exclude_rules": list of row-filtering rules (e.g. "skip rows where customer_name is blank")
4. "id_columns": list of entity identifier columns
5. "notes": observations about data quality or mapping challenges
"""

    system = f"""You are a data engineer. Map source columns to canonical field names precisely.
{CANONICAL_SCHEMA}
Rules:
- Map to EXACT canonical field names only
- Use null for columns with no canonical mapping
- For wide formats, the date/month columns are value columns (map their values to 'revenue')
- Entity columns in wide format are the non-date columns"""

    text, usage = call_llm(
        llm_client, prompt, system=system, max_tokens=3000, model=model
    )
    mapping = extract_json(text)

    # Unwrap if LLM returned an array
    if isinstance(mapping, list):
        for item in mapping:
            if isinstance(item, dict) and "column_mapping" in item:
                mapping = item
                break
        else:
            dicts = [x for x in mapping if isinstance(x, dict)]
            mapping = dicts[0] if dicts else None

    return df, mapping, usage


# ═══════════════════════════════════════════════════════════════════
# STAGE 4: TRANSFORM
# ═══════════════════════════════════════════════════════════════════

def transform_sheet(df, mapping, sheet_name, source_file):
    if mapping is None:
        return []

    col_map = mapping.get("column_mapping", {})
    data_format = mapping.get("data_format", "long")
    exclude_rules = mapping.get("exclude_rules", [])
    id_columns = mapping.get("id_columns", [])

    df_clean = df.copy()

    # Apply exclusion rules
    for rule in exclude_rules:
        rl = rule.lower()
        if "total" in rl:
            for col in df_clean.columns:
                mask = df_clean[col].astype(str).str.lower().str.contains(
                    "total", na=False
                )
                df_clean = df_clean[~mask]
        if any(w in rl for w in ("blank", "empty", "nan")):
            for id_col in id_columns:
                if id_col in df_clean.columns:
                    df_clean = df_clean[df_clean[id_col].notna()]
                    df_clean = df_clean[
                        df_clean[id_col].astype(str).str.strip() != ""
                    ]
                    break

    records = []

    if data_format in ("wide_dates", "wide_months"):
        # Detect date header columns
        date_cols = {}
        for col in df_clean.columns:
            try:
                dt = pd.to_datetime(str(col).strip())
                if 2018 <= dt.year <= 2030:
                    date_cols[col] = dt
            except Exception:
                pass

        # Fallback: month name columns
        if not date_cols:
            month_map = {
                "january": 1, "february": 2, "march": 3, "april": 4,
                "may": 5, "june": 6, "july": 7, "august": 8,
                "september": 9, "october": 10, "november": 11, "december": 12,
                "jan": 1, "feb": 2, "mar": 3, "apr": 4,
                "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10,
                "nov": 11, "dec": 12,
            }
            for col in df_clean.columns:
                cl = str(col).lower().strip()
                if cl in month_map:
                    date_cols[col] = pd.Timestamp(
                        year=2024, month=month_map[cl], day=15
                    )

        static_cols = [
            c for c in df_clean.columns
            if c not in date_cols and not c.startswith("Unnamed")
        ]
        mapped_statics = {
            src: col_map[src]
            for src in static_cols
            if col_map.get(src) and col_map[src] not in (None, "null")
        }

        for _, row in df_clean.iterrows():
            base = {}
            for src, canon in mapped_statics.items():
                val = row.get(src)
                if isinstance(val, pd.Series):
                    val = val.iloc[0]
                if pd.notna(val):
                    base[canon] = val
            if not base:
                continue
            for col, dt_val in date_cols.items():
                val = row.get(col)
                if pd.notna(val):
                    try:
                        amount = float(val)
                    except (ValueError, TypeError):
                        continue
                    if abs(amount) < 0.01:
                        continue
                    rec = base.copy()
                    rec["transaction_date"] = dt_val.strftime("%Y-%m-%d")
                    rec["revenue"] = round(amount, 2)
                    rec["_source_file"] = source_file
                    rec["_source_sheet"] = sheet_name
                    records.append(rec)
    else:
        # Long format
        for _, row in df_clean.iterrows():
            rec = {}
            for src, canon in col_map.items():
                if (
                    canon
                    and canon not in (None, "null")
                    and src in df_clean.columns
                ):
                    val = row.get(src)
                    if isinstance(val, pd.Series):
                        val = val.iloc[0]
                    if pd.notna(val):
                        rec[canon] = val
            if rec and len(rec) >= 2:
                rec["_source_file"] = source_file
                rec["_source_sheet"] = sheet_name
                records.append(rec)

    return records


# ═══════════════════════════════════════════════════════════════════
# BUILD CANONICAL TABLES (add IDs, clean types)
# ═══════════════════════════════════════════════════════════════════

def build_canonical_tables(all_records, prefix, client_name):
    canonical = {}
    for table_name, records in all_records.items():
        if not records:
            continue
        df = pd.DataFrame(records)

        # Generate IDs where missing
        if "transaction_id" not in df.columns and "revenue" in df.columns:
            df.insert(
                0, "transaction_id",
                [f"{prefix}-TXN-{i+1:06d}" for i in range(len(df))],
            )
        if "customer_name" in df.columns and "customer_id" not in df.columns:
            names = sorted(
                n for n in
                (str(x).strip() for x in df["customer_name"].dropna().unique())
                if n and n.lower() not in ("nan", "none", "")
            )
            cmap = {n: f"{prefix}-CUST-{i+1:04d}" for i, n in enumerate(names)}
            idx = list(df.columns).index("customer_name") + 1
            df.insert(
                idx, "customer_id",
                df["customer_name"].map(lambda x: cmap.get(str(x).strip(), "")),
            )
        if "product_name" in df.columns and "product_sku" not in df.columns:
            prods = sorted(
                p for p in
                (str(x).strip() for x in df["product_name"].dropna().unique())
                if p and p.lower() not in ("nan", "none", "")
            )
            pmap = {p: f"{prefix}-SKU-{i+1:04d}" for i, p in enumerate(prods)}
            idx = list(df.columns).index("product_name")
            df.insert(
                idx, "product_sku",
                df["product_name"].map(lambda x: pmap.get(str(x).strip(), "")),
            )
        if "sales_rep_name" in df.columns and "sales_rep_id" not in df.columns:
            reps = sorted(
                r for r in
                (str(x).strip() for x in df["sales_rep_name"].dropna().unique())
                if r and r.lower() not in ("nan", "none", "")
            )
            rmap = {r: f"{prefix}-REP-{i+1:04d}" for i, r in enumerate(reps)}
            idx = list(df.columns).index("sales_rep_name")
            df.insert(
                idx, "sales_rep_id",
                df["sales_rep_name"].map(lambda x: rmap.get(str(x).strip(), "")),
            )
        if "deal_id" not in df.columns and table_name == "deal_opportunity":
            df.insert(
                0, "deal_id",
                [f"{prefix}-DEAL-{i+1:04d}" for i in range(len(df))],
            )
        if "contract_id" not in df.columns and table_name == "contract":
            df.insert(
                0, "contract_id",
                [f"{prefix}-CTR-{i+1:04d}" for i in range(len(df))],
            )

        # Clean date columns
        for col in df.columns:
            if "date" in col.lower():
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Clean numeric columns
        num_cols = [
            "revenue", "deal_amount", "contract_value", "annual_value", "amount",
            "list_price", "invoice_price", "pocket_price", "unit_price",
            "cost_of_goods", "gross_margin", "discount_pct", "discount_amount",
            "quantity", "margin_pct", "quota", "commission_rate",
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Add client identifier
        df.insert(0, "client_name", client_name)

        # Drop internal tracking columns
        df = df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")

        canonical[table_name] = df
        print(f"  {table_name}: {len(df):,} rows, {len(df.columns)} cols")
        if "revenue" in df.columns:
            rev = pd.to_numeric(df["revenue"], errors="coerce")
            print(f"    Revenue: ${rev.sum():,.0f} | avg ${rev.mean():,.0f}")
        if "customer_name" in df.columns:
            print(f"    Unique customers: {df['customer_name'].nunique()}")

    return canonical


# ═══════════════════════════════════════════════════════════════════
# STAGE 5: LLM VALIDATION
# ═══════════════════════════════════════════════════════════════════

def llm_validate(llm_client, canonical_tables, client_name, client_context, model):
    if not canonical_tables:
        return {}

    parts = []
    for tname, df in canonical_tables.items():
        parts.append(
            f"Table: {tname} ({len(df):,} rows, cols: {list(df.columns)[:12]})"
        )
        sample = df.head(2).to_dict("records")
        parts.append(f"  Sample: {json.dumps(sample, default=str)[:300]}")
        if "revenue" in df.columns:
            r = pd.to_numeric(df["revenue"], errors="coerce")
            parts.append(f"  Revenue: ${r.sum():,.0f}, negatives: {int((r < 0).sum())}")
        if "customer_name" in df.columns:
            parts.append(f"  Unique customers: {df['customer_name'].nunique()}")

    prompt = f"""Review these extracted tables from the {client_name} client for a pricing diagnostic platform.

{chr(10).join(parts)}

Client context: {client_context[:400] if client_context else 'Not provided'}

Return a JSON object:
{{
  "quality_score": 0-100,
  "grade": "A" | "B" | "C" | "D",
  "issues": ["..."],
  "recommendations": ["..."],
  "analyses_coverage": ["list of the 20 analyses that have sufficient data"]
}}
"""

    text, usage = call_llm(llm_client, prompt, max_tokens=3000, model=model)
    result = extract_json(text)
    if isinstance(result, list) and result:
        result = result[0] if isinstance(result[0], dict) else {}
    return result or {}, usage


# ═══════════════════════════════════════════════════════════════════
# EXPAND ALL_DISTRIBUTOR_SHEETS
# ═══════════════════════════════════════════════════════════════════

def expand_distributor_sheets(extract_list, file_profiles, max_distributor_sheets=8):
    expanded = []
    skip_words = [
        "summary", "accrual", "volume", "growth", "top 10", "offers",
        "liability", "itd", "validation", "style", "cover",
    ]
    for item in extract_list:
        if item.get("sheet") != "ALL_DISTRIBUTOR_SHEETS":
            expanded.append(item)
            continue
        fname = item.get("file", "")
        for fp in file_profiles:
            if fp["filename"] == fname or fp.get("rel_path") == fname or fname in fp.get("rel_path", ""):
                all_sheets = fp.get("all_sheet_names", list(fp["sheets"].keys()))
                dist_sheets = [
                    s for s in all_sheets
                    if not any(w in s.lower() for w in skip_words)
                ]
                for s in dist_sheets[:max_distributor_sheets]:
                    entry = item.copy()
                    entry["sheet"] = s
                    expanded.append(entry)
                print(
                    f"    Expanded {fname} -> "
                    f"{min(len(dist_sheets), max_distributor_sheets)}/{len(dist_sheets)} sheets"
                )
                break
    return expanded


# ═══════════════════════════════════════════════════════════════════
# SAVE OUTPUTS
# ═══════════════════════════════════════════════════════════════════

def save_outputs(canonical_tables, output_folder, client_name, meta):
    os.makedirs(output_folder, exist_ok=True)

    for tname, df in canonical_tables.items():
        path = os.path.join(output_folder, f"{client_name}_{tname}.xlsx")
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=tname[:31])
        print(f"  Saved: {os.path.basename(path)} ({len(df):,} rows)")

    # Combined workbook
    combined = os.path.join(output_folder, f"{client_name}_All_Canonical_Tables.xlsx")
    with pd.ExcelWriter(combined, engine="openpyxl") as writer:
        for tname, df in canonical_tables.items():
            df.to_excel(writer, index=False, sheet_name=tname[:31])
    print(f"  Saved: {os.path.basename(combined)} ({len(canonical_tables)} tables)")

    # Metadata
    meta_path = os.path.join(output_folder, f"{client_name}_pipeline_metadata.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2, default=str)
    print(f"  Saved: {os.path.basename(meta_path)}")


# ═══════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════

def run_pipeline(args):
    folder_path = os.path.abspath(args.folder)
    if not os.path.isdir(folder_path):
        print(f"ERROR: Folder not found: {folder_path}")
        sys.exit(1)

    # Derive client name and prefix from folder if not specified
    folder_name = os.path.basename(folder_path)
    client_name = args.client or re.sub(r"\s*(Client\s*Data|Data)$", "", folder_name, flags=re.IGNORECASE).strip() or folder_name
    prefix = args.prefix or derive_prefix(client_name)
    model = args.model

    # Output folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_folder = args.output or os.path.join(
        project_root, "4. Cleaned Output", f"{client_name}_Bronze"
    )

    # LLM client
    api_key = args.api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("ERROR: No API key. Set ANTHROPIC_API_KEY or pass --api-key")
        sys.exit(1)
    llm_client = get_llm_client(api_key)

    # Priority files
    priority_files = None
    if args.priority:
        priority_files = [p.strip() for p in args.priority.split(",")]

    print("=" * 70)
    print(f"FINTASTIQ UNIVERSAL BRONZE ETL — {client_name.upper()}")
    print("=" * 70)
    print(f"  Folder:  {folder_path}")
    print(f"  Output:  {output_folder}")
    print(f"  Prefix:  {prefix}")
    print(f"  Model:   {model}")
    print(f"  Run:     {datetime.now().isoformat()}")

    total_in, total_out = 0, 0

    # ── Load client context ──
    client_context = load_client_context(folder_path)
    if client_context:
        print(f"  Context: loaded ({len(client_context)} chars)")
    else:
        print("  Context: none (client_context.json not found)")

    # ══════════════════════════════════════════════════════
    # STAGE 1: DISCOVER & PROFILE
    # ══════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("STAGE 1: Discover & Profile")
    print(f"{'='*70}")

    files = discover_files(folder_path, priority_files)
    print(f"Found {len(files)} spreadsheet file(s)")

    file_profiles = []
    for f in files:
        rel = os.path.relpath(f, folder_path)
        print(f"  Profiling: {rel}")
        try:
            p = profile_file(f, folder_path, max_sheets=args.max_sheets)
            p["filepath"] = f
            file_profiles.append(p)
            n_sheets = len([s for s in p["sheets"] if p["sheets"][s] is not None])
            skipped = p.get("skipped_sheets", 0)
            note = f" (sampled {n_sheets}/{p.get('total_sheets', n_sheets)})" if skipped else ""
            print(f"    -> {n_sheets} sheets{note}")
        except Exception as e:
            print(f"    -> ERROR: {str(e)[:80]}")
            file_profiles.append({
                "filename": os.path.basename(f), "rel_path": rel,
                "filepath": f, "sheets": {}, "error": str(e)[:80],
            })

    if not file_profiles:
        print("No files found. Check --folder path.")
        return

    # ══════════════════════════════════════════════════════
    # STAGE 2: LLM CLASSIFICATION
    # ══════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("STAGE 2: LLM Classification")
    print(f"{'='*70}")

    t0 = time.time()
    classifications, usage = llm_classify_folder(
        llm_client, file_profiles, client_name, client_context, model
    )
    total_in += usage.input_tokens
    total_out += usage.output_tokens
    print(f"  {time.time()-t0:.1f}s | {usage.input_tokens:,} in / {usage.output_tokens:,} out tokens")

    if not classifications:
        print("FATAL: Could not parse classifications from LLM")
        return

    extract_list = []
    for item in classifications:
        if not isinstance(item, dict):
            continue
        action = item.get("action", "SKIP")
        if action == "EXTRACT":
            print(f"  [EXTRACT] {item.get('file','')} / {item.get('sheet','')} -> {item.get('target_table','?')}")
            extract_list.append(item)
        elif action == "CONTEXT":
            print(f"  [CONTEXT] {item.get('file','')} / {item.get('sheet','')}")

    # Expand ALL_DISTRIBUTOR_SHEETS
    extract_list = expand_distributor_sheets(
        extract_list, file_profiles, max_distributor_sheets=args.max_distributor_sheets
    )
    ctx_c = sum(1 for c in classifications if isinstance(c, dict) and c.get("action") == "CONTEXT")
    print(f"\n  EXTRACT: {len(extract_list)} | CONTEXT: {ctx_c}")

    # ══════════════════════════════════════════════════════
    # STAGE 3+4: EXTRACT & TRANSFORM
    # ══════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("STAGE 3+4: Extract & Transform")
    print(f"{'='*70}")

    all_records = {}

    for cls_item in extract_list:
        fname = cls_item.get("file", "")
        sname = cls_item.get("sheet", "")
        target = cls_item.get("target_table", "transaction_fact")
        if isinstance(target, list):
            target = target[0] if target else "transaction_fact"

        # Find matching file profile
        matched = None
        for fp in file_profiles:
            if fp["filename"] == fname or fp.get("rel_path") == fname:
                matched = fp
                break
        if not matched:
            for fp in file_profiles:
                if fname in fp.get("filename", "") or fname in fp.get("rel_path", ""):
                    matched = fp
                    break
        if not matched:
            print(f"  {fname}/{sname}: FILE NOT FOUND — skipping")
            continue

        filepath = matched["filepath"]
        ext = os.path.splitext(filepath)[1].lower()
        engine = "pyxlsb" if ext == ".xlsb" else None

        # Resolve sheet name (case-insensitive)
        if ext not in (".csv", ".tsv"):
            try:
                xl = pd.ExcelFile(filepath, engine=engine)
                avail = xl.sheet_names
            except Exception:
                avail = list(matched["sheets"].keys())

            if sname not in avail:
                found = next((a for a in avail if a.lower() == sname.lower()), None)
                if found:
                    sname = found
                else:
                    print(f"  {fname}/{sname}: sheet not found (available: {avail[:5]}) — skipping")
                    continue

        # LLM mapping
        t0 = time.time()
        try:
            df, mapping, m_usage = llm_map_sheet(
                llm_client, filepath, sname, cls_item,
                client_name, client_context, model, engine=engine,
            )
        except httpx.TimeoutException:
            print(f"  {fname}/{sname}: TIMEOUT after {time.time()-t0:.1f}s")
            continue
        except Exception as e:
            print(f"  {fname}/{sname}: ERROR — {str(e)[:80]}")
            continue

        elapsed = time.time() - t0
        if m_usage:
            total_in += m_usage.input_tokens
            total_out += m_usage.output_tokens

        if mapping is None:
            print(f"  {fname}/{sname}: mapping failed ({elapsed:.1f}s)")
            continue

        # For large files, re-read full data for transform
        if os.path.getsize(filepath) / (1024 * 1024) > 20 and ext not in (".csv", ".tsv"):
            try:
                df_raw_h = pd.read_excel(filepath, sheet_name=sname, header=None, engine=engine, nrows=20)
                best_row = 0
                best_score = 0
                for i in range(min(15, len(df_raw_h))):
                    score = sum(1 for v in df_raw_h.iloc[i] if isinstance(v, str) and len(str(v).strip()) > 2)
                    if score > best_score:
                        best_score = score
                        best_row = i
                df = pd.read_excel(filepath, sheet_name=sname, header=best_row, engine=engine, nrows=100000)
                df.columns = [str(c).strip() for c in df.columns]
                seen = {}
                new_cols = []
                for c in df.columns:
                    if c in seen:
                        seen[c] += 1
                        new_cols.append(f"{c}_{seen[c]}")
                    else:
                        seen[c] = 0
                        new_cols.append(c)
                df.columns = new_cols
            except Exception as e:
                print(f"  {fname}/{sname}: large-file re-read error: {str(e)[:60]}, using sample")

        try:
            records = transform_sheet(df, mapping, sname, fname)
        except Exception as e:
            print(f"  {fname}/{sname}: transform error — {str(e)[:80]}")
            continue

        table_name = (
            target.split(",")[0].split("+")[0].split("/")[0].strip().replace(" ", "_")
        )
        all_records.setdefault(table_name, []).extend(records)

        data_fmt = mapping.get("data_format", "?") if isinstance(mapping, dict) else "?"
        notes = mapping.get("notes", "") if isinstance(mapping, dict) else ""
        notes_str = ("; ".join(str(n) for n in notes) if isinstance(notes, list) else str(notes))[:80]
        print(
            f"  {fname[:45]:45s} / {sname[:25]:25s} "
            f"-> {len(records):5,} rows [{data_fmt}] ({elapsed:.1f}s)"
            + (f"\n    Note: {notes_str}" if notes_str else "")
        )

    # ══════════════════════════════════════════════════════
    # BUILD CANONICAL TABLES
    # ══════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("Building Canonical Tables")
    print(f"{'='*70}")

    canonical_tables = build_canonical_tables(all_records, prefix, client_name)

    if not canonical_tables:
        print("WARNING: No canonical tables produced. Check classifications and mappings.")

    # ══════════════════════════════════════════════════════
    # STAGE 5: LLM VALIDATION
    # ══════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("STAGE 5: LLM Validation")
    print(f"{'='*70}")

    validation = {}
    if canonical_tables:
        t0 = time.time()
        try:
            validation, v_usage = llm_validate(
                llm_client, canonical_tables, client_name, client_context, model
            )
            total_in += v_usage.input_tokens
            total_out += v_usage.output_tokens
            grade = validation.get("grade", "?")
            score = validation.get("quality_score", 0)
            print(f"  Grade: {grade} ({score}/100) — {time.time()-t0:.1f}s")
            for issue in validation.get("issues", [])[:5]:
                print(f"    Issue: {issue}")
            for rec in validation.get("recommendations", [])[:3]:
                print(f"    Rec:   {rec}")
            coverage = validation.get("analyses_coverage", [])
            if coverage:
                print(f"  Analyses covered: {len(coverage)}/20")
        except Exception as e:
            print(f"  Validation error: {str(e)[:80]}")

    # ══════════════════════════════════════════════════════
    # SAVE OUTPUTS
    # ══════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("Saving Bronze Tables")
    print(f"{'='*70}")

    total_records = sum(len(d) for d in canonical_tables.values())
    est_cost = (total_in * 3 + total_out * 15) / 1_000_000

    meta = {
        "client": client_name,
        "prefix": prefix,
        "run_timestamp": datetime.now().isoformat(),
        "source_folder": folder_path,
        "output_folder": output_folder,
        "model": model,
        "files_scanned": len(file_profiles),
        "sheets_extracted": len(extract_list),
        "tables_created": {t: len(d) for t, d in canonical_tables.items()},
        "total_records": total_records,
        "api_usage": {
            "input_tokens": total_in,
            "output_tokens": total_out,
            "est_cost_usd": round(est_cost, 4),
        },
        "classifications": classifications,
        "validation": validation,
    }

    if canonical_tables:
        save_outputs(canonical_tables, output_folder, client_name, meta)
    else:
        os.makedirs(output_folder, exist_ok=True)
        meta_path = os.path.join(output_folder, f"{client_name}_pipeline_metadata.json")
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2, default=str)

    # ══════════════════════════════════════════════════════
    # SUMMARY
    # ══════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("PIPELINE COMPLETE")
    print(f"{'='*70}")
    print(f"  Client:          {client_name}")
    print(f"  Files scanned:   {len(file_profiles)}")
    print(f"  Sheets extracted:{len(extract_list)}")
    print(f"  Tables produced: {len(canonical_tables)}")
    print(f"  Total records:   {total_records:,}")
    for t, d in canonical_tables.items():
        rev_str = ""
        if "revenue" in d.columns:
            r = pd.to_numeric(d["revenue"], errors="coerce")
            rev_str = f"  (${r.sum():,.0f})"
        print(f"    {t}: {len(d):,} rows{rev_str}")
    print(f"\n  API tokens:      {total_in:,} in / {total_out:,} out")
    print(f"  Estimated cost:  ${est_cost:.4f}")
    if validation:
        print(f"  Quality:         {validation.get('grade','?')} ({validation.get('quality_score',0)}/100)")
    print(f"  Output:          {output_folder}")

    return canonical_tables


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="FintastIQ Universal Bronze ETL — maps any client folder to canonical schema",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_bronze_universal.py --folder "2. Client Internal Data/Gravitate Client Data"
  python run_bronze_universal.py --folder "/data/NewCo Data" --client "NewCo" --prefix "NWC"
  python run_bronze_universal.py --folder "/data/BigCo" --priority "orders.xlsx,pricing.csv"
  python run_bronze_universal.py --folder "/data/Client" --max-sheets 20 --max-distributor-sheets 5
        """,
    )
    parser.add_argument("--folder", required=True, help="Path to client data folder")
    parser.add_argument("--client", default=None, help="Client name (auto-derived from folder if omitted)")
    parser.add_argument("--prefix", default=None, help="3-letter ID prefix (auto-derived if omitted)")
    parser.add_argument("--output", default=None, help="Output folder (default: 4. Cleaned Output/<client>_Bronze)")
    parser.add_argument("--priority", default=None, help="Comma-separated list of priority filenames to process first")
    parser.add_argument("--max-sheets", type=int, default=12, help="Max sheets to profile per Excel file (default: 12)")
    parser.add_argument("--max-distributor-sheets", type=int, default=8, help="Max distributor/per-entity sheets to expand (default: 8)")
    parser.add_argument("--model", default="claude-sonnet-4-20250514", help="Claude model to use")
    parser.add_argument("--api-key", default=None, help="Anthropic API key (or set ANTHROPIC_API_KEY env var)")

    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
