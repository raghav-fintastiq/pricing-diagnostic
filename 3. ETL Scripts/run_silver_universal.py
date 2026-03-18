"""
FintastIQ Universal Silver Cleaning Script
Cleans bronze output for any client — no client-specific code.

Usage:
    python run_silver_universal.py --folder "/path/to/Bronze_Output" [options]

Options:
    --folder   Path to bronze output folder (required)
    --client   Client name (auto-derived from folder name if omitted)
    --prefix   3-letter prefix (auto-derived from client name if omitted)
    --output   Output folder (defaults to <folder>/../<Client>_Silver)
    --model    Claude model (default: claude-sonnet-4-20250514)
    --api-key  Anthropic API key (defaults to ANTHROPIC_API_KEY env var)
"""

import argparse, json, os, re, time, warnings
import numpy as np
import pandas as pd
import httpx, anthropic
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")

# ── DEFAULTS ──────────────────────────────────────────────────────────────────
DEFAULT_MODEL = "claude-sonnet-4-20250514"


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def get_client(api_key: str, model: str):
    return anthropic.Anthropic(
        api_key=api_key,
        http_client=httpx.Client(verify=False, timeout=120.0)
    ), model


def llm_call(anthropic_client, model, prompt, max_tokens=8000):
    resp = anthropic_client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}]
    )
    return resp.content[0].text, resp.usage


def extract_json(text, prefer_array=False):
    first_char = "[" if prefer_array else "{"
    second_char = "{" if prefer_array else "["
    for ch in [first_char, second_char]:
        idx = text.find(ch)
        if idx == -1:
            continue
        depth = 0
        close = "]" if ch == "[" else "}"
        for i in range(idx, len(text)):
            if text[i] == ch:
                depth += 1
            elif text[i] == close:
                depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[idx:i + 1])
                except Exception:
                    break
    try:
        return json.loads(text.strip())
    except Exception:
        return None


def derive_client_name(folder_path: str) -> str:
    """Infer client name from folder path."""
    name = Path(folder_path).name
    for suffix in ["_Bronze", "_LLM", "_LLM_v2", "_v2", "_Output", "_Bronze_Output"]:
        name = re.sub(re.escape(suffix), "", name, flags=re.IGNORECASE)
    # If it looks like "ClientName_prefix", strip the prefix
    name = re.sub(r"_[A-Z]{2,4}$", "", name)
    return name.strip()


def derive_prefix(client_name: str) -> str:
    """Generate 3-letter prefix from client name."""
    words = re.sub(r"[^a-zA-Z ]", "", client_name).upper().split()
    if len(words) == 1:
        return words[0][:3]
    elif len(words) == 2:
        return words[0][:2] + words[1][:1]
    else:
        return "".join(w[0] for w in words[:3])


def load_client_context(bronze_folder: str, client_name: str) -> str:
    """Load client_context.json from nearby directories."""
    candidates = [
        os.path.join(bronze_folder, "client_context.json"),
        os.path.join(os.path.dirname(bronze_folder), "client_context.json"),
        os.path.join(os.path.dirname(bronze_folder), f"{client_name} Client Data", "client_context.json"),
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                with open(path) as f:
                    ctx = json.load(f)
                # Flatten nested structures
                if isinstance(ctx, dict):
                    flat = {}
                    for k, v in ctx.items():
                        if isinstance(v, dict):
                            flat.update(v)
                        else:
                            flat[k] = v
                    return json.dumps(flat, indent=2)
                return json.dumps(ctx, indent=2)
            except Exception:
                pass
    return f'{{"client_name": "{client_name}"}}'


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1: DATE & TYPE FIXES
# ═══════════════════════════════════════════════════════════════════════════════

def fix_dates(df: pd.DataFrame, log: list, tname: str) -> pd.DataFrame:
    date_cols = [c for c in df.columns if "date" in c.lower()]
    for col in date_cols:
        # Detect Excel serial numbers stored as floats/ints
        numeric_vals = pd.to_numeric(df[col], errors="coerce")
        serial_mask = numeric_vals.notna() & (numeric_vals > 25000) & (numeric_vals < 55000)
        if serial_mask.sum() > 0:
            converted = pd.to_datetime(
                numeric_vals[serial_mask], unit="D", origin="1899-12-30", errors="coerce"
            )
            df.loc[serial_mask, col] = converted
            log.append({"table": tname, "column": col, "action": "convert_serial_dates",
                        "detail": f"Converted {serial_mask.sum()} Excel serial numbers"})

        df[col] = pd.to_datetime(df[col], errors="coerce")

        # Flag 1970 epochs
        if df[col].notna().any():
            mask_1970 = df[col].dt.year == 1970
            count_1970 = mask_1970.sum()
            if count_1970 > 0:
                log.append({"table": tname, "column": col, "action": "flag_1970_dates",
                            "detail": f"{count_1970} rows may have epoch corruption"})
                print(f"    WARNING: {count_1970} rows with 1970 dates in {col}")

    return df


def fix_numerics(df: pd.DataFrame, log: list, tname: str) -> pd.DataFrame:
    # Detect numeric columns by name pattern
    numeric_patterns = [
        "revenue", "price", "cost", "amount", "value", "discount",
        "quantity", "margin", "pct", "rate", "quota", "budget",
        "commission", "variance", "probability", "score", "index",
    ]
    for col in df.columns:
        col_lower = col.lower()
        if any(p in col_lower for p in numeric_patterns):
            before = df[col].notna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            after = df[col].notna().sum()
            if before != after:
                log.append({"table": tname, "column": col, "action": "coerce_numeric",
                            "detail": f"{before - after} values became NaN"})
    return df


def stage1_type_fixes(tables: dict, log: list) -> tuple:
    print(f"\n{'='*70}")
    print("STAGE 1: Date & Type Fixes")
    print(f"{'='*70}")

    for tname, df in tables.items():
        print(f"\n  {tname} ({len(df):,} rows)...")

        df = fix_dates(df, log, tname)
        df = fix_numerics(df, log, tname)

        # Drop fully empty rows
        before = len(df)
        df = df.dropna(how="all")
        if len(df) < before:
            log.append({"table": tname, "action": "drop_empty_rows",
                        "detail": f"Dropped {before - len(df)} empty rows"})
            print(f"    Dropped {before - len(df)} empty rows")

        # Drop exact duplicates
        before = len(df)
        df = df.drop_duplicates()
        if len(df) < before:
            log.append({"table": tname, "action": "drop_duplicates",
                        "detail": f"Dropped {before - len(df)} duplicates"})
            print(f"    Dropped {before - len(df)} duplicates")

        # Strip strings
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(["nan", "None", "NaT", ""], np.nan)

        tables[tname] = df
        print(f"    → {len(df):,} rows after fixes")

    return tables, log


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2: ENTITY RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

def stage2_entity_resolution(
    tables: dict, log: list, client_name: str, prefix: str,
    context_str: str, anthropic_client, model: str
) -> tuple:
    print(f"\n{'='*70}")
    print("STAGE 2: Entity Resolution")
    print(f"{'='*70}")

    total_in = total_out = 0

    # ── Customer name deduplication ────────────────────────────────────────
    customer_names = set()
    for df in tables.values():
        if "customer_name" in df.columns:
            vals = df["customer_name"].dropna().unique()
            customer_names.update(
                str(v).strip() for v in vals
                if str(v).strip() and str(v).strip().lower() not in ("nan", "none", "")
            )

    if len(customer_names) > 5:
        print(f"\n  Found {len(customer_names)} unique customer names")
        name_list = sorted(customer_names)
        batch_size = 200
        merge_map = {}

        for i in range(0, len(name_list), batch_size):
            batch = name_list[i:i + batch_size]
            prompt = f"""You are a data steward for a B2B company. Client context:
{context_str}

Below are {len(batch)} customer names from the database. Many are duplicates with variations.

Group them into canonical names. Return a JSON object: canonical_name → [list of all variants including canonical].
Rules:
- Use the most complete/formal version as canonical
- Only merge if confident they're the same entity
- Include single entries (no merge needed) too

Names:
{json.dumps(batch, indent=1)}
"""
            try:
                text, usage = llm_call(anthropic_client, model, prompt)
                total_in += usage.input_tokens
                total_out += usage.output_tokens
                groups = extract_json(text)
                if groups and isinstance(groups, dict):
                    for canonical, variants in groups.items():
                        if isinstance(variants, list):
                            for v in variants:
                                if str(v).strip() != str(canonical).strip():
                                    merge_map[str(v).strip()] = str(canonical).strip()
                    print(f"    Batch {i // batch_size + 1}: {len(merge_map)} merges so far")
            except Exception as e:
                print(f"    Batch {i // batch_size + 1}: ERROR {str(e)[:60]}")

        if merge_map:
            for tname, df in tables.items():
                if "customer_name" in df.columns:
                    before = df["customer_name"].nunique()
                    df["customer_name"] = df["customer_name"].map(
                        lambda x: merge_map.get(str(x).strip(), str(x).strip()) if pd.notna(x) else x
                    )
                    after = df["customer_name"].nunique()
                    if before != after:
                        log.append({"table": tname, "column": "customer_name", "action": "entity_merge",
                                    "detail": f"Merged {before} → {after} unique names"})
                        print(f"    {tname}: {before} → {after} unique customers")
                    tables[tname] = df

    # ── Dedup deal_opportunity ─────────────────────────────────────────────
    for tname, df in tables.items():
        if "deal_opportunity" in tname:
            if "customer_name" in df.columns and "opportunity_name" in df.columns:
                before = len(df)
                df = df.drop_duplicates(subset=["customer_name", "opportunity_name"], keep="last")
                if len(df) < before:
                    log.append({"table": tname, "action": "dedup_deals",
                                "detail": f"{before} → {len(df)} deals"})
                    print(f"    Deduped deals: {before} → {len(df)}")
                tables[tname] = df

    # ── Dedup sales_rep_dim ────────────────────────────────────────────────
    for tname, df in tables.items():
        if "sales_rep" in tname:
            rep_col = next((c for c in ["sales_rep_name", "rep_name", "rep"] if c in df.columns), None)
            if rep_col:
                before = len(df)
                df = df.drop_duplicates(subset=[rep_col], keep="last")
                if len(df) < before:
                    log.append({"table": tname, "action": "dedup_reps",
                                "detail": f"{before} → {len(df)} reps"})
                    print(f"    Deduped reps: {before} → {len(df)}")
                tables[tname] = df

    return tables, log, total_in, total_out


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 3: IMPUTATION & ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════════════

def stage3_imputation(
    tables: dict, log: list, client_name: str,
    context_str: str, anthropic_client, model: str
) -> tuple:
    print(f"\n{'='*70}")
    print("STAGE 3: Imputation & Enrichment")
    print(f"{'='*70}")

    total_in = total_out = 0

    # ── Customer dim enrichment ────────────────────────────────────────────
    cust_table = next(
        (k for k in tables if "customer_dim" in k or "customer" in k and "fact" not in k), None
    )
    if cust_table:
        df_cust = tables[cust_table]
        print(f"\n  Enriching {cust_table}...")

        # Show fill rates
        for col in df_cust.columns:
            fill = df_cust[col].notna().sum() / max(len(df_cust), 1) * 100
            if fill < 80:
                print(f"    {col}: {fill:.0f}% filled")

        if "customer_name" in df_cust.columns:
            names_sample = df_cust["customer_name"].dropna().unique()[:50]
            if len(names_sample) > 0:
                prompt = f"""You are analyzing customer data for {client_name}.
Context: {context_str}

For each customer name below, infer:
1. "industry" - their primary industry
2. "segment" - Enterprise / Mid-Market / SMB / Distributor / Other
3. "tier" - Large / Mid / Small (based on company size/prestige)
4. "company_type" - Corporation / LLC / Partnership / Government / Unknown

Return a JSON array: [{{"customer_name": ..., "industry": ..., "segment": ..., "tier": ..., "company_type": ...}}]

Customer names:
{json.dumps(list(names_sample), indent=1)}
"""
                try:
                    text, usage = llm_call(anthropic_client, model, prompt)
                    total_in += usage.input_tokens
                    total_out += usage.output_tokens
                    enrichments = extract_json(text, prefer_array=True)
                    # Unwrap if wrapped in outer array
                    if isinstance(enrichments, list) and len(enrichments) == 1 and isinstance(enrichments[0], list):
                        enrichments = enrichments[0]
                    if enrichments and isinstance(enrichments, list):
                        enrich_map = {
                            item["customer_name"]: item
                            for item in enrichments
                            if isinstance(item, dict) and "customer_name" in item
                        }
                        filled = 0
                        for field in ["industry", "segment", "tier", "company_type"]:
                            if field not in df_cust.columns:
                                df_cust[field] = np.nan
                            for idx, row in df_cust.iterrows():
                                name = str(row.get("customer_name", ""))
                                if name in enrich_map and pd.isna(row.get(field)):
                                    val = enrich_map[name].get(field)
                                    if val:
                                        df_cust.at[idx, field] = val
                                        filled += 1
                        if filled:
                            log.append({"table": cust_table, "action": "llm_enrich",
                                        "detail": f"Filled {filled} fields via LLM"})
                            print(f"    Enriched {filled} fields via LLM")
                except Exception as e:
                    print(f"    Enrichment error: {str(e)[:80]}")

        tables[cust_table] = df_cust

    # ── Flag negative revenue ──────────────────────────────────────────────
    for tname, df in tables.items():
        if "transaction" in tname or "fact" in tname:
            if "revenue" in df.columns:
                neg_mask = pd.to_numeric(df["revenue"], errors="coerce") < 0
                neg_count = neg_mask.sum()
                if neg_count > 0:
                    if "revenue_type" not in df.columns:
                        df["revenue_type"] = "standard"
                    df.loc[neg_mask, "revenue_type"] = "credit/adjustment"
                    log.append({"table": tname, "action": "flag_negatives",
                                "detail": f"Flagged {neg_count} negative revenue rows"})
                    print(f"    {tname}: flagged {neg_count} negative revenue rows")
                tables[tname] = df

    # ── Categorize financial line items ───────────────────────────────────
    for tname, df in tables.items():
        if "financial" in tname or "p_and_l" in tname:
            if "line_item" in df.columns and "category" not in df.columns:
                df["category"] = "uncategorized"
                for idx, row in df.iterrows():
                    item = str(row.get("line_item", "")).lower()
                    if any(w in item for w in ["revenue", "arr", "mrr", "sales", "income"]):
                        df.at[idx, "category"] = "revenue"
                    elif any(w in item for w in ["cost", "cogs", "expense", "salary", "rent", "depreciation"]):
                        df.at[idx, "category"] = "expense"
                    elif any(w in item for w in ["margin", "profit", "ebitda", "net"]):
                        df.at[idx, "category"] = "margin"
                log.append({"table": tname, "action": "categorize_line_items",
                            "detail": "Auto-categorized financial line items"})
                tables[tname] = df

    # ── Compute missing discount_pct from list_price / invoice_price ──────
    for tname, df in tables.items():
        if "discount_pct" not in df.columns and "list_price" in df.columns and "invoice_price" in df.columns:
            lp = pd.to_numeric(df["list_price"], errors="coerce")
            ip = pd.to_numeric(df["invoice_price"], errors="coerce")
            valid = lp.notna() & ip.notna() & (lp > 0)
            if valid.sum() > 0:
                df["discount_pct"] = np.where(valid, (lp - ip) / lp * 100, np.nan)
                log.append({"table": tname, "action": "compute_discount_pct",
                            "detail": f"Derived discount_pct for {valid.sum()} rows"})
                print(f"    {tname}: derived discount_pct for {valid.sum()} rows")
            tables[tname] = df

    return tables, log, total_in, total_out


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 4: RECONCILIATION REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def stage4_reconciliation(
    bronze_tables: dict, silver_tables: dict, log: list, client_name: str
) -> dict:
    print(f"\n{'='*70}")
    print("STAGE 4: Reconciliation Report")
    print(f"{'='*70}")

    report = {
        "client": client_name,
        "timestamp": datetime.now().isoformat(),
        "tables": {},
    }

    for tname, s_df in silver_tables.items():
        b_df = bronze_tables.get(tname, pd.DataFrame())
        entry = {
            "bronze_rows": len(b_df),
            "silver_rows": len(s_df),
            "row_delta": len(s_df) - len(b_df),
            "bronze_cols": len(b_df.columns) if len(b_df) > 0 else 0,
            "silver_cols": len(s_df.columns),
        }

        if "revenue" in s_df.columns:
            s_rev = pd.to_numeric(s_df["revenue"], errors="coerce").sum()
            b_rev = pd.to_numeric(b_df["revenue"], errors="coerce").sum() if "revenue" in b_df.columns else 0
            entry["bronze_revenue"] = round(float(b_rev), 2)
            entry["silver_revenue"] = round(float(s_rev), 2)
            entry["revenue_delta"] = round(float(s_rev - b_rev), 2)

        if "customer_name" in s_df.columns:
            entry["silver_unique_customers"] = int(s_df["customer_name"].nunique())
            if "customer_name" in b_df.columns:
                entry["bronze_unique_customers"] = int(b_df["customer_name"].nunique())

        report["tables"][tname] = entry
        print(f"  {tname}: {entry['bronze_rows']:,} → {entry['silver_rows']:,} rows ({entry['row_delta']:+d})")

    report["cleaning_log"] = log
    report["total_log_entries"] = len(log)
    return report


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def run_cleaning_pipeline(args) -> dict:
    bronze_folder = args.folder
    api_key = args.api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    model = args.model

    # Derive client metadata
    client_name = args.client or derive_client_name(bronze_folder)
    prefix = args.prefix or derive_prefix(client_name)
    output_folder = args.output or os.path.join(
        os.path.dirname(bronze_folder), f"{client_name}_Silver"
    )

    context_str = load_client_context(bronze_folder, client_name)
    anthropic_client, _ = get_client(api_key, model)

    print(f"{'='*70}")
    print(f"FINTASTIQ UNIVERSAL SILVER CLEANING MODULE")
    print(f"{'='*70}")
    print(f"  Client:        {client_name} [{prefix}]")
    print(f"  Bronze folder: {bronze_folder}")
    print(f"  Output folder: {output_folder}")
    print(f"  Model:         {model}")
    print(f"  Run:           {datetime.now().isoformat()}")

    os.makedirs(output_folder, exist_ok=True)

    # ── Load bronze tables ──────────────────────────────────────────────────
    print(f"\nLoading Bronze tables...")
    table_files = [
        f for f in os.listdir(bronze_folder)
        if (f.endswith(".xlsx") or f.endswith(".csv"))
        and not f.endswith("_All_Tables.xlsx")
        and not f.endswith("_silver.xlsx")
        and "pipeline_metadata" not in f
        and "reconciliation" not in f
        and "cleaning_log" not in f
    ]

    if not table_files:
        raise FileNotFoundError(f"No bronze table files found in: {bronze_folder}")

    bronze_tables = {}
    for fname in sorted(table_files):
        tname = os.path.splitext(fname)[0]
        fpath = os.path.join(bronze_folder, fname)
        try:
            if fname.endswith(".csv"):
                df = pd.read_csv(fpath, low_memory=False)
            else:
                df = pd.read_excel(fpath)
            bronze_tables[tname] = df
            print(f"  Loaded {tname}: {len(df):,} rows × {len(df.columns)} cols")
        except Exception as e:
            print(f"  WARNING: Could not load {fname}: {str(e)[:60]}")

    # Keep originals for reconciliation
    bronze_copies = {k: v.copy() for k, v in bronze_tables.items()}
    log = []

    # ── Stage 1 ─────────────────────────────────────────────────────────────
    t0 = time.time()
    bronze_tables, log = stage1_type_fixes(bronze_tables, log)
    print(f"\n  Stage 1 complete ({time.time()-t0:.1f}s)")

    # ── Stage 2 ─────────────────────────────────────────────────────────────
    t0 = time.time()
    silver, log, s2_in, s2_out = stage2_entity_resolution(
        bronze_tables, log, client_name, prefix, context_str, anthropic_client, model
    )
    print(f"\n  Stage 2 complete ({time.time()-t0:.1f}s)")

    # ── Stage 3 ─────────────────────────────────────────────────────────────
    t0 = time.time()
    silver, log, s3_in, s3_out = stage3_imputation(
        silver, log, client_name, context_str, anthropic_client, model
    )
    print(f"\n  Stage 3 complete ({time.time()-t0:.1f}s)")

    # ── Stage 4 ─────────────────────────────────────────────────────────────
    report = stage4_reconciliation(bronze_copies, silver, log, client_name)

    total_in = s2_in + s3_in
    total_out = s2_out + s3_out

    # ── Save silver tables ──────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("Saving Silver Tables")
    print(f"{'='*70}")

    for tname, df in silver.items():
        outpath = os.path.join(output_folder, f"{tname}_silver.xlsx")
        with pd.ExcelWriter(outpath, engine="openpyxl") as writer:
            sheet = tname[:31]
            df.to_excel(writer, index=False, sheet_name=sheet)
        print(f"  Saved: {tname}_silver.xlsx ({len(df):,} rows)")

    # Combined workbook
    combined_path = os.path.join(output_folder, f"{client_name}_Silver_All_Tables.xlsx")
    with pd.ExcelWriter(combined_path, engine="openpyxl") as writer:
        for tname, df in silver.items():
            df.to_excel(writer, index=False, sheet_name=tname[:31])
    print(f"  Saved: {client_name}_Silver_All_Tables.xlsx (combined)")

    # Reports
    for fname, obj in [("reconciliation_report.json", report), ("cleaning_log.json", log)]:
        with open(os.path.join(output_folder, fname), "w") as f:
            json.dump(obj, f, indent=2, default=str)
        print(f"  Saved: {fname}")

    # ── Summary ─────────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("CLEANING SUMMARY")
    print(f"{'='*70}")
    print(f"  Client:          {client_name} [{prefix}]")
    print(f"  Tables cleaned:  {len(silver)}")
    total_rows = sum(len(df) for df in silver.values())
    print(f"  Total rows:      {total_rows:,}")
    for tname, df in silver.items():
        extras = []
        if "revenue" in df.columns:
            r = pd.to_numeric(df["revenue"], errors="coerce")
            extras.append(f"${r.sum():,.0f} revenue")
        if "customer_name" in df.columns:
            extras.append(f"{df['customer_name'].nunique()} customers")
        extra = f"  ({', '.join(extras)})" if extras else ""
        print(f"    {tname}: {len(df):,} rows{extra}")
    print(f"\n  API tokens:  {total_in:,} in / {total_out:,} out")
    print(f"  Est. cost:   ${(total_in * 3 + total_out * 15) / 1_000_000:.4f}")
    print(f"  Log entries: {len(log)}")
    print(f"\n  Output: {output_folder}")

    return {"silver_tables": silver, "report": report, "output_folder": output_folder}


def main():
    parser = argparse.ArgumentParser(
        description="FintastIQ Universal Silver Cleaning Script"
    )
    parser.add_argument(
        "--folder", required=True,
        help="Path to the bronze output folder"
    )
    parser.add_argument(
        "--client", default=None,
        help="Client name (auto-derived from folder name if omitted)"
    )
    parser.add_argument(
        "--prefix", default=None,
        help="3-letter prefix (auto-derived if omitted)"
    )
    parser.add_argument(
        "--output", default=None,
        help="Output folder for silver tables (defaults to sibling <Client>_Silver dir)"
    )
    parser.add_argument(
        "--model", default=DEFAULT_MODEL,
        help=f"Claude model to use (default: {DEFAULT_MODEL})"
    )
    parser.add_argument(
        "--api-key", default=None,
        help="Anthropic API key (defaults to ANTHROPIC_API_KEY env var)"
    )

    args = parser.parse_args()

    if not os.path.isdir(args.folder):
        print(f"ERROR: folder not found: {args.folder}")
        raise SystemExit(1)

    result = run_cleaning_pipeline(args)
    print(f"\nDone. Silver tables in: {result['output_folder']}")


if __name__ == "__main__":
    main()
