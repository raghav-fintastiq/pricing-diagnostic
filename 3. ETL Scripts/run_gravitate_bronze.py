"""
FintastIQ ETL — Gravitate Bronze Pipeline
Adapts the universal folder pipeline for Gravitate client data.
Injects Gravitate client context into LLM prompts.
"""
import pandas as pd
import numpy as np
import json, re, time, os, sys, glob, traceback
import httpx, anthropic
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-20250514"
CLIENT = "Gravitate"
PREFIX = "GRV"

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
FOLDER = os.path.join(_PROJECT_ROOT, "2. Client Internal Data", "Gravitate Client Data")
OUTPUT = os.path.join(_PROJECT_ROOT, "4. Cleaned Output", "Gravitate_LLM_v2")
CONTEXT_PATH = os.path.join(FOLDER, "client_context.json")

# Load client context
CLIENT_CONTEXT = ""
if os.path.exists(CONTEXT_PATH):
    with open(CONTEXT_PATH) as f:
        ctx = json.load(f)
    # Build context block — handle flat or nested structure
    parts = []
    # Flat structure (keys at top level)
    if 'company_name' in ctx:
        parts.append(f"Company: {ctx.get('company_name','?')} | Industry: {ctx.get('industry','?')} | Model: {ctx.get('business_model','?')}")
        parts.append(f"Revenue: {ctx.get('revenue_range','?')} | Products: {ctx.get('product_type','?')}")
        parts.append(f"Goal: {ctx.get('engagement_goal','?')}")
        for k in ['primary_pricing_model', 'customer_segmentation', 'professional_services_structure',
                   'product_modules', 'pricing_variables', 'current_arr_estimate']:
            v = ctx.get(k)
            if v and str(v).strip():
                parts.append(f"{k}: {str(v)[:150]}")
    # Nested structure
    elif 'tier1' in ctx:
        t1 = ctx['tier1']
        parts.append(f"Company: {t1.get('company_name','?')} | Industry: {t1.get('industry','?')} | Model: {t1.get('business_model','?')}")
        parts.append(f"Revenue: {t1.get('revenue_range','?')} | Products: {t1.get('product_type','?')}")
        parts.append(f"Goal: {t1.get('engagement_goal','?')}")
        t3 = ctx.get('tier3_open', {})
        if t3:
            for k, v in t3.items():
                if v and str(v).strip():
                    parts.append(f"{k}: {str(v)[:150]}")
    CLIENT_CONTEXT = "\n".join(parts)
    print(f"Loaded client context ({len(CLIENT_CONTEXT)} chars)")

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
Contract Leakage, SKU Pareto, Rep Behavior, Churn Sensitivity, etc.
"""


def get_client():
    return anthropic.Anthropic(api_key=API_KEY, http_client=httpx.Client(verify=False, timeout=120.0))


def call_llm(prompt, system=None, max_tokens=4096):
    client = get_client()
    messages = [{"role": "user", "content": prompt}]
    for attempt in range(3):
        try:
            kwargs = {"model": MODEL, "max_tokens": max_tokens, "messages": messages}
            if system:
                kwargs["system"] = system
            response = client.messages.create(**kwargs)
            return response.content[0].text, response.usage
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                raise e


def extract_json(text, prefer_array=False):
    match = re.search(r'```(?:json)?\s*\n?([\s\S]*?)\n?```', text)
    if match:
        text = match.group(1)
    order = [('[', ']'), ('{', '}')] if prefer_array else [('{', '}'), ('[', ']')]
    for sc, ec in order:
        start = text.find(sc)
        if start >= 0:
            depth = 0
            for i in range(start, len(text)):
                if text[i] == sc: depth += 1
                elif text[i] == ec:
                    depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(text[start:i+1])
                        except:
                            break
    try:
        return json.loads(text.strip())
    except:
        return None


# ═══════════════════════════════════════════════════════════════════
# STAGE 1: DISCOVER & PROFILE ALL FILES
# ═══════════════════════════════════════════════════════════════════

def discover_files(folder_path):
    """Find all spreadsheet files in the folder tree, excluding contract PDFs."""
    extensions = ['*.xlsx', '*.xlsb', '*.csv', '*.tsv', '*.xls']
    files = []
    for ext in extensions:
        files.extend(glob.glob(os.path.join(folder_path, '**', ext), recursive=True))
    # Filter out known non-data files
    filtered = []
    for f in sorted(files):
        bn = os.path.basename(f).lower()
        # Skip zip files disguised, temp files, etc
        if bn.startswith('~') or bn.startswith('.'):
            continue
        filtered.append(f)
    return filtered


def profile_file(filepath):
    """Profile all sheets in a spreadsheet file."""
    ext = os.path.splitext(filepath)[1].lower()
    filename = os.path.basename(filepath)
    # Build relative path from Gravitate folder
    rel_path = filepath.replace(FOLDER + '/', '') if FOLDER in filepath else filename

    if ext == '.xlsb':
        try:
            xl = pd.ExcelFile(filepath, engine='pyxlsb')
            sheets = xl.sheet_names
        except Exception as e:
            return {'filename': filename, 'rel_path': rel_path, 'error': f'xlsb read error: {str(e)[:60]}', 'sheets': {}}
    elif ext in ('.csv', '.tsv'):
        try:
            sep = '\t' if ext == '.tsv' else ','
            df = pd.read_csv(filepath, nrows=10, sep=sep)
            return {
                'filename': filename, 'rel_path': rel_path,
                'sheets': {'Sheet1': profile_single_sheet_df(df, filename)}
            }
        except Exception as e:
            return {'filename': filename, 'rel_path': rel_path, 'error': str(e)[:60], 'sheets': {}}
    else:
        try:
            xl = pd.ExcelFile(filepath)
            sheets = xl.sheet_names
        except Exception as e:
            return {'filename': filename, 'rel_path': rel_path, 'error': str(e)[:60], 'sheets': {}}

    profiles = {}
    for sheet in sheets:
        try:
            engine = 'pyxlsb' if ext == '.xlsb' else None
            df_raw = pd.read_excel(filepath, sheet_name=sheet, header=None, engine=engine)
            if df_raw.empty:
                profiles[sheet] = None
                continue

            # Detect header row
            best_row, best_score = 0, 0
            for i in range(min(15, len(df_raw))):
                score = sum(1 for v in df_raw.iloc[i] if isinstance(v, str) and len(str(v).strip()) > 2)
                if score > best_score:
                    best_score = score
                    best_row = i

            df = pd.read_excel(filepath, sheet_name=sheet, header=best_row, engine=engine)
            df.columns = [str(c).strip() for c in df.columns]
            profiles[sheet] = profile_single_sheet_df(df, sheet)
        except Exception as e:
            profiles[sheet] = {'error': str(e)[:60]}

    return {'filename': filename, 'rel_path': rel_path, 'sheets': profiles}


def profile_single_sheet_df(df, name):
    """Profile a DataFrame for LLM classification."""
    cols = [str(c) for c in df.columns if not str(c).startswith('Unnamed')][:20]
    samples = {}
    for c in cols[:10]:
        if c in df.columns:
            s = df[c].dropna().head(3).tolist()
            samples[c] = [str(v)[:50] for v in s]
    return {
        'rows': len(df),
        'cols': len(df.columns),
        'named_cols': cols,
        'samples': samples,
    }


# ═══════════════════════════════════════════════════════════════════
# STAGE 2: LLM FILE-LEVEL CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════

def llm_classify_folder(file_profiles):
    """Single LLM call to classify ALL files and sheets in the folder."""

    summaries = []
    for fp in file_profiles:
        fname = fp['rel_path']
        if fp.get('error'):
            summaries.append(f"FILE: {fname} — ERROR: {fp['error']}")
            continue

        file_summary = f"FILE: {fname}"
        for sname, sp in fp['sheets'].items():
            if sp is None:
                file_summary += f"\n  Sheet '{sname}': EMPTY"
            elif sp.get('error'):
                file_summary += f"\n  Sheet '{sname}': ERROR {sp['error']}"
            else:
                file_summary += f"\n  Sheet '{sname}': {sp['rows']}r x {sp['cols']}c"
                file_summary += f"\n    Columns: {sp['named_cols'][:12]}"
                for col, samps in list(sp['samples'].items())[:5]:
                    file_summary += f"\n    {col}: {samps}"
        summaries.append(file_summary)

    folder_text = "\n\n".join(summaries)

    context_block = ""
    if CLIENT_CONTEXT:
        context_block = f"""
CLIENT CONTEXT (use this to better understand the data):
{CLIENT_CONTEXT}
"""

    system = f"""You are an expert data analyst for a pricing diagnostic platform. You are scanning an entire client data folder to determine which files and sheets contain useful data for pricing analyses.
{context_block}
{CANONICAL_SCHEMA}

CLASSIFICATION RULES:
- "EXTRACT" = contains actual structured data (transactions, customers, products, pricing, contracts, deals, commissions, financial schedules, ARR/revenue data) that can be loaded into our schema
- "CONTEXT" = useful reference but already aggregated or not directly loadable
- "SKIP" = working documents, presentations, surveys, org charts, strategy analyses, data request trackers, market research, roadmaps, meeting notes
- PDFs are NOT in this list (we're only looking at spreadsheets)
- For EXTRACT sheets, specify which canonical table(s) the data maps to
- Be aggressive about extracting — if it has customer names + revenue/ARR, it's useful
- Revenue analysis files with wide-format monthly columns need unpivoting
- ARR/MRR/subscription data maps to transaction_fact and/or contract
- Sales funnel / pipeline data maps to deal_opportunity
- Commission models map to sales_rep_dim
- Financial schedules (P&L, budget vs actual) map to monthly_financials
- Product roadmaps with features/pricing map to product_dim
- Board reporting packages with financial data should be extracted
"""

    prompt = f"""Classify every file and sheet in this {CLIENT} client data folder.

{folder_text}

Return a JSON array where each item has:
- "file": filename
- "sheet": sheet name
- "action": "EXTRACT" | "CONTEXT" | "SKIP"
- "target_table": for EXTRACT only — which canonical table(s)
- "reason": 1 sentence
- "mapping_hints": for EXTRACT only — brief notes on which columns map where
"""

    response_text, usage = call_llm(prompt, system=system, max_tokens=16000)
    classifications = extract_json(response_text, prefer_array=True)

    if isinstance(classifications, list):
        classifications = [c for c in classifications if isinstance(c, dict)]

    return classifications, usage


# ═══════════════════════════════════════════════════════════════════
# STAGE 3: LLM COLUMN MAPPING
# ═══════════════════════════════════════════════════════════════════

def llm_map_sheet(filepath, sheet_name, classification, engine=None):
    """Read a sheet and use LLM to map its columns to canonical schema."""

    df_raw = pd.read_excel(filepath, sheet_name=sheet_name, header=None, engine=engine)
    if df_raw.empty:
        return None, None, None

    if df_raw.shape[1] > 500:
        df_raw = df_raw.iloc[:, :500]

    # Detect header
    best_row, best_score = 0, 0
    for i in range(min(15, len(df_raw))):
        score = sum(1 for v in df_raw.iloc[i] if isinstance(v, str) and len(str(v).strip()) > 2)
        if score > best_score:
            best_score = score
            best_row = i

    df = pd.read_excel(filepath, sheet_name=sheet_name, header=best_row, engine=engine)
    if df.shape[1] > 500:
        df = df.iloc[:, :500]
    df.columns = [str(c).strip() for c in df.columns]

    # Deduplicate columns
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

    # Build column info
    cols_detail = []
    for c in df.columns[:25]:  # show more columns for Gravitate
        if c.startswith('Unnamed'):
            continue
        dtype = str(df[c].dtype)
        samples = df[c].dropna().head(3).tolist()
        samples_str = [str(s)[:50] for s in samples]
        cols_detail.append(f"  - '{c}' ({dtype}, samples: {samples_str})")
    cols_text = "\n".join(cols_detail)

    # Preview rows
    preview = df.head(3).to_dict('records')
    preview_clean = []
    for row in preview:
        clean = {k: str(v)[:50] for k, v in row.items() if pd.notna(v) and not k.startswith('Unnamed')}
        preview_clean.append(clean)
    preview_text = json.dumps(preview_clean[:3], indent=2, default=str)

    target = classification.get('target_table', 'transaction_fact')
    hints = classification.get('mapping_hints', '')

    # Detect date columns
    date_cols = {}
    for col in df.columns:
        try:
            dt = pd.to_datetime(str(col).strip())
            if 2018 <= dt.year <= 2030:
                date_cols[col] = dt
        except:
            pass
    has_date_cols = len(date_cols) >= 3

    context_block = ""
    if CLIENT_CONTEXT:
        context_block = f"\nClient context: {CLIENT_CONTEXT[:300]}\n"

    prompt = f"""Map columns from this sheet to our canonical schema.

Target table: {target}
Hints: {hints}
{context_block}
Sheet: "{sheet_name}" ({len(df)} rows x {len(df.columns)} cols)
{'This sheet has ' + str(len(date_cols)) + ' date-formatted columns — needs wide-to-long unpivoting.' if has_date_cols else ''}

Columns:
{cols_text}

Sample rows:
{preview_text}

Provide a JSON object with:
1. "column_mapping": dict of source_column -> canonical_field (null to skip)
2. "data_format": "wide_dates" | "wide_months" | "long"
3. "exclude_rules": list of rules for rows to skip (e.g. "skip totals", "skip blank customer_name")
4. "id_columns": list of entity identifier columns
5. "notes": observations about data quality
"""

    system = f"""You are a data engineer. Map source columns to canonical fields precisely.
{CANONICAL_SCHEMA}
Rules: Map to exact canonical field names. Use null for columns to skip. For wide formats, specify which columns are date/value columns vs entity columns."""

    response_text, usage = call_llm(prompt, system=system, max_tokens=3000)
    mapping = extract_json(response_text)

    if isinstance(mapping, list):
        for item in mapping:
            if isinstance(item, dict) and 'column_mapping' in item:
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
    """Transform a sheet using LLM mapping instructions."""
    if mapping is None:
        return []

    col_map = mapping.get('column_mapping', {})
    data_format = mapping.get('data_format', 'long')
    exclude_rules = mapping.get('exclude_rules', [])
    id_columns = mapping.get('id_columns', [])

    # Apply exclusion rules
    df_clean = df.copy()
    for rule in exclude_rules:
        rl = rule.lower()
        if 'total' in rl:
            for col in df_clean.columns:
                mask = df_clean[col].astype(str).str.lower().str.contains('total', na=False)
                df_clean = df_clean[~mask]
        if 'blank' in rl or 'empty' in rl or 'nan' in rl:
            for id_col in id_columns:
                if id_col in df_clean.columns:
                    df_clean = df_clean[df_clean[id_col].notna()]
                    df_clean = df_clean[df_clean[id_col].astype(str).str.strip() != '']
                    break

    records = []

    if data_format in ('wide_dates', 'wide_months'):
        # Find date columns
        date_cols = {}
        for col in df_clean.columns:
            try:
                dt = pd.to_datetime(str(col).strip())
                if 2018 <= dt.year <= 2030:
                    date_cols[col] = dt
            except:
                pass

        if not date_cols:
            month_map = {'january':1,'february':2,'march':3,'april':4,'may':5,'june':6,
                        'july':7,'august':8,'september':9,'october':10,'november':11,'december':12,
                        'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
                        'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
            for col in df_clean.columns:
                cl = str(col).lower().strip()
                if cl in month_map:
                    date_cols[col] = pd.Timestamp(year=2023, month=month_map[cl], day=15)

        static_cols = [c for c in df_clean.columns if c not in date_cols and not c.startswith('Unnamed')]
        mapped_statics = {}
        for src in static_cols:
            canon = col_map.get(src)
            if canon and canon != 'null' and canon is not None:
                mapped_statics[src] = canon

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
                    rec['transaction_date'] = dt_val.strftime('%Y-%m-%d')
                    rec['revenue'] = round(amount, 2)
                    rec['_source_file'] = source_file
                    rec['_source_sheet'] = sheet_name
                    records.append(rec)
    else:
        # Long format
        for _, row in df_clean.iterrows():
            rec = {}
            for src, canon in col_map.items():
                if canon and canon != 'null' and canon is not None and src in df_clean.columns:
                    val = row.get(src)
                    if isinstance(val, pd.Series):
                        val = val.iloc[0]
                    if pd.notna(val):
                        rec[canon] = val
            if rec and len(rec) >= 2:
                rec['_source_file'] = source_file
                rec['_source_sheet'] = sheet_name
                records.append(rec)

    return records


# ═══════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════

def run_folder_pipeline(folder_path, output_folder):
    print("=" * 70)
    print(f"FINTASTIQ FOLDER-LEVEL ETL — {CLIENT} CLIENT DATA")
    print("=" * 70)
    print(f"Source folder: {folder_path}")
    print(f"Output folder: {output_folder}")
    print(f"Model: {MODEL}")
    print(f"Run: {datetime.now().isoformat()}")

    total_in, total_out = 0, 0

    # ── STAGE 1: DISCOVER & PROFILE ──
    print(f"\n{'='*70}")
    print("STAGE 1: Discover & Profile")
    print(f"{'='*70}")

    files = discover_files(folder_path)
    print(f"Found {len(files)} spreadsheet files")

    file_profiles = []
    for f in files:
        rel = f.replace(folder_path + '/', '') if folder_path in f else os.path.basename(f)
        print(f"  Profiling: {rel}")
        try:
            p = profile_file(f)
            p['filepath'] = f
            file_profiles.append(p)
            sheet_count = len([s for s in p['sheets'] if p['sheets'][s] is not None])
            print(f"    -> {sheet_count} sheets")
        except Exception as e:
            print(f"    -> ERROR: {str(e)[:60]}")
            file_profiles.append({'filename': os.path.basename(f), 'rel_path': rel, 'filepath': f, 'sheets': {}, 'error': str(e)[:60]})

    # ── STAGE 2: LLM CLASSIFICATION ──
    print(f"\n{'='*70}")
    print("STAGE 2: LLM File & Sheet Classification")
    print(f"{'='*70}")

    t0 = time.time()
    classifications, usage = llm_classify_folder(file_profiles)
    t1 = time.time()
    total_in += usage.input_tokens
    total_out += usage.output_tokens
    print(f"  API call: {t1-t0:.1f}s, {usage.input_tokens} in / {usage.output_tokens} out")

    if classifications is None:
        print("  FATAL: Could not parse classifications")
        return

    # Index classifications
    extract_list = []
    for item in classifications:
        if not isinstance(item, dict):
            continue
        fname = item.get('file', '')
        sname = item.get('sheet', '')
        action = item.get('action', 'SKIP')

        if action == 'EXTRACT':
            target = item.get('target_table', '?')
            reason = item.get('reason', '')
            print(f"  [EXTRACT] {fname} / {sname}")
            print(f"           -> {target} -- {reason[:70]}")
            extract_list.append(item)
        elif action == 'CONTEXT':
            print(f"  [CONTEXT] {fname} / {sname}")

    total_sheets = sum(len(fp['sheets']) for fp in file_profiles)
    ctx_count = sum(1 for c in classifications if isinstance(c, dict) and c.get('action') == 'CONTEXT')
    skip_count = total_sheets - len(extract_list) - ctx_count
    print(f"\n  Total sheets: {total_sheets}")
    print(f"  EXTRACT: {len(extract_list)}, CONTEXT: {ctx_count}, SKIP: {skip_count}")

    # ── STAGE 3+4: EXTRACT & TRANSFORM ──
    print(f"\n{'='*70}")
    print("STAGE 3+4: Extract & Transform")
    print(f"{'='*70}")

    all_records = {}

    for cls_item in extract_list:
        fname = cls_item.get('file', '')
        sname = cls_item.get('sheet', '')
        target = cls_item.get('target_table', 'transaction_fact')
        if isinstance(target, list):
            target = target[0] if target else 'transaction_fact'

        # Find the file
        matched_fp = None
        for fp in file_profiles:
            if fp['filename'] == fname or fp['rel_path'] == fname:
                matched_fp = fp
                break
        if not matched_fp:
            for fp in file_profiles:
                if fname in fp['filename'] or fname in fp['rel_path']:
                    matched_fp = fp
                    break

        if not matched_fp:
            print(f"  {fname}/{sname}: FILE NOT FOUND")
            continue

        filepath = matched_fp['filepath']
        ext = os.path.splitext(filepath)[1].lower()
        engine = 'pyxlsb' if ext == '.xlsb' else None

        # Check if sheet exists
        if sname not in matched_fp['sheets']:
            avail = list(matched_fp['sheets'].keys())
            found = False
            for a in avail:
                if sname.lower() == a.lower():
                    sname = a
                    found = True
                    break
            if not found:
                print(f"  {fname}/{sname}: SHEET NOT FOUND (available: {avail[:5]})")
                continue

        # LLM mapping
        t0 = time.time()
        try:
            df, mapping, usage = llm_map_sheet(filepath, sname, cls_item, engine=engine)
        except httpx.TimeoutException:
            print(f"  {fname:50s} / {sname:30s} -> TIMEOUT ({time.time()-t0:.1f}s)")
            continue
        except Exception as e:
            print(f"  {fname}/{sname}: ERROR reading: {str(e)[:80]}")
            continue
        t1 = time.time()

        if usage:
            total_in += usage.input_tokens
            total_out += usage.output_tokens

        if mapping is None:
            print(f"  {fname}/{sname}: MAPPING FAILED ({t1-t0:.1f}s)")
            continue

        # Transform
        try:
            records = transform_sheet(df, mapping, sname, fname)
        except Exception as e:
            print(f"  {fname}/{sname}: TRANSFORM ERROR: {str(e)[:80]}")
            continue

        # Route to target table
        table_name = target.split(',')[0].split('+')[0].split('/')[0].strip().replace(' ', '_')
        if table_name not in all_records:
            all_records[table_name] = []
        all_records[table_name].extend(records)

        notes = mapping.get('notes', '') if isinstance(mapping, dict) else ''
        fmt = mapping.get('data_format', '?') if isinstance(mapping, dict) else '?'
        print(f"  {fname:50s} / {sname:30s} -> {len(records):5d} [{fmt}] ({t1-t0:.1f}s)")
        if notes:
            if isinstance(notes, list):
                notes = '; '.join(str(n) for n in notes)
            print(f"    Note: {str(notes)[:90]}")

    # ── BUILD CANONICAL DATAFRAMES ──
    print(f"\n{'='*70}")
    print("Building Canonical Tables")
    print(f"{'='*70}")

    canonical_tables = {}

    for table_name, records in all_records.items():
        if not records:
            continue

        df = pd.DataFrame(records)

        # Generate IDs with Gravitate prefix
        if 'transaction_id' not in df.columns and 'revenue' in df.columns:
            df.insert(0, 'transaction_id', [f"{PREFIX}-TXN-{i+1:06d}" for i in range(len(df))])

        if 'customer_name' in df.columns and 'customer_id' not in df.columns:
            names = sorted(set(str(x).strip() for x in df['customer_name'].dropna().unique()
                              if str(x).strip() and str(x).strip().lower() not in ('nan', 'none', '')))
            cmap = {n: f"{PREFIX}-CUST-{i+1:04d}" for i, n in enumerate(names)}
            idx = list(df.columns).index('customer_name') + 1
            df.insert(idx, 'customer_id', df['customer_name'].map(lambda x: cmap.get(str(x).strip(), '')))

        if 'product_name' in df.columns and 'product_sku' not in df.columns:
            prods = sorted(set(str(x).strip() for x in df['product_name'].dropna().unique()
                              if str(x).strip() and str(x).strip().lower() not in ('nan', 'none', '')))
            pmap = {n: f"{PREFIX}-SKU-{i+1:04d}" for i, n in enumerate(prods)}
            idx = list(df.columns).index('product_name')
            df.insert(idx, 'product_sku', df['product_name'].map(lambda x: pmap.get(str(x).strip(), '')))

        if 'sales_rep_name' in df.columns and 'sales_rep_id' not in df.columns:
            reps = sorted(set(str(x).strip() for x in df['sales_rep_name'].dropna().unique()
                             if str(x).strip() and str(x).strip().lower() not in ('nan', 'none', '')))
            rmap = {n: f"{PREFIX}-REP-{i+1:04d}" for i, n in enumerate(reps)}
            idx = list(df.columns).index('sales_rep_name')
            df.insert(idx, 'sales_rep_id', df['sales_rep_name'].map(lambda x: rmap.get(str(x).strip(), '')))

        if 'deal_id' not in df.columns and table_name == 'deal_opportunity':
            df.insert(0, 'deal_id', [f"{PREFIX}-DEAL-{i+1:04d}" for i in range(len(df))])

        if 'contract_id' not in df.columns and table_name == 'contract':
            df.insert(0, 'contract_id', [f"{PREFIX}-CTR-{i+1:04d}" for i in range(len(df))])

        # Clean dates
        for col in df.columns:
            if 'date' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Clean revenue
        for rev_col in ['revenue', 'deal_amount', 'contract_value', 'annual_value', 'amount']:
            if rev_col in df.columns:
                df[rev_col] = pd.to_numeric(df[rev_col], errors='coerce')

        # Remove internal columns
        internal_cols = [c for c in df.columns if c.startswith('_')]
        df_clean = df.drop(columns=internal_cols, errors='ignore')

        canonical_tables[table_name] = df_clean
        print(f"  {table_name}: {len(df_clean)} rows, {len(df_clean.columns)} columns")
        if 'revenue' in df_clean.columns:
            rev = pd.to_numeric(df_clean['revenue'], errors='coerce')
            print(f"    Revenue: ${rev.sum():,.2f} (avg ${rev.mean():,.2f})")
        if 'customer_name' in df_clean.columns:
            print(f"    Unique customers: {df_clean['customer_name'].nunique()}")

    # ── STAGE 5: LLM VALIDATE ──
    print(f"\n{'='*70}")
    print("STAGE 5: LLM Validation")
    print(f"{'='*70}")

    val_parts = []
    for tname, df in canonical_tables.items():
        val_parts.append(f"Table: {tname} ({len(df)} rows, cols: {list(df.columns)[:12]})")
        val_parts.append(f"  Sample: {df.head(2).to_dict('records')}")
        if 'revenue' in df.columns:
            r = pd.to_numeric(df['revenue'], errors='coerce')
            val_parts.append(f"  Revenue: ${r.sum():,.2f}, neg={int((r<0).sum())}")
        if 'customer_name' in df.columns:
            val_parts.append(f"  Customers: {df['customer_name'].nunique()}")

    val_prompt = f"""Review these extracted tables from the {CLIENT} client data folder for a pricing diagnostic platform.

{chr(10).join(val_parts)}

Client context: {CLIENT_CONTEXT[:500]}

Provide a JSON object with:
1. "quality_score": 0-100
2. "grade": "A"/"B"/"C"/"D"
3. "issues": list of quality issues
4. "recommendations": list of improvements
5. "analyses_coverage": which of the 20 pricing analyses have data
"""

    t0 = time.time()
    val_text, val_usage = call_llm(val_prompt, max_tokens=3000)
    t1 = time.time()
    total_in += val_usage.input_tokens
    total_out += val_usage.output_tokens
    validation = extract_json(val_text)
    if isinstance(validation, list) and validation:
        validation = validation[0] if isinstance(validation[0], dict) else {}

    if validation:
        grade = validation.get('grade', '?')
        score = validation.get('quality_score', 0)
        print(f"  Grade: {grade} ({score}/100) ({t1-t0:.1f}s)")
        for issue in validation.get('issues', [])[:5]:
            print(f"    Issue: {issue}")
        for rec in validation.get('recommendations', [])[:5]:
            print(f"    Rec: {rec}")
        coverage = validation.get('analyses_coverage', [])
        if coverage and isinstance(coverage, list):
            cov_strs = [str(c) for c in coverage[:8]]
            print(f"  Analyses covered ({len(coverage)}/20): {', '.join(cov_strs)}...")

    # ── SAVE OUTPUT ──
    print(f"\n{'='*70}")
    print("Saving Bronze Tables")
    print(f"{'='*70}")

    os.makedirs(output_folder, exist_ok=True)

    for tname, df in canonical_tables.items():
        fname = f"{CLIENT}_{tname}.xlsx"
        fpath = os.path.join(output_folder, fname)
        with pd.ExcelWriter(fpath, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=tname)
        print(f"  Saved: {fname} ({len(df)} rows)")

    # Combined workbook
    combined_path = os.path.join(output_folder, f"{CLIENT}_All_Canonical_Tables.xlsx")
    with pd.ExcelWriter(combined_path, engine='openpyxl') as writer:
        for tname, df in canonical_tables.items():
            sname = tname[:31]
            df.to_excel(writer, index=False, sheet_name=sname)
    print(f"  Saved: {CLIENT}_All_Canonical_Tables.xlsx (combined, {len(canonical_tables)} sheets)")

    # Save metadata
    meta = {
        'client': CLIENT,
        'run_timestamp': datetime.now().isoformat(),
        'source_folder': folder_path,
        'files_scanned': len(file_profiles),
        'sheets_extracted': len(extract_list),
        'tables_created': {t: len(d) for t, d in canonical_tables.items()},
        'total_records': sum(len(d) for d in canonical_tables.values()),
        'api_usage': {'input_tokens': total_in, 'output_tokens': total_out,
                      'est_cost': (total_in * 3 + total_out * 15) / 1_000_000},
        'classifications': classifications,
        'validation': validation,
    }
    meta_path = os.path.join(output_folder, f"{CLIENT}_pipeline_metadata.json")
    with open(meta_path, 'w') as f:
        json.dump(meta, f, indent=2, default=str)
    print(f"  Saved: {CLIENT}_pipeline_metadata.json")

    # ── SUMMARY ──
    total_records = sum(len(d) for d in canonical_tables.values())
    est_cost = (total_in * 3 + total_out * 15) / 1_000_000

    print(f"\n{'='*70}")
    print("PIPELINE SUMMARY")
    print(f"{'='*70}")
    print(f"  Client:           {CLIENT}")
    print(f"  Files scanned:    {len(file_profiles)}")
    print(f"  Sheets extracted: {len(extract_list)}")
    print(f"  Tables created:   {len(canonical_tables)}")
    print(f"  Total records:    {total_records:,}")
    for t, d in canonical_tables.items():
        rev = ""
        if 'revenue' in d.columns:
            r = pd.to_numeric(d['revenue'], errors='coerce')
            rev = f" (${r.sum():,.2f})"
        print(f"    {t}: {len(d)} rows{rev}")
    print(f"\n  API: {total_in:,} in / {total_out:,} out tokens")
    print(f"  Cost: ${est_cost:.4f}")
    if validation:
        print(f"  Quality: {validation.get('grade', '?')} ({validation.get('quality_score', 0)}/100)")

    return canonical_tables


if __name__ == '__main__':
    run_folder_pipeline(FOLDER, OUTPUT)
