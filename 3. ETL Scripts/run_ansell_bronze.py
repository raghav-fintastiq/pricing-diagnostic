"""
FintastIQ ETL — Ansell Bronze Pipeline
Handles Ansell's large dataset (231 files, 1000+ sheets) by:
1. Profiling core files in full
2. Sampling representative sheets from large tracker files
3. Processing the most important data files for pricing diagnostics
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
CLIENT = "Ansell"
PREFIX = "ANS"

# Paths relative to this script's location (works on any machine)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)  # one level up from "3. ETL Scripts"
FOLDER = os.path.join(_PROJECT_ROOT, "2. Client Internal Data", "Ansell Client Data")
OUTPUT = os.path.join(_PROJECT_ROOT, "4. Cleaned Output", "Ansell_LLM")
CONTEXT_PATH = os.path.join(FOLDER, "client_context.json")

# Load client context
CLIENT_CONTEXT = ""
if os.path.exists(CONTEXT_PATH):
    with open(CONTEXT_PATH) as f:
        ctx = json.load(f)
    parts = []
    if 'company_name' in ctx:
        parts.append(f"Company: {ctx.get('company_name','?')} | Industry: {ctx.get('industry','?')} | Model: {ctx.get('business_model','?')}")
        parts.append(f"Revenue: {ctx.get('revenue_range','?')} | Products: {ctx.get('product_type','?')}")
        parts.append(f"Goal: {ctx.get('engagement_goal','?')}")
    elif 'tier1' in ctx:
        t1 = ctx['tier1']
        parts.append(f"Company: {t1.get('company_name','?')} | Industry: {t1.get('industry','?')} | Model: {t1.get('business_model','?')}")
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


# Priority files for Ansell
PRIORITY_FILES = [
    # Core transaction data
    "F24_US_Orders_Shipped.xlsx",
    "US - FY24 - Orders Shipped with Price Source.xlsx",
    # Claims / rebates
    "Claim Report_Jan 2020 to May 2024 ORIGINAL.xlsx",
    "Claim Report_Jan to Dec 2019 ORIGINAL.xlsx",
    # End-user net billing
    "EUNB_Sales_JAN19 to tilldate.xlsx",
    # Pricing reference
    "Sample Pricing Data GIS.xlsx",
    # Incentive agreements
    "2024 Incentive Agreement Summary.xlsx",
    "2023 Incentive Agreement Summary.xlsx",
    # Rebate tracking
    "2024 Partnership Rebate Calculations 06 June 2024.xlsx",
    "Ansell Canada Partnership Rebates June 2024.xlsx",
    # Product / cost reference
    "UOM PC to CA conversion.xlsx",
    # IND Trackers (most recent year only)
    "Ansell US IND Tracker 2023.xlsx",
    "Ansell Canada IND Tracker 2023.xlsx",
]

MAX_SHEETS_PER_FILE = 12


def profile_file(filepath, max_sheets=MAX_SHEETS_PER_FILE):
    ext = os.path.splitext(filepath)[1].lower()
    filename = os.path.basename(filepath)
    rel_path = filepath.replace(FOLDER + '/', '') if FOLDER in filepath else filename

    if ext == '.xlsb':
        try:
            xl = pd.ExcelFile(filepath, engine='pyxlsb')
            sheets = xl.sheet_names
        except Exception as e:
            return {'filename': filename, 'rel_path': rel_path, 'error': str(e)[:60], 'sheets': {}}
    elif ext in ('.csv', '.tsv'):
        try:
            sep = '\t' if ext == '.tsv' else ','
            df = pd.read_csv(filepath, nrows=10, sep=sep)
            cols = [str(c) for c in df.columns if not str(c).startswith('Unnamed')][:15]
            samples = {}
            for c in cols[:8]:
                if c in df.columns:
                    s = df[c].dropna().head(3).tolist()
                    samples[c] = [str(v)[:50] for v in s]
            return {'filename': filename, 'rel_path': rel_path,
                    'sheets': {'Sheet1': {'rows': len(df), 'cols': len(df.columns), 'named_cols': cols, 'samples': samples}}}
        except Exception as e:
            return {'filename': filename, 'rel_path': rel_path, 'error': str(e)[:60], 'sheets': {}}
    else:
        try:
            xl = pd.ExcelFile(filepath)
            sheets = xl.sheet_names
        except Exception as e:
            return {'filename': filename, 'rel_path': rel_path, 'error': str(e)[:60], 'sheets': {}}

    if len(sheets) > max_sheets:
        sampled = sheets[:4] + sheets[-2:]
        step = max(1, len(sheets) // (max_sheets - 6))
        for i in range(4, len(sheets) - 2, step):
            if sheets[i] not in sampled and len(sampled) < max_sheets:
                sampled.append(sheets[i])
        skipped = len(sheets) - len(sampled)
    else:
        sampled = sheets
        skipped = 0

    profiles = {}
    engine = 'pyxlsb' if ext == '.xlsb' else None
    # For large files, only read a sample of rows during profiling
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    profile_nrows = 20 if file_size_mb > 20 else None  # sample 20 rows for large files
    for sheet in sampled:
        try:
            df_raw = pd.read_excel(filepath, sheet_name=sheet, header=None, engine=engine, nrows=25)
            if df_raw.empty:
                profiles[sheet] = None
                continue
            best_row, best_score = 0, 0
            for i in range(min(15, len(df_raw))):
                score = sum(1 for v in df_raw.iloc[i] if isinstance(v, str) and len(str(v).strip()) > 2)
                if score > best_score:
                    best_score = score
                    best_row = i
            df = pd.read_excel(filepath, sheet_name=sheet, header=best_row, engine=engine, nrows=profile_nrows)
            df.columns = [str(c).strip() for c in df.columns]
            cols = [str(c) for c in df.columns if not str(c).startswith('Unnamed')][:15]
            samples = {}
            for c in cols[:8]:
                if c in df.columns:
                    s = df[c].dropna().head(3).tolist()
                    samples[c] = [str(v)[:50] for v in s]
            # For large files, estimate total rows from file size
            if profile_nrows is not None:
                est_rows = int(file_size_mb * 500)  # rough estimate
                profiles[sheet] = {'rows': f"~{est_rows:,}+ (large file, sampled)", 'cols': len(df.columns), 'named_cols': cols, 'samples': samples}
            else:
                profiles[sheet] = {'rows': len(df), 'cols': len(df.columns), 'named_cols': cols, 'samples': samples}
        except Exception as e:
            profiles[sheet] = {'error': str(e)[:60]}

    result = {'filename': filename, 'rel_path': rel_path, 'sheets': profiles, 'total_sheets': len(sheets)}
    if skipped > 0:
        result['skipped_sheets'] = skipped
        result['all_sheet_names'] = sheets
    return result


def llm_classify_folder(file_profiles):
    summaries = []
    for fp in file_profiles:
        fname = fp['rel_path']
        if fp.get('error'):
            summaries.append(f"FILE: {fname} — ERROR: {fp['error']}")
            continue
        file_summary = f"FILE: {fname}"
        total = fp.get('total_sheets', len(fp['sheets']))
        skipped = fp.get('skipped_sheets', 0)
        if skipped > 0:
            file_summary += f" ({total} total sheets, showing {total - skipped} representative samples)"
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
        if skipped > 0:
            file_summary += f"\n  NOTE: {skipped} additional sheets follow same pattern (per-distributor breakdowns)"
        summaries.append(file_summary)

    folder_text = "\n\n".join(summaries)
    context_block = f"\nCLIENT CONTEXT:\n{CLIENT_CONTEXT}\n" if CLIENT_CONTEXT else ""

    system = f"""You are an expert data analyst for a pricing diagnostic platform scanning {CLIENT} client data.
{context_block}
{CANONICAL_SCHEMA}

CLASSIFICATION RULES:
- "EXTRACT" = contains actual structured data loadable into our schema
- "CONTEXT" = useful reference but not directly loadable
- "SKIP" = working documents, trackers, templates
- For EXTRACT: specify which canonical table(s) the data maps to
- Orders shipped data -> transaction_fact
- Claim/rebate data -> transaction_fact (as discount/rebate records)
- Incentive agreements -> contract
- Pricing data -> product_dim / list_price_history
- RSM (Regional Sales Manager) -> sales_rep_dim
- For large per-distributor files, classify the PATTERN with sheet="ALL_DISTRIBUTOR_SHEETS"
"""

    prompt = f"""Classify every file and sheet in this {CLIENT} client data folder.

{folder_text}

Return a JSON array where each item has:
- "file": filename
- "sheet": sheet name (or "ALL_DISTRIBUTOR_SHEETS" for per-distributor pattern)
- "action": "EXTRACT" | "CONTEXT" | "SKIP"
- "target_table": for EXTRACT only
- "reason": 1 sentence
- "mapping_hints": for EXTRACT only
"""

    response_text, usage = call_llm(prompt, system=system, max_tokens=16000)
    classifications = extract_json(response_text, prefer_array=True)
    if isinstance(classifications, list):
        classifications = [c for c in classifications if isinstance(c, dict)]
    return classifications, usage


def llm_map_sheet(filepath, sheet_name, classification, engine=None):
    # For large files, only read a header sample for mapping
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    sample_nrows = 30 if file_size_mb > 20 else None
    df_raw = pd.read_excel(filepath, sheet_name=sheet_name, header=None, engine=engine, nrows=sample_nrows or 100)
    if df_raw.empty:
        return None, None, None
    if df_raw.shape[1] > 500:
        df_raw = df_raw.iloc[:, :500]

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

    cols_detail = []
    for c in df.columns[:25]:
        if c.startswith('Unnamed'):
            continue
        dtype = str(df[c].dtype)
        samples = df[c].dropna().head(3).tolist()
        samples_str = [str(s)[:50] for s in samples]
        cols_detail.append(f"  - '{c}' ({dtype}, samples: {samples_str})")
    cols_text = "\n".join(cols_detail)

    preview = df.head(3).to_dict('records')
    preview_clean = [{k: str(v)[:50] for k, v in row.items() if pd.notna(v) and not k.startswith('Unnamed')} for row in preview]
    preview_text = json.dumps(preview_clean[:3], indent=2, default=str)

    target = classification.get('target_table', 'transaction_fact')
    hints = classification.get('mapping_hints', '')

    date_cols = {}
    for col in df.columns:
        try:
            dt = pd.to_datetime(str(col).strip())
            if 2018 <= dt.year <= 2030:
                date_cols[col] = dt
        except:
            pass
    has_date_cols = len(date_cols) >= 3

    prompt = f"""Map columns from this sheet to our canonical schema.
Target table: {target}
Hints: {hints}
Client: {CLIENT} (Industrial Safety/PPE manufacturer, B2B)

Sheet: "{sheet_name}" ({len(df)} rows x {len(df.columns)} cols)
{'This sheet has ' + str(len(date_cols)) + ' date-formatted columns — needs wide-to-long unpivoting.' if has_date_cols else ''}

Columns:
{cols_text}

Sample rows:
{preview_text}

Return JSON with: "column_mapping", "data_format" (wide_dates/wide_months/long), "exclude_rules", "id_columns", "notes"
"""

    system = f"""You are a data engineer mapping source columns to canonical fields.
{CANONICAL_SCHEMA}
Rules: Map to exact canonical field names. Use null for columns to skip."""

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


def transform_sheet(df, mapping, sheet_name, source_file):
    if mapping is None:
        return []
    col_map = mapping.get('column_mapping', {})
    data_format = mapping.get('data_format', 'long')
    exclude_rules = mapping.get('exclude_rules', [])
    id_columns = mapping.get('id_columns', [])

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
        mapped_statics = {src: col_map[src] for src in static_cols if col_map.get(src) and col_map[src] != 'null'}

        for _, row in df_clean.iterrows():
            base = {}
            for src, canon in mapped_statics.items():
                val = row.get(src)
                if isinstance(val, pd.Series): val = val.iloc[0]
                if pd.notna(val): base[canon] = val
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
        for _, row in df_clean.iterrows():
            rec = {}
            for src, canon in col_map.items():
                if canon and canon != 'null' and src in df_clean.columns:
                    val = row.get(src)
                    if isinstance(val, pd.Series): val = val.iloc[0]
                    if pd.notna(val): rec[canon] = val
            if rec and len(rec) >= 2:
                rec['_source_file'] = source_file
                rec['_source_sheet'] = sheet_name
                records.append(rec)
    return records


def run_folder_pipeline(folder_path, output_folder):
    print("=" * 70)
    print(f"FINTASTIQ FOLDER-LEVEL ETL — {CLIENT} CLIENT DATA")
    print("=" * 70)
    print(f"Source: {folder_path}")
    print(f"Output: {output_folder}")
    print(f"Model: {MODEL}")
    print(f"Run: {datetime.now().isoformat()}")

    total_in, total_out = 0, 0

    # STAGE 1: DISCOVER & PROFILE (PRIORITY FILES)
    print(f"\n{'='*70}")
    print("STAGE 1: Discover & Profile (Priority Files)")
    print(f"{'='*70}")

    priority_paths = []
    for item in PRIORITY_FILES:
        full = os.path.join(folder_path, item)
        if os.path.isfile(full):
            priority_paths.append(full)
        elif os.path.isdir(full):
            for ext in ['*.xlsx', '*.csv', '*.xlsb']:
                priority_paths.extend(glob.glob(os.path.join(full, '**', ext), recursive=True))
    priority_paths = sorted(set(priority_paths))
    print(f"Found {len(priority_paths)} priority spreadsheet files")

    file_profiles = []
    for f in priority_paths:
        rel = f.replace(folder_path + '/', '') if folder_path in f else os.path.basename(f)
        print(f"  Profiling: {rel}")
        try:
            p = profile_file(f)
            p['filepath'] = f
            file_profiles.append(p)
            total_s = p.get('total_sheets', len(p['sheets']))
            profiled_s = len([s for s in p['sheets'] if p['sheets'][s] is not None])
            skip_note = f" (sampled {profiled_s}/{total_s})" if p.get('skipped_sheets', 0) > 0 else ""
            print(f"    -> {profiled_s} sheets profiled{skip_note}")
        except Exception as e:
            print(f"    -> ERROR: {str(e)[:60]}")
            file_profiles.append({'filename': os.path.basename(f), 'rel_path': rel, 'filepath': f, 'sheets': {}, 'error': str(e)[:60]})

    # STAGE 2: LLM CLASSIFICATION
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

    extract_list = []
    for item in classifications:
        if not isinstance(item, dict):
            continue
        action = item.get('action', 'SKIP')
        if action == 'EXTRACT':
            print(f"  [EXTRACT] {item.get('file','')} / {item.get('sheet','')} -> {item.get('target_table','?')}")
            extract_list.append(item)
        elif action == 'CONTEXT':
            print(f"  [CONTEXT] {item.get('file','')} / {item.get('sheet','')}")

    # Expand ALL_DISTRIBUTOR_SHEETS — but cap at 5 sample sheets to avoid 100+ LLM calls
    # For per-distributor sheets, use a template mapping approach
    expanded_list = []
    for cls_item in extract_list:
        sname = cls_item.get('sheet', '')
        fname = cls_item.get('file', '')
        if sname == 'ALL_DISTRIBUTOR_SHEETS':
            for fp in file_profiles:
                if fp['filename'] == fname or fp['rel_path'] == fname or fname in fp.get('rel_path', ''):
                    all_sheets = fp.get('all_sheet_names', list(fp['sheets'].keys()))
                    # Skip aggregate/summary sheets
                    summary_words = ['summary', 'accrual', 'volume', 'growth', 'top 10', 'offers',
                                     'liability', 'itd', 'validation', 'style']
                    distributor_sheets = [s for s in all_sheets
                                          if not any(w in s.lower() for w in summary_words)]
                    # Cap at 8 distributor sheets as a representative sample
                    sampled_sheets = distributor_sheets[:8]
                    for s in sampled_sheets:
                        expanded = cls_item.copy()
                        expanded['sheet'] = s
                        expanded_list.append(expanded)
                    print(f"    Expanded {fname} -> {len(sampled_sheets)}/{len(distributor_sheets)} distributor sheets (sampled)")
                    break
        else:
            expanded_list.append(cls_item)

    print(f"\n  Total EXTRACT items: {len(expanded_list)}")

    # STAGE 3+4: EXTRACT & TRANSFORM
    print(f"\n{'='*70}")
    print("STAGE 3+4: Extract & Transform")
    print(f"{'='*70}")

    all_records = {}
    for cls_item in expanded_list:
        fname = cls_item.get('file', '')
        sname = cls_item.get('sheet', '')
        target = cls_item.get('target_table', 'transaction_fact')
        if isinstance(target, list):
            target = target[0] if target else 'transaction_fact'

        matched_fp = None
        for fp in file_profiles:
            if fp['filename'] == fname or fp['rel_path'] == fname:
                matched_fp = fp
                break
        if not matched_fp:
            for fp in file_profiles:
                if fname in fp['filename'] or fname in fp.get('rel_path', ''):
                    matched_fp = fp
                    break
        if not matched_fp:
            continue

        filepath = matched_fp['filepath']
        ext = os.path.splitext(filepath)[1].lower()
        engine = 'pyxlsb' if ext == '.xlsb' else None

        try:
            xl = pd.ExcelFile(filepath, engine=engine)
            avail = xl.sheet_names
        except:
            avail = list(matched_fp['sheets'].keys())

        if sname not in avail:
            found = False
            for a in avail:
                if sname.lower() == a.lower():
                    sname = a
                    found = True
                    break
            if not found:
                continue

        t0 = time.time()
        try:
            df, mapping, m_usage = llm_map_sheet(filepath, sname, cls_item, engine=engine)
        except httpx.TimeoutException:
            print(f"  {fname:50s} / {sname:30s} -> TIMEOUT")
            continue
        except Exception as e:
            print(f"  {fname}/{sname}: ERROR: {str(e)[:80]}")
            continue
        t1 = time.time()

        if m_usage:
            total_in += m_usage.input_tokens
            total_out += m_usage.output_tokens

        if mapping is None:
            print(f"  {fname}/{sname}: MAPPING FAILED ({t1-t0:.1f}s)")
            continue

        # For large files, read full data (capped at 100k rows) for transform
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        if file_size_mb > 20:
            try:
                best_row = 0
                df_raw_head = pd.read_excel(filepath, sheet_name=sname, header=None, engine=engine, nrows=20)
                for i in range(min(15, len(df_raw_head))):
                    score = sum(1 for v in df_raw_head.iloc[i] if isinstance(v, str) and len(str(v).strip()) > 2)
                    if score > best_row:
                        best_row = i
                df_full = pd.read_excel(filepath, sheet_name=sname, header=best_row, engine=engine, nrows=100000)
                df_full.columns = [str(c).strip() for c in df_full.columns]
                seen = {}
                new_cols = []
                for c in df_full.columns:
                    if c in seen:
                        seen[c] += 1
                        new_cols.append(f"{c}_{seen[c]}")
                    else:
                        seen[c] = 0
                        new_cols.append(c)
                df_full.columns = new_cols
                df = df_full
            except Exception as e:
                print(f"  {fname}/{sname}: Large file read error: {str(e)[:60]}, using sample")

        try:
            records = transform_sheet(df, mapping, sname, fname)
        except Exception as e:
            print(f"  {fname}/{sname}: TRANSFORM ERROR: {str(e)[:80]}")
            continue

        table_name = target.split(',')[0].split('+')[0].split('/')[0].strip().replace(' ', '_')
        if table_name not in all_records:
            all_records[table_name] = []
        all_records[table_name].extend(records)

        fmt = mapping.get('data_format', '?') if isinstance(mapping, dict) else '?'
        print(f"  {fname:50s} / {sname:30s} -> {len(records):5d} [{fmt}] ({t1-t0:.1f}s)")

    # BUILD CANONICAL DATAFRAMES
    print(f"\n{'='*70}")
    print("Building Canonical Tables")
    print(f"{'='*70}")

    canonical_tables = {}
    for table_name, records in all_records.items():
        if not records:
            continue
        df = pd.DataFrame(records)

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
        if 'contract_id' not in df.columns and table_name == 'contract':
            df.insert(0, 'contract_id', [f"{PREFIX}-CTR-{i+1:04d}" for i in range(len(df))])

        for col in df.columns:
            if 'date' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')
        for rev_col in ['revenue', 'deal_amount', 'contract_value', 'annual_value', 'amount', 'list_price', 'unit_price', 'cost', 'invoice_price', 'discount_amount']:
            if rev_col in df.columns:
                df[rev_col] = pd.to_numeric(df[rev_col], errors='coerce')

        internal_cols = [c for c in df.columns if c.startswith('_')]
        df_clean = df.drop(columns=internal_cols, errors='ignore')
        canonical_tables[table_name] = df_clean
        print(f"  {table_name}: {len(df_clean)} rows, {len(df_clean.columns)} columns")
        if 'revenue' in df_clean.columns:
            rev = pd.to_numeric(df_clean['revenue'], errors='coerce')
            print(f"    Revenue: ${rev.sum():,.2f}")
        if 'customer_name' in df_clean.columns:
            print(f"    Unique customers: {df_clean['customer_name'].nunique()}")

    # STAGE 5: VALIDATE
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

    val_prompt = f"""Review these extracted tables from {CLIENT} (Industrial Safety/PPE manufacturer) for pricing diagnostics.

{chr(10).join(val_parts)}

Return JSON with: quality_score (0-100), grade (A-D), issues, recommendations, analyses_coverage
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
        print(f"  Grade: {validation.get('grade','?')} ({validation.get('quality_score',0)}/100) ({t1-t0:.1f}s)")
        for issue in validation.get('issues', [])[:5]:
            print(f"    Issue: {issue}")

    # SAVE
    print(f"\n{'='*70}")
    print("Saving Bronze Tables")
    print(f"{'='*70}")

    os.makedirs(output_folder, exist_ok=True)
    for tname, df in canonical_tables.items():
        fpath = os.path.join(output_folder, f"{CLIENT}_{tname}.xlsx")
        with pd.ExcelWriter(fpath, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=tname[:31])
        print(f"  Saved: {CLIENT}_{tname}.xlsx ({len(df)} rows)")

    combined = os.path.join(output_folder, f"{CLIENT}_All_Canonical_Tables.xlsx")
    with pd.ExcelWriter(combined, engine='openpyxl') as writer:
        for tname, df in canonical_tables.items():
            df.to_excel(writer, index=False, sheet_name=tname[:31])
    print(f"  Saved: {CLIENT}_All_Canonical_Tables.xlsx ({len(canonical_tables)} sheets)")

    meta = {
        'client': CLIENT, 'run_timestamp': datetime.now().isoformat(),
        'files_scanned': len(file_profiles), 'sheets_extracted': len(expanded_list),
        'tables_created': {t: len(d) for t, d in canonical_tables.items()},
        'total_records': sum(len(d) for d in canonical_tables.values()),
        'api_usage': {'input_tokens': total_in, 'output_tokens': total_out,
                      'est_cost': (total_in * 3 + total_out * 15) / 1_000_000},
        'classifications': classifications, 'validation': validation,
    }
    with open(os.path.join(output_folder, f"{CLIENT}_pipeline_metadata.json"), 'w') as f:
        json.dump(meta, f, indent=2, default=str)

    total_records = sum(len(d) for d in canonical_tables.values())
    est_cost = (total_in * 3 + total_out * 15) / 1_000_000
    print(f"\n{'='*70}")
    print("PIPELINE SUMMARY")
    print(f"{'='*70}")
    print(f"  Client:           {CLIENT}")
    print(f"  Files scanned:    {len(file_profiles)}")
    print(f"  Sheets extracted: {len(expanded_list)}")
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
        print(f"  Quality: {validation.get('grade','?')} ({validation.get('quality_score',0)}/100)")

    return canonical_tables


if __name__ == '__main__':
    run_folder_pipeline(FOLDER, OUTPUT)
