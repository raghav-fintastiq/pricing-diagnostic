"""
FintastIQ Cleaning Module — Gravitate Bronze → Silver
Adapts the cleaning pipeline for Gravitate's table names.
Runs the same 4-stage cleaning process as NPI.
"""
import pandas as pd
import numpy as np
import json, os, time, re, hashlib
from datetime import datetime
from collections import Counter
import httpx, anthropic
import warnings
warnings.filterwarnings('ignore')

# ── CONFIG ──
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-20250514"
CLIENT = "Gravitate"
PREFIX = "GRV"

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
BRONZE_FOLDER = os.path.join(_PROJECT_ROOT, "4. Cleaned Output", "Gravitate_LLM_v2")
OUTPUT_FOLDER = os.path.join(_PROJECT_ROOT, "4. Cleaned Output", "Gravitate_Silver")
SOURCE_FOLDER = os.path.join(_PROJECT_ROOT, "2. Client Internal Data", "Gravitate Client Data")

def get_client():
    return anthropic.Anthropic(api_key=API_KEY, http_client=httpx.Client(verify=False, timeout=120.0))

def llm_call(prompt, max_tokens=4096):
    client = get_client()
    resp = client.messages.create(model=MODEL, max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}])
    return resp.content[0].text, resp.usage

def extract_json(text, prefer_array=False):
    first_char = '[' if prefer_array else '{'
    second_char = '{' if prefer_array else '['
    for ch in [first_char, second_char]:
        idx = text.find(ch)
        if idx == -1:
            continue
        depth = 0
        close = ']' if ch == '[' else '}'
        for i in range(idx, len(text)):
            if text[i] == ch: depth += 1
            elif text[i] == close: depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[idx:i+1])
                except:
                    break
    try:
        return json.loads(text.strip())
    except:
        return None


# ═══════════════════════════════════════════════════════════════════
# STAGE 1: DATE & TYPE FIXES
# ═══════════════════════════════════════════════════════════════════

def fix_dates_in_df(df, log, table_name):
    """Fix date columns — convert serial numbers, fix 1970 epochs, etc."""
    date_cols = [c for c in df.columns if 'date' in c.lower()]
    for col in date_cols:
        if col not in df.columns:
            continue

        # Convert to datetime
        df[col] = pd.to_datetime(df[col], errors='coerce')

        # Check for 1970 epoch dates (Excel serial number corruption)
        if df[col].notna().any():
            mask_1970 = df[col].dt.year == 1970
            count_1970 = mask_1970.sum()
            if count_1970 > 0:
                log.append({"table": table_name, "column": col, "action": "flag_1970_dates",
                           "detail": f"{count_1970} rows have 1970 dates (possible epoch corruption)"})
                print(f"    WARNING: {count_1970} rows with 1970 dates in {col}")

        # Check for Excel serial numbers stored as floats
        numeric_vals = pd.to_numeric(df[col], errors='coerce')
        serial_mask = numeric_vals.notna() & (numeric_vals > 25000) & (numeric_vals < 55000)
        if serial_mask.sum() > 0:
            converted = pd.to_datetime(numeric_vals[serial_mask], unit='D', origin='1899-12-30', errors='coerce')
            df.loc[serial_mask, col] = converted
            log.append({"table": table_name, "column": col, "action": "convert_serial_dates",
                       "detail": f"Converted {serial_mask.sum()} Excel serial numbers to dates"})

    return df


def fix_numeric_columns(df, log, table_name):
    """Ensure numeric columns are properly typed."""
    numeric_cols = ['revenue', 'list_price', 'invoice_price', 'pocket_price', 'discount_pct',
                   'discount_amount', 'quantity', 'unit_price', 'cost_of_goods', 'gross_margin',
                   'deal_amount', 'probability', 'expected_revenue', 'contract_value', 'annual_value',
                   'amount', 'budget_amount', 'variance', 'quota', 'commission_rate', 'margin_pct',
                   'annual_revenue', 'lifetime_value']

    for col in numeric_cols:
        if col in df.columns:
            before = df[col].notna().sum()
            df[col] = pd.to_numeric(df[col], errors='coerce')
            after = df[col].notna().sum()
            if before != after:
                log.append({"table": table_name, "column": col, "action": "coerce_numeric",
                           "detail": f"Coerced to numeric, {before - after} values became NaN"})

    return df


def stage1_type_fixes(tables, log):
    """Stage 1: Fix dates, types, and basic data quality issues."""
    print(f"\n{'='*70}")
    print("STAGE 1: Date & Type Fixes")
    print(f"{'='*70}")

    for tname, df in tables.items():
        print(f"\n  Processing {tname} ({len(df)} rows)...")

        # Fix dates
        df = fix_dates_in_df(df, log, tname)

        # Fix numeric columns
        df = fix_numeric_columns(df, log, tname)

        # Remove fully empty rows
        before = len(df)
        df = df.dropna(how='all')
        if len(df) < before:
            log.append({"table": tname, "action": "drop_empty_rows", "detail": f"Dropped {before - len(df)} empty rows"})
            print(f"    Dropped {before - len(df)} empty rows")

        # Remove duplicate rows
        before = len(df)
        df = df.drop_duplicates()
        if len(df) < before:
            log.append({"table": tname, "action": "drop_duplicates", "detail": f"Dropped {before - len(df)} exact duplicates"})
            print(f"    Dropped {before - len(df)} exact duplicates")

        # Strip string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['nan', 'None', 'NaT', ''], np.nan)

        tables[tname] = df
        print(f"    → {len(df)} rows after fixes")

    return tables, log


# ═══════════════════════════════════════════════════════════════════
# STAGE 2: ENTITY RESOLUTION (LLM-POWERED)
# ═══════════════════════════════════════════════════════════════════

def stage2_entity_resolution(tables, log):
    """Stage 2: Deduplicate entities using LLM."""
    print(f"\n{'='*70}")
    print("STAGE 2: Entity Resolution")
    print(f"{'='*70}")

    total_in, total_out = 0, 0

    # Customer name deduplication across all tables with customer_name
    customer_names = set()
    for tname, df in tables.items():
        if 'customer_name' in df.columns:
            names = df['customer_name'].dropna().unique()
            customer_names.update(str(n).strip() for n in names if str(n).strip() and str(n).strip().lower() not in ('nan', 'none', ''))

    if len(customer_names) > 5:
        print(f"\n  Found {len(customer_names)} unique customer names across all tables")

        # Batch them for LLM
        name_list = sorted(customer_names)
        batch_size = 200
        merge_map = {}

        for i in range(0, len(name_list), batch_size):
            batch = name_list[i:i+batch_size]
            prompt = f"""You are a data steward for a B2B SaaS company in the Energy/Oil & Gas sector.
Below are {len(batch)} customer company names from our database. Many are duplicates with variations (abbreviations, punctuation, case, typos, suffixes).

Group them into canonical names. Return a JSON object where each key is the CANONICAL name and the value is a list of ALL variations (including the canonical one).

Rules:
- Use the most complete/formal version as canonical
- Group obvious abbreviations (e.g., "Shell" = "Shell Oil" = "Shell Trading")
- Be conservative — only merge if you're confident they're the same entity
- Include single-entry groups too (no merge needed)

Names:
{json.dumps(batch, indent=1)}
"""
            try:
                text, usage = llm_call(prompt, max_tokens=8000)
                total_in += usage.input_tokens
                total_out += usage.output_tokens
                groups = extract_json(text)

                if groups and isinstance(groups, dict):
                    for canonical, variants in groups.items():
                        if isinstance(variants, list):
                            for v in variants:
                                if str(v).strip() != str(canonical).strip():
                                    merge_map[str(v).strip()] = str(canonical).strip()
                    print(f"    Batch {i//batch_size + 1}: {len(merge_map)} merges so far")
            except Exception as e:
                print(f"    Batch {i//batch_size + 1}: ERROR {str(e)[:60]}")

        # Apply merge map
        if merge_map:
            for tname, df in tables.items():
                if 'customer_name' in df.columns:
                    before_unique = df['customer_name'].nunique()
                    df['customer_name'] = df['customer_name'].map(lambda x: merge_map.get(str(x).strip(), str(x).strip()) if pd.notna(x) else x)
                    after_unique = df['customer_name'].nunique()
                    if before_unique != after_unique:
                        log.append({"table": tname, "column": "customer_name", "action": "entity_merge",
                                   "detail": f"Merged {before_unique} → {after_unique} unique names"})
                        print(f"    {tname}: {before_unique} → {after_unique} unique customers")
                    tables[tname] = df

    # Deal opportunity deduplication
    deal_key = f'{CLIENT}_deal_opportunity'
    if deal_key in tables:
        df_deals = tables[deal_key]
        if 'customer_name' in df_deals.columns and 'opportunity_name' in df_deals.columns:
            before = len(df_deals)
            df_deals = df_deals.drop_duplicates(subset=['customer_name', 'opportunity_name'], keep='last')
            if len(df_deals) < before:
                log.append({"table": deal_key, "action": "dedup_deals",
                           "detail": f"Deduped {before} → {len(df_deals)} deals"})
                print(f"    Deduped deals: {before} → {len(df_deals)}")
            tables[deal_key] = df_deals

    # Rebuild sales_rep_dim from all tables
    rep_key = f'{CLIENT}_sales_rep_dim'
    if rep_key in tables:
        df_reps = tables[rep_key]
        before = len(df_reps)
        if 'sales_rep_name' in df_reps.columns:
            df_reps = df_reps.drop_duplicates(subset=['sales_rep_name'], keep='last')
        if len(df_reps) < before:
            log.append({"table": rep_key, "action": "dedup_reps",
                       "detail": f"Deduped {before} → {len(df_reps)} reps"})
            print(f"    Deduped reps: {before} → {len(df_reps)}")
        tables[rep_key] = df_reps

    return tables, log, total_in, total_out


# ═══════════════════════════════════════════════════════════════════
# STAGE 3: IMPUTATION & ENRICHMENT (LLM-POWERED)
# ═══════════════════════════════════════════════════════════════════

def stage3_imputation(tables, log):
    """Stage 3: Use LLM to fill gaps and enrich data."""
    print(f"\n{'='*70}")
    print("STAGE 3: Imputation & Enrichment")
    print(f"{'='*70}")

    total_in, total_out = 0, 0

    # Customer dim enrichment
    cust_key = f'{CLIENT}_customer_dim'
    if cust_key in tables:
        df_cust = tables[cust_key]

        # Fill rates
        print(f"\n  {cust_key} fill rates:")
        for col in df_cust.columns:
            fill = df_cust[col].notna().sum() / len(df_cust) * 100 if len(df_cust) > 0 else 0
            if fill < 100:
                print(f"    {col}: {fill:.0f}%")

        # LLM enrichment for customer tiers/segments
        if 'customer_name' in df_cust.columns:
            names_sample = df_cust['customer_name'].dropna().unique()[:50]
            if len(names_sample) > 0:
                prompt = f"""You are analyzing customer data for {CLIENT}, a B2B Energy/Oil & Gas SaaS company.

Here are {len(names_sample)} customer names. For each, infer:
1. "industry" - their primary industry
2. "segment" - Retail Chain / Major Oil / Independent / Distributor / Agriculture / Other
3. "tier" - Large / Mid / Small (based on company size)
4. "company_type" - Corporation / LLC / Partnership / Government / Unknown

Return a JSON array of objects, each with "customer_name", "industry", "segment", "tier", "company_type".

Customer names:
{json.dumps(list(names_sample), indent=1)}
"""
                try:
                    text, usage = llm_call(prompt, max_tokens=8000)
                    total_in += usage.input_tokens
                    total_out += usage.output_tokens
                    enrichments = extract_json(text, prefer_array=True)

                    if enrichments and isinstance(enrichments, list):
                        enrich_map = {}
                        for item in enrichments:
                            if isinstance(item, dict) and 'customer_name' in item:
                                enrich_map[item['customer_name']] = item

                        filled_count = 0
                        for field in ['industry', 'segment', 'tier', 'company_type']:
                            if field not in df_cust.columns:
                                df_cust[field] = np.nan
                            for idx, row in df_cust.iterrows():
                                name = str(row.get('customer_name', ''))
                                if name in enrich_map and pd.isna(row.get(field)):
                                    val = enrich_map[name].get(field)
                                    if val:
                                        df_cust.at[idx, field] = val
                                        filled_count += 1

                        if filled_count > 0:
                            log.append({"table": cust_key, "action": "llm_enrich",
                                       "detail": f"Filled {filled_count} fields via LLM enrichment"})
                            print(f"    Enriched {filled_count} fields via LLM")
                except Exception as e:
                    print(f"    LLM enrichment error: {str(e)[:60]}")

        tables[cust_key] = df_cust

    # Transaction fact — flag negative revenue
    txn_key = f'{CLIENT}_transaction_fact'
    if txn_key in tables:
        tf = tables[txn_key]
        if 'revenue' in tf.columns:
            neg_mask = pd.to_numeric(tf['revenue'], errors='coerce') < 0
            neg_count = neg_mask.sum()
            if neg_count > 0:
                if 'revenue_type' not in tf.columns:
                    tf['revenue_type'] = 'standard'
                tf.loc[neg_mask, 'revenue_type'] = 'credit/adjustment'
                log.append({"table": txn_key, "column": "revenue", "action": "flag_negatives",
                           "detail": f"Flagged {neg_count} negative revenue rows as credit/adjustment"})
                print(f"    Flagged {neg_count} negative revenue entries")
            tables[txn_key] = tf

    # Monthly financials — categorize line items
    fin_key = f'{CLIENT}_monthly_financials'
    if fin_key in tables:
        mf = tables[fin_key]
        if 'line_item' in mf.columns and 'category' not in mf.columns:
            mf['category'] = 'uncategorized'
            # Simple rule-based categorization
            for idx, row in mf.iterrows():
                item = str(row.get('line_item', '')).lower()
                if any(w in item for w in ['revenue', 'arr', 'mrr', 'sales', 'income']):
                    mf.at[idx, 'category'] = 'revenue'
                elif any(w in item for w in ['cost', 'cogs', 'expense', 'salary', 'rent', 'depreciation']):
                    mf.at[idx, 'category'] = 'expense'
                elif any(w in item for w in ['margin', 'profit', 'ebitda', 'net']):
                    mf.at[idx, 'category'] = 'margin'
            log.append({"table": fin_key, "action": "categorize_line_items",
                       "detail": "Auto-categorized financial line items"})
        tables[fin_key] = mf

    return tables, log, total_in, total_out


# ═══════════════════════════════════════════════════════════════════
# STAGE 4: RECONCILIATION
# ═══════════════════════════════════════════════════════════════════

def stage4_reconciliation(bronze_tables, silver_tables, log):
    """Stage 4: Generate reconciliation report."""
    print(f"\n{'='*70}")
    print("STAGE 4: Reconciliation Report")
    print(f"{'='*70}")

    report = {
        'client': CLIENT,
        'timestamp': datetime.now().isoformat(),
        'tables': {}
    }

    for tname in silver_tables:
        b_df = bronze_tables.get(tname, pd.DataFrame())
        s_df = silver_tables[tname]

        table_report = {
            'bronze_rows': len(b_df),
            'silver_rows': len(s_df),
            'row_delta': len(s_df) - len(b_df),
            'bronze_cols': len(b_df.columns) if len(b_df) > 0 else 0,
            'silver_cols': len(s_df.columns),
        }

        # Check revenue reconciliation
        if 'revenue' in s_df.columns:
            s_rev = pd.to_numeric(s_df['revenue'], errors='coerce').sum()
            b_rev = pd.to_numeric(b_df['revenue'], errors='coerce').sum() if 'revenue' in b_df.columns else 0
            table_report['bronze_revenue'] = round(float(b_rev), 2)
            table_report['silver_revenue'] = round(float(s_rev), 2)
            table_report['revenue_delta'] = round(float(s_rev - b_rev), 2)

        # Check unique customers
        if 'customer_name' in s_df.columns:
            table_report['silver_unique_customers'] = int(s_df['customer_name'].nunique())
            if 'customer_name' in b_df.columns:
                table_report['bronze_unique_customers'] = int(b_df['customer_name'].nunique())

        report['tables'][tname] = table_report
        print(f"  {tname}: {table_report['bronze_rows']} → {table_report['silver_rows']} rows ({table_report['row_delta']:+d})")

    report['cleaning_log'] = log
    report['total_log_entries'] = len(log)

    return report


# ═══════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════

def run_cleaning_pipeline(bronze_folder, output_folder):
    print(f"{'='*70}")
    print(f"FINTASTIQ CLEANING MODULE — {CLIENT} Bronze → Silver")
    print(f"{'='*70}")
    print(f"Bronze folder: {bronze_folder}")
    print(f"Output folder: {output_folder}")
    print(f"Model: {MODEL}")
    print(f"Run: {datetime.now().isoformat()}")

    os.makedirs(output_folder, exist_ok=True)

    # Load Bronze tables
    print(f"\nLoading Bronze tables...")
    table_files = [f for f in os.listdir(bronze_folder)
                   if f.endswith('.xlsx') and not f.startswith(f'{CLIENT}_All') and f != f'{CLIENT}_pipeline_metadata.json']

    bronze_tables = {}
    for fname in sorted(table_files):
        tname = os.path.splitext(fname)[0]
        df = pd.read_excel(os.path.join(bronze_folder, fname))
        bronze_tables[tname] = df
        print(f"  Loaded {tname}: {len(df):,} rows x {len(df.columns)} cols")

    # Keep copies for reconciliation
    bronze_copies = {k: v.copy() for k, v in bronze_tables.items()}

    log = []

    # Stage 1: Date & Type Fixes
    t0 = time.time()
    bronze_tables, log = stage1_type_fixes(bronze_tables, log)
    t1 = time.time()
    print(f"\n  Stage 1 complete ({t1-t0:.1f}s)")

    # Stage 2: Entity Resolution
    t0 = time.time()
    silver, log, s2_in, s2_out = stage2_entity_resolution(bronze_tables, log)
    t1 = time.time()
    print(f"\n  Stage 2 complete ({t1-t0:.1f}s)")

    # Stage 3: Imputation & Enrichment
    t0 = time.time()
    silver, log, s3_in, s3_out = stage3_imputation(silver, log)
    t1 = time.time()
    print(f"\n  Stage 3 complete ({t1-t0:.1f}s)")

    # Stage 4: Reconciliation
    report = stage4_reconciliation(bronze_copies, silver, log)

    total_in = s2_in + s3_in
    total_out = s2_out + s3_out

    # ── SAVE SILVER TABLES ──
    print(f"\n{'='*70}")
    print("Saving Silver Tables")
    print(f"{'='*70}")

    for tname, df in silver.items():
        outpath = os.path.join(output_folder, f"{tname}_silver.xlsx")
        with pd.ExcelWriter(outpath, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=tname.replace(f'{CLIENT}_', '')[:31])
        print(f"  Saved: {tname}_silver.xlsx ({len(df):,} rows)")

    # Combined workbook
    combined_path = os.path.join(output_folder, f'{CLIENT}_Silver_All_Tables.xlsx')
    with pd.ExcelWriter(combined_path, engine='openpyxl') as writer:
        for tname, df in silver.items():
            sheet = tname.replace(f'{CLIENT}_', '')[:31]
            df.to_excel(writer, index=False, sheet_name=sheet)
    print(f"  Saved: {CLIENT}_Silver_All_Tables.xlsx (combined)")

    # Save reports
    report_path = os.path.join(output_folder, 'reconciliation_report.json')
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    print(f"  Saved: reconciliation_report.json")

    log_path = os.path.join(output_folder, 'cleaning_log.json')
    with open(log_path, 'w') as f:
        json.dump(log, f, indent=2, default=str)
    print(f"  Saved: cleaning_log.json")

    # Summary
    print(f"\n{'='*70}")
    print("CLEANING SUMMARY")
    print(f"{'='*70}")
    print(f"  Client: {CLIENT}")
    print(f"  Tables cleaned: {len(silver)}")
    total_rows = sum(len(df) for df in silver.values())
    print(f"  Total Silver rows: {total_rows:,}")
    for tname, df in silver.items():
        rev = ""
        if 'revenue' in df.columns:
            r = pd.to_numeric(df['revenue'], errors='coerce')
            rev = f" (${r.sum():,.2f})"
        cust = ""
        if 'customer_name' in df.columns:
            cust = f" ({df['customer_name'].nunique()} customers)"
        print(f"    {tname}: {len(df):,} rows{rev}{cust}")
    print(f"\n  API usage: {total_in:,} in / {total_out:,} out tokens")
    print(f"  Est. cost: ${(total_in * 3 + total_out * 15) / 1_000_000:.4f}")
    print(f"  Log entries: {len(log)}")

    return silver, report


if __name__ == '__main__':
    silver, report = run_cleaning_pipeline(BRONZE_FOLDER, OUTPUT_FOLDER)
