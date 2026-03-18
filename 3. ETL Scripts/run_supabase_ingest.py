"""
FintastIQ Supabase Ingestion Script
Takes silver CSVs/Excel files and loads them into Supabase canonical tables.

Usage:
    python run_supabase_ingest.py --folder "/path/to/Silver_Output" [options]

Options:
    --folder          Path to silver output folder (required)
    --client-id       Client ID slug (e.g. "gravitate", auto-derived if omitted)
    --client-name     Human-readable name (auto-derived if omitted)
    --industry        Client industry (optional)
    --supabase-url    Supabase project URL (or SUPABASE_URL env var)
    --service-key     Supabase service role key (or SUPABASE_SERVICE_KEY env var)
    --model           Claude model for column mapping (default: claude-sonnet-4-20250514)
    --api-key         Anthropic API key (or ANTHROPIC_API_KEY env var)
    --dry-run         Print what would be loaded without writing to Supabase
    --skip-classify   Skip LLM classification, use filename heuristics only
"""

import argparse, json, os, re, time, uuid, warnings
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import httpx
import anthropic

warnings.filterwarnings("ignore")

# ── Lazy import for supabase-py ────────────────────────────────────────────────
try:
    from supabase import create_client, Client as SupabaseClient
    SUPABASE_PY_AVAILABLE = True
except ImportError:
    SUPABASE_PY_AVAILABLE = False

DEFAULT_MODEL = "claude-sonnet-4-20250514"

# ═══════════════════════════════════════════════════════════════════════════════
# CANONICAL SCHEMA MAP
# key   = table name in canonical schema
# pk    = primary key columns (list; uuid pks handled specially)
# uuid_pk = True means the PK is server-generated; delete+reinsert strategy
# required = columns that must be present
# ═══════════════════════════════════════════════════════════════════════════════
CANONICAL_TABLES = {
    "dim_customer": {
        "pk": ["customer_id"], "uuid_pk": False,
        "required": ["customer_id", "client_id", "customer_name"],
        "keywords": ["customer", "account", "client dim", "buyers"],
    },
    "dim_product": {
        "pk": ["product_sku"], "uuid_pk": False,
        "required": ["product_sku", "client_id", "product_name", "product_type"],
        "defaults": {"product_type": "physical_sku"},
        "keywords": ["product", "sku", "item", "catalog"],
    },
    "dim_sales_rep": {
        "pk": ["sales_rep_id"], "uuid_pk": False,
        "required": ["sales_rep_id", "client_id", "sales_rep_name"],
        "keywords": ["sales rep", "rep", "salesperson", "agent"],
    },
    "dim_contract": {
        "pk": ["contract_id"], "uuid_pk": False,
        "required": ["contract_id", "client_id", "customer_id"],
        "keywords": ["contract", "agreement", "sla"],
    },
    "dim_promotion": {
        "pk": ["promotion_id"], "uuid_pk": False,
        "required": ["promotion_id", "client_id", "promotion_type"],
        "keywords": ["promotion", "promo", "campaign", "discount program"],
    },
    "dim_intermediary": {
        "pk": ["intermediary_id"], "uuid_pk": False,
        "required": ["intermediary_id", "client_id", "intermediary_name", "intermediary_type"],
        "defaults": {"intermediary_type": "distributor"},
        "keywords": ["distributor", "broker", "reseller", "intermediary", "channel partner"],
    },
    "dim_vendor_platform": {
        "pk": ["vendor_id"], "uuid_pk": False,
        "required": ["vendor_id", "client_id", "vendor_name", "vendor_type"],
        "defaults": {"vendor_type": "other"},
        "keywords": ["vendor", "supplier", "platform"],
    },
    "fact_revenue": {
        "pk": ["transaction_id"], "uuid_pk": False,
        "required": ["transaction_id", "client_id", "transaction_date", "customer_id", "product_sku", "grain_type"],
        "defaults": {"grain_type": "invoice_line"},
        "keywords": ["transaction", "revenue", "invoice", "sales fact", "order line"],
    },
    "fact_deal_opportunity": {
        "pk": ["deal_id"], "uuid_pk": False,
        "required": ["deal_id", "client_id", "customer_id", "deal_outcome"],
        "defaults": {"deal_outcome": "Won"},
        "keywords": ["deal", "opportunity", "crm", "pipeline", "won lost"],
    },
    "fact_recurring_revenue": {
        "pk": ["id"], "uuid_pk": True,
        "required": ["client_id", "customer_id", "arrangement_type"],
        "defaults": {"arrangement_type": "annual_agreement"},
        "keywords": ["subscription", "arr", "mrr", "recurring", "saas"],
    },
    "fact_cost_to_serve": {
        "pk": ["id"], "uuid_pk": True,
        "required": ["client_id", "customer_id", "period"],
        "keywords": ["cost to serve", "cts", "service cost", "logistics cost"],
    },
    "fact_list_price_history": {
        "pk": ["id"], "uuid_pk": True,
        "required": ["client_id", "product_sku", "effective_date", "list_price"],
        "defaults": {"price_tier": "Default"},
        "keywords": ["list price", "price history", "price change"],
    },
    "fact_competitive_intel": {
        "pk": ["id"], "uuid_pk": True,
        "required": ["client_id", "competitor_name"],
        "keywords": ["competitive", "competitor", "market price", "benchmarking"],
    },
    "fact_rebate_incentive": {
        "pk": ["rebate_id"], "uuid_pk": False,
        "required": ["rebate_id", "client_id", "customer_id", "period", "rebate_type"],
        "keywords": ["rebate", "incentive", "back-end discount"],
    },
}

# Column name aliases — maps common bronze column names → canonical column names
COLUMN_ALIASES = {
    "customer_id": ["cust_id", "account_id", "buyer_id", "client_customer_id", "id"],
    "customer_name": ["account_name", "company_name", "buyer_name", "customer"],
    "product_sku": ["sku", "product_code", "item_code", "item_id", "product_id"],
    "product_name": ["item_name", "product_description", "product_desc", "name"],
    "transaction_id": ["txn_id", "invoice_id", "order_id", "line_id", "id"],
    "transaction_date": ["invoice_date", "order_date", "txn_date", "date", "sale_date"],
    "revenue": ["amount", "net_revenue", "sales_amount", "invoice_amount"],
    "list_price": ["msrp", "standard_price", "base_price", "price"],
    "invoice_price": ["net_price", "final_price", "billed_price", "selling_price"],
    "discount_pct": ["discount", "discount_percent", "disc_pct", "rebate_pct"],
    "deal_id": ["opportunity_id", "opp_id", "crm_id"],
    "deal_outcome": ["outcome", "status", "win_loss", "result"],
    "sales_rep_id": ["rep_id", "owner_id", "agent_id"],
    "sales_rep_name": ["rep_name", "owner_name", "rep", "salesperson"],
    "contract_id": ["agreement_id", "contract_number"],
}


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def make_anthropic(api_key: str):
    return anthropic.Anthropic(
        api_key=api_key,
        http_client=httpx.Client(verify=False, timeout=120.0)
    )


def llm_call(client, model: str, prompt: str, max_tokens: int = 4096):
    resp = client.messages.create(
        model=model, max_tokens=max_tokens,
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
        depth, close = 0, "]" if ch == "[" else "}"
        for i in range(idx, len(text)):
            if text[i] == ch: depth += 1
            elif text[i] == close: depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[idx:i + 1])
                except Exception:
                    break
    try:
        return json.loads(text.strip())
    except Exception:
        return None


def slugify(name: str) -> str:
    """'Gravitate Energy Solutions' → 'gravitate-energy-solutions'"""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def derive_client_id(folder_path: str, client_name: str) -> str:
    # Try to find existing client context
    ctx_path = os.path.join(folder_path, "client_context.json")
    if os.path.exists(ctx_path):
        try:
            ctx = json.load(open(ctx_path))
            cid = ctx.get("client_id") or ctx.get("id")
            if cid:
                return str(cid).lower()
        except Exception:
            pass
    return slugify(client_name)


def sanitize_value(val):
    """Convert a Python/numpy value to something JSON/Supabase-safe."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.isoformat()
    if isinstance(val, float) and (val == float("inf") or val == float("-inf")):
        return None
    return val


def df_to_records(df: pd.DataFrame) -> list:
    """Convert a DataFrame to a list of clean dicts suitable for Supabase upsert."""
    records = []
    for row in df.to_dict(orient="records"):
        clean = {k: sanitize_value(v) for k, v in row.items() if sanitize_value(v) is not None}
        records.append(clean)
    return records


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: LOAD SILVER FILES
# ═══════════════════════════════════════════════════════════════════════════════

def load_silver_files(folder: str) -> dict:
    """Load all silver tables from folder. Returns {filename_stem: DataFrame}."""
    files = [
        f for f in os.listdir(folder)
        if (f.endswith(".xlsx") or f.endswith(".csv"))
        and "All_Tables" not in f
        and "reconciliation" not in f
        and "cleaning_log" not in f
        and "pipeline_metadata" not in f
    ]
    tables = {}
    for fname in sorted(files):
        stem = os.path.splitext(fname)[0]
        # Strip _silver suffix
        stem = re.sub(r"_silver$", "", stem, flags=re.IGNORECASE)
        fpath = os.path.join(folder, fname)
        try:
            df = pd.read_csv(fpath, low_memory=False) if fname.endswith(".csv") else pd.read_excel(fpath)
            if len(df) > 0:
                tables[stem] = df
                print(f"  Loaded {stem}: {len(df):,} rows × {len(df.columns)} cols")
        except Exception as e:
            print(f"  WARNING: could not load {fname}: {str(e)[:60]}")
    return tables


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: CLASSIFY TABLES → CANONICAL TARGETS
# ═══════════════════════════════════════════════════════════════════════════════

def heuristic_classify(stem: str) -> Optional[str]:
    """Rule-based classification from filename stem."""
    s = stem.lower()
    for canonical, meta in CANONICAL_TABLES.items():
        for kw in meta.get("keywords", []):
            if kw in s:
                return canonical
    # Exact partial matches
    if "customer" in s and ("dim" in s or "master" in s or "list" in s):
        return "dim_customer"
    if "product" in s or "sku" in s or "catalog" in s:
        return "dim_product"
    if "rep" in s or "salesperson" in s:
        return "dim_sales_rep"
    if "contract" in s:
        return "dim_contract"
    if "revenue" in s or "transaction" in s or "invoice" in s or "sales" in s:
        return "fact_revenue"
    if "deal" in s or "opportunity" in s or "pipeline" in s:
        return "fact_deal_opportunity"
    if "rebate" in s:
        return "fact_rebate_incentive"
    if "subscription" in s or "arr" in s or "mrr" in s:
        return "fact_recurring_revenue"
    return None


def llm_classify_tables(tables: dict, client_name: str, ac, model: str) -> dict:
    """Use LLM to classify each silver table to a canonical target table."""
    canonical_list = list(CANONICAL_TABLES.keys())
    table_summaries = {}
    for stem, df in tables.items():
        cols = list(df.columns[:20])  # First 20 cols for prompt
        sample = df.head(3).to_dict(orient="records")
        # Clean sample for display
        sample_clean = [{k: str(v)[:50] for k, v in row.items() if v is not None} for row in sample]
        table_summaries[stem] = {"columns": cols, "sample": sample_clean, "rows": len(df)}

    prompt = f"""You are a data engineer for {client_name}. You need to classify client data tables into canonical schema targets.

Available canonical tables:
{json.dumps(canonical_list, indent=2)}

Table descriptions:
- dim_customer: customer/account master data
- dim_product: product catalog with SKUs, prices, costs
- dim_sales_rep: sales rep profiles and territories
- dim_contract: pricing contracts and agreements
- dim_promotion: promotional pricing programs
- dim_intermediary: distributors, brokers, resellers
- dim_vendor_platform: vendors and platforms
- fact_revenue: transaction-level or period revenue records
- fact_deal_opportunity: CRM deal/opportunity win-loss data
- fact_recurring_revenue: subscription/recurring revenue arrangements
- fact_cost_to_serve: per-customer service/logistics costs
- fact_list_price_history: product price change history
- fact_competitive_intel: competitor pricing observations
- fact_rebate_incentive: rebate accruals, claims, and payments

Tables to classify:
{json.dumps(table_summaries, indent=2)}

Return a JSON object: {{table_stem: canonical_table_name or null if no match}}
Only use table names from the available canonical tables list. Use null if the table doesn't map to any.
"""
    try:
        text, usage = llm_call(ac, model, prompt, max_tokens=2000)
        result = extract_json(text)
        if isinstance(result, dict):
            print(f"  LLM classified {len(result)} tables ({usage.input_tokens} in / {usage.output_tokens} out tokens)")
            return result
    except Exception as e:
        print(f"  LLM classify error: {str(e)[:80]}, falling back to heuristics")
    return {}


def classify_all(tables: dict, client_name: str, ac, model: str, skip_llm: bool = False) -> dict:
    """Classify each table stem to a canonical target. Returns {stem: canonical_table}."""
    print(f"\n{'='*70}")
    print("STEP 2: Classify Tables → Canonical Targets")
    print(f"{'='*70}")

    # Start with heuristics
    mapping = {}
    for stem in tables:
        guess = heuristic_classify(stem)
        if guess:
            mapping[stem] = guess
            print(f"  {stem} → {guess} (heuristic)")
        else:
            mapping[stem] = None
            print(f"  {stem} → ??? (needs LLM)")

    # Fill gaps with LLM
    unclassified = {k: v for k, v in tables.items() if mapping.get(k) is None}
    if unclassified and not skip_llm and ac:
        print(f"\n  Sending {len(unclassified)} unclassified tables to LLM...")
        llm_results = llm_classify_tables(unclassified, client_name, ac, model)
        for stem, canonical in llm_results.items():
            if canonical and canonical in CANONICAL_TABLES:
                mapping[stem] = canonical
                print(f"  {stem} → {canonical} (LLM)")
            elif canonical is None:
                print(f"  {stem} → SKIP (no canonical match)")

    # Final report
    classified = {k: v for k, v in mapping.items() if v is not None}
    skipped = [k for k, v in mapping.items() if v is None]
    print(f"\n  Classified: {len(classified)} tables | Skipped: {len(skipped)} tables")
    if skipped:
        print(f"  Skipped: {', '.join(skipped)}")

    return mapping


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: COLUMN MAPPING (bronze column → canonical column)
# ═══════════════════════════════════════════════════════════════════════════════

def heuristic_column_map(df_cols: list, canonical: str) -> dict:
    """Quick alias-based column mapping. Returns {df_col: canonical_col}."""
    col_map = {}
    canonical_cols = list(CANONICAL_TABLES[canonical].get("required", []))
    # Add all known canonical columns from the schema (hardcoded below for speed)
    all_canonical_cols = {
        "dim_customer": ["customer_id","client_id","customer_name","customer_segment","customer_group","customer_rollup","industry","sub_industry","geography_primary","customer_since_date","annual_revenue_band","account_owner_id","payment_terms_contracted","price_tier","strategic_flag","credit_score","switching_cost_proxy","member_count"],
        "dim_product": ["product_sku","client_id","product_name","product_category","product_family","current_list_price","standard_cost","launch_date","product_status","unit_of_measure","product_type","value_metric","competitor_equivalent","cannibalization_group"],
        "dim_sales_rep": ["sales_rep_id","client_id","sales_rep_name","manager_id","territory","segment_coverage","tenure_months","comp_plan_type","quota","start_date"],
        "dim_contract": ["contract_id","client_id","customer_id","contract_start_date","contract_end_date","contracted_price","contracted_payment_terms","contracted_volume_commitment","price_escalation_clause","escalation_pct","auto_renewal_flag","sla_terms","product_scope","data_source_id"],
        "dim_promotion": ["promotion_id","client_id","product_sku","promotion_type","promotion_start_date","promotion_end_date","promotional_price","regular_price","promotional_cost","target_segment","promotion_channel"],
        "dim_intermediary": ["intermediary_id","client_id","intermediary_name","intermediary_type","parent_organization","territory","commission_structure","commission_rate","agreement_id","agreement_start_date","tier_assigned","active_flag","client_count"],
        "dim_vendor_platform": ["vendor_id","client_id","vendor_name","vendor_type","parent_organization","contract_terms_summary","cost_model","active_client_count","active_flag","geography","market_share_estimate"],
        "fact_revenue": ["transaction_id","client_id","transaction_date","customer_id","product_sku","quantity","list_price","invoice_price","revenue","grain_type","data_source_id","cogs","volume_discount","prompt_pay_discount","rebate_amount","freight_absorption","special_pricing_agreement","sales_channel","sales_rep_id","intermediary_id","vendor_id","geography","currency","promotional_flag","contract_id","co_op_marketing","credit_memo_amount","exchange_rate","promotion_id"],
        "fact_deal_opportunity": ["deal_id","client_id","customer_id","deal_outcome","deal_close_date","product_sku","list_price_quoted","final_price","discount_pct","deal_value","sales_rep_id","customer_segment","loss_reason","competitor_cited","sales_cycle_days","approval_level","initial_discount_requested","override_flag","number_of_approvals","data_source_id"],
        "fact_recurring_revenue": ["client_id","customer_id","arrangement_type","plan_tier","plan_price","billing_cycle","plan_start_date","usage_volume","tier_limit","churn_flag","churn_date","churn_reason","price_change_at_renewal","price_increase_pct","nps_score","usage_trend_3m","intermediary_id","data_source_id"],
        "fact_cost_to_serve": ["client_id","customer_id","period","service_cost","logistics_cost","returns_credits_cost","intermediary_cost","customization_cost","payment_terms_cost","sales_coverage_cost","order_frequency","avg_order_size","data_source_id"],
        "fact_list_price_history": ["client_id","product_sku","effective_date","list_price","price_tier","previous_list_price","change_pct","change_reason","geography","data_source_id"],
        "fact_competitive_intel": ["client_id","competitor_name","competitor_product","own_product_sku","competitor_price","price_type","observation_date","source","confidence_level","market_segment","feature_comparison_score","geography","data_source_id"],
        "fact_rebate_incentive": ["rebate_id","client_id","contract_id","customer_id","product_sku","period","rebate_type","accrued_amount","claimed_amount","paid_amount","status","qualifying_volume","tier_threshold","intermediary_id","data_source_id"],
    }

    target_cols = set(all_canonical_cols.get(canonical, []))

    for df_col in df_cols:
        norm = df_col.lower().strip().replace(" ", "_").replace("-", "_")
        # Exact match
        if norm in target_cols:
            col_map[df_col] = norm
            continue
        # Check COLUMN_ALIASES
        for canonical_col, aliases in COLUMN_ALIASES.items():
            if canonical_col in target_cols and norm in aliases:
                col_map[df_col] = canonical_col
                break

    return col_map


def llm_map_columns(df_cols: list, canonical: str, client_name: str, ac, model: str) -> dict:
    """Use LLM to map df columns to canonical columns. Returns {df_col: canonical_col}."""
    target_cols_map = {
        "dim_customer": ["customer_id","client_id","customer_name","customer_segment","industry","geography_primary","annual_revenue_band","price_tier"],
        "dim_product": ["product_sku","product_name","product_category","product_family","current_list_price","standard_cost","product_status","product_type"],
        "dim_sales_rep": ["sales_rep_id","sales_rep_name","territory","segment_coverage","tenure_months","quota"],
        "dim_contract": ["contract_id","customer_id","contract_start_date","contract_end_date","contracted_price","contracted_payment_terms"],
        "dim_promotion": ["promotion_id","product_sku","promotion_type","promotion_start_date","promotion_end_date","promotional_price","regular_price","promotional_cost"],
        "fact_revenue": ["transaction_id","transaction_date","customer_id","product_sku","quantity","list_price","invoice_price","revenue","cogs","volume_discount","discount_pct","sales_rep_id","geography","sales_channel"],
        "fact_deal_opportunity": ["deal_id","customer_id","deal_outcome","deal_close_date","product_sku","list_price_quoted","final_price","discount_pct","deal_value","sales_rep_id","loss_reason","sales_cycle_days"],
        "fact_rebate_incentive": ["rebate_id","customer_id","period","rebate_type","accrued_amount","claimed_amount","paid_amount","status"],
        "fact_cost_to_serve": ["customer_id","period","service_cost","logistics_cost","returns_credits_cost","customization_cost","payment_terms_cost","order_frequency","avg_order_size"],
        "fact_list_price_history": ["product_sku","effective_date","list_price","previous_list_price","change_pct","change_reason"],
        "fact_competitive_intel": ["competitor_name","own_product_sku","competitor_price","confidence_level","market_segment"],
        "fact_recurring_revenue": ["customer_id","arrangement_type","plan_tier","plan_price","billing_cycle","churn_flag","churn_date","nps_score"],
    }
    target_cols = target_cols_map.get(canonical, [])

    prompt = f"""You are mapping data columns for {client_name} to a canonical pricing analytics schema.

Source table columns: {json.dumps(df_cols)}
Target canonical table: {canonical}
Target columns: {json.dumps(target_cols)}

Map source columns to target columns where there is a clear semantic match.
Return a JSON object: {{"source_col": "target_col"}} for each match.
Only include mappings you are confident about. Omit columns with no clear match.
Do NOT map a source column to multiple targets or vice versa.
"""
    try:
        text, usage = llm_call(ac, model, prompt, max_tokens=1000)
        result = extract_json(text)
        if isinstance(result, dict):
            # Clean — remove reverse-bad mappings
            valid = {k: v for k, v in result.items() if k in df_cols and v in target_cols}
            return valid
    except Exception as e:
        print(f"    LLM column map error: {str(e)[:60]}")
    return {}


def map_columns(df: pd.DataFrame, canonical: str, client_name: str, ac, model: str) -> dict:
    """Return final {df_col → canonical_col} map (heuristic + LLM gap fill)."""
    col_map = heuristic_column_map(list(df.columns), canonical)
    mapped_df_cols = set(col_map.keys())
    unmapped = [c for c in df.columns if c not in mapped_df_cols]

    if unmapped and ac:
        llm_map = llm_map_columns(unmapped, canonical, client_name, ac, model)
        col_map.update(llm_map)

    return col_map


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: TRANSFORM → CANONICAL SHAPE
# ═══════════════════════════════════════════════════════════════════════════════

def transform_to_canonical(
    df: pd.DataFrame, canonical: str, col_map: dict,
    client_id: str, stem: str
) -> pd.DataFrame:
    """Apply column mapping, inject client_id, fill defaults, handle PKs."""
    meta = CANONICAL_TABLES[canonical]
    defaults = meta.get("defaults", {})

    # Rename columns
    df = df.rename(columns=col_map)
    # Keep only canonical columns
    all_canonical = [c for c in df.columns if c in _get_all_canonical_cols(canonical)]
    df = df[all_canonical].copy() if all_canonical else df.copy()

    # Inject client_id
    df["client_id"] = client_id

    # Apply defaults for missing required columns
    for col, default_val in defaults.items():
        if col not in df.columns:
            df[col] = default_val
        else:
            df[col] = df[col].fillna(default_val)

    # Ensure required columns exist (fill with None if missing)
    for col in meta.get("required", []):
        if col not in df.columns:
            df[col] = None

    # Handle auto-PK for uuid_pk tables
    if meta.get("uuid_pk"):
        df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Drop rows missing required non-nullable columns
    required = meta.get("required", [])
    if required:
        before = len(df)
        df = df.dropna(subset=[c for c in required if c in df.columns and c != "id"])
        if len(df) < before:
            print(f"    Dropped {before - len(df)} rows with null required fields")

    # Coerce date columns
    for col in df.columns:
        if "date" in col.lower() or col == "effective_date":
            df[col] = pd.to_datetime(df[col], errors="coerce")
            # Convert to ISO string for JSON
            df[col] = df[col].apply(lambda x: x.date().isoformat() if pd.notna(x) else None)

    # Coerce boolean columns
    bool_cols = ["promotional_flag", "override_flag", "churn_flag", "auto_renewal_flag",
                 "price_change_at_renewal", "strategic_flag", "active_flag"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].map(
                lambda x: True if str(x).lower() in ("true", "1", "yes") else
                          False if str(x).lower() in ("false", "0", "no") else None
            )

    return df


def _get_all_canonical_cols(canonical: str) -> set:
    """Return the set of all known canonical columns for a table."""
    all_cols = {
        "dim_customer": {"customer_id","client_id","customer_name","customer_segment","customer_group","customer_rollup","industry","sub_industry","geography_primary","customer_since_date","annual_revenue_band","account_owner_id","payment_terms_contracted","price_tier","strategic_flag","credit_score","switching_cost_proxy","member_count"},
        "dim_product": {"product_sku","client_id","product_name","product_category","product_family","current_list_price","standard_cost","launch_date","product_status","unit_of_measure","product_type","value_metric","competitor_equivalent","cannibalization_group"},
        "dim_sales_rep": {"sales_rep_id","client_id","sales_rep_name","manager_id","territory","segment_coverage","tenure_months","comp_plan_type","quota","start_date"},
        "dim_contract": {"contract_id","client_id","customer_id","contract_start_date","contract_end_date","contracted_price","contracted_payment_terms","contracted_volume_commitment","price_escalation_clause","escalation_pct","auto_renewal_flag","sla_terms","product_scope","data_source_id"},
        "dim_promotion": {"promotion_id","client_id","product_sku","promotion_type","promotion_start_date","promotion_end_date","promotional_price","regular_price","promotional_cost","target_segment","promotion_channel"},
        "dim_intermediary": {"intermediary_id","client_id","intermediary_name","intermediary_type","parent_organization","territory","commission_structure","commission_rate","agreement_id","agreement_start_date","tier_assigned","active_flag","client_count"},
        "dim_vendor_platform": {"vendor_id","client_id","vendor_name","vendor_type","parent_organization","contract_terms_summary","cost_model","active_client_count","active_flag","geography","market_share_estimate"},
        "fact_revenue": {"transaction_id","client_id","transaction_date","customer_id","product_sku","quantity","list_price","invoice_price","revenue","grain_type","data_source_id","cogs","volume_discount","prompt_pay_discount","rebate_amount","freight_absorption","special_pricing_agreement","sales_channel","sales_rep_id","intermediary_id","vendor_id","geography","currency","promotional_flag","contract_id","co_op_marketing","credit_memo_amount","exchange_rate","promotion_id"},
        "fact_deal_opportunity": {"deal_id","client_id","customer_id","deal_outcome","deal_close_date","product_sku","list_price_quoted","final_price","discount_pct","deal_value","sales_rep_id","customer_segment","loss_reason","competitor_cited","sales_cycle_days","approval_level","initial_discount_requested","override_flag","number_of_approvals","data_source_id"},
        "fact_recurring_revenue": {"id","client_id","customer_id","arrangement_type","plan_tier","plan_price","billing_cycle","plan_start_date","usage_volume","tier_limit","churn_flag","churn_date","churn_reason","price_change_at_renewal","price_increase_pct","nps_score","usage_trend_3m","intermediary_id","data_source_id"},
        "fact_cost_to_serve": {"id","client_id","customer_id","period","service_cost","logistics_cost","returns_credits_cost","intermediary_cost","customization_cost","payment_terms_cost","sales_coverage_cost","order_frequency","avg_order_size","data_source_id"},
        "fact_list_price_history": {"id","client_id","product_sku","effective_date","list_price","price_tier","previous_list_price","change_pct","change_reason","geography","data_source_id"},
        "fact_competitive_intel": {"id","client_id","competitor_name","competitor_product","own_product_sku","competitor_price","price_type","observation_date","source","confidence_level","market_segment","feature_comparison_score","geography","data_source_id"},
        "fact_rebate_incentive": {"rebate_id","client_id","contract_id","customer_id","product_sku","period","rebate_type","accrued_amount","claimed_amount","paid_amount","status","qualifying_volume","tier_threshold","intermediary_id","data_source_id"},
    }
    return all_cols.get(canonical, set())


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5: LOAD INTO SUPABASE
# ═══════════════════════════════════════════════════════════════════════════════

def supabase_upsert(sb: "SupabaseClient", canonical: str, records: list) -> int:
    """Upsert records into canonical schema. Returns rows inserted/updated."""
    if not records:
        return 0

    meta = CANONICAL_TABLES[canonical]
    BATCH_SIZE = 500

    total = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        if meta.get("uuid_pk"):
            # For UUID-PK tables: insert (no conflict check needed since UUIDs are fresh)
            result = sb.schema("canonical").table(canonical).insert(batch).execute()
        else:
            # For named-PK tables: upsert
            result = (
                sb.schema("canonical")
                .table(canonical)
                .upsert(batch, on_conflict=",".join(meta["pk"]))
                .execute()
            )
        if hasattr(result, "data") and result.data:
            total += len(result.data)
        else:
            total += len(batch)
    return total


def delete_existing_client_records(sb: "SupabaseClient", canonical: str, client_id: str):
    """For UUID-PK tables, delete existing client rows before re-inserting."""
    try:
        sb.schema("canonical").table(canonical).delete().eq("client_id", client_id).execute()
        print(f"    Deleted existing {canonical} rows for client {client_id}")
    except Exception as e:
        print(f"    Warning: could not delete {canonical} rows: {str(e)[:60]}")


def register_data_source(sb: "SupabaseClient", client_id: str, stem: str,
                          canonical: str, row_count: int, col_map: dict):
    """Register this load in canonical.meta_data_source."""
    ds_id = f"{client_id}_{stem}_{int(time.time())}"
    record = {
        "data_source_id": ds_id,
        "client_id": client_id,
        "source_file_name": stem,
        "grain_type": "period_aggregate" if "revenue" in canonical else None,
        "load_timestamp": datetime.now().isoformat(),
        "row_count": row_count,
        "mapping_config_json": col_map,
        "validation_status": "passed",
    }
    try:
        sb.schema("canonical").table("meta_data_source").upsert(record).execute()
    except Exception as e:
        print(f"    Warning: could not register data source: {str(e)[:60]}")


def upsert_client_row(sb: "SupabaseClient", client_id: str, client_name: str,
                       industry: str, total_records: int, tables_loaded: list):
    """Create or update the public.clients row."""
    record = {
        "client_id": client_id,
        "client_name": client_name,
        "industry": industry or "Unknown",
        "bronze_status": "complete",
        "silver_status": "complete",
        "gold_status": "pending",
        "total_records": total_records,
        "total_tables": len(tables_loaded),
        "last_run_at": datetime.now().isoformat(),
    }
    try:
        sb.table("clients").upsert(record, on_conflict="client_id").execute()
        print(f"  ✓ clients row upserted for {client_id}")
    except Exception as e:
        print(f"  Warning: could not upsert clients row: {str(e)[:60]}")
        print(f"    Manual SQL: INSERT INTO clients (client_id, client_name, ...) VALUES ...")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def run_ingest(args) -> dict:
    supabase_url = args.supabase_url or os.environ.get("SUPABASE_URL", "")
    service_key  = args.service_key  or os.environ.get("SUPABASE_SERVICE_KEY", "")
    api_key      = args.api_key      or os.environ.get("ANTHROPIC_API_KEY", "")
    model        = args.model
    folder       = args.folder
    dry_run      = args.dry_run

    # ── Client metadata ──────────────────────────────────────────────────────
    client_name  = args.client_name or Path(folder).name.replace("_Silver", "").replace("_silver", "").strip()
    client_id    = args.client_id   or derive_client_id(folder, client_name)
    industry     = args.industry or "Unknown"

    print(f"{'='*70}")
    print(f"FINTASTIQ SUPABASE INGEST")
    print(f"{'='*70}")
    print(f"  Client:     {client_name} ({client_id})")
    print(f"  Industry:   {industry}")
    print(f"  Folder:     {folder}")
    print(f"  Dry run:    {dry_run}")
    print(f"  Supabase:   {supabase_url[:40] + '...' if supabase_url else '⚠ NOT SET'}")

    # ── Validate connections ─────────────────────────────────────────────────
    if not dry_run:
        if not supabase_url or not service_key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_SERVICE_KEY are required. "
                "Set as env vars or pass --supabase-url / --service-key flags."
            )
        if not SUPABASE_PY_AVAILABLE:
            raise ImportError("supabase-py not installed. Run: pip install supabase")

        sb = create_client(supabase_url, service_key)
        print("  ✓ Supabase client connected")
    else:
        sb = None
        print("  (dry run — no writes)")

    ac = make_anthropic(api_key) if api_key else None

    # ── Step 1: Load silver files ────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("STEP 1: Load Silver Files")
    print(f"{'='*70}")
    tables = load_silver_files(folder)
    if not tables:
        raise FileNotFoundError(f"No silver table files found in: {folder}")

    # ── Step 2: Classify ─────────────────────────────────────────────────────
    classification = classify_all(tables, client_name, ac, model, args.skip_classify)

    # ── Steps 3-5: For each table — map, transform, load ────────────────────
    print(f"\n{'='*70}")
    print("STEPS 3-5: Map Columns → Transform → Load")
    print(f"{'='*70}")

    results = {}
    total_records = 0
    tables_loaded = []

    # Load dims first (fact tables have FK constraints to dims)
    dim_tables = [(s, c) for s, c in classification.items() if c and c.startswith("dim_")]
    fact_tables = [(s, c) for s, c in classification.items() if c and c.startswith("fact_")]
    meta_tables = [(s, c) for s, c in classification.items() if c and c.startswith("meta_")]
    ordered = dim_tables + fact_tables + meta_tables

    for stem, canonical in ordered:
        if canonical is None:
            continue

        df = tables[stem]
        print(f"\n  [{canonical}] ← {stem} ({len(df):,} rows)")

        # Step 3: Column mapping
        col_map = map_columns(df, canonical, client_name, ac, model)
        mapped_count = len(col_map)
        print(f"    Column map: {mapped_count} columns mapped")
        for src, tgt in list(col_map.items())[:6]:
            print(f"      {src} → {tgt}")
        if mapped_count > 6:
            print(f"      ... +{mapped_count - 6} more")

        # Step 4: Transform
        try:
            canonical_df = transform_to_canonical(df, canonical, col_map, client_id, stem)
        except Exception as e:
            print(f"    ERROR in transform: {str(e)[:100]}")
            results[stem] = {"status": "error", "error": str(e)}
            continue

        print(f"    Transformed: {len(canonical_df):,} rows × {len(canonical_df.columns)} cols")

        if dry_run:
            print(f"    DRY RUN: would upsert {len(canonical_df):,} rows into canonical.{canonical}")
            results[stem] = {"status": "dry_run", "rows": len(canonical_df), "canonical": canonical}
            continue

        # Step 5: Load into Supabase
        meta = CANONICAL_TABLES[canonical]
        try:
            # For UUID-PK tables: clear existing data first
            if meta.get("uuid_pk"):
                delete_existing_client_records(sb, canonical, client_id)

            records = df_to_records(canonical_df)
            if not records:
                print(f"    SKIP: 0 valid records after sanitization")
                continue

            n = supabase_upsert(sb, canonical, records)
            print(f"    ✓ Loaded {n:,} rows into canonical.{canonical}")

            register_data_source(sb, client_id, stem, canonical, n, col_map)

            results[stem] = {"status": "success", "rows": n, "canonical": canonical}
            total_records += n
            tables_loaded.append(canonical)

        except Exception as e:
            print(f"    ERROR loading {canonical}: {str(e)[:120]}")
            results[stem] = {"status": "error", "error": str(e), "canonical": canonical}

    # ── Update clients row ───────────────────────────────────────────────────
    if not dry_run and sb:
        print(f"\n{'='*70}")
        print("STEP 6: Update clients Row")
        print(f"{'='*70}")
        upsert_client_row(sb, client_id, client_name, industry, total_records, tables_loaded)

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("INGEST SUMMARY")
    print(f"{'='*70}")
    successes = [s for s, r in results.items() if r.get("status") in ("success", "dry_run")]
    errors    = [s for s, r in results.items() if r.get("status") == "error"]
    print(f"  Client:        {client_name} ({client_id})")
    print(f"  Tables loaded: {len(successes)}")
    print(f"  Total rows:    {total_records:,}")
    print(f"  Errors:        {len(errors)}")
    for s in successes:
        r = results[s]
        print(f"    ✓ {s} → canonical.{r['canonical']} ({r.get('rows', '?'):,} rows)")
    for s in errors:
        print(f"    ✗ {s}: {results[s].get('error', '')[:80]}")

    if dry_run:
        print(f"\n  ⚠ DRY RUN complete — no data was written to Supabase.")
        print(f"  To actually load, remove the --dry-run flag and provide:")
        print(f"    --supabase-url  https://<project-id>.supabase.co")
        print(f"    --service-key   <service-role-key>")

    return {
        "client_id": client_id,
        "total_records": total_records,
        "tables_loaded": tables_loaded,
        "results": results,
    }


def main():
    parser = argparse.ArgumentParser(description="FintastIQ Supabase Ingestion Script")
    parser.add_argument("--folder",       required=True, help="Silver output folder path")
    parser.add_argument("--client-id",    default=None,  help="Client ID slug (auto-derived if omitted)")
    parser.add_argument("--client-name",  default=None,  help="Human-readable client name")
    parser.add_argument("--industry",     default=None,  help="Client industry")
    parser.add_argument("--supabase-url", default=None,  help="Supabase project URL")
    parser.add_argument("--service-key",  default=None,  help="Supabase service role key")
    parser.add_argument("--model",        default=DEFAULT_MODEL, help="Claude model for column mapping")
    parser.add_argument("--api-key",      default=None,  help="Anthropic API key")
    parser.add_argument("--dry-run",      action="store_true", help="Print plan without writing to Supabase")
    parser.add_argument("--skip-classify", action="store_true", help="Use heuristics only, skip LLM classification")
    args = parser.parse_args()

    if not os.path.isdir(args.folder):
        print(f"ERROR: folder not found: {args.folder}")
        raise SystemExit(1)

    result = run_ingest(args)
    print(f"\nDone. {result['total_records']:,} records loaded for client '{result['client_id']}'.")


if __name__ == "__main__":
    main()
