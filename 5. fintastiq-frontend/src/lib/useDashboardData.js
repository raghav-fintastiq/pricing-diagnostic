import { useState, useEffect } from "react";
import { supabase } from "./supabaseClient";

/* ─── helpers ─────────────────────────────────────────────── */
function n(v) { return parseFloat(v) || 0; }

function fmtImpact(raw) {
  const num = n(raw);
  if (num >= 1_000_000) return `$${(num / 1_000_000).toFixed(1)}M`;
  if (num >= 1_000) return `$${(num / 1_000).toFixed(0)}K`;
  return `$${num}`;
}

function shortName(full) {
  // "James Kowalski" → "James K."
  const parts = (full || "").trim().split(/\s+/);
  if (parts.length < 2) return full;
  return `${parts[0]} ${parts[parts.length - 1][0]}.`;
}

function buildWaterfall(row) {
  if (!row) return [];
  const items = [
    { name: "List Price",     value:  n(row.list_price_index),   fill: "#c5d44b" },
    { name: "Vol. Discount",  value: -n(row.volume_discount_pct), fill: "#ef5350" },
    { name: "Prompt Pay",     value: -n(row.prompt_pay_pct),      fill: "#ef5350" },
    { name: "Rebates",        value: -n(row.rebate_pct),          fill: "#ef5350" },
    { name: "Freight",        value: -n(row.freight_pct),         fill: "#ef5350" },
    { name: "Co-op Mktg",    value: -n(row.coop_pct),            fill: "#ef5350" },
    { name: "Credit Memos",   value: -n(row.credit_memo_pct),     fill: "#ef5350" },
    { name: "Invoice Price",  value:  n(row.invoice_price_index), fill: "#3e8c7f" },
    { name: "COGS",           value: -n(row.cogs_index),          fill: "#3b5068" },
    { name: "Pocket Margin",  value:  n(row.pocket_margin_index), fill: "#00a86b" },
  ];
  let cumulative = 0;
  return items.map((d, i) => {
    if (i === 0 || d.name === "Invoice Price" || d.name === "Pocket Margin") {
      cumulative = d.value;
      return { ...d, base: 0, height: Math.abs(d.value) };
    }
    const base = cumulative + d.value;
    const height = Math.abs(d.value);
    cumulative = base;
    return { ...d, base, height };
  });
}

/* ─── main hook ───────────────────────────────────────────── */
export function useDashboardData(clientId) {
  const [data, setData]       = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState(null);

  useEffect(() => {
    if (!clientId) return;
    setLoading(true);
    setError(null);

    async function fetchAll() {
      try {
        const [
          summaryRes,
          waterfallRes,
          trendRes,
          custProfitRes,
          skuParetoRes,
          pvmRes,
          dealRes,
          churnRes,
          geoRes,
          oppsRes,
        ] = await Promise.all([
          supabase.from("vw_executive_summary").select("*").eq("client_id", clientId).single(),
          supabase.from("vw_waterfall").select("*").eq("client_id", clientId).single(),
          supabase.from("vw_revenue_trend").select("*").eq("client_id", clientId).order("month_date"),
          supabase.from("vw_customer_profitability").select("*").eq("client_id", clientId),
          supabase.from("vw_sku_pareto").select("*").eq("client_id", clientId),
          supabase.from("vw_price_volume_mix").select("*").eq("client_id", clientId),
          supabase.from("vw_deal_scorecard").select("*").eq("client_id", clientId),
          supabase.from("vw_churn_risk").select("*").eq("client_id", clientId),
          supabase.from("vw_geo_pricing").select("*").eq("client_id", clientId),
          supabase.from("vw_opportunities").select("*").eq("client_id", clientId).order("priority"),
        ]);

        const raw = summaryRes.data || {};
        const churn = (churnRes.data || []).map(r => ({
          name: r.name, revenue: n(r.revenue), riskScore: r.risk_score, trend: r.trend,
        }));
        const oppsRaw = oppsRes.data || [];
        const opportunities = oppsRaw.map(r => ({
          id: r.priority, priority: r.priority,
          type: r.type, analysis: r.analysis,
          impact: fmtImpact(r.impact_mid),
          impactRaw: n(r.impact_mid),
          confidence: r.confidence, desc: r.description,
        }));

        const revenueAtRisk = churn
          .filter(c => c.riskScore > 70)
          .reduce((s, c) => s + c.revenue, 0);
        const opportunityValue = oppsRaw.reduce((s, o) => s + n(o.impact_mid), 0);

        // Tier derived from pipeline status
        const tier =
          raw.gold_status === "complete"   ? "Gold"   :
          raw.silver_status === "complete" ? "Silver" : "Bronze";

        const lastUpdated = raw.last_run_at
          ? new Date(raw.last_run_at).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })
          : "—";

        setData({
          clientMeta: {
            name:         raw.client_name || clientId,
            industry:     raw.industry || "—",
            tier,
            dataQuality:  raw.data_quality_grade || "—",
            lastUpdated,
          },
          summary: {
            totalRevenue:       n(raw.total_revenue),
            avgMargin:          n(raw.avg_margin),
            totalCustomers:     raw.total_customers || 0,
            activeProducts:     raw.active_products || 0,
            avgDiscount:        n(raw.avg_discount),
            totalLeakage:       n(raw.total_leakage),
            revenueAtRisk,
            opportunityValue,
          },
          waterfall:    buildWaterfall(waterfallRes.data),
          revenueTrend: (trendRes.data || []).map(r => ({
            month: r.month, revenue: n(r.revenue), margin: n(r.margin),
          })),
          custProfit: (custProfitRes.data || []).map(r => ({
            name: r.name, revenue: n(r.revenue), margin: n(r.margin),
            size: n(r.size), segment: r.segment,
          })),
          skuPareto: (skuParetoRes.data || []).map(r => ({
            sku: r.sku, revenue: n(r.revenue), cumPct: n(r.cum_pct),
          })),
          pvm: (pvmRes.data || []).map(r => ({
            period: r.period,
            price:  n(r.price),
            volume: n(r.volume_effect),
            mix:    n(r.mix),
            total:  n(r.total),
          })),
          deals: (dealRes.data || []).map(r => ({
            rep:         shortName(r.rep),
            winRate:     n(r.win_rate),
            avgDiscount: n(r.avg_discount),
            deals:       r.deals,
            revenue:     n(r.revenue),
          })),
          churn,
          geo: (geoRes.data || []).map(r => ({
            region:   r.region,
            avgPrice: n(r.avg_price),
            national: n(r.national),
            variance: n(r.variance),
            revenue:  n(r.revenue),
          })),
          opportunities,
        });
      } catch (err) {
        setError(err.message || "Failed to load data");
      } finally {
        setLoading(false);
      }
    }

    fetchAll();
  }, [clientId]);

  return { data, loading, error };
}
