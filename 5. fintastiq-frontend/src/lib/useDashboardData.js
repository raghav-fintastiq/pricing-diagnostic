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
          priceBandRes,
          winLossRes,
          dealVelocityRes,
          discountGovRes,
          cohortRes,
          repPerfRes,
          dealSizeRes,
          compIntelRes,
          rebateRes,
          ctsRes,
          newProductPricingRes,
          promotionRoiRes,
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
          supabase.from("vw_price_band").select("*").eq("client_id", clientId).order("band_order"),
          supabase.from("vw_win_loss").select("*").eq("client_id", clientId),
          supabase.from("vw_deal_velocity").select("*").eq("client_id", clientId),
          supabase.from("vw_discount_governance").select("*").eq("client_id", clientId).order("band_order"),
          supabase.from("vw_cohort_revenue").select("*").eq("client_id", clientId).order("cohort_month").order("rev_month").limit(300),
          supabase.from("vw_sales_rep_perf").select("*").eq("client_id", clientId),
          supabase.from("vw_deal_size_dist").select("*").eq("client_id", clientId).order("band_order"),
          supabase.from("vw_competitive_intel").select("*").eq("client_id", clientId),
          supabase.from("vw_rebate_analysis").select("*").eq("client_id", clientId),
          supabase.from("vw_cost_to_serve").select("*").eq("client_id", clientId),
          supabase.from("vw_new_product_pricing").select("*").eq("client_id", clientId).order("launch_date"),
          supabase.from("vw_promotion_roi").select("*").eq("client_id", clientId).order("promotion_start_date"),
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
          priceBand: (priceBandRes.data || []).map(r => ({
            band: r.price_band, bandOrder: +r.band_order, segment: r.segment,
            dealCount: +r.deal_count, revenue: n(r.revenue),
            avgPrice: n(r.avg_price), avgDiscountPct: n(r.avg_discount_pct),
          })),
          winLoss: (winLossRes.data || []).map(r => ({
            outcome: r.deal_outcome, lossReason: r.loss_reason,
            competitor: r.competitor, segment: r.customer_segment,
            dealCount: +r.deal_count, totalValue: n(r.total_value),
            avgDiscount: n(r.avg_discount), avgCycleDays: n(r.avg_cycle_days),
          })),
          dealVelocity: (dealVelocityRes.data || []).map(r => ({
            segment: r.customer_segment, rep: r.rep, outcome: r.deal_outcome,
            cycleBucket: r.cycle_bucket, dealCount: +r.deal_count,
            avgDays: n(r.avg_days), avgDealValue: n(r.avg_deal_value),
          })),
          discountGov: (discountGovRes.data || []).map(r => ({
            band: r.discount_band, bandOrder: +r.band_order,
            approval: r.required_approval, segment: r.customer_segment,
            dealCount: +r.deal_count, overrideCount: +r.override_count,
            totalValue: n(r.total_value), avgDealValue: n(r.avg_deal_value),
          })),
          cohortRevenue: (cohortRes.data || []).map(r => ({
            cohortMonth: r.cohort_month, revMonth: r.rev_month,
            activeCustomers: +r.active_customers, revenue: n(r.revenue),
          })),
          repPerf: (repPerfRes.data || []).map(r => ({
            rep: r.rep, territory: r.territory, segment: r.segment_coverage,
            tenure: +r.tenure_months, quota: n(r.quota),
            totalDeals: +r.total_deals, wonDeals: +r.won_deals,
            winRate: n(r.win_rate), bookings: n(r.bookings),
            quotaAttainment: n(r.quota_attainment), avgDiscount: n(r.avg_discount),
            avgCycleDays: n(r.avg_cycle_days_won),
          })),
          dealSizeDist: (dealSizeRes.data || []).map(r => ({
            band: r.size_band, bandOrder: +r.band_order, segment: r.customer_segment,
            outcome: r.deal_outcome, dealCount: +r.deal_count,
            totalValue: n(r.total_value), avgDiscount: n(r.avg_discount),
            avgCycleDays: n(r.avg_cycle_days),
          })),
          compIntel: (compIntelRes.data || []).map(r => ({
            competitor: r.competitor_name, sku: r.own_product_sku,
            productName: r.own_product_name, category: r.product_category,
            segment: r.market_segment, geography: r.geography,
            priceType: r.price_type, competitorPrice: n(r.avg_competitor_price),
            observations: +r.observations, featureScore: n(r.avg_feature_score),
            confidence: r.confidence_level,
          })),
          rebateAnalysis: (rebateRes.data || []).map(r => ({
            customer: r.customer_name, customerId: r.customer_id,
            segment: r.segment, rebateType: r.rebate_type, period: r.period,
            accrued: n(r.total_accrued), claimed: n(r.total_claimed),
            paid: n(r.total_paid), payoutRate: n(r.payout_rate),
            leakage: n(r.leakage),
          })),
          costToServe: (ctsRes.data || []).map(r => ({
            customer: r.customer_name, customerId: r.customer_id,
            segment: r.customer_segment, region: r.region, period: r.period,
            serviceCost: n(r.service_cost), logisticsCost: n(r.logistics_cost),
            returnsCost: n(r.returns_credits_cost), intermediaryCost: n(r.intermediary_cost),
            customizationCost: n(r.customization_cost), paymentTermsCost: n(r.payment_terms_cost),
            salesCoverageCost: n(r.sales_coverage_cost),
            totalCts: n(r.total_cost_to_serve),
            orderFrequency: +r.order_frequency, avgOrderSize: n(r.avg_order_size),
          })),
          newProductPricing: (newProductPricingRes.data || []).map(r => ({
            productSku: r.product_sku, productName: r.product_name,
            productCategory: r.product_category, productFamily: r.product_family,
            productStatus: r.product_status, launchDate: r.launch_date,
            currentListPrice: n(r.current_list_price), standardCost: n(r.standard_cost),
            grossMarginPct: n(r.gross_margin_pct), valueMetric: r.value_metric,
            competitorEquivalent: r.competitor_equivalent,
            lastPriceChangeDate: r.last_price_change_date,
            previousListPrice: n(r.previous_list_price), lastChangePct: n(r.last_change_pct),
            changeReason: r.change_reason,
            avgCompetitorPrice: r.avg_competitor_price ? n(r.avg_competitor_price) : null,
            avgFeatureScore: r.avg_feature_score ? n(r.avg_feature_score) : null,
            competitorCount: r.competitor_count ? +r.competitor_count : 0,
            competitors: r.competitors,
            priceVsMarketPct: r.price_vs_market_pct != null ? n(r.price_vs_market_pct) : null,
            suggestedPrice: n(r.suggested_price),
            marketPosition: r.market_position,
          })),
          promotionRoi: (promotionRoiRes.data || []).map(r => ({
            promotionId: r.promotion_id, productSku: r.product_sku,
            promotionType: r.promotion_type,
            startDate: r.promotion_start_date, endDate: r.promotion_end_date,
            durationDays: +r.promo_duration_days,
            promotionalPrice: n(r.promotional_price), regularPrice: n(r.regular_price),
            discountDepthPct: n(r.discount_depth_pct),
            promotionalCost: n(r.promotional_cost),
            targetSegment: r.target_segment, promotionChannel: r.promotion_channel,
            promoTxnCount: +r.promo_txn_count,
            promoRevenue: n(r.promo_revenue), baselineRevenue: n(r.baseline_revenue),
            revenueLift: n(r.revenue_lift), roiPct: r.roi_pct != null ? n(r.roi_pct) : null,
            performanceBand: r.performance_band,
          })),
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
