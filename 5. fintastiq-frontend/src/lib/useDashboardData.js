import { useState, useEffect } from 'react';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.REACT_APP_SUPABASE_URL;
const supabaseAnonKey = process.env.REACT_APP_SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseAnonKey);

export const useDashboardData = (clientId) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchDashboardData = async () => {
      try {
        setLoading(true);
        setError(null);

        const results = await Promise.all([
          // Existing views
          supabase
            .from('vw_revenue_by_product')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_customer_health')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_deal_summary')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_discount_analysis')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_segment_metrics')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_churn_risk')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_contract_value')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_pricing_tiers')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_segment_profitability')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_sales_metrics')
            .select('*')
            .eq('client_id', clientId),
          // New views for extended analysis
          supabase
            .from('vw_price_band')
            .select('*')
            .eq('client_id', clientId)
            .order('band_order'),
          supabase
            .from('vw_win_loss')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_deal_velocity')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_discount_governance')
            .select('*')
            .eq('client_id', clientId)
            .order('band_order'),
          supabase
            .from('vw_cohort_revenue')
            .select('*')
            .eq('client_id', clientId)
            .order('cohort_month', { ascending: false })
            .limit(288),
          supabase
            .from('vw_sales_rep_perf')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_deal_size_dist')
            .select('*')
            .eq('client_id', clientId)
            .order('band_order'),
          supabase
            .from('vw_competitive_intel')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_rebate_analysis')
            .select('*')
            .eq('client_id', clientId),
          supabase
            .from('vw_cost_to_serve')
            .select('*')
            .eq('client_id', clientId),
        ]);

        // Check for errors
        results.forEach((result, index) => {
          if (result.error) {
            throw new Error(`Error fetching data index ${index}: ${result.error.message}`);
          }
        });

        const [
          revByProduct,
          custHealth,
          dealSummary,
          discountAnalysis,
          segmentMetrics,
          churnRisk,
          contractValue,
          pricingTiers,
          segmentProfitability,
          salesMetrics,
          priceBandRaw,
          winLossRaw,
          dealVelocityRaw,
          discountGovRaw,
          cohortRevenueRaw,
          repPerfRaw,
          dealSizeDistRaw,
          competitiveIntelRaw,
          rebateAnalysisRaw,
          costToServeRaw,
        ] = results.map(r => r.data || []);

        // Transform and map new view data to clean JS objects
        const priceBand = (priceBandRaw || []).map(row => ({
          priceBand: row.price_band,
          segment: row.segment,
          dealCount: row.deal_count,
          revenue: row.revenue,
          avgPrice: row.avg_price,
          avgDiscountPct: row.avg_discount_pct,
        }));

        const winLoss = (winLossRaw || []).map(row => ({
          outcome: row.deal_outcome,
          lossReason: row.loss_reason,
          competitor: row.competitor,
          segment: row.customer_segment,
          dealCount: row.deal_count,
          totalValue: row.total_value,
          avgDiscount: row.avg_discount,
          avgCycleDays: row.avg_cycle_days,
        }));

        const dealVelocity = (dealVelocityRaw || []).map(row => ({
          segment: row.customer_segment,
          rep: row.rep,
          outcome: row.deal_outcome,
          cycleBucket: row.cycle_bucket,
          dealCount: row.deal_count,
          avgDays: row.avg_days,
          avgDealValue: row.avg_deal_value,
        }));

        const discountGov = (discountGovRaw || []).map(row => ({
          discountBand: row.discount_band,
          bandOrder: row.band_order,
          requiredApproval: row.required_approval,
          segment: row.customer_segment,
          dealCount: row.deal_count,
          overrideCount: row.override_count,
          totalValue: row.total_value,
          avgDealValue: row.avg_deal_value,
        }));

        const cohortRevenue = (cohortRevenueRaw || []).map(row => ({
          cohortMonth: row.cohort_month,
          revMonth: row.rev_month,
          activeCustomers: row.active_customers,
          revenue: row.revenue,
        }));

        const repPerf = (repPerfRaw || []).map(row => ({
          repId: row.sales_rep_id,
          rep: row.rep,
          territory: row.territory,
          segmentCoverage: row.segment_coverage,
          tenureMonths: row.tenure_months,
          quota: row.quota,
          totalDeals: row.total_deals,
          wonDeals: row.won_deals,
          lostDeals: row.lost_deals,
          winRate: row.win_rate,
          bookings: row.bookings,
          quotaAttainment: row.quota_attainment,
          avgDiscount: row.avg_discount,
          avgCycleDaysWon: row.avg_cycle_days_won,
        }));

        const dealSizeDist = (dealSizeDistRaw || []).map(row => ({
          sizeBand: row.size_band,
          bandOrder: row.band_order,
          segment: row.customer_segment,
          outcome: row.deal_outcome,
          dealCount: row.deal_count,
          totalValue: row.total_value,
          avgDiscount: row.avg_discount,
          avgCycleDays: row.avg_cycle_days,
        }));

        const competitiveIntel = (competitiveIntelRaw || []).map(row => ({
          competitor: row.competitor_name,
          ownProductSku: row.own_product_sku,
          ownProductName: row.own_product_name,
          productCategory: row.product_category,
          segment: row.market_segment,
          geography: row.geography,
          priceType: row.price_type,
          avgCompetitorPrice: row.avg_competitor_price,
          observations: row.observations,
          avgFeatureScore: row.avg_feature_score,
          confidenceLevel: row.confidence_level,
        }));

        const rebateAnalysis = (rebateAnalysisRaw || []).map(row => ({
          customerName: row.customer_name,
          customerId: row.customer_id,
          segment: row.customer_segment,
          rebateType: row.rebate_type,
          period: row.period,
          totalAccrued: row.total_accrued,
          totalClaimed: row.total_claimed,
          totalPaid: row.total_paid,
          payoutRate: row.payout_rate,
          leakage: row.leakage,
        }));

        const costToServe = (costToServeRaw || []).map(row => ({
          customerName: row.customer_name,
          customerId: row.customer_id,
          segment: row.customer_segment,
          region: row.region,
          period: row.period,
          serviceCost: row.service_cost,
          logisticsCost: row.logistics_cost,
          returnsCredits: row.returns_credits_cost,
          intermediaryCost: row.intermediary_cost,
          customizationCost: row.customization_cost,
          paymentTermsCost: row.payment_terms_cost,
          salesCoverageCost: row.sales_coverage_cost,
          totalCostToServe: row.total_cost_to_serve,
          orderFrequency: row.order_frequency,
          avgOrderSize: row.avg_order_size,
        }));

        setData({
          // Existing data
          revenueByProduct: revByProduct,
          customerHealth: custHealth,
          dealSummary: dealSummary,
          discountAnalysis: discountAnalysis,
          segmentMetrics: segmentMetrics,
          churnRisk: churnRisk,
          contractValue: contractValue,
          pricingTiers: pricingTiers,
          segmentProfitability: segmentProfitability,
          salesMetrics: salesMetrics,
          // New data
          priceBand,
          winLoss,
          dealVelocity,
          discountGov,
          cohortRevenue,
          repPerf,
          dealSizeDist,
          competitiveIntel,
          rebateAnalysis,
          costToServe,
        });
      } catch (err) {
        console.error('Dashboard data fetch error:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    if (clientId) {
      fetchDashboardData();
    }
  }, [clientId]);

  return { data, loading, error };
};
