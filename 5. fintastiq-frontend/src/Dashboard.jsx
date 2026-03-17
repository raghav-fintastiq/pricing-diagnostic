import React, { useState } from 'react';
import {
  LineChart, Line, BarChart, Bar, PieChart, Pie, Cell,
  ComposedChart, ScatterChart, Scatter, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ProgressBar
} from 'recharts';
import { useDashboardData } from './lib/useDashboardData';

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#FF6B9D', '#C44569', '#A7226E'];

// Utility functions
const formatCurrency = (value) => {
  if (!value) return '$0';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
};

const formatPercent = (value) => {
  if (value === null || value === undefined) return '0%';
  return `${Number(value).toFixed(1)}%`;
};

// Existing components (10)
const RevenueByProductCard = ({ data }) => {
  if (!data?.revenueByProduct?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Revenue by Product</h3>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={data.revenueByProduct}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="product_sku" />
          <YAxis />
          <Tooltip formatter={(value) => formatCurrency(value)} />
          <Bar dataKey="revenue" fill="#0088FE" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

const CustomerHealthCard = ({ data }) => {
  if (!data?.customerHealth?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Customer Health</h3>
      <div className="space-y-4">
        {data.customerHealth.slice(0, 5).map((item, idx) => (
          <div key={idx} className="flex justify-between items-center">
            <span className="text-sm">{item.customer_name || 'N/A'}</span>
            <span className="text-sm font-semibold">{formatPercent(item.health_score)}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

const DealSummaryCard = ({ data }) => {
  if (!data?.dealSummary?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  const summary = data.dealSummary[0];
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Deal Summary</h3>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <p className="text-gray-600 text-sm">Total Deals</p>
          <p className="text-2xl font-bold">{summary.deal_count || 0}</p>
        </div>
        <div>
          <p className="text-gray-600 text-sm">Total Value</p>
          <p className="text-2xl font-bold">{formatCurrency(summary.total_value)}</p>
        </div>
      </div>
    </div>
  );
};

const DiscountAnalysisCard = ({ data }) => {
  if (!data?.discountAnalysis?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Average Discount</h3>
      <ResponsiveContainer width="100%" height={250}>
        <BarChart data={data.discountAnalysis}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="segment" />
          <YAxis />
          <Tooltip formatter={(value) => formatPercent(value)} />
          <Bar dataKey="avg_discount_pct" fill="#FFBB28" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

const SegmentMetricsCard = ({ data }) => {
  if (!data?.segmentMetrics?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Segment Metrics</h3>
      <div className="space-y-3">
        {data.segmentMetrics.map((item, idx) => (
          <div key={idx} className="flex justify-between text-sm">
            <span>{item.segment}</span>
            <span className="font-semibold">{formatCurrency(item.revenue)}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

const ChurnRiskCard = ({ data }) => {
  if (!data?.churnRisk?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Churn Risk</h3>
      <ResponsiveContainer width="100%" height={250}>
        <BarChart data={data.churnRisk.slice(0, 10)}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="customer_name" width={80} angle={-45} />
          <YAxis />
          <Tooltip />
          <Bar dataKey="churn_probability" fill="#FF8042" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

const ContractValueCard = ({ data }) => {
  if (!data?.contractValue?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  const total = data.contractValue.reduce((sum, item) => sum + (item.total_contract_value || 0), 0);
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Contract Value</h3>
      <p className="text-gray-600 text-sm mb-2">Total ACV</p>
      <p className="text-3xl font-bold">{formatCurrency(total)}</p>
      <p className="text-xs text-gray-500 mt-2">{data.contractValue.length} contracts</p>
    </div>
  );
};

const PricingTiersCard = ({ data }) => {
  if (!data?.pricingTiers?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Pricing Tiers</h3>
      <ResponsiveContainer width="100%" height={250}>
        <PieChart>
          <Pie
            data={data.pricingTiers}
            dataKey="customer_count"
            nameKey="price_tier"
            cx="50%"
            cy="50%"
            outerRadius={80}
            label
          >
            {data.pricingTiers.map((_, idx) => (
              <Cell key={idx} fill={COLORS[idx % COLORS.length]} />
            ))}
          </Pie>
          <Tooltip />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
};

const SegmentProfitabilityCard = ({ data }) => {
  if (!data?.segmentProfitability?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Segment Profitability</h3>
      <ResponsiveContainer width="100%" height={300}>
        <ComposedChart data={data.segmentProfitability}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="segment" />
          <YAxis />
          <Tooltip formatter={(value) => formatCurrency(value)} />
          <Bar dataKey="gross_profit" fill="#00C49F" />
          <Line type="monotone" dataKey="profit_margin" stroke="#FF6B9D" />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
};

const SalesMetricsCard = ({ data }) => {
  if (!data?.salesMetrics?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Sales Metrics</h3>
      <div className="space-y-3">
        {data.salesMetrics.slice(0, 5).map((item, idx) => (
          <div key={idx} className="text-sm">
            <div className="flex justify-between mb-1">
              <span>{item.sales_rep_name || 'N/A'}</span>
              <span className="font-semibold">{formatPercent(item.win_rate)}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

// New extended analysis components (10)

const PriceBandCard = ({ data }) => {
  if (!data?.priceBand?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Price Band Distribution</h3>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={data.priceBand}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="priceBand" />
          <YAxis />
          <Tooltip formatter={(value) => value} />
          <Legend />
          <Bar dataKey="dealCount" fill="#0088FE" name="Enterprise" stackId="a" />
          <Bar dataKey="dealCount" fill="#00C49F" name="Mid-Market" stackId="a" />
          <Bar dataKey="dealCount" fill="#FFBB28" name="SMB" stackId="a" />
        </BarChart>
      </ResponsiveContainer>
      <div className="mt-4 overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-100">
            <tr>
              <th className="px-2 py-1 text-left">Band</th>
              <th className="px-2 py-1 text-right">Avg Price</th>
              <th className="px-2 py-1 text-right">Avg Discount</th>
            </tr>
          </thead>
          <tbody>
            {data.priceBand.map((row, idx) => (
              <tr key={idx} className="border-t">
                <td className="px-2 py-1">{row.priceBand}</td>
                <td className="px-2 py-1 text-right">{formatCurrency(row.avgPrice)}</td>
                <td className="px-2 py-1 text-right">{formatPercent(row.avgDiscountPct)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

const WinLossCard = ({ data }) => {
  if (!data?.winLoss?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  const outcomeData = {};
  data.winLoss.forEach(row => {
    outcomeData[row.outcome] = (outcomeData[row.outcome] || 0) + row.dealCount;
  });
  const pieData = Object.entries(outcomeData).map(([k, v]) => ({ name: k, value: v }));
  
  const lostDeals = data.winLoss.filter(r => r.outcome === 'Lost');
  const lossReasonData = {};
  lostDeals.forEach(row => {
    lossReasonData[row.lossReason] = (lossReasonData[row.lossReason] || 0) + row.dealCount;
  });
  const lossReasonChart = Object.entries(lossReasonData).map(([k, v]) => ({ reason: k, count: v }));
  
  const competitors = {};
  data.winLoss.forEach(row => {
    competitors[row.competitor] = (competitors[row.competitor] || 0) + row.dealCount;
  });
  
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Win/Loss Analysis</h3>
      <div className="grid grid-cols-2 gap-4 mb-4">
        <div>
          <p className="text-sm font-semibold mb-2">Outcome Distribution</p>
          <ResponsiveContainer width="100%" height={200}>
            <PieChart>
              <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={60}>
                {pieData.map((_, idx) => (
                  <Cell key={idx} fill={COLORS[idx % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>
        <div>
          <p className="text-sm font-semibold mb-2">Loss Reasons</p>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={lossReasonChart}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="reason" width={60} angle={-45} />
              <YAxis />
              <Tooltip />
              <Bar dataKey="count" fill="#FF8042" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
      <div className="mt-4">
        <p className="text-sm font-semibold mb-2">Top Competitors</p>
        <div className="space-y-1">
          {Object.entries(competitors).slice(0, 5).map(([comp, count], idx) => (
            <div key={idx} className="text-sm flex justify-between">
              <span>{comp}</span>
              <span className="font-semibold">{count} deals</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

const DealVelocityCard = ({ data }) => {
  if (!data?.dealVelocity?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Deal Velocity Distribution</h3>
      <ResponsiveContainer width="100%" height={300}>
        <ComposedChart data={data.dealVelocity}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="cycleBucket" />
          <YAxis yAxisId="left" />
          <YAxis yAxisId="right" orientation="right" />
          <Tooltip />
          <Legend />
          <Bar yAxisId="left" dataKey="dealCount" fill="#0088FE" name="Deal Count" />
          <Line yAxisId="right" type="monotone" dataKey="avgDays" stroke="#FF6B9D" name="Avg Days" />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
};

const DiscountGovCard = ({ data }) => {
  if (!data?.discountGov?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  const totalOverrides = data.discountGov.reduce((sum, row) => sum + (row.overrideCount || 0), 0);
  const directorPlus = data.discountGov.reduce((sum, row) => 
    row.requiredApproval === 'Director' || row.requiredApproval === 'Executive' 
      ? sum + row.dealCount 
      : sum, 0
  );
  const totalDeals = data.discountGov.reduce((sum, row) => sum + row.dealCount, 0);
  
  const approvalColors = {
    'Self-Service': '#00C49F',
    'Manager': '#FFBB28',
    'Director': '#FF8042',
    'Executive': '#C44569'
  };
  
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Discount Governance</h3>
      <div className="grid grid-cols-2 gap-4 mb-4">
        <div className="bg-gray-50 p-3 rounded">
          <p className="text-xs text-gray-600">Overrides</p>
          <p className="text-2xl font-bold">{totalOverrides}</p>
        </div>
        <div className="bg-gray-50 p-3 rounded">
          <p className="text-xs text-gray-600">Director+ Approvals</p>
          <p className="text-2xl font-bold">{formatPercent((directorPlus / totalDeals) * 100)}</p>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={250}>
        <BarChart data={data.discountGov}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="discountBand" />
          <YAxis />
          <Tooltip />
          <Bar dataKey="dealCount" shape={() => null}>
            {data.discountGov.map((entry, idx) => (
              <Cell key={idx} fill={approvalColors[entry.requiredApproval] || '#0088FE'} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

const CohortRevenueCard = ({ data }) => {
  if (!data?.cohortRevenue?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  const cohorts = {};
  data.cohortRevenue.forEach(row => {
    if (!cohorts[row.cohortMonth]) cohorts[row.cohortMonth] = [];
    cohorts[row.cohortMonth].push(row);
  });
  
  const latestCohorts = Object.keys(cohorts).sort().reverse().slice(0, 8);
  
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Cohort Revenue (Retention)</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="bg-gray-100">
            <tr>
              <th className="px-2 py-1 text-left">Cohort</th>
              <th className="px-2 py-1 text-right">Month 1</th>
              <th className="px-2 py-1 text-right">Month 2</th>
              <th className="px-2 py-1 text-right">Month 3</th>
              <th className="px-2 py-1 text-right">Month 4</th>
              <th className="px-2 py-1 text-right">Month 6</th>
            </tr>
          </thead>
          <tbody>
            {latestCohorts.map((cohort, idx) => {
              const months = cohorts[cohort];
              return (
                <tr key={idx} className="border-t">
                  <td className="px-2 py-1">{cohort}</td>
                  {[1, 2, 3, 4, 6].map(m => {
                    const month = months.find(row => {
                      const revDate = new Date(row.revMonth);
                      const cohortDate = new Date(cohort + '-01');
                      const monthDiff = (revDate.getFullYear() - cohortDate.getFullYear()) * 12 + 
                                      (revDate.getMonth() - cohortDate.getMonth()) + 1;
                      return monthDiff === m;
                    });
                    const rev = month?.revenue || 0;
                    return (
                      <td key={m} className="px-2 py-1 text-right bg-opacity-50" 
                        style={{ backgroundColor: rev > 100000 ? '#dcfce7' : rev > 50000 ? '#dbeafe' : '#f3f4f6' }}>
                        {formatCurrency(rev)}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

const RepPerfCard = ({ data }) => {
  if (!data?.repPerf?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Sales Rep Performance</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="bg-gray-100">
            <tr>
              <th className="px-2 py-1 text-left">Rep</th>
              <th className="px-2 py-1 text-center">Win Rate</th>
              <th className="px-2 py-1 text-right">Bookings</th>
              <th className="px-2 py-1 text-center">Quota %</th>
              <th className="px-2 py-1 text-right">Avg Discount</th>
            </tr>
          </thead>
          <tbody>
            {data.repPerf.map((row, idx) => {
              const quotaColor = row.quotaAttainment >= 100 ? 'bg-green-200' : 
                                row.quotaAttainment >= 80 ? 'bg-yellow-200' : 'bg-red-200';
              return (
                <tr key={idx} className="border-t">
                  <td className="px-2 py-1">{row.rep}</td>
                  <td className="px-2 py-1 text-center">{formatPercent(row.winRate)}</td>
                  <td className="px-2 py-1 text-right">{formatCurrency(row.bookings)}</td>
                  <td className={`px-2 py-1 text-center ${quotaColor}`}>{formatPercent(row.quotaAttainment)}</td>
                  <td className="px-2 py-1 text-right">{formatPercent(row.avgDiscount)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

const DealSizeCard = ({ data }) => {
  if (!data?.dealSizeDist?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  const large25kPlus = data.dealSizeDist
    .filter(r => r.sizeBand === '$25K–100K' || r.sizeBand === '$100K–500K' || r.sizeBand === '>$500K')
    .reduce((sum, r) => sum + (r.totalValue || 0), 0);
  const totalRev = data.dealSizeDist.reduce((sum, r) => sum + (r.totalValue || 0), 0);
  
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Deal Size Distribution</h3>
      <div className="grid grid-cols-2 gap-4 mb-4">
        <div className="bg-gray-50 p-3 rounded">
          <p className="text-xs text-gray-600">Median Deal Size</p>
          <p className="text-xl font-bold">$50K–100K</p>
        </div>
        <div className="bg-gray-50 p-3 rounded">
          <p className="text-xs text-gray-600">Revenue >$25K</p>
          <p className="text-xl font-bold">{formatPercent((large25kPlus / totalRev) * 100)}</p>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={250}>
        <BarChart data={data.dealSizeDist}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="sizeBand" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Bar dataKey="dealCount" fill="#0088FE" name="Won" />
          <Bar dataKey="dealCount" fill="#FF8042" name="Lost" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

const CompetitiveCard = ({ data }) => {
  if (!data?.competitiveIntel?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Competitive Intelligence</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="bg-gray-100">
            <tr>
              <th className="px-2 py-1 text-left">Competitor</th>
              <th className="px-2 py-1 text-left">Our Product</th>
              <th className="px-2 py-1 text-right">Their Price</th>
              <th className="px-2 py-1 text-center">Feature Score</th>
              <th className="px-2 py-1 text-left">Segment</th>
            </tr>
          </thead>
          <tbody>
            {data.competitiveIntel.slice(0, 10).map((row, idx) => {
              const priceCompare = row.avgCompetitorPrice > 50000 ? 'text-red-600' : 'text-green-600';
              return (
                <tr key={idx} className="border-t">
                  <td className="px-2 py-1 font-semibold">{row.competitor}</td>
                  <td className="px-2 py-1">{row.ownProductSku}</td>
                  <td className={`px-2 py-1 text-right ${priceCompare}`}>{formatCurrency(row.avgCompetitorPrice)}</td>
                  <td className="px-2 py-1 text-center">
                    <div className="w-full bg-gray-200 rounded" style={{ height: '4px' }}>
                      <div style={{ width: `${row.avgFeatureScore * 100}%`, height: '100%', backgroundColor: '#00C49F' }} />
                    </div>
                  </td>
                  <td className="px-2 py-1">{row.segment}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

const RebateCard = ({ data }) => {
  if (!data?.rebateAnalysis?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  const topCustomers = data.rebateAnalysis
    .sort((a, b) => (b.totalAccrued || 0) - (a.totalAccrued || 0))
    .slice(0, 10);
  
  const totalLeakage = data.rebateAnalysis.reduce((sum, r) => sum + (r.leakage || 0), 0);
  const avgPayoutRate = data.rebateAnalysis.reduce((sum, r) => sum + (r.payoutRate || 0), 0) / data.rebateAnalysis.length;
  
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Rebate Analysis</h3>
      <div className="grid grid-cols-2 gap-4 mb-4">
        <div className="bg-red-50 p-3 rounded">
          <p className="text-xs text-gray-600">Total Leakage</p>
          <p className="text-lg font-bold text-red-600">{formatCurrency(totalLeakage)}</p>
        </div>
        <div className="bg-blue-50 p-3 rounded">
          <p className="text-xs text-gray-600">Avg Payout Rate</p>
          <p className="text-lg font-bold text-blue-600">{formatPercent(avgPayoutRate)}</p>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={250}>
        <BarChart data={topCustomers}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="customerName" width={80} angle={-45} />
          <YAxis />
          <Tooltip formatter={(value) => formatCurrency(value)} />
          <Legend />
          <Bar dataKey="totalAccrued" fill="#0088FE" name="Accrued" stackId="a" />
          <Bar dataKey="totalClaimed" fill="#00C49F" name="Claimed" stackId="a" />
          <Bar dataKey="totalPaid" fill="#FFBB28" name="Paid" stackId="a" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

const CostToServeCard = ({ data }) => {
  if (!data?.costToServe?.length) {
    return <div className="bg-gray-100 p-4 rounded text-gray-500">No data available</div>;
  }
  const topCustomers = data.costToServe
    .sort((a, b) => (b.totalCostToServe || 0) - (a.totalCostToServe || 0))
    .slice(0, 15);
  
  return (
    <div className="bg-white p-6 rounded-lg shadow">
      <h3 className="text-lg font-semibold mb-4">Cost to Serve Analysis</h3>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={topCustomers}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="customerName" width={80} angle={-45} />
          <YAxis />
          <Tooltip formatter={(value) => formatCurrency(value)} />
          <Legend />
          <Bar dataKey="serviceCost" fill="#0088FE" stackId="a" />
          <Bar dataKey="logisticsCost" fill="#00C49F" stackId="a" />
          <Bar dataKey="customizationCost" fill="#FFBB28" stackId="a" />
          <Bar dataKey="salesCoverageCost" fill="#FF8042" stackId="a" />
        </BarChart>
      </ResponsiveContainer>
      <div className="mt-4 overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="bg-gray-100">
            <tr>
              <th className="px-2 py-1 text-left">Customer</th>
              <th className="px-2 py-1 text-right">Total CTS</th>
              <th className="px-2 py-1 text-right">Avg Order Size</th>
            </tr>
          </thead>
          <tbody>
            {topCustomers.map((row, idx) => (
              <tr key={idx} className="border-t">
                <td className="px-2 py-1">{row.customerName}</td>
                <td className="px-2 py-1 text-right">{formatCurrency(row.totalCostToServe)}</td>
                <td className="px-2 py-1 text-right">{formatCurrency(row.avgOrderSize)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// Main Dashboard Component
export const Dashboard = () => {
  const [clientId] = useState('acmecorp');
  const { data, loading, error } = useDashboardData(clientId);

  if (loading) return <div className="p-8 text-center">Loading dashboard data...</div>;
  if (error) return <div className="p-8 text-center text-red-600">Error: {error}</div>;

  return (
    <div className="bg-gray-50 min-h-screen p-8">
      <div className="max-w-7xl mx-auto">
        <h1 className="text-4xl font-bold mb-8">Pricing Diagnostics Dashboard</h1>
        
        <h2 className="text-2xl font-semibold mt-12 mb-6">Core Analysis</h2>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-12">
          <RevenueByProductCard data={data} />
          <CustomerHealthCard data={data} />
          <DealSummaryCard data={data} />
          <DiscountAnalysisCard data={data} />
          <SegmentMetricsCard data={data} />
          <ChurnRiskCard data={data} />
          <ContractValueCard data={data} />
          <PricingTiersCard data={data} />
          <SegmentProfitabilityCard data={data} />
          <SalesMetricsCard data={data} />
        </div>

        <h2 className="text-2xl font-semibold mt-12 mb-6">Extended Analysis</h2>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <PriceBandCard data={data} />
          <WinLossCard data={data} />
          <DealVelocityCard data={data} />
          <DiscountGovCard data={data} />
          <CohortRevenueCard data={data} />
          <RepPerfCard data={data} />
          <DealSizeCard data={data} />
          <CompetitiveCard data={data} />
          <RebateCard data={data} />
          <CostToServeCard data={data} />
        </div>
      </div>
    </div>
  );
};

export default Dashboard;
