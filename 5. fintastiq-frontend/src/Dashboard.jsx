import { useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  ScatterChart, Scatter, ZAxis, Cell,
  LineChart, Line,
  PieChart, Pie,
  AreaChart, Area,
  ComposedChart,
} from "recharts";

/* ═══════════════════════════════════════════════════════════════
   DUMMY DATA — simulates Gold layer output for AcmeCorp
   ═══════════════════════════════════════════════════════════════ */

const CLIENT = { name: "Gravitate Energy", industry: "SaaS / Fuel Distribution", tier: "Gold", dataQuality: "B+", lastUpdated: "2026-03-07" };

const EXECUTIVE_SUMMARY = {
  totalRevenue: 48700000,
  avgMargin: 34.2,
  revenueAtRisk: 3200000,
  opportunityValue: 5800000,
  totalCustomers: 847,
  activeProducts: 312,
  avgDiscount: 18.7,
  contractCompliance: 76,
};

// A01 - Pocket Price Waterfall
const waterfallData = [
  { name: "List Price", value: 100, fill: "#c5d44b" },
  { name: "Volume Discount", value: -8.2, fill: "#ef5350" },
  { name: "Prompt Pay", value: -3.1, fill: "#ef5350" },
  { name: "Rebates", value: -4.5, fill: "#ef5350" },
  { name: "Freight", value: -2.8, fill: "#ef5350" },
  { name: "Co-op Marketing", value: -1.2, fill: "#ef5350" },
  { name: "Credit Memos", value: -0.9, fill: "#ef5350" },
  { name: "Invoice Price", value: 82.4, fill: "#3e8c7f" },
  { name: "COGS", value: -48.6, fill: "#3b5068" },
  { name: "Pocket Margin", value: 30.7, fill: "#00a86b" },
];

// Waterfall needs cumulative calculation
const waterfallProcessed = (() => {
  let cumulative = 0;
  return waterfallData.map((d, i) => {
    if (i === 0 || d.name === "Invoice Price" || d.name === "Pocket Margin") {
      cumulative = d.value;
      return { ...d, base: 0, height: d.value };
    }
    const base = cumulative + d.value;
    const height = Math.abs(d.value);
    cumulative = base;
    return { ...d, base, height, value: d.value };
  });
})();

// A03 - Price Volume Mix
const pvmData = [
  { period: "Q1→Q2", price: 420000, volume: -180000, mix: 95000, total: 335000 },
  { period: "Q2→Q3", price: -150000, volume: 380000, mix: -60000, total: 170000 },
  { period: "Q3→Q4", price: 280000, volume: 220000, mix: 140000, total: 640000 },
  { period: "Q4→Q1", price: 510000, volume: -90000, mix: -30000, total: 390000 },
];

// A04 - Customer Profitability
const custProfitData = [
  { name: "Enterprise A", revenue: 4200, margin: 42, size: 400, segment: "Enterprise" },
  { name: "Enterprise B", revenue: 3800, margin: 38, size: 350, segment: "Enterprise" },
  { name: "Mid-Market C", revenue: 1900, margin: 31, size: 200, segment: "Mid-Market" },
  { name: "Mid-Market D", revenue: 1500, margin: 28, size: 180, segment: "Mid-Market" },
  { name: "Mid-Market E", revenue: 1200, margin: 44, size: 150, segment: "Mid-Market" },
  { name: "SMB F", revenue: 800, margin: 22, size: 100, segment: "SMB" },
  { name: "SMB G", revenue: 600, margin: 18, size: 80, segment: "SMB" },
  { name: "SMB H", revenue: 400, margin: -5, size: 60, segment: "SMB" },
  { name: "Enterprise I", revenue: 3200, margin: 35, size: 300, segment: "Enterprise" },
  { name: "Long Tail J", revenue: 200, margin: 8, size: 40, segment: "Long Tail" },
  { name: "Long Tail K", revenue: 150, margin: -12, size: 30, segment: "Long Tail" },
  { name: "Mid-Market L", revenue: 2100, margin: 39, size: 220, segment: "Mid-Market" },
];

const segmentColors = { Enterprise: "#3e8c7f", "Mid-Market": "#c5d44b", SMB: "#3b5068", "Long Tail": "#ef5350" };

// A05 - Discount Effectiveness
const discountData = [
  { discount: 2, revenueGrowth: 1.2, segment: "Enterprise" },
  { discount: 5, revenueGrowth: 4.8, segment: "Enterprise" },
  { discount: 8, revenueGrowth: 7.1, segment: "Mid-Market" },
  { discount: 10, revenueGrowth: 8.5, segment: "Enterprise" },
  { discount: 12, revenueGrowth: 6.2, segment: "Mid-Market" },
  { discount: 15, revenueGrowth: 4.1, segment: "SMB" },
  { discount: 18, revenueGrowth: 2.8, segment: "Mid-Market" },
  { discount: 20, revenueGrowth: 1.5, segment: "SMB" },
  { discount: 22, revenueGrowth: 0.3, segment: "SMB" },
  { discount: 25, revenueGrowth: -1.2, segment: "Long Tail" },
  { discount: 28, revenueGrowth: -3.8, segment: "Long Tail" },
  { discount: 30, revenueGrowth: -5.5, segment: "Long Tail" },
];

// A08 - Deal Scorecard
const dealData = [
  { rep: "Sarah M.", winRate: 68, avgDiscount: 12.4, deals: 45, revenue: 2800000 },
  { rep: "James K.", winRate: 52, avgDiscount: 18.7, deals: 38, revenue: 1900000 },
  { rep: "Maria L.", winRate: 71, avgDiscount: 10.2, deals: 52, revenue: 3400000 },
  { rep: "David R.", winRate: 45, avgDiscount: 22.1, deals: 31, revenue: 1200000 },
  { rep: "Lisa W.", winRate: 63, avgDiscount: 14.8, deals: 41, revenue: 2300000 },
];

// A10 - Price Corridor (heatmap data as bar chart)
const corridorData = [
  { sku: "Fuel Premium", p10: 82, p25: 88, median: 95, p75: 102, p90: 110 },
  { sku: "Fuel Regular", p10: 72, p25: 76, median: 82, p75: 88, p90: 94 },
  { sku: "Fleet Mgmt SaaS", p10: 450, p25: 520, median: 600, p75: 680, p90: 750 },
  { sku: "Logistics API", p10: 180, p25: 210, median: 250, p75: 290, p90: 340 },
  { sku: "Analytics Pro", p10: 320, p25: 380, median: 420, p75: 480, p90: 540 },
];

// A11 - SKU Pareto
const paretoData = [
  { sku: "Fleet Mgmt SaaS", revenue: 12400, cumPct: 25.5 },
  { sku: "Fuel Premium", revenue: 8900, cumPct: 43.8 },
  { sku: "Analytics Pro", revenue: 6200, cumPct: 56.5 },
  { sku: "Logistics API", revenue: 4800, cumPct: 66.4 },
  { sku: "Fuel Regular", revenue: 3900, cumPct: 74.4 },
  { sku: "Telematics", revenue: 3100, cumPct: 80.8 },
  { sku: "Fleet Cards", revenue: 2400, cumPct: 85.7 },
  { sku: "EV Charging", revenue: 1900, cumPct: 89.6 },
  { sku: "Route Optimizer", revenue: 1500, cumPct: 92.7 },
  { sku: "Other (18 SKUs)", revenue: 3600, cumPct: 100 },
];

// A13 - Churn Risk
const churnData = [
  { name: "Corp Alpha", revenue: 3200, riskScore: 82, trend: "declining" },
  { name: "Beta Fleet", revenue: 2800, riskScore: 71, trend: "declining" },
  { name: "Gamma Logistics", revenue: 1500, riskScore: 65, trend: "flat" },
  { name: "Delta Transport", revenue: 4100, riskScore: 45, trend: "flat" },
  { name: "Epsilon Fuel", revenue: 900, riskScore: 88, trend: "declining" },
  { name: "Zeta Corp", revenue: 2200, riskScore: 35, trend: "growing" },
  { name: "Eta Systems", revenue: 1800, riskScore: 55, trend: "flat" },
  { name: "Theta Inc", revenue: 3500, riskScore: 28, trend: "growing" },
  { name: "Iota Fleet", revenue: 700, riskScore: 91, trend: "declining" },
  { name: "Kappa Energy", revenue: 5200, riskScore: 22, trend: "growing" },
];

// A14 - Geographic Pricing
const geoData = [
  { region: "Northeast", avgPrice: 94.2, national: 88.5, variance: 6.4, revenue: 12400 },
  { region: "Southeast", avgPrice: 82.1, national: 88.5, variance: -7.2, revenue: 9800 },
  { region: "Midwest", avgPrice: 86.9, national: 88.5, variance: -1.8, revenue: 8200 },
  { region: "Southwest", avgPrice: 91.5, national: 88.5, variance: 3.4, revenue: 7600 },
  { region: "West Coast", avgPrice: 97.8, national: 88.5, variance: 10.5, revenue: 10700 },
];

// A16 - Promotion ROI
const promoData = [
  { name: "Q1 Fleet Promo", spend: 120, incremental: 480, roi: 300 },
  { name: "Summer Fuel Deal", spend: 85, incremental: 195, roi: 129 },
  { name: "Year-End Push", spend: 200, incremental: 140, roi: -30 },
  { name: "New Client Offer", spend: 65, incremental: 310, roi: 377 },
  { name: "Loyalty Program", spend: 150, incremental: 520, roi: 247 },
];

// Revenue trend
const revenueTrend = [
  { month: "Apr", revenue: 3800, margin: 32.1 },
  { month: "May", revenue: 4100, margin: 33.5 },
  { month: "Jun", revenue: 3900, margin: 31.8 },
  { month: "Jul", revenue: 4400, margin: 34.2 },
  { month: "Aug", revenue: 4200, margin: 33.9 },
  { month: "Sep", revenue: 4600, margin: 35.1 },
  { month: "Oct", revenue: 4300, margin: 34.5 },
  { month: "Nov", revenue: 4800, margin: 36.2 },
  { month: "Dec", revenue: 4500, margin: 35.0 },
  { month: "Jan", revenue: 4100, margin: 33.8 },
  { month: "Feb", revenue: 4700, margin: 35.5 },
  { month: "Mar", revenue: 4900, margin: 36.8 },
];

// Opportunities
const opportunities = [
  { id: 1, type: "Price Correction", analysis: "Geographic Pricing", impact: "$1.2M", confidence: "High", priority: 1, desc: "West Coast and Northeast pricing 7-10% above national avg — reduce to capture volume" },
  { id: 2, type: "Discount Optimization", analysis: "Discount Effectiveness", impact: "$890K", confidence: "High", priority: 2, desc: "Discounts >18% show negative ROI — cap SMB discounts at 15%" },
  { id: 3, type: "Churn Prevention", analysis: "Churn Risk", impact: "$780K", confidence: "Medium", priority: 3, desc: "5 high-value customers at >70% churn risk — initiate retention outreach" },
  { id: 4, type: "Contract Recovery", analysis: "Contract Leakage", impact: "$650K", confidence: "High", priority: 4, desc: "24% of transactions priced below contracted rates" },
  { id: 5, type: "Promotion Reallocation", analysis: "Promotion ROI", impact: "$340K", confidence: "Medium", priority: 5, desc: "Shift budget from Year-End Push (-30% ROI) to New Client Offer (377% ROI)" },
  { id: 6, type: "Escalation Compliance", analysis: "Annual Escalation", impact: "$520K", confidence: "High", priority: 6, desc: "41% of contracts under-escalated vs contractual terms" },
  { id: 7, type: "Portfolio Pruning", analysis: "SKU Pareto", impact: "$280K", confidence: "Low", priority: 7, desc: "Bottom 18 SKUs contribute 7.3% revenue but consume 22% of support costs" },
];

const ANALYSIS_LIST = [
  { id: "01", name: "Pocket Price Waterfall", icon: "\u2193" },
  { id: "02", name: "Margin Bridge", icon: "\u2194" },
  { id: "03", name: "Price-Volume-Mix", icon: "\u2261" },
  { id: "04", name: "Customer Profitability", icon: "\u25CE" },
  { id: "05", name: "Discount Effectiveness", icon: "\u25E2" },
  { id: "06", name: "Price Elasticity", icon: "\u223F" },
  { id: "07", name: "Contract Leakage", icon: "\u26A0" },
  { id: "08", name: "Deal Scorecard", icon: "\u2605" },
  { id: "09", name: "Competitive Positioning", icon: "\u2694" },
  { id: "10", name: "Price Corridor", icon: "\u2550" },
  { id: "11", name: "SKU Pareto", icon: "\u25A4" },
  { id: "12", name: "New Product Pricing", icon: "\u2726" },
  { id: "13", name: "Churn Risk", icon: "\u25BC" },
  { id: "14", name: "Geographic Pricing", icon: "\u25C9" },
  { id: "15", name: "Channel Conflict", icon: "\u21C4" },
  { id: "16", name: "Promotion ROI", icon: "\u2606" },
  { id: "17", name: "Cost-to-Serve", icon: "\u25A3" },
  { id: "18", name: "Rebate Optimization", icon: "\u21BA" },
  { id: "19", name: "Annual Escalation", icon: "\u2191" },
  { id: "20", name: "Win/Loss", icon: "\u2713" },
];

/* ═══════════════════════════════════════════════════════════════
   COMPONENTS
   ═══════════════════════════════════════════════════════════════ */

const fmt = (n) => n >= 1000000 ? `$${(n / 1000000).toFixed(1)}M` : n >= 1000 ? `$${(n / 1000).toFixed(0)}K` : `$${n}`;

function MetricCard({ label, value, sub, color = "#c5d44b", trend }) {
  return (
    <div style={{
      background: "#1e1e21",
      borderRadius: 14,
      padding: "16px 20px",
      border: "1px solid hsla(0,0%,100%,.03)",
      flex: "1 1 0",
      boxShadow: "5px 5px 12px rgba(0,0,0,.55), -5px -5px 12px hsla(0,0%,100%,.04)",
    }}>
      <div style={{ fontSize: 11, color: "#888", marginBottom: 4, textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</div>
      <div style={{ fontSize: 26, fontWeight: 800, color }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: trend === "up" ? "#00a86b" : trend === "down" ? "#ef5350" : "#888", marginTop: 2 }}>{sub}</div>}
    </div>
  );
}

function SectionTitle({ children, sub }) {
  return (
    <div style={{ marginBottom: 12 }}>
      <h3 style={{ fontSize: 15, fontWeight: 700, color: "#f0f0f2", margin: 0 }}>{children}</h3>
      {sub && <p style={{ fontSize: 11, color: "#888", margin: "2px 0 0" }}>{sub}</p>}
    </div>
  );
}

function ChartCard({ children, title, sub, span = 1 }) {
  return (
    <div style={{
      background: "#1e1e21",
      borderRadius: 14,
      padding: 20,
      border: "1px solid hsla(0,0%,100%,.03)",
      gridColumn: `span ${span}`,
      boxShadow: "5px 5px 12px rgba(0,0,0,.55), -5px -5px 12px hsla(0,0%,100%,.04)",
    }}>
      {title && <SectionTitle sub={sub}>{title}</SectionTitle>}
      {children}
    </div>
  );
}

function Badge({ children, color }) {
  return (
    <span style={{
      display: "inline-block",
      padding: "2px 10px",
      borderRadius: 9999,
      fontSize: 11,
      fontWeight: 600,
      background: color + "22",
      color,
      border: `1px solid ${color}44`,
    }}>{children}</span>
  );
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "#1e1e21",
      border: "1px solid hsla(0,0%,100%,.03)",
      borderRadius: 8,
      padding: "8px 12px",
      fontSize: 11,
    }}>
      <div style={{ color: "#f0f0f2", fontWeight: 600, marginBottom: 4 }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color || "#888" }}>
          {p.name}: {typeof p.value === "number" ? p.value.toLocaleString() : p.value}
        </div>
      ))}
    </div>
  );
};

/* ═══════════════════════════════════════════════════════════════
   ANALYSIS VIEWS
   ═══════════════════════════════════════════════════════════════ */

function ExecutiveDashboard() {
  const s = EXECUTIVE_SUMMARY;
  return (
    <div>
      <div style={{ display: "flex", gap: 12, marginBottom: 20 }}>
        <MetricCard label="Total Revenue" value={fmt(s.totalRevenue)} sub="+8.2% YoY" trend="up" color="#c5d44b" />
        <MetricCard label="Avg Margin" value={`${s.avgMargin}%`} sub="+1.8pp vs prior" trend="up" color="#00a86b" />
        <MetricCard label="Revenue at Risk" value={fmt(s.revenueAtRisk)} sub="5 customers flagged" trend="down" color="#ef5350" />
        <MetricCard label="Opportunity Value" value={fmt(s.opportunityValue)} sub="7 identified" trend="up" color="#c5d44b" />
      </div>
      <div style={{ display: "flex", gap: 12, marginBottom: 20 }}>
        <MetricCard label="Active Customers" value={s.totalCustomers.toLocaleString()} color="#3e8c7f" />
        <MetricCard label="Active Products" value={s.activeProducts} color="#7ea05e" />
        <MetricCard label="Avg Discount" value={`${s.avgDiscount}%`} sub="Target: 15%" trend="down" color="#3b5068" />
        <MetricCard label="Contract Compliance" value={`${s.contractCompliance}%`} sub="Target: 90%" trend="down" color="#ef5350" />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 16 }}>
        <ChartCard title="Revenue & Margin Trend (12mo)" sub="Monthly revenue ($K) with margin % overlay">
          <ResponsiveContainer width="100%" height={260}>
            <ComposedChart data={revenueTrend}>
              <CartesianGrid strokeDasharray="3 3" stroke="#333" />
              <XAxis dataKey="month" tick={{ fill: "#888", fontSize: 11 }} />
              <YAxis yAxisId="left" tick={{ fill: "#888", fontSize: 11 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: "#888", fontSize: 11 }} domain={[25, 45]} />
              <Tooltip content={<CustomTooltip />} />
              <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
              <Bar yAxisId="left" dataKey="revenue" name="Revenue ($K)" fill="#c5d44b" radius={[4, 4, 0, 0]} />
              <Line yAxisId="right" type="monotone" dataKey="margin" name="Margin %" stroke="#00a86b" strokeWidth={2} dot={{ fill: "#00a86b", r: 3 }} />
            </ComposedChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Top Opportunities" sub="Ranked by revenue impact">
          <div style={{ maxHeight: 260, overflowY: "auto" }}>
            {opportunities.slice(0, 5).map((o, i) => (
              <div key={o.id} style={{
                padding: "10px 12px",
                borderRadius: 8,
                marginBottom: 6,
                background: "rgba(197,212,75,.08)",
                border: "1px solid rgba(197,212,75,.15)",
              }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
                  <span style={{ fontSize: 12, fontWeight: 700, color: "#f0f0f2" }}>{o.type}</span>
                  <span style={{ fontSize: 13, fontWeight: 800, color: "#00a86b" }}>{o.impact}</span>
                </div>
                <div style={{ fontSize: 10, color: "#888" }}>{o.analysis}</div>
              </div>
            ))}
          </div>
        </ChartCard>
      </div>
    </div>
  );
}

function PocketPriceWaterfallView() {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12, marginBottom: 4 }}>
        <MetricCard label="List Price (Index)" value="100.0" color="#c5d44b" />
        <MetricCard label="Invoice Price" value="82.4" sub="-17.6% leakage" trend="down" color="#3e8c7f" />
        <MetricCard label="Pocket Margin" value="30.7%" color="#00a86b" />
        <MetricCard label="Total Leakage" value="$8.5M" sub="Annualized" trend="down" color="#ef5350" />
      </div>
      <ChartCard title="Pocket Price Waterfall" sub="Price cascade from list to pocket margin (indexed to 100)">
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={waterfallProcessed} barSize={40}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" vertical={false} />
            <XAxis dataKey="name" tick={{ fill: "#888", fontSize: 10 }} angle={-20} textAnchor="end" height={60} />
            <YAxis tick={{ fill: "#888", fontSize: 11 }} domain={[0, 110]} />
            <Tooltip content={<CustomTooltip />} />
            <Bar dataKey="base" stackId="a" fill="transparent" />
            <Bar dataKey="height" stackId="a" name="Value" radius={[4, 4, 0, 0]}>
              {waterfallProcessed.map((entry, i) => (
                <Cell key={i} fill={entry.fill} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>
      <ChartCard title="Key Insight">
        <p style={{ fontSize: 12, color: "#d0d0d0", lineHeight: 1.7, margin: 0 }}>
          The largest price leakage points are <strong style={{ color: "#ef5350" }}>Volume Discounts (-8.2%)</strong> and{" "}
          <strong style={{ color: "#ef5350" }}>Rebates (-4.5%)</strong>. Combined, off-invoice adjustments erode 17.6% of list price.
          Tightening volume discount thresholds by 2pp and enforcing rebate claim deadlines could recover an estimated <strong style={{ color: "#00a86b" }}>$1.4M</strong> annually.
        </p>
      </ChartCard>
    </div>
  );
}

function CustomerProfitabilityView() {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12 }}>
        <MetricCard label="Profitable Customers" value="89%" color="#00a86b" />
        <MetricCard label="Unprofitable" value="11%" sub="2 customers" trend="down" color="#ef5350" />
        <MetricCard label="Top Decile Margin" value="42.0%" color="#3e8c7f" />
        <MetricCard label="Bottom Decile" value="-8.5%" color="#3b5068" />
      </div>
      <ChartCard title="Customer Profitability Map" sub="Revenue ($K) vs Margin (%) — bubble size = transaction volume">
        <ResponsiveContainer width="100%" height={340}>
          <ScatterChart>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="revenue" name="Revenue ($K)" tick={{ fill: "#888", fontSize: 11 }} label={{ value: "Revenue ($K)", position: "bottom", fill: "#888", fontSize: 11, offset: -5 }} />
            <YAxis dataKey="margin" name="Margin %" tick={{ fill: "#888", fontSize: 11 }} label={{ value: "Margin %", angle: -90, position: "insideLeft", fill: "#888", fontSize: 11 }} />
            <ZAxis dataKey="size" range={[40, 400]} />
            <Tooltip content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const d = payload[0].payload;
              return (
                <div style={{ background: "#1e1e21", border: "1px solid hsla(0,0%,100%,.03)", borderRadius: 8, padding: "8px 12px", fontSize: 11 }}>
                  <div style={{ fontWeight: 700, color: "#f0f0f2" }}>{d.name}</div>
                  <div style={{ color: "#888" }}>Revenue: ${d.revenue}K</div>
                  <div style={{ color: "#888" }}>Margin: {d.margin}%</div>
                  <div style={{ color: "#888" }}>Segment: {d.segment}</div>
                </div>
              );
            }} />
            <Scatter data={custProfitData}>
              {custProfitData.map((d, i) => (
                <Cell key={i} fill={segmentColors[d.segment] || "#888"} fillOpacity={0.8} />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
        <div style={{ display: "flex", gap: 16, justifyContent: "center", marginTop: 8 }}>
          {Object.entries(segmentColors).map(([seg, col]) => (
            <div key={seg} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: "#888" }}>
              <div style={{ width: 10, height: 10, borderRadius: "50%", background: col }} />{seg}
            </div>
          ))}
        </div>
      </ChartCard>
    </div>
  );
}

function DealScorecardView() {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12 }}>
        <MetricCard label="Avg Win Rate" value="59.8%" color="#00a86b" />
        <MetricCard label="Avg Discount" value="15.6%" color="#3b5068" />
        <MetricCard label="Total Pipeline" value="$11.6M" color="#c5d44b" />
        <MetricCard label="Top Rep Revenue" value="$3.4M" sub="Maria L." color="#3e8c7f" />
      </div>
      <ChartCard title="Sales Rep Scorecard" sub="Win rate vs average discount by rep">
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={dealData} barGap={4}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="rep" tick={{ fill: "#888", fontSize: 11 }} />
            <YAxis tick={{ fill: "#888", fontSize: 11 }} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
            <Bar dataKey="winRate" name="Win Rate %" fill="#00a86b" radius={[4, 4, 0, 0]} />
            <Bar dataKey="avgDiscount" name="Avg Discount %" fill="#ef5350" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>
      <ChartCard title="Key Insight">
        <p style={{ fontSize: 12, color: "#d0d0d0", lineHeight: 1.7, margin: 0 }}>
          <strong style={{ color: "#00a86b" }}>Maria L.</strong> leads with 71% win rate and lowest discount (10.2%), proving premium pricing is achievable.{" "}
          <strong style={{ color: "#ef5350" }}>David R.</strong> has the lowest win rate (45%) despite the highest discounts (22.1%) — suggesting discount-led selling isn't working and coaching is needed.
        </p>
      </ChartCard>
    </div>
  );
}

function SkuParetoView() {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12 }}>
        <MetricCard label="Top 20% SKUs" value="6 products" sub="80.8% of revenue" color="#c5d44b" />
        <MetricCard label="Long Tail" value="18 SKUs" sub="7.3% of revenue" color="#3b5068" />
        <MetricCard label="Highest SKU" value="$12.4M" sub="Fleet Mgmt SaaS" color="#00a86b" />
      </div>
      <ChartCard title="SKU Revenue Pareto" sub="Revenue ($K) with cumulative % line">
        <ResponsiveContainer width="100%" height={320}>
          <ComposedChart data={paretoData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="sku" tick={{ fill: "#888", fontSize: 9 }} angle={-15} textAnchor="end" height={60} />
            <YAxis yAxisId="left" tick={{ fill: "#888", fontSize: 11 }} />
            <YAxis yAxisId="right" orientation="right" tick={{ fill: "#888", fontSize: 11 }} domain={[0, 105]} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
            <Bar yAxisId="left" dataKey="revenue" name="Revenue ($K)" fill="#c5d44b" radius={[4, 4, 0, 0]} />
            <Line yAxisId="right" type="monotone" dataKey="cumPct" name="Cumulative %" stroke="#7ea05e" strokeWidth={2} dot={{ fill: "#7ea05e", r: 3 }} />
          </ComposedChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  );
}

function ChurnRiskView() {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12 }}>
        <MetricCard label="High Risk (>70)" value="3 customers" color="#ef5350" />
        <MetricCard label="Revenue at Risk" value="$7.0M" sub="High risk customers" color="#ef5350" />
        <MetricCard label="Avg Risk Score" value="58.2" color="#3b5068" />
        <MetricCard label="Low Risk (<30)" value="2 customers" sub="$8.7M secured" color="#00a86b" />
      </div>
      <ChartCard title="Churn Risk Map" sub="Revenue ($K) vs Risk Score — high risk = top right">
        <ResponsiveContainer width="100%" height={320}>
          <ScatterChart>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="revenue" name="Revenue ($K)" tick={{ fill: "#888", fontSize: 11 }} label={{ value: "Revenue ($K)", position: "bottom", fill: "#888", fontSize: 11, offset: -5 }} />
            <YAxis dataKey="riskScore" name="Risk Score" tick={{ fill: "#888", fontSize: 11 }} domain={[0, 100]} label={{ value: "Risk Score", angle: -90, position: "insideLeft", fill: "#888", fontSize: 11 }} />
            <Tooltip content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const d = payload[0].payload;
              return (
                <div style={{ background: "#1e1e21", border: "1px solid hsla(0,0%,100%,.03)", borderRadius: 8, padding: "8px 12px", fontSize: 11 }}>
                  <div style={{ fontWeight: 700, color: "#f0f0f2" }}>{d.name}</div>
                  <div style={{ color: "#888" }}>Revenue: ${d.revenue}K | Risk: {d.riskScore}</div>
                  <div style={{ color: "#888" }}>Trend: {d.trend}</div>
                </div>
              );
            }} />
            <Scatter data={churnData}>
              {churnData.map((d, i) => (
                <Cell key={i} fill={d.riskScore > 70 ? "#ef5350" : d.riskScore > 50 ? "#3b5068" : "#00a86b"} fillOpacity={0.85} />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  );
}

function GeoPricingView() {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12 }}>
        <MetricCard label="National Avg Price" value="$88.50" color="#c5d44b" />
        <MetricCard label="Highest Region" value="$97.80" sub="West Coast (+10.5%)" color="#ef5350" />
        <MetricCard label="Lowest Region" value="$82.10" sub="Southeast (-7.2%)" color="#00a86b" />
        <MetricCard label="Max Variance" value="17.7%" sub="West Coast vs Southeast" color="#3e8c7f" />
      </div>
      <ChartCard title="Regional Pricing vs National Average" sub="Average realized price by region with national benchmark">
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={geoData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="region" tick={{ fill: "#888", fontSize: 11 }} />
            <YAxis tick={{ fill: "#888", fontSize: 11 }} domain={[70, 110]} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
            <Bar dataKey="avgPrice" name="Avg Price ($)" fill="#c5d44b" radius={[4, 4, 0, 0]} />
            <Line type="monotone" dataKey="national" name="National Avg" stroke="#3b5068" strokeWidth={2} strokeDasharray="8 4" dot={false} />
          </ComposedChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  );
}

// Capture rate labels and multipliers for the slider
const CAPTURE_PRESETS = [
  { label: "Conservative", pct: 0.30, desc: "Early-stage pricing maturity, limited change management capacity" },
  { label: "Moderate", pct: 0.50, desc: "Some pricing processes in place, management aligned" },
  { label: "Ambitious", pct: 0.70, desc: "Strong pricing governance, executive buy-in, dedicated team" },
];

function parseDollar(str) {
  const num = parseFloat(str.replace(/[$MK,]/g, ""));
  if (str.includes("M")) return num * 1_000_000;
  if (str.includes("K")) return num * 1_000;
  return num;
}

function OpportunitiesView() {
  const [captureIdx, setCaptureIdx] = useState(1); // default: Moderate
  const preset = CAPTURE_PRESETS[captureIdx];

  const rawTotal = opportunities.reduce((sum, o) => sum + parseDollar(o.impact), 0);
  const capturedTotal = rawTotal * preset.pct;
  const fmt2 = (n) => n >= 1_000_000 ? `$${(n / 1_000_000).toFixed(1)}M` : `$${(n / 1000).toFixed(0)}K`;

  return (
    <div>
      <div style={{ display: "flex", gap: 12, marginBottom: 20 }}>
        <MetricCard label="Total Opportunities" value="7" color="#c5d44b" />
        <MetricCard label="Full Opportunity" value={fmt2(rawTotal)} sub="100% capture" color="#3b5068" />
        <MetricCard label="Realistic Capture" value={fmt2(capturedTotal)} sub={`${preset.label} (${Math.round(preset.pct * 100)}%)`} color="#00a86b" />
        <MetricCard label="High Confidence" value="4" sub="$3.3M impact" color="#00a86b" />
      </div>

      {/* Capture rate slider */}
      <div style={{
        background: "#1e1e21",
        borderRadius: 14,
        padding: "20px 24px",
        border: "1px solid rgba(197,212,75,.15)",
        marginBottom: 20,
        boxShadow: "5px 5px 12px rgba(0,0,0,.55), -5px -5px 12px hsla(0,0%,100%,.04)",
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 14 }}>
          <div>
            <div style={{ fontSize: 13, fontWeight: 700, color: "#f0f0f2", marginBottom: 2 }}>Capture Rate</div>
            <div style={{ fontSize: 11, color: "#888" }}>
              What % of the total opportunity can your organisation realistically capture based on pricing maturity?
            </div>
          </div>
          <div style={{ textAlign: "right" }}>
            <div style={{ fontSize: 22, fontWeight: 800, color: "#00a86b" }}>{fmt2(capturedTotal)}</div>
            <div style={{ fontSize: 10, color: "#666" }}>addressable at {Math.round(preset.pct * 100)}% capture</div>
          </div>
        </div>

        {/* Slider buttons */}
        <div style={{ display: "flex", gap: 10, marginBottom: 14 }}>
          {CAPTURE_PRESETS.map((p, i) => (
            <button
              key={p.label}
              onClick={() => setCaptureIdx(i)}
              style={{
                flex: 1,
                background: captureIdx === i ? "rgba(197,212,75,.15)" : "#222225",
                border: `1px solid ${captureIdx === i ? "rgba(197,212,75,.4)" : "hsla(0,0%,100%,.06)"}`,
                borderRadius: 9,
                padding: "10px 12px",
                cursor: "pointer",
                transition: "all 0.2s",
                textAlign: "left",
              }}
            >
              <div style={{ fontSize: 12, fontWeight: 700, color: captureIdx === i ? "#c5d44b" : "#888", marginBottom: 2 }}>
                {p.label}
              </div>
              <div style={{ fontSize: 18, fontWeight: 800, color: captureIdx === i ? "#f0f0f2" : "#555" }}>
                {Math.round(p.pct * 100)}%
              </div>
            </button>
          ))}
        </div>

        {/* Range slider */}
        <input
          type="range" min={0} max={2} step={1}
          value={captureIdx}
          onChange={e => setCaptureIdx(Number(e.target.value))}
          style={{ width: "100%", accentColor: "#c5d44b", cursor: "pointer" }}
        />
        <div style={{ fontSize: 11, color: "#666", marginTop: 8 }}>
          {preset.desc}
        </div>

        {/* Capture bar visualisation */}
        <div style={{ marginTop: 14 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#555", marginBottom: 4 }}>
            <span>$0</span><span>Full opportunity: {fmt2(rawTotal)}</span>
          </div>
          <div style={{ height: 10, background: "#222225", borderRadius: 9999, overflow: "hidden" }}>
            <div style={{
              height: "100%",
              width: `${preset.pct * 100}%`,
              background: "linear-gradient(90deg, #3e8c7f, #c5d44b)",
              borderRadius: 9999,
              transition: "width 0.4s ease",
            }} />
          </div>
          <div style={{
            display: "flex", justifyContent: "space-between",
            fontSize: 10, color: "#555", marginTop: 4
          }}>
            <span style={{ color: "#00a86b", fontWeight: 600 }}>Captured: {fmt2(capturedTotal)}</span>
            <span>Uncaptured: {fmt2(rawTotal - capturedTotal)}</span>
          </div>
        </div>
      </div>

      {/* Opportunity list */}
      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {opportunities.map(o => {
          const raw = parseDollar(o.impact);
          const captured = raw * preset.pct;
          return (
            <div key={o.id} style={{
              background: "#1e1e21",
              borderRadius: 12,
              padding: "16px 20px",
              border: `1px solid ${o.confidence === "High" ? "rgba(0,168,107,.3)" : o.confidence === "Medium" ? "rgba(197,212,75,.2)" : "rgba(136,136,136,.2)"}`,
              display: "grid",
              gridTemplateColumns: "40px 1fr 150px 80px",
              gap: 16,
              alignItems: "center",
              boxShadow: "5px 5px 12px rgba(0,0,0,.55), -5px -5px 12px hsla(0,0%,100%,.04)",
            }}>
              <div style={{
                width: 36, height: 36, borderRadius: 8,
                background: "rgba(197,212,75,.15)",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 14, fontWeight: 800, color: "#c5d44b",
              }}>#{o.priority}</div>
              <div>
                <div style={{ fontSize: 13, fontWeight: 700, color: "#f0f0f2", marginBottom: 2 }}>{o.type}</div>
                <div style={{ fontSize: 11, color: "#888" }}>{o.desc}</div>
                <div style={{ fontSize: 10, color: "#666", marginTop: 4 }}>Source: {o.analysis}</div>
              </div>
              <div style={{ textAlign: "right" }}>
                <div style={{ fontSize: 16, fontWeight: 800, color: "#00a86b" }}>{fmt2(captured)}</div>
                <div style={{ fontSize: 10, color: "#555" }}>captured ({Math.round(preset.pct * 100)}%)</div>
                <div style={{ fontSize: 10, color: "#444", textDecoration: "line-through", marginTop: 1 }}>{o.impact} full</div>
              </div>
              <div style={{ textAlign: "center" }}>
                <Badge color={o.confidence === "High" ? "#00a86b" : o.confidence === "Medium" ? "#c5d44b" : "#888"}>
                  {o.confidence}
                </Badge>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════
   MAIN APP
   ═══════════════════════════════════════════════════════════════ */

/* ═══════════════════════════════════════════════════════════════
   FILTER BAR — shows above chart views for quick slicing
   ═══════════════════════════════════════════════════════════════ */

const FILTER_DIMS = ["Segment", "Region", "Product", "Time Period"];

function FilterBar() {
  const [active, setActive] = useState({});

  const OPTIONS = {
    Segment: ["Enterprise", "Mid-Market", "SMB", "Long Tail"],
    Region: ["Northeast", "Southeast", "Midwest", "Southwest", "West Coast"],
    Product: ["Fleet Mgmt SaaS", "Fuel Premium", "Analytics Pro", "Logistics API", "Other"],
    "Time Period": ["Last 3mo", "Last 6mo", "Last 12mo", "YTD"],
  };

  return (
    <div style={{
      display: "flex", gap: 10, marginBottom: 18, flexWrap: "wrap", alignItems: "center",
    }}>
      <span style={{ fontSize: 10, color: "#666", textTransform: "uppercase", letterSpacing: 0.8, marginRight: 4 }}>Filters</span>
      {FILTER_DIMS.map(dim => {
        const isOpen = active[dim];
        return (
          <div key={dim} style={{ position: "relative" }}>
            <button
              onClick={() => setActive(prev => ({ ...prev, [dim]: !prev[dim] }))}
              style={{
                background: isOpen ? "rgba(197,212,75,.12)" : "#222225",
                border: `1px solid ${isOpen ? "rgba(197,212,75,.35)" : "hsla(0,0%,100%,.06)"}`,
                borderRadius: 8, padding: "5px 12px",
                fontSize: 11, color: isOpen ? "#c5d44b" : "#888",
                cursor: "pointer", display: "flex", alignItems: "center", gap: 5,
                transition: "all 0.2s",
              }}
            >
              {dim}
              <span style={{ fontSize: 8, marginLeft: 2 }}>{isOpen ? "\u25B2" : "\u25BC"}</span>
            </button>
            {isOpen && (
              <div style={{
                position: "absolute", top: "calc(100% + 4px)", left: 0, zIndex: 50,
                background: "#1e1e21", border: "1px solid hsla(0,0%,100%,.08)",
                borderRadius: 10, padding: "8px 0", minWidth: 160,
                boxShadow: "0 12px 40px rgba(0,0,0,.6)",
              }}>
                {OPTIONS[dim].map(opt => (
                  <div key={opt} style={{
                    padding: "7px 14px", fontSize: 11, color: "#ccc",
                    cursor: "pointer", transition: "background 0.15s",
                  }}
                    onMouseEnter={e => e.currentTarget.style.background = "rgba(197,212,75,.08)"}
                    onMouseLeave={e => e.currentTarget.style.background = "transparent"}
                    onClick={() => setActive(prev => ({ ...prev, [dim]: false }))}
                  >{opt}</div>
                ))}
              </div>
            )}
          </div>
        );
      })}
      <button style={{
        background: "transparent", border: "1px solid hsla(0,0%,100%,.04)",
        borderRadius: 8, padding: "5px 10px", fontSize: 10,
        color: "#555", cursor: "pointer",
      }}>Clear all</button>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   TABLE BUILDER — stub for custom table creation
   ═══════════════════════════════════════════════════════════════ */

const AVAILABLE_COLUMNS = [
  { id: "customer_name", label: "Customer", table: "customer_dim" },
  { id: "product_name", label: "Product", table: "product_dim" },
  { id: "region", label: "Region", table: "customer_dim" },
  { id: "segment", label: "Segment", table: "customer_dim" },
  { id: "revenue", label: "Revenue", table: "transaction_fact" },
  { id: "margin_pct", label: "Margin %", table: "transaction_fact" },
  { id: "discount_pct", label: "Discount %", table: "transaction_fact" },
  { id: "list_price", label: "List Price", table: "transaction_fact" },
  { id: "invoice_price", label: "Invoice Price", table: "transaction_fact" },
  { id: "contract_start", label: "Contract Start", table: "contract_dim" },
  { id: "contract_end", label: "Contract End", table: "contract_dim" },
  { id: "escalation_rate", label: "Escalation Rate", table: "contract_dim" },
  { id: "sku", label: "SKU Code", table: "product_dim" },
  { id: "category", label: "Product Category", table: "product_dim" },
  { id: "rep_name", label: "Sales Rep", table: "sales_rep_dim" },
];

const SAMPLE_ROWS = [
  { customer_name: "Enterprise A", product_name: "Fleet Mgmt SaaS", region: "Northeast", revenue: "$4,200K", margin_pct: "42%", discount_pct: "8%" },
  { customer_name: "Enterprise B", product_name: "Fuel Premium", region: "West Coast", revenue: "$3,800K", margin_pct: "38%", discount_pct: "12%" },
  { customer_name: "Mid-Market C", product_name: "Analytics Pro", region: "Midwest", revenue: "$1,900K", margin_pct: "31%", discount_pct: "15%" },
  { customer_name: "Mid-Market D", product_name: "Logistics API", region: "Southeast", revenue: "$1,500K", margin_pct: "28%", discount_pct: "18%" },
  { customer_name: "SMB F", product_name: "Fleet Cards", region: "Southwest", revenue: "$800K", margin_pct: "22%", discount_pct: "20%" },
];

function TableBuilderView() {
  const [selectedCols, setSelectedCols] = useState(["customer_name", "product_name", "region", "revenue", "margin_pct"]);
  const [searchTerm, setSearchTerm] = useState("");

  const toggleCol = (colId) => {
    setSelectedCols(prev =>
      prev.includes(colId) ? prev.filter(c => c !== colId) : [...prev, colId]
    );
  };

  const filteredCols = AVAILABLE_COLUMNS.filter(c =>
    c.label.toLowerCase().includes(searchTerm.toLowerCase()) ||
    c.table.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div>
      <div style={{ display: "flex", gap: 16 }}>
        {/* Column picker */}
        <div style={{
          width: 260, flexShrink: 0,
          background: "#1e1e21", borderRadius: 14, padding: 20,
          border: "1px solid hsla(0,0%,100%,.03)",
          boxShadow: "5px 5px 12px rgba(0,0,0,.55), -5px -5px 12px hsla(0,0%,100%,.04)",
        }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: "#f0f0f2", marginBottom: 12 }}>Columns</div>
          <input
            placeholder="Search columns..."
            value={searchTerm}
            onChange={e => setSearchTerm(e.target.value)}
            style={{
              width: "100%", boxSizing: "border-box",
              background: "#222225", border: "1px solid hsla(0,0%,100%,.08)",
              borderRadius: 8, padding: "8px 12px", fontSize: 12,
              color: "#f0f0f2", outline: "none", marginBottom: 12,
            }}
          />
          <div style={{ maxHeight: 400, overflowY: "auto" }}>
            {filteredCols.map(col => {
              const sel = selectedCols.includes(col.id);
              return (
                <div key={col.id}
                  onClick={() => toggleCol(col.id)}
                  style={{
                    display: "flex", alignItems: "center", gap: 10,
                    padding: "8px 10px", borderRadius: 8,
                    cursor: "pointer", marginBottom: 2,
                    background: sel ? "rgba(197,212,75,.08)" : "transparent",
                    transition: "background 0.15s",
                  }}
                >
                  <div style={{
                    width: 16, height: 16, borderRadius: 4, flexShrink: 0,
                    border: `1.5px solid ${sel ? "#c5d44b" : "hsla(0,0%,100%,.15)"}`,
                    background: sel ? "rgba(197,212,75,.25)" : "transparent",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: 10, color: "#c5d44b",
                  }}>{sel ? "\u2713" : ""}</div>
                  <div>
                    <div style={{ fontSize: 12, color: sel ? "#f0f0f2" : "#888" }}>{col.label}</div>
                    <div style={{ fontSize: 9, color: "#555" }}>{col.table}</div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Preview table */}
        <div style={{
          flex: 1, background: "#1e1e21", borderRadius: 14, padding: 20,
          border: "1px solid hsla(0,0%,100%,.03)",
          boxShadow: "5px 5px 12px rgba(0,0,0,.55), -5px -5px 12px hsla(0,0%,100%,.04)",
          overflow: "auto",
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
            <div>
              <div style={{ fontSize: 13, fontWeight: 700, color: "#f0f0f2" }}>Preview</div>
              <div style={{ fontSize: 11, color: "#888" }}>{selectedCols.length} columns selected · {SAMPLE_ROWS.length} sample rows</div>
            </div>
            <div style={{ display: "flex", gap: 8 }}>
              <button style={{
                background: "#222225", border: "1px solid hsla(0,0%,100%,.06)",
                borderRadius: 8, padding: "6px 14px", fontSize: 11,
                color: "#888", cursor: "pointer",
              }}>Export CSV</button>
              <button style={{
                background: "linear-gradient(135deg, #c5d44b, #3e8c7f)",
                border: "none", borderRadius: 8, padding: "6px 14px",
                fontSize: 11, fontWeight: 600, color: "#161618", cursor: "pointer",
                boxShadow: "0 0 15px rgba(197,212,75,.2)",
              }}>Build Table</button>
            </div>
          </div>

          {selectedCols.length === 0 ? (
            <div style={{ textAlign: "center", padding: "60px 20px", color: "#555", fontSize: 13 }}>
              Select columns from the left panel to build your table
            </div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                <thead>
                  <tr>
                    {selectedCols.map(colId => {
                      const col = AVAILABLE_COLUMNS.find(c => c.id === colId);
                      return (
                        <th key={colId} style={{
                          textAlign: "left", padding: "10px 14px",
                          borderBottom: "1px solid hsla(0,0%,100%,.06)",
                          color: "#888", fontWeight: 600, fontSize: 10,
                          textTransform: "uppercase", letterSpacing: 0.5,
                          whiteSpace: "nowrap",
                        }}>{col?.label || colId}</th>
                      );
                    })}
                  </tr>
                </thead>
                <tbody>
                  {SAMPLE_ROWS.map((row, ri) => (
                    <tr key={ri}>
                      {selectedCols.map(colId => (
                        <td key={colId} style={{
                          padding: "10px 14px",
                          borderBottom: "1px solid hsla(0,0%,100%,.02)",
                          color: "#ccc", whiteSpace: "nowrap",
                        }}>{row[colId] || "—"}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <div style={{
            marginTop: 16, padding: "12px 16px",
            background: "rgba(62,140,127,.08)", borderRadius: 10,
            border: "1px solid rgba(62,140,127,.15)", fontSize: 11, color: "#888",
          }}>
            This is a preview with sample data. Connect to your Gold layer to see real results.
            Custom tables can be saved and shared with your team.
          </div>
        </div>
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   MAIN APP
   ═══════════════════════════════════════════════════════════════ */

export default function App({ clientName, userRole, userName, onBack, onLogout }) {
  const [view, setView] = useState("dashboard");
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const displayClient = clientName ? { ...CLIENT, name: clientName } : CLIENT;

  const viewMap = {
    dashboard: { title: "Executive Dashboard", component: <ExecutiveDashboard /> },
    "01": { title: "Pocket Price Waterfall", component: <PocketPriceWaterfallView /> },
    "04": { title: "Customer Profitability", component: <CustomerProfitabilityView /> },
    "08": { title: "Deal Scorecard", component: <DealScorecardView /> },
    "11": { title: "SKU Pareto", component: <SkuParetoView /> },
    "13": { title: "Churn Risk", component: <ChurnRiskView /> },
    "14": { title: "Geographic Pricing", component: <GeoPricingView /> },
    opportunities: { title: "Opportunities", component: <OpportunitiesView /> },
    tablebuilder: { title: "Custom Table Builder", component: <TableBuilderView /> },
  };

  const current = viewMap[view] || viewMap.dashboard;

  return (
    <div style={{ display: "flex", minHeight: "100vh", background: "#161618", color: "#f0f0f2", fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif" }}>
      {/* Sidebar */}
      <div style={{
        width: sidebarOpen ? 260 : 60,
        background: "#1e1e21",
        borderRight: "1px solid hsla(0,0%,100%,.03)",
        transition: "width 0.3s",
        overflow: "hidden",
        flexShrink: 0,
        display: "flex",
        flexDirection: "column",
        boxShadow: "6px 0 20px rgba(0,0,0,.3)",
      }}>
        {/* Logo */}
        <div style={{ padding: "20px 16px", borderBottom: "1px solid hsla(0,0%,100%,.03)", display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8, flexShrink: 0,
            background: "linear-gradient(135deg, #c5d44b, #3e8c7f)",
            display: "flex", alignItems: "center", justifyContent: "center",
            fontWeight: 900, fontSize: 14, color: "#161618",
            boxShadow: "0 0 15px rgba(197,212,75,.3)",
          }}>F</div>
          {sidebarOpen && (
            <div>
              <div style={{ fontSize: 14, fontWeight: 800, color: "#f0f0f2" }}>FintastIQ</div>
              <div style={{ fontSize: 10, color: "#888" }}>Pricing Diagnostic</div>
            </div>
          )}
        </div>

        {/* Client selector */}
        {sidebarOpen && (
          <div style={{ padding: "12px 16px", borderBottom: "1px solid hsla(0,0%,100%,.03)" }}>
            <div style={{ fontSize: 10, color: "#888", marginBottom: 4, textTransform: "uppercase" }}>Client</div>
            <div style={{
              background: "#222225", borderRadius: 8, padding: "8px 12px",
              border: "1px solid hsla(0,0%,100%,.03)", fontSize: 12,
              color: "#f0f0f2", fontWeight: 600,
            }}>
              {displayClient.name}
              <span style={{ fontSize: 10, color: "#888", marginLeft: 8 }}>{displayClient.tier}</span>
            </div>
          </div>
        )}

        {/* Nav items */}
        <div style={{ flex: 1, overflowY: "auto", padding: "8px 0" }}>
          {/* Dashboard */}
          <div
            onClick={() => setView("dashboard")}
            style={{
              padding: "10px 16px",
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              gap: 10,
              background: view === "dashboard" ? "rgba(197,212,75,.1)" : "transparent",
              borderLeft: view === "dashboard" ? "3px solid #c5d44b" : "3px solid transparent",
              transition: "all 0.2s",
            }}
          >
            <span style={{ fontSize: 16, width: 24, textAlign: "center" }}>{"\u25A3"}</span>
            {sidebarOpen && <span style={{ fontSize: 12, fontWeight: 600, color: view === "dashboard" ? "#f0f0f2" : "#888" }}>Executive Dashboard</span>}
          </div>

          {/* Opportunities */}
          <div
            onClick={() => setView("opportunities")}
            style={{
              padding: "10px 16px",
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              gap: 10,
              background: view === "opportunities" ? "rgba(0,168,107,.1)" : "transparent",
              borderLeft: view === "opportunities" ? "3px solid #00a86b" : "3px solid transparent",
              transition: "all 0.2s",
            }}
          >
            <span style={{ fontSize: 16, width: 24, textAlign: "center" }}>{"\u2605"}</span>
            {sidebarOpen && <span style={{ fontSize: 12, fontWeight: 600, color: view === "opportunities" ? "#f0f0f2" : "#888" }}>Opportunities</span>}
          </div>

          {/* Table Builder */}
          <div
            onClick={() => setView("tablebuilder")}
            style={{
              padding: "10px 16px",
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              gap: 10,
              background: view === "tablebuilder" ? "rgba(62,140,127,.1)" : "transparent",
              borderLeft: view === "tablebuilder" ? "3px solid #3e8c7f" : "3px solid transparent",
              transition: "all 0.2s",
            }}
          >
            <span style={{ fontSize: 16, width: 24, textAlign: "center" }}>{"\u2637"}</span>
            {sidebarOpen && <span style={{ fontSize: 12, fontWeight: 600, color: view === "tablebuilder" ? "#f0f0f2" : "#888" }}>Table Builder</span>}
          </div>

          {sidebarOpen && <div style={{ padding: "12px 16px 4px", fontSize: 10, color: "#666", textTransform: "uppercase", letterSpacing: 1 }}>Analyses</div>}

          {ANALYSIS_LIST.map(a => {
            const hasView = viewMap[a.id];
            const isActive = view === a.id;
            return (
              <div key={a.id}
                onClick={() => hasView && setView(a.id)}
                style={{
                  padding: "7px 16px",
                  cursor: hasView ? "pointer" : "default",
                  display: "flex",
                  alignItems: "center",
                  gap: 10,
                  background: isActive ? "rgba(62,140,127,.1)" : "transparent",
                  borderLeft: isActive ? "3px solid #3e8c7f" : "3px solid transparent",
                  opacity: hasView ? 1 : 0.4,
                  transition: "all 0.2s",
                }}
              >
                <span style={{ fontSize: 12, width: 24, textAlign: "center", color: isActive ? "#3e8c7f" : "#666" }}>{a.icon}</span>
                {sidebarOpen && (
                  <span style={{ fontSize: 11, color: isActive ? "#f0f0f2" : "#888" }}>
                    <span style={{ color: "#666", marginRight: 4 }}>{a.id}.</span>{a.name}
                  </span>
                )}
              </div>
            );
          })}
        </div>

        {/* Footer */}
        {sidebarOpen && (
          <div style={{ padding: "12px 16px", borderTop: "1px solid hsla(0,0%,100%,.03)", fontSize: 10, color: "#666" }}>
            Data Quality: <Badge color="#00a86b">{displayClient.dataQuality}</Badge>
            <div style={{ marginTop: 4 }}>Updated: {displayClient.lastUpdated}</div>
            {userRole === "admin" && onBack && (
              <button onClick={onBack} style={{
                marginTop: 10, width: "100%", background: "transparent",
                border: "1px solid hsla(0,0%,100%,.08)", borderRadius: 7,
                padding: "6px", fontSize: 10, color: "#888", cursor: "pointer",
              }}>← All Clients</button>
            )}
            {onLogout && (
              <button onClick={onLogout} style={{
                marginTop: 6, width: "100%", background: "transparent",
                border: "1px solid hsla(0,0%,100%,.05)", borderRadius: 7,
                padding: "6px", fontSize: 10, color: "#555", cursor: "pointer",
              }}>Sign out</button>
            )}
          </div>
        )}
      </div>

      {/* Main content */}
      <div style={{ flex: 1, overflow: "auto" }}>
        {/* Top bar */}
        <div style={{
          padding: "16px 28px",
          borderBottom: "1px solid hsla(0,0%,100%,.03)",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          background: "#161618",
          boxShadow: "0 2px 8px rgba(0,0,0,.3)",
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <button onClick={() => setSidebarOpen(!sidebarOpen)} style={{
              background: "#1e1e21",
              border: "1px solid hsla(0,0%,100%,.03)",
              borderRadius: 6,
              color: "#888",
              padding: "4px 8px",
              cursor: "pointer",
              fontSize: 14,
              transition: "all 0.2s",
              boxShadow: "4px 4px 10px rgba(0,0,0,.4), -4px -4px 10px hsla(0,0%,100%,.02)",
            }}>{sidebarOpen ? "\u2630" : "\u2192"}</button>
            <h2 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: "#f0f0f2" }}>{current.title}</h2>
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <button style={{
              background: "#1e1e21",
              border: "1px solid hsla(0,0%,100%,.03)",
              borderRadius: 8,
              color: "#888",
              padding: "6px 14px",
              fontSize: 11,
              cursor: "pointer",
              transition: "all 0.2s",
              boxShadow: "4px 4px 10px rgba(0,0,0,.4), -4px -4px 10px hsla(0,0%,100%,.02)",
            }}>Export PPTX</button>
            <button style={{
              background: "linear-gradient(135deg, #c5d44b, #3e8c7f)",
              border: "none",
              borderRadius: 8,
              color: "#161618",
              padding: "6px 14px",
              fontSize: 11,
              fontWeight: 600,
              cursor: "pointer",
              transition: "all 0.2s",
              boxShadow: "0 0 20px rgba(197,212,75,.3)",
            }}>Re-run Analysis</button>
          </div>
        </div>

        {/* Content */}
        <div style={{ padding: 28 }}>
          {/* Filter bar on chart views */}
          {view !== "dashboard" && view !== "opportunities" && view !== "tablebuilder" && (
            <FilterBar />
          )}
          {current.component}
        </div>
      </div>
    </div>
  );
}
