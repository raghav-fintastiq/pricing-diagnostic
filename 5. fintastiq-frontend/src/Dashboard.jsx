import { useState, useContext, createContext } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  ScatterChart, Scatter, ZAxis, Cell,
  LineChart, Line,
  ComposedChart,
  PieChart, Pie,
} from "recharts";
import { useDashboardData } from "./lib/useDashboardData";

/* ═══════════════════════════════════════════════════════════════
   DATA CONTEXT
   ═══════════════════════════════════════════════════════════════ */
const DataCtx = createContext(null);
function useData() { return useContext(DataCtx); }

/* ═══════════════════════════════════════════════════════════════
   HELPERS
   ═══════════════════════════════════════════════════════════════ */
const fmt = (n) =>
  n >= 1_000_000 ? `$${(n / 1_000_000).toFixed(1)}M`
  : n >= 1_000   ? `$${(n / 1_000).toFixed(0)}K`
  : `$${n}`;

const fmtK = (n) => fmt(n * 1_000); // revenue-trend values are already in $K

/* ═══════════════════════════════════════════════════════════════
   SHARED UI COMPONENTS
   ═══════════════════════════════════════════════════════════════ */
function MetricCard({ label, value, sub, color = "#c5d44b", trend }) {
  return (
    <div style={{
      background: "#1e1e21", borderRadius: 14, padding: "16px 20px",
      border: "1px solid hsla(0,0%,100%,.03)", flex: "1 1 0",
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
      background: "#1e1e21", borderRadius: 14, padding: 20,
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
      display: "inline-block", padding: "2px 10px", borderRadius: 9999,
      fontSize: 11, fontWeight: 600,
      background: color + "22", color, border: `1px solid ${color}44`,
    }}>{children}</span>
  );
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "#1e1e21", border: "1px solid hsla(0,0%,100%,.06)",
      borderRadius: 8, padding: "8px 12px", fontSize: 11,
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
   ANALYSIS LIST (sidebar navigation)
   ═══════════════════════════════════════════════════════════════ */
const ANALYSIS_LIST = [
  { id: "01", name: "Pocket Price Waterfall", icon: "↓" },
  { id: "02", name: "Margin Bridge",          icon: "↔" },
  { id: "03", name: "Price-Volume-Mix",       icon: "≡" },
  { id: "04", name: "Customer Profitability", icon: "◎" },
  { id: "05", name: "Discount Effectiveness", icon: "◢" },
  { id: "06", name: "Price Elasticity",       icon: "∿" },
  { id: "07", name: "Contract Leakage",       icon: "⚠" },
  { id: "08", name: "Deal Scorecard",         icon: "★" },
  { id: "09", name: "Competitive Position",   icon: "⚔" },
  { id: "10", name: "Price Corridor",         icon: "═" },
  { id: "11", name: "SKU Pareto",             icon: "▤" },
  { id: "12", name: "New Product Pricing",    icon: "✦" },
  { id: "13", name: "Churn Risk",             icon: "▼" },
  { id: "14", name: "Geographic Pricing",     icon: "◉" },
  { id: "15", name: "Channel Conflict",       icon: "⇄" },
  { id: "16", name: "Promotion ROI",          icon: "☆" },
  { id: "17", name: "Cost-to-Serve",          icon: "▣" },
  { id: "18", name: "Rebate Optimization",    icon: "↺" },
  { id: "19", name: "Annual Escalation",      icon: "↑" },
  { id: "20", name: "Win/Loss",               icon: "✓" },
];

/* ═══════════════════════════════════════════════════════════════
   VIEW: EXECUTIVE DASHBOARD
   ═══════════════════════════════════════════════════════════════ */
function ExecutiveDashboard() {
  const { summary, revenueTrend, opportunities } = useData();
  if (!summary) return null;
  const s = summary;
  return (
    <div>
      <div style={{ display: "flex", gap: 12, marginBottom: 20 }}>
        <MetricCard label="Total Revenue"    value={fmt(s.totalRevenue)}       sub="Recognized"                  color="#c5d44b" />
        <MetricCard label="Avg Margin"       value={`${s.avgMargin.toFixed(1)}%`} sub="Gross pocket margin"       color="#00a86b" />
        <MetricCard label="Revenue at Risk"  value={fmt(s.revenueAtRisk * 1000)} sub="High churn customers" trend="down" color="#ef5350" />
        <MetricCard label="Opportunity Value" value={fmt(s.opportunityValue)}   sub={`${opportunities.length} identified`} trend="up" color="#c5d44b" />
      </div>
      <div style={{ display: "flex", gap: 12, marginBottom: 20 }}>
        <MetricCard label="Active Customers"    value={s.totalCustomers.toLocaleString()} color="#3e8c7f" />
        <MetricCard label="Active Products"     value={s.activeProducts}                   color="#7ea05e" />
        <MetricCard label="Avg Discount"        value={`${s.avgDiscount.toFixed(1)}%`}     sub="Target: 15%" trend="down" color="#3b5068" />
        <MetricCard label="Total Price Leakage" value={fmt(s.totalLeakage)}                sub="Off-invoice" trend="down" color="#ef5350" />
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 16 }}>
        <ChartCard title="Revenue & Margin Trend" sub="Monthly revenue ($K) with margin % overlay">
          <ResponsiveContainer width="100%" height={260}>
            <ComposedChart data={revenueTrend}>
              <CartesianGrid strokeDasharray="3 3" stroke="#333" />
              <XAxis dataKey="month" tick={{ fill: "#888", fontSize: 11 }} />
              <YAxis yAxisId="left"  tick={{ fill: "#888", fontSize: 11 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: "#888", fontSize: 11 }} domain={[60, 85]} />
              <Tooltip content={<CustomTooltip />} />
              <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
              <Bar  yAxisId="left"  dataKey="revenue" name="Revenue ($K)"  fill="#c5d44b" radius={[4,4,0,0]} />
              <Line yAxisId="right" type="monotone" dataKey="margin" name="Margin %"
                stroke="#00a86b" strokeWidth={2} dot={{ fill: "#00a86b", r: 3 }} />
            </ComposedChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Top Opportunities" sub="Ranked by revenue impact">
          <div style={{ maxHeight: 260, overflowY: "auto" }}>
            {opportunities.slice(0, 5).map(o => (
              <div key={o.id} style={{
                padding: "10px 12px", borderRadius: 8, marginBottom: 6,
                background: "rgba(197,212,75,.08)", border: "1px solid rgba(197,212,75,.15)",
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

/* ═══════════════════════════════════════════════════════════════
   VIEW: POCKET PRICE WATERFALL
   ═══════════════════════════════════════════════════════════════ */
function PocketPriceWaterfallView() {
  const { waterfall, summary } = useData();
  if (!waterfall?.length) return null;
  const invoice = waterfall.find(d => d.name === "Invoice Price");
  const margin  = waterfall.find(d => d.name === "Pocket Margin");
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12, marginBottom: 4 }}>
        <MetricCard label="List Price (Index)"  value="100.0"                                   color="#c5d44b" />
        <MetricCard label="Invoice Price Index" value={invoice ? invoice.height.toFixed(1) : "—"}
          sub={invoice ? `-${(100 - invoice.height).toFixed(1)}% leakage` : ""} trend="down" color="#3e8c7f" />
        <MetricCard label="Pocket Margin Index" value={margin ? `${margin.height.toFixed(1)}` : "—"} color="#00a86b" />
        <MetricCard label="Total Price Leakage" value={fmt(summary?.totalLeakage || 0)} sub="Annualized" trend="down" color="#ef5350" />
      </div>
      <ChartCard title="Pocket Price Waterfall" sub="Price cascade from list to pocket margin (indexed to 100)">
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={waterfall} barSize={40}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" vertical={false} />
            <XAxis dataKey="name" tick={{ fill: "#888", fontSize: 10 }} angle={-20} textAnchor="end" height={60} />
            <YAxis tick={{ fill: "#888", fontSize: 11 }} domain={[0, 110]} />
            <Tooltip content={<CustomTooltip />} />
            <Bar dataKey="base"   stackId="a" fill="transparent" />
            <Bar dataKey="height" stackId="a" name="Index Value" radius={[4,4,0,0]}>
              {waterfall.map((entry, i) => <Cell key={i} fill={entry.fill} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>
      <ChartCard title="Key Insight">
        <p style={{ fontSize: 12, color: "#d0d0d0", lineHeight: 1.7, margin: 0 }}>
          The largest leakage drivers are <strong style={{ color: "#ef5350" }}>Volume Discounts
          ({waterfall.find(d => d.name === "Vol. Discount")?.height?.toFixed(1) || "—"}%)</strong> and{" "}
          <strong style={{ color: "#ef5350" }}>Rebates
          ({waterfall.find(d => d.name === "Rebates")?.height?.toFixed(1) || "—"}%)</strong>.{" "}
          Combined off-invoice adjustments erode{" "}
          <strong style={{ color: "#ef5350" }}>{(100 - (invoice?.height || 83)).toFixed(1)}%</strong> of list price.
          Tightening discount thresholds and enforcing rebate claim deadlines could
          recover an estimated <strong style={{ color: "#00a86b" }}>{fmt((summary?.totalLeakage || 0) * 0.15)}</strong> annually.
        </p>
      </ChartCard>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   VIEW: CUSTOMER PROFITABILITY
   ═══════════════════════════════════════════════════════════════ */
const segmentColors = { Enterprise: "#3e8c7f", "Mid-Market": "#c5d44b", SMB: "#3b5068", "Long Tail": "#ef5350" };

function CustomerProfitabilityView() {
  const { custProfit } = useData();
  if (!custProfit?.length) return null;
  const profitable   = custProfit.filter(c => c.margin >= 0).length;
  const pct          = Math.round((profitable / custProfit.length) * 100);
  const topMargin    = Math.max(...custProfit.map(c => c.margin));
  const bottomMargin = Math.min(...custProfit.map(c => c.margin));
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12 }}>
        <MetricCard label="Profitable Customers" value={`${pct}%`}                    color="#00a86b" />
        <MetricCard label="Unprofitable"          value={`${100 - pct}%`}             trend="down" color="#ef5350" />
        <MetricCard label="Top Decile Margin"     value={`${topMargin.toFixed(1)}%`}  color="#3e8c7f" />
        <MetricCard label="Bottom Decile Margin"  value={`${bottomMargin.toFixed(1)}%`} color="#3b5068" />
      </div>
      <ChartCard title="Customer Profitability Map" sub="Revenue ($K) vs Margin (%) — bubble size = transaction volume">
        <ResponsiveContainer width="100%" height={340}>
          <ScatterChart>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="revenue" name="Revenue ($K)" tick={{ fill: "#888", fontSize: 11 }}
              label={{ value: "Revenue ($K)", position: "bottom", fill: "#888", fontSize: 11, offset: -5 }} />
            <YAxis dataKey="margin" name="Margin %" tick={{ fill: "#888", fontSize: 11 }}
              label={{ value: "Margin %", angle: -90, position: "insideLeft", fill: "#888", fontSize: 11 }} />
            <ZAxis dataKey="size" range={[40, 400]} />
            <Tooltip content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const d = payload[0].payload;
              return (
                <div style={{ background: "#1e1e21", border: "1px solid hsla(0,0%,100%,.06)", borderRadius: 8, padding: "8px 12px", fontSize: 11 }}>
                  <div style={{ fontWeight: 700, color: "#f0f0f2" }}>{d.name}</div>
                  <div style={{ color: "#888" }}>Revenue: ${d.revenue}K · Margin: {d.margin}%</div>
                  <div style={{ color: "#888" }}>Segment: {d.segment}</div>
                </div>
              );
            }} />
            <Scatter data={custProfit}>
              {custProfit.map((d, i) => (
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

/* ═══════════════════════════════════════════════════════════════
   VIEW: DEAL SCORECARD
   ═══════════════════════════════════════════════════════════════ */
function DealScorecardView() {
  const { deals } = useData();
  if (!deals?.length) return null;
  const avgWinRate   = (deals.reduce((s, d) => s + d.winRate, 0) / deals.length).toFixed(1);
  const avgDiscount  = (deals.reduce((s, d) => s + d.avgDiscount, 0) / deals.length).toFixed(1);
  const totalRev     = deals.reduce((s, d) => s + d.revenue, 0);
  const topRep       = deals.reduce((a, b) => a.revenue > b.revenue ? a : b);
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12 }}>
        <MetricCard label="Avg Win Rate"     value={`${avgWinRate}%`}       color="#00a86b" />
        <MetricCard label="Avg Discount"     value={`${avgDiscount}%`}      color="#3b5068" />
        <MetricCard label="Total Pipeline"   value={fmt(totalRev)}          color="#c5d44b" />
        <MetricCard label="Top Rep Revenue"  value={fmt(topRep.revenue)}    sub={topRep.rep} color="#3e8c7f" />
      </div>
      <ChartCard title="Sales Rep Scorecard" sub="Win rate vs average discount by rep">
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={deals} barGap={4}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="rep" tick={{ fill: "#888", fontSize: 11 }} />
            <YAxis tick={{ fill: "#888", fontSize: 11 }} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
            <Bar dataKey="winRate"     name="Win Rate %"    fill="#00a86b" radius={[4,4,0,0]} />
            <Bar dataKey="avgDiscount" name="Avg Discount %" fill="#ef5350" radius={[4,4,0,0]} />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>
      <ChartCard title="Key Insight">
        {(() => {
          const best  = deals.reduce((a, b) => a.winRate > b.winRate ? a : b);
          const worst = deals.reduce((a, b) => a.winRate < b.winRate ? a : b);
          return (
            <p style={{ fontSize: 12, color: "#d0d0d0", lineHeight: 1.7, margin: 0 }}>
              <strong style={{ color: "#00a86b" }}>{best.rep}</strong> leads with {best.winRate}% win rate
              and {best.avgDiscount}% avg discount, proving premium pricing is achievable.{" "}
              <strong style={{ color: "#ef5350" }}>{worst.rep}</strong> has the lowest win rate ({worst.winRate}%){" "}
              — focused coaching on value-based selling is recommended.
            </p>
          );
        })()}
      </ChartCard>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   VIEW: SKU PARETO
   ═══════════════════════════════════════════════════════════════ */
function SkuParetoView() {
  const { skuPareto } = useData();
  if (!skuPareto?.length) return null;
  const top80  = skuPareto.filter(s => s.cumPct <= 80).length;
  const tail   = skuPareto.length - top80;
  const topSku = skuPareto[0];
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12 }}>
        <MetricCard label={`Top ${top80} SKUs`}   value={`${top80} products`}   sub="~80% of revenue"     color="#c5d44b" />
        <MetricCard label="Long Tail"              value={`${tail} SKUs`}        sub="remaining revenue"   color="#3b5068" />
        <MetricCard label="Highest Revenue SKU"   value={fmtK(topSku.revenue)}  sub={topSku.sku}          color="#00a86b" />
      </div>
      <ChartCard title="SKU Revenue Pareto" sub="Revenue ($K) with cumulative % line">
        <ResponsiveContainer width="100%" height={320}>
          <ComposedChart data={skuPareto}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="sku" tick={{ fill: "#888", fontSize: 9 }} angle={-15} textAnchor="end" height={60} />
            <YAxis yAxisId="left"  tick={{ fill: "#888", fontSize: 11 }} />
            <YAxis yAxisId="right" orientation="right" tick={{ fill: "#888", fontSize: 11 }} domain={[0, 105]} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
            <Bar  yAxisId="left"  dataKey="revenue" name="Revenue ($K)" fill="#c5d44b" radius={[4,4,0,0]} />
            <Line yAxisId="right" type="monotone" dataKey="cumPct" name="Cumulative %" stroke="#7ea05e" strokeWidth={2} dot={{ fill: "#7ea05e", r: 3 }} />
          </ComposedChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   VIEW: CHURN RISK
   ═══════════════════════════════════════════════════════════════ */
function ChurnRiskView() {
  const { churn } = useData();
  if (!churn?.length) return null;
  const highRisk  = churn.filter(c => c.riskScore > 70);
  const lowRisk   = churn.filter(c => c.riskScore < 30);
  const atRisk    = highRisk.reduce((s, c) => s + c.revenue, 0);
  const secured   = lowRisk.reduce((s, c) => s + c.revenue, 0);
  const avgScore  = (churn.reduce((s, c) => s + c.riskScore, 0) / churn.length).toFixed(1);
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12 }}>
        <MetricCard label="High Risk (>70)"  value={`${highRisk.length} customers`}    color="#ef5350" />
        <MetricCard label="Revenue at Risk"  value={fmtK(atRisk)}   sub="High risk accounts" color="#ef5350" />
        <MetricCard label="Avg Risk Score"   value={avgScore}                            color="#3b5068" />
        <MetricCard label="Low Risk (<30)"   value={`${lowRisk.length} customers`} sub={`${fmtK(secured)} secured`} color="#00a86b" />
      </div>
      <ChartCard title="Churn Risk Map" sub="Revenue ($K) vs Risk Score — high risk = top right">
        <ResponsiveContainer width="100%" height={320}>
          <ScatterChart>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="revenue" name="Revenue ($K)" tick={{ fill: "#888", fontSize: 11 }}
              label={{ value: "Revenue ($K)", position: "bottom", fill: "#888", fontSize: 11, offset: -5 }} />
            <YAxis dataKey="riskScore" name="Risk Score" tick={{ fill: "#888", fontSize: 11 }} domain={[0, 100]}
              label={{ value: "Risk Score", angle: -90, position: "insideLeft", fill: "#888", fontSize: 11 }} />
            <Tooltip content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const d = payload[0].payload;
              return (
                <div style={{ background: "#1e1e21", border: "1px solid hsla(0,0%,100%,.06)", borderRadius: 8, padding: "8px 12px", fontSize: 11 }}>
                  <div style={{ fontWeight: 700, color: "#f0f0f2" }}>{d.name}</div>
                  <div style={{ color: "#888" }}>Revenue: ${d.revenue}K · Risk: {d.riskScore}</div>
                  <div style={{ color: "#888" }}>Trend: {d.trend}</div>
                </div>
              );
            }} />
            <Scatter data={churn}>
              {churn.map((d, i) => (
                <Cell key={i} fill={d.riskScore > 70 ? "#ef5350" : d.riskScore > 50 ? "#3b5068" : "#00a86b"} fillOpacity={0.85} />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   VIEW: GEOGRAPHIC PRICING
   ═══════════════════════════════════════════════════════════════ */
function GeoPricingView() {
  const { geo } = useData();
  if (!geo?.length) return null;
  const national    = geo[0]?.national || 0;
  const highest     = geo.reduce((a, b) => a.avgPrice > b.avgPrice ? a : b);
  const lowest      = geo.reduce((a, b) => a.avgPrice < b.avgPrice ? a : b);
  const maxVariance = (highest.variance - lowest.variance).toFixed(1);
  const fmtPrice    = (n) => n >= 1000 ? `$${(n/1000).toFixed(1)}K` : `$${n.toFixed(2)}`;
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
      <div style={{ display: "flex", gap: 12 }}>
        <MetricCard label="National Avg Price"  value={fmtPrice(national)}                                  color="#c5d44b" />
        <MetricCard label="Highest Region"      value={fmtPrice(highest.avgPrice)} sub={`${highest.region} (+${highest.variance}%)`} color="#ef5350" />
        <MetricCard label="Lowest Region"       value={fmtPrice(lowest.avgPrice)}  sub={`${lowest.region} (${lowest.variance}%)`}    color="#00a86b" />
        <MetricCard label="Max Variance"        value={`${maxVariance}%`}          sub="Highest vs lowest region"                     color="#3e8c7f" />
      </div>
      <ChartCard title="Regional Pricing vs National Average" sub="Average realized price by region with national benchmark">
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={geo}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="region" tick={{ fill: "#888", fontSize: 11 }} />
            <YAxis tick={{ fill: "#888", fontSize: 11 }} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
            <Bar  dataKey="avgPrice" name="Avg Price ($)"  fill="#c5d44b" radius={[4,4,0,0]} />
            <Line type="monotone" dataKey="national" name="National Avg" stroke="#3b5068" strokeWidth={2} strokeDasharray="8 4" dot={false} />
          </ComposedChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   VIEW: OPPORTUNITIES
   ═══════════════════════════════════════════════════════════════ */
const CAPTURE_PRESETS = [
  { label: "Conservative", pct: 0.30, desc: "Early-stage pricing maturity, limited change management capacity" },
  { label: "Moderate",     pct: 0.50, desc: "Some pricing processes in place, management aligned" },
  { label: "Ambitious",    pct: 0.70, desc: "Strong pricing governance, executive buy-in, dedicated team" },
];

function OpportunitiesView() {
  const { opportunities } = useData();
  const [captureIdx, setCaptureIdx] = useState(1);
  if (!opportunities?.length) return null;
  const preset      = CAPTURE_PRESETS[captureIdx];
  const rawTotal    = opportunities.reduce((s, o) => s + o.impactRaw, 0);
  const captured    = rawTotal * preset.pct;
  const highConf    = opportunities.filter(o => o.confidence === "High").length;
  const highConfVal = opportunities.filter(o => o.confidence === "High").reduce((s, o) => s + o.impactRaw, 0);

  return (
    <div>
      <div style={{ display: "flex", gap: 12, marginBottom: 20 }}>
        <MetricCard label="Total Opportunities"  value={opportunities.length}          color="#c5d44b" />
        <MetricCard label="Full Opportunity"      value={fmt(rawTotal)}  sub="100% capture" color="#3b5068" />
        <MetricCard label="Realistic Capture"     value={fmt(captured)}  sub={`${preset.label} (${Math.round(preset.pct * 100)}%)`} color="#00a86b" />
        <MetricCard label="High Confidence"       value={highConf}       sub={`${fmt(highConfVal)} impact`} color="#00a86b" />
      </div>

      {/* Capture rate slider */}
      <div style={{
        background: "#1e1e21", borderRadius: 14, padding: "20px 24px",
        border: "1px solid rgba(197,212,75,.15)", marginBottom: 20,
        boxShadow: "5px 5px 12px rgba(0,0,0,.55), -5px -5px 12px hsla(0,0%,100%,.04)",
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 14 }}>
          <div>
            <div style={{ fontSize: 13, fontWeight: 700, color: "#f0f0f2", marginBottom: 2 }}>Capture Rate</div>
            <div style={{ fontSize: 11, color: "#888" }}>What % of the opportunity can your organisation realistically capture based on pricing maturity?</div>
          </div>
          <div style={{ textAlign: "right" }}>
            <div style={{ fontSize: 22, fontWeight: 800, color: "#00a86b" }}>{fmt(captured)}</div>
            <div style={{ fontSize: 10, color: "#666" }}>addressable at {Math.round(preset.pct * 100)}% capture</div>
          </div>
        </div>
        <div style={{ display: "flex", gap: 10, marginBottom: 14 }}>
          {CAPTURE_PRESETS.map((p, i) => (
            <button key={p.label} onClick={() => setCaptureIdx(i)} style={{
              flex: 1, background: captureIdx === i ? "rgba(197,212,75,.15)" : "#222225",
              border: `1px solid ${captureIdx === i ? "rgba(197,212,75,.4)" : "hsla(0,0%,100%,.06)"}`,
              borderRadius: 9, padding: "10px 12px", cursor: "pointer", transition: "all 0.2s", textAlign: "left",
            }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: captureIdx === i ? "#c5d44b" : "#888", marginBottom: 2 }}>{p.label}</div>
              <div style={{ fontSize: 18, fontWeight: 800, color: captureIdx === i ? "#f0f0f2" : "#555" }}>{Math.round(p.pct * 100)}%</div>
            </button>
          ))}
        </div>
        <input type="range" min={0} max={2} step={1} value={captureIdx}
          onChange={e => setCaptureIdx(Number(e.target.value))}
          style={{ width: "100%", accentColor: "#c5d44b", cursor: "pointer" }} />
        <div style={{ fontSize: 11, color: "#666", marginTop: 8 }}>{preset.desc}</div>
        <div style={{ marginTop: 14 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#555", marginBottom: 4 }}>
            <span>$0</span><span>Full opportunity: {fmt(rawTotal)}</span>
          </div>
          <div style={{ height: 10, background: "#222225", borderRadius: 9999, overflow: "hidden" }}>
            <div style={{
              height: "100%", width: `${preset.pct * 100}%`,
              background: "linear-gradient(90deg, #3e8c7f, #c5d44b)",
              borderRadius: 9999, transition: "width 0.4s ease",
            }} />
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#555", marginTop: 4 }}>
            <span style={{ color: "#00a86b", fontWeight: 600 }}>Captured: {fmt(captured)}</span>
            <span>Uncaptured: {fmt(rawTotal - captured)}</span>
          </div>
        </div>
      </div>

      {/* Opportunity list */}
      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {opportunities.map(o => {
          const cap = o.impactRaw * preset.pct;
          return (
            <div key={o.id} style={{
              background: "#1e1e21", borderRadius: 12, padding: "16px 20px",
              border: `1px solid ${o.confidence === "High" ? "rgba(0,168,107,.3)" : o.confidence === "Medium" ? "rgba(197,212,75,.2)" : "rgba(136,136,136,.2)"}`,
              display: "grid", gridTemplateColumns: "40px 1fr 150px 80px",
              gap: 16, alignItems: "center",
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
                <div style={{ fontSize: 16, fontWeight: 800, color: "#00a86b" }}>{fmt(cap)}</div>
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
   10 NEW ANALYSIS CARDS
   ═══════════════════════════════════════════════════════════════ */

// 1. PRICE BAND CARD
function PriceBandCard({ data }) {
  if (!data?.priceBand?.length) return null;
  const bySegment = {};
  data.priceBand.forEach(r => {
    if (!bySegment[r.band]) bySegment[r.band] = { band: r.band, bandOrder: r.bandOrder, entries: [] };
    bySegment[r.band].entries.push(r);
  });
  const chartData = Object.values(bySegment).map(b => ({
    band: b.band,
    Enterprise: b.entries.filter(e => e.segment === "Enterprise").reduce((s, e) => s + e.dealCount, 0),
    "Mid-Market": b.entries.filter(e => e.segment === "Mid-Market").reduce((s, e) => s + e.dealCount, 0),
    SMB: b.entries.filter(e => e.segment === "SMB").reduce((s, e) => s + e.dealCount, 0),
  }));
  const tableData = data.priceBand.slice(0, 5);
  return (
    <ChartCard title="Price Band Analysis" sub="Deal count by segment and avg pricing">
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis dataKey="band" tick={{ fill: "#888", fontSize: 10 }} />
          <YAxis tick={{ fill: "#888", fontSize: 11 }} />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
          <Bar dataKey="Enterprise" fill="#3e8c7f" radius={[4,4,0,0]} />
          <Bar dataKey="Mid-Market" fill="#c5d44b" radius={[4,4,0,0]} />
          <Bar dataKey="SMB" fill="#3b5068" radius={[4,4,0,0]} />
        </BarChart>
      </ResponsiveContainer>
      <div style={{ marginTop: 16 }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: "#f0f0f2", marginBottom: 8 }}>Summary</div>
        <table style={{ width: "100%", fontSize: 11, borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #333" }}>
              <th style={{ textAlign: "left", padding: "6px 8px", color: "#888" }}>Band</th>
              <th style={{ textAlign: "right", padding: "6px 8px", color: "#888" }}>Avg Price</th>
              <th style={{ textAlign: "right", padding: "6px 8px", color: "#888" }}>Discount %</th>
            </tr>
          </thead>
          <tbody>
            {tableData.map(r => (
              <tr key={r.band} style={{ borderBottom: "1px solid #222" }}>
                <td style={{ padding: "6px 8px", color: "#f0f0f2" }}>{r.band}</td>
                <td style={{ textAlign: "right", padding: "6px 8px", color: "#c5d44b" }}>{fmt(r.avgPrice)}</td>
                <td style={{ textAlign: "right", padding: "6px 8px", color: "#ef5350" }}>{r.avgDiscountPct.toFixed(1)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </ChartCard>
  );
}

// 2. WIN/LOSS CARD
function WinLossCard({ data }) {
  if (!data?.winLoss?.length) return null;
  const outcomes = {};
  const lossReasons = {};
  const competitors = {};
  data.winLoss.forEach(r => {
    outcomes[r.outcome] = (outcomes[r.outcome] || 0) + r.dealCount;
    if (r.outcome === "Lost") {
      lossReasons[r.lossReason] = (lossReasons[r.lossReason] || 0) + r.dealCount;
    }
    if (r.competitor !== "None" && r.competitor) {
      competitors[r.competitor] = (competitors[r.competitor] || 0) + 1;
    }
  });
  const pieData = Object.entries(outcomes).map(([k, v]) => ({ name: k, value: v }));
  const lossData = Object.entries(lossReasons).map(([k, v]) => ({ reason: k, count: v }));
  const topCompetitors = Object.entries(competitors).sort((a, b) => b[1] - a[1]).slice(0, 3);
  const colors = ["#00a86b", "#ef5350", "#3b5068"];
  return (
    <ChartCard title="Win/Loss Analysis" sub="Deal outcomes and loss drivers">
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
        <ResponsiveContainer width="100%" height={250}>
          <PieChart>
            <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={70} fill="#888">
              {pieData.map((e, i) => <Cell key={i} fill={colors[i] || "#888"} />)}
            </Pie>
            <Tooltip content={<CustomTooltip />} />
          </PieChart>
        </ResponsiveContainer>
        <ResponsiveContainer width="100%" height={250}>
          <BarChart data={lossData} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis type="number" tick={{ fill: "#888", fontSize: 10 }} />
            <YAxis dataKey="reason" type="category" tick={{ fill: "#888", fontSize: 9 }} width={80} />
            <Bar dataKey="count" fill="#ef5350" radius={[0,4,4,0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div style={{ fontSize: 11, fontWeight: 700, color: "#f0f0f2", marginBottom: 8 }}>Top Competitors</div>
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {topCompetitors.map(([comp, count]) => (
          <div key={comp} style={{ display: "flex", justifyContent: "space-between", fontSize: 11, color: "#888" }}>
            <span>{comp}</span><span style={{ color: "#f0f0f2" }}>{count} losses</span>
          </div>
        ))}
      </div>
    </ChartCard>
  );
}

// 3. DEAL VELOCITY CARD
function DealVelocityCard({ data }) {
  if (!data?.dealVelocity?.length) return null;
  const byBucket = {};
  data.dealVelocity.forEach(r => {
    if (!byBucket[r.cycleBucket]) byBucket[r.cycleBucket] = { bucket: r.cycleBucket, entries: [] };
    byBucket[r.cycleBucket].entries.push(r);
  });
  const chartData = Object.values(byBucket).map(b => ({
    bucket: b.bucket,
    Enterprise: b.entries.filter(e => e.segment === "Enterprise").reduce((s, e) => s + e.dealCount, 0),
    "Mid-Market": b.entries.filter(e => e.segment === "Mid-Market").reduce((s, e) => s + e.dealCount, 0),
    SMB: b.entries.filter(e => e.segment === "SMB").reduce((s, e) => s + e.dealCount, 0),
  }));
  const segmentAvg = {};
  data.dealVelocity.forEach(r => {
    if (!segmentAvg[r.segment]) segmentAvg[r.segment] = { days: 0, count: 0 };
    segmentAvg[r.segment].days += r.avgDays;
    segmentAvg[r.segment].count += 1;
  });
  return (
    <ChartCard title="Deal Velocity by Cycle" sub="Deal count by cycle bucket and segment">
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis dataKey="bucket" tick={{ fill: "#888", fontSize: 10 }} />
          <YAxis tick={{ fill: "#888", fontSize: 11 }} />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
          <Bar dataKey="Enterprise" fill="#3e8c7f" radius={[4,4,0,0]} />
          <Bar dataKey="Mid-Market" fill="#c5d44b" radius={[4,4,0,0]} />
          <Bar dataKey="SMB" fill="#3b5068" radius={[4,4,0,0]} />
        </BarChart>
      </ResponsiveContainer>
      <div style={{ display: "flex", gap: 12, marginTop: 16 }}>
        {Object.entries(segmentAvg).map(([seg, data]) => (
          <div key={seg} style={{ flex: 1, padding: "10px", background: "#222225", borderRadius: 8, fontSize: 11 }}>
            <div style={{ color: "#888", marginBottom: 4 }}>{seg}</div>
            <div style={{ fontSize: 16, fontWeight: 700, color: "#c5d44b" }}>{(data.days / data.count).toFixed(0)} days</div>
          </div>
        ))}
      </div>
    </ChartCard>
  );
}

// 4. DISCOUNT GOVERNANCE CARD
function DiscountGovCard({ data }) {
  if (!data?.discountGov?.length) return null;
  const approvalColors = {
    "Self-Service": "#22c55e",
    "Manager": "#eab308",
    "Director": "#f97316",
    "Executive": "#ef4444",
  };
  const byBand = data.discountGov.reduce((acc, r) => {
    if (!acc[r.band]) acc[r.band] = { band: r.band, dealCount: 0, overrideCount: 0, approval: r.approval };
    acc[r.band].dealCount += r.dealCount;
    acc[r.band].overrideCount += r.overrideCount;
    return acc;
  }, {});
  const chartData = Object.values(byBand);
  const directorPlus = data.discountGov.filter(r => r.approval === "Director" || r.approval === "Executive").reduce((s, r) => s + r.dealCount, 0);
  const totalDeals = data.discountGov.reduce((s, r) => s + r.dealCount, 0);
  const totalOverrides = data.discountGov.reduce((s, r) => s + r.overrideCount, 0);
  return (
    <ChartCard title="Discount Governance" sub="Deal volume by discount band and approval level">
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis dataKey="band" tick={{ fill: "#888", fontSize: 10 }} />
          <YAxis tick={{ fill: "#888", fontSize: 11 }} />
          <Tooltip content={<CustomTooltip />} />
          <Bar dataKey="dealCount" fill="#c5d44b" radius={[4,4,0,0]}>
            {chartData.map((d, i) => <Cell key={i} fill={approvalColors[d.approval] || "#888"} />)}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div style={{ display: "flex", gap: 12, marginTop: 16 }}>
        <div style={{ flex: 1, padding: "10px", background: "#222225", borderRadius: 8, fontSize: 11 }}>
          <div style={{ color: "#888", marginBottom: 4 }}>Total Overrides</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: "#ef5350" }}>{totalOverrides}</div>
        </div>
        <div style={{ flex: 1, padding: "10px", background: "#222225", borderRadius: 8, fontSize: 11 }}>
          <div style={{ color: "#888", marginBottom: 4 }}>Director+ Approval</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: "#eab308" }}>{Math.round((directorPlus / totalDeals) * 100)}%</div>
        </div>
      </div>
    </ChartCard>
  );
}

// 5. COHORT REVENUE CARD (Heatmap)
function CohortRevenueCard({ data }) {
  if (!data?.cohortRevenue?.length) return null;
  const uniqueCohorts = [...new Set(data.cohortRevenue.map(r => r.cohortMonth))].sort().reverse().slice(0, 8);
  const uniqueMonths = [...new Set(data.cohortRevenue.map(r => r.revMonth))].sort();
  const revMap = {};
  data.cohortRevenue.forEach(r => {
    revMap[`${r.cohortMonth}|${r.revMonth}`] = r.revenue;
  });
  const maxRev = Math.max(...data.cohortRevenue.map(r => r.revenue));
  const getColor = (rev) => {
    if (!rev) return "#f0f0f2";
    const intensity = rev / maxRev;
    if (intensity < 0.2) return "#dcfce7";
    if (intensity < 0.4) return "#a7f3d0";
    if (intensity < 0.6) return "#6ee7b7";
    if (intensity < 0.8) return "#34d399";
    return "#059669";
  };
  return (
    <ChartCard title="Cohort Revenue Heatmap" sub="Revenue ($k) by cohort month and age">
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", fontSize: 10, borderCollapse: "collapse", minWidth: "500px" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #333" }}>
              <th style={{ textAlign: "left", padding: "8px", color: "#888" }}>Cohort</th>
              {uniqueMonths.slice(0, 12).map((m, i) => (
                <th key={m} style={{ textAlign: "center", padding: "8px", color: "#888" }}>M{i + 1}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {uniqueCohorts.map(cohort => (
              <tr key={cohort} style={{ borderBottom: "1px solid #222" }}>
                <td style={{ padding: "8px", color: "#f0f0f2", fontWeight: 600 }}>{cohort}</td>
                {uniqueMonths.slice(0, 12).map((m, i) => {
                  const rev = revMap[`${cohort}|${m}`] || 0;
                  return (
                    <td key={m} style={{
                      textAlign: "center", padding: "8px",
                      background: getColor(rev), color: rev > maxRev * 0.5 ? "#000" : "#f0f0f2",
                      fontSize: 9, fontWeight: 600,
                    }}>
                      {rev ? `$${(rev / 1000).toFixed(0)}k` : "-"}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </ChartCard>
  );
}

// 6. REP PERF CARD
function RepPerfCard({ data }) {
  if (!data?.repPerf?.length) return null;
  const sorted = [...data.repPerf].sort((a, b) => b.bookings - a.bookings).slice(0, 10);
  const getAttainColor = (pct) => pct < 80 ? "#ef5350" : pct < 100 ? "#eab308" : "#22c55e";
  return (
    <ChartCard title="Sales Rep Performance" sub="Top 10 reps by bookings">
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", fontSize: 10, borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #333" }}>
              <th style={{ textAlign: "left", padding: "8px", color: "#888" }}>Rep</th>
              <th style={{ textAlign: "center", padding: "8px", color: "#888" }}>Win Rate</th>
              <th style={{ textAlign: "right", padding: "8px", color: "#888" }}>Bookings</th>
              <th style={{ textAlign: "center", padding: "8px", color: "#888" }}>Quota %</th>
              <th style={{ textAlign: "right", padding: "8px", color: "#888" }}>Avg Disc %</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(r => (
              <tr key={r.rep} style={{ borderBottom: "1px solid #222" }}>
                <td style={{ padding: "8px", color: "#f0f0f2" }}>{r.rep}</td>
                <td style={{ textAlign: "center", padding: "8px", color: "#00a86b" }}>{r.winRate.toFixed(0)}%</td>
                <td style={{ textAlign: "right", padding: "8px", color: "#c5d44b" }}>{fmt(r.bookings)}</td>
                <td style={{ textAlign: "center", padding: "8px", color: getAttainColor(r.quotaAttainment) }}>
                  {r.quotaAttainment.toFixed(0)}%
                </td>
                <td style={{ textAlign: "right", padding: "8px", color: "#3b5068" }}>{r.avgDiscount.toFixed(1)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </ChartCard>
  );
}

// 7. DEAL SIZE DISTRIBUTION CARD
function DealSizeCard({ data }) {
  if (!data?.dealSizeDist?.length) return null;
  const byBand = {};
  data.dealSizeDist.forEach(r => {
    if (!byBand[r.band]) byBand[r.band] = { band: r.band, Won: 0, Lost: 0, "No Decision": 0 };
    byBand[r.band][r.outcome] = (byBand[r.band][r.outcome] || 0) + r.dealCount;
  });
  const chartData = Object.values(byBand);
  const largeValue = data.dealSizeDist.filter(r => r.band && r.band.includes("25K") || r.band.includes("50K") || r.band.includes("100K"))
    .reduce((s, r) => s + r.totalValue, 0);
  const totalValue = data.dealSizeDist.reduce((s, r) => s + r.totalValue, 0);
  const largeValuePct = totalValue ? Math.round((largeValue / totalValue) * 100) : 0;
  return (
    <ChartCard title="Deal Size Distribution" sub="Deal count by size and outcome">
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis dataKey="band" tick={{ fill: "#888", fontSize: 10 }} />
          <YAxis tick={{ fill: "#888", fontSize: 11 }} />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
          <Bar dataKey="Won" fill="#00a86b" radius={[4,4,0,0]} />
          <Bar dataKey="Lost" fill="#ef5350" radius={[4,4,0,0]} />
          <Bar dataKey="No Decision" fill="#3b5068" radius={[4,4,0,0]} />
        </BarChart>
      </ResponsiveContainer>
      <div style={{ marginTop: 16, padding: "10px", background: "#222225", borderRadius: 8, fontSize: 11 }}>
        <div style={{ color: "#888", marginBottom: 4 }}>Large Deals (25K+)</div>
        <div style={{ fontSize: 16, fontWeight: 700, color: "#c5d44b" }}>{largeValuePct}% of total value</div>
      </div>
    </ChartCard>
  );
}

// 8. COMPETITIVE INTELLIGENCE CARD
function CompetitiveCard({ data }) {
  if (!data?.compIntel?.length) return null;
  const sorted = [...data.compIntel].sort((a, b) => {
    if (a.competitor !== b.competitor) return a.competitor.localeCompare(b.competitor);
    return a.sku.localeCompare(b.sku);
  }).slice(0, 10);
  return (
    <ChartCard title="Competitive Intelligence" sub="Top competitors and feature comparison">
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", fontSize: 9, borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #333" }}>
              <th style={{ textAlign: "left", padding: "6px", color: "#888" }}>Competitor</th>
              <th style={{ textAlign: "left", padding: "6px", color: "#888" }}>Our SKU</th>
              <th style={{ textAlign: "right", padding: "6px", color: "#888" }}>Their Price</th>
              <th style={{ textAlign: "center", padding: "6px", color: "#888" }}>Feature</th>
              <th style={{ textAlign: "left", padding: "6px", color: "#888" }}>Segment</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r, i) => (
              <tr key={i} style={{ borderBottom: "1px solid #222" }}>
                <td style={{ padding: "6px", color: "#f0f0f2" }}>{r.competitor}</td>
                <td style={{ padding: "6px", color: "#888" }}>{r.sku}</td>
                <td style={{ textAlign: "right", padding: "6px", color: "#c5d44b" }}>{fmt(r.competitorPrice)}</td>
                <td style={{ textAlign: "center", padding: "6px" }}>
                  <div style={{ width: "30px", height: "8px", background: "#333", borderRadius: 4, margin: "0 auto", position: "relative" }}>
                    <div style={{ width: `${Math.min(r.featureScore * 100, 100)}%`, height: "100%", background: "#00a86b", borderRadius: 4 }} />
                  </div>
                </td>
                <td style={{ padding: "6px", color: "#888", fontSize: 8 }}>{r.segment}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </ChartCard>
  );
}

// 9. REBATE ANALYSIS CARD
function RebateCard({ data }) {
  if (!data?.rebateAnalysis?.length) return null;
  const byCustomer = {};
  data.rebateAnalysis.forEach(r => {
    if (!byCustomer[r.customer]) {
      byCustomer[r.customer] = { customer: r.customer, accrued: 0, claimed: 0, paid: 0, leakage: 0, count: 0 };
    }
    byCustomer[r.customer].accrued += r.accrued;
    byCustomer[r.customer].claimed += r.claimed;
    byCustomer[r.customer].paid += r.paid;
    byCustomer[r.customer].leakage += r.leakage;
    byCustomer[r.customer].count += 1;
  });
  const sorted = Object.values(byCustomer).sort((a, b) => b.accrued - a.accrued).slice(0, 10);
  const chartData = sorted.map(r => ({ name: r.customer.split(" ")[0], ...r }));
  const totalLeakage = data.rebateAnalysis.reduce((s, r) => s + r.leakage, 0);
  const avgPayoutRate = data.rebateAnalysis.length ? (data.rebateAnalysis.reduce((s, r) => s + r.payoutRate, 0) / data.rebateAnalysis.length) : 0;
  return (
    <ChartCard title="Rebate Analysis" sub="Top 10 customers by accrued rebates">
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis dataKey="name" tick={{ fill: "#888", fontSize: 9 }} />
          <YAxis tick={{ fill: "#888", fontSize: 11 }} />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ fontSize: 10, color: "#f0f0f2" }} />
          <Bar dataKey="accrued" name="Accrued" fill="#c5d44b" radius={[4,4,0,0]} />
          <Bar dataKey="claimed" name="Claimed" fill="#3e8c7f" radius={[4,4,0,0]} />
          <Bar dataKey="paid" name="Paid" fill="#00a86b" radius={[4,4,0,0]} />
        </BarChart>
      </ResponsiveContainer>
      <div style={{ display: "flex", gap: 12, marginTop: 16 }}>
        <div style={{ flex: 1, padding: "10px", background: "#222225", borderRadius: 8, fontSize: 11 }}>
          <div style={{ color: "#888", marginBottom: 4 }}>Total Leakage</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: "#ef5350" }}>{fmt(totalLeakage)}</div>
        </div>
        <div style={{ flex: 1, padding: "10px", background: "#222225", borderRadius: 8, fontSize: 11 }}>
          <div style={{ color: "#888", marginBottom: 4 }}>Avg Payout Rate</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: "#00a86b" }}>{(avgPayoutRate * 100).toFixed(1)}%</div>
        </div>
      </div>
    </ChartCard>
  );
}

// 10. COST TO SERVE CARD
function CostToServeCard({ data }) {
  if (!data?.costToServe?.length) return null;
  const byCustomer = {};
  data.costToServe.forEach(r => {
    if (!byCustomer[r.customer]) {
      byCustomer[r.customer] = { customer: r.customer, ...r, count: 0 };
    }
    byCustomer[r.customer].count += 1;
  });
  const sorted = Object.values(byCustomer).sort((a, b) => b.totalCts - a.totalCts).slice(0, 12);
  const chartData = sorted.map(r => ({ name: r.customer.split(" ")[0], ...r }));
  const enterpriseCts = data.costToServe.filter(r => r.segment === "Enterprise").reduce((s, r) => s + r.totalCts, 0) / Math.max(data.costToServe.filter(r => r.segment === "Enterprise").length, 1);
  const midMarketCts = data.costToServe.filter(r => r.segment === "Mid-Market").reduce((s, r) => s + r.totalCts, 0) / Math.max(data.costToServe.filter(r => r.segment === "Mid-Market").length, 1);
  return (
    <ChartCard title="Cost to Serve" sub="Top 12 customers by total CTS">
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis dataKey="name" tick={{ fill: "#888", fontSize: 9 }} />
          <YAxis tick={{ fill: "#888", fontSize: 11 }} />
          <Tooltip content={<CustomTooltip />} />
          <Bar dataKey="serviceCost" fill="#c5d44b" radius={[4,4,0,0]} stackId="a" />
          <Bar dataKey="logisticsCost" fill="#3e8c7f" radius={[0,0,0,0]} stackId="a" />
          <Bar dataKey="returnsCost" fill="#ef5350" radius={[0,0,0,0]} stackId="a" />
          <Bar dataKey="intermediaryCost" fill="#3b5068" radius={[0,0,0,0]} stackId="a" />
          <Bar dataKey="customizationCost" fill="#7ea05e" radius={[0,0,0,0]} stackId="a" />
          <Bar dataKey="paymentTermsCost" fill="#f59e0b" radius={[0,0,0,0]} stackId="a" />
          <Bar dataKey="salesCoverageCost" fill="#8b5cf6" radius={[0,4,4,0]} stackId="a" />
        </BarChart>
      </ResponsiveContainer>
      <div style={{ display: "flex", gap: 12, marginTop: 16 }}>
        <div style={{ flex: 1, padding: "10px", background: "#222225", borderRadius: 8, fontSize: 11 }}>
          <div style={{ color: "#888", marginBottom: 4 }}>Avg CTS (Enterprise)</div>
          <div style={{ fontSize: 14, fontWeight: 700, color: "#c5d44b" }}>{fmt(enterpriseCts)}</div>
        </div>
        <div style={{ flex: 1, padding: "10px", background: "#222225", borderRadius: 8, fontSize: 11 }}>
          <div style={{ color: "#888", marginBottom: 4 }}>Avg CTS (Mid-Market)</div>
          <div style={{ fontSize: 14, fontWeight: 700, color: "#3e8c7f" }}>{fmt(midMarketCts)}</div>
        </div>
      </div>
    </ChartCard>
  );
}

/* ═══════════════════════════════════════════════════════════════
   FILTER BAR
   ═══════════════════════════════════════════════════════════════ */
const FILTER_DIMS = ["Segment", "Region", "Product", "Time Period"];
function FilterBar() {
  const [active, setActive] = useState({});
  const OPTIONS = {
    Segment:      ["Enterprise", "Mid-Market", "SMB", "Long Tail"],
    Region:       ["Northeast", "Southeast", "Midwest", "Southwest", "West Coast"],
    Product:      ["AC-PM-ENT", "AC-PM-PRO", "AC-AN-ADV", "AC-AN-BSC", "AC-SU-STD"],
    "Time Period": ["Last 3mo", "Last 6mo", "Last 12mo", "YTD"],
  };
  return (
    <div style={{ display: "flex", gap: 10, marginBottom: 18, flexWrap: "wrap", alignItems: "center" }}>
      <span style={{ fontSize: 10, color: "#666", textTransform: "uppercase", letterSpacing: 0.8, marginRight: 4 }}>Filters</span>
      {FILTER_DIMS.map(dim => {
        const isOpen = active[dim];
        return (
          <div key={dim} style={{ position: "relative" }}>
            <button onClick={() => setActive(prev => ({ ...prev, [dim]: !prev[dim] }))} style={{
              background: isOpen ? "rgba(197,212,75,.12)" : "#222225",
              border: `1px solid ${isOpen ? "rgba(197,212,75,.35)" : "hsla(0,0%,100%,.06)"}`,
              borderRadius: 8, padding: "5px 12px", fontSize: 11,
              color: isOpen ? "#c5d44b" : "#888", cursor: "pointer",
              display: "flex", alignItems: "center", gap: 5, transition: "all 0.2s",
            }}>
              {dim} <span style={{ fontSize: 8 }}>{isOpen ? "▲" : "▼"}</span>
            </button>
            {isOpen && (
              <div style={{
                position: "absolute", top: "calc(100% + 4px)", left: 0, zIndex: 50,
                background: "#1e1e21", border: "1px solid hsla(0,0%,100%,.08)",
                borderRadius: 10, padding: "8px 0", minWidth: 160,
                boxShadow: "0 12px 40px rgba(0,0,0,.6)",
              }}>
                {OPTIONS[dim].map(opt => (
                  <div key={opt} style={{ padding: "7px 14px", fontSize: 11, color: "#ccc", cursor: "pointer" }}
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
        borderRadius: 8, padding: "5px 10px", fontSize: 10, color: "#555", cursor: "pointer",
      }}>Clear all</button>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   12. NEW PRODUCT PRICING CARD
   ═══════════════════════════════════════════════════════════════ */
function NewProductPricingCard({ data }) {
  if (!data?.newProductPricing?.length) return null;
  const products = data.newProductPricing;

  const chartData = products.map(p => ({
    name: p.productName.replace("AI Insights", "AI").replace("Workflow Automation", "Workflow"),
    "Our Price":       +p.currentListPrice,
    "Competitor Avg":  p.avgCompetitorPrice ? +p.avgCompetitorPrice : null,
    "Suggested":       p.suggestedPrice && p.suggestedPrice !== p.currentListPrice ? +p.suggestedPrice : null,
  }));

  const positionColor = (pos) =>
    pos === "Premium" ? "#c5d44b" : pos === "Underpriced" ? "#ef5350" : "#3e8c7f";
  const statusColor = (s) =>
    s === "Beta" ? "#e8c56c" : s === "Pipeline" ? "#3b5068" : "#3e8c7f";

  return (
    <ChartCard title="New Product Pricing" sub="Beta & pipeline SKUs — price positioning vs market">
      <ResponsiveContainer width="100%" height={260}>
        <BarChart data={chartData} layout="vertical">
          <CartesianGrid strokeDasharray="3 3" stroke="#333" horizontal={false} />
          <XAxis type="number" tick={{ fill: "#888", fontSize: 10 }}
            tickFormatter={v => `$${v}`} />
          <YAxis type="category" dataKey="name" tick={{ fill: "#888", fontSize: 10 }} width={90} />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
          <Bar dataKey="Our Price"      fill="#c5d44b" radius={[0,4,4,0]} barSize={14} />
          <Bar dataKey="Competitor Avg" fill="#3b5068" radius={[0,4,4,0]} barSize={14} />
          <Bar dataKey="Suggested"      fill="#3e8c7f" radius={[0,4,4,0]} barSize={14} />
        </BarChart>
      </ResponsiveContainer>

      <table style={{ width: "100%", fontSize: 11, borderCollapse: "collapse", marginTop: 16 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid #333" }}>
            <th style={{ textAlign: "left",  padding: "6px 8px", color: "#888" }}>Product</th>
            <th style={{ textAlign: "center",padding: "6px 8px", color: "#888" }}>Status</th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "#888" }}>List Price</th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "#888" }}>Margin</th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "#888" }}>vs Market</th>
            <th style={{ textAlign: "center",padding: "6px 8px", color: "#888" }}>Position</th>
          </tr>
        </thead>
        <tbody>
          {products.map(p => (
            <tr key={p.productSku} style={{ borderBottom: "1px solid #222" }}>
              <td style={{ padding: "6px 8px", color: "#f0f0f2" }}>
                <div style={{ fontWeight: 600 }}>{p.productName}</div>
                <div style={{ color: "#555", fontSize: 10 }}>{p.productSku}</div>
              </td>
              <td style={{ textAlign: "center", padding: "6px 8px" }}>
                <span style={{
                  background: statusColor(p.productStatus) + "33",
                  color: statusColor(p.productStatus),
                  padding: "2px 8px", borderRadius: 99, fontSize: 10, fontWeight: 700,
                }}>{p.productStatus}</span>
              </td>
              <td style={{ textAlign: "right", padding: "6px 8px", color: "#c5d44b" }}>
                ${(+p.currentListPrice).toLocaleString()}
              </td>
              <td style={{ textAlign: "right", padding: "6px 8px", color: "#3e8c7f" }}>
                {p.grossMarginPct}%
              </td>
              <td style={{ textAlign: "right", padding: "6px 8px",
                color: +p.priceVsMarketPct > 5 ? "#c5d44b" : +p.priceVsMarketPct < -5 ? "#ef5350" : "#888" }}>
                {p.priceVsMarketPct != null ? `${+p.priceVsMarketPct > 0 ? "+" : ""}${p.priceVsMarketPct}%` : "—"}
              </td>
              <td style={{ textAlign: "center", padding: "6px 8px" }}>
                <span style={{
                  background: positionColor(p.marketPosition) + "33",
                  color: positionColor(p.marketPosition),
                  padding: "2px 8px", borderRadius: 99, fontSize: 10, fontWeight: 700,
                }}>{p.marketPosition}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </ChartCard>
  );
}

/* ═══════════════════════════════════════════════════════════════
   16. PROMOTION ROI CARD
   ═══════════════════════════════════════════════════════════════ */
function PromotionRoiCard({ data }) {
  if (!data?.promotionRoi?.length) return null;
  const promos = data.promotionRoi.filter(p => p.promoRevenue > 0);

  const chartData = promos.map(p => ({
    name: p.promotionType.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase()).substring(0, 16),
    "Promo Rev":    Math.round(p.promoRevenue / 1000),
    "Baseline Rev": Math.round(p.baselineRevenue / 1000),
  }));

  const perfColor = (band) =>
    band === "High Performer" ? "#c5d44b" : band === "Positive" ? "#3e8c7f" : "#ef5350";

  const totalLift = promos.reduce((s, p) => s + p.revenueLift, 0);
  const totalCost = promos.reduce((s, p) => s + p.promotionalCost, 0);
  const overallRoi = totalCost > 0 ? Math.round((totalLift - totalCost) / totalCost * 100) : null;
  const winners = promos.filter(p => p.performanceBand !== "Below Baseline").length;

  return (
    <ChartCard title="Promotion ROI" sub="Revenue lift vs baseline and cost of promotion">
      <div style={{ display: "flex", gap: 16, marginBottom: 16 }}>
        {[
          { label: "Total Lift", value: fmt(Math.abs(totalLift)), color: totalLift > 0 ? "#c5d44b" : "#ef5350" },
          { label: "Total Promo Cost", value: fmt(totalCost), color: "#888" },
          { label: "Overall ROI", value: overallRoi != null ? `${overallRoi}%` : "—",
            color: overallRoi > 0 ? "#3e8c7f" : "#ef5350" },
          { label: "Winners", value: `${winners} / ${promos.length}`, color: "#e8c56c" },
        ].map(m => (
          <div key={m.label} style={{ flex: 1, background: "#252528", borderRadius: 10, padding: "10px 14px" }}>
            <div style={{ fontSize: 10, color: "#888", marginBottom: 4 }}>{m.label}</div>
            <div style={{ fontSize: 18, fontWeight: 800, color: m.color }}>{m.value}</div>
          </div>
        ))}
      </div>

      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis dataKey="name" tick={{ fill: "#888", fontSize: 9 }} />
          <YAxis tick={{ fill: "#888", fontSize: 10 }} tickFormatter={v => `$${v}K`} />
          <Tooltip content={<CustomTooltip />} formatter={v => `$${v}K`} />
          <Legend wrapperStyle={{ fontSize: 11, color: "#f0f0f2" }} />
          <Bar dataKey="Promo Rev"    fill="#c5d44b" radius={[4,4,0,0]} />
          <Bar dataKey="Baseline Rev" fill="#3b5068" radius={[4,4,0,0]} />
        </BarChart>
      </ResponsiveContainer>

      <table style={{ width: "100%", fontSize: 11, borderCollapse: "collapse", marginTop: 16 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid #333" }}>
            <th style={{ textAlign: "left",  padding: "6px 8px", color: "#888" }}>Promotion</th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "#888" }}>SKU</th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "#888" }}>Discount</th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "#888" }}>Lift</th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "#888" }}>ROI</th>
            <th style={{ textAlign: "center",padding: "6px 8px", color: "#888" }}>Band</th>
          </tr>
        </thead>
        <tbody>
          {promos.map(p => (
            <tr key={p.promotionId} style={{ borderBottom: "1px solid #222" }}>
              <td style={{ padding: "6px 8px", color: "#f0f0f2" }}>
                {p.promotionType.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase())}
              </td>
              <td style={{ textAlign: "right", padding: "6px 8px", color: "#888" }}>{p.productSku}</td>
              <td style={{ textAlign: "right", padding: "6px 8px", color: "#e8c56c" }}>
                {p.discountDepthPct}%
              </td>
              <td style={{ textAlign: "right", padding: "6px 8px",
                color: p.revenueLift > 0 ? "#3e8c7f" : "#ef5350" }}>
                {p.revenueLift >= 0 ? "+" : ""}{fmt(Math.abs(p.revenueLift))}
              </td>
              <td style={{ textAlign: "right", padding: "6px 8px",
                color: p.roiPct > 0 ? "#c5d44b" : "#ef5350" }}>
                {p.roiPct != null ? `${p.roiPct}%` : "—"}
              </td>
              <td style={{ textAlign: "center", padding: "6px 8px" }}>
                <span style={{
                  background: perfColor(p.performanceBand) + "33",
                  color: perfColor(p.performanceBand),
                  padding: "2px 6px", borderRadius: 99, fontSize: 9, fontWeight: 700,
                }}>{p.performanceBand}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </ChartCard>
  );
}

/* ═══════════════════════════════════════════════════════════════
   LOADING / ERROR SCREENS
   ═══════════════════════════════════════════════════════════════ */
function LoadingScreen() {
  return (
    <div style={{
      display: "flex", alignItems: "center", justifyContent: "center",
      minHeight: "100vh", background: "#161618", color: "#888", flexDirection: "column", gap: 16,
    }}>
      <div style={{
        width: 40, height: 40, borderRadius: 10,
        background: "linear-gradient(135deg, #c5d44b, #3e8c7f)",
        display: "flex", alignItems: "center", justifyContent: "center",
        fontWeight: 900, fontSize: 18, color: "#161618",
        animation: "pulse 1.5s ease-in-out infinite",
      }}>F</div>
      <div style={{ fontSize: 13 }}>Loading diagnostic data…</div>
      <style>{`@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }`}</style>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   VIEW WRAPPERS — new analyses wired to sidebar IDs
   ═══════════════════════════════════════════════════════════════ */
function PvmView() {
  const data = useData();
  const pvm = data?.pvm || [];
  if (!pvm.length) return <div style={{padding:32,color:"#888",textAlign:"center"}}>No PVM data available</div>;
  return (
    <ChartCard title="Price-Volume-Mix Decomposition" sub="Revenue change split into Price, Volume and Mix effects">
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={pvm}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis dataKey="period" tick={{ fill:"#888", fontSize:11 }} />
          <YAxis tick={{ fill:"#888", fontSize:11 }} />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ fontSize:11, color:"#f0f0f2" }} />
          <Bar dataKey="price"  name="Price Effect"  fill="#c5d44b" radius={[3,3,0,0]} />
          <Bar dataKey="volume" name="Volume Effect" fill="#3e8c7f" radius={[3,3,0,0]} />
          <Bar dataKey="mix"    name="Mix Effect"    fill="#7ea05e" radius={[3,3,0,0]} />
          <Bar dataKey="total"  name="Net Change"    fill="#e8c56c" radius={[3,3,0,0]} />
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}
// Simple wrappers — render the card components with data from context
function wrapCard(Component) {
  return function WrappedView() {
    const data = useData();
    return <div style={{ padding: 4 }}><Component data={data} /></div>;
  };
}
const WinLossView         = wrapCard(WinLossCard);
const DealVelocityView    = wrapCard(DealVelocityCard);
const DiscountGovView     = wrapCard(DiscountGovCard);
const PriceBandView       = wrapCard(PriceBandCard);
const RebateView          = wrapCard(RebateCard);
const CompetitiveView     = wrapCard(CompetitiveCard);
const DealSizeView        = wrapCard(DealSizeCard);
const RepPerfView         = wrapCard(RepPerfCard);
const CostToServeView     = wrapCard(CostToServeCard);
const CohortView          = wrapCard(CohortRevenueCard);
const NewProductPricingView = wrapCard(NewProductPricingCard);
const PromotionRoiView    = wrapCard(PromotionRoiCard);

/* ═══════════════════════════════════════════════════════════════
   MAIN EXPORT
   ═══════════════════════════════════════════════════════════════ */
export default function Dashboard({ clientId, clientName, userRole, userName, onBack, onLogout }) {
  const [view, setView]             = useState("dashboard");
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const { data, loading, error }    = useDashboardData(clientId);

  if (loading) return <LoadingScreen />;
  if (error) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100vh", background: "#161618", color: "#ef5350", fontSize: 13 }}>
      Error loading data: {error}
    </div>
  );

  const meta = data?.clientMeta || {};
  const displayName = clientName || meta.name || clientId;

  const viewMap = {
    dashboard:    { title: "Executive Dashboard",    component: <ExecutiveDashboard /> },
    "01":         { title: "Pocket Price Waterfall", component: <PocketPriceWaterfallView /> },
    "02":         { title: "Deal Velocity",          component: <DealVelocityView /> },
    "03":         { title: "Price-Volume-Mix",       component: <PvmView /> },
    "04":         { title: "Customer Profitability", component: <CustomerProfitabilityView /> },
    "05":         { title: "Discount Effectiveness", component: <DiscountGovView /> },
    "06":         { title: "Price Bands",            component: <PriceBandView /> },
    "07":         { title: "Contract Leakage",       component: <RebateView /> },
    "08":         { title: "Deal Scorecard",         component: <DealScorecardView /> },
    "09":         { title: "Competitive Position",   component: <CompetitiveView /> },
    "10":         { title: "Price Corridor",         component: <DealSizeView /> },
    "11":         { title: "SKU Pareto",             component: <SkuParetoView /> },
    "12":         { title: "New Product Pricing",    component: <NewProductPricingView /> },
    "13":         { title: "Churn Risk",             component: <ChurnRiskView /> },
    "14":         { title: "Geographic Pricing",     component: <GeoPricingView /> },
    "15":         { title: "Sales Rep Performance",  component: <RepPerfView /> },
    "16":         { title: "Promotion ROI",           component: <PromotionRoiView /> },
    "17":         { title: "Cost-to-Serve",          component: <CostToServeView /> },
    "18":         { title: "Rebate Optimization",    component: <RebateView /> },
    "19":         { title: "Cohort Revenue",         component: <CohortView /> },
    "20":         { title: "Win/Loss Analysis",      component: <WinLossView /> },
    opportunities:{ title: "Opportunities",          component: <OpportunitiesView /> },
  };

  const current = viewMap[view] || viewMap.dashboard;

  return (
    <DataCtx.Provider value={data}>
      <div style={{ display: "flex", minHeight: "100vh", background: "#161618", color: "#f0f0f2", fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif" }}>

        {/* Sidebar */}
        <div style={{
          width: sidebarOpen ? 260 : 60, background: "#1e1e21",
          borderRight: "1px solid hsla(0,0%,100%,.03)",
          transition: "width 0.3s", overflow: "hidden", flexShrink: 0,
          display: "flex", flexDirection: "column",
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

          {/* Client chip */}
          {sidebarOpen && (
            <div style={{ padding: "12px 16px", borderBottom: "1px solid hsla(0,0%,100%,.03)" }}>
              <div style={{ fontSize: 10, color: "#888", marginBottom: 4, textTransform: "uppercase" }}>Client</div>
              <div style={{
                background: "#222225", borderRadius: 8, padding: "8px 12px",
                border: "1px solid hsla(0,0%,100%,.03)", fontSize: 12,
                color: "#f0f0f2", fontWeight: 600,
              }}>
                {displayName}
                <span style={{ fontSize: 10, color: "#888", marginLeft: 8 }}>{meta.tier}</span>
              </div>
            </div>
          )}

          {/* Nav */}
          <div style={{ flex: 1, overflowY: "auto", padding: "8px 0" }}>
            {[
              { id: "dashboard",     icon: "▣", label: "Executive Dashboard",  color: "#c5d44b" },
              { id: "opportunities", icon: "★", label: "Opportunities",         color: "#00a86b" },
            ].map(item => (
              <div key={item.id} onClick={() => setView(item.id)} style={{
                padding: "10px 16px", cursor: "pointer", display: "flex", alignItems: "center", gap: 10,
                background: view === item.id ? `rgba(${item.id === "dashboard" ? "197,212,75" : "0,168,107"},.1)` : "transparent",
                borderLeft: view === item.id ? `3px solid ${item.color}` : "3px solid transparent",
                transition: "all 0.2s",
              }}>
                <span style={{ fontSize: 16, width: 24, textAlign: "center" }}>{item.icon}</span>
                {sidebarOpen && <span style={{ fontSize: 12, fontWeight: 600, color: view === item.id ? "#f0f0f2" : "#888" }}>{item.label}</span>}
              </div>
            ))}

            {sidebarOpen && <div style={{ padding: "12px 16px 4px", fontSize: 10, color: "#666", textTransform: "uppercase", letterSpacing: 1 }}>Analyses</div>}

            {ANALYSIS_LIST.map(a => {
              const hasView = !!viewMap[a.id];
              const isActive = view === a.id;
              return (
                <div key={a.id} onClick={() => hasView && setView(a.id)} style={{
                  padding: "7px 16px", cursor: hasView ? "pointer" : "default",
                  display: "flex", alignItems: "center", gap: 10,
                  background: isActive ? "rgba(62,140,127,.1)" : "transparent",
                  borderLeft: isActive ? "3px solid #3e8c7f" : "3px solid transparent",
                  opacity: hasView ? 1 : 0.4, transition: "all 0.2s",
                }}>
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
              Data Quality: <Badge color="#00a86b">{meta.dataQuality || "—"}</Badge>
              <div style={{ marginTop: 4 }}>Updated: {meta.lastUpdated || "—"}</div>
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
            padding: "16px 28px", borderBottom: "1px solid hsla(0,0%,100%,.03)",
            display: "flex", justifyContent: "space-between", alignItems: "center",
            background: "#161618", boxShadow: "0 2px 8px rgba(0,0,0,.3)",
          }}>
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <button onClick={() => setSidebarOpen(!sidebarOpen)} style={{
                background: "#1e1e21", border: "1px solid hsla(0,0%,100%,.03)",
                borderRadius: 6, color: "#888", padding: "4px 8px",
                cursor: "pointer", fontSize: 14, transition: "all 0.2s",
                boxShadow: "4px 4px 10px rgba(0,0,0,.4), -4px -4px 10px hsla(0,0%,100%,.02)",
              }}>{sidebarOpen ? "☰" : "→"}</button>
              <h2 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: "#f0f0f2" }}>{current.title}</h2>
            </div>
            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <button style={{
                background: "#1e1e21", border: "1px solid hsla(0,0%,100%,.03)",
                borderRadius: 8, color: "#888", padding: "6px 14px", fontSize: 11,
                cursor: "pointer", boxShadow: "4px 4px 10px rgba(0,0,0,.4), -4px -4px 10px hsla(0,0%,100%,.02)",
              }}>Export PPTX</button>
              <button style={{
                background: "linear-gradient(135deg, #c5d44b, #3e8c7f)",
                border: "none", borderRadius: 8, color: "#161618",
                padding: "6px 14px", fontSize: 11, fontWeight: 600,
                cursor: "pointer", boxShadow: "0 0 20px rgba(197,212,75,.3)",
              }}>Re-run Analysis</button>
            </div>
          </div>

          {/* Content area */}
          <div style={{ padding: 28 }}>
            {view !== "dashboard" && view !== "opportunities" && <FilterBar />}
            {current.component}
            {view === "dashboard" && (
              <div style={{ marginTop: 32 }}>
                <h2 style={{ textAlign: "left", fontSize: 18, fontWeight: 700, color: "#f0f0f2", marginBottom: 4, paddingLeft: 4 }}>Extended Analysis</h2>
                <div style={{ gridColumn: "span 1", gap: 6, marginBottom: 16, paddingLeft: 4 }}>
                  <p style={{ fontSize: 11, color: "#888", margin: 0 }}>Advanced pricing insights and operational metrics</p>
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(500px, 1fr))", gap: 20 }}>
                  <PriceBandCard data={data} />
                  <WinLossCard data={data} />
                  <DealVelocityCard data={data} />
                  <DiscountGovCard data={data} />
                  <RepPerfCard data={data} />
                  <DealSizeCard data={data} />
                  <CompetitiveCard data={data} />
                  <RebateCard data={data} />
                  <CostToServeCard data={data} />
                  <CohortRevenueCard data={data} />
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </DataCtx.Provider>
  );
}
