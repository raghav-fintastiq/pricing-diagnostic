import { useState } from "react";

const READINESS_ITEMS = [
  { key: "nda", label: "NDA" },
  { key: "dataDict", label: "Data Dict" },
  { key: "contact", label: "Contact" },
  { key: "goals", label: "Goals" },
];

const CLIENTS = [
  {
    name: "Gravitate Energy",
    industry: "SaaS / Fuel Distribution",
    bronze: "complete", silver: "complete", gold: "pending",
    dataQuality: "B+", tables: 6, records: "1,921", revenue: "$34.9M",
    lastRun: "2026-03-07", analyses: 7,
    tags: ["SaaS", "B2B", "Mid-Market"],
    readiness: { nda: true, dataDict: true, contact: true, goals: true },
  },
  {
    name: "NPI",
    industry: "Distribution / Wholesale",
    bronze: "complete", silver: "complete", gold: "pending",
    dataQuality: "B", tables: 5, records: "4,200+", revenue: "—",
    lastRun: "2026-03-05", analyses: 5,
    tags: ["Distribution", "B2B"],
    readiness: { nda: true, dataDict: true, contact: true, goals: false },
  },
  {
    name: "Ansell",
    industry: "Industrial Safety / PPE",
    bronze: "complete", silver: "pending", gold: "pending",
    dataQuality: "D", tables: 3, records: "16,195", revenue: "$1.09B*",
    lastRun: "2026-03-12", analyses: 3,
    tags: ["Manufacturing", "B2B", "Enterprise"],
    warning: "Revenue inflation detected — re-run needed",
    readiness: { nda: true, dataDict: false, contact: true, goals: false },
  },
  {
    name: "RxBenefits",
    industry: "Pharmacy Benefit Management",
    bronze: "complete", silver: "pending", gold: "pending",
    dataQuality: "B-", tables: 7, records: "23,703", revenue: "—",
    lastRun: "2026-03-13", analyses: 7,
    tags: ["PBM", "Healthcare", "B2B"],
    readiness: { nda: true, dataDict: false, contact: false, goals: false },
  },
];

const STAGE_COLOR = { complete: "#00a86b", pending: "#c5d44b", running: "#3e8c7f", error: "#ef5350" };
const GRADE_COLOR = { "A": "#00a86b", "A+": "#00a86b", "B+": "#00a86b", "B": "#c5d44b", "B-": "#c5d44b", "C": "#3b5068", "D": "#ef5350" };

function StageDot({ stage }) {
  return (
    <span style={{
      display: "inline-block", width: 8, height: 8, borderRadius: "50%",
      background: STAGE_COLOR[stage] || "#444",
      boxShadow: stage === "complete" ? "0 0 6px rgba(0,168,107,.5)" : "none",
      marginRight: 5,
    }} />
  );
}

export default function AdminPage({ user, onSelectClient, onLogout }) {
  const [uploadOpen, setUploadOpen] = useState(false);
  const [uploadClient, setUploadClient] = useState("");
  const [dragging, setDragging] = useState(false);
  const [uploadTab, setUploadTab] = useState("files"); // "files" | "context"
  const [contextText, setContextText] = useState("");

  const totalRecords = "46,019+";
  const activeClients = CLIENTS.length;
  const readyClients = CLIENTS.filter(c => c.silver === "complete").length;

  return (
    <div style={{
      minHeight: "100vh",
      background: "#161618",
      color: "#f0f0f2",
      fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
    }}>

      {/* Top bar */}
      <div style={{
        padding: "0 28px",
        height: 56,
        borderBottom: "1px solid hsla(0,0%,100%,.04)",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        background: "#1e1e21",
        boxShadow: "0 2px 12px rgba(0,0,0,.3)",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{
            width: 28, height: 28, borderRadius: 7,
            background: "linear-gradient(135deg, #c5d44b, #3e8c7f)",
            display: "flex", alignItems: "center", justifyContent: "center",
            fontWeight: 900, fontSize: 13, color: "#161618",
            boxShadow: "0 0 12px rgba(197,212,75,.3)",
          }}>F</div>
          <span style={{ fontSize: 15, fontWeight: 700, color: "#f0f0f2" }}>FintastIQ</span>
          <span style={{ fontSize: 11, color: "#555", padding: "2px 8px", background: "#222225", borderRadius: 6, border: "1px solid hsla(0,0%,100%,.05)" }}>Admin</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
          <span style={{ fontSize: 12, color: "#888" }}>{user.name}</span>
          <button
            onClick={onLogout}
            style={{
              background: "transparent", border: "1px solid hsla(0,0%,100%,.08)",
              borderRadius: 7, padding: "5px 12px", fontSize: 11, color: "#888", cursor: "pointer",
            }}
          >Sign out</button>
        </div>
      </div>

      <div style={{ padding: "28px 32px", maxWidth: 1100, margin: "0 auto" }}>

        {/* Summary metrics */}
        <div style={{ display: "flex", gap: 14, marginBottom: 28 }}>
          {[
            { label: "Active Clients", value: activeClients, color: "#c5d44b" },
            { label: "Silver-Ready", value: `${readyClients} / ${activeClients}`, color: "#00a86b" },
            { label: "Total Records", value: totalRecords, color: "#3e8c7f" },
            { label: "Pipeline Stage", value: "Bronze → Silver", color: "#c5d44b" },
          ].map(m => (
            <div key={m.label} style={{
              flex: 1, background: "#1e1e21", borderRadius: 12,
              padding: "16px 20px", border: "1px solid hsla(0,0%,100%,.04)",
              boxShadow: "4px 4px 12px rgba(0,0,0,.4)",
            }}>
              <div style={{ fontSize: 11, color: "#666", marginBottom: 4, textTransform: "uppercase", letterSpacing: 0.5 }}>{m.label}</div>
              <div style={{ fontSize: 22, fontWeight: 800, color: m.color }}>{m.value}</div>
            </div>
          ))}
        </div>

        {/* Header row */}
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 18 }}>
          <div>
            <h2 style={{ margin: 0, fontSize: 17, fontWeight: 700, color: "#f0f0f2" }}>Client Pipelines</h2>
            <p style={{ margin: "2px 0 0", fontSize: 12, color: "#666" }}>Click any client to view their diagnostic dashboard</p>
          </div>
          <button
            onClick={() => setUploadOpen(true)}
            style={{
              background: "linear-gradient(135deg, #c5d44b, #3e8c7f)",
              border: "none", borderRadius: 9, padding: "9px 18px",
              fontSize: 12, fontWeight: 600, color: "#161618",
              cursor: "pointer", boxShadow: "0 0 18px rgba(197,212,75,.25)",
            }}
          >
            + Upload Client Data
          </button>
        </div>

        {/* Client cards */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {CLIENTS.map(c => (
            <div
              key={c.name}
              onClick={() => onSelectClient(c.name)}
              style={{
                background: "#1e1e21", borderRadius: 14,
                padding: "18px 22px",
                border: `1px solid ${c.warning ? "rgba(239,83,80,.2)" : "hsla(0,0%,100%,.04)"}`,
                cursor: "pointer",
                display: "grid",
                gridTemplateColumns: "2fr 1fr 1fr 1fr 120px",
                alignItems: "center",
                gap: 16,
                transition: "all 0.2s",
                boxShadow: "4px 4px 12px rgba(0,0,0,.4)",
              }}
              onMouseEnter={e => {
                e.currentTarget.style.borderColor = "rgba(197,212,75,.25)";
                e.currentTarget.style.transform = "translateY(-1px)";
              }}
              onMouseLeave={e => {
                e.currentTarget.style.borderColor = c.warning ? "rgba(239,83,80,.2)" : "hsla(0,0%,100%,.04)";
                e.currentTarget.style.transform = "translateY(0)";
              }}
            >
              {/* Name + industry */}
              <div>
                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 4 }}>
                  <span style={{ fontSize: 14, fontWeight: 700, color: "#f0f0f2" }}>{c.name}</span>
                  <span style={{
                    fontSize: 10, fontWeight: 700,
                    padding: "2px 8px", borderRadius: 9999,
                    background: `${GRADE_COLOR[c.dataQuality] || "#888"}22`,
                    color: GRADE_COLOR[c.dataQuality] || "#888",
                    border: `1px solid ${GRADE_COLOR[c.dataQuality] || "#888"}44`,
                  }}>{c.dataQuality}</span>
                </div>
                <div style={{ fontSize: 11, color: "#666" }}>{c.industry}</div>
                {c.warning && (
                  <div style={{ fontSize: 10, color: "#ef5350", marginTop: 4 }}>⚠ {c.warning}</div>
                )}
                <div style={{ display: "flex", gap: 5, marginTop: 6 }}>
                  {c.tags.map(t => (
                    <span key={t} style={{
                      fontSize: 9, padding: "1px 6px", borderRadius: 9999,
                      background: "rgba(62,140,127,.15)", color: "#3e8c7f",
                      border: "1px solid rgba(62,140,127,.2)",
                    }}>{t}</span>
                  ))}
                </div>
              </div>

              {/* Pipeline stages */}
              <div>
                <div style={{ fontSize: 10, color: "#555", marginBottom: 6, textTransform: "uppercase" }}>Pipeline</div>
                {["bronze", "silver", "gold"].map(stage => (
                  <div key={stage} style={{ display: "flex", alignItems: "center", marginBottom: 3 }}>
                    <StageDot stage={c[stage]} />
                    <span style={{ fontSize: 11, color: c[stage] === "complete" ? "#888" : "#555", textTransform: "capitalize" }}>
                      {stage}
                    </span>
                  </div>
                ))}
              </div>

              {/* Data stats */}
              <div>
                <div style={{ fontSize: 10, color: "#555", marginBottom: 6, textTransform: "uppercase" }}>Data</div>
                <div style={{ fontSize: 12, color: "#888", marginBottom: 2 }}>{c.tables} tables</div>
                <div style={{ fontSize: 12, color: "#888", marginBottom: 2 }}>{c.records} records</div>
                <div style={{ fontSize: 12, color: "#888" }}>Rev: {c.revenue}</div>
              </div>

              {/* Readiness */}
              <div>
                <div style={{ fontSize: 10, color: "#555", marginBottom: 6, textTransform: "uppercase" }}>Readiness</div>
                {(() => {
                  const done = READINESS_ITEMS.filter(r => c.readiness[r.key]).length;
                  const total = READINESS_ITEMS.length;
                  const pct = Math.round((done / total) * 100);
                  const color = pct === 100 ? "#00a86b" : pct >= 50 ? "#c5d44b" : "#ef5350";
                  return (
                    <>
                      <div style={{ fontSize: 16, fontWeight: 800, color, marginBottom: 4 }}>{done}/{total}</div>
                      <div style={{ display: "flex", gap: 3 }}>
                        {READINESS_ITEMS.map(r => (
                          <div key={r.key} title={r.label} style={{
                            width: 18, height: 4, borderRadius: 2,
                            background: c.readiness[r.key] ? "#00a86b" : "hsla(0,0%,100%,.08)",
                            transition: "background 0.2s",
                          }} />
                        ))}
                      </div>
                      <div style={{ fontSize: 9, color: "#555", marginTop: 4 }}>
                        {READINESS_ITEMS.filter(r => !c.readiness[r.key]).map(r => r.label).join(", ") || "All clear"}
                      </div>
                    </>
                  );
                })()}
              </div>

              {/* Action */}
              <div style={{ textAlign: "right" }}>
                <div style={{
                  display: "inline-block",
                  background: "rgba(197,212,75,.12)",
                  border: "1px solid rgba(197,212,75,.2)",
                  borderRadius: 8, padding: "7px 14px",
                  fontSize: 11, fontWeight: 600, color: "#c5d44b",
                }}>View Dashboard →</div>
                <div style={{ fontSize: 10, color: "#555", marginTop: 5 }}>Updated {c.lastRun}</div>
              </div>
            </div>
          ))}
        </div>

        {/* Upload modal */}
        {uploadOpen && (
          <div style={{
            position: "fixed", inset: 0,
            background: "rgba(0,0,0,.7)",
            display: "flex", alignItems: "center", justifyContent: "center",
            zIndex: 100, backdropFilter: "blur(4px)",
          }}
            onClick={() => setUploadOpen(false)}
          >
            <div
              style={{
                background: "#1e1e21", borderRadius: 18, padding: "30px 34px",
                width: "100%", maxWidth: 460,
                border: "1px solid hsla(0,0%,100%,.06)",
                boxShadow: "0 30px 80px rgba(0,0,0,.6)",
              }}
              onClick={e => e.stopPropagation()}
            >
              <h3 style={{ margin: "0 0 6px", fontSize: 17, fontWeight: 700, color: "#f0f0f2" }}>Upload Client Data</h3>
              <p style={{ margin: "0 0 16px", fontSize: 12, color: "#888" }}>
                Files are processed locally — raw data never leaves your machine.
              </p>

              {/* Client selector */}
              <div style={{ marginBottom: 14 }}>
                <label style={{ fontSize: 11, color: "#888", display: "block", marginBottom: 6 }}>Client</label>
                <select
                  value={uploadClient}
                  onChange={e => setUploadClient(e.target.value)}
                  style={{
                    width: "100%", background: "#222225",
                    border: "1px solid hsla(0,0%,100%,.08)", borderRadius: 8,
                    padding: "9px 12px", fontSize: 13, color: "#f0f0f2",
                    boxSizing: "border-box",
                  }}
                >
                  <option value="">Select client...</option>
                  {CLIENTS.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
                  <option value="new">+ New client</option>
                </select>
              </div>

              {/* Tab bar */}
              <div style={{ display: "flex", borderBottom: "1px solid hsla(0,0%,100%,.06)", marginBottom: 16 }}>
                {[
                  { id: "files", label: "Files" },
                  { id: "context", label: "Context" },
                ].map(tab => (
                  <button
                    key={tab.id}
                    onClick={() => setUploadTab(tab.id)}
                    style={{
                      flex: 1, background: "transparent",
                      border: "none",
                      borderBottom: `2px solid ${uploadTab === tab.id ? "#c5d44b" : "transparent"}`,
                      padding: "8px 0", fontSize: 12,
                      fontWeight: uploadTab === tab.id ? 700 : 400,
                      color: uploadTab === tab.id ? "#c5d44b" : "#666",
                      cursor: "pointer", transition: "all 0.2s",
                    }}
                  >{tab.label}</button>
                ))}
              </div>

              {/* Tab: Files */}
              {uploadTab === "files" && (
                <>
                  <div
                    onDragOver={e => { e.preventDefault(); setDragging(true); }}
                    onDragLeave={() => setDragging(false)}
                    onDrop={e => { e.preventDefault(); setDragging(false); }}
                    style={{
                      border: `2px dashed ${dragging ? "#c5d44b" : "hsla(0,0%,100%,.1)"}`,
                      borderRadius: 12, padding: "28px 20px",
                      textAlign: "center",
                      background: dragging ? "rgba(197,212,75,.05)" : "transparent",
                      transition: "all 0.2s", cursor: "pointer", marginBottom: 14,
                    }}
                  >
                    <div style={{ fontSize: 28, marginBottom: 8 }}>📂</div>
                    <div style={{ fontSize: 13, color: "#888", marginBottom: 4 }}>Drag & drop files here</div>
                    <div style={{ fontSize: 11, color: "#555" }}>Excel (.xlsx, .xlsb), CSV — any size</div>
                  </div>
                </>
              )}

              {/* Tab: Context */}
              {uploadTab === "context" && (
                <div>
                  <div style={{ fontSize: 11, color: "#888", marginBottom: 8 }}>
                    Add business context to help the LLM understand this client's data. Include pricing model, key terminology, and any domain-specific notes.
                  </div>
                  <textarea
                    value={contextText}
                    onChange={e => setContextText(e.target.value)}
                    placeholder={"e.g. Gravitate Energy uses a SaaS + fuel distribution model.\nPricing tiers: Standard, Pro, Enterprise.\nKey metric: PEPM (per employee per month).\nDiscount approval required above 20%."}
                    style={{
                      width: "100%", boxSizing: "border-box", minHeight: 140,
                      background: "#222225", border: "1px solid hsla(0,0%,100%,.08)",
                      borderRadius: 10, padding: "12px 14px", fontSize: 12,
                      color: "#f0f0f2", resize: "vertical", outline: "none",
                      lineHeight: 1.6, fontFamily: "inherit",
                    }}
                  />
                  <div style={{ fontSize: 10, color: "#555", marginTop: 6 }}>
                    This will be saved as client_context.md alongside the data files.
                  </div>
                </div>
              )}

              {/* Action buttons */}
              <div style={{ display: "flex", gap: 10, marginTop: 16 }}>
                <button
                  onClick={() => setUploadOpen(false)}
                  style={{
                    flex: 1, background: "transparent",
                    border: "1px solid hsla(0,0%,100%,.08)", borderRadius: 9,
                    padding: "10px", fontSize: 12, color: "#888", cursor: "pointer",
                  }}
                >Cancel</button>
                <button
                  style={{
                    flex: 2,
                    background: uploadClient ? "linear-gradient(135deg, #c5d44b, #3e8c7f)" : "#333",
                    border: "none", borderRadius: 9,
                    padding: "10px", fontSize: 12, fontWeight: 600,
                    color: uploadClient ? "#161618" : "#555",
                    cursor: uploadClient ? "pointer" : "not-allowed",
                  }}
                >
                  Run Bronze Pipeline →
                </button>
              </div>

              <p style={{ fontSize: 10, color: "#444", marginTop: 12, textAlign: "center" }}>
                Pipeline runs on your local machine via the ETL scripts in 3. ETL Scripts/
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
