import { useState, useEffect, useRef } from "react";
import { supabase } from "./lib/supabaseClient";

const LOCAL_SERVER = "http://127.0.0.1:5001";

const READINESS_ITEMS = [
  { key: "readiness_nda",       label: "NDA" },
  { key: "readiness_data_dict", label: "Data Dict" },
  { key: "readiness_contact",   label: "Contact" },
  { key: "readiness_goals",     label: "Goals" },
];

const STAGE_COLOR = { complete: "#00a86b", pending: "#c5d44b", running: "#3e8c7f", error: "#ef5350" };
const GRADE_COLOR = { "A": "#00a86b", "A+": "#00a86b", "A-": "#00a86b", "B+": "#00a86b", "B": "#c5d44b", "B-": "#c5d44b", "C": "#3b5068", "D": "#ef5350" };

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

// ── Pipeline progress panel ───────────────────────────────────────────────────
function PipelineProgress({ jobId, onClose, onComplete }) {
  const [logs, setLogs] = useState([]);
  const [status, setStatus] = useState("running");
  const logRef = useRef(null);

  useEffect(() => {
    if (!jobId) return;
    const es = new EventSource(`${LOCAL_SERVER}/job/${jobId}`);

    es.onmessage = (e) => {
      const data = JSON.parse(e.data);
      if (data.type === "log") {
        setLogs(prev => [...prev, data]);
        setTimeout(() => {
          if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
        }, 50);
      } else if (data.type === "status" || data.type === "done") {
        setStatus(data.status);
        if (data.type === "done") {
          es.close();
          if (data.status === "complete") setTimeout(onComplete, 1500);
        }
      }
    };
    es.onerror = () => {
      es.close();
      setStatus("error");
      setLogs(prev => [...prev, { level: "error", msg: "Lost connection to pipeline server.", ts: "" }]);
    };
    return () => es.close();
  }, [jobId]);

  const statusColor = status === "complete" ? "#00a86b" : status === "error" ? "#ef5350" : "#c5d44b";
  const statusLabel = status === "complete" ? "Complete ✓" : status === "error" ? "Failed ✗" : "Running…";

  return (
    <div style={{ marginTop: 16 }}>
      <div style={{
        display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10,
      }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: statusColor }}>{statusLabel}</div>
        {(status === "complete" || status === "error") && (
          <button onClick={onClose} style={{
            background: "transparent", border: "1px solid hsla(0,0%,100%,.08)",
            borderRadius: 6, padding: "4px 10px", fontSize: 11, color: "#888", cursor: "pointer",
          }}>Close</button>
        )}
      </div>
      <div ref={logRef} style={{
        background: "#111", borderRadius: 10, padding: "12px 14px",
        maxHeight: 280, overflowY: "auto", fontFamily: "monospace", fontSize: 11,
        border: "1px solid hsla(0,0%,100%,.05)",
      }}>
        {logs.length === 0 && (
          <div style={{ color: "#555" }}>Waiting for pipeline output…</div>
        )}
        {logs.map((l, i) => (
          <div key={i} style={{
            color: l.level === "error" ? "#ef5350" : l.level === "warn" ? "#c5d44b" :
                   l.level === "success" ? "#00a86b" : "#aaa",
            marginBottom: 2, lineHeight: 1.5,
          }}>
            {l.ts && <span style={{ color: "#444", marginRight: 8 }}>{l.ts}</span>}
            {l.msg}
          </div>
        ))}
        {status === "running" && (
          <div style={{ color: "#555", marginTop: 6 }}>
            <span style={{ animation: "blink 1s infinite" }}>▌</span>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Main Admin Page ───────────────────────────────────────────────────────────
export default function AdminPage({ user, onSelectClient, onLogout }) {
  const [clients, setClients]           = useState([]);
  const [loadingClients, setLoadingClients] = useState(true);

  // Upload modal state
  const [uploadOpen, setUploadOpen]     = useState(false);
  const [uploadClient, setUploadClient] = useState("");
  const [newClientName, setNewClientName] = useState("");
  const [industry, setIndustry]         = useState("");
  const [dragging, setDragging]         = useState(false);
  const [uploadTab, setUploadTab]       = useState("files");
  const [contextText, setContextText]   = useState("");
  const [files, setFiles]               = useState([]);

  // Credentials state (persisted in localStorage for convenience)
  const [supabaseUrl, setSupabaseUrl]   = useState(() => localStorage.getItem("fiq_supabase_url") || "");
  const [serviceKey, setServiceKey]     = useState(() => localStorage.getItem("fiq_service_key") || "");
  const [anthropicKey, setAnthropicKey] = useState(() => localStorage.getItem("fiq_anthropic_key") || "");
  const [dryRun, setDryRun]             = useState(false);

  // Pipeline state
  const [serverReady, setServerReady]   = useState(null); // null=checking, true/false
  const [jobId, setJobId]               = useState(null);
  const [running, setRunning]           = useState(false);

  // Load clients from Supabase
  useEffect(() => {
    fetchClients();
  }, []);

  // Check if local server is running
  useEffect(() => {
    if (uploadOpen) {
      fetch(`${LOCAL_SERVER}/health`, { signal: AbortSignal.timeout(2000) })
        .then(r => r.ok ? setServerReady(true) : setServerReady(false))
        .catch(() => setServerReady(false));
    }
  }, [uploadOpen]);

  // Persist credentials
  useEffect(() => {
    if (supabaseUrl)  localStorage.setItem("fiq_supabase_url", supabaseUrl);
    if (serviceKey)   localStorage.setItem("fiq_service_key",  serviceKey);
    if (anthropicKey) localStorage.setItem("fiq_anthropic_key", anthropicKey);
  }, [supabaseUrl, serviceKey, anthropicKey]);

  function fetchClients() {
    setLoadingClients(true);
    supabase
      .from("clients")
      .select("*")
      .order("client_name")
      .then(({ data, error }) => {
        if (!error && data) setClients(data);
        setLoadingClients(false);
      });
  }

  const activeClients = clients.length;
  const readyClients  = clients.filter(c => c.silver_status === "complete").length;
  const totalRecords  = clients.reduce((sum, c) => sum + (c.total_records || 0), 0)
    .toLocaleString() + "+";

  // File handling
  function handleDrop(e) {
    e.preventDefault();
    setDragging(false);
    const dropped = Array.from(e.dataTransfer.files);
    setFiles(prev => {
      const names = new Set(prev.map(f => f.name));
      return [...prev, ...dropped.filter(f => !names.has(f.name))];
    });
  }

  function handleFileInput(e) {
    const picked = Array.from(e.target.files);
    setFiles(prev => {
      const names = new Set(prev.map(f => f.name));
      return [...prev, ...picked.filter(f => !names.has(f.name))];
    });
  }

  function removeFile(name) {
    setFiles(prev => prev.filter(f => f.name !== name));
  }

  // Determine effective client name
  const effectiveClientName = uploadClient === "new" ? newClientName : (clients.find(c => c.client_id === uploadClient)?.client_name || "");

  // Run pipeline
  async function handleRunPipeline() {
    if (!effectiveClientName || files.length === 0 || running) return;

    if (!serverReady) {
      alert(
        "Local pipeline server is not running.\n\n" +
        "Start it with:\n  cd '3. ETL Scripts'\n  python run_local_server.py"
      );
      return;
    }

    setRunning(true);
    setJobId(null);

    try {
      const form = new FormData();
      files.forEach(f => form.append("files", f));
      form.append("client_name",  effectiveClientName);
      form.append("client_id",    uploadClient !== "new" ? uploadClient : "");
      form.append("industry",     industry);
      form.append("context_text", contextText);
      form.append("supabase_url", supabaseUrl);
      form.append("service_key",  serviceKey);
      form.append("api_key",      anthropicKey);
      form.append("dry_run",      dryRun ? "true" : "false");

      const res = await fetch(`${LOCAL_SERVER}/run-pipeline`, { method: "POST", body: form });
      if (!res.ok) throw new Error(await res.text());

      const { job_id } = await res.json();
      setJobId(job_id);
      setUploadTab("progress");
    } catch (err) {
      alert(`Failed to start pipeline: ${err.message}`);
      setRunning(false);
    }
  }

  function handlePipelineComplete() {
    setRunning(false);
    fetchClients(); // refresh client list
  }

  function closeModal() {
    if (running) return; // block close while running
    setUploadOpen(false);
    setUploadClient("");
    setNewClientName("");
    setIndustry("");
    setFiles([]);
    setContextText("");
    setUploadTab("files");
    setJobId(null);
    setRunning(false);
  }

  const canRun = effectiveClientName.trim() && files.length > 0 && !running;

  const tabs = [
    { id: "files",    label: "Files" },
    { id: "context",  label: "Context" },
    { id: "settings", label: "Settings" },
    ...(jobId ? [{ id: "progress", label: "Progress" }] : []),
  ];

  return (
    <div style={{
      minHeight: "100vh",
      background: "#161618",
      color: "#f0f0f2",
      fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
    }}>
      {/* Top bar */}
      <div style={{
        padding: "0 28px", height: 56,
        borderBottom: "1px solid hsla(0,0%,100%,.04)",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        background: "#1e1e21", boxShadow: "0 2px 12px rgba(0,0,0,.3)",
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
            { label: "Active Clients",  value: loadingClients ? "—" : activeClients,                        color: "#c5d44b" },
            { label: "Silver-Ready",    value: loadingClients ? "—" : `${readyClients} / ${activeClients}`, color: "#00a86b" },
            { label: "Total Records",   value: loadingClients ? "—" : totalRecords,                         color: "#3e8c7f" },
            { label: "Pipeline Stage",  value: "Bronze → Silver",                                            color: "#c5d44b" },
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
          >+ Upload Client Data</button>
        </div>

        {/* Client cards */}
        {loadingClients ? (
          <div style={{ textAlign: "center", padding: "60px", color: "#555", fontSize: 13 }}>
            Loading clients...
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {clients.map(c => (
              <div
                key={c.client_id}
                onClick={() => onSelectClient({ id: c.client_id, name: c.client_name })}
                style={{
                  background: "#1e1e21", borderRadius: 14, padding: "18px 22px",
                  border: `1px solid ${c.warning_message ? "rgba(239,83,80,.2)" : "hsla(0,0%,100%,.04)"}`,
                  cursor: "pointer",
                  display: "grid",
                  gridTemplateColumns: "2fr 1fr 1fr 1fr 120px",
                  alignItems: "center", gap: 16, transition: "all 0.2s",
                  boxShadow: "4px 4px 12px rgba(0,0,0,.4)",
                }}
                onMouseEnter={e => {
                  e.currentTarget.style.borderColor = "rgba(197,212,75,.25)";
                  e.currentTarget.style.transform = "translateY(-1px)";
                }}
                onMouseLeave={e => {
                  e.currentTarget.style.borderColor = c.warning_message ? "rgba(239,83,80,.2)" : "hsla(0,0%,100%,.04)";
                  e.currentTarget.style.transform = "translateY(0)";
                }}
              >
                {/* Name + industry */}
                <div>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 4 }}>
                    <span style={{ fontSize: 14, fontWeight: 700, color: "#f0f0f2" }}>{c.client_name}</span>
                    <span style={{
                      fontSize: 10, fontWeight: 700, padding: "2px 8px", borderRadius: 9999,
                      background: `${GRADE_COLOR[c.data_quality_grade] || "#888"}22`,
                      color: GRADE_COLOR[c.data_quality_grade] || "#888",
                      border: `1px solid ${GRADE_COLOR[c.data_quality_grade] || "#888"}44`,
                    }}>{c.data_quality_grade}</span>
                  </div>
                  <div style={{ fontSize: 11, color: "#666" }}>{c.industry}</div>
                  {c.warning_message && (
                    <div style={{ fontSize: 10, color: "#ef5350", marginTop: 4 }}>⚠ {c.warning_message}</div>
                  )}
                  <div style={{ display: "flex", gap: 5, marginTop: 6 }}>
                    {(c.tags || []).map(t => (
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
                  {["bronze_status", "silver_status", "gold_status"].map(key => {
                    const label = key.replace("_status", "");
                    return (
                      <div key={key} style={{ display: "flex", alignItems: "center", marginBottom: 3 }}>
                        <StageDot stage={c[key]} />
                        <span style={{ fontSize: 11, color: c[key] === "complete" ? "#888" : "#555", textTransform: "capitalize" }}>
                          {label}
                        </span>
                      </div>
                    );
                  })}
                </div>

                {/* Data stats */}
                <div>
                  <div style={{ fontSize: 10, color: "#555", marginBottom: 6, textTransform: "uppercase" }}>Data</div>
                  <div style={{ fontSize: 12, color: "#888", marginBottom: 2 }}>{c.total_tables} tables</div>
                  <div style={{ fontSize: 12, color: "#888", marginBottom: 2 }}>{(c.total_records || 0).toLocaleString()} records</div>
                  <div style={{ fontSize: 12, color: "#888" }}>Rev: {c.revenue_label || "—"}</div>
                </div>

                {/* Readiness */}
                <div>
                  <div style={{ fontSize: 10, color: "#555", marginBottom: 6, textTransform: "uppercase" }}>Readiness</div>
                  {(() => {
                    const done  = READINESS_ITEMS.filter(r => c[r.key]).length;
                    const total = READINESS_ITEMS.length;
                    const color = Math.round((done / total) * 100) === 100 ? "#00a86b" : done >= 2 ? "#c5d44b" : "#ef5350";
                    return (
                      <>
                        <div style={{ fontSize: 16, fontWeight: 800, color, marginBottom: 4 }}>{done}/{total}</div>
                        <div style={{ display: "flex", gap: 3 }}>
                          {READINESS_ITEMS.map(r => (
                            <div key={r.key} title={r.label} style={{
                              width: 18, height: 4, borderRadius: 2,
                              background: c[r.key] ? "#00a86b" : "hsla(0,0%,100%,.08)",
                            }} />
                          ))}
                        </div>
                        <div style={{ fontSize: 9, color: "#555", marginTop: 4 }}>
                          {READINESS_ITEMS.filter(r => !c[r.key]).map(r => r.label).join(", ") || "All clear"}
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
                  <div style={{ fontSize: 10, color: "#555", marginTop: 5 }}>
                    Updated {c.last_run_at
                      ? new Date(c.last_run_at).toLocaleDateString("en-US", { month: "short", day: "numeric" })
                      : "—"}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* ── Upload Modal ─────────────────────────────────────────────── */}
        {uploadOpen && (
          <div style={{
            position: "fixed", inset: 0, background: "rgba(0,0,0,.7)",
            display: "flex", alignItems: "center", justifyContent: "center",
            zIndex: 100, backdropFilter: "blur(4px)",
          }}
            onClick={closeModal}
          >
            <div
              style={{
                background: "#1e1e21", borderRadius: 18, padding: "30px 34px",
                width: "100%", maxWidth: 500,
                border: "1px solid hsla(0,0%,100%,.06)",
                boxShadow: "0 30px 80px rgba(0,0,0,.6)",
              }}
              onClick={e => e.stopPropagation()}
            >
              {/* Header */}
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16 }}>
                <div>
                  <h3 style={{ margin: "0 0 4px", fontSize: 17, fontWeight: 700, color: "#f0f0f2" }}>Upload Client Data</h3>
                  <p style={{ margin: 0, fontSize: 11, color: "#555" }}>
                    Pipeline runs locally — data never leaves your machine
                  </p>
                </div>
                {/* Server status indicator */}
                <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 10 }}>
                  <div style={{
                    width: 6, height: 6, borderRadius: "50%",
                    background: serverReady === null ? "#555" : serverReady ? "#00a86b" : "#ef5350",
                    boxShadow: serverReady ? "0 0 6px rgba(0,168,107,.5)" : "none",
                  }} />
                  <span style={{ color: "#555" }}>
                    {serverReady === null ? "checking…" : serverReady ? "server ready" : "server offline"}
                  </span>
                </div>
              </div>

              {/* Client selector */}
              <div style={{ marginBottom: 12 }}>
                <label style={{ fontSize: 11, color: "#888", display: "block", marginBottom: 5 }}>Client</label>
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
                  <option value="">Select client…</option>
                  {clients.map(c => <option key={c.client_id} value={c.client_id}>{c.client_name}</option>)}
                  <option value="new">+ New client</option>
                </select>
              </div>

              {/* New client fields */}
              {uploadClient === "new" && (
                <div style={{ display: "flex", gap: 10, marginBottom: 12 }}>
                  <div style={{ flex: 2 }}>
                    <label style={{ fontSize: 11, color: "#888", display: "block", marginBottom: 5 }}>Client Name</label>
                    <input
                      value={newClientName}
                      onChange={e => setNewClientName(e.target.value)}
                      placeholder="e.g. Acme Corp"
                      style={{
                        width: "100%", boxSizing: "border-box",
                        background: "#222225", border: "1px solid hsla(0,0%,100%,.08)",
                        borderRadius: 8, padding: "9px 12px", fontSize: 13, color: "#f0f0f2",
                      }}
                    />
                  </div>
                  <div style={{ flex: 1 }}>
                    <label style={{ fontSize: 11, color: "#888", display: "block", marginBottom: 5 }}>Industry</label>
                    <input
                      value={industry}
                      onChange={e => setIndustry(e.target.value)}
                      placeholder="e.g. SaaS"
                      style={{
                        width: "100%", boxSizing: "border-box",
                        background: "#222225", border: "1px solid hsla(0,0%,100%,.08)",
                        borderRadius: 8, padding: "9px 12px", fontSize: 13, color: "#f0f0f2",
                      }}
                    />
                  </div>
                </div>
              )}

              {/* Tabs */}
              <div style={{ display: "flex", borderBottom: "1px solid hsla(0,0%,100%,.06)", marginBottom: 16 }}>
                {tabs.map(tab => (
                  <button key={tab.id} onClick={() => setUploadTab(tab.id)} style={{
                    flex: 1, background: "transparent", border: "none",
                    borderBottom: `2px solid ${uploadTab === tab.id ? "#c5d44b" : "transparent"}`,
                    padding: "8px 0", fontSize: 11,
                    fontWeight: uploadTab === tab.id ? 700 : 400,
                    color: uploadTab === tab.id ? "#c5d44b" : "#666",
                    cursor: "pointer", transition: "all 0.2s",
                  }}>{tab.label}{tab.id === "files" && files.length > 0 ? ` (${files.length})` : ""}</button>
                ))}
              </div>

              {/* Tab: Files */}
              {uploadTab === "files" && (
                <>
                  <div
                    onDragOver={e => { e.preventDefault(); setDragging(true); }}
                    onDragLeave={() => setDragging(false)}
                    onDrop={handleDrop}
                    onClick={() => document.getElementById("fileInput").click()}
                    style={{
                      border: `2px dashed ${dragging ? "#c5d44b" : "hsla(0,0%,100%,.1)"}`,
                      borderRadius: 12, padding: "20px 20px", textAlign: "center",
                      background: dragging ? "rgba(197,212,75,.05)" : "transparent",
                      transition: "all 0.2s", cursor: "pointer", marginBottom: 10,
                    }}
                  >
                    <div style={{ fontSize: 24, marginBottom: 6 }}>📂</div>
                    <div style={{ fontSize: 12, color: "#888", marginBottom: 3 }}>Drag & drop files, or click to browse</div>
                    <div style={{ fontSize: 10, color: "#555" }}>Excel (.xlsx, .xlsb, .xls), CSV — any size</div>
                    <input id="fileInput" type="file" multiple accept=".xlsx,.xlsb,.xls,.csv,.tsv"
                      onChange={handleFileInput} style={{ display: "none" }} />
                  </div>

                  {files.length > 0 && (
                    <div style={{ maxHeight: 140, overflowY: "auto" }}>
                      {files.map(f => (
                        <div key={f.name} style={{
                          display: "flex", justifyContent: "space-between", alignItems: "center",
                          padding: "5px 8px", borderRadius: 6, marginBottom: 3,
                          background: "#222225", fontSize: 11,
                        }}>
                          <span style={{ color: "#aaa", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 340 }}>
                            {f.name}
                          </span>
                          <div style={{ display: "flex", alignItems: "center", gap: 8, flexShrink: 0 }}>
                            <span style={{ color: "#555" }}>{(f.size / 1024 / 1024).toFixed(1)}MB</span>
                            <button onClick={() => removeFile(f.name)} style={{
                              background: "transparent", border: "none", color: "#555",
                              cursor: "pointer", fontSize: 13, lineHeight: 1, padding: 0,
                            }}>×</button>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </>
              )}

              {/* Tab: Context */}
              {uploadTab === "context" && (
                <div>
                  <div style={{ fontSize: 11, color: "#888", marginBottom: 8 }}>
                    Add business context to help the AI understand this client's data and terminology.
                  </div>
                  <textarea
                    value={contextText}
                    onChange={e => setContextText(e.target.value)}
                    placeholder="e.g. Pricing tiers, key terminology, discount approval thresholds, industry-specific context..."
                    style={{
                      width: "100%", boxSizing: "border-box", minHeight: 140,
                      background: "#222225", border: "1px solid hsla(0,0%,100%,.08)",
                      borderRadius: 10, padding: "12px 14px", fontSize: 12,
                      color: "#f0f0f2", resize: "vertical", outline: "none",
                      lineHeight: 1.6, fontFamily: "inherit",
                    }}
                  />
                </div>
              )}

              {/* Tab: Settings */}
              {uploadTab === "settings" && (
                <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                  {[
                    { label: "Supabase URL", value: supabaseUrl, set: setSupabaseUrl, placeholder: "https://xxx.supabase.co" },
                    { label: "Service Role Key", value: serviceKey, set: setServiceKey, placeholder: "eyJ..." },
                    { label: "Anthropic API Key", value: anthropicKey, set: setAnthropicKey, placeholder: "sk-ant-..." },
                  ].map(({ label, value, set, placeholder }) => (
                    <div key={label}>
                      <label style={{ fontSize: 11, color: "#888", display: "block", marginBottom: 4 }}>{label}</label>
                      <input
                        type="password"
                        value={value}
                        onChange={e => set(e.target.value)}
                        placeholder={placeholder}
                        style={{
                          width: "100%", boxSizing: "border-box",
                          background: "#222225", border: "1px solid hsla(0,0%,100%,.08)",
                          borderRadius: 8, padding: "9px 12px", fontSize: 12, color: "#f0f0f2",
                        }}
                      />
                    </div>
                  ))}
                  <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", marginTop: 4 }}>
                    <input type="checkbox" checked={dryRun} onChange={e => setDryRun(e.target.checked)} />
                    <span style={{ fontSize: 11, color: "#888" }}>Dry run (skip Supabase writes)</span>
                  </label>
                  <p style={{ fontSize: 10, color: "#444", margin: 0 }}>
                    Credentials are stored in browser localStorage only.
                  </p>
                </div>
              )}

              {/* Tab: Progress */}
              {uploadTab === "progress" && jobId && (
                <PipelineProgress
                  jobId={jobId}
                  onClose={closeModal}
                  onComplete={handlePipelineComplete}
                />
              )}

              {/* Footer buttons */}
              {uploadTab !== "progress" && (
                <div style={{ display: "flex", gap: 10, marginTop: 18 }}>
                  <button onClick={closeModal} style={{
                    flex: 1, background: "transparent",
                    border: "1px solid hsla(0,0%,100%,.08)", borderRadius: 9,
                    padding: "10px", fontSize: 12, color: "#888", cursor: "pointer",
                  }}>Cancel</button>

                  {serverReady ? (
                    <button
                      onClick={handleRunPipeline}
                      disabled={!canRun}
                      style={{
                        flex: 2,
                        background: canRun ? "linear-gradient(135deg, #c5d44b, #3e8c7f)" : "#2a2a2d",
                        border: "none", borderRadius: 9, padding: "10px",
                        fontSize: 12, fontWeight: 600,
                        color: canRun ? "#161618" : "#444",
                        cursor: canRun ? "pointer" : "not-allowed",
                        transition: "all 0.2s",
                      }}
                    >
                      {running ? "Running…" : "Run Pipeline →"}
                    </button>
                  ) : (
                    <div style={{
                      flex: 2, background: "#222225", border: "1px solid #333",
                      borderRadius: 9, padding: "8px 12px", fontSize: 10, color: "#555",
                      display: "flex", flexDirection: "column", gap: 2,
                    }}>
                      <span style={{ color: "#ef5350" }}>⚠ Local server not running</span>
                      <code style={{ fontSize: 9, color: "#666" }}>python run_local_server.py</code>
                    </div>
                  )}
                </div>
              )}

              {uploadTab !== "progress" && (
                <p style={{ fontSize: 10, color: "#444", marginTop: 10, textAlign: "center" }}>
                  Pipeline runs on your local machine · files processed in a temp directory
                </p>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
