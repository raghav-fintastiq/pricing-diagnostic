import { useState } from "react";

const DEMO_HINTS = [
  { label: "FintastIQ Admin", email: "admin@fintastiq.com" },
  { label: "Gravitate (Client)", email: "demo@gravitate.com" },
  { label: "RxBenefits (Client)", email: "demo@rxbenefits.com" },
];

export default function LoginPage({ onLogin }) {
  const [email, setEmail] = useState("");
  const [sent, setSent] = useState(false);
  const [loading, setLoading] = useState(false);

  const handleSend = (e) => {
    e.preventDefault();
    if (!email.trim()) return;
    setLoading(true);
    // Simulate magic link send — in production: Supabase signInWithOtp
    setTimeout(() => {
      setLoading(false);
      setSent(true);
    }, 900);
  };

  const handleDemoLogin = (demoEmail) => {
    setEmail(demoEmail);
    onLogin(demoEmail);
  };

  return (
    <div style={{
      minHeight: "100vh",
      background: "#161618",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
      padding: 24,
    }}>
      <div style={{ width: "100%", maxWidth: 420 }}>

        {/* Logo */}
        <div style={{ textAlign: "center", marginBottom: 40 }}>
          <div style={{
            width: 52,
            height: 52,
            borderRadius: 14,
            background: "linear-gradient(135deg, #c5d44b, #3e8c7f)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontWeight: 900,
            fontSize: 22,
            color: "#161618",
            boxShadow: "0 0 30px rgba(197,212,75,.35)",
            margin: "0 auto 14px",
          }}>F</div>
          <div style={{ fontSize: 22, fontWeight: 800, color: "#f0f0f2" }}>FintastIQ</div>
          <div style={{ fontSize: 13, color: "#888", marginTop: 4 }}>Pricing Diagnostic Platform</div>
        </div>

        {/* Card */}
        <div style={{
          background: "#1e1e21",
          borderRadius: 18,
          padding: "32px 36px",
          border: "1px solid hsla(0,0%,100%,.05)",
          boxShadow: "0 20px 60px rgba(0,0,0,.5)",
        }}>
          {!sent ? (
            <>
              <h2 style={{ margin: "0 0 6px", fontSize: 18, fontWeight: 700, color: "#f0f0f2" }}>Sign in</h2>
              <p style={{ margin: "0 0 24px", fontSize: 13, color: "#888" }}>
                Enter your work email and we'll send you a secure sign-in link — no password needed.
              </p>

              <form onSubmit={handleSend}>
                <label style={{ fontSize: 12, color: "#888", display: "block", marginBottom: 6 }}>
                  Work email
                </label>
                <input
                  type="email"
                  value={email}
                  onChange={e => setEmail(e.target.value)}
                  placeholder="you@company.com"
                  required
                  style={{
                    width: "100%",
                    background: "#222225",
                    border: "1px solid hsla(0,0%,100%,.08)",
                    borderRadius: 10,
                    padding: "11px 14px",
                    fontSize: 14,
                    color: "#f0f0f2",
                    outline: "none",
                    boxSizing: "border-box",
                    marginBottom: 14,
                    transition: "border-color 0.2s",
                  }}
                  onFocus={e => e.target.style.borderColor = "#c5d44b"}
                  onBlur={e => e.target.style.borderColor = "hsla(0,0%,100%,.08)"}
                />
                <button
                  type="submit"
                  disabled={loading}
                  style={{
                    width: "100%",
                    background: loading ? "#333" : "linear-gradient(135deg, #c5d44b, #3e8c7f)",
                    border: "none",
                    borderRadius: 10,
                    padding: "12px",
                    fontSize: 14,
                    fontWeight: 600,
                    color: loading ? "#888" : "#161618",
                    cursor: loading ? "not-allowed" : "pointer",
                    transition: "all 0.2s",
                    boxShadow: loading ? "none" : "0 0 20px rgba(197,212,75,.25)",
                  }}
                >
                  {loading ? "Sending..." : "Continue with email →"}
                </button>
              </form>

              {/* Demo shortcuts */}
              <div style={{ marginTop: 28, paddingTop: 20, borderTop: "1px solid hsla(0,0%,100%,.04)" }}>
                <div style={{ fontSize: 11, color: "#666", marginBottom: 10, textAlign: "center" }}>
                  DEMO — click to sign in instantly
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  {DEMO_HINTS.map(d => (
                    <button
                      key={d.email}
                      onClick={() => handleDemoLogin(d.email)}
                      style={{
                        background: "#222225",
                        border: "1px solid hsla(0,0%,100%,.06)",
                        borderRadius: 8,
                        padding: "9px 14px",
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        cursor: "pointer",
                        transition: "all 0.2s",
                      }}
                      onMouseEnter={e => e.currentTarget.style.borderColor = "rgba(197,212,75,.3)"}
                      onMouseLeave={e => e.currentTarget.style.borderColor = "hsla(0,0%,100%,.06)"}
                    >
                      <span style={{ fontSize: 12, fontWeight: 600, color: "#f0f0f2" }}>{d.label}</span>
                      <span style={{ fontSize: 11, color: "#666" }}>{d.email}</span>
                    </button>
                  ))}
                </div>
              </div>
            </>
          ) : (
            <div style={{ textAlign: "center", padding: "16px 0" }}>
              <div style={{ fontSize: 36, marginBottom: 16 }}>✉️</div>
              <h3 style={{ color: "#f0f0f2", margin: "0 0 8px", fontSize: 17, fontWeight: 700 }}>Check your inbox</h3>
              <p style={{ color: "#888", fontSize: 13, margin: "0 0 20px", lineHeight: 1.6 }}>
                We sent a secure sign-in link to <strong style={{ color: "#f0f0f2" }}>{email}</strong>.<br />
                Click the link to access your dashboard — it expires in 10 minutes.
              </p>
              <button
                onClick={() => setSent(false)}
                style={{
                  background: "transparent",
                  border: "1px solid hsla(0,0%,100%,.08)",
                  borderRadius: 8,
                  padding: "8px 18px",
                  fontSize: 12,
                  color: "#888",
                  cursor: "pointer",
                }}
              >
                ← Use a different email
              </button>

              {/* In demo, also show bypass */}
              <div style={{ marginTop: 20, paddingTop: 16, borderTop: "1px solid hsla(0,0%,100%,.04)" }}>
                <div style={{ fontSize: 11, color: "#666", marginBottom: 8 }}>DEMO — skip email verification</div>
                <button
                  onClick={() => onLogin(email)}
                  style={{
                    background: "linear-gradient(135deg, #c5d44b, #3e8c7f)",
                    border: "none",
                    borderRadius: 8,
                    padding: "9px 20px",
                    fontSize: 12,
                    fontWeight: 600,
                    color: "#161618",
                    cursor: "pointer",
                  }}
                >
                  Enter dashboard →
                </button>
              </div>
            </div>
          )}
        </div>

        <p style={{ textAlign: "center", fontSize: 11, color: "#555", marginTop: 20 }}>
          Secured by row-level encryption · Data never leaves your environment
        </p>
      </div>
    </div>
  );
}
