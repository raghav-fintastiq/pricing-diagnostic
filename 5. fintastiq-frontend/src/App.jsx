import { useState } from "react";
import Dashboard from "./Dashboard.jsx";
import LoginPage from "./LoginPage.jsx";
import AdminPage from "./AdminPage.jsx";

// Demo accounts — role + clientId maps to Supabase client_id
const DEMO_USERS = {
  "admin@fintastiq.com":  { role: "admin",  name: "Raghav Hada",  org: "FintastIQ" },
  "demo@gravitate.com":   { role: "client", name: "Client Demo",  org: "Gravitate Energy",   clientId: "gravitate" },
  "demo@rxbenefits.com":  { role: "client", name: "Client Demo",  org: "RxBenefits",         clientId: "rxbenefits" },
  "demo@ansell.com":      { role: "client", name: "Client Demo",  org: "Ansell",             clientId: "ansell" },
  "demo@npi.com":         { role: "client", name: "Client Demo",  org: "NPI",                clientId: "npi" },
  "demo@acmecorp.com":    { role: "client", name: "Client Demo",  org: "AcmeCorp Software",  clientId: "acmecorp" },
};

export default function App() {
  const [user, setUser] = useState(null);
  // adminSelectedClient = { id: "acmecorp", name: "AcmeCorp Software" }
  const [adminSelectedClient, setAdminSelectedClient] = useState(null);

  const handleLogin = (email) => {
    const found = DEMO_USERS[email.toLowerCase().trim()];
    if (found) {
      setUser({ email, ...found });
    } else {
      setUser({ email, role: "admin", name: email.split("@")[0], org: "FintastIQ" });
    }
  };

  const handleLogout = () => {
    setUser(null);
    setAdminSelectedClient(null);
  };

  if (!user) {
    return <LoginPage onLogin={handleLogin} />;
  }

  if (user.role === "admin") {
    if (adminSelectedClient) {
      return (
        <Dashboard
          clientId={adminSelectedClient.id}
          clientName={adminSelectedClient.name}
          userRole="admin"
          userName={user.name}
          onBack={() => setAdminSelectedClient(null)}
          onLogout={handleLogout}
        />
      );
    }
    return (
      <AdminPage
        user={user}
        onSelectClient={setAdminSelectedClient}
        onLogout={handleLogout}
      />
    );
  }

  // Client role — goes straight to their dashboard
  return (
    <Dashboard
      clientId={user.clientId || "acmecorp"}
      clientName={user.org}
      userRole="client"
      userName={user.name}
      onLogout={handleLogout}
    />
  );
}
