import { useState } from "react";
import Dashboard from "./Dashboard.jsx";
import LoginPage from "./LoginPage.jsx";
import AdminPage from "./AdminPage.jsx";

// Demo accounts — in production these come from Supabase auth (magic link)
const DEMO_USERS = {
  "admin@fintastiq.com": { role: "admin", name: "Raghav Hada", org: "FintastIQ" },
  "demo@gravitate.com": { role: "client", name: "Client Demo", org: "Gravitate Energy", client: "Gravitate Energy" },
  "demo@rxbenefits.com": { role: "client", name: "Client Demo", org: "RxBenefits", client: "RxBenefits" },
  "demo@ansell.com": { role: "client", name: "Client Demo", org: "Ansell", client: "Ansell" },
  "demo@npi.com": { role: "client", name: "Client Demo", org: "NPI", client: "NPI" },
};

export default function App() {
  const [user, setUser] = useState(null);                   // null = logged out
  const [adminSelectedClient, setAdminSelectedClient] = useState(null);

  const handleLogin = (email) => {
    const found = DEMO_USERS[email.toLowerCase().trim()];
    if (found) {
      setUser({ email, ...found });
    } else {
      // Default: treat as admin for demo purposes
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
          clientName={adminSelectedClient}
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
      clientName={user.client || user.org}
      userRole="client"
      userName={user.name}
      onLogout={handleLogout}
    />
  );
}
