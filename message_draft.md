Hey team — the Pricing Diagnostic project is now on Git and ready to go.

**Repo structure:**
- `0. Context/` — PRD, playbook, sprint plan
- `1. Data Schema/` — canonical schema + dependency map
- `3. ETL Scripts/` — Bronze & Silver pipeline scripts (Gravitate, Ansell, RxBenefits, NPI)
- `5. fintastiq-frontend/` — React dashboard (Vite + Recharts)

**Frontend** (`5. fintastiq-frontend/`):
To run locally: `npm install && npm run dev`
To connect to Vercel: point it at the `5. fintastiq-frontend` subfolder as the root directory. It's a standard Vite app — Vercel auto-detects the build settings.

**Note:** Client data and cleaned outputs are gitignored (sensitive). API keys have been moved to env vars — set `ANTHROPIC_API_KEY` before running any ETL script.
