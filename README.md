# FintastIQ Pricing Diagnostic Platform

An internal tool for running pricing diagnostics across client accounts — including data ingestion, ETL processing, and an interactive dashboard for analysis and opportunity identification.

---

## Project Structure

```
Project Pricing Diagnostics/
├── 1. Client Data/              # Raw client data files (not committed to git)
├── 2. Cleaned Output/           # Processed outputs (not committed to git)
├── 3. ETL Scripts/              # Python scripts for data ingestion and processing
├── 4. Analysis & Prompts/       # Claude prompts and analysis templates
├── 5. fintastiq-frontend/       # React + Vite web application
└── README.md
```

---

## Frontend (fintastiq-frontend)

Built with **React + Vite** and **Recharts** for data visualisation.

### Getting started

```bash
cd "5. fintastiq-frontend"
npm install
npm run dev
```

The app will be available at `http://localhost:5173`.

### Key views

- **Dashboard** — Overview cards and KPI charts for the active client
- **Revenue / Margin / Volume** — Segment-level charts with filter bar (Segment, Region, Product, Time Period)
- **Opportunities** — Identified pricing opportunities and recommendations
- **Table Builder** — Custom column picker for exporting sliced data views
- **Admin** — Client management, file uploads, context notes, and readiness tracking

### Building for production

```bash
npm run build
```

Output goes to `dist/`. Deploy via Vercel by connecting this repo — set the root directory to `5. fintastiq-frontend`.

---

## ETL Scripts (3. ETL Scripts)

Python scripts that ingest raw client files, clean them, and produce analysis-ready outputs.

### Setup

```bash
cd "3. ETL Scripts"
pip install -r requirements.txt   # if present
cp .env.example .env              # then add your API key
```

### Environment variables

Create a `.env` file (see `.env.example`):

```
ANTHROPIC_API_KEY=your-key-here
```

> **Never commit your `.env` file.** It is gitignored.

### Scripts

| Script | Purpose |
|---|---|
| `run_gravitate_bronze.py` | Ingest raw Gravitate data |
| `run_ansell_bronze.py` | Ingest raw Ansell data |
| `run_rxbenefits_bronze.py` | Ingest raw RxBenefits data |
| `run_gravitate_silver.py` | Clean and transform Gravitate data |
| `context_intake.py` | Process client context notes |

---

## Deployment

The frontend is designed to be deployed on **Vercel**:

1. Connect this GitHub repo to a new Vercel project
2. Set the **root directory** to `5. fintastiq-frontend`
3. Framework preset: **Vite**
4. Build command: `npm run build`
5. Output directory: `dist`

---

## Git workflow

Client data folders (`1. Client Data/`, `2. Cleaned Output/`) are excluded from git for confidentiality. Push changes after each working session:

```bash
git add .
git commit -m "your message"
git push
```

> If you're working from OneDrive, pause sync before running git commands to avoid lock file errors.

---

## Team

Built by the FintastIQ team. Questions → raghav@fintastiq.com
