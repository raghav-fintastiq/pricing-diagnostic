"""
FintastIQ Local Pipeline Server
Runs on localhost:5001 and lets the frontend trigger ETL pipelines.

Usage:
    pip install fastapi uvicorn python-multipart
    python run_local_server.py [--port 5001]

The frontend (AdminPage) calls:
    POST /run-pipeline  — upload files + metadata, starts full Bronze→Silver→Ingest
    GET  /job/{job_id}  — SSE stream of log lines for a running job
    GET  /jobs          — list all recent jobs
    GET  /health        — alive check (frontend pings this to know server is up)
"""

import argparse, json, os, queue, subprocess, sys, tempfile, threading, time, uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── FastAPI ────────────────────────────────────────────────────────────────────
try:
    from fastapi import FastAPI, File, Form, UploadFile, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import StreamingResponse, JSONResponse
    import uvicorn
except ImportError:
    print("ERROR: Missing dependencies. Run:")
    print("  pip install fastapi uvicorn python-multipart")
    sys.exit(1)

app = FastAPI(title="FintastIQ Local Pipeline Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # localhost dev — restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Job state ──────────────────────────────────────────────────────────────────
JOBS: dict[str, dict] = {}   # job_id → {status, log_queue, created_at, meta}
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))


# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return {"status": "ok", "server": "FintastIQ Local Pipeline Server", "timestamp": datetime.now().isoformat()}


# ═══════════════════════════════════════════════════════════════════════════════
# RUN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/run-pipeline")
async def run_pipeline(
    files: list[UploadFile] = File(...),
    client_name: str = Form(...),
    client_id: str = Form(""),
    industry: str = Form(""),
    context_text: str = Form(""),
    supabase_url: str = Form(""),
    service_key: str = Form(""),
    api_key: str = Form(""),
    dry_run: bool = Form(False),
):
    """
    Accept uploaded files + metadata, save to temp dir, run full pipeline.
    Returns a job_id immediately; stream progress via GET /job/{job_id}.
    """
    if not files:
        raise HTTPException(400, "No files provided")
    if not client_name.strip():
        raise HTTPException(400, "client_name is required")

    job_id = str(uuid.uuid4())[:8]
    log_q: queue.Queue = queue.Queue()

    JOBS[job_id] = {
        "status": "queued",
        "log_queue": log_q,
        "created_at": datetime.now().isoformat(),
        "client_name": client_name,
        "client_id": client_id or _slugify(client_name),
    }

    # Save uploaded files to a temp directory
    work_dir = tempfile.mkdtemp(prefix=f"fintastiq_{job_id}_")
    client_folder = os.path.join(work_dir, f"{client_name} Client Data")
    os.makedirs(client_folder, exist_ok=True)

    for f in files:
        dest = os.path.join(client_folder, f.filename)
        content = await f.read()
        with open(dest, "wb") as out:
            out.write(content)

    # Save context JSON if provided
    if context_text.strip():
        ctx = {"client_name": client_name, "industry": industry, "context": context_text}
        with open(os.path.join(client_folder, "client_context.json"), "w") as f:
            json.dump(ctx, f, indent=2)

    # Launch pipeline in background thread
    threading.Thread(
        target=_run_pipeline_thread,
        args=(job_id, work_dir, client_folder, client_name, client_id or _slugify(client_name),
              industry, supabase_url, service_key, api_key, dry_run, log_q),
        daemon=True,
    ).start()

    return {"job_id": job_id, "status": "queued", "message": f"Pipeline started for '{client_name}'"}


def _slugify(name: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _log(q: queue.Queue, msg: str, level: str = "info"):
    ts = datetime.now().strftime("%H:%M:%S")
    q.put(json.dumps({"ts": ts, "level": level, "msg": msg}))
    print(f"[{level.upper()}] {msg}")


def _run_pipeline_thread(
    job_id, work_dir, client_folder, client_name, client_id,
    industry, supabase_url, service_key, api_key, dry_run, log_q
):
    JOBS[job_id]["status"] = "running"
    try:
        bronze_out = os.path.join(work_dir, f"{client_name}_Bronze")
        silver_out = os.path.join(work_dir, f"{client_name}_Silver")
        os.makedirs(bronze_out, exist_ok=True)
        os.makedirs(silver_out, exist_ok=True)

        python = sys.executable

        # ── STAGE 1: Bronze ──────────────────────────────────────────────────
        _log(log_q, f"▶ Stage 1/3 — Bronze ETL for '{client_name}'")
        bronze_script = os.path.join(SCRIPTS_DIR, "run_bronze_universal.py")
        bronze_cmd = [
            python, bronze_script,
            "--folder", client_folder,
            "--output", bronze_out,
            "--client", client_name,
        ]
        if api_key:
            bronze_cmd += ["--api-key", api_key]

        ok = _stream_subprocess(bronze_cmd, log_q, env_extra={"ANTHROPIC_API_KEY": api_key})
        if not ok:
            raise RuntimeError("Bronze ETL failed")
        _log(log_q, "✓ Bronze complete", "success")

        # ── STAGE 2: Silver ──────────────────────────────────────────────────
        _log(log_q, f"▶ Stage 2/3 — Silver Cleaning for '{client_name}'")
        silver_script = os.path.join(SCRIPTS_DIR, "run_silver_universal.py")
        silver_cmd = [
            python, silver_script,
            "--folder", bronze_out,
            "--output", silver_out,
            "--client", client_name,
        ]
        if api_key:
            silver_cmd += ["--api-key", api_key]

        ok = _stream_subprocess(silver_cmd, log_q, env_extra={"ANTHROPIC_API_KEY": api_key})
        if not ok:
            _log(log_q, "⚠ Silver cleaning failed — continuing to ingest with bronze data", "warn")
            silver_out = bronze_out  # Fall back to bronze

        _log(log_q, "✓ Silver complete", "success")

        # ── STAGE 3: Supabase Ingest ─────────────────────────────────────────
        _log(log_q, f"▶ Stage 3/3 — Supabase Ingest for '{client_name}'")
        if not supabase_url or not service_key:
            _log(log_q, "⚠ Supabase credentials not provided — running dry-run only", "warn")
            dry_run = True

        ingest_script = os.path.join(SCRIPTS_DIR, "run_supabase_ingest.py")
        ingest_cmd = [
            python, ingest_script,
            "--folder", silver_out,
            "--client-name", client_name,
            "--client-id", client_id,
            "--industry", industry or "Unknown",
        ]
        if supabase_url:  ingest_cmd += ["--supabase-url", supabase_url]
        if service_key:   ingest_cmd += ["--service-key",  service_key]
        if api_key:       ingest_cmd += ["--api-key",      api_key]
        if dry_run:       ingest_cmd.append("--dry-run")

        ok = _stream_subprocess(ingest_cmd, log_q, env_extra={
            "SUPABASE_URL": supabase_url,
            "SUPABASE_SERVICE_KEY": service_key,
            "ANTHROPIC_API_KEY": api_key,
        })
        if not ok and not dry_run:
            raise RuntimeError("Supabase ingest failed")

        _log(log_q, "✓ Ingest complete", "success")
        _log(log_q, f"🎉 Pipeline complete for '{client_name}'! Refresh the dashboard to see data.", "success")
        JOBS[job_id]["status"] = "complete"

    except Exception as e:
        _log(log_q, f"✗ Pipeline failed: {str(e)}", "error")
        JOBS[job_id]["status"] = "error"
    finally:
        log_q.put(None)  # sentinel — done


def _stream_subprocess(cmd: list, log_q: queue.Queue, env_extra: dict = None) -> bool:
    """Run subprocess, stream stdout/stderr to log_q. Returns True if exit code 0."""
    env = os.environ.copy()
    if env_extra:
        for k, v in env_extra.items():
            if v:
                env[k] = v

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1, env=env
    )
    for line in proc.stdout:
        line = line.rstrip()
        if line:
            level = "error" if "ERROR" in line or "✗" in line else \
                    "warn"  if "WARNING" in line or "⚠" in line else "info"
            log_q.put(json.dumps({"ts": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": line}))
    proc.wait()
    return proc.returncode == 0


# ═══════════════════════════════════════════════════════════════════════════════
# SSE STREAM
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/job/{job_id}")
def stream_job(job_id: str):
    """Server-Sent Events stream for a running job."""
    if job_id not in JOBS:
        raise HTTPException(404, f"Job {job_id} not found")

    job = JOBS[job_id]
    log_q: queue.Queue = job["log_queue"]

    def event_generator():
        # Send current status immediately
        yield f"data: {json.dumps({'type': 'status', 'status': job['status']})}\n\n"

        while True:
            try:
                msg = log_q.get(timeout=30)
                if msg is None:  # sentinel — pipeline done
                    yield f"data: {json.dumps({'type': 'done', 'status': job['status']})}\n\n"
                    break
                yield f"data: {json.dumps({'type': 'log', **json.loads(msg)})}\n\n"
            except queue.Empty:
                # Heartbeat to keep connection alive
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ═══════════════════════════════════════════════════════════════════════════════
# JOB LIST
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/jobs")
def list_jobs():
    return {
        "jobs": [
            {
                "job_id": jid,
                "status": j["status"],
                "client_name": j.get("client_name"),
                "created_at": j.get("created_at"),
            }
            for jid, j in sorted(JOBS.items(), key=lambda x: x[1].get("created_at", ""), reverse=True)
        ]
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="FintastIQ Local Pipeline Server")
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    print(f"""
╔══════════════════════════════════════════════════════╗
║     FintastIQ Local Pipeline Server                  ║
╠══════════════════════════════════════════════════════╣
║  URL:     http://{args.host}:{args.port}
║  Health:  http://{args.host}:{args.port}/health
║                                                      ║
║  Keep this terminal open while using the admin UI.  ║
╚══════════════════════════════════════════════════════╝
""")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
