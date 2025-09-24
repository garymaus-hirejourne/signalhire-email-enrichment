
# SignalHire Email Enrichment Webhook (FastAPI)

Production-ready FastAPI app that submits LinkedIn URLs to SignalHire Person API and receives callbacks with enriched contact data. Results are stored on a mounted volume and delivered via email.

## Endpoints
- `GET /` – HTML form to upload a CSV (single column of LinkedIn URLs) and your email
- `POST /upload` – Creates a batch, submits URLs to SignalHire, and tracks `request_id`s
- `POST /signalhire/callback` – Receives Person API callbacks and appends rows to `results.csv`
- `GET /status/{batch_id}` – Current batch status and per-item diagnostics
- `GET /download/{batch_id}` – Download `results.csv` for the batch
- `GET /credits` – Proxy to SignalHire credits endpoint
- `GET /health` – Health check

## Environment Variables
- `SIGNALHIRE_API_KEY` – SignalHire API key (required)
- `GMAIL_USER` – From address (e.g. `gary@hirejourne.com`)
- `GMAIL_APP_PASSWORD` – App-specific password for SMTP
- `CALLBACK_BASE_URL` – Your public service URL (e.g. `https://webhook--...code.run`)
- `DATA_ROOT` – Data volume root (default `/data`)
- `PORT` – Provided by platform (defaults to `8080` in Dockerfile)

## Deployment (Northflank)
1. Create a Public Service from this GitHub repo.
2. Build method: Dockerfile.
3. Add a persistent volume mounted at `/data`.
4. Set environment variables listed above.
5. Deploy. Verify `GET /health` and `GET /`.

## Local Development
```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
export SIGNALHIRE_API_KEY=...  # set your envs
export DATA_ROOT=./data
uvicorn src.app:app --reload --port 8080
```

## Notes
- Requires paid SignalHire reveal credits for Person API. Check credits via `GET /credits`.
- All data is written to `DATA_ROOT` under `batches/{batch_id}/`.
- Docker image is kept lean via `.dockerignore`.
