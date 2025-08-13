
# SignalHire Webhook (Northflank-ready)

Minimal Flask webhook that receives SignalHire results and appends them to a CSV on a mounted volume.

## Endpoints
- `GET /health` → simple health probe
- `POST /signalhire/webhook` → accepts an array of results from SignalHire

## Environment Variables
- `PORT` (default 8080)
- `SIGNALHIRE_RESULTS_CSV` (default `/data/results.csv`)

## Deploy on Northflank
1. Create a Public Service from this GitHub repo.
2. Build method: Dockerfile or Procfile.
3. Add a volume mounted at `/data` (read-write).
4. Set env vars:
   - `SIGNALHIRE_RESULTS_CSV=/data/results.csv`
5. Deploy. Use the given public URL for your SignalHire `callbackUrl`.

## Local Dev
```bash
pip install -r requirements.txt
export PORT=8080
export SIGNALHIRE_RESULTS_CSV=./results.csv
python webhook_server.py
```
