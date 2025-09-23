#!/usr/bin/env python3
from __future__ import annotations

import os
import csv
import json
from pathlib import Path
from typing import Any, List
import httpx

from fastapi import FastAPI, UploadFile, Form, File, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse

from .lib import storage
from .lib.emailer import send_result_email, send_error_email
from .services.signalhire_client import submit_identifier, API_BASE, API_PREFIX, API_KEY
from .lib.csv_writer import flatten_callback_payload

APP_NAME = "SignalHire Cloud Webhook"

app = FastAPI(title=APP_NAME, version="1.0.0")


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return (
        """
        <html><body>
        <h2>SignalHire Cloud Webhook</h2>
        <p>Upload a CSV (single column of LinkedIn URLs) and an email to receive results.</p>
        <form action="/upload" method="post" enctype="multipart/form-data">
          <label>CSV file:</label>
          <input type="file" name="csv_file" accept=".csv" required />
          <br/>
          <label>Your email:</label>
          <input type="email" name="user_email" required />
          <br/>
          <button type="submit">Submit</button>
        </form>
        <p>Health: <a href="/health">/health</a></p>
        </body></html>
        """
    )


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "healthy", "service": APP_NAME}


@app.get("/credits")
async def credits() -> JSONResponse:
    """Proxy to SignalHire credits endpoint using configured API key.

    Returns JSON with remaining credits or an error with diagnostics.
    """
    try:
        if not API_KEY:
            raise HTTPException(status_code=500, detail="Missing SIGNALHIRE_API_KEY")
        url = f"{API_BASE}{API_PREFIX}/credits?withoutContacts=true"
        headers = {"apikey": API_KEY}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, headers=headers)
            try:
                data = resp.json()
            except Exception:
                raw = await resp.aread()
                data = {"raw": raw[:1024].decode(errors="ignore")}
            return JSONResponse(
                {
                    "ok": resp.status_code in range(200, 300),
                    "status_code": resp.status_code,
                    "headers": {k: v for k, v in resp.headers.items() if k.lower() in {"content-type"}},
                    "data": data,
                },
                status_code=resp.status_code,
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload")
async def upload(csv_file: UploadFile = File(...), user_email: str = Form(...)) -> JSONResponse:
    """Accept CSV of LinkedIn URLs and user email, create batch, submit Person API requests."""
    try:
        content = await csv_file.read()
        # Create batch and persist original CSV
        batch_id = storage.new_batch_id()
        storage.save_original_csv(batch_id, content)

        # Parse LinkedIn URLs (single-column CSV)
        urls: List[str] = []
        for row in csv.reader(content.decode("utf-8-sig").splitlines()):
            if not row:
                continue
            url = (row[0] or "").strip()
            if url and url.lower().startswith("http"):
                urls.append(url)
        if not urls:
            raise HTTPException(status_code=400, detail="No LinkedIn URLs found in CSV")

        # Initialize status
        status = {
            "status": "processing",
            "email": user_email,
            "total_items": len(urls),
            "pending": [],
            "received": 0,
            "errors": [],
            "submissions": [],  # per-item diagnostics
        }

        # Submit identifiers to SignalHire Person API with callbackUrl
        callback_base = os.getenv(
            "CALLBACK_BASE_URL",
            "https://webhook--signalhire-webhook--jgdqh2mydks5.code.run",
        ).rstrip("/")
        callback_url = f"{callback_base}/signalhire/callback"

        for url in urls:
            resp = await submit_identifier(url, callback_url)
            # record diagnostics for visibility
            status["submissions"].append({
                "item": url,
                "success": resp.get("success"),
                "request_id": resp.get("request_id"),
                "error": resp.get("error"),
                "diagnostics": resp.get("diagnostics"),
            })
            if not resp["success"]:
                status["errors"].append({"item": url, "error": resp.get("error")})
                continue
            rid = resp.get("request_id")
            if rid:
                storage.map_request_to_batch(rid, batch_id)
                status["pending"].append(rid)

        storage.write_status(batch_id, status)
        return JSONResponse({
            "status": "accepted",
            "batch_id": batch_id,
            "submitted": len(status["pending"]),
            "errors": len(status["errors"]),
            "callback_url": callback_url,
        })
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/signalhire/callback")
async def callback(request: Request) -> JSONResponse:
    """Handle SignalHire Person API callback, append JSON/CSV, and manage batch status and email."""
    try:
        request_id = request.headers.get("Request-Id") or request.headers.get("Request-ID")
        if not request_id:
            raise HTTPException(status_code=400, detail="Missing Request-Id header")

        payload = await request.json()
        # Lookup batch
        batch_id = storage.find_batch_by_request(request_id)
        if not batch_id:
            # Accept but log unknown request id
            return JSONResponse({"status": "accepted", "warning": "unknown request id"})

        # Append raw JSON per request
        storage.append_results_json(batch_id, request_id, payload)

        # Flatten and append CSV rows
        rows = flatten_callback_payload(payload)
        storage.append_results_csv(batch_id, rows)

        # Update status: remove pending id, increment received
        status = storage.read_status(batch_id)
        pending = status.get("pending", [])
        if request_id in pending:
            pending.remove(request_id)
        status["pending"] = pending
        status["received"] = int(status.get("received", 0)) + 1

        # If no pending, mark complete and send email
        if not pending:
            status["status"] = "complete"
            storage.write_status(batch_id, status)
            try:
                # Email results.csv
                csv_path = storage.batch_csv_path(batch_id)
                user_email = status.get("email")
                if user_email and csv_path.exists():
                    await send_result_email(user_email, batch_id, csv_path)
            except Exception as email_err:
                # Record email error but do not fail webhook
                status.setdefault("errors", []).append({"email_error": str(email_err)})
                storage.write_status(batch_id, status)
        else:
            # Save interim status
            storage.write_status(batch_id, status)

        return JSONResponse({"status": "accepted", "batch_id": batch_id})
    except HTTPException:
        raise
    except Exception as e:
        # Try to notify user on error if possible
        try:
            # If we can derive a batch, send error email
            req_id = request.headers.get("Request-Id") or ""
            batch_id = storage.find_batch_by_request(req_id) if req_id else None
            if batch_id:
                st = storage.read_status(batch_id)
                st.setdefault("errors", []).append({"callback_error": str(e)})
                storage.write_status(batch_id, st)
                user_email = st.get("email")
                if user_email:
                    await send_error_email(user_email, batch_id, str(e))
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status/{batch_id}")
async def status(batch_id: str) -> JSONResponse:
    st = storage.read_status(batch_id)
    if not st:
        raise HTTPException(status_code=404, detail="Unknown batch id")
    return JSONResponse(st)


@app.get("/download/{batch_id}")
async def download(batch_id: str) -> FileResponse:
    csv_path = storage.batch_csv_path(batch_id)
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="results.csv not found for batch")
    return FileResponse(str(csv_path), media_type="text/csv", filename=f"results_{batch_id}.csv")
