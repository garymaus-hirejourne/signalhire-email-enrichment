from __future__ import annotations

import os
import httpx
from typing import Any, Dict

API_BASE = os.getenv("SIGNALHIRE_API_BASE_URL", "https://www.signalhire.com").rstrip("/")
API_PREFIX = os.getenv("SIGNALHIRE_API_PREFIX", "/api/v1")
API_KEY = os.getenv("SIGNALHIRE_API_KEY")


async def submit_identifier(identifier: str, callback_url: str) -> Dict[str, Any]:
    """Submit a single identifier (LinkedIn URL/email/phone/uid) to SignalHire Person API.

    Returns: { success: bool, request_id?: str, error?: str }
    """
    if not API_KEY:
        return {"success": False, "error": "Missing SIGNALHIRE_API_KEY"}

    url = f"{API_BASE}{API_PREFIX}/person"
    headers = {"Content-Type": "application/json", "apikey": API_KEY}
    payload = {"items": [identifier], "callbackUrl": callback_url}

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(url, headers=headers, json=payload)
            data: Dict[str, Any]
            try:
                data = resp.json()
            except Exception:
                raw = await resp.aread()
                # Keep a short snippet to avoid logging secrets / large payloads
                data = {"raw": raw[:512].decode(errors="ignore")}

            if resp.status_code >= 200 and resp.status_code < 300:
                # Expect various casings or header for request id
                request_id = (
                    data.get("request_id")
                    or data.get("Request-Id")
                    or data.get("requestId")
                    or data.get("id")
                    or resp.headers.get("Request-Id")
                    or resp.headers.get("request-id")
                )
                diagnostics = {
                    "status_code": resp.status_code,
                    # Only keep a few safe headers
                    "headers": {k: v for k, v in resp.headers.items() if k.lower() in {"content-type", "request-id"}},
                    "body": data,
                }
                if not request_id:
                    return {"success": False, "error": "No request_id returned by SignalHire", "diagnostics": diagnostics}
                return {"success": True, "request_id": request_id, "diagnostics": diagnostics}
            else:
                diagnostics = {
                    "status_code": resp.status_code,
                    "headers": {k: v for k, v in resp.headers.items() if k.lower() in {"content-type", "request-id"}},
                    "body": data,
                }
                return {"success": False, "error": data.get("error") or f"HTTP {resp.status_code}", "diagnostics": diagnostics}
        except httpx.TimeoutException:
            return {"success": False, "error": "Timeout contacting SignalHire API"}
        except Exception as e:
            return {"success": False, "error": str(e)}
