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
                data = {"raw": await resp.aread()}

            if resp.status_code >= 200 and resp.status_code < 300:
                # Expecting { request_id: "..." } or similar
                request_id = data.get("request_id") or data.get("Request-Id") or data.get("id")
                return {"success": True, "request_id": request_id}
            else:
                return {"success": False, "error": data.get("error") or f"HTTP {resp.status_code}"}
        except httpx.TimeoutException:
            return {"success": False, "error": "Timeout contacting SignalHire API"}
        except Exception as e:
            return {"success": False, "error": str(e)}
