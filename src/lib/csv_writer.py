from __future__ import annotations

from typing import Any, Dict, Iterable, List


def flatten_callback_payload(payload: Any) -> List[Dict[str, Any]]:
    """Flatten SignalHire Person API callback payload to CSV-like rows.

    Expected payload is a list of items, each with fields:
      - status
      - item (original identifier)
      - candidate: { fullName, uid, contacts[], social[], experience[], ... }
    """
    rows: List[Dict[str, Any]] = []
    if not payload:
        return rows

    items = payload if isinstance(payload, list) else [payload]

    for it in items:
        status = it.get("status")
        original_item = it.get("item")
        cand = it.get("candidate") or {}
        uid = cand.get("uid")
        full_name = cand.get("fullName") or cand.get("full_name")
        socials = cand.get("social") or []
        linkedin_url = None
        for s in socials:
            if (s.get("type") or "").lower() in ("li", "linkedin"):
                linkedin_url = s.get("link")
                break
        contacts = cand.get("contacts") or []

        if contacts:
            for c in contacts:
                rows.append(
                    {
                        "uid": uid,
                        "full_name": full_name,
                        "status": status,
                        "linkedin_url": linkedin_url or original_item,
                        "contact_type": c.get("type"),
                        "contact_value": c.get("value"),
                        "contact_subtype": c.get("subType") or c.get("sub_type"),
                    }
                )
        else:
            # No contacts -> still emit a row for traceability
            rows.append(
                {
                    "uid": uid,
                    "full_name": full_name,
                    "status": status,
                    "linkedin_url": linkedin_url or original_item,
                    "contact_type": None,
                    "contact_value": None,
                    "contact_subtype": None,
                }
            )

    return rows
