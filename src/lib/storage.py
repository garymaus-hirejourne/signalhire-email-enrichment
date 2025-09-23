from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable
from datetime import datetime

DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data")).resolve()
BATCHES_DIR = DATA_ROOT / "batches"
REQUESTS_DIR = DATA_ROOT / "requests"
BATCHES_DIR.mkdir(parents=True, exist_ok=True)
REQUESTS_DIR.mkdir(parents=True, exist_ok=True)


def new_batch_id() -> str:
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S%f")
    return now[-8:]  # short but unique-ish for demo; replace with uuid if needed


def batch_dir(batch_id: str) -> Path:
    d = BATCHES_DIR / batch_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_original_csv(batch_id: str, content: bytes) -> Path:
    d = batch_dir(batch_id)
    p = d / "original.csv"
    p.write_bytes(content)
    return p


def write_status(batch_id: str, status: dict[str, Any]) -> Path:
    d = batch_dir(batch_id)
    p = d / "status.json"
    p.write_text(json.dumps(status, ensure_ascii=False, indent=2))
    return p


def read_status(batch_id: str) -> dict[str, Any]:
    p = batch_dir(batch_id) / "status.json"
    if p.exists():
        return json.loads(p.read_text())
    return {}


def map_request_to_batch(request_id: str, batch_id: str) -> Path:
    p = REQUESTS_DIR / f"{request_id}.txt"
    p.write_text(batch_id)
    return p


def find_batch_by_request(request_id: str) -> str | None:
    p = REQUESTS_DIR / f"{request_id}.txt"
    return p.read_text().strip() if p.exists() else None


def append_results_json(batch_id: str, request_id: str, payload: Any) -> Path:
    d = batch_dir(batch_id)
    p = d / "results.json"
    data: dict[str, Any] = {}
    if p.exists():
        data = json.loads(p.read_text() or "{}")
    data.setdefault(request_id, payload)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    return p


def append_results_csv(batch_id: str, rows: Iterable[dict[str, Any]]) -> Path:
    import csv

    d = batch_dir(batch_id)
    p = d / "results.csv"
    rows = list(rows)
    if not rows:
        return p
    fieldnames = list(rows[0].keys())
    exists = p.exists()
    with p.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)
    return p


def batch_csv_path(batch_id: str) -> Path:
    return batch_dir(batch_id) / "results.csv"
