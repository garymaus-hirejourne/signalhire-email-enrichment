#!/usr/bin/env python
"""Fetch contacts from Hunter.io Domain Search and save to CSV.
Usage:
    python hunter_domain_search.py eriez.com --api-key <key> --output output/eriez_hunter_contacts.csv
"""
from __future__ import annotations
import argparse
import csv
import requests
from pathlib import Path

HUNTER_ENDPOINT = "https://api.hunter.io/v2/domain-search"


def fetch_contacts(domain: str, api_key: str, limit: int = 100):
    params = {
        "domain": domain,
        "api_key": api_key,
        "limit": limit,
    }
    r = requests.get(HUNTER_ENDPOINT, params=params, timeout=15)
    r.raise_for_status()
    data = r.json().get("data", {})
    return data.get("emails", [])


def main():
    parser = argparse.ArgumentParser(description="Hunter.io domain search to CSV")
    parser.add_argument("domain", help="Company domain, e.g. eriez.com")
    parser.add_argument("--api-key", required=True, help="Hunter API key")
    parser.add_argument("--output", "-o", default="contacts_hunter.csv", help="Output CSV path")
    args = parser.parse_args()

    emails = fetch_contacts(args.domain, args.api_key)
    if not emails:
        print("No contacts found.")
        return

    out = Path(args.output)
    out.parent.mkdir(exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "value",
            "type",
            "first_name",
            "last_name",
            "position",
            "company",
            "confidence",
            "phone_number",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for e in emails:
            writer.writerow({
                "value": e.get("value"),
                "type": e.get("type"),
                "first_name": e.get("first_name"),
                "last_name": e.get("last_name"),
                "position": e.get("position"),
                "company": e.get("company"),
                "confidence": e.get("confidence"),
                "phone_number": e.get("phone_number"),
            })
    print(f"Saved {len(emails)} contacts to {out}")


if __name__ == "__main__":
    main()
