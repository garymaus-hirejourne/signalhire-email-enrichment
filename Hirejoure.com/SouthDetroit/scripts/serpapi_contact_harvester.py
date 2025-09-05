#!/usr/bin/env python
"""Harvest public emails/phones for a company using SerpAPI search results.

Example:
    python serpapi_contact_harvester.py "Eriez Magnetics Erie PA" -o output/eriez_serpapi_contacts.csv

The script:
1. Queries Google via SerpAPI for the given search term.
2. Filters result links to public profile / contact pages (LinkedIn, Facebook, Crunchbase, company domains, etc.).
3. Downloads each page (no login) and extracts emails & phone numbers via regex.
4. Outputs a CSV of url,type,value.

Requires the env var SERPAPI_API_KEY or the key constant in the old debug script.
"""
from __future__ import annotations
import argparse
import csv
import os
import re
import sys
from pathlib import Path
from typing import List, Tuple, Set
import requests
from bs4 import BeautifulSoup

try:
    from serpapi import GoogleSearch
except ImportError:
    print("[error] serpapi package not installed. pip install serpapi", file=sys.stderr)
    sys.exit(1)

# Attempt to load API key
API_KEY = os.getenv("SERPAPI_API_KEY")
if not API_KEY:
    # Fallback: try to read from the legacy debug script
    legacy_path = Path(__file__).parent.parent / "debug scripts" / "pre-Boston" / "serpapi_company_scraper.py"
    if legacy_path.exists():
        import re as _re
        m = _re.search(r"SERPAPI_API_KEY\s*=\s*\"([0-9a-f]+)\"", legacy_path.read_text())
        if m:
            API_KEY = m.group(1)
if not API_KEY:
    print("[error] No SERPAPI_API_KEY found. Set env var or edit script.", file=sys.stderr)
    sys.exit(1)

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")
PHONE_REGEX = re.compile(r"\+?\d[\d\-() ]{7,}\d")


def extract_contacts(text: str) -> Tuple[Set[str], Set[str]]:
    emails = set(EMAIL_REGEX.findall(text))
    phones = set(PHONE_REGEX.findall(text))
    return emails, phones


def google_search(query: str, api_key: str, num: int = 10):
    search = GoogleSearch({"q": query, "api_key": api_key, "engine": "google", "num": num, "location": "United States"})
    results = search.get_dict()
    return [r.get("link") for r in results.get("organic_results", []) if r.get("link")]


def fetch_page(url: str, timeout: int = 15) -> str | None:
    try:
        resp = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        return resp.text
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description="Find public emails/phones via SerpAPI search results.")
    parser.add_argument("query", help="Search query, e.g. 'Eriez Magnetics Erie PA'")
    parser.add_argument("--output", "-o", default="contacts_serpapi.csv", help="Output CSV file")
    parser.add_argument("--results", "-n", type=int, default=20, help="Max search results to process")
    args = parser.parse_args()

    links: List[str] = google_search(args.query, API_KEY, num=args.results)
    print(f"Fetched {len(links)} search result links")

    rows = []
    for link in links:
        html = fetch_page(link)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        emails, phones = extract_contacts(soup.get_text(" "))
        for email in emails:
            rows.append({"url": link, "type": "email", "value": email})
        for phone in phones:
            rows.append({"url": link, "type": "phone", "value": phone})

    out_path = Path(args.output)
    out_path.parent.mkdir(exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "type", "value"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} contacts -> {out_path}")


if __name__ == "__main__":
    main()
