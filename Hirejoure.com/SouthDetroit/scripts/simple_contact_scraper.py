#!/usr/bin/env python
"""
Simple contact scraper
----------------------
Given a starting URL, crawls pages within the same domain (breadth-first) up to a
given depth and extracts e-mail addresses and telephone numbers found in the
HTML. Results are written to CSV (stdout or file).

Usage (command line):
    python simple_contact_scraper.py https://example.com --depth 2 --output contacts.csv

Dependencies: requests, beautifulsoup4, tqdm (optional, for progress bar)
"""
from __future__ import annotations
import argparse
import csv
import re
import sys
from collections import deque
from pathlib import Path
from typing import Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm  # type: ignore
except ImportError:  # graceful fallback if tqdm not present
    def tqdm(iterable, **kwargs):
        return iterable  # type: ignore

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_REGEX = re.compile(r"\+?\d[\d\-() ]{7,}\d")


def extract_contacts(text: str) -> Tuple[Set[str], Set[str]]:
    """Return sets of (emails, phones) found in the given plain text."""
    emails = set(EMAIL_REGEX.findall(text))
    phones = set(PHONE_REGEX.findall(text))
    return emails, phones


def same_domain(url1: str, url2: str) -> bool:
    return urlparse(url1).netloc == urlparse(url2).netloc


def crawl(start_url: str, depth: int = 1, timeout: int = 15):
    """Generator yielding (url, emails, phones) for each crawled page."""
    visited: Set[str] = set()
    queue: deque[Tuple[str, int]] = deque([(start_url, 0)])

    while queue:
        url, d = queue.popleft()
        if url in visited or d > depth:
            continue
        visited.add(url)
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
        except Exception as e:
            print(f"[warn] Failed {url}: {e}", file=sys.stderr)
            continue

        ctype = resp.headers.get("Content-Type", "").lower()
        page_emails: Set[str] = set()
        page_phones: Set[str] = set()
        is_html = "text/html" in ctype or ctype.startswith("text/")
        if is_html:
            # Parse HTML safely
            try:
                soup = BeautifulSoup(resp.text, "html.parser")
                text = soup.get_text(" ")
            except Exception as e:
                print(f"[warn] Parser error {url}: {e}", file=sys.stderr)
                text = resp.text
            page_emails, page_phones = extract_contacts(text)
        else:
            # Binary (PDF, DOC, etc.). Attempt to decode bytes and run regex.
            try:
                text = resp.content.decode("utf-8", errors="ignore")
            except Exception:
                text = resp.content.decode("latin1", errors="ignore")
            page_emails, page_phones = extract_contacts(text)
        yield url, page_emails, page_phones

        # enqueue links only for HTML pages
        if d < depth and is_html:
            for a in soup.find_all("a", href=True):
                link = urljoin(url, a["href"])
                if link.startswith("http") and same_domain(start_url, link):
                    queue.append((link, d + 1))


def main():
    parser = argparse.ArgumentParser(description="Scrape emails and phone numbers from a website domain.")
    parser.add_argument("url", help="Starting URL to crawl")
    parser.add_argument("--depth", "-d", type=int, default=1, help="Link depth to crawl (default 1)")
    parser.add_argument("--output", "-o", help="Output CSV file (defaults to stdout)")
    args = parser.parse_args()

    rows = []
    for url, emails, phones in tqdm(list(crawl(args.url, depth=args.depth)), desc="Pages"):
        for email in sorted(emails):
            rows.append({"url": url, "type": "email", "value": email})
        for phone in sorted(phones):
            rows.append({"url": url, "type": "phone", "value": phone})

    out_file = Path(args.output) if args.output else None
    out_fh = open(out_file, "w", newline="", encoding="utf-8") if out_file else sys.stdout
    with out_fh as fh:
        writer = csv.DictWriter(fh, fieldnames=["url", "type", "value"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Extracted {len(rows)} contacts", file=sys.stderr)
    if out_file:
        print(f"Saved to {out_file}", file=sys.stderr)


if __name__ == "__main__":
    main()
