#!/usr/bin/env python
"""
Scrape LinkedIn for VP executives and Presidents of the following TIC (Testing, Inspection, Certification) sector corporations:
- SGS SA
- Intertek Group Plc
- Bureau Veritas
- TUV SUD
- Applus Services, SA
- UL LLC
- DNV GL
- Mistras Group
- DEKRA SE
- ALS Limited

For each company, collect up to 10 executives with titles containing 'VP', 'Vice President', or 'President'.
Enrich with email and phone using SignalHire API if possible.
Output is a CSV with as much detail as possible (name, title, company, LinkedIn URL, email, phone, etc.).
"""

import os
import pandas as pd
from time import sleep
from dotenv import load_dotenv
import requests

# Load API keys from .env
try:
    desktop_env = os.path.join(os.path.expanduser('~'), 'Desktop', 'southdetroit_api_keys.env')
    if os.path.exists(desktop_env):
        load_dotenv(desktop_env)
    else:
        load_dotenv()
except ImportError:
    pass

SIGNALHIRE_API_KEY = os.environ.get("SIGNALHIRE_API_KEY", "")
SIGNALHIRE_ENDPOINT = "https://api.signalhire.com/v2/email-finder"

# --- User-defined parameters ---
# TIC (Testing, Inspection, Certification) sector companies
COMPANIES = [
    "SGS SA",
    "Intertek Group Plc", 
    "Bureau Veritas",
    "TUV SUD",
    "Applus Services, SA",
    "UL LLC",
    "DNV GL", 
    "Mistras Group",
    "DEKRA SE",
    "ALS Limited"
]
TITLES = ["VP", "Vice President", "President"]
MAX_EXECUTIVES_PER_COMPANY = 10

# --- Helper Functions ---
def signalhire_email_lookup(first_name, last_name, company_domain):
    params = {
        'first_name': first_name,
        'last_name': last_name,
        'company_domain': company_domain,
        'api_key': SIGNALHIRE_API_KEY
    }
    try:
        resp = requests.get(SIGNALHIRE_ENDPOINT, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if 'error' in data:
                return {}
            email = data.get('email')
            phone = data.get('phone') or data.get('phone_number') or ''
            social = data.get('social', {})
            return {
                'email': email,
                'phone_number': phone,
                'linkedin': social.get('linkedin'),
            }
        else:
            return {}
    except Exception:
        return {}

def linkedin_search(company, titles, max_results=10):
    """
    Use SerpAPI to search for LinkedIn profiles of executives at the given company with the specified titles.
    Returns a list of dicts with keys: first_name, last_name, title, company, linkedin_url.
    """
    from serpapi import GoogleSearch
    import re

    SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")
    if not SERPAPI_API_KEY:
        raise RuntimeError("SERPAPI_API_KEY not set in environment.")

    results = []
    for title in titles:
        query = f"site:linkedin.com/in \"{title}\" \"{company}\""
        search = GoogleSearch({
            "q": query,
            "api_key": SERPAPI_API_KEY,
            "engine": "google",
            "num": max_results,
            "location": "United States"
        })
        data = search.get_dict()
        organic = data.get("organic_results", [])
        count = 0
        for r in organic:
            link = r.get("link", "")
            title_text = r.get("title", "")
            snippet = r.get("snippet", "")
            # Only consider LinkedIn profile URLs
            if "linkedin.com/in/" not in link:
                continue
            # Try to extract name and title
            m = re.match(r"([A-Za-z .'-]+) \| ([^|]+) \| LinkedIn", title_text)
            if m:
                name = m.group(1).strip()
                exec_title = m.group(2).strip()
            else:
                # Fallback: use snippet or skip
                name = title_text.split("|")[0].strip()
                exec_title = title
            # Split name into first/last
            name_parts = name.split()
            first_name = name_parts[0] if name_parts else ""
            last_name = name_parts[-1] if len(name_parts) > 1 else ""
            results.append({
                "first_name": first_name,
                "last_name": last_name,
                "title": exec_title,
                "company": company,
                "linkedin_url": link,
                "company_domain": None  # Can be filled later if needed
            })
            count += 1
            if count >= max_results:
                break
        if len(results) >= max_results:
            break
    return results[:max_results]


def main():
    all_execs = []
    for company in COMPANIES:
        print(f"Searching executives for {company}...")
        execs = linkedin_search(company, TITLES, max_results=MAX_EXECUTIVES_PER_COMPANY)
        for exec in execs:
            # Try to enrich with email/phone using SignalHire if possible
            domain = exec.get('company_domain') or ''
            sh = {}
            if SIGNALHIRE_API_KEY and exec.get('first_name') and exec.get('last_name') and domain:
                sh = signalhire_email_lookup(exec['first_name'], exec['last_name'], domain)
                sleep(1)  # Be nice to the API
            exec['email'] = sh.get('email', '')
            exec['phone_number'] = sh.get('phone_number', '')
            exec['linkedin_url'] = exec.get('linkedin_url', sh.get('linkedin', ''))
            all_execs.append(exec)
        print(f"Found {len(execs)} executives for {company}.")

    # Output to CSV
    df = pd.DataFrame(all_execs)
    output_file = r"G:\My Drive\Hirejoure.com\Instrumentation_and_Measurement_Execs.csv"
    df.to_csv(output_file, index=False)
    print(f"Saved {len(df)} executives to {output_file}")

if __name__ == "__main__":
    main()
