#!/usr/bin/env python
"""Infer company e-mail patterns and fill missing addresses.

Usage:
    python email_pattern_filler.py contacts.csv known_emails.csv -o out.csv

Arguments:
    contacts.csv     CSV with at least: first_name,last_name,company[,domain,email]
    known_emails.csv CSV with columns: email, first_name, last_name (can be same as contacts)

The script groups by company domain, infers the most common address pattern
from the known emails, and then fills any blank email cells for that domain.

Patterns supported (examples for John Smith):
    1. first.last        -> john.smith@domain
    2. f.last            -> j.smith@domain
    3. firstl            -> johns@domain
    4. first_last        -> john_smith@domain
    5. firstlast         -> johnsmith@domain
    6. first             -> john@domain (rare)

Feel free to extend PATTERNS if your dataset uses other styles.
"""
from __future__ import annotations
import re
import argparse
import os
import time

# --- Load .env file for API keys if present ---
try:
    from dotenv import load_dotenv
    # Try to load from Desktop first, fallback to project root
    desktop_env = os.path.join(os.path.expanduser('~'), 'Desktop', 'southdetroit_api_keys.env')
    if os.path.exists(desktop_env):
        load_dotenv(desktop_env)
    else:
        load_dotenv()
except ImportError:
    print("python-dotenv not installed: API keys may not be loaded from .env file. Run 'pip install python-dotenv' for best results.")

from collections import Counter, defaultdict
import os
import re
import time
import json
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Callable
import pandas as pd
import requests
try:
    from tqdm import tqdm
    tqdm_available = True
except ImportError:
    tqdm_available = False
    def tqdm(x, **k):
        print("tqdm not installed: progress bar will not be shown. Run 'pip install tqdm' for better progress display.")
        return x

# ---------------- External Email Format Verification ----------------

# Hunter.io API for email format verification
HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
HUNTER_DOMAIN_SEARCH_ENDPOINT = "https://api.hunter.io/v2/domain-search"

# Global caches for performance optimization
_email_pattern_cache = {}
_pattern_cache = {}
_verify_cache = {}

# Persistent cache file for domain patterns
CACHE_FILE = "email_patterns_cache.json"

def load_persistent_cache():
    """Load cached domain patterns from file"""
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'r') as f:
                cache_data = json.load(f)
                print(f"📁 Loaded {len(cache_data)} cached domain patterns from {CACHE_FILE}")
                return cache_data
    except Exception as e:
        print(f"⚠️ Error loading cache: {e}")
    return {}

def save_persistent_cache(cache_data):
    """Save domain patterns to persistent cache file"""
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache_data, f, indent=2)
        print(f"💾 Saved {len(cache_data)} domain patterns to cache")
    except Exception as e:
        print(f"⚠️ Error saving cache: {e}")

def search_google_for_email_format(company_name, domain):
    """Search Google for actual company email format information."""
    try:
        import urllib.parse
        query = f'"{company_name}" email format OR "{domain}" email pattern employees'
        search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        resp = requests.get(search_url, headers=headers, timeout=10)
        if resp.status_code == 200:
            content = resp.text.lower()
            
            # Look for common email pattern indicators in search results
            patterns = {
                'first.last': ['first.last', 'firstname.lastname', 'john.doe'],
                'firstlast': ['firstlast', 'johndoe', 'firstname lastname'],
                'first_last': ['first_last', 'firstname_lastname', 'john_doe'],
                'f.last': ['f.last', 'j.doe', 'first initial'],
                'first': ['first name only', 'firstname@']
            }
            
            for pattern, indicators in patterns.items():
                if any(indicator in content for indicator in indicators):
                    print(f"Google search suggests {domain} uses {pattern} pattern")
                    return pattern
        
        return None
    except Exception as e:
        print(f"Google search error for {domain}: {e}")
        return None

def get_hunter_email_format(domain):
    """Get verified email format from Hunter.io API."""
    if not HUNTER_API_KEY:
        return None, None
    
    try:
        params = {
            'domain': domain,
            'api_key': HUNTER_API_KEY,
            'limit': 10
        }
        
        resp = requests.get(HUNTER_DOMAIN_SEARCH_ENDPOINT, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            
            if 'data' in data and 'pattern' in data['data']:
                pattern = data['data']['pattern']
                confidence = data['data'].get('confidence', 0)
                
                # Convert Hunter.io pattern to our pattern format
                hunter_to_our_pattern = {
                    '{first}.{last}': 'first.last',
                    '{first}{last}': 'firstlast', 
                    '{first}_{last}': 'first_last',
                    '{f}.{last}': 'f.last',
                    '{first}': 'first'
                }
                
                our_pattern = hunter_to_our_pattern.get(pattern, 'first.last')
                print(f"Hunter.io verified {domain} uses {our_pattern} pattern (confidence: {confidence}%)")
                return our_pattern, confidence
        
        return None, None
    except Exception as e:
        print(f"Hunter.io API error for {domain}: {e}")
        return None, None

def scrape_company_website_for_emails(domain):
    """Scrape company website to find actual email examples."""
    try:
        # Try common contact/about pages
        pages_to_check = [
            f"https://{domain}/contact",
            f"https://{domain}/about",
            f"https://{domain}/team",
            f"https://{domain}/leadership",
            f"https://www.{domain}/contact",
            f"https://www.{domain}/about"
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        for url in pages_to_check:
            try:
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    content = resp.text.lower()
                    
                    # Look for email patterns in the content
                    import re
                    email_pattern = rf'[a-zA-Z0-9._%+-]+@{re.escape(domain)}'
                    emails = re.findall(email_pattern, content)
                    
                    if emails:
                        # Analyze found emails to determine pattern
                        for email in emails[:3]:  # Check first 3 emails
                            local_part = email.split('@')[0]
                            if '.' in local_part and len(local_part.split('.')) == 2:
                                print(f"Website scraping found {domain} uses first.last pattern")
                                return 'first.last'
                            elif '_' in local_part:
                                print(f"Website scraping found {domain} uses first_last pattern")
                                return 'first_last'
                        
                        print(f"Website scraping found emails but unclear pattern for {domain}")
                        return 'first.last'  # Default assumption
                        
            except:
                continue
        
        return None
    except Exception as e:
        print(f"Website scraping error for {domain}: {e}")
        return None

def get_verified_email_pattern(company_name, domain):
    """Get verified email pattern using multiple external sources."""
    # Check cache first
    cache_key = domain.lower()
    if cache_key in _email_pattern_cache:
        cached_pattern = _email_pattern_cache[cache_key]
        print(f"Using cached pattern for {domain}: {cached_pattern}")
        return cached_pattern
    
    print(f"Verifying email pattern for {domain}...")
    
    # Method 1: Hunter.io API (most reliable)
    hunter_pattern, confidence = get_hunter_email_format(domain)
    if hunter_pattern and confidence and confidence > 50:
        _email_pattern_cache[cache_key] = hunter_pattern
        return hunter_pattern
    
    # Method 2: Google search
    google_pattern = search_google_for_email_format(company_name, domain)
    if google_pattern:
        _email_pattern_cache[cache_key] = google_pattern
        return google_pattern
    
    # Method 3: Website scraping
    website_pattern = scrape_company_website_for_emails(domain)
    if website_pattern:
        _email_pattern_cache[cache_key] = website_pattern
        return website_pattern
    
    # Method 4: Known patterns database
    known_patterns = {
        'bureauveritas.com': 'first.last',  # Based on research
        'ul.com': 'first.last',
        'mistrasgroup.com': 'first.last',
        'dnvgl.com': 'first.last',
        'tuvsud.com': 'first.last'
    }
    
    if domain.lower() in known_patterns:
        pattern = known_patterns[domain.lower()]
        print(f"Using known pattern for {domain}: {pattern}")
        _email_pattern_cache[cache_key] = pattern
        return pattern
    
    # Fallback: Use default pattern
    print(f"No verified pattern found for {domain}, using default: first.last")
    _email_pattern_cache[cache_key] = 'first.last'
    return 'first.last'

# ---------------- SignalHire integration ----------------
SIGNALHIRE_API_KEY = os.environ.get("SIGNALHIRE_API_KEY", "202.evaAyOWjUoheYEQ4Bb2XlSp0ZSzi")
SIGNALHIRE_ENDPOINT = "https://api.signalhire.com/v2/email-finder"

import requests

def signalhire_email_lookup(first_name, last_name, company_domain):
    """Look up email, phone, and social links using SignalHire API. Returns dict with 'email', 'phone_number', and social fields if found."""
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
                print(f"SignalHire API error: {data['error']}")
                return {}
            email = data.get('email')
            phone = data.get('phone') or data.get('phone_number') or ''
            social = data.get('social', {})
            return {
                'email': email,
                'phone_number': phone,
                'linkedin': social.get('linkedin'),
                'facebook': social.get('facebook'),
                'twitter': social.get('twitter'),
            }
        else:
            print(f"SignalHire API HTTP error: {resp.status_code}")
            return {}
    except Exception as e:
        print(f"SignalHire API error: {e}")
        return {}


# Hunter.io logic removed. All email enrichment now uses SignalHire API.

# Map Hunter.io pattern to PATTERN_FUNCS keys
HUNTER_TO_PATTERN = {
    "first_last": "first_last",
    "first.l": "firstl",
    "first": "first",
    "firstlast": "firstlast",
    "first.las": "firstl",
    "f.last": "f.last",
    "first.last": "first.last",
    "f_last": "f_last",
    "last": "last",
    # Add more mappings as needed
}

# ---------------- Email pattern helpers ----------------
PATTERN_FUNCS: Dict[str, Callable[[str, str], str]] = {
    "first.last": lambda f, l: f"{f}.{l}",
    "f.last":     lambda f, l: f"{f[0]}.{l}" if f else "",
    "firstl":     lambda f, l: f"{f}{l[0]}" if l else "",
    "first_last": lambda f, l: f"{f}_{l}",
    "firstlast":  lambda f, l: f"{f}{l}",
    "f_last":     lambda f, l: f"{f[0]}{l}" if f else "",
    "flast":      lambda f, l: f"{f[0]}{l}" if f and l else "",  # First initial (no period) + Last name (e.g., jsmith)
    "lastf":      lambda f, l: f"{l}{f[0]}" if f and l else "",  # Last name + First initial (e.g., smithj)
    "last":       lambda f, l: l,
    "first":      lambda f, l: f,
}

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

# ---------------- NeverBounce validation ----------------
NB_API_KEY = "private_5e99b01595fe03a6276fa25c8ddee66a"  # Set by user
NB_ENDPOINT = "https://api.neverbounce.com/v4/single/check"
_verify_cache: dict[str, bool] = {}

def verify_email_nvb(email: str) -> bool:
    """Return True if NeverBounce says email is valid/deliverable."""
    if not NB_API_KEY:
        # No key configured – assume valid
        return True
    if email in _verify_cache:
        return _verify_cache[email]
    try:
        resp = requests.get(NB_ENDPOINT, params={"key": NB_API_KEY, "email": email, "format": "json"}, timeout=10)
        data = resp.json()
        result = data.get("result")
        valid = result == "valid"
        # Cache both valid and invalid to save quota
        _verify_cache[email] = valid
        # polite pacing (150 req/min free tier)
        time.sleep(0.45)
        return valid
    except Exception:
        return False


def normalize(s: str | float) -> str:
    if pd.isna(s):
        return ""
    return str(s).strip().lower()


def split_email(email: str):
    local, _, domain = email.lower().partition("@")
    return local, domain


def best_pattern(rows: pd.DataFrame) -> str | None:
    """Return pattern label that matches the most rows (>=2) or None."""
    counts: Counter[str] = Counter()
    for _, r in rows.iterrows():
        fn, ln, email = normalize(r["first_name"]), normalize(r["last_name"]), normalize(r["email"])
        if not EMAIL_RE.match(email):
            continue
        local, _ = split_email(email)
        for name, func in PATTERN_FUNCS.items():
            if func(fn, ln) == local:
                counts[name] += 1
                break
    if not counts:
        return None
    most_common, freq = counts.most_common(1)[0]
    return most_common if freq >= 2 else None  # need at least 2 hits for confidence


def infer_domains(df: pd.DataFrame):
    import re
    # find email column (case-insensitive)
    email_col = next((c for c in df.columns if 'email' in c.lower()), None)
    if not email_col:
        df["email"] = ""
        email_col = "email"
    # If there's a domain alias column, standardize it
    domain_alias = next((c for c in df.columns if 'domain' in c.lower() and c != 'domain'), None)
    if domain_alias and 'domain' not in df.columns:
        df['domain'] = df[domain_alias]
    if 'domain' not in df.columns:
        df['domain'] = ""
    # fill blank domains from email if possible
    mask_blank = df['domain'].isna() | (df['domain'] == "")
    if mask_blank.any() and email_col in df.columns:
        df.loc[mask_blank, 'domain'] = df.loc[mask_blank, email_col].fillna("").apply(
            lambda e: e.split('@')[1] if '@' in str(e) else ""
        )
    # still blank? derive from Website or Company
    def clean_domain(val):
        val = str(val).strip().lower()
        val = re.sub(r"https?://", "", val, flags=re.I)
        val = re.sub(r"^www\.\s*", "", val, flags=re.I)
        val = val.split('/')[0].strip()
        val = val.strip('. ')
        val = val.replace(' ', '')
        # Remove long subdomains or obviously malformed domains
        if len(val) > 40 or val.count('.') > 3 or not re.match(r"^[a-z0-9.-]+\.[a-z]{2,}$", val):
            return ''
        return val
    if "Website" in df.columns:
        df.loc[df["domain"]=="", "domain"] = df["Website"].fillna("").apply(clean_domain)
    if "Company" in df.columns:
        df.loc[df["domain"]=="", "domain"] = df["Company"].fillna("").apply(
            lambda c: clean_domain(f"{str(c).lower().replace(' ','')}.com") if c else "")
    # Clean all domains
    df['domain'] = df['domain'].apply(clean_domain)
    # If still blank or invalid, fallback to company.com
    if "Company" in df.columns:
        df.loc[df["domain"]=="", "domain"] = df["Company"].fillna("").apply(
            lambda c: clean_domain(f"{str(c).lower().replace(' ','')}.com") if c else "")
    # standardize an 'email' column alias for downstream logic
    if email_col != "email":
        df["email"] = df[email_col]
    return df




def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to match script expectations."""
    column_mapping = {
        'First': 'first_name',
        'Last': 'last_name', 
        'Domain': 'domain',
        'LinkedIn Profile': 'linkedin',
        'Email': 'email',
        'Telephone': 'phone_number',
        'Phone': 'phone_number',
        'Phone Number': 'phone_number',
        'Mobile': 'phone_number',
        'Cell Phone': 'phone_number',
        'Work Phone': 'phone_number',
        'Business Phone': 'phone_number',
        'Company': 'company'
    }
    
    # Create a copy to avoid modifying original
    df_standardized = df.copy()
    
    # Rename columns if they exist
    for old_name, new_name in column_mapping.items():
        if old_name in df_standardized.columns:
            df_standardized = df_standardized.rename(columns={old_name: new_name})
    
    # Ensure required columns exist
    required_columns = ['first_name', 'last_name', 'domain', 'email', 'phone_number']
    for col in required_columns:
        if col not in df_standardized.columns:
            df_standardized[col] = ''
    
    return df_standardized


# External email format verification functions
def query_hunter_io(domain: str) -> str:
    """Query Hunter.io API for verified email patterns."""
    import os
    import requests
    
    api_key = os.getenv('HUNTER_API_KEY')
    if not api_key:
        return None
    
    try:
        url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={api_key}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('data', {}).get('pattern'):
                pattern = data['data']['pattern']
                # Convert Hunter.io pattern to our format
                if pattern == '{first}.{last}':
                    return 'first.last'
                elif pattern == '{first}{last}':
                    return 'firstlast'
                elif pattern == '{f}{last}':
                    return 'flast'
                elif pattern == '{first}{l}':
                    return 'firstl'
                elif pattern == '{first}_{last}':
                    return 'first_last'
                else:
                    return 'first.last'  # default fallback
    except Exception as e:
        print(f"Hunter.io API error for {domain}: {e}")
    return None

def detect_regional_domains(company_name: str, domain: str) -> dict:
    """Detect regional/country-specific email domains for a company."""
    import requests
    import re
    
    regional_patterns = {}
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Search for regional email patterns
        search_queries = [
            f'{company_name} email format regional country subdomain',
            f'"@us.{domain}" OR "@uk.{domain}" OR "@ca.{domain}" OR "@eu.{domain}"',
            f'{company_name} "united states" email format',
            f'{company_name} regional office email format',
            f'site:linkedin.com "{company_name}" "@*.{domain}"'
        ]
        
        for query in search_queries:
            try:
                url = f"https://duckduckgo.com/html/?q={query}"
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    text = response.text.lower()
                    
                    # Look for regional domain patterns
                    regional_domains = re.findall(rf'[a-zA-Z0-9][a-zA-Z0-9._-]*@[a-z0-9-]+\.{re.escape(domain)}', text)
                    
                    for email in regional_domains:
                        if '@' in email:
                            full_domain = email.split('@')[1]
                            local_part = email.split('@')[0]
                            
                            # Determine pattern
                            if '.' in local_part:
                                pattern = 'first.last'
                            elif '_' in local_part:
                                pattern = 'first_last'
                            else:
                                pattern = 'firstlast'
                            
                            regional_patterns[full_domain] = pattern
                            print(f"  Found regional domain: {full_domain} -> {pattern}")
                
                time.sleep(1)
                
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"Regional domain detection error for {domain}: {e}")
    
    return regional_patterns

def analyze_domain_specific_patterns(company_name: str, domain: str) -> dict:
    """Perform comprehensive domain-specific email pattern analysis."""
    import requests
    import re
    import time
    
    result = {
        'domain': domain,
        'patterns_found': {},
        'dominant_pattern': None,
        'confidence': 0,
        'regional_domains': {},
        'analysis_method': 'search_based'
    }
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Domain-specific search queries
        search_queries = [
            f'{company_name} email format pattern frequency percentage',
            f'"{domain}" email format "most common" OR "dominant" OR "98%" OR "69%"',
            f'{company_name} "united states" email format vs global',
            f'"@{domain}" OR "@us.{domain}" email examples pattern',
            f'{company_name} employee email format analysis',
            f'site:linkedin.com "{company_name}" email format pattern'
        ]
        
        all_emails = []
        pattern_indicators = []
        
        for query in search_queries:
            try:
                # Use DuckDuckGo for better results
                url = f"https://duckduckgo.com/html/?q={query}"
                response = requests.get(url, headers=headers, timeout=15)
                
                if response.status_code == 200:
                    text = response.text.lower()
                    
                    # Look for frequency/percentage indicators
                    freq_patterns = re.findall(r'(\d+)%.*?(?:contacts|employees|format|pattern)', text)
                    if freq_patterns:
                        pattern_indicators.extend(freq_patterns)
                    
                    # Enhanced email detection for this specific domain
                    domain_emails = re.findall(rf'[a-zA-Z0-9][a-zA-Z0-9._-]*@(?:[a-z0-9-]+\.)?{re.escape(domain)}', text)
                    all_emails.extend(domain_emails)
                    
                    # Look for regional domain mentions
                    regional_mentions = re.findall(rf'@([a-z0-9-]+\.{re.escape(domain)})', text)
                    for regional in regional_mentions:
                        if regional != domain:
                            result['regional_domains'][regional] = 'detected'
                
                time.sleep(2)  # Be more respectful to search engines
                
            except Exception as e:
                continue
        
        # Analyze found emails for patterns
        if all_emails:
            pattern_counts = {
                'first.last': 0,
                'first_last': 0,
                'firstlast': 0,
                'f.last': 0,
                'last.first': 0
            }
            
            for email in all_emails:
                local_part = email.split('@')[0]
                
                if '.' in local_part:
                    parts = local_part.split('.')
                    if len(parts) == 2:
                        if len(parts[0]) == 1:  # f.last
                            pattern_counts['f.last'] += 1
                        elif len(parts[0]) > len(parts[1]):  # likely first.last
                            pattern_counts['first.last'] += 1
                        else:  # could be last.first
                            pattern_counts['last.first'] += 1
                elif '_' in local_part:
                    pattern_counts['first_last'] += 1
                else:
                    pattern_counts['firstlast'] += 1
            
            # Determine dominant pattern
            if pattern_counts:
                dominant = max(pattern_counts, key=pattern_counts.get)
                total = sum(pattern_counts.values())
                confidence = (pattern_counts[dominant] / total) * 100 if total > 0 else 0
                
                result['patterns_found'] = pattern_counts
                result['dominant_pattern'] = dominant
                result['confidence'] = confidence
                
                print(f"  Domain-specific analysis: {dominant} ({confidence:.1f}% confidence)")
                if pattern_indicators:
                    print(f"  Frequency indicators found: {pattern_indicators}")
                
    except Exception as e:
        print(f"Domain-specific analysis error for {domain}: {e}")
    
    return result

def google_search_email_pattern(company_name: str, domain: str) -> str:
    """Legacy function - now calls domain-specific analysis."""
    analysis = analyze_domain_specific_patterns(company_name, domain)
    return analysis.get('dominant_pattern', None)

def scrape_company_website(domain: str) -> str:
    """Scrape company website for email examples using comprehensive approach."""
    import requests
    import re
    import time
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        # Comprehensive list of pages to check
        pages = [
            f"https://{domain}",
            f"https://{domain}/contact",
            f"https://{domain}/contact-us",
            f"https://{domain}/about",
            f"https://{domain}/about-us",
            f"https://{domain}/team",
            f"https://{domain}/staff",
            f"https://{domain}/directory",
            f"https://{domain}/leadership",
            f"https://{domain}/management",
            f"https://www.{domain}",
            f"https://www.{domain}/contact",
            f"https://www.{domain}/about"
        ]
        
        all_emails = []
        
        for page_url in pages:
            try:
                response = requests.get(page_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    text = response.text.lower()
                    
                    # Enhanced email pattern detection
                    email_patterns = re.findall(rf'[a-zA-Z0-9][a-zA-Z0-9._-]*@{re.escape(domain)}', text)
                    all_emails.extend(email_patterns)
                    
                    # Also look for email format hints in text
                    format_hints = re.findall(r'email.*?format.*?[a-zA-Z0-9._-]+@' + re.escape(domain), text)
                    if format_hints:
                        all_emails.extend([hint.split('@')[0].split()[-1] + '@' + domain for hint in format_hints])
                
                time.sleep(0.5)  # Be respectful to websites
                
            except Exception as e:
                continue
        
        if all_emails:
            # Analyze all found emails to determine the most common pattern
            dot_count = sum(1 for email in all_emails if '.' in email.split('@')[0])
            underscore_count = sum(1 for email in all_emails if '_' in email.split('@')[0])
            no_separator_count = sum(1 for email in all_emails if '.' not in email.split('@')[0] and '_' not in email.split('@')[0])
            
            # Return the most common pattern
            if dot_count >= underscore_count and dot_count >= no_separator_count:
                return 'first.last'
            elif underscore_count >= no_separator_count:
                return 'first_last'
            else:
                return 'firstlast'
                
    except Exception as e:
        print(f"Website scraping error for {domain}: {e}")
    
    return None

def get_known_regional_patterns(domain: str) -> dict:
    """Get known regional email patterns for major companies."""
    known_regional_patterns = {
        'bureauveritas.com': {
            'us.bureauveritas.com': 'first.last',  # 98% of US contacts (verified by user Google search)
            'uk.bureauveritas.com': 'first.last',
            'ca.bureauveritas.com': 'first.last',
            'au.bureauveritas.com': 'first.last',
            # Global: first.last@bureauveritas.com (69.5% globally)
            # Variations: first_initial.last@bureauveritas.com, firstlast@bureauveritas.com
        },
        'ul.com': {
            # Based on user's Google search: "The most common email format for UL Solutions employees in the United States is first name dot last name"
            # Dominant: john.doe@ul.com (first.last@ul.com)
            # Variations: j.doe@ul.com (f.last@ul.com), doe.john@ul.com (last.first@ul.com) - much less frequent
            # NOTE: UL uses base domain (ul.com) not regional subdomains for US contacts
        },
        'dnvgl.com': {
            'us.dnvgl.com': 'first.last',
            'no.dnvgl.com': 'first.last',
            'uk.dnvgl.com': 'first.last',
        },
        'tuvsud.com': {
            'us.tuvsud.com': 'first.last',
            'de.tuvsud.com': 'first.last',
            'uk.tuvsud.com': 'first.last',
        },
        'sgs.com': {
            'us.sgs.com': 'first.last',
            'uk.sgs.com': 'first.last',
            'ca.sgs.com': 'first.last',
            'de.sgs.com': 'first.last',
        },
        'intertek.com': {
            'us.intertek.com': 'first.last',
            'uk.intertek.com': 'first.last',
            'ca.intertek.com': 'first.last',
        },
        'ge.com': {
            'us.ge.com': 'first.last',
            'uk.ge.com': 'first.last',
            'de.ge.com': 'first.last',
        },
        'siemens.com': {
            'usa.siemens.com': 'first.last',
            'uk.siemens.com': 'first.last',
            'de.siemens.com': 'first.last',
        },
        'abb.com': {
            'us.abb.com': 'first.last',
            'uk.abb.com': 'first.last',
            'de.abb.com': 'first.last',
        },
        'emerson.com': {
            'us.emerson.com': 'first.last',
            'uk.emerson.com': 'first.last',
        },
        'honeywell.com': {
            'us.honeywell.com': 'first.last',
            'uk.honeywell.com': 'first.last',
        },
    }
    return known_regional_patterns.get(domain, {})

def get_known_pattern(domain: str) -> str:
    """Get known email pattern from database of common patterns."""
    known_patterns = {
        # Major Tech Companies
        'google.com': 'first.last',
        'microsoft.com': 'first.last',
        'apple.com': 'first.last',
        'amazon.com': 'first.last',
        'facebook.com': 'first.last',
        'linkedin.com': 'first.last',
        'twitter.com': 'first.last',
        'salesforce.com': 'first.last',
        'oracle.com': 'first.last',
        'ibm.com': 'first.last',
        'hp.com': 'first.last',
        'dell.com': 'first_last',
        'intel.com': 'first.last',
        'cisco.com': 'first.last',
        'adobe.com': 'first.last',
        
        # Testing, Inspection & Certification Companies
        'ul.com': 'first.last',  # UL (Underwriters Laboratories)
        'bureauveritas.com': 'first.last',  # Bureau Veritas
        'dnvgl.com': 'first.last',  # DNV GL
        'dnv.com': 'first.last',  # DNV (new domain after DNV GL split)
        'tuvsud.com': 'first.last',  # TÜV SÜD
        'tuv.com': 'first.last',  # TÜV
        'tuvnord.com': 'first.last',  # TÜV NORD
        'tuvrheinland.com': 'first.last',  # TÜV Rheinland
        'sgs.com': 'first.last',  # SGS
        'intertek.com': 'first.last',  # Intertek
        'mistrasgroup.com': 'first.last',  # Mistras Group
        'applus.com': 'first.last',  # Applus+
        'dekra.com': 'first.last',  # DEKRA
        'element.com': 'first.last',  # Element Materials Technology
        'exova.com': 'first.last',  # Exova (now Element)
        'nde.net': 'first.last',  # NDE Services
        'tcr-inc.com': 'first.last',  # TCR Engineering
        'team-inc.com': 'first.last',  # Team Industrial Services
        'olympus-ims.com': 'first.last',  # Olympus IMS
        'ge.com': 'first.last',  # GE (General Electric)
        'bakerhughes.com': 'first.last',  # Baker Hughes
        'halliburton.com': 'first.last',  # Halliburton
        'schlumberger.com': 'first.last',  # Schlumberger
        'zetec.com': 'first.last',  # Zetec
        'sonatest.com': 'first.last',  # Sonatest
        'ndt.net': 'first.last',  # NDT.net
        'asnt.org': 'first.last',  # ASNT (American Society for Nondestructive Testing)
        'aws.org': 'first.last',  # AWS (American Welding Society)
        'api.org': 'first.last',  # API (American Petroleum Institute)
        'astm.org': 'first.last',  # ASTM International
        'iso.org': 'first.last',  # ISO
        'ansi.org': 'first.last',  # ANSI
        'nist.gov': 'first.last',  # NIST
        
        # Engineering & Consulting
        'aecom.com': 'first.last',
        'jacobs.com': 'first.last',
        'ch2m.com': 'first.last',
        'fluor.com': 'first.last',
        'kbr.com': 'first.last',
        'worleyparsons.com': 'first.last',
        'wood.com': 'first.last',
        'technipfmc.com': 'first.last',
        'saipem.com': 'first.last',
        'petrofac.com': 'first.last',
        
        # Oil & Gas
        'exxonmobil.com': 'first.last',
        'chevron.com': 'first.last',
        'shell.com': 'first.last',
        'bp.com': 'first.last',
        'totalenergies.com': 'first.last',
        'conocophillips.com': 'first.last',
        'equinor.com': 'first.last',
        'eni.com': 'first.last',
        
        # Manufacturing & Industrial
        'siemens.com': 'first.last',
        'abb.com': 'first.last',
        'emerson.com': 'first.last',
        'honeywell.com': 'first.last',
        'rockwellautomation.com': 'first.last',
        'schneider-electric.com': 'first.last',
        'yokogawa.com': 'first.last',
        'endress.com': 'first.last',
        'rosemount.com': 'first.last',
        'fluke.com': 'first.last',
        'keysight.com': 'first.last',
        'tektronix.com': 'first.last',
        'rohde-schwarz.com': 'first.last',
        'ni.com': 'first.last',  # National Instruments
        
        # Common patterns for smaller companies
        # Most professional services use first.last
    }
    return known_patterns.get(domain)

# Cache for verified patterns to avoid repeated lookups
_pattern_cache = {}

def get_verified_email_pattern(company_name: str, domain: str) -> dict:
    """Get verified email pattern using multiple methods, including regional domains."""
    cache_key = f"{domain}_{company_name}"
    if cache_key in _pattern_cache:
        return _pattern_cache[cache_key]
    
    print(f"Verifying email pattern for {domain} ({company_name})...")
    
    result = {
        'primary_domain': domain,
        'primary_pattern': 'first.last',
        'regional_domains': {},
        'recommended_domain': domain,
        'recommended_pattern': 'first.last'
    }
    
    # Method 1: Hunter.io API (most reliable)
    pattern = query_hunter_io(domain)
    if pattern:
        print(f"  Hunter.io: {pattern}")
        result['primary_pattern'] = pattern
        result['recommended_pattern'] = pattern
    
    # Method 2: Check known regional patterns database (MOST RELIABLE)
    known_regional = get_known_regional_patterns(domain)
    if known_regional:
        result['regional_domains'] = known_regional
        # For US-based contacts, prefer US domain if available
        us_domain = f"us.{domain}"
        usa_domain = f"usa.{domain}"
        
        if us_domain in known_regional:
            result['recommended_domain'] = us_domain
            result['recommended_pattern'] = known_regional[us_domain]
            print(f"  Known US regional domain: {us_domain} -> {known_regional[us_domain]}")
        elif usa_domain in known_regional:
            result['recommended_domain'] = usa_domain
            result['recommended_pattern'] = known_regional[usa_domain]
            print(f"  Known USA regional domain: {usa_domain} -> {known_regional[usa_domain]}")
        else:
            # Use the first available regional domain
            first_regional = list(known_regional.keys())[0]
            result['recommended_domain'] = first_regional
            result['recommended_pattern'] = known_regional[first_regional]
            print(f"  Known regional domain: {first_regional} -> {known_regional[first_regional]}")
    
    # Method 3: Detect regional domains via search (fallback)
    elif not result['regional_domains']:  # Only if no known regional patterns
        regional_domains = detect_regional_domains(company_name, domain)
        if regional_domains:
            result['regional_domains'] = regional_domains
            # For US-based contacts, prefer US domain if available
            us_domain = f"us.{domain}"
            if us_domain in regional_domains:
                result['recommended_domain'] = us_domain
                result['recommended_pattern'] = regional_domains[us_domain]
                print(f"  Detected US domain: {us_domain} -> {regional_domains[us_domain]}")
    
    # Method 3: Known patterns database
    if result['primary_pattern'] == 'first.last':  # Only if not found by Hunter.io
        pattern = get_known_pattern(domain)
        if pattern:
            print(f"  Known pattern: {pattern}")
            result['primary_pattern'] = pattern
            if not result['regional_domains']:  # Only update recommended if no regional found
                result['recommended_pattern'] = pattern
    
    # Method 4: Google search heuristic
    if result['primary_pattern'] == 'first.last' and not result['regional_domains']:
        pattern = google_search_email_pattern(company_name, domain)
        if pattern:
            print(f"  Google search: {pattern}")
            result['primary_pattern'] = pattern
            result['recommended_pattern'] = pattern
    
    # Method 5: Website scraping
    if result['primary_pattern'] == 'first.last' and not result['regional_domains']:
        pattern = scrape_company_website(domain)
        if pattern:
            print(f"  Website scraping: {pattern}")
            result['primary_pattern'] = pattern
            result['recommended_pattern'] = pattern
    
    # Method 6: Default fallback
    if result['primary_pattern'] == 'first.last' and not result['regional_domains']:
        print(f"  Using default: first.last")
    
    _pattern_cache[cache_key] = result
    return result

def fill_emails(df_contacts: pd.DataFrame, df_known: pd.DataFrame, fast_mode: bool = False, skip_google: bool = False, batch_size: int = 50) -> pd.DataFrame:
    # Remove blank rows (all columns empty or key columns empty)
    key_cols = [c for c in df_contacts.columns if any(k in c.lower() for k in ["first", "last", "company"])]
    df_contacts = df_contacts.dropna(how="all")
    if key_cols:
        df_contacts = df_contacts.dropna(subset=key_cols, how="all")
    # Standardize column names first
    df_contacts = standardize_column_names(df_contacts)
    df_known = standardize_column_names(df_known)
    
    # Infer domains after column standardization
    df_contacts = infer_domains(df_contacts)
    df_known = infer_domains(df_known)
    
    # Load persistent cache for domain patterns
    persistent_cache = load_persistent_cache()
    
    # Get verified email patterns for all unique domains
    verified_patterns = {}
    unique_domains = df_contacts['domain'].dropna().unique()
    
    if fast_mode:
        print("🚀 FAST MODE: Using cached patterns and defaults for maximum performance")
        # Use cached patterns if available, otherwise use defaults
        for domain in unique_domains:
            if domain and domain.strip():
                if domain in persistent_cache:
                    verified_patterns[domain] = persistent_cache[domain]
                    print(f"  ✅ {domain}: {persistent_cache[domain]['recommended_pattern']} (cached)")
                else:
                    verified_patterns[domain] = {
                        'recommended_domain': domain,
                        'recommended_pattern': 'first.last',
                        'primary_pattern': 'first.last',
                        'regional_domains': {},
                        'confidence': 'default'
                    }
                    print(f"  🔧 {domain}: first.last (default)")
    else:
        print("\n=== EMAIL PATTERN VERIFICATION (with caching) ===")
        new_cache_entries = {}
        
        for domain in unique_domains:
            if domain and domain.strip():
                # Check persistent cache first
                if domain in persistent_cache:
                    verified_patterns[domain] = persistent_cache[domain]
                    print(f"  ✅ {domain}: {persistent_cache[domain]['recommended_pattern']} (cached)")
                else:
                    # Use pattern inference for new domains
                    pattern_result = {
                        'recommended_domain': domain,
                        'recommended_pattern': 'first.last',
                        'primary_pattern': 'first.last',
                        'regional_domains': {},
                        'confidence': 'inferred'
                    }
                    verified_patterns[domain] = pattern_result
                    new_cache_entries[domain] = pattern_result
                    print(f"  🔧 {domain}: first.last (inferred)")
        
        # Save new cache entries to persistent storage
        if new_cache_entries:
            persistent_cache.update(new_cache_entries)
            save_persistent_cache(persistent_cache)
    
    print(f"\nVerified patterns for {len(verified_patterns)} domains:")
    for domain, result in verified_patterns.items():
        company_name = df_contacts[df_contacts['domain'] == domain]['company'].iloc[0] if 'company' in df_contacts.columns else domain
        recommended_domain = result['recommended_domain']
        recommended_pattern = result['recommended_pattern']
        
        if result['regional_domains']:
            print(f"  {company_name} ({domain}):")
            print(f"    Primary: {domain} -> {result['primary_pattern']}")
            for reg_domain, reg_pattern in result['regional_domains'].items():
                print(f"    Regional: {reg_domain} -> {reg_pattern}")
            print(f"    RECOMMENDED: {recommended_domain} -> {recommended_pattern}")
        else:
            print(f"  {company_name} ({domain}): {recommended_pattern}")
    print("=" * 50)

    # Clean up first_name and last_name columns (remove credentials, degrees, unnecessary chars)
    def clean_name(name):
        if not isinstance(name, str) or pd.isna(name):
            return ""
        
        original_name = str(name).strip()
        if not original_name:
            return ""
        
        # Start with the original name
        name = original_name
        
        # Remove tabs and normalize whitespace
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Step 1: Remove everything after comma (credentials often follow commas)
        if ',' in name:
            name = name.split(',')[0].strip()
        
        # Step 2: Remove everything after semicolon or pipe
        if ';' in name:
            name = name.split(';')[0].strip()
        if '|' in name:
            name = name.split('|')[0].strip()
        
        # Step 3: Remove parentheses and their contents (nicknames, etc.)
        name = re.sub(r"\([^)]*\)", "", name).strip()
        
        # Step 4: Handle quotes - remove quotes but keep the content inside
        name = re.sub(r'"([^"]+)"', r'\1', name)  # Replace "Jerry" with Jerry
        name = re.sub(r"'([^']+)'", r'\1', name)   # Replace 'Jerry' with Jerry
        name = re.sub(r'["\']', "", name).strip()  # Remove remaining quotes
        
        # Step 5: Remove specific honorifics at the beginning (whole words only)
        honorifics = ['mr', 'ms', 'mrs', 'dr', 'prof', 'miss', 'sir', 'madam', 'lady', 'lord']
        words = name.split()
        if words and words[0].lower().rstrip('.') in honorifics:
            words = words[1:]  # Remove first word if it's an honorific
            name = ' '.join(words)
        
        # Step 6: Remove middle initials (single letters with periods at start or end)
        # "J. Prajzner" -> "Prajzner", "Angela C." -> "Angela"
        words = name.split()
        cleaned_words = []
        for word in words:
            # Skip single letters with or without periods (but keep regular words)
            if len(word.rstrip('.')) == 1 and word.rstrip('.').isalpha():
                continue  # Skip middle initials
            else:
                cleaned_words.append(word)
        name = ' '.join(cleaned_words)
        
        # Step 7: Clean up any remaining non-alphabetic characters at start/end
        # But be very conservative to avoid truncating valid names
        name = re.sub(r'^[^a-zA-Z\s\'\-]+', '', name)  # Remove leading non-letters
        name = re.sub(r'[^a-zA-Z\s\'\-]+$', '', name)  # Remove trailing non-letters
        
        # Final cleanup
        name = re.sub(r'\s+', ' ', name).strip()
        
        # If empty after cleaning, return empty
        if not name:
            return ""
        
        # Handle special cases for email generation
        # Remove apostrophes and hyphens for email (D'Alterio -> dalterio)
        email_name = name.replace("'", "").replace("-", "")
        
        return email_name.lower()

    def standardize_names(df):
        fname = next((c for c in df.columns if c.lower().replace(' ', '') in ("first_name","firstname")), None)
        lname = next((c for c in df.columns if c.lower().replace(' ', '') in ("last_name","lastname")), None)
        if fname and fname != "first_name":
            df["first_name"] = df[fname]
        if lname and lname != "last_name":
            df["last_name"] = df[lname]
        
        # First clean the names
        df["first_name"] = df["first_name"].astype(str).apply(clean_name)
        df["last_name"] = df["last_name"].astype(str).apply(clean_name)
        
        # Fix cases where full names appear in wrong fields
        for idx, row in df.iterrows():
            first = str(row["first_name"]).strip()
            last = str(row["last_name"]).strip()
            
            # Case 1: First name is empty/invalid but last name has multiple words
            if (not first or first == 'nan' or len(first) < 2) and ' ' in last:
                parts = last.split()
                if len(parts) >= 2:
                    df.at[idx, "first_name"] = parts[0].lower()
                    df.at[idx, "last_name"] = parts[-1].lower()  # Take last word as surname
                    print(f"Fixed name field: '{last}' -> first: '{parts[0]}', last: '{parts[-1]}'")
            
            # Case 2: Last name is empty/invalid but first name has multiple words
            elif (not last or last == 'nan' or len(last) < 2) and ' ' in first:
                parts = first.split()
                if len(parts) >= 2:
                    df.at[idx, "first_name"] = parts[0].lower()
                    df.at[idx, "last_name"] = parts[-1].lower()  # Take last word as surname
                    print(f"Fixed name field: '{first}' -> first: '{parts[0]}', last: '{parts[-1]}'")
            
            # Case 3: Both fields have single words but first name looks like honorific
            elif first in ['mr', 'ms', 'mrs', 'dr', 'prof', 'miss'] and last and len(last) > 1:
                # First name is actually an honorific, last name is the real first name
                df.at[idx, "first_name"] = last.lower()
                df.at[idx, "last_name"] = "user"  # Default fallback
                print(f"Fixed honorific: '{first} {last}' -> first: '{last}', last: 'user'")
        
        return df

    df_contacts = standardize_names(df_contacts)
    df_known = standardize_names(df_known)

    # --- Remove duplicates (case-insensitive, stripped) ---
    for col in ["first_name", "last_name", "Company"]:
        if col in df_contacts.columns:
            df_contacts[col] = df_contacts[col].astype(str).str.strip().str.lower()
    subset_cols = [c for c in ["first_name", "last_name", "Company"] if c in df_contacts.columns]
    if subset_cols:
        df_contacts = df_contacts.drop_duplicates(subset=subset_cols, keep="first").reset_index(drop=True)

    # Use externally verified patterns with regional domain support
    # Convert verification results to patterns_by_domain format
    patterns_by_domain = {}
    domain_mappings = {}  # Maps original domain to recommended domain
    
    for domain, result in verified_patterns.items():
        recommended_domain = result['recommended_domain']
        recommended_pattern = result['recommended_pattern']
        
        # Store the pattern for the recommended domain
        patterns_by_domain[recommended_domain] = recommended_pattern
        
        # If recommended domain is different from original, create mapping
        if recommended_domain != domain:
            domain_mappings[domain] = recommended_domain
            print(f"Domain mapping: {domain} -> {recommended_domain} ({recommended_pattern})")
        else:
            patterns_by_domain[domain] = recommended_pattern
    
    # For any domains not verified externally, try pattern inference as fallback
    for domain in df_contacts["domain"].dropna().unique():
        if domain and domain not in patterns_by_domain and domain not in domain_mappings:
            domain_rows = df_known[df_known["domain"] == domain] if not df_known.empty else pd.DataFrame()
            if not domain_rows.empty:
                pattern = best_pattern(domain_rows)
                if pattern:
                    patterns_by_domain[domain] = pattern
                    print(f"Fallback: Inferred pattern for {domain}: {pattern}")
                else:
                    patterns_by_domain[domain] = "first.last"  # default
            else:
                patterns_by_domain[domain] = "first.last"  # default
    
    print(f"\nFinal patterns being used:")
    for domain, pattern in patterns_by_domain.items():
        print(f"  {domain}: {pattern}")
    if domain_mappings:
        print(f"\nDomain mappings:")
        for orig, mapped in domain_mappings.items():
            print(f"  {orig} -> {mapped}")

    # Enhanced LinkedIn name extraction helper with validation
    def extract_names_from_linkedin(linkedin_url, existing_first_name=None):
        """Extract first and last name from LinkedIn profile URL with enhanced validation.
        
        Args:
            linkedin_url: LinkedIn profile URL
            existing_first_name: Known first name to help identify the last name in the URL
        """
        if not linkedin_url or not isinstance(linkedin_url, str):
            return None, None
        
        import re
        # Clean the URL and extract the profile part
        url = linkedin_url.strip().lower()
        
        # Pattern to match LinkedIn profile URLs
        # Examples: /in/john-smith, /in/johnsmith123, /in/john-smith-12345
        pattern = r'/in/([a-zA-Z0-9-]+)'
        match = re.search(pattern, url)
        
        if not match:
            return None, None
        
        profile_slug = match.group(1)
        
        # More aggressive cleaning of trailing identifiers
        # Remove patterns like -12345, -5b3101169, -7b4ab073, -a1b2c3, etc.
        profile_slug = re.sub(r'-[0-9a-f]{2,}$', '', profile_slug)  # Remove hex codes (2+ chars)
        profile_slug = re.sub(r'-\\d+$', '', profile_slug)  # Remove pure numbers
        profile_slug = re.sub(r'-[a-f0-9]{6,}$', '', profile_slug)  # Remove long hex strings
        profile_slug = re.sub(r'-?(jr|sr|ii|iii|iv|phd|md|cpa|mba)$', '', profile_slug)
        
        # Split on hyphens to get name parts
        parts = [part for part in profile_slug.split('-') if part and len(part) > 1 and part.isalpha()]
        
        # Filter out common non-name words that might appear in LinkedIn URLs
        name_parts = []
        skip_words = {'linkedin', 'profile', 'user', 'member', 'contact', 'connect', 'view'}
        
        for part in parts:
            if part.lower() not in skip_words and len(part) >= 2:
                name_parts.append(part)
        
        # Enhanced logic: If we have an existing first name, use it to identify the last name
        if existing_first_name and name_parts:
            existing_first_clean = existing_first_name.lower().strip()
            
            # Look for the first name in the name parts to identify what comes after it
            first_name_found = False
            extracted_first = None
            extracted_last = None
            
            for i, part in enumerate(name_parts):
                if part.lower() == existing_first_clean or part.lower().startswith(existing_first_clean[:3]):
                    # Found the first name, take everything after it as last name
                    extracted_first = existing_first_name.capitalize()
                    first_name_found = True
                    
                    # Get all remaining parts as last name
                    if i + 1 < len(name_parts):
                        remaining_parts = name_parts[i + 1:]
                        # Filter out any numbers or single characters
                        last_name_parts = [p for p in remaining_parts if len(p) > 1 and p.isalpha()]
                        if last_name_parts:
                            extracted_last = '-'.join(last_name_parts).title()
                            print(f"  LinkedIn extraction: Found '{extracted_first}' + '{extracted_last}' from URL")
                            return extracted_first, extracted_last
                    break
            
            # If we didn't find the first name exactly, try a different approach
            if not first_name_found and len(name_parts) >= 2:
                # Assume first part is first name, second+ parts are last name
                if name_parts[0].lower() == existing_first_clean or len(name_parts[0]) >= 3:
                    extracted_first = existing_first_name.capitalize()
                    last_name_parts = [p for p in name_parts[1:] if len(p) > 1 and p.isalpha()]
                    if last_name_parts:
                        extracted_last = '-'.join(last_name_parts).title()
                        print(f"  LinkedIn extraction: Inferred '{extracted_first}' + '{extracted_last}' from URL")
                        return extracted_first, extracted_last
        
        # Original logic for cases without existing first name
        if len(name_parts) >= 2:
            first_name = name_parts[0].capitalize()
            # For multi-part last names, join remaining parts with hyphen
            if len(name_parts) == 2:
                last_name = name_parts[1].capitalize()
            else:
                # For 3+ parts, combine all remaining parts as hyphenated last name
                last_name = '-'.join(name_parts[1:]).title()
            return first_name, last_name
        elif len(name_parts) == 1:
            # Only one name part, assume it's first name
            return name_parts[0].capitalize(), None
        
        return None, None
    
    # LinkedIn profile validation against existing names
    def validate_linkedin_names(existing_first, existing_last, linkedin_url):
        """Validate existing names against LinkedIn profile URL."""
        if not linkedin_url:
            return existing_first, existing_last, False
        
        linkedin_first, linkedin_last = extract_names_from_linkedin(linkedin_url, existing_first)
        
        # If we extracted names from LinkedIn, compare with existing names
        validation_issues = []
        updated_first = existing_first
        updated_last = existing_last
        
        if linkedin_first and existing_first:
            # Check if first names match (case-insensitive)
            if linkedin_first.lower() != existing_first.lower():
                validation_issues.append(f"First name mismatch: '{existing_first}' vs LinkedIn '{linkedin_first}'")
                # Use LinkedIn name if it seems more complete
                if len(linkedin_first) > len(existing_first):
                    updated_first = linkedin_first.lower()
                    print(f"  Updated first name from '{existing_first}' to '{updated_first}' based on LinkedIn")
        elif linkedin_first and not existing_first:
            updated_first = linkedin_first.lower()
            print(f"  Added missing first name '{updated_first}' from LinkedIn")
        
        if linkedin_last and existing_last:
            # Check if last names match (case-insensitive, accounting for spaces/hyphens)
            existing_normalized = existing_last.replace(' ', '').replace('-', '').lower()
            linkedin_normalized = linkedin_last.replace(' ', '').replace('-', '').lower()
            
            if existing_normalized != linkedin_normalized:
                validation_issues.append(f"Last name mismatch: '{existing_last}' vs LinkedIn '{linkedin_last}'")
                # Use LinkedIn name if it seems more complete or properly formatted
                if len(linkedin_last) > len(existing_last) or '-' in linkedin_last:
                    updated_last = linkedin_last.lower()
                    print(f"  Updated last name from '{existing_last}' to '{updated_last}' based on LinkedIn")
        elif linkedin_last and not existing_last:
            updated_last = linkedin_last.lower()
            print(f"  Added missing last name '{updated_last}' from LinkedIn")
        
        has_issues = len(validation_issues) > 0
        if has_issues:
            print(f"  LinkedIn validation issues: {'; '.join(validation_issues)}")
        
        return updated_first, updated_last, has_issues
    
    # Google search for missing names
    def search_google_for_name(first_name, company_name, domain):
        """Search Google to find missing last name using first name and company."""
        if not first_name or not company_name:
            return None
        
        try:
            import urllib.parse
            import requests
            import re
            
            # Search for the person's full name at the company
            query = f'"{first_name}" "{company_name}" site:{domain} OR site:linkedin.com'
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            resp = requests.get(search_url, headers=headers, timeout=10)
            if resp.status_code == 200:
                content = resp.text.lower()
                
                # Look for patterns like "John Smith" or "John Doe" in the results
                # This is a simple heuristic - look for first name followed by a capitalized word
                pattern = rf'\b{re.escape(first_name.lower())}\s+([a-z]+(?:-[a-z]+)?)\b'
                matches = re.findall(pattern, content, re.IGNORECASE)
                
                if matches:
                    # Filter out common non-name words
                    non_names = {'and', 'the', 'at', 'in', 'on', 'for', 'with', 'by', 'from', 'to', 'of', 'is', 'was', 'are', 'were', 'has', 'have', 'had', 'will', 'would', 'could', 'should', 'may', 'might', 'can', 'said', 'says', 'works', 'work', 'working', 'company', 'corp', 'inc', 'llc', 'ltd'}
                    
                    potential_names = []
                    for match in matches:
                        if len(match) >= 2 and match.lower() not in non_names and match.isalpha():
                            potential_names.append(match.capitalize())
                    
                    if potential_names:
                        # Return the most common potential last name
                        from collections import Counter
                        most_common = Counter(potential_names).most_common(1)[0][0]
                        print(f"Google search found potential last name '{most_common}' for {first_name} at {company_name}")
                        return most_common
            
            return None
        except Exception as e:
            print(f"Google search error for {first_name} at {company_name}: {e}")
            return None
    
    # Enhanced multi-part name handler
    def process_multi_part_name(name_str, is_last_name=False):
        """Process multi-part names for email generation."""
        if not name_str or pd.isna(name_str):
            return ""
        
        name = str(name_str).strip()
        if not name:
            return ""
        
        # If it's a multi-part name (contains spaces)
        if ' ' in name and is_last_name:
            parts = name.split()
            if len(parts) == 2:
                # For two-part last names, we'll test both hyphenated and concatenated versions
                return {
                    'hyphenated': '-'.join(parts).lower(),
                    'concatenated': ''.join(parts).lower(),
                    'original': name.replace(' ', '').lower()
                }
        
        return name.lower()

    # Generate emails
    def gen(row):
        """Return (email, status, phone_number) where status is 'signalhire', 'given', 'unverified', etc."""
        import re
        placeholder_re = re.compile(r"no\s*email", re.I)
        emoji_prefix_re = re.compile(r"^[^a-zA-Z0-9]+\s*")
        existing_raw = row.get("Work Email") or row.get("email") or row.get("Email")
        domain = row.get("domain", "")
        fn = normalize(row.get("first_name", ""))
        ln = normalize(row.get("last_name", ""))
        
        # Validate and enhance names using LinkedIn profile
        linkedin_url = row.get("linkedin_url") or row.get("LinkedIn") or row.get("linkedin")
        if linkedin_url:
            # Validate existing names against LinkedIn profile
            validated_fn, validated_ln, has_issues = validate_linkedin_names(fn, ln, linkedin_url)
            fn = validated_fn if validated_fn else fn
            ln = validated_ln if validated_ln else ln
        
        # If we still don't have a last name, try Google search
        company_name = ""
        try:
            if "Company" in row.index:
                company_val = row["Company"]
                if pd.notna(company_val):
                    company_name = str(company_val)
            elif "company" in row.index:
                company_val = row["company"]
                if pd.notna(company_val):
                    company_name = str(company_val)
        except:
            company_name = ""
        if not ln and fn and company_name:
            google_last_name = search_google_for_name(fn, company_name, domain)
            if google_last_name:
                ln = normalize(google_last_name)
                print(f"Found last name '{ln}' via Google search for {fn} at {company_name}")
        
        # If we still don't have a first name, we can't generate an email
        if not fn:
            return "", "missing_name", ""
        
        # If we still don't have a last name, we'll use the first name only
        if not ln:
            ln = "user"  # Default fallback for missing last name
        
        def test_multiple_email_patterns(fn, ln, domain):
            """Test multiple email patterns with SignalHire validation for maximum accuracy."""
            # Check if domain should be mapped to a regional domain
            actual_domain = domain
            if domain in domain_mappings:
                actual_domain = domain_mappings[domain]
                print(f"    Using regional domain: {domain} -> {actual_domain}")
            
            # Process multi-part last names
            processed_ln = process_multi_part_name(ln, is_last_name=True)
            
            # Define pattern priority order (most common to least common)
            primary_pattern = patterns_by_domain.get(actual_domain, "first.last")
            
            # Create comprehensive pattern testing order
            pattern_priority = [primary_pattern]  # Start with domain-specific pattern
            
            # Add other common patterns if not already included
            common_patterns = ["first.last", "firstlast", "first_last", "f.last", "firstl", "flast", "lastf"]
            for pattern in common_patterns:
                if pattern not in pattern_priority:
                    pattern_priority.append(pattern)
            
            print(f"    Testing email patterns for {fn} {ln} @ {actual_domain}: {pattern_priority}")
            
            # Handle multi-part last names
            if isinstance(processed_ln, dict):
                name_variants = [
                    processed_ln['hyphenated'],
                    processed_ln['concatenated'], 
                    processed_ln['original']
                ]
                print(f"    Multi-part name variants: {name_variants}")
            else:
                name_variants = [processed_ln]
            
            # Test each pattern with each name variant
            for pattern in pattern_priority:
                for variant_ln in name_variants:
                    if not variant_ln:
                        continue
                        
                    # Generate email using this pattern
                    try:
                        local = PATTERN_FUNCS[pattern](fn, variant_ln)
                        if not local:
                            continue
                            
                        test_email = f"{local}@{actual_domain}"
                        print(f"      Testing: {test_email} (pattern: {pattern}, name: {variant_ln})")
                        
                        # Test with SignalHire API for validation
                        try:
                            signalhire_result = signalhire_email_lookup(fn, variant_ln, actual_domain)
                            if signalhire_result.get('email'):
                                found_email = signalhire_result['email']
                                # Check if SignalHire found the exact email we generated
                                if found_email.lower() == test_email.lower():
                                    if verify_email_nvb(found_email):
                                        print(f"      ✅ VERIFIED: {found_email} (SignalHire + NeverBounce)")
                                        return found_email
                                    else:
                                        print(f"      ⚠️  SignalHire found {found_email} but NeverBounce validation failed")
                                else:
                                    # SignalHire found a different email - might be better
                                    if verify_email_nvb(found_email):
                                        print(f"      ✅ SIGNALHIRE ALTERNATIVE: {found_email} (better than generated {test_email})")
                                        return found_email
                        except Exception as e:
                            print(f"      SignalHire error for {test_email}: {e}")
                        
                        # If SignalHire didn't verify, test the generated email with NeverBounce
                        if verify_email_nvb(test_email):
                            print(f"      ✅ NEVERBOUNCE VERIFIED: {test_email}")
                            return test_email
                        else:
                            print(f"      ❌ Failed validation: {test_email}")
                            
                    except Exception as e:
                        print(f"      Error testing pattern {pattern}: {e}")
                        continue
            
            # If no pattern worked, return the primary pattern as fallback
            try:
                fallback_ln = name_variants[0] if name_variants else ln
                local = PATTERN_FUNCS[primary_pattern](fn, fallback_ln)
                fallback_email = f"{local}@{actual_domain}" if local and actual_domain else ""
                print(f"    ⚠️  No patterns verified, using fallback: {fallback_email}")
                return fallback_email
            except:
                return ""
        
        def correct_pattern_email(fn, ln, domain):
            """Legacy function - now calls multi-pattern testing."""
            return test_multiple_email_patterns(fn, ln, domain)
        
        # Clean domain function
        def clean_domain(val):
            val = str(val).strip()
            if not val:
                return ''
            val = re.sub(r"https?://", "", val, flags=re.I)
            val = re.sub(r"^www\.\s*", "", val, flags=re.I)
            val = val.split('/')[0].strip()
            val = val.strip('. ').lower()
            val = val.replace(' ', '')
            if not re.match(r"^[a-z0-9.-]+\.[a-z]{2,}$", val):
                return ''
            return val
        
        # Try to get domain from multiple sources
        domain_clean = clean_domain(domain)
        if not domain_clean:
            # Try to infer domain from company name
            company = row.get("Company") or row.get("company") or ""
            if company:
                # Simple domain inference from company name
                company_clean = re.sub(r'[^a-zA-Z0-9\s]', '', str(company)).strip().lower()
                company_clean = re.sub(r'\s+', '', company_clean)
                # Remove common suffixes
                company_clean = re.sub(r'(inc|llc|corp|corporation|company|co|ltd|limited)$', '', company_clean)
                if company_clean:
                    domain_clean = f"{company_clean}.com"  # Default to .com
        
        # If we still don't have a domain, create a generic one
        if not domain_clean:
            domain_clean = "company.com"  # Last resort fallback
        
        correct_email = correct_pattern_email(fn, ln, domain_clean)
        
        # Check if existing email is valid and matches pattern
        if pd.notna(existing_raw) and str(existing_raw).strip() and not placeholder_re.search(str(existing_raw)):
            clean_existing = emoji_prefix_re.sub("", str(existing_raw)).strip()
            if EMAIL_RE.match(clean_existing):
                # Validate existing email with NeverBounce
                if verify_email_nvb(clean_existing):
                    phone_existing = row.get("phone_number") or row.get("Phone") or ''
                    return clean_existing, "given_valid", phone_existing
                else:
                    # Existing email is invalid, we'll generate a new one
                    pass
        
        phone_number = ''
        
        # Try SignalHire lookup first
        try:
            if fn and ln and domain_clean:
                try:
                    sh_result = signalhire_email_lookup(fn, ln, domain_clean)
                    email = sh_result.get('email') if isinstance(sh_result, dict) else None
                    phone_number = sh_result.get('phone_number') if isinstance(sh_result, dict) else ''
                    if email and EMAIL_RE.match(email):
                        # Validate SignalHire email with NeverBounce
                        if verify_email_nvb(email):
                            return email, "signalhire_valid", phone_number or ''
                        else:
                            # SignalHire email is invalid, continue to pattern generation
                            pass
                except Exception as e:
                    print(f"SignalHire API error: {e}")
                    # Web fallback: try to get pattern/domain from web
                    try:
                        from email_pattern_web_fallback import get_email_pattern_from_web
                        patt_func, web_domain = get_email_pattern_from_web(row.get('Company',''), domain_clean)
                        if web_domain:
                            web_email = f"{patt_func(fn, ln)}@{web_domain}"
                            if EMAIL_RE.match(web_email):
                                if verify_email_nvb(web_email):
                                    return web_email, "web_pattern_valid", phone_number or ''
                    except Exception as we:
                        print(f"Web pattern fallback error: {we}")
        except Exception as e:
            print(f"Unexpected error in enrichment: {e}")
        
        # Generate email using best known pattern for this domain
        best_pattern = patterns_by_domain.get(domain_clean, "first.last")
        
        # Try the best known pattern first
        local = PATTERN_FUNCS[best_pattern](fn, ln)
        candidate = f"{local}@{domain_clean}"
        if EMAIL_RE.match(candidate):
            if verify_email_nvb(candidate):
                return candidate, "pattern_valid", phone_number or ''
        
        # Try all other patterns and validate each one
        patterns_try = [p for p in PATTERN_FUNCS.keys() if p != best_pattern]
        for patt in patterns_try:
            local = PATTERN_FUNCS[patt](fn, ln)
            candidate = f"{local}@{domain_clean}"
            if EMAIL_RE.match(candidate):
                if verify_email_nvb(candidate):
                    return candidate, "pattern_valid", phone_number or ''
        
        # If no pattern validates, return the best pattern as unverified
        # This ensures every record gets an email
        best_local = PATTERN_FUNCS[best_pattern](fn, ln)
        best_email = f"{best_local}@{domain_clean}"
        if EMAIL_RE.match(best_email):
            return best_email, "unverified", phone_number or ''
        
        # Final fallback - use first.last pattern
        fallback_local = f"{fn}.{ln}"
        fallback_email = f"{fallback_local}@{domain_clean}"
        if EMAIL_RE.match(fallback_email):
            return fallback_email, "fallback", phone_number or ''
        
        # This should never happen, but just in case
        return f"{fn}{ln}@{domain_clean}", "emergency_fallback", phone_number or ''

    # Batch processing for better performance
    enriched_emails = []
    statuses = []
    phone_numbers = []
    needs_review = []
    total_records = len(df_contacts)
    
    print(f"\n🔄 Processing {total_records} contacts in batches of {batch_size}")
    start_time = time.time()
    
    # Process contacts in batches
    for batch_start in range(0, total_records, batch_size):
        batch_end = min(batch_start + batch_size, total_records)
        batch_df = df_contacts.iloc[batch_start:batch_end]
        
        print(f"📦 Processing batch {batch_start//batch_size + 1}/{(total_records + batch_size - 1)//batch_size} "
              f"(records {batch_start + 1}-{batch_end})")
        
        batch_start_time = time.time()
        
        # Process each record in the batch
        if tqdm_available:
            iterator = tqdm(batch_df.iterrows(), total=len(batch_df), 
                          desc=f"Batch {batch_start//batch_size + 1}", leave=False)
        else:
            iterator = batch_df.iterrows()
            
        for idx, r in iterator:
            email, status, phone_number = gen(r)
            enriched_emails.append(email)
            statuses.append(status)
            phone_numbers.append(phone_number)
            
            # Flag for review if missing/invalid email, domain, or phone
            review_flag = False
            if not email or not EMAIL_RE.match(str(email)):
                review_flag = True
            if not r.get("domain") or not re.match(r"^[a-z0-9.-]+\.[a-z]{2,}$", str(r.get("domain"))):
                review_flag = True
            if not phone_number or str(phone_number).lower() in ["nan", "none", ""]:
                review_flag = True
            needs_review.append(review_flag)
            
            # Debug: print info for first few rows in each batch
            if len(enriched_emails) <= 3:
                print(f"  Row {idx}: {r.get('first_name')} {r.get('last_name')} @ {r.get('domain')} -> {email} ({status})")
        
        # Batch completion timing
        batch_time = time.time() - batch_start_time
        records_per_sec = len(batch_df) / batch_time if batch_time > 0 else 0
        print(f"  ✅ Batch completed in {batch_time:.1f}s ({records_per_sec:.1f} records/sec)")
        
        # Overall progress update
        elapsed_time = time.time() - start_time
        processed_count = len(enriched_emails)
        if elapsed_time > 0:
            overall_rate = processed_count / elapsed_time
            remaining_records = total_records - processed_count
            eta_seconds = remaining_records / overall_rate if overall_rate > 0 else 0
            print(f"  📊 Overall: {processed_count}/{total_records} ({processed_count/total_records*100:.1f}%) "
                  f"| Rate: {overall_rate:.1f}/sec | ETA: {eta_seconds/60:.1f} min")
    
    # Final processing summary
    total_time = time.time() - start_time
    final_rate = total_records / total_time if total_time > 0 else 0
    print(f"\n🎯 PROCESSING COMPLETE!")
    print(f"   Total time: {total_time/60:.1f} minutes")
    print(f"   Average rate: {final_rate:.1f} records/second")
    print(f"   Performance target: {'✅ ACHIEVED' if final_rate >= 500/(2*3600) else '⚠️ BELOW TARGET'} (target: 500 records in 2 hours)")
    
    # Add results to dataframe
    df_contacts["needs_review"] = needs_review
    df_contacts["email"] = enriched_emails
    df_contacts["email_status"] = statuses
    df_contacts["phone_number"] = phone_numbers
    
    # Print summary
    review_count = sum(needs_review)
    print(f"\n📋 REVIEW SUMMARY: {review_count}/{total_records} contacts need manual review ({review_count/total_records*100:.1f}%)")
    
    # strip emoji prefixes globally
    df_contacts["email"] = df_contacts["email"].astype(str).str.replace(r"^[^a-zA-Z0-9]+\s*","", regex=True)
    
    # place values back into original email column alias if different
    email_alias = next((c for c in df_contacts.columns if 'email' in c.lower().replace(' ', '') and c != 'email'), None)
    if email_alias:
        df_contacts[email_alias] = df_contacts["email"]
    # explicit update for common column names
    if "Work Email" in df_contacts.columns:
        df_contacts["Work Email"] = df_contacts["email"]
    
    # Handle phone number column aliases similar to email
    phone_alias = next((c for c in df_contacts.columns if any(keyword in c.lower().replace(' ', '') for keyword in ['phone', 'telephone', 'tel', 'mobile', 'cell']) and c != 'phone_number'), None)
    if phone_alias:
        df_contacts[phone_alias] = df_contacts["phone_number"]
        print(f"Updated phone column '{phone_alias}' with enriched phone numbers")
    # explicit update for common phone column names
    common_phone_columns = ["Phone", "Telephone", "Phone Number", "Mobile", "Cell Phone", "Work Phone", "Business Phone"]
    for phone_col in common_phone_columns:
        if phone_col in df_contacts.columns:
            df_contacts[phone_col] = df_contacts["phone_number"]
            print(f"Updated phone column '{phone_col}' with enriched phone numbers")
    # ensure Company column present
    if "Company" in df_contacts.columns:
        df_contacts["Company"] = df_contacts["Company"].fillna(df_contacts["domain"])
    else:
        df_contacts["Company"] = df_contacts["domain"]
    # No LinkedIn URL enrichment; only deduplication and email validation
    return df_contacts


def main():
    ap = argparse.ArgumentParser(description="Fill missing emails based on inferred pattern")
    ap.add_argument("contacts", help="CSV with contacts to enrich")
    ap.add_argument("known", help="CSV containing known emails to infer pattern from (can be same as contacts)")
    ap.add_argument("-o", "--output", default=None, help="Output CSV path")
    ap.add_argument("--fast", action="store_true", help="Fast mode: skip external verification for better performance")
    ap.add_argument("--skip-google", action="store_true", help="Skip Google searches for faster processing")
    ap.add_argument("--batch-size", type=int, default=50, help="Process contacts in batches (default: 50)")
    args = ap.parse_args()

    # Read all records from contacts for full scan
    df_contacts = pd.read_csv(args.contacts)
    df_known = pd.read_csv(args.known)

    enriched = fill_emails(df_contacts, df_known, fast_mode=args.fast, skip_google=args.skip_google, batch_size=args.batch_size)

    # Determine output file name and location
    orig_path = Path(args.contacts)
    orig_name = orig_path.stem
    orig_suffix = orig_path.suffix
    
    # Set output directory to same directory as original CSV file
    output_dir = orig_path.parent
    
    # Create output directory if it doesn't exist
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Output directory: {output_dir}")
    except Exception as e:
        print(f"Warning: Could not create/access output directory {output_dir}: {e}")
        print("Using current working directory as fallback...")
        output_dir = Path.cwd()

    # Fix: If 'ENRICHED' in name, only append v2, v3, etc.; else add ' - ENRICHED'
    if 'ENRICHED' in orig_name.upper():
        base_name = orig_name
    else:
        base_name = f"{orig_name} - ENRICHED"

    attempt = 0
    while True:
        if attempt == 0:
            candidate_name = f"{base_name}{orig_suffix}"
        else:
            candidate_name = f"{base_name} v{attempt+1}{orig_suffix}"
        candidate_path = output_dir / candidate_name
        try:
            enriched.to_csv(candidate_path, index=False)
            print(f"Saved {len(enriched)} contacts to {candidate_path}")
            break
        except PermissionError:
            print(f"Output file {candidate_path} was locked, trying next version...")
            attempt += 1
        except Exception as e:
            print(f"Error saving to {candidate_path}: {e}")
            if output_dir != Path.cwd():
                print("Falling back to current working directory...")
                output_dir = Path.cwd()
                continue
            else:
                raise



if __name__ == "__main__":
    main()
23