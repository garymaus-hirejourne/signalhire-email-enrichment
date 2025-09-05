#!/usr/bin/env python
"""
Enhanced Email Pattern Filler with Google-based Domain Format Detection
Detects actual email patterns for each domain using Google search and applies them.

Usage:
    python enhanced_email_filler.py "path/to/input.csv"
"""

import pandas as pd
import requests
import re
import os
import time
import urllib.parse
from pathlib import Path
from typing import Optional, Dict

# Load environment variables for API keys
try:
    from dotenv import load_dotenv
    desktop_env = os.path.join(os.path.expanduser('~'), 'Desktop', 'southdetroit_api_keys.env')
    if os.path.exists(desktop_env):
        load_dotenv(desktop_env)
    else:
        load_dotenv()
except ImportError:
    print("python-dotenv not installed: API keys may not be loaded from .env file.")

# API Configuration
HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
NB_API_KEY = "private_5e99b01595fe03a6276fa25c8ddee66a"  # NeverBounce API key
NB_ENDPOINT = "https://api.neverbounce.com/v4/single/check"

# Cache for verified patterns to avoid repeated lookups
_pattern_cache = {}

# Email pattern functions
PATTERN_FUNCS = {
    "first.last": lambda f, l: f"{f.lower()}.{l.lower()}",
    "firstlast": lambda f, l: f"{f.lower()}{l.lower()}",
    "first_last": lambda f, l: f"{f.lower()}_{l.lower()}",
    "f.last": lambda f, l: f"{f[0].lower()}.{l.lower()}",
    "firstl": lambda f, l: f"{f.lower()}{l[0].lower()}",
    "first": lambda f, l: f.lower(),
    "last.first": lambda f, l: f"{l.lower()}.{f.lower()}",
    "l.first": lambda f, l: f"{l[0].lower()}.{f.lower()}",
}

# Known email patterns for specific domains (from your research)
KNOWN_PATTERNS = {
    "bureauveritas.com": "first.last",
    "us.bureauveritas.com": "first.last", 
    "ul.com": "first.last",
    "dnvgl.com": "first.last",
    "tuvsud.com": "first.last",
    "mistrasgroup.com": "first.last",
}

def clean_name(name):
    """Clean and normalize name for email generation"""
    if pd.isna(name) or not name:
        return ""
    
    name = str(name).strip()
    # Remove common suffixes and prefixes
    name = re.sub(r'\b(jr|sr|ii|iii|iv|phd|md|mba|cpa)\b\.?', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(mr|mrs|ms|dr|prof)\b\.?\s*', '', name, flags=re.IGNORECASE)
    # Remove special characters but keep hyphens and apostrophes
    name = re.sub(r'[^\w\s\-\']', '', name)
    # Remove extra whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

def search_google_for_email_pattern(company_name: str, domain: str) -> Optional[str]:
    """Search Google for actual company email format information"""
    print(f"   [SEARCH] Looking up email pattern for {domain}...")
    
    try:
        # Multiple search queries to find email patterns
        queries = [
            f'"{domain}" email format employees contact',
            f'"{company_name}" email address format',
            f'site:{domain} email contact',
            f'"{domain}" "@{domain}" email pattern'
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        for query in queries:
            try:
                search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
                resp = requests.get(search_url, headers=headers, timeout=10)
                
                if resp.status_code == 200:
                    content = resp.text.lower()
                    
                    # Look for email pattern indicators in search results
                    pattern_indicators = {
                        'first.last': [
                            'firstname.lastname', 'first.last', 'john.doe', 'jane.smith',
                            'name.surname', 'givenname.familyname'
                        ],
                        'f.last': [
                            'j.doe', 'first initial', 'f.lastname', 'initial.surname',
                            'firstletter.last'
                        ],
                        'firstlast': [
                            'johndoe', 'firstlast', 'firstname lastname', 'no dot',
                            'together', 'concatenated'
                        ],
                        'first_last': [
                            'first_last', 'firstname_lastname', 'john_doe', 'underscore'
                        ],
                        'firstl': [
                            'johns', 'firstl', 'first letter last', 'abbreviated'
                        ],
                        'first': [
                            'first name only', 'firstname@', 'just first'
                        ]
                    }
                    
                    # Score patterns based on indicators found
                    pattern_scores = {}
                    for pattern, indicators in pattern_indicators.items():
                        score = sum(1 for indicator in indicators if indicator in content)
                        if score > 0:
                            pattern_scores[pattern] = score
                    
                    if pattern_scores:
                        best_pattern = max(pattern_scores, key=pattern_scores.get)
                        print(f"   [FOUND] Google suggests {domain} uses '{best_pattern}' pattern (score: {pattern_scores[best_pattern]})")
                        return best_pattern
                
                # Small delay between searches
                time.sleep(1)
                
            except Exception as e:
                print(f"   [WARNING] Search query failed: {str(e)[:30]}...")
                continue
        
        return None
        
    except Exception as e:
        print(f"   [ERROR] Google search error for {domain}: {str(e)[:50]}...")
        return None

def get_hunter_email_pattern(domain: str) -> Optional[str]:
    """Get verified email pattern from Hunter.io API"""
    if not HUNTER_API_KEY:
        return None
    
    try:
        params = {
            'domain': domain,
            'api_key': HUNTER_API_KEY,
            'limit': 1
        }
        
        response = requests.get("https://api.hunter.io/v2/domain-search", params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            pattern = data.get("data", {}).get("pattern")
            if pattern:
                # Convert Hunter.io pattern to our format
                hunter_to_pattern = {
                    "{first}.{last}": "first.last",
                    "{first}{last}": "firstlast", 
                    "{first}_{last}": "first_last",
                    "{f}.{last}": "f.last",
                    "{first}{l}": "firstl",
                    "{first}": "first",
                    "{last}.{first}": "last.first",
                    "{l}.{first}": "l.first"
                }
                detected_pattern = hunter_to_pattern.get(pattern, "first.last")
                print(f"   [HUNTER] Found pattern '{detected_pattern}' for {domain}")
                return detected_pattern
                
    except Exception as e:
        print(f"   [WARNING] Hunter.io API error: {str(e)[:50]}...")
    
    return None

def get_verified_email_pattern(company_name: str, domain: str) -> str:
    """Get verified email pattern using multiple methods"""
    domain = domain.lower().strip()
    
    # Check cache first
    if domain in _pattern_cache:
        print(f"   [CACHE] Using cached pattern '{_pattern_cache[domain]}' for {domain}")
        return _pattern_cache[domain]
    
    # Check known patterns first
    if domain in KNOWN_PATTERNS:
        pattern = KNOWN_PATTERNS[domain]
        print(f"   [KNOWN] Using known pattern '{pattern}' for {domain}")
        _pattern_cache[domain] = pattern
        return pattern
    
    # Try Hunter.io API
    hunter_pattern = get_hunter_email_pattern(domain)
    if hunter_pattern:
        _pattern_cache[domain] = hunter_pattern
        return hunter_pattern
    
    # Try Google search
    google_pattern = search_google_for_email_pattern(company_name, domain)
    if google_pattern:
        _pattern_cache[domain] = google_pattern
        return google_pattern
    
    # Default fallback
    default_pattern = "first.last"
    print(f"   [DEFAULT] Using default pattern '{default_pattern}' for {domain}")
    _pattern_cache[domain] = default_pattern
    return default_pattern

def verify_email_neverbounce(email: str) -> bool:
    """Verify email using NeverBounce API"""
    if not email or not NB_API_KEY:
        return False
    
    try:
        params = {
            "key": NB_API_KEY,
            "email": email
        }
        
        response = requests.get(NB_ENDPOINT, params=params, timeout=8)
        if response.status_code == 200:
            data = response.json()
            result = data.get("result", "").lower()
            return result in ["valid", "catchall"]
        
    except Exception as e:
        print(f"   [WARNING] Email verification timeout: {str(e)[:30]}...")
    
    return False

def generate_email_with_pattern(first_name: str, last_name: str, domain: str, pattern: str) -> Optional[str]:
    """Generate email address using specific pattern"""
    # Clean names
    first = clean_name(first_name)
    last = clean_name(last_name)
    
    if not first or not last or not domain:
        return None
    
    # Handle special cases for single letter last names
    if len(last) == 1 and pattern in ["firstl", "f.last"]:
        pattern = "first"  # Fall back to first name only
    
    # Generate email using pattern
    if pattern in PATTERN_FUNCS:
        local_part = PATTERN_FUNCS[pattern](first, last)
        email = f"{local_part}@{domain}"
        return email
    
    return None

def generate_and_verify_email(first_name: str, last_name: str, company_name: str, domain: str) -> tuple[str, bool]:
    """Generate email with domain-specific pattern and verify it"""
    
    # Get the best pattern for this domain
    pattern = get_verified_email_pattern(company_name, domain)
    
    # Generate email using detected pattern
    email = generate_email_with_pattern(first_name, last_name, domain, pattern)
    
    if not email:
        return None, False
    
    print(f"   Generated: {email}")
    
    # Verify the generated email
    is_valid = verify_email_neverbounce(email)
    
    if is_valid:
        print(f"   [VERIFIED] Email is deliverable!")
        return email, True
    else:
        # If primary pattern fails, try alternative patterns
        alternative_patterns = ["first.last", "f.last", "firstlast", "first_last", "firstl"]
        
        for alt_pattern in alternative_patterns:
            if alt_pattern == pattern:  # Skip the one we already tried
                continue
                
            alt_email = generate_email_with_pattern(first_name, last_name, domain, alt_pattern)
            if alt_email and verify_email_neverbounce(alt_email):
                print(f"   [ALTERNATIVE] Found working pattern '{alt_pattern}': {alt_email}")
                # Update cache with working pattern
                _pattern_cache[domain] = alt_pattern
                return alt_email, True
        
        print(f"   [UNVERIFIED] Using generated email anyway: {email}")
        return email, False

def process_csv_file(input_file: str):
    """Process CSV file and generate missing emails with domain-specific patterns"""
    print(f"\n[START] Enhanced email generation with domain pattern detection")
    print(f"File: {input_file}")
    print("=" * 80)
    
    # Read CSV file
    try:
        df = pd.read_csv(input_file)
        print(f"[INFO] Loaded {len(df)} records")
    except Exception as e:
        print(f"[ERROR] Error reading CSV file: {e}")
        return
    
    # Standardize column names
    column_mapping = {
        'First': 'first_name',
        'Last': 'last_name', 
        'Company': 'company',
        'Domain': 'domain',
        'Email': 'email'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Ensure required columns exist
    required_cols = ['first_name', 'last_name', 'company', 'domain', 'email']
    for col in required_cols:
        if col not in df.columns:
            df[col] = ''
    
    # Count records needing emails
    df['email'] = df['email'].astype(str)  # Convert to string to handle NaN values
    missing_emails = df['email'].isna() | (df['email'] == '') | (df['email'] == 'nan') | (df['email'].str.strip() == '')
    records_to_process = missing_emails.sum()
    
    print(f"[INFO] Records needing emails: {records_to_process}")
    print(f"[INFO] Records with existing emails: {len(df) - records_to_process}")
    print("\n" + "=" * 80)
    
    if records_to_process == 0:
        print("[SUCCESS] All records already have email addresses!")
        return
    
    # Process each record
    generated_count = 0
    verified_count = 0
    
    for idx, row in df.iterrows():
        # Skip if email already exists
        email_val = str(row['email']).strip()
        if email_val and email_val != 'nan' and email_val != '':
            continue
        
        print(f"\n[PROCESSING] Record {idx + 1}/{len(df)}: {row['first_name']} {row['last_name']}")
        print(f"   Company: {row.get('company', 'N/A')}")
        print(f"   Domain: {row['domain']}")
        
        # Generate and verify email with domain-specific pattern
        email, is_verified = generate_and_verify_email(
            row['first_name'], 
            row['last_name'], 
            row['company'], 
            row['domain']
        )
        
        if email:
            df.at[idx, 'email'] = email
            generated_count += 1
            
            if is_verified:
                verified_count += 1
                print(f"   [SUCCESS] Verified email added to record")
            else:
                print(f"   [SUCCESS] Unverified email added to record")
        else:
            print(f"   [FAILED] Could not generate email")
        
        # Small delay to make progress visible and avoid rate limiting
        time.sleep(0.5)
    
    print("\n" + "=" * 80)
    print(f"[SUMMARY]")
    print(f"   Emails generated: {generated_count}")
    print(f"   Emails verified: {verified_count}")
    print(f"   Verification rate: {(verified_count/generated_count*100):.1f}%" if generated_count > 0 else "N/A")
    print(f"   Emails unverified: {generated_count - verified_count}")
    
    # Save updated file
    output_file = input_file.replace('.csv', ' - ENHANCED.csv')
    
    # Restore original column names
    reverse_mapping = {v: k for k, v in column_mapping.items()}
    df = df.rename(columns=reverse_mapping)
    
    try:
        df.to_csv(output_file, index=False)
        print(f"[SAVED] Saved to: {output_file}")
        print("[SUCCESS] Processing complete!")
        
        # Show pattern cache summary
        print(f"\n[PATTERNS] Detected patterns for domains:")
        for domain, pattern in _pattern_cache.items():
            print(f"   {domain}: {pattern}")
            
    except Exception as e:
        print(f"[ERROR] Error saving file: {e}")

def main():
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python enhanced_email_filler.py \"path/to/input.csv\"")
        return
    
    input_file = sys.argv[1]
    
    if not os.path.exists(input_file):
        print(f"[ERROR] File not found: {input_file}")
        return
    
    process_csv_file(input_file)

if __name__ == "__main__":
    main()
