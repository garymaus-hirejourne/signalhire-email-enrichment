#!/usr/bin/env python
"""
Streamlined Email Pattern Filler
Focuses on domain format checking and email generation for missing records only.
Shows real-time progress for each record being processed.

Usage:
    python streamlined_email_filler.py "path/to/input.csv"
"""

import pandas as pd
import requests
import re
import os
import time
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

# Email pattern functions
PATTERN_FUNCS = {
    "first.last": lambda f, l: f"{f.lower()}.{l.lower()}",
    "firstlast": lambda f, l: f"{f.lower()}{l.lower()}",
    "first_last": lambda f, l: f"{f.lower()}_{l.lower()}",
    "f.last": lambda f, l: f"{f[0].lower()}.{l.lower()}",
    "firstl": lambda f, l: f"{f.lower()}{l[0].lower()}",
    "first": lambda f, l: f.lower(),
}

# Known email patterns for specific domains
KNOWN_PATTERNS = {
    "bureauveritas.com": "first.last",
    "us.bureauveritas.com": "first.last", 
    "ul.com": "first.last",
    "dnvgl.com": "first.last",
    "tuvsud.com": "first.last",
    "mistrasgroup.com": "first.last",
    "octosafety.com": "first.last",
    "turnerxray.com": "first.last",
    "passy-muir.com": "first.last",
    "sis-usa.com": "first.last",
    "kestramedical.com": "first.last",
    "medtechgrowthpartners.com": "first.last",
    "amanngirrbach.us": "first.last",
    "endogastricsolutions.com": "first.last",
    "tritoneinc.net": "first.last",
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

def verify_email_neverbounce(email: str) -> bool:
    """Verify email using NeverBounce API"""
    if not email or not NB_API_KEY:
        return False
    
    try:
        params = {
            "key": NB_API_KEY,
            "email": email
        }
        
        response = requests.get(NB_ENDPOINT, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            result = data.get("result", "").lower()
            return result in ["valid", "catchall"]
        
    except Exception as e:
        print(f"    [WARNING] Email verification error: {str(e)[:50]}...")
    
    return False

def get_domain_pattern(domain: str) -> str:
    """Get the best email pattern for a domain"""
    domain = domain.lower().strip()
    
    # Check known patterns first
    if domain in KNOWN_PATTERNS:
        return KNOWN_PATTERNS[domain]
    
    # Try Hunter.io if API key available
    if HUNTER_API_KEY:
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
                        "{first}": "first"
                    }
                    return hunter_to_pattern.get(pattern, "first.last")
        except Exception as e:
            print(f"    ⚠️  Hunter.io API error: {e}")
    
    # Default to most common pattern
    return "first.last"

def generate_email(first_name: str, last_name: str, domain: str) -> Optional[str]:
    """Generate email address using best pattern for domain"""
    # Clean names
    first = clean_name(first_name)
    last = clean_name(last_name)
    
    if not first or not last or not domain:
        return None
    
    # Get pattern for domain
    pattern = get_domain_pattern(domain)
    
    # Generate email using pattern
    if pattern in PATTERN_FUNCS:
        local_part = PATTERN_FUNCS[pattern](first, last)
        email = f"{local_part}@{domain}"
        return email
    
    return None

def process_csv_file(input_file: str):
    """Process CSV file and generate missing emails with real-time progress"""
    print(f"\n[START] Starting email generation for: {input_file}")
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
    required_cols = ['first_name', 'last_name', 'domain', 'email']
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
        
        # Generate email
        email = generate_email(row['first_name'], row['last_name'], row['domain'])
        
        if email:
            print(f"   Generated: {email}")
            df.at[idx, 'email'] = email
            generated_count += 1
            
            # Skip verification for now to avoid timeouts
            print(f"   [SUCCESS] Email added to record")
        else:
            print(f"   [FAILED] Could not generate email")
        
        # Small delay to make progress visible
        time.sleep(0.1)
    
    print("\n" + "=" * 80)
    print(f"[SUMMARY]")
    print(f"   Emails generated: {generated_count}")
    print(f"   Emails verified: {verified_count}")
    print(f"   Emails unverified: {generated_count - verified_count}")
    
    # Save updated file
    output_file = input_file.replace('.csv', ' - ENRICHED.csv')
    
    # Restore original column names
    reverse_mapping = {v: k for k, v in column_mapping.items()}
    df = df.rename(columns=reverse_mapping)
    
    try:
        df.to_csv(output_file, index=False)
        print(f"[SAVED] Saved to: {output_file}")
        print("[SUCCESS] Processing complete!")
    except Exception as e:
        print(f"[ERROR] Error saving file: {e}")

def main():
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python streamlined_email_filler.py \"path/to/input.csv\"")
        return
    
    input_file = sys.argv[1]
    
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        return
    
    process_csv_file(input_file)

if __name__ == "__main__":
    main()
