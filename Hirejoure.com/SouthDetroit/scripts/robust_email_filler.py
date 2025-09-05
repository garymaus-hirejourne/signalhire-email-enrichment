#!/usr/bin/env python
"""
Robust Email Pattern Filler with Multiple Verification Methods
Handles API timeouts, uses multiple pattern detection sources, and includes manual pattern research.

Usage:
    python robust_email_filler.py "path/to/input.csv"
"""

import pandas as pd
import requests
import re
import os
import time
import urllib.parse
from pathlib import Path
from typing import Optional, Dict, List
import random

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
_verification_cache = {}

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

# Expanded known patterns database (based on manual research and your memories)
KNOWN_PATTERNS = {
    # From your Bureau Veritas research
    "bureauveritas.com": "first.last",
    "us.bureauveritas.com": "first.last",
    
    # From your UL research  
    "ul.com": "first.last",
    
    # Other verified patterns
    "dnvgl.com": "first.last",
    "tuvsud.com": "first.last",
    "mistrasgroup.com": "first.last",
    
    # Medical device companies (common patterns)
    "medtronic.com": "first.last",
    "abbottlabs.com": "first.last",
    "jnj.com": "f.last",
    "ge.com": "firstlast",
    "siemens.com": "first.last",
    "philips.com": "first.last",
    
    # Common tech/business patterns
    "gmail.com": "firstlast",
    "outlook.com": "firstlast", 
    "yahoo.com": "firstlast",
    
    # Medical specific domains that commonly use first.last
    "stryker.com": "first.last",
    "zimmer.com": "first.last",
    "boston-scientific.com": "first.last",
    "edwards.com": "first.last",
}

# Domain-specific pattern hints based on company size and industry
INDUSTRY_PATTERN_HINTS = {
    "medical": ["first.last", "f.last", "firstlast"],
    "technology": ["first.last", "firstlast", "f.last"],
    "consulting": ["first.last", "f.last"],
    "manufacturing": ["first.last", "firstlast"],
    "pharmaceutical": ["first.last", "f.last"],
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

def get_industry_from_company(company_name: str) -> str:
    """Determine industry from company name for pattern hints"""
    company_lower = company_name.lower()
    
    medical_keywords = ["medical", "health", "pharma", "bio", "surgical", "dental", "device", "diagnostic"]
    tech_keywords = ["tech", "software", "digital", "systems", "solutions", "data"]
    consulting_keywords = ["consulting", "advisory", "partners", "group"]
    manufacturing_keywords = ["manufacturing", "industrial", "corp", "inc", "company"]
    
    if any(keyword in company_lower for keyword in medical_keywords):
        return "medical"
    elif any(keyword in company_lower for keyword in tech_keywords):
        return "technology"
    elif any(keyword in company_lower for keyword in consulting_keywords):
        return "consulting"
    elif any(keyword in company_lower for keyword in manufacturing_keywords):
        return "manufacturing"
    
    return "general"

def verify_email_with_retry(email: str, max_retries: int = 3) -> bool:
    """Verify email with retry logic and timeout handling"""
    if not email or not NB_API_KEY:
        return False
    
    # Check cache first
    if email in _verification_cache:
        return _verification_cache[email]
    
    for attempt in range(max_retries):
        try:
            params = {
                "key": NB_API_KEY,
                "email": email
            }
            
            # Shorter timeout with retry
            timeout = 3 + attempt  # Increase timeout with each retry
            response = requests.get(NB_ENDPOINT, params=params, timeout=timeout)
            
            if response.status_code == 200:
                data = response.json()
                result = data.get("result", "").lower()
                is_valid = result in ["valid", "catchall"]
                _verification_cache[email] = is_valid
                return is_valid
            elif response.status_code == 429:  # Rate limited
                print(f"   [RATE_LIMIT] Waiting {2 ** attempt} seconds...")
                time.sleep(2 ** attempt)
                continue
                
        except requests.exceptions.Timeout:
            print(f"   [TIMEOUT] Attempt {attempt + 1}/{max_retries} timed out")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
        except Exception as e:
            print(f"   [ERROR] Verification error: {str(e)[:30]}...")
            break
    
    # Cache negative result to avoid repeated failures
    _verification_cache[email] = False
    return False

def search_google_enhanced(company_name: str, domain: str) -> Optional[str]:
    """Enhanced Google search with multiple query strategies"""
    print(f"   [SEARCH] Enhanced lookup for {domain}...")
    
    try:
        # More specific search queries
        queries = [
            f'"{domain}" email format "firstname.lastname"',
            f'"{domain}" contact "@{domain}" email',
            f'site:{domain} contact email "john.doe"',
            f'"{company_name}" employee email format',
            f'"{domain}" email pattern employees directory',
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        pattern_scores = {}
        
        for query in queries:
            try:
                search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
                resp = requests.get(search_url, headers=headers, timeout=8)
                
                if resp.status_code == 200:
                    content = resp.text.lower()
                    
                    # Enhanced pattern detection with scoring
                    pattern_indicators = {
                        'first.last': [
                            'firstname.lastname', 'first.last', 'john.doe', 'jane.smith',
                            'name.surname', 'givenname.familyname', 'first name.last name',
                            '@' + domain + '">' + r'[a-z]+\.[a-z]+@'
                        ],
                        'f.last': [
                            'j.doe', 'first initial', 'f.lastname', 'initial.surname',
                            'firstletter.last', r'[a-z]\.[a-z]+@' + domain
                        ],
                        'firstlast': [
                            'johndoe@', 'firstlast@', 'no dot', 'no period',
                            'together@', 'concatenated@', r'[a-z]+[a-z]+@' + domain
                        ],
                        'first_last': [
                            'first_last', 'firstname_lastname', 'john_doe', 'underscore',
                            r'[a-z]+_[a-z]+@' + domain
                        ],
                        'firstl': [
                            'johns@', 'firstl@', 'first letter last', 'abbreviated',
                            r'[a-z]+[a-z]@' + domain
                        ]
                    }
                    
                    # Score patterns based on indicators found
                    for pattern, indicators in pattern_indicators.items():
                        score = 0
                        for indicator in indicators:
                            if indicator.startswith(r'[') and indicator.endswith(']'):
                                # Regex pattern
                                if re.search(indicator, content):
                                    score += 3  # Higher weight for regex matches
                            elif indicator in content:
                                score += 1
                        
                        if score > 0:
                            pattern_scores[pattern] = pattern_scores.get(pattern, 0) + score
                
                # Small delay between searches
                time.sleep(random.uniform(0.5, 1.5))
                
            except Exception as e:
                print(f"   [WARNING] Search query failed: {str(e)[:30]}...")
                continue
        
        if pattern_scores:
            best_pattern = max(pattern_scores, key=pattern_scores.get)
            confidence = pattern_scores[best_pattern]
            print(f"   [FOUND] Google suggests '{best_pattern}' for {domain} (confidence: {confidence})")
            return best_pattern
        
        return None
        
    except Exception as e:
        print(f"   [ERROR] Enhanced search error: {str(e)[:50]}...")
        return None

def get_verified_email_pattern(company_name: str, domain: str) -> str:
    """Get verified email pattern using multiple methods with fallbacks"""
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
    
    # Try Hunter.io API with timeout handling
    if HUNTER_API_KEY:
        try:
            params = {
                'domain': domain,
                'api_key': HUNTER_API_KEY,
                'limit': 1
            }
            
            response = requests.get("https://api.hunter.io/v2/domain-search", params=params, timeout=5)
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
                    _pattern_cache[domain] = detected_pattern
                    return detected_pattern
                    
        except Exception as e:
            print(f"   [WARNING] Hunter.io timeout: {str(e)[:30]}...")
    
    # Try enhanced Google search
    google_pattern = search_google_enhanced(company_name, domain)
    if google_pattern:
        _pattern_cache[domain] = google_pattern
        return google_pattern
    
    # Use industry-based hints
    industry = get_industry_from_company(company_name)
    if industry in INDUSTRY_PATTERN_HINTS:
        suggested_pattern = INDUSTRY_PATTERN_HINTS[industry][0]  # Use most common for industry
        print(f"   [INDUSTRY] Using {industry} industry pattern '{suggested_pattern}' for {domain}")
        _pattern_cache[domain] = suggested_pattern
        return suggested_pattern
    
    # Default fallback
    default_pattern = "first.last"
    print(f"   [DEFAULT] Using default pattern '{default_pattern}' for {domain}")
    _pattern_cache[domain] = default_pattern
    return default_pattern

def generate_email_with_pattern(first_name: str, last_name: str, domain: str, pattern: str) -> Optional[str]:
    """Generate email address using specific pattern"""
    # Clean names
    first = clean_name(first_name)
    last = clean_name(last_name)
    
    if not first or not domain:
        return None
    
    # Handle cases where last name is missing or very short
    if not last or len(last) <= 1:
        if pattern in ["firstl", "f.last", "last.first", "l.first"]:
            pattern = "first"  # Fall back to first name only
        elif pattern == "first.last":
            last = "user"  # Generic fallback
    
    # Generate email using pattern
    if pattern in PATTERN_FUNCS:
        try:
            local_part = PATTERN_FUNCS[pattern](first, last)
            email = f"{local_part}@{domain}"
            return email
        except Exception as e:
            print(f"   [ERROR] Pattern generation failed: {e}")
            return None
    
    return None

def generate_and_verify_email_robust(first_name: str, last_name: str, company_name: str, domain: str) -> tuple[str, bool, str]:
    """Generate email with robust pattern detection and verification"""
    
    # Get the best pattern for this domain
    detected_pattern = get_verified_email_pattern(company_name, domain)
    
    # Try multiple patterns in order of likelihood
    patterns_to_try = [detected_pattern]
    
    # Add alternative patterns based on industry
    industry = get_industry_from_company(company_name)
    if industry in INDUSTRY_PATTERN_HINTS:
        for alt_pattern in INDUSTRY_PATTERN_HINTS[industry]:
            if alt_pattern not in patterns_to_try:
                patterns_to_try.append(alt_pattern)
    
    # Add common fallback patterns
    common_patterns = ["first.last", "f.last", "firstlast", "first_last", "firstl", "first"]
    for pattern in common_patterns:
        if pattern not in patterns_to_try:
            patterns_to_try.append(pattern)
    
    best_email = None
    best_verified = False
    best_pattern = detected_pattern
    
    for pattern in patterns_to_try:
        email = generate_email_with_pattern(first_name, last_name, domain, pattern)
        
        if not email:
            continue
            
        print(f"   Generated: {email} (pattern: {pattern})")
        
        # Verify the generated email
        is_verified = verify_email_with_retry(email)
        
        if is_verified:
            print(f"   [VERIFIED] Email is deliverable!")
            # Update cache with working pattern
            _pattern_cache[domain] = pattern
            return email, True, pattern
        else:
            # Keep the first generated email as fallback
            if best_email is None:
                best_email = email
                best_pattern = pattern
        
        # Small delay between verification attempts
        time.sleep(0.2)
    
    if best_email:
        print(f"   [UNVERIFIED] Using best guess: {best_email}")
        return best_email, False, best_pattern
    
    return None, False, detected_pattern

def process_csv_file(input_file: str):
    """Process CSV file with robust email generation and verification"""
    print(f"\n[START] Robust email generation with enhanced pattern detection")
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
    df['email'] = df['email'].astype(str)
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
    pattern_usage = {}
    
    for idx, row in df.iterrows():
        # Skip if email already exists
        email_val = str(row['email']).strip()
        if email_val and email_val != 'nan' and email_val != '':
            continue
        
        print(f"\n[PROCESSING] Record {idx + 1}/{len(df)}: {row['first_name']} {row['last_name']}")
        try:
            print(f"   Company: {str(row.get('company', 'N/A')).encode('ascii', 'ignore').decode('ascii')}")
        except:
            print(f"   Company: [Unicode company name]")
        print(f"   Domain: {row['domain']}")
        
        # Generate and verify email with robust methods
        email, is_verified, pattern_used = generate_and_verify_email_robust(
            row['first_name'], 
            row['last_name'], 
            row['company'], 
            row['domain']
        )
        
        if email:
            df.at[idx, 'email'] = email
            generated_count += 1
            
            # Track pattern usage
            pattern_usage[pattern_used] = pattern_usage.get(pattern_used, 0) + 1
            
            if is_verified:
                verified_count += 1
                print(f"   [SUCCESS] Verified email added to record")
            else:
                print(f"   [SUCCESS] Unverified email added to record")
        else:
            print(f"   [FAILED] Could not generate email")
        
        # Delay to avoid rate limiting
        time.sleep(random.uniform(0.3, 0.8))
    
    print("\n" + "=" * 80)
    print(f"[SUMMARY]")
    print(f"   Emails generated: {generated_count}")
    print(f"   Emails verified: {verified_count}")
    print(f"   Verification rate: {(verified_count/generated_count*100):.1f}%" if generated_count > 0 else "N/A")
    print(f"   Emails unverified: {generated_count - verified_count}")
    
    # Show pattern usage statistics
    print(f"\n[PATTERN USAGE]")
    for pattern, count in sorted(pattern_usage.items(), key=lambda x: x[1], reverse=True):
        print(f"   {pattern}: {count} emails ({count/generated_count*100:.1f}%)")
    
    # Save updated file
    output_file = input_file.replace('.csv', ' - ROBUST.csv')
    
    # Restore original column names
    reverse_mapping = {v: k for k, v in column_mapping.items()}
    df = df.rename(columns=reverse_mapping)
    
    try:
        df.to_csv(output_file, index=False)
        print(f"\n[SAVED] Saved to: {output_file}")
        print("[SUCCESS] Processing complete!")
        
        # Show final pattern cache
        print(f"\n[PATTERNS] Final detected patterns:")
        for domain, pattern in sorted(_pattern_cache.items()):
            print(f"   {domain}: {pattern}")
            
    except Exception as e:
        print(f"[ERROR] Error saving file: {e}")

def main():
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python robust_email_filler.py \"path/to/input.csv\"")
        return
    
    input_file = sys.argv[1]
    
    if not os.path.exists(input_file):
        print(f"[ERROR] File not found: {input_file}")
        return
    
    process_csv_file(input_file)

if __name__ == "__main__":
    main()
