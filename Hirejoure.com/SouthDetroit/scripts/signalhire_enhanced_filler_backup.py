#!/usr/bin/env python
"""
SignalHire Enhanced Email Filler
Combines SignalHire direct contact lookup with robust pattern detection for maximum accuracy.

Usage:
    python signalhire_enhanced_filler.py "path/to/input.csv"
"""

import pandas as pd
import requests
import re
import os
import time
import urllib.parse
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import random
import phonenumbers
from phonenumbers import geocoder, carrier

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
SIGNALHIRE_API_KEY = os.environ.get("SIGNALHIRE_API_KEY", "202.evaAyOWjUoheYEQ4Bb2XlSp0ZSzi")
SIGNALHIRE_ENDPOINT = "https://www.signalhire.com/api/v1/candidate/search"
HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
NB_API_KEY = "private_5e99b01595fe03a6276fa25c8ddee66a"  # NeverBounce API key
NB_ENDPOINT = "https://api.neverbounce.com/v4/single/check"

# Cache for verified patterns, contacts, phone numbers, and CRM data
_pattern_cache = {}
_verification_cache = {}
_signalhire_cache = {}
_phone_cache = {}
_crm_cache = {}

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

# Expanded known patterns database
KNOWN_PATTERNS = {
    # From your Bureau Veritas research
    "bureauveritas.com": "first.last",
    "us.bureauveritas.com": "first.last",
    
    # From your UL research  
    "ul.com": "first.last",
    
    # From Google AI Overview research
    "turnerxray.com": "first",  # Uses name@turnerxray.com format (first name only)
    
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
    
    # Electronics/Technology companies
    "intel.com": "first.last",
    "amd.com": "first.last",
    "nvidia.com": "first.last",
    "apple.com": "first.last",
    "microsoft.com": "first.last",
    
    # Common business patterns by domain type
    "gmail.com": "firstlast",
    "yahoo.com": "firstlast",
    "hotmail.com": "firstlast",
    "outlook.com": "first.last",
    
    # Government and education patterns
    "gov": "first.last",
    "edu": "first.last",
    "ac.uk": "first.last",
    "org": "first.last",
}

# Domain-specific pattern hints based on company size and industry
INDUSTRY_PATTERN_HINTS = {
    "medical": ["first.last", "f.last", "firstlast"],
    "technology": ["first.last", "firstlast", "f.last"],
    "consulting": ["first.last", "f.last"],
    "manufacturing": ["first.last", "firstlast"],
    "pharmaceutical": ["first.last", "f.last"],
    "electronics": ["first.last", "f.last", "firstlast"],
}

def clean_name(name):
    """Clean and normalize name for email generation - removes initials, degrees, suffixes"""
    if pd.isna(name) or not name:
        return ""
    
    name = str(name).strip()
    
    # Remove common degrees and certifications
    name = re.sub(r'\b(phd|md|mba|cpa|rn|bsn|msn|dds|dvm|jd|esq|cfa|pe|pmp)\b\.?', '', name, flags=re.IGNORECASE)
    
    # Remove common suffixes
    name = re.sub(r'\b(jr|sr|ii|iii|iv|v|vi)\b\.?', '', name, flags=re.IGNORECASE)
    
    # Remove common prefixes
    name = re.sub(r'\b(mr|mrs|ms|dr|prof|rev)\b\.?\s*', '', name, flags=re.IGNORECASE)
    
    # Remove single letter initials with periods (like "J." or "R.")
    name = re.sub(r'\b[A-Z]\.\s*', '', name)
    
    # Remove standalone single letters at word boundaries
    name = re.sub(r'\b[A-Z]\b\.?\s*', '', name)
    
    # Remove special characters but keep hyphens and apostrophes
    name = re.sub(r'[^\w\s\-\']', '', name)
    
    # Remove extra whitespace and clean up
    name = re.sub(r'\s+', ' ', name).strip()
    
    # Split into words and take only the first meaningful word
    words = [word for word in name.split() if len(word) > 1]  # Only words longer than 1 char
    
    if words:
        return words[0].lower()  # Return first meaningful word in lowercase
    
    return ""

def get_industry_from_company(company_name: str) -> str:
    """Determine industry from company name for pattern hints"""
    company_lower = company_name.lower()
    
    medical_keywords = ["medical", "health", "pharma", "bio", "surgical", "dental", "device", "diagnostic"]
    tech_keywords = ["tech", "software", "digital", "systems", "solutions", "data", "electronics"]
    consulting_keywords = ["consulting", "advisory", "partners", "group"]
    manufacturing_keywords = ["manufacturing", "industrial", "corp", "inc", "company"]
    electronics_keywords = ["electronics", "electronic", "electrical", "circuit", "semiconductor"]
    
    if any(keyword in company_lower for keyword in electronics_keywords):
        return "electronics"
    elif any(keyword in company_lower for keyword in medical_keywords):
        return "medical"
    elif any(keyword in company_lower for keyword in tech_keywords):
        return "technology"
    elif any(keyword in company_lower for keyword in consulting_keywords):
        return "consulting"
    elif any(keyword in company_lower for keyword in manufacturing_keywords):
        return "manufacturing"
    
    return "general"

def signalhire_email_lookup(linkedin_url: str, first_name: str = "", last_name: str = "") -> Dict:
    """Look up email, phone, and social links using SignalHire API v1 with LinkedIn URL"""
    if not linkedin_url or linkedin_url.strip() == "":
        print(f"   [SIGNALHIRE_SKIP] No LinkedIn URL provided")
        return {}
    
    # Create cache key based on LinkedIn URL
    cache_key = linkedin_url.lower().strip()
    
    # Check cache first
    if cache_key in _signalhire_cache:
        print(f"   [SIGNALHIRE_CACHE] Using cached result for LinkedIn profile")
        return _signalhire_cache[cache_key]
    
    # SignalHire v1 API with LinkedIn URL
    headers = {
        'apikey': SIGNALHIRE_API_KEY,
        'Content-Type': 'application/json'
    }
    
    payload = {
        'items': [linkedin_url.strip()],
        'callbackUrl': 'https://httpbin.org/post'  # Dummy callback for testing
    }
    
    try:
        print(f"   [SIGNALHIRE] Looking up LinkedIn profile: {linkedin_url[:50]}...")
        
        # Make POST request to SignalHire v1 API
        response = requests.post(SIGNALHIRE_ENDPOINT, 
                               headers=headers, 
                               json=payload, 
                               timeout=15)
        
        print(f"   [SIGNALHIRE] Response status: {response.status_code}")
        
        if response.status_code == 201:  # Accepted - request queued
            data = response.json()
            request_id = data.get('requestId')
            print(f"   [SIGNALHIRE] Request accepted, ID: {request_id}")
            
            # For now, we can't wait for the callback in a synchronous workflow
            # The new API is designed for async processing
            print(f"   [SIGNALHIRE_ASYNC] API is callback-based - cannot get immediate results")
            result = {}
            
        elif response.status_code == 401:
            print(f"   [SIGNALHIRE_AUTH] Authentication failed - check API key")
            result = {}
        elif response.status_code == 402:
            print(f"   [SIGNALHIRE_CREDITS] Out of credits")
            result = {}
        else:
            print(f"   [SIGNALHIRE_ERROR] HTTP {response.status_code}: {response.text[:100]}")
            result = {}
            
    except Exception as e:
        print(f"   [SIGNALHIRE_ERROR] API error: {str(e)[:50]}...")
        result = {}
    
    # Cache the result
    _signalhire_cache[cache_key] = result
    return result

def signalhire_email_lookup_v1_async(first_name: str, last_name: str, company_domain: str) -> Dict:
    """
    SignalHire v1 API lookup (callback-based) - Currently disabled
    The new SignalHire API requires:
    1. LinkedIn profile URLs as identifiers
    2. Callback URLs for async responses
    3. POST requests with JSON payload
    
    This is not suitable for our synchronous email generation workflow.
    """
    # Create cache key
    cache_key = f"{first_name.lower()}_{last_name.lower()}_{company_domain.lower()}"
    
    # Check cache first
    if cache_key in _signalhire_cache:
        return _signalhire_cache[cache_key]
    
    # For now, we'll disable SignalHire v1 API integration
    # as it requires LinkedIn URLs which we don't have in our CSV data
    print(f"   [SIGNALHIRE_V1] API requires LinkedIn URLs - not compatible with name+domain lookup")
    
    result = {}
    _signalhire_cache[cache_key] = result
    return result

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

def get_google_email_pattern(domain: str, company_name: str = "") -> Optional[str]:
    """Search Google for actual email patterns used by the domain"""
    try:
        # Multiple search strategies
        search_queries = [
            f'site:{domain} email contact "@{domain}"',
            f'"{domain}" email format employees contact',
            f'site:{domain} "@{domain}" -unsubscribe -privacy',
            f'{company_name} email format contact employees' if company_name else None
        ]
        
        for query in search_queries:
            if not query:
                continue
                
            print(f"   [GOOGLE] Searching: {query[:50]}...")
            
            # Use requests to search (basic implementation)
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            try:
                response = requests.get(search_url, headers=headers, timeout=5)
                if response.status_code == 200:
                    content = response.text.lower()
                    
                    # Look for email patterns in the content
                    email_patterns = re.findall(rf'[a-z]+@{re.escape(domain)}', content)
                    
                    if email_patterns:
                        # Analyze the patterns found
                        pattern_analysis = {}
                        for email in email_patterns[:5]:  # Analyze first 5 emails
                            local_part = email.split('@')[0]
                            
                            # Detect pattern type
                            if '.' in local_part and len(local_part.split('.')) == 2:
                                parts = local_part.split('.')
                                if len(parts[0]) > 1 and len(parts[1]) > 1:
                                    pattern_analysis['first.last'] = pattern_analysis.get('first.last', 0) + 1
                                elif len(parts[0]) == 1:
                                    pattern_analysis['f.last'] = pattern_analysis.get('f.last', 0) + 1
                            elif '_' in local_part:
                                pattern_analysis['first_last'] = pattern_analysis.get('first_last', 0) + 1
                            elif len(local_part) > 6:  # Likely firstlast
                                pattern_analysis['firstlast'] = pattern_analysis.get('firstlast', 0) + 1
                            elif len(local_part) <= 6:  # Likely first name only or firstl
                                pattern_analysis['first'] = pattern_analysis.get('first', 0) + 1
                        
                        if pattern_analysis:
                            # Return most common pattern
                            best_pattern = max(pattern_analysis, key=pattern_analysis.get)
                            print(f"   [GOOGLE] Found pattern '{best_pattern}' for {domain} (examples: {email_patterns[:2]})")
                            return best_pattern
                            
            except Exception as e:
                print(f"   [GOOGLE] Search error: {str(e)[:30]}...")
                continue
                
            # Small delay between searches
            time.sleep(0.5)
            
    except Exception as e:
        print(f"   [GOOGLE] General error: {str(e)[:30]}...")
    
    return None

def get_website_email_pattern(domain: str, company_name: str = "") -> Optional[str]:
    """Scrape company website for email patterns"""
    try:
        # Try common contact pages
        contact_pages = [
            f"https://{domain}/contact",
            f"https://{domain}/contact-us",
            f"https://{domain}/about",
            f"https://{domain}/team",
            f"https://{domain}/staff",
            f"https://{domain}/people",
            f"https://{domain}"
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        for url in contact_pages:
            try:
                print(f"   [WEBSITE] Checking: {url}")
                response = requests.get(url, headers=headers, timeout=5)
                
                if response.status_code == 200:
                    content = response.text.lower()
                    
                    # Find email addresses for this domain
                    email_pattern = rf'[a-z][a-z0-9._-]*@{re.escape(domain)}'
                    found_emails = re.findall(email_pattern, content)
                    
                    if found_emails:
                        # Filter out common non-personal emails
                        personal_emails = []
                        skip_patterns = ['info@', 'contact@', 'support@', 'sales@', 'admin@', 'webmaster@', 'noreply@', 'no-reply@']
                        
                        for email in found_emails:
                            if not any(email.startswith(skip) for skip in skip_patterns):
                                personal_emails.append(email)
                        
                        if personal_emails:
                            # Analyze patterns
                            pattern_counts = {}
                            for email in personal_emails[:5]:  # Analyze first 5
                                local_part = email.split('@')[0]
                                
                                if '.' in local_part and len(local_part.split('.')) == 2:
                                    parts = local_part.split('.')
                                    if len(parts[0]) > 1 and len(parts[1]) > 1:
                                        pattern_counts['first.last'] = pattern_counts.get('first.last', 0) + 1
                                    elif len(parts[0]) == 1:
                                        pattern_counts['f.last'] = pattern_counts.get('f.last', 0) + 1
                                elif '_' in local_part:
                                    pattern_counts['first_last'] = pattern_counts.get('first_last', 0) + 1
                                elif len(local_part) > 8:
                                    pattern_counts['firstlast'] = pattern_counts.get('firstlast', 0) + 1
                                else:
                                    pattern_counts['first'] = pattern_counts.get('first', 0) + 1
                            
                            if pattern_counts:
                                best_pattern = max(pattern_counts, key=pattern_counts.get)
                                print(f"   [WEBSITE] Found pattern '{best_pattern}' for {domain} (examples: {personal_emails[:2]})")
                                return best_pattern
                                
            except Exception as e:
                print(f"   [WEBSITE] Error checking {url}: {str(e)[:30]}...")
                continue
                
            time.sleep(0.3)  # Small delay between requests
            
    except Exception as e:
        print(f"   [WEBSITE] General error: {str(e)[:30]}...")
    
    return None

def get_rocketreach_pattern(domain: str) -> Optional[str]:
    """Check RocketReach for email format information"""
    try:
        # Search for the domain on RocketReach email format pages
        search_url = f"https://www.google.com/search?q=site:rocketreach.co+{domain}+email+format"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(search_url, headers=headers, timeout=5)
        if response.status_code == 200:
            content = response.text.lower()
            
            # Look for common RocketReach pattern descriptions
            if 'first_initial' in content and 'last' in content:
                print(f"   [ROCKETREACH] Found f.last pattern for {domain}")
                return "f.last"
            elif 'first.last' in content or 'first][last' in content:
                print(f"   [ROCKETREACH] Found first.last pattern for {domain}")
                return "first.last"
            elif 'firstlast' in content:
                print(f"   [ROCKETREACH] Found firstlast pattern for {domain}")
                return "firstlast"
                
    except Exception as e:
        print(f"   [ROCKETREACH] Error: {str(e)[:30]}...")
    
    return None

def get_clearbit_pattern(domain: str) -> Optional[str]:
    """Check Clearbit Connect for email patterns (if available)"""
    try:
        # Basic pattern inference based on domain characteristics
        domain_lower = domain.lower()
        
        # Technology companies often use first.last
        tech_indicators = ['tech', 'software', 'systems', 'digital', 'data', 'cloud', 'ai', 'ml']
        if any(indicator in domain_lower for indicator in tech_indicators):
            print(f"   [CLEARBIT] Tech domain detected, suggesting first.last for {domain}")
            return "first.last"
        
        # Startups and smaller companies often use first@domain
        startup_indicators = ['app', 'labs', 'studio', 'ventures', 'solutions']
        if any(indicator in domain_lower for indicator in startup_indicators):
            print(f"   [CLEARBIT] Startup domain detected, suggesting first for {domain}")
            return "first"
            
        # Large corporations often use f.last
        corp_indicators = ['corp', 'inc', 'ltd', 'llc', 'company', 'group', 'international']
        if any(indicator in domain_lower for indicator in corp_indicators):
            print(f"   [CLEARBIT] Corporate domain detected, suggesting f.last for {domain}")
            return "f.last"
            
    except Exception as e:
        print(f"   [CLEARBIT] Error: {str(e)[:30]}...")
    
    return None

def get_domain_age_pattern(domain: str) -> Optional[str]:
    """Infer pattern based on domain characteristics and age"""
    try:
        domain_parts = domain.split('.')
        tld = domain_parts[-1] if domain_parts else ""
        domain_name = domain_parts[0] if domain_parts else ""
        
        # Country-specific patterns
        country_patterns = {
            'uk': 'first.last',
            'ca': 'first.last', 
            'au': 'first.last',
            'de': 'first.last',
            'fr': 'first.last',
            'jp': 'f.last',
            'cn': 'firstlast'
        }
        
        if tld in country_patterns:
            pattern = country_patterns[tld]
            print(f"   [DOMAIN_GEO] {tld.upper()} domain suggests '{pattern}' pattern for {domain}")
            return pattern
        
        # Domain length-based inference
        if len(domain_name) <= 5:  # Short domains often use first@domain
            print(f"   [DOMAIN_LENGTH] Short domain suggests 'first' pattern for {domain}")
            return "first"
        elif len(domain_name) >= 15:  # Long domains often use initials
            print(f"   [DOMAIN_LENGTH] Long domain suggests 'f.last' pattern for {domain}")
            return "f.last"
            
    except Exception as e:
        print(f"   [DOMAIN_ANALYSIS] Error: {str(e)[:30]}...")
    
    return None

def get_linkedin_company_pattern(domain: str, company_name: str = "") -> Optional[str]:
    """Search LinkedIn company pages for email patterns"""
    try:
        if not company_name:
            return None
            
        # Search for LinkedIn company page
        search_query = f'site:linkedin.com/company {company_name} employees contact'
        search_url = f"https://www.google.com/search?q={urllib.parse.quote(search_query)}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(search_url, headers=headers, timeout=5)
        if response.status_code == 200:
            content = response.text.lower()
            
            # Look for email patterns in LinkedIn content
            email_patterns = re.findall(rf'[a-z][a-z0-9._-]*@{re.escape(domain)}', content)
            
            if email_patterns:
                # Simple pattern analysis
                if any('.' in email.split('@')[0] for email in email_patterns):
                    print(f"   [LINKEDIN] Found first.last pattern for {domain}")
                    return "first.last"
                else:
                    print(f"   [LINKEDIN] Found first pattern for {domain}")
                    return "first"
                    
    except Exception as e:
        print(f"   [LINKEDIN] Error: {str(e)[:30]}...")
    
    return None

def extract_phone_numbers_from_text(text: str, country_code: str = "US") -> List[str]:
    """Extract and validate phone numbers from text content"""
    phone_numbers = []
    
    try:
        # Common phone number patterns
        phone_patterns = [
            r'\b\d{3}[-.]\d{3}[-.]\d{4}\b',  # 123-456-7890 or 123.456.7890
            r'\b\(\d{3}\)\s*\d{3}[-.]\d{4}\b',  # (123) 456-7890
            r'\b\d{3}\s+\d{3}\s+\d{4}\b',  # 123 456 7890
            r'\b1[-.]\d{3}[-.]\d{3}[-.]\d{4}\b',  # 1-123-456-7890
            r'\b\+1[-.]\d{3}[-.]\d{3}[-.]\d{4}\b',  # +1-123-456-7890
            r'\b\d{10}\b',  # 1234567890
            r'\b\+\d{1,3}[-.]\d{3,4}[-.]\d{3,4}[-.]\d{3,4}\b',  # International
        ]
        
        found_numbers = set()
        for pattern in phone_patterns:
            matches = re.findall(pattern, text)
            found_numbers.update(matches)
        
        # Validate and format phone numbers
        for number_str in found_numbers:
            try:
                # Clean the number
                clean_number = re.sub(r'[^\d+]', '', number_str)
                if not clean_number.startswith('+'):
                    if len(clean_number) == 10:
                        clean_number = '+1' + clean_number
                    elif len(clean_number) == 11 and clean_number.startswith('1'):
                        clean_number = '+' + clean_number
                
                # Parse and validate
                parsed = phonenumbers.parse(clean_number, country_code)
                if phonenumbers.is_valid_number(parsed):
                    formatted = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
                    phone_numbers.append(formatted)
                    
            except Exception:
                continue
                
    except Exception as e:
        print(f"   [PHONE_EXTRACT] Error: {str(e)[:30]}...")
    
    return list(set(phone_numbers))  # Remove duplicates

def get_website_phone_numbers(domain: str, company_name: str = "") -> List[str]:
    """Scrape company website for phone numbers"""
    try:
        # Try common contact pages
        contact_pages = [
            f"https://{domain}/contact",
            f"https://{domain}/contact-us",
            f"https://{domain}/about",
            f"https://{domain}/team",
            f"https://{domain}"
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        all_phone_numbers = []
        
        for url in contact_pages:
            try:
                print(f"   [PHONE_WEBSITE] Checking: {url}")
                response = requests.get(url, headers=headers, timeout=5)
                
                if response.status_code == 200:
                    content = response.text
                    
                    # Extract phone numbers from page content
                    phone_numbers = extract_phone_numbers_from_text(content)
                    
                    if phone_numbers:
                        print(f"   [PHONE_WEBSITE] Found {len(phone_numbers)} phone numbers on {url}")
                        all_phone_numbers.extend(phone_numbers)
                        break  # Stop after finding phones on first successful page
                        
            except Exception as e:
                print(f"   [PHONE_WEBSITE] Error checking {url}: {str(e)[:30]}...")
                continue
                
            time.sleep(0.3)  # Small delay between requests
            
        return list(set(all_phone_numbers))  # Remove duplicates
        
    except Exception as e:
        print(f"   [PHONE_WEBSITE] General error: {str(e)[:30]}...")
    
    return []

def get_google_phone_numbers(domain: str, company_name: str = "") -> List[str]:
    """Search Google for company phone numbers"""
    try:
        # Multiple search strategies for phone numbers
        search_queries = [
            f'site:{domain} phone contact number',
            f'"{company_name}" phone number contact',
            f'{domain} telephone contact customer service',
            f'"{company_name}" phone directory contact'
        ]
        
        all_phone_numbers = []
        
        for query in search_queries:
            if not query:
                continue
                
            print(f"   [PHONE_GOOGLE] Searching: {query[:50]}...")
            
            # Use requests to search
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            try:
                response = requests.get(search_url, headers=headers, timeout=5)
                if response.status_code == 200:
                    content = response.text
                    
                    # Extract phone numbers from search results
                    phone_numbers = extract_phone_numbers_from_text(content)
                    
                    if phone_numbers:
                        print(f"   [PHONE_GOOGLE] Found {len(phone_numbers)} phone numbers")
                        all_phone_numbers.extend(phone_numbers)
                        break  # Stop after finding phones
                        
            except Exception as e:
                print(f"   [PHONE_GOOGLE] Search error: {str(e)[:30]}...")
                continue
                
            time.sleep(0.5)  # Small delay between searches
            
        return list(set(all_phone_numbers))  # Remove duplicates
        
    except Exception as e:
        print(f"   [PHONE_GOOGLE] General error: {str(e)[:30]}...")
    
    return []

def get_directory_phone_numbers(company_name: str, domain: str = "") -> List[str]:
    """Search business directories for phone numbers"""
    try:
        if not company_name:
            return []
            
        # Search business directories
        directory_queries = [
            f'site:yellowpages.com "{company_name}" phone',
            f'site:whitepages.com "{company_name}" phone',
            f'site:yelp.com "{company_name}" phone',
            f'site:bbb.org "{company_name}" phone'
        ]
        
        all_phone_numbers = []
        
        for query in directory_queries:
            try:
                print(f"   [PHONE_DIRECTORY] Searching: {query[:50]}...")
                
                search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                response = requests.get(search_url, headers=headers, timeout=5)
                if response.status_code == 200:
                    content = response.text
                    
                    # Extract phone numbers from directory results
                    phone_numbers = extract_phone_numbers_from_text(content)
                    
                    if phone_numbers:
                        print(f"   [PHONE_DIRECTORY] Found {len(phone_numbers)} phone numbers")
                        all_phone_numbers.extend(phone_numbers)
                        break  # Stop after finding phones
                        
            except Exception as e:
                print(f"   [PHONE_DIRECTORY] Error: {str(e)[:30]}...")
                continue
                
            time.sleep(0.5)
            
        return list(set(all_phone_numbers))  # Remove duplicates
        
    except Exception as e:
        print(f"   [PHONE_DIRECTORY] General error: {str(e)[:30]}...")
    
    return []

def get_comprehensive_phone_numbers(first_name: str, last_name: str, company_name: str, domain: str, linkedin_url: str = "") -> Tuple[List[str], Dict]:
    """Get phone numbers using multiple detection methods"""
    # Create cache key
    cache_key = f"{company_name.lower()}_{domain.lower()}"
    
    # Check cache first
    if cache_key in _phone_cache:
        print(f"   [PHONE_CACHE] Using cached phone numbers for {company_name}")
        return _phone_cache[cache_key]
    
    all_phone_numbers = []
    phone_sources = {}
    
    # Method 1: SignalHire (already integrated in main function)
    # This will be handled in the main processing function
    
    # Method 2: Website scraping
    website_phones = get_website_phone_numbers(domain, company_name)
    if website_phones:
        all_phone_numbers.extend(website_phones)
        phone_sources['website'] = website_phones
    
    # Method 3: Google search
    google_phones = get_google_phone_numbers(domain, company_name)
    if google_phones:
        all_phone_numbers.extend(google_phones)
        phone_sources['google'] = google_phones
    
    # Method 4: Business directories
    directory_phones = get_directory_phone_numbers(company_name, domain)
    if directory_phones:
        all_phone_numbers.extend(directory_phones)
        phone_sources['directory'] = directory_phones
    
    # Remove duplicates and validate
    unique_phones = list(set(all_phone_numbers))
    
    # Cache the results
    result = (unique_phones, phone_sources)
    _phone_cache[cache_key] = result
    
    if unique_phones:
        print(f"   [PHONE_SUCCESS] Found {len(unique_phones)} unique phone numbers")
        for phone in unique_phones:
            print(f"   [PHONE] {phone}")
    else:
        print(f"   [PHONE_NONE] No phone numbers found")
    
    return result

def detect_crm_technology(domain: str, company_name: str = "") -> Dict[str, any]:
    """Detect CRM and marketing automation technologies used by the company"""
    # Create cache key
    cache_key = f"{company_name.lower()}_{domain.lower()}"
    
    # Check cache first
    if cache_key in _crm_cache:
        print(f"   [CRM_CACHE] Using cached CRM data for {company_name}")
        return _crm_cache[cache_key]
    
    crm_data = {
        'salesforce': False,
        'hubspot': False,
        'marketo': False,
        'pardot': False,
        'pipedrive': False,
        'zoho': False,
        'microsoft_dynamics': False,
        'detected_technologies': [],
        'confidence_score': 0,
        'detection_methods': []
    }
    
    try:
        # Method 1: Fast website technology detection (reduced timeout)
        website_crm = detect_website_crm_technologies(domain)
        if website_crm['technologies']:
            crm_data['detected_technologies'].extend(website_crm['technologies'])
            crm_data['detection_methods'].append('website_analysis')
            crm_data['confidence_score'] += website_crm['confidence']
            
            # Update specific CRM flags
            for tech in website_crm['technologies']:
                if 'salesforce' in tech.lower() or 'pardot' in tech.lower():
                    crm_data['salesforce'] = True
                    crm_data['pardot'] = 'pardot' in tech.lower()
                elif 'hubspot' in tech.lower():
                    crm_data['hubspot'] = True
                elif 'marketo' in tech.lower():
                    crm_data['marketo'] = True
                elif 'pipedrive' in tech.lower():
                    crm_data['pipedrive'] = True
                elif 'zoho' in tech.lower():
                    crm_data['zoho'] = True
                elif 'dynamics' in tech.lower():
                    crm_data['microsoft_dynamics'] = True
        
        # Method 2: Fast DNS/Subdomain analysis (only check Salesforce)
        dns_crm = detect_crm_subdomains_fast(domain)
        if dns_crm['found']:
            crm_data['detected_technologies'].extend(dns_crm['technologies'])
            crm_data['detection_methods'].append('dns_analysis')
            crm_data['confidence_score'] += dns_crm['confidence']
            
            # Update CRM flags from DNS
            for tech in dns_crm['technologies']:
                if 'salesforce' in tech.lower():
                    crm_data['salesforce'] = True
                elif 'hubspot' in tech.lower():
                    crm_data['hubspot'] = True
        
        # Skip slow methods (job posting and public records) for speed
        # These can be enabled later if needed
        
        # Remove duplicates and calculate final confidence
        crm_data['detected_technologies'] = list(set(crm_data['detected_technologies']))
        crm_data['confidence_score'] = min(100, crm_data['confidence_score'])  # Cap at 100%
        
        # Cache the results
        _crm_cache[cache_key] = crm_data
        
        # Log results
        if crm_data['detected_technologies']:
            print(f"   [CRM_SUCCESS] Found {len(crm_data['detected_technologies'])} CRM technologies")
            for tech in crm_data['detected_technologies']:
                print(f"   [CRM] {tech}")
            print(f"   [CRM_CONFIDENCE] {crm_data['confidence_score']}% confidence")
        else:
            print(f"   [CRM_NONE] No CRM technologies detected")
        
        return crm_data
        
    except Exception as e:
        print(f"   [CRM_ERROR] Error detecting CRM: {str(e)[:50]}...")
        _crm_cache[cache_key] = crm_data
        return crm_data

def detect_website_crm_technologies(domain: str) -> Dict[str, any]:
    """Analyze website for CRM technology indicators"""
    result = {'technologies': [], 'confidence': 0}
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Check main website
        url = f"https://{domain}"
        print(f"   [CRM_WEBSITE] Analyzing: {url}")
        
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            content = response.text.lower()
            
            # Salesforce indicators
            salesforce_indicators = [
                'salesforce.com/sfdc/',
                'pi.pardot.com',
                'pardot.com',
                'force.com',
                'salesforce-communities',
                'lightning.force.com',
                'my.salesforce.com'
            ]
            
            # HubSpot indicators
            hubspot_indicators = [
                'hubspot.com',
                'hs-analytics.net',
                'hs-banner.com',
                'hubapi.com',
                'hsforms.net'
            ]
            
            # Marketo indicators
            marketo_indicators = [
                'marketo.com',
                'mktoresp.com',
                'mktoapi.com',
                'marketo.net'
            ]
            
            # Other CRM indicators
            other_crm_indicators = {
                'pipedrive': ['pipedrive.com', 'pipedriveapi.com'],
                'zoho': ['zoho.com', 'zohostatic.com'],
                'dynamics': ['dynamics.com', 'crm.dynamics.com'],
                'freshworks': ['freshworks.com', 'freshsales.io']
            }
            
            # Check for Salesforce
            for indicator in salesforce_indicators:
                if indicator in content:
                    result['technologies'].append(f"Salesforce ({indicator})")
                    result['confidence'] += 25
                    break
            
            # Check for HubSpot
            for indicator in hubspot_indicators:
                if indicator in content:
                    result['technologies'].append(f"HubSpot ({indicator})")
                    result['confidence'] += 25
                    break
            
            # Check for Marketo
            for indicator in marketo_indicators:
                if indicator in content:
                    result['technologies'].append(f"Marketo ({indicator})")
                    result['confidence'] += 25
                    break
            
            # Check for other CRMs
            for crm_name, indicators in other_crm_indicators.items():
                for indicator in indicators:
                    if indicator in content:
                        result['technologies'].append(f"{crm_name.title()} ({indicator})")
                        result['confidence'] += 15
                        break
            
            # Check for generic CRM terms
            crm_terms = ['customer relationship management', 'crm system', 'sales automation']
            for term in crm_terms:
                if term in content:
                    result['confidence'] += 5
                    break
                    
    except Exception as e:
        print(f"   [CRM_WEBSITE] Error: {str(e)[:30]}...")
    
    return result

def detect_crm_subdomains_fast(domain: str) -> Dict[str, any]:
    """Fast check for CRM-related subdomains (Salesforce only)"""
    result = {'found': False, 'technologies': [], 'confidence': 0}
    
    try:
        # Only check the most common Salesforce subdomain for speed
        salesforce_subdomain = f"{domain.split('.')[0]}.my.salesforce.com"
        
        try:
            print(f"   [CRM_DNS_FAST] Checking: {salesforce_subdomain}")
            response = requests.head(f"https://{salesforce_subdomain}", timeout=2)
            if response.status_code < 400:
                result['technologies'].append(f"Salesforce (subdomain: {salesforce_subdomain})")
                result['confidence'] += 40
                result['found'] = True
                
        except Exception:
            pass
                
    except Exception as e:
        print(f"   [CRM_DNS_FAST] Error: {str(e)[:30]}...")
    
    return result

def detect_crm_subdomains(domain: str) -> Dict[str, any]:
    """Check for CRM-related subdomains"""
    result = {'found': False, 'technologies': [], 'confidence': 0}
    
    try:
        # Common CRM subdomains to check
        crm_subdomains = [
            f"{domain.split('.')[0]}.my.salesforce.com",
            f"go.{domain}",  # Marketo
            f"info.{domain}",  # Marketing automation
            f"crm.{domain}",
            f"sales.{domain}",
            f"marketing.{domain}",
            f"pardot.{domain}",
            f"hubspot.{domain}"
        ]
        
        for subdomain in crm_subdomains:
            try:
                print(f"   [CRM_DNS] Checking: {subdomain}")
                response = requests.head(f"https://{subdomain}", timeout=2)
                if response.status_code < 400:
                    if 'salesforce' in subdomain:
                        result['technologies'].append(f"Salesforce (subdomain: {subdomain})")
                        result['confidence'] += 40
                    elif 'marketo' in subdomain or 'go.' in subdomain:
                        result['technologies'].append(f"Marketo (subdomain: {subdomain})")
                        result['confidence'] += 30
                    elif 'hubspot' in subdomain:
                        result['technologies'].append(f"HubSpot (subdomain: {subdomain})")
                        result['confidence'] += 30
                    else:
                        result['technologies'].append(f"CRM System (subdomain: {subdomain})")
                        result['confidence'] += 20
                    
                    result['found'] = True
                    break  # Stop after finding first CRM subdomain
                    
            except Exception:
                continue
                
    except Exception as e:
        print(f"   [CRM_DNS] Error: {str(e)[:30]}...")
    
    return result

def detect_crm_from_job_postings(company_name: str, domain: str) -> Dict[str, any]:
    """Analyze job postings for CRM technology requirements"""
    result = {'technologies': [], 'confidence': 0}
    
    try:
        if not company_name:
            return result
            
        # Search for job postings mentioning CRM technologies
        job_search_queries = [
            f'site:linkedin.com/jobs "{company_name}" salesforce administrator',
            f'site:indeed.com "{company_name}" CRM experience required',
            f'site:glassdoor.com "{company_name}" hubspot marketo',
            f'"{company_name}" hiring "salesforce experience"'
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        for query in job_search_queries:
            try:
                print(f"   [CRM_JOBS] Searching: {query[:50]}...")
                search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
                response = requests.get(search_url, headers=headers, timeout=5)
                
                if response.status_code == 200:
                    content = response.text.lower()
                    
                    # Check for CRM mentions in job search results
                    if 'salesforce' in content and company_name.lower() in content:
                        result['technologies'].append('Salesforce (job posting)')
                        result['confidence'] += 20
                    
                    if 'hubspot' in content and company_name.lower() in content:
                        result['technologies'].append('HubSpot (job posting)')
                        result['confidence'] += 20
                    
                    if 'marketo' in content and company_name.lower() in content:
                        result['technologies'].append('Marketo (job posting)')
                        result['confidence'] += 20
                    
                    if result['technologies']:
                        break  # Stop after finding CRM mentions
                        
            except Exception as e:
                print(f"   [CRM_JOBS] Search error: {str(e)[:30]}...")
                continue
                
            time.sleep(0.5)  # Rate limiting
            
    except Exception as e:
        print(f"   [CRM_JOBS] Error: {str(e)[:30]}...")
    
    return result

def detect_crm_from_public_records(company_name: str, domain: str) -> Dict[str, any]:
    """Search public records and press releases for CRM mentions"""
    result = {'technologies': [], 'confidence': 0}
    
    try:
        if not company_name:
            return result
            
        # Search for press releases and public announcements
        public_search_queries = [
            f'"{company_name}" "implements salesforce" OR "deploys salesforce"',
            f'"{company_name}" "partnership with salesforce"',
            f'"{company_name}" "migrates to hubspot" OR "adopts hubspot"',
            f'"{company_name}" press release CRM implementation'
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        for query in public_search_queries:
            try:
                print(f"   [CRM_PUBLIC] Searching: {query[:50]}...")
                search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
                response = requests.get(search_url, headers=headers, timeout=5)
                
                if response.status_code == 200:
                    content = response.text.lower()
                    
                    # Look for CRM implementation announcements
                    if 'salesforce' in content and ('implements' in content or 'deploys' in content):
                        result['technologies'].append('Salesforce (public announcement)')
                        result['confidence'] += 30
                    
                    if 'hubspot' in content and ('migrates' in content or 'adopts' in content):
                        result['technologies'].append('HubSpot (public announcement)')
                        result['confidence'] += 30
                    
                    if result['technologies']:
                        break  # Stop after finding announcements
                        
            except Exception as e:
                print(f"   [CRM_PUBLIC] Search error: {str(e)[:30]}...")
                continue
                
            time.sleep(0.5)  # Rate limiting
            
    except Exception as e:
        print(f"   [CRM_PUBLIC] Error: {str(e)[:30]}...")
    
    return result

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
                return detected_pattern
                
    except Exception as e:
        print(f"   [WARNING] Hunter.io timeout: {str(e)[:30]}...")
    
    return None

def get_verified_email_pattern(company_name: str, domain: str) -> str:
    """Get verified email pattern using multiple detection methods"""
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
    
    # Method 1: Google search for actual email examples (highest priority)
    google_pattern = get_google_email_pattern(domain, company_name)
    if google_pattern:
        _pattern_cache[domain] = google_pattern
        return google_pattern
    
    # Method 2: Website scraping for contact pages
    website_pattern = get_website_email_pattern(domain, company_name)
    if website_pattern:
        _pattern_cache[domain] = website_pattern
        return website_pattern
    
    # Method 3: Hunter.io API
    hunter_pattern = get_hunter_email_pattern(domain)
    if hunter_pattern:
        _pattern_cache[domain] = hunter_pattern
        return hunter_pattern
    
    # Method 4: RocketReach pattern lookup
    rocketreach_pattern = get_rocketreach_pattern(domain)
    if rocketreach_pattern:
        _pattern_cache[domain] = rocketreach_pattern
        return rocketreach_pattern
    
    # Method 5: LinkedIn company page analysis
    linkedin_pattern = get_linkedin_company_pattern(domain, company_name)
    if linkedin_pattern:
        _pattern_cache[domain] = linkedin_pattern
        return linkedin_pattern
    
    # Method 6: Domain characteristics analysis
    domain_pattern = get_domain_age_pattern(domain)
    if domain_pattern:
        _pattern_cache[domain] = domain_pattern
        return domain_pattern
    
    # Method 7: Clearbit-style inference
    clearbit_pattern = get_clearbit_pattern(domain)
    if clearbit_pattern:
        _pattern_cache[domain] = clearbit_pattern
        return clearbit_pattern
    
    # Method 8: Industry-based hints
    industry = get_industry_from_company(company_name)
    if industry in INDUSTRY_PATTERN_HINTS:
        suggested_pattern = INDUSTRY_PATTERN_HINTS[industry][0]  # Use most common for industry
        print(f"   [INDUSTRY] Using {industry} industry pattern '{suggested_pattern}' for {domain}")
        _pattern_cache[domain] = suggested_pattern
        return suggested_pattern
    
    # Final fallback
    default_pattern = "first.last"
    print(f"   [DEFAULT] Using default pattern '{default_pattern}' for {domain}")
    _pattern_cache[domain] = default_pattern
    return default_pattern

def generate_email_with_pattern(first_name: str, last_name: str, domain: str, pattern: str) -> Optional[str]:
    """Generate email address using specific pattern with improved name cleaning"""
    # Clean names thoroughly
    first = clean_name(first_name)
    last = clean_name(last_name)
    
    print(f"   [NAME_CLEAN] '{first_name}' -> '{first}', '{last_name}' -> '{last}'")
    
    if not first or not domain:
        return None
    
    # Handle cases where last name is missing or very short
    if not last or len(last) <= 1:
        if pattern in ["firstl", "f.last", "last.first", "l.first"]:
            pattern = "first"  # Fall back to first name only
            print(f"   [PATTERN_ADJUST] Changed to 'first' pattern due to missing last name")
        elif pattern == "first.last":
            last = "user"  # Generic fallback
            print(f"   [PATTERN_ADJUST] Using 'user' as last name fallback")
    
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

def generate_and_verify_email_enhanced(first_name: str, last_name: str, company_name: str, domain: str, linkedin_url: str = "") -> tuple[str, bool, str, Dict, List[str], Dict]:
    """Enhanced email generation with SignalHire + pattern detection + phone number extraction + CRM detection"""
    
    # Step 1: Try SignalHire first for direct contact lookup using LinkedIn URL
    signalhire_result = signalhire_email_lookup(linkedin_url, first_name, last_name)
    signalhire_email = signalhire_result.get('email')
    
    # Step 2: Get comprehensive phone numbers
    phone_numbers, phone_sources = get_comprehensive_phone_numbers(
        first_name, last_name, company_name, domain, linkedin_url
    )
    
    # Step 3: Detect CRM technologies
    crm_data = detect_crm_technology(domain, company_name)
    
    if signalhire_email:
        print(f"   SignalHire email: {signalhire_email}")
        
        # Verify SignalHire email
        is_verified = verify_email_with_retry(signalhire_email)
        
        if is_verified:
            print(f"   [SIGNALHIRE_VERIFIED] Email is deliverable!")
            return signalhire_email, True, "signalhire", signalhire_result
        else:
            print(f"   [SIGNALHIRE_UNVERIFIED] Email failed verification, trying patterns...")
    
    # Step 2: If SignalHire fails, use pattern-based generation
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
    
    best_email = signalhire_email  # Use SignalHire email as fallback if available
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
            print(f"   [PATTERN_VERIFIED] Email is deliverable!")
            # Update cache with working pattern
            _pattern_cache[domain] = pattern
            return email, True, pattern, signalhire_result
        else:
            # Keep the first generated email as fallback if no SignalHire email
            if best_email is None:
                best_email = email
                best_pattern = pattern
        
        # Small delay between verification attempts
        time.sleep(0.2)
    
    if best_email:
        print(f"   [UNVERIFIED] Using best guess: {best_email}")
        return best_email, False, best_pattern, signalhire_result, phone_numbers, crm_data
    
    return None, False, detected_pattern, signalhire_result, phone_numbers, crm_data

def process_csv_file(input_file: str):
    """Process CSV file with SignalHire + robust email generation"""
    print(f"\n[START] SignalHire Enhanced email generation")
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
        'Email': 'email',
        'Telephone': 'phone',
        'LinkedIn Profile': 'linkedin_url'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Ensure required columns exist
    required_cols = ['first_name', 'last_name', 'company', 'domain', 'email', 'linkedin_url']
    for col in required_cols:
        if col not in df.columns:
            df[col] = ''
    
    # Add phone column if not exists
    if 'phone' not in df.columns:
        df['phone'] = ''
    
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
    signalhire_success = 0
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
        
        # Show LinkedIn URL if available
        linkedin_url = str(row.get('linkedin_url', '')).strip()
        if linkedin_url and linkedin_url != 'nan' and linkedin_url != '':
            print(f"   LinkedIn: {linkedin_url[:60]}...")
        else:
            print(f"   LinkedIn: [No URL available]")
        
        # Generate and verify email with SignalHire + patterns + phone numbers + CRM detection
        email, is_verified, method_used, signalhire_data, phone_numbers, crm_data = generate_and_verify_email_enhanced(
            row['first_name'], 
            row['last_name'], 
            row['company'], 
            row['domain'],
            linkedin_url
        )
        
        if email:
            df.at[idx, 'email'] = email
            generated_count += 1
            
            # Update phone numbers from multiple sources
            phone_to_use = None
            
            # Priority: SignalHire phone first, then scraped phones
            if signalhire_data.get('phone_number'):
                phone_to_use = signalhire_data['phone_number']
                print(f"   [PHONE_SIGNALHIRE] Using SignalHire phone: {phone_to_use}")
            elif phone_numbers:
                phone_to_use = phone_numbers[0]  # Use first found phone
                print(f"   [PHONE_SCRAPED] Using scraped phone: {phone_to_use}")
            
            if phone_to_use:
                df.at[idx, 'phone'] = phone_to_use
            
            # Update CRM technology information
            if crm_data['detected_technologies']:
                crm_list = ', '.join(crm_data['detected_technologies'])
                df.at[idx, 'crm_technology'] = crm_list
                print(f"   [CRM_UPDATE] Added CRM info: {crm_list}")
            
            # Add CRM confidence score
            if crm_data['confidence_score'] > 0:
                df.at[idx, 'crm_confidence'] = f"{crm_data['confidence_score']}%"
            
            # Add specific CRM flags
            if crm_data['salesforce']:
                df.at[idx, 'uses_salesforce'] = 'Yes'
            if crm_data['hubspot']:
                df.at[idx, 'uses_hubspot'] = 'Yes'
            if crm_data['marketo']:
                df.at[idx, 'uses_marketo'] = 'Yes'
            
            # Track method usage
            if method_used == "signalhire":
                signalhire_success += 1
                pattern_usage['signalhire'] = pattern_usage.get('signalhire', 0) + 1
            else:
                pattern_usage[method_used] = pattern_usage.get(method_used, 0) + 1
            
            if is_verified:
                verified_count += 1
                print(f"   [SUCCESS] Verified email added to record")
            else:
                print(f"   [SUCCESS] Unverified email added to record")
        else:
            print(f"   [FAILED] Could not generate email")
        
        # Delay to avoid rate limiting
        time.sleep(random.uniform(0.5, 1.0))
    
    print("\n" + "=" * 80)
    print(f"[SUMMARY]")
    print(f"   Emails generated: {generated_count}")
    print(f"   Emails verified: {verified_count}")
    print(f"   Verification rate: {(verified_count/generated_count*100):.1f}%" if generated_count > 0 else "N/A")
    print(f"   SignalHire successes: {signalhire_success}")
    print(f"   SignalHire success rate: {(signalhire_success/generated_count*100):.1f}%" if generated_count > 0 else "N/A")
    
    # Show method usage statistics
    print(f"\n[METHOD USAGE]")
    for method, count in sorted(pattern_usage.items(), key=lambda x: x[1], reverse=True):
        print(f"   {method}: {count} emails ({count/generated_count*100:.1f}%)")
    
    # Save updated file
    output_file = input_file.replace('.csv', ' - SIGNALHIRE.csv')
    
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
        print("Usage: python signalhire_enhanced_filler.py \"path/to/input.csv\"")
        return
    
    input_file = sys.argv[1]
    
    if not os.path.exists(input_file):
        print(f"[ERROR] File not found: {input_file}")
        return
    
    process_csv_file(input_file)

if __name__ == "__main__":
    main()
