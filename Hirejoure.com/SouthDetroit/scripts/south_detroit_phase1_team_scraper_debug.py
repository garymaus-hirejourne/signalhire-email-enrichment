from playwright.sync_api import sync_playwright
import re
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import logging
import os
import time
from pathlib import Path
from typing import List, Dict, Optional, Union, Tuple
import random
from datetime import datetime
import json
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants for rate limiting
THROTTLE_ERRORS = [
    'Too Many Requests',
    'Rate limit exceeded',
    '429',
    '503',
    '504',
    'net::ERR_HTTP2_PROTOCOL_ERROR',
    'net::ERR_CONNECTION_RESET',
    'net::ERR_CONNECTION_TIMED_OUT'
]

# Rate limiting constants
MIN_REQUEST_INTERVAL = 5  # Minimum seconds between requests
MAX_REQUEST_INTERVAL = 10  # Maximum seconds between requests
RANDOM_DELAY = 3  # Random delay between requests
MAX_RETRIES = 5  # Maximum number of retries for failed requests
RETRY_DELAY = 5  # Base seconds to wait between retries
MAX_BACKOFF = 30  # Maximum backoff time in seconds

class ContactScraper:
    def __init__(self, hunter_api_key=None, serpapi_key=None):
        """Initialize the scraper with API keys"""
        self.hunter_api_key = hunter_api_key
        self.serpapi_key = serpapi_key
        self.seen_contacts = set()
        
        # Initialize rate limiting
        self.last_request_time = 0.0
        self.backoff_time = RETRY_DELAY
        self.min_request_interval = 1.5
        
        # Initialize API clients if keys are provided
        self.hunter_client = None
        self.serpapi_client = None
        
        if hunter_api_key:
            try:
                from hunter import HunterClient
                self.hunter_client = HunterClient(hunter_api_key)
            except ImportError:
                logger.warning("Hunter API client not available")
            except Exception as e:
                logger.error(f"Failed to initialize Hunter client: {e}")
                
        if serpapi_key:
            try:
                from serpapi import GoogleSearch
                self.serpapi_client = GoogleSearch({"api_key": serpapi_key})
            except ImportError:
                logger.warning("SerpAPI client not available")
            except Exception as e:
                logger.error(f"Failed to initialize SerpAPI client: {e}")
                
        self.debug_dir = Path("debug_dumps")
        self.debug_dir.mkdir(exist_ok=True)
        self.results = []

    def extract_name(self, title: str) -> Tuple[str, str]:
        """Extract first and last name from a title string"""
        name = ""
        title = title.strip()
        
        # Try different splitting methods
        if ',' in title:
            parts = [p.strip() for p in title.split(',')]
            if len(parts) >= 2:
                name = parts[0]
                title = ' '.join(parts[1:])
                return self.extract_name(title)

        if '/' in title:
            parts = [p.strip() for p in title.split('/')]
            if len(parts) >= 2:
                name = parts[0]
                title = ' '.join(parts[1:])
                return self.extract_name(title)

        # Try splitting by backslash
        if title.find('\\') != -1:
            parts = [p.strip() for p in title.split('\\')]
            if len(parts) >= 2:
                name = parts[0]
                title = ' '.join(parts[1:])
                return self.extract_name(title)

        if '(' in title and ')' in title:
            name = title[:title.find('(')].strip()
            title = title[title.find('('):].strip('()')
            return self.extract_name(title)

        # If no special characters, split by spaces
        parts = title.split()
        if len(parts) >= 2:
            first_name = parts[0]
            last_name = parts[-1]
            middle_parts = parts[1:-1]
            if middle_parts:
                title = ' '.join(middle_parts)
            return first_name, last_name

        return "", title

    def scrape_team_page(self, company_name: str, url: str, limit: int = 150) -> List[Dict]:
        """
        Scrape team page for contact information
        """
        results = []
        start_time = time.time()
        logger.info(f"Scraping {company_name} at {url}")

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context()
                page = context.new_page()

                try:
                    # Navigate to URL
                    page.goto(url)
                    self.rate_limit()

                    # Get page content
                    content = page.content()
                    # Get all visible text from the page
                    text = page.evaluate("() => document.body.innerText")

                    # Find contacts
                    contacts = self.find_contacts(page, content, text)
                    
                    for contact in contacts:
                        # Extract name and title
                        name, title = self.extract_name(contact['text'])
                        
                        if not name or not title:
                            continue

                        # Get email
                        email = self.find_email(page, text)
                        if not email:
                            # Try Hunter.io
                            email = self.get_hunter_email(name, title, company_name)

                        # Get LinkedIn URL
                        linkedin_url = self.find_linkedin_url(page, text)

                        # Get phone number
                        phone = self.find_phone(page, text)

                        # Create contact record
                        contact_record = {
                            'company': company_name,
                            'first_name': name.split()[0],
                            'last_name': name.split()[-1],
                            'title': title,
                            'email': email,
                            'linkedin_url': linkedin_url,
                            'phone': phone,
                            'source_url': url
                        }

                        # Validate contact
                        if self.validate_contact(contact_record):
                            results.append(contact_record)

                except Exception as e:
                    logger.error(f"Error scraping {company_name}: {e}")
                    self.save_debug_dump(page, f"error_{company_name}", True)

                finally:
                    browser.close()

        except Exception as e:
            logger.error(f"Fatal error in scrape_team_page for {company_name}: {e}")
            return []

        end_time = time.time()
        logger.info(f"Scraped {len(results)} contacts for {company_name} in {end_time - start_time:.2f}s")
        return results

    def find_contacts(self, page, content: str, text: str) -> List[Dict]:
        """Find potential contact elements in the page"""
        contacts = []
        
        # Try to find contacts using Playwright selectors
        try:
            # Common patterns for team member elements
            selectors = [
                "div.team-member",
                "div.employee",
                "div.person",
                "div.staff",
                "div.member",
                "article.team",
                "article.person",
                ".team-member",
                ".employee",
                ".person",
                ".staff",
                ".member",
                "[class*='team']",
                "[class*='member']",
                "[class*='person']"
            ]
            
            for selector in selectors:
                elements = page.query_selector_all(selector)
                if elements:
                    logger.info(f"Found {len(elements)} elements with selector: {selector}")
                    for element in elements:
                        try:
                            # Get text content of the element
                            contact_text = element.text_content()
                            if contact_text:
                                contacts.append({
                                    'text': contact_text,
                                    'html': element.inner_html()
                                })
                        except Exception as e:
                            logger.error(f"Error processing element: {e}")
                            continue
        except Exception as e:
            logger.error(f"Error finding contacts with selectors: {e}")
            
        # Try to find contacts using regex patterns in text
        if not contacts:
            # Comprehensive patterns for names and titles
            patterns = [
                # Standard formats
                r'([A-Z][a-z]+\s+[A-Z][a-z]+),\s+(.+)',  # Last, First, Title
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+-\s+(.+)',  # First Last - Title
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\((.+)\)',  # First Last (Title)
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\[(.+)\]',  # First Last [Title]
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\|\s+(.+)',  # First Last | Title
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\:\s+(.+)',  # First Last : Title
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\-\s+(.+)',  # First Last - Title
                
                # With middle names
                r'([A-Z][a-z]+\s+[A-Z][a-z]+\s+[A-Z][a-z]+),\s+(.+)',  # Last, First Middle, Title
                r'([A-Z][a-z]+\s+[A-Z][a-z]+\s+[A-Z][a-z]+)\s+-\s+(.+)',  # First Middle Last - Title
                
                # With initials
                r'([A-Z][a-z]+\s+[A-Z]\.[A-Z][a-z]+),\s+(.+)',  # Last, F. First, Title
                r'([A-Z]\.[A-Z][a-z]+\s+[A-Z][a-z]+),\s+(.+)',  # F. First, Last, Title
                
                # With prefixes/suffixes
                r'([Mm]r\.|[Mm]s\.|[Mm]rs\.|[Dd]r\.)\s+([A-Z][a-z]+\s+[A-Z][a-z]+),\s+(.+)',  # Prefix First Last, Title
                r'([A-Z][a-z]+\s+[A-Z][a-z]+),\s+(.+),\s+([Jj]r\.|[Ss]r\.|[Ii][Vv][Ii][Ii]\.|[Ii][Vv]\.|[Ii][Ii][Ii]\.|[Ii][Ii]\.|[Ii])',  # Last, First, Title, Suffix
                
                # With multiple titles
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\((.+),\s+(.+)\)',  # First Last (Title1, Title2)
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\((.+),\s+(.+),\s+(.+)\)',  # First Last (Title1, Title2, Title3)
                
                # With academic titles
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\((.+),\s+Ph\.?D\.?\)',  # First Last (Title, PhD)
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\((.+),\s+MBA\)',  # First Last (Title, MBA)
            ]
            
            for pattern in patterns:
                matches = re.finditer(pattern, text)
                for match in matches:
                    contacts.append({
                        'text': match.group(0),
                        'html': None
                    })

        return contacts

    def find_email(self, page, text: str) -> Optional[str]:
        """Find email addresses in the text and page with sophisticated patterns"""
        try:
            # Try to find email directly on the page
            email = page.query_selector("a[href^='mailto:']")
            if email:
                href = email.get_attribute("href")
                if href:
                    email = href.replace("mailto:", "")
                    if self.validate_email(email):
                        return email

            # Try to find email in text content with various patterns
            email_patterns = [
                # Standard email patterns
                r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                r'[a-zA-Z0-9._%+-]+\@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                
                # Common email obfuscation patterns
                r'at\s+[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # user at domain.com
                r'\[at\]\s+[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # user [at] domain.com
                r'\[dot\]\s+[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # user@domain [dot] com
                
                # Common business email patterns
                r'([a-z]+)\s+[a-z]+\s+at\s+[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # first last at domain.com
                r'([a-z]+)\s+[a-z]+\s+\[at\]\s+[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # first last [at] domain.com
                
                # Common department patterns
                r'([a-z]+)\s+team\s+at\s+[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # sales team at domain.com
                r'([a-z]+)\s+department\s+at\s+[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # hr department at domain.com
            ]
            
            # Try to find email patterns in text
            for pattern in email_patterns:
                matches = re.finditer(pattern, text)
                for match in matches:
                    email = match.group(0)
                    # Clean up obfuscated emails
                    email = email.replace(' at ', '@')
                    email = email.replace(' [at] ', '@')
                    email = email.replace(' [dot] ', '.')
                    
                    # Try to construct email from name and domain
                    if ' at ' in email:
                        name, domain = email.split(' at ')
                        # Try different common patterns
                        for format in ['{first}.{last}', '{first}_{last}', '{first}{last}', '{last}.{first}', '{last}_{first}']:
                            try:
                                first, last = name.split()
                                email = format.format(first=first.lower(), last=last.lower()) + '@' + domain
                                if self.validate_email(email):
                                    return email
                            except:
                                continue
                    
                    if self.validate_email(email):
                        return email

            # Try to find domain from page and use Hunter.io
            try:
                # Look for domain in various ways
                domain = None
                domain_patterns = [
                    r'www\.[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                    r'[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                    r'\.[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                ]
                
                for pattern in domain_patterns:
                    matches = re.finditer(pattern, text)
                    for match in matches:
                        domain = match.group(0)
                        if domain.startswith('www.'):
                            domain = domain[4:]
                        if domain.startswith('.'):
                            domain = domain[1:]
                        break
                
                if domain and self.hunter_client:
                    try:
                        # Get email pattern from Hunter.io
                        result = self.hunter_client.get_domain_pattern(domain)
                        if result.get('pattern'):
                            # Try to construct email from name and pattern
                            for contact in self.results:
                                if contact.get('first_name') and contact.get('last_name'):
                                    try:
                                        pattern = result['pattern']
                                        email = pattern.format(
                                            first=contact['first_name'].lower(),
                                            last=contact['last_name'].lower()
                                        )
                                        if self.validate_email(email):
                                            return email
                                    except:
                                        continue
                    except Exception as e:
                        logger.error(f"Error using Hunter.io: {e}")

            except Exception as e:
                logger.error(f"Error finding domain: {e}")

        except Exception as e:
            logger.error(f"Error finding email: {e}")
        
        return None

    def find_linkedin_url(self, page, text: str) -> Optional[str]:
        """Find LinkedIn URLs in the text and page"""
        try:
            # Try to find LinkedIn URL directly on the page
            linkedin = page.query_selector("a[href*='linkedin.com']")
            if linkedin:
                href = linkedin.get_attribute("href")
                if href:
                    return href

            # Try to find LinkedIn URL in text content
            linkedin_patterns = [
                r'linkedin\.com/in/[a-zA-Z0-9\-]+',
                r'linkedin\.com/\w+/[a-zA-Z0-9\-]+'  # For country-specific LinkedIn URLs
            ]
            
            for pattern in linkedin_patterns:
                matches = re.finditer(pattern, text)
                for match in matches:
                    url = match.group(0)
                    if url.startswith('linkedin.com'):
                        return f"https://{url}"
                    else:
                        return url

        except Exception as e:
            logger.error(f"Error finding LinkedIn URL: {e}")
        
        return None

    def find_phone(self, page, text: str) -> Optional[str]:
        """Find phone numbers in the text and page"""
        try:
            # Try to find phone number directly on the page
            phone = page.query_selector("a[href^='tel:']")
            if phone:
                href = phone.get_attribute("href")
                if href:
                    return href.replace("tel:", "")

            # Try to find phone number in text content
            phone_patterns = [
                r'\+?\d{1,3}?[-.\s]?\(?(\d{3})\)?[-.\s]?\d{3}[-.\s]?\d{4}',
                r'\+?\d{1,3}?[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}'
            ]
            
            for pattern in phone_patterns:
                matches = re.finditer(pattern, text)
                for match in matches:
                    phone = match.group(0)
                    # Clean up phone number
                    phone = re.sub(r'[\D]', '', phone)
                    if len(phone) >= 10:  # Minimum length for a valid phone number
                        return phone

        except Exception as e:
            logger.error(f"Error finding phone number: {e}")
        
        return None

    def validate_email(self, email: str) -> bool:
        """Validate email format and common patterns with sophisticated checks"""
        if not email or not isinstance(email, str):
            return False
            
        # Basic email format validation
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return False
            
        # Split email into local and domain parts
        local, domain = email.rsplit('@', 1)
        
        # Validate local part
        if not re.match(r'^[a-zA-Z0-9._%+-]+$', local):
            return False
            
        # Validate domain part
        if not re.match(r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', domain):
            return False
            
        # Check for common invalid patterns
        invalid_patterns = [
            r'example\.com',
            r'test\.com',
            r'invalid\.com',
            r'fake\.com',
            r'@\.com',
            r'@\d+\.com',
            r'@localhost',
            r'@localdomain',
            r'@invalid',
            r'@test',
            r'@demo',
            r'@sample',
            r'@mailinator',
            r'@yopmail',
            r'@guerrillamail',
            r'@trashmail'
        ]
        
        if any(re.search(pattern, email) for pattern in invalid_patterns):
            return False
            
        # Check for common business email patterns
        business_patterns = [
            r'^(first|last|full|first_last|last_first|first\.last|last\.first|first_last|last_first)$',
            r'^(first|last)\d+$',
            r'^\d+(first|last)$',
            r'^first\d+last$',
            r'^last\d+first$',
            r'^first\.last\d+$',
            r'^last\.first\d+$'
        ]
        
        # If email matches a common business pattern, it's more likely to be valid
        if any(re.search(pattern, local.lower()) for pattern in business_patterns):
            return True
            
        # Check for common department names in email
        departments = [
            'sales', 'marketing', 'hr', 'recruitment', 'talent', 'operations',
            'finance', 'accounting', 'legal', 'compliance', 'it', 'tech',
            'engineering', 'product', 'development', 'support', 'help',
            'info', 'contact', 'general', 'admin', 'management', 'executive'
        ]
        
        if any(dept in local.lower() for dept in departments):
            return True
            
        return True

    def validate_contact(self, contact: dict) -> bool:
        """Validate contact information integrity"""
        if not contact:
            return False
        # Validate first and last name parts
        for key in ['first_name', 'last_name']:
            part = contact.get(key, '')
            if not part:
                continue
            if '-' in part:
                subparts = part.split('-')
                if all(re.match(r'^[A-Za-z]+$', sp) for sp in subparts):
                    continue
            # Validate regular name parts
            if not re.match(r'^[A-Za-z]+$', part):
                return False
        return True

    def validate_title(self, title: str) -> bool:
        """Validate if a title is a plausible professional title"""
        if not title or not isinstance(title, str):
            return False
            
        # Normalize title by removing common prefixes and suffixes
        title = title.lower()
        title = re.sub(r'^(senior|principal|lead|head|chief)\s+', '', title)
        title = re.sub(r'\s+(of|for|at|in|on)$', '', title)
        
        # List of common professional titles and their variations
        valid_titles = {
            'executive': ['ceo', 'president', 'coo', 'cfo', 'cfo', 'cto', 'cio'],
            'management': ['director', 'manager', 'supervisor', 'leader'],
            'investment': ['partner', 'principal', 'analyst', 'associate', 'advisor'],
            'operations': ['operations', 'operations', 'operations', 'operations'],
            'finance': ['finance', 'treasury', 'accounting', 'audit'],
            'technology': ['technology', 'engineering', 'development', 'research'],
            'strategy': ['strategy', 'planning', 'consulting', 'advisory'],
            'business': ['business', 'corporate', 'enterprise', 'commercial'],
            'legal': ['legal', 'compliance', 'risk', 'regulatory'],
            'hr': ['human resources', 'talent', 'recruitment', 'staffing'],
            'marketing': ['marketing', 'communications', 'brand', 'digital']
        }
        
        # Check if title contains any valid professional terms
        for category, terms in valid_titles.items():
            if any(term in title for term in terms):
                return True
                
        # Check for common title patterns
        if any(pattern in title for pattern in [
            'vp of', 'vice president', 'managing director', 'senior partner',
            'principal', 'founder', 'co-founder', 'co founder', 'executive chairman',
            'board member', 'advisory board', 'investment committee',
            'investment professional', 'portfolio manager'
        ]):
            return True
            
        return False

    def find_email(self, page, text: str) -> Optional[str]:
        """Find email addresses in the text and page"""
        # Add your email finding logic here
        return None

    def find_linkedin_url(self, page, text: str) -> Optional[str]:
        """Find LinkedIn URLs in the text and page"""
        # Add your LinkedIn URL finding logic here
        return None

    def find_phone(self, page, text: str) -> Optional[str]:
        """Find phone numbers in the text and page"""
        # Add your phone finding logic here
        return None

    def get_hunter_email(self, first_name: str, last_name: str, company_domain: str) -> Optional[str]:
        """Use Hunter.io API to find email pattern and generate email"""
        if not self.hunter_client:
            return None
            
        try:
            # Get email pattern from Hunter.io
            pattern = self.hunter_client.get_email_pattern(company_domain)
            
            if pattern:
                # Generate email using pattern
                return f"{first_name.lower()}.{last_name.lower()}@{company_domain}"
                
        except Exception as e:
            logger.error(f"Error getting email pattern: {e}")
            
        return None

    def validate_contact(self, contact: dict) -> bool:
        """Validate contact information integrity"""
        # Add your validation logic here
        return True

    def save_debug_dump(self, page, filename: str, error: bool = False):
        """Save page content for debugging"""
        dir_name = "js_errors" if error else "processed"
        dir_path = self.debug_dir / dir_name
        dir_path.mkdir(exist_ok=True)
        
        with open(dir_path / filename, 'w', encoding='utf-8') as f:
            f.write(page.content())

    def rate_limit(self):
        """Implement rate limiting between requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        
        self.last_request_time = time.time()

    def with_retries(self, func, *args, **kwargs):
        """Execute function with retries and error handling"""
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if any(throttle in str(e).lower() for throttle in THROTTLE_ERRORS):
                    logger.warning(f"Rate limited, retrying in {self.backoff_time}s")
                    time.sleep(self.backoff_time)
                    self.backoff_time = min(self.backoff_time * 2, MAX_BACKOFF)
                else:
                    raise
        raise Exception("Max retries exceeded")
