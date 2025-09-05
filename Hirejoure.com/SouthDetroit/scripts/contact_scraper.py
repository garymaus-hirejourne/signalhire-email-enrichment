from playwright.sync_api import sync_playwright
import re
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import logging
import os
import time
from pathlib import Path
from typing import List, Dict, Optional, Union
import random
from datetime import datetime
import json

# Constants for rate limiting
MIN_REQUEST_INTERVAL = 2  # Minimum seconds between requests
MAX_REQUEST_INTERVAL = 5  # Maximum seconds between requests
MAX_RETRIES = 3  # Maximum number of retries for failed requests
RETRY_DELAY = 5  # Seconds to wait between retries

# Error handling constants
ERROR_RETRIES = 3  # Number of retries for errors
ERROR_DELAY = 10  # Delay between error retries

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ContactScraper:
    def __init__(self, hunter_api_key: str, serpapi_key: str):
        self.hunter_api_key = hunter_api_key
        self.serpapi_key = serpapi_key
        self.debug_dir = Path("debug_dumps")
        self.debug_dir.mkdir(exist_ok=True)
        self.results = []
        
    def validate_name(self, name: str) -> bool:
        """Validate if a name is a plausible English name"""
        # Remove any non-alphabetic characters
        name = ''.join(filter(str.isalpha, name))
        
        # Check basic name structure
        if not name or len(name) < 2:
            return False
            
        # Check for common non-name patterns
        if any(pattern in name.lower() for pattern in [
            'team', 'join', 'board', 'about', 'contact', 'footer',
            'menu', 'nav', 'header', 'footer', 'copyright',
            'terms', 'privacy', 'policy', 'sitemap'
        ]):
            return False
            
        # Check for plausible name structure
        # At least one space or hyphen indicating first and last name
        if not any(char in name for char in ' -'):
            return False
            
        # Check for reasonable length
        if len(name) > 50:  # Unusually long name
            return False
            
        # Check for common name patterns
        if not any(char.isupper() for char in name):  # No capital letters
            return False
            
        return True
        
    def validate_contact(self, contact: dict) -> bool:
        """Validate contact information integrity"""
        name = contact.get('Full Name', '')
        title = contact.get('Title', '')
        
        # Validate name
        if not self.validate_name(name):
            return False
            
        # Validate title (if present)
        if title and len(title) > 100:  # Unusually long title
            return False
            
        # Check for duplicate entries
        if any(c['Full Name'].lower() == name.lower() and 
               c['Company'] == contact['Company'] for c in self.results):
            return False
            
        return True

    def extract_name(self, title: str) -> tuple:
        """Extract first and last name from a title string"""
        # Basic name patterns
        patterns = [
            r"^(\w+\s+\w+)[,\s]+(\w+)",  # Last, First
            r"^(\w+)[,\s]+(\w+\s+\w+)",  # First, Last Last
            r"^(\w+\.?\s+)?(\w+)[,\s]+(\w+\.?\s+)?(\w+)",  # First M., Last
            r"^(\w+\.?\s+)?(\w+)\s+(\w+\.?\s+)?(\w+)",  # First M. Last
            r"^(\w+\.?\s+)?(\w+\s+\w+\.?\s+)?(\w+)",  # First M. Last Last
            r"^(\w+)\s+(\w+\.?\s+)?(\w+\.?\s+)?(\w+)",  # First Last M. Last
            r"^(\w+)\s+(\w+\.?\s+)?(\w+\.?\s+)?(\w+)"  # First Last
        ]
        
        for pattern in patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                # Get all groups that matched
                groups = [g for g in match.groups() if g]
                if len(groups) >= 2:
                    # First name is usually the first group
                    first_name = groups[0].strip()
                    # Last name is usually the last group
                    last_name = groups[-1].strip()
                    return first_name, last_name
        
        # If no match, return empty names
        return "", ""

    def find_linkedin_url(self, page, text: str) -> Optional[str]:
        """Search for LinkedIn URLs in the text and page"""
        # Direct URL in text
        linkedin_url = re.search(r'linkedin\.com/in/[\w-]+', text, re.IGNORECASE)
        if linkedin_url:
            return f"https://www.linkedin.com/in/{linkedin_url.group(0)}"
            
        # JavaScript variables
        linkedin_data = page.evaluate("""
            () => {
                if (window.__linkedinData) {
                    return window.__linkedinData.url;
                }
                return null;
            }
        """)
        if linkedin_data:
            return linkedin_data
            
        # Data attributes
        elements = page.locator("[data-linkedin-url], [data-profile-url]").all()
        for element in elements:
            url = element.get_attribute("data-linkedin-url") or element.get_attribute("data-profile-url")
            if url:
                return url
                
        return None

    def find_email(self, page, text: str) -> Optional[str]:
        """Search for email addresses in the text and page"""
        # Email patterns
        email_patterns = [
            r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}",  # Standard email
            r"[\w\.-]+\s+at\s+[\w\.-]+\s+dot\s+\w+",  # Obfuscated email
            r"[\w\.-]+\s+at\s+[\w\.-]+\.[a-zA-Z]{2,}"  # Partially obfuscated email
        ]
        
        for pattern in email_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                email = match.group(0)
                if self.validate_email(email):
                    return email
                    
        # Try to get email from API
        try:
            first_name, last_name = self.extract_name(text)
            if first_name and last_name:
                domain = re.search(r'@([\w\.-]+\.[a-zA-Z]{2,})', text)
                if domain:
                    email = self.get_hunter_email(first_name, last_name, domain.group(1))
                    if email:
                        return email
        except Exception as e:
            logger.error(f"Error getting email from API: {e}")
            
        return None

    def validate_email(self, email: str) -> bool:
        """Validate email format and common patterns"""
        if not email:
            return False
            
        # Check basic email format
        if not re.match(r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}", email):
            return False
            
        # Check for common invalid patterns
        invalid_patterns = [
            "example.com",
            "domain.com",
            "test.com",
            "invalid.com",
            "no-reply",
            "noreply",
            "donotreply",
            "info@",
            "contact@",
            "support@",
            "admin@"
        ]
        
        if any(pattern in email.lower() for pattern in invalid_patterns):
            return False
            
        return True

    def find_phone(self, page, text: str) -> Optional[str]:
        """Search for phone numbers in the text and page"""
        # Phone patterns
        phone_patterns = [
            r'\+?\d{1,3}?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # International format
            r'\(\d{3}\)\s?\d{3}-\d{4}',  # (123) 456-7890
            r'\d{3}\.\d{3}\.\d{4}',  # 123.456.7890
            r'\d{3}-\d{3}-\d{4}',  # 123-456-7890
            r'\d{10}'  # 1234567890
        ]
        
        for pattern in phone_patterns:
            match = re.search(pattern, text)
            if match:
                phone = match.group(0)
                # Format phone number
                phone = re.sub(r'[^\d]', '', phone)
                if len(phone) == 10:
                    return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
                elif len(phone) == 11 and phone[0] == '1':
                    return f"({phone[1:4]}) {phone[4:7]}-{phone[7:]}"
        
        return None

    def get_hunter_email(self, first_name: str, last_name: str, company_domain: str) -> Optional[str]:
        """Use Hunter.io API to find email pattern and generate email"""
        try:
            url = f"https://api.hunter.io/v2/email-finder"
            params = {
                'first_name': first_name,
                'last_name': last_name,
                'domain': company_domain,
                'api_key': self.hunter_api_key
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get('data', {}).get('email'):
                    return data['data']['email']
        except Exception as e:
            logger.error(f"Error getting email from Hunter.io: {e}")
        
        return None

    def save_debug_dump(self, page, filename: str, error: bool = False):
        """Save page content for debugging"""
        try:
            # Create debug directory if it doesn't exist
            debug_dir = self.debug_dir / ('errors' if error else 'success')
            debug_dir.mkdir(parents=True, exist_ok=True)
            
            # Save HTML
            html_path = debug_dir / f"{filename}.html"
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(page.content())
                
            # Save screenshot
            screenshot_path = debug_dir / f"{filename}.png"
            page.screenshot(path=screenshot_path)
            
            logger.info(f"Saved debug dump to {debug_dir}")
            
        except Exception as e:
            logger.error(f"Error saving debug dump: {e}")

    def scrape_team_page(self, company_name: str, url: str, limit: int = 150) -> list:
        """Scrape team member information with validation"""
        results = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                try:
                    # Navigate to page with timeout
                    page.goto(url, timeout=60000)
                    
                    # Wait for page load
                    page.wait_for_load_state("networkidle", timeout=30000)
                    
                    # Find team member blocks
                    blocks = page.locator("section, div, li, article").all()[:limit]
                    
                    for block in blocks:
                        try:
                            # Extract text content
                            text = block.inner_text()
                            if not text or len(text.strip()) < 5:
                                continue
                                
                            # Extract name from first line
                            name_line = text.split("\n")[0].strip()
                            
                            # Skip common non-team member text
                            skip_patterns = ["our team", "join us", "board", "overview", "footer", "navigation"]
                            if any(x in name_line.lower() for x in skip_patterns):
                                continue
                                
                            # Extract title from second line if available
                            title_line = text.split("\n")[1].strip() if len(text.split("\n")) > 1 else "N/A"
                            
                            # Create contact entry
                            contact = {
                                "Company": company_name,
                                "Full Name": name_line,
                                "Title": title_line,
                                "Source URL": url,
                                "Timestamp": datetime.now().isoformat()
                            }
                            
                            # Validate contact before adding
                            if self.validate_contact(contact):
                                results.append(contact)
                            else:
                                logger.warning(f"Invalid contact skipped: {contact['Full Name']}")
                                
                        except Exception as e:
                            logger.error(f"Error processing block: {e}")
                            continue
                            
                except Exception as e:
                    logger.error(f"Error scraping {url}: {e}")
                    self.save_debug_dump(page, f"error_{company_name.replace(' ', '_')}")
                    
                finally:
                    browser.close()
                    
        except Exception as e:
            logger.error(f"Error in scrape_team_page: {e}")
            
        return results

    def rate_limit(self):
        """Implement rate limiting between requests"""
        current_time = time.time()
        if not hasattr(self, 'last_request_time'):
            self.last_request_time = current_time
            return
            
        time_since_last = current_time - self.last_request_time
        if time_since_last < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - time_since_last)
            
        self.last_request_time = current_time

    def with_retries(self, func, *args, **kwargs):
        """Execute function with retries and error handling"""
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
