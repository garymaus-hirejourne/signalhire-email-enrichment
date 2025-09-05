#!/usr/bin/env python3
"""
Wound Specialist Scraper for 495 Beltway Area
Scrapes wound care specialists from healthcare directories and creates enriched mail merge lists.
Integrates with existing SignalHire API and email_pattern_filler.py infrastructure.
"""

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import json
import time
import re
import os
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse
import argparse
from datetime import datetime
import logging

# 495 Beltway ZIP codes (Massachusetts Route 495 corridor)
BELTWAY_ZIP_CODES = [
    # Northern section
    "01810", "01826", "01830", "01832", "01840", "01841", "01843", "01844",
    "01850", "01851", "01852", "01854", "01876", "01886", "01913", "01921",
    "01923", "01960", "01983",
    # Western section  
    "01730", "01752", "01757", "01772", "01773", "01778", "01784", "01801",
    "01803", "01824", "01827", "01833", "01862", "01863", "01864", "01867",
    "01890", "01891", "01906", "01908",
    # Southern section
    "02021", "02026", "02030", "02032", "02035", "02038", "02048", "02052",
    "02054", "02056", "02061", "02062", "02067", "02071", "02072", "02081",
    "02090", "02093", "02180", "02184", "02186", "02187", "02188", "02189",
    # Eastern section
    "02324", "02330", "02331", "02332", "02333", "02339", "02341", "02343",
    "02346", "02347", "02348", "02349", "02351", "02355", "02356", "02357",
    "02358", "02359", "02360", "02361", "02362", "02364", "02366", "02367",
    "02368", "02370", "02375", "02379", "02382", "02384"
]

# Wound care specialties and keywords
WOUND_CARE_KEYWORDS = [
    "wound care", "wound specialist", "wound healing", "wound management",
    "diabetic wound", "chronic wound", "ulcer treatment", "pressure sore",
    "vascular wound", "surgical wound", "burn care", "hyperbaric medicine",
    "podiatric wound", "dermatology wound", "plastic surgery wound"
]

class WoundSpecialistScraper:
    def __init__(self, output_dir="wound_specialists_data", headless=True):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.headless = headless
        self.setup_logging()
        self.specialists = []
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_file = self.output_dir / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_driver(self):
        """Setup Chrome WebDriver with appropriate options"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            return driver
        except Exception as e:
            self.logger.error(f"Failed to setup Chrome driver: {e}")
            self.logger.info("Please ensure ChromeDriver is installed and in PATH")
            return None
    
    def scrape_healthgrades(self, zip_codes, max_pages=5):
        """Scrape wound care specialists from Healthgrades"""
        self.logger.info("Starting Healthgrades scraping...")
        driver = self.setup_driver()
        if not driver:
            return []
        
        specialists = []
        base_url = "https://www.healthgrades.com"
        
        try:
            for zip_code in zip_codes[:10]:  # Limit for testing
                self.logger.info(f"Scraping ZIP code: {zip_code}")
                
                # Search for wound care specialists
                search_url = f"{base_url}/find-a-doctor/massachusetts/wound-care?zip={zip_code}&distance=25"
                
                try:
                    driver.get(search_url)
                    time.sleep(3)
                    
                    # Handle cookie consent if present
                    try:
                        cookie_button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'OK')]"))
                        )
                        cookie_button.click()
                        time.sleep(1)
                    except TimeoutException:
                        pass
                    
                    # Scrape multiple pages
                    for page in range(1, max_pages + 1):
                        self.logger.info(f"Scraping page {page} for ZIP {zip_code}")
                        
                        # Wait for results to load
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "provider-card"))
                            )
                        except TimeoutException:
                            self.logger.warning(f"No results found for ZIP {zip_code}, page {page}")
                            break
                        
                        # Parse current page
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        page_specialists = self.parse_healthgrades_page(soup, zip_code)
                        specialists.extend(page_specialists)
                        
                        # Try to go to next page
                        try:
                            next_button = driver.find_element(By.XPATH, "//a[contains(@class, 'next') or contains(text(), 'Next')]")
                            if next_button.is_enabled():
                                next_button.click()
                                time.sleep(3)
                            else:
                                break
                        except NoSuchElementException:
                            break
                    
                    # Rate limiting between ZIP codes
                    time.sleep(2)
                    
                except Exception as e:
                    self.logger.error(f"Error scraping ZIP {zip_code}: {e}")
                    continue
                    
        finally:
            driver.quit()
        
        self.logger.info(f"Healthgrades scraping complete. Found {len(specialists)} specialists")
        return specialists
    
    def parse_healthgrades_page(self, soup, zip_code):
        """Parse Healthgrades page for specialist information"""
        specialists = []
        
        # Find provider cards (adjust selectors based on actual site structure)
        provider_cards = soup.find_all(['div', 'article'], class_=re.compile(r'provider|doctor|physician'))
        
        for card in provider_cards:
            try:
                specialist = {
                    'source': 'Healthgrades',
                    'zip_code': zip_code,
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Extract name
                name_elem = card.find(['h1', 'h2', 'h3'], class_=re.compile(r'name|title'))
                if name_elem:
                    full_name = name_elem.get_text(strip=True)
                    specialist['full_name'] = full_name
                    
                    # Split name
                    name_parts = full_name.replace(',', '').replace('Dr.', '').replace('MD', '').strip().split()
                    if len(name_parts) >= 2:
                        specialist['first_name'] = name_parts[0]
                        specialist['last_name'] = ' '.join(name_parts[1:])
                
                # Extract practice/clinic name
                practice_elem = card.find(['div', 'span'], class_=re.compile(r'practice|clinic|hospital'))
                if practice_elem:
                    specialist['practice_name'] = practice_elem.get_text(strip=True)
                
                # Extract specialty
                specialty_elem = card.find(['div', 'span'], class_=re.compile(r'specialty|specialization'))
                if specialty_elem:
                    specialist['specialty'] = specialty_elem.get_text(strip=True)
                
                # Extract address
                address_elem = card.find(['div', 'span'], class_=re.compile(r'address|location'))
                if address_elem:
                    specialist['address'] = address_elem.get_text(strip=True)
                
                # Extract phone
                phone_elem = card.find(['a', 'span'], href=re.compile(r'tel:'), class_=re.compile(r'phone'))
                if phone_elem:
                    phone = phone_elem.get_text(strip=True)
                    specialist['phone'] = self.clean_phone(phone)
                
                # Extract website/profile URL
                profile_link = card.find('a', href=True)
                if profile_link:
                    href = profile_link['href']
                    if href.startswith('/'):
                        href = urljoin('https://www.healthgrades.com', href)
                    specialist['profile_url'] = href
                
                # Only add if we have essential information
                if specialist.get('full_name') and specialist.get('practice_name'):
                    specialists.append(specialist)
                    
            except Exception as e:
                self.logger.warning(f"Error parsing provider card: {e}")
                continue
        
        return specialists
    
    def scrape_vitals(self, zip_codes, max_pages=3):
        """Scrape wound care specialists from Vitals.com"""
        self.logger.info("Starting Vitals scraping...")
        driver = self.setup_driver()
        if not driver:
            return []
        
        specialists = []
        base_url = "https://www.vitals.com"
        
        try:
            for zip_code in zip_codes[:5]:  # Limit for testing
                self.logger.info(f"Scraping Vitals for ZIP code: {zip_code}")
                
                search_url = f"{base_url}/doctors/wound-care/ma/{zip_code}"
                
                try:
                    driver.get(search_url)
                    time.sleep(3)
                    
                    # Parse pages
                    for page in range(1, max_pages + 1):
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        page_specialists = self.parse_vitals_page(soup, zip_code)
                        specialists.extend(page_specialists)
                        
                        # Try next page
                        try:
                            next_button = driver.find_element(By.XPATH, "//a[contains(@class, 'next') or contains(text(), 'Next')]")
                            if next_button.is_enabled():
                                next_button.click()
                                time.sleep(3)
                            else:
                                break
                        except NoSuchElementException:
                            break
                    
                    time.sleep(2)
                    
                except Exception as e:
                    self.logger.error(f"Error scraping Vitals ZIP {zip_code}: {e}")
                    continue
                    
        finally:
            driver.quit()
        
        self.logger.info(f"Vitals scraping complete. Found {len(specialists)} specialists")
        return specialists
    
    def parse_vitals_page(self, soup, zip_code):
        """Parse Vitals page for specialist information"""
        specialists = []
        
        # Find doctor cards (adjust selectors based on actual site structure)
        doctor_cards = soup.find_all(['div', 'article'], class_=re.compile(r'doctor|physician|provider'))
        
        for card in doctor_cards:
            try:
                specialist = {
                    'source': 'Vitals',
                    'zip_code': zip_code,
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Similar parsing logic as Healthgrades
                # Extract name, practice, specialty, etc.
                # (Implementation details would be similar but adjusted for Vitals' HTML structure)
                
                # Only add if we have essential information
                if specialist.get('full_name'):
                    specialists.append(specialist)
                    
            except Exception as e:
                self.logger.warning(f"Error parsing Vitals doctor card: {e}")
                continue
        
        return specialists
    
    def scrape_hospital_directories(self):
        """Scrape major hospital wound care departments"""
        self.logger.info("Scraping hospital directories...")
        
        hospitals = [
            {
                'name': 'Massachusetts General Hospital',
                'wound_care_url': 'https://www.massgeneral.org/medicine/wound-care',
                'base_url': 'https://www.massgeneral.org'
            },
            {
                'name': 'Brigham and Women\'s Hospital',
                'wound_care_url': 'https://www.brighamandwomens.org/surgery/wound-care',
                'base_url': 'https://www.brighamandwomens.org'
            },
            {
                'name': 'Beth Israel Deaconess Medical Center',
                'wound_care_url': 'https://www.bidmc.org/centers-and-departments/wound-care',
                'base_url': 'https://www.bidmc.org'
            }
        ]
        
        specialists = []
        driver = self.setup_driver()
        if not driver:
            return []
        
        try:
            for hospital in hospitals:
                self.logger.info(f"Scraping {hospital['name']}")
                
                try:
                    driver.get(hospital['wound_care_url'])
                    time.sleep(3)
                    
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    hospital_specialists = self.parse_hospital_page(soup, hospital)
                    specialists.extend(hospital_specialists)
                    
                    time.sleep(2)
                    
                except Exception as e:
                    self.logger.error(f"Error scraping {hospital['name']}: {e}")
                    continue
                    
        finally:
            driver.quit()
        
        return specialists
    
    def parse_hospital_page(self, soup, hospital):
        """Parse hospital wound care page for specialists"""
        specialists = []
        
        # Look for staff listings, doctor profiles, etc.
        staff_sections = soup.find_all(['div', 'section'], class_=re.compile(r'staff|doctor|physician|provider|team'))
        
        for section in staff_sections:
            # Extract doctor information
            # (Implementation would depend on each hospital's specific HTML structure)
            pass
        
        return specialists
    
    def clean_phone(self, phone_str):
        """Clean and format phone numbers"""
        if not phone_str:
            return None
        
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', phone_str)
        
        # Format as (XXX) XXX-XXXX if 10 digits
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        else:
            return phone_str
    
    def deduplicate_specialists(self, specialists):
        """Remove duplicate specialists based on name and practice"""
        seen = set()
        unique_specialists = []
        
        for specialist in specialists:
            # Create a key for deduplication
            key = (
                specialist.get('full_name', '').lower().strip(),
                specialist.get('practice_name', '').lower().strip(),
                specialist.get('phone', '')
            )
            
            if key not in seen and key[0]:  # Ensure we have a name
                seen.add(key)
                unique_specialists.append(specialist)
        
        self.logger.info(f"Deduplicated {len(specialists)} -> {len(unique_specialists)} specialists")
        return unique_specialists
    
    def save_raw_data(self, specialists, filename="raw_wound_specialists.csv"):
        """Save raw scraped data to CSV"""
        if not specialists:
            self.logger.warning("No specialists to save")
            return None
        
        df = pd.DataFrame(specialists)
        output_file = self.output_dir / filename
        df.to_csv(output_file, index=False)
        
        self.logger.info(f"Saved {len(specialists)} specialists to {output_file}")
        return output_file
    
    def prepare_for_enrichment(self, specialists, output_file="wound_specialists_for_enrichment.csv"):
        """Prepare data in format compatible with email_pattern_filler.py"""
        enrichment_data = []
        
        for specialist in specialists:
            # Convert to format expected by email_pattern_filler.py
            row = {
                'First Name': specialist.get('first_name', ''),
                'Last Name': specialist.get('last_name', ''),
                'Full Name': specialist.get('full_name', ''),
                'Company Name': specialist.get('practice_name', ''),
                'Job Title': specialist.get('specialty', 'Wound Care Specialist'),
                'Phone': specialist.get('phone', ''),
                'Address': specialist.get('address', ''),
                'LinkedIn Profile': '',  # Will be filled by enrichment if found
                'Email': '',  # To be filled by enrichment
                'Company Domain': '',  # To be inferred by enrichment
                'Source': specialist.get('source', ''),
                'ZIP Code': specialist.get('zip_code', ''),
                'Profile URL': specialist.get('profile_url', '')
            }
            enrichment_data.append(row)
        
        df = pd.DataFrame(enrichment_data)
        output_path = self.output_dir / output_file
        df.to_csv(output_path, index=False)
        
        self.logger.info(f"Prepared {len(enrichment_data)} records for enrichment: {output_path}")
        return output_path
    
    def run_full_scrape(self, zip_codes=None, include_hospitals=True):
        """Run complete scraping process"""
        if zip_codes is None:
            zip_codes = BELTWAY_ZIP_CODES
        
        self.logger.info(f"Starting full scrape for {len(zip_codes)} ZIP codes")
        
        all_specialists = []
        
        # Scrape Healthgrades
        try:
            healthgrades_specialists = self.scrape_healthgrades(zip_codes)
            all_specialists.extend(healthgrades_specialists)
        except Exception as e:
            self.logger.error(f"Healthgrades scraping failed: {e}")
        
        # Scrape Vitals
        try:
            vitals_specialists = self.scrape_vitals(zip_codes)
            all_specialists.extend(vitals_specialists)
        except Exception as e:
            self.logger.error(f"Vitals scraping failed: {e}")
        
        # Scrape hospital directories
        if include_hospitals:
            try:
                hospital_specialists = self.scrape_hospital_directories()
                all_specialists.extend(hospital_specialists)
            except Exception as e:
                self.logger.error(f"Hospital scraping failed: {e}")
        
        # Deduplicate
        unique_specialists = self.deduplicate_specialists(all_specialists)
        
        # Save raw data
        raw_file = self.save_raw_data(unique_specialists)
        
        # Prepare for enrichment
        enrichment_file = self.prepare_for_enrichment(unique_specialists)
        
        self.logger.info("Scraping complete!")
        self.logger.info(f"Total specialists found: {len(unique_specialists)}")
        self.logger.info(f"Raw data saved to: {raw_file}")
        self.logger.info(f"Enrichment file ready: {enrichment_file}")
        
        return {
            'total_specialists': len(unique_specialists),
            'raw_file': raw_file,
            'enrichment_file': enrichment_file,
            'specialists': unique_specialists
        }

def main():
    parser = argparse.ArgumentParser(description='Scrape wound care specialists in 495 Beltway area')
    parser.add_argument('--output-dir', default='wound_specialists_data', help='Output directory')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--zip-codes', nargs='+', help='Specific ZIP codes to scrape')
    parser.add_argument('--no-hospitals', action='store_true', help='Skip hospital directory scraping')
    parser.add_argument('--test-mode', action='store_true', help='Run with limited data for testing')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = WoundSpecialistScraper(
        output_dir=args.output_dir,
        headless=args.headless
    )
    
    # Determine ZIP codes to scrape
    zip_codes = args.zip_codes if args.zip_codes else BELTWAY_ZIP_CODES
    if args.test_mode:
        zip_codes = zip_codes[:3]  # Limit to 3 ZIP codes for testing
    
    # Run scraping
    results = scraper.run_full_scrape(
        zip_codes=zip_codes,
        include_hospitals=not args.no_hospitals
    )
    
    print("\n" + "="*60)
    print("WOUND SPECIALIST SCRAPER - RESULTS")
    print("="*60)
    print(f"Total specialists found: {results['total_specialists']}")
    print(f"Raw data file: {results['raw_file']}")
    print(f"Ready for enrichment: {results['enrichment_file']}")
    print("\nNext steps:")
    print("1. Review the raw data file for accuracy")
    print("2. Run email enrichment using your existing email_pattern_filler.py:")
    print(f"   python email_pattern_filler.py \"{results['enrichment_file']}\" \"{results['enrichment_file']}\" --fast --batch-size 50")
    print("3. Use the enriched file for your mail merge campaign")
    print("="*60)

if __name__ == "__main__":
    main()
