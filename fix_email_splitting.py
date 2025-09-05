#!/usr/bin/env python3
"""
Fix Email Splitting Script
Processes the current SignalHire results and properly splits multi-value emails into separate columns
Also generates missing emails for records that need them
"""

import pandas as pd
import csv
import re
from pathlib import Path

def extract_multi_values(field):
    """Extract multiple values from semicolon or comma separated field"""
    if not field or pd.isna(field):
        return []
    
    # Split by semicolon first, then comma
    values = []
    for item in str(field).split(';'):
        for subitem in item.split(','):
            clean_item = subitem.strip()
            if clean_item:
                values.append(clean_item)
    
    return values

def clean_and_dedupe_emails(emails):
    """Clean and deduplicate email addresses"""
    clean_emails = []
    seen = set()
    
    for email in emails:
        clean_email = str(email).strip().lower()
        if clean_email and '@' in clean_email and clean_email not in seen:
            clean_emails.append(clean_email)
            seen.add(clean_email)
    
    return clean_emails

def clean_and_dedupe_phones(phones):
    """Clean and deduplicate phone numbers"""
    clean_phones = []
    seen = set()
    
    for phone in phones:
        if not phone or pd.isna(phone):
            continue
            
        # Clean phone number - remove +1, parentheses, dashes, spaces
        clean_phone = str(phone).strip()
        clean_phone = re.sub(r'^\+1\s*', '', clean_phone)
        clean_phone = re.sub(r'[^\d]', '', clean_phone)
        
        if len(clean_phone) == 10:
            # Format as (XXX) XXX-XXXX
            formatted = f"({clean_phone[:3]}) {clean_phone[3:6]}-{clean_phone[6:]}"
            if formatted not in seen:
                clean_phones.append(formatted)
                seen.add(formatted)
        elif len(clean_phone) > 10:
            # Keep original if not standard US format
            if clean_phone not in seen:
                clean_phones.append(clean_phone)
                seen.add(clean_phone)
    
    return clean_phones

def generate_email_from_company(first_name, last_name, company):
    """Generate email address from company name and person details"""
    if not (first_name and last_name and company):
        return None
    
    # Healthcare domain mappings
    domain_mappings = {
        'tufts medical center': 'tuftsmedicalcenter.org',
        'mass general brigham': 'massgeneralbrigham.org',
        'harvard medical school': 'hms.harvard.edu',
        'harvard university': 'harvard.edu',
        'harvard business school': 'hbs.edu',
        'boston medical center': 'bmc.org',
        'beth israel lahey health': 'bilh.org',
        'brown university health': 'brownhealth.org',
        'northeastern university': 'northeastern.edu',
        'boston university': 'bu.edu',
        'massachusetts eye and ear': 'meei.harvard.edu',
        'brigham and women\'s hospital': 'bwh.harvard.edu',
        'johnson & johnson': 'jnj.com',
        'pfizer': 'pfizer.com'
    }
    
    # Clean company name and find domain
    company_clean = company.lower().strip()
    domain = None
    
    for company_key, mapped_domain in domain_mappings.items():
        if company_key in company_clean:
            domain = mapped_domain
            break
    
    if not domain:
        # Generate domain from company name
        domain_name = re.sub(r'[^a-zA-Z0-9\s]', '', company_clean)
        domain_name = re.sub(r'\s+', '', domain_name)
        domain = f"{domain_name}.com"
    
    # Generate email using first.last pattern
    first_clean = re.sub(r'[^a-zA-Z]', '', first_name.lower())
    last_clean = re.sub(r'[^a-zA-Z]', '', last_name.lower())
    
    if first_clean and last_clean:
        return f"{first_clean}.{last_clean}@{domain}"
    
    return None

def process_signalhire_results(input_file, output_file):
    """Process SignalHire results and split emails properly"""
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # New headers for split format
    headers = [
        "LinkedIn Profile", "Status", "First Name", "Last Name", "Full Name",
        "Current Position", "Company", "Country", "City", 
        "Email1", "Email2", "Email3", "Phone1", "Phone2", "Phone3",
        "Skills", "Education"
    ]
    
    processed_records = []
    
    for _, row in df.iterrows():
        # Extract basic info
        first_name = str(row.get('First Name', '')).strip()
        last_name = str(row.get('Last Name', '')).strip()
        company = str(row.get('Company', '')).strip()
        
        # Extract and split emails
        work_emails = extract_multi_values(row.get('Work Emails', ''))
        personal_emails = extract_multi_values(row.get('Personal Emails', ''))
        
        all_emails = work_emails + personal_emails
        clean_emails = clean_and_dedupe_emails(all_emails)
        
        # Generate email if missing or insufficient
        if len(clean_emails) == 0 and first_name and last_name and company:
            generated_email = generate_email_from_company(first_name, last_name, company)
            if generated_email:
                clean_emails = [generated_email]
                try:
                    print(f"Generated email for {first_name} {last_name}: {generated_email}")
                except UnicodeEncodeError:
                    print(f"Generated email for contact: {generated_email}")
        elif len(clean_emails) == 1 and first_name and last_name and company:
            # Has one email but could use another
            generated_email = generate_email_from_company(first_name, last_name, company)
            if generated_email and generated_email not in clean_emails:
                clean_emails.append(generated_email)
                try:
                    print(f"Generated additional email for {first_name} {last_name}: {generated_email}")
                except UnicodeEncodeError:
                    print(f"Generated additional email for contact: {generated_email}")
        
        # Extract and split phones
        mobile_phones = extract_multi_values(row.get('Mobile Phone', ''))
        work_phones = extract_multi_values(row.get('Work Phone', ''))
        home_phones = extract_multi_values(row.get('Home Phone', ''))
        
        all_phones = mobile_phones + work_phones + home_phones
        clean_phones = clean_and_dedupe_phones(all_phones)
        
        # Skip if missing required fields
        if not (first_name and last_name and company and (clean_emails or clean_phones)):
            print(f"FILTERED OUT: {first_name} {last_name} - Missing required fields")
            continue
        
        # Build record with split contacts
        record = {
            "LinkedIn Profile": row.get('LinkedIn Profile', ''),
            "Status": "Success",
            "First Name": first_name,
            "Last Name": last_name,
            "Full Name": str(row.get('Full Name', '')).strip(),
            "Current Position": str(row.get('Current Position', '')).strip(),
            "Company": company,
            "Country": str(row.get('Country', '')).strip(),
            "City": str(row.get('City', '')).strip(),
            "Email1": clean_emails[0] if len(clean_emails) > 0 else "",
            "Email2": clean_emails[1] if len(clean_emails) > 1 else "",
            "Email3": clean_emails[2] if len(clean_emails) > 2 else "",
            "Phone1": clean_phones[0] if len(clean_phones) > 0 else "",
            "Phone2": clean_phones[1] if len(clean_phones) > 1 else "",
            "Phone3": clean_phones[2] if len(clean_phones) > 2 else "",
            "Skills": str(row.get('Skills', '')).strip(),
            "Education": str(row.get('Education', '')).strip()
        }
        
        processed_records.append(record)
    
    # Write processed results
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(processed_records)
    
    print(f"\nProcessed {len(processed_records)} records")
    print(f"Output saved to: {output_file}")
    return len(processed_records)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python fix_email_splitting.py <input_file> <output_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    print("Processing SignalHire results with proper email splitting...")
    process_signalhire_results(input_file, output_file)
