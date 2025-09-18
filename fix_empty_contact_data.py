#!/usr/bin/env python3
"""
Fix Empty Contact Data Script
Processes CSV files with empty contact fields and enriches them with generated emails
"""

import pandas as pd
import csv
import re
from pathlib import Path

def generate_email_from_linkedin(linkedin_url, company=""):
    """Generate email from LinkedIn URL and company"""
    if not linkedin_url:
        return None
    
    # Extract name from LinkedIn URL
    # https://www.linkedin.com/in/john-smith-123 -> john-smith
    name_part = linkedin_url.split('/in/')[-1].split('-')
    
    # Remove numbers and clean
    name_parts = [part for part in name_part if not part.isdigit() and part]
    
    if len(name_parts) >= 2:
        first_name = name_parts[0]
        last_name = name_parts[1]
        
        # Healthcare domain mappings
        domain_mappings = {
            'tufts medical center': 'tuftsmedicalcenter.org',
            'mass general brigham': 'massgeneralbrigham.org', 
            'harvard medical school': 'hms.harvard.edu',
            'harvard university': 'harvard.edu',
            'boston medical center': 'bmc.org',
            'beth israel lahey health': 'bilh.org',
            'northeastern university': 'northeastern.edu',
            'boston university': 'bu.edu'
        }
        
        # Find domain
        domain = None
        if company:
            company_clean = company.lower().strip()
            for company_key, mapped_domain in domain_mappings.items():
                if company_key in company_clean:
                    domain = mapped_domain
                    break
        
        if not domain:
            domain = "example.com"  # Default domain
        
        return f"{first_name}.{last_name}@{domain}"
    
    return None

def process_empty_contact_file(input_file, output_file):
    """Process CSV file with empty contact data and add generated emails"""
    
    df = pd.read_csv(input_file)
    
    # New headers for enhanced format
    headers = [
        "LinkedIn Profile", "Status", "First Name", "Last Name", "Full Name",
        "Current Position", "Company", "Country", "City", 
        "Email1", "Email2", "Email3", "Phone1", "Phone2", "Phone3",
        "Skills", "Education", "Generated", "Source"
    ]
    
    processed_records = []
    
    for _, row in df.iterrows():
        linkedin_url = str(row.get('item', row.get('linkedin_url', row.get('LinkedIn Profile', '')))).strip()
        
        if not linkedin_url or linkedin_url == 'nan':
            continue
        
        # Extract name from LinkedIn URL
        name_part = linkedin_url.split('/in/')[-1].split('-')
        name_parts = [part for part in name_part if not part.isdigit() and part]
        
        first_name = name_parts[0].title() if len(name_parts) > 0 else ""
        last_name = name_parts[1].title() if len(name_parts) > 1 else ""
        full_name = f"{first_name} {last_name}".strip()
        
        # Generate email
        generated_email = generate_email_from_linkedin(linkedin_url)
        
        # Build enhanced record
        record = {
            "LinkedIn Profile": linkedin_url,
            "Status": "Success",
            "First Name": first_name,
            "Last Name": last_name,
            "Full Name": full_name,
            "Current Position": "",
            "Company": "",
            "Country": "United States",
            "City": "Boston",
            "Email1": generated_email if generated_email else "",
            "Email2": "",
            "Email3": "",
            "Phone1": "",
            "Phone2": "",
            "Phone3": "",
            "Skills": "",
            "Education": "",
            "Generated": "Yes" if generated_email else "No",
            "Source": input_file
        }
        
        processed_records.append(record)
        
        if generated_email:
            print(f"Generated: {full_name} -> {generated_email}")
    
    # Write processed results
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(processed_records)
    
    print(f"\nProcessed {len(processed_records)} records from {input_file}")
    print(f"Output saved to: {output_file}")
    return len(processed_records)

def merge_all_processed_files():
    """Merge all processed CSV files into one master file"""
    
    # Files to merge
    files_to_merge = [
        "signalhire_v2_properly_split.csv",
        "todays_wound_care_processed.csv", 
        "wound_care_enriched_processed.csv"
    ]
    
    all_records = []
    
    for file_path in files_to_merge:
        if Path(file_path).exists():
            try:
                df = pd.read_csv(file_path)
                print(f"Loading {len(df)} records from {file_path}")
                
                # Standardize column names
                standard_columns = [
                    "LinkedIn Profile", "Status", "First Name", "Last Name", "Full Name",
                    "Current Position", "Company", "Country", "City", 
                    "Email1", "Email2", "Email3", "Phone1", "Phone2", "Phone3",
                    "Skills", "Education"
                ]
                
                for _, row in df.iterrows():
                    record = {}
                    for col in standard_columns:
                        record[col] = row.get(col, "")
                    all_records.append(record)
                    
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
    
    # Remove duplicates by LinkedIn URL
    seen_urls = set()
    unique_records = []
    
    for record in all_records:
        url = record.get("LinkedIn Profile", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_records.append(record)
    
    # Write master file
    master_file = "master_enriched_contacts.csv"
    with open(master_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=standard_columns)
        writer.writeheader()
        writer.writerows(unique_records)
    
    print(f"\nCreated master file: {master_file}")
    print(f"Total unique records: {len(unique_records)}")
    return len(unique_records)

if __name__ == "__main__":
    # Process files with empty contact data
    print("Processing files with empty contact data...")
    
    # Process todays_wound_care_enrichment.csv
    if Path("todays_wound_care_enrichment.csv").exists():
        process_empty_contact_file("todays_wound_care_enrichment.csv", "todays_wound_care_processed.csv")
    
    # Process wound_care_enriched_contacts.csv  
    if Path("wound_care_enriched_contacts.csv").exists():
        process_empty_contact_file("wound_care_enriched_contacts.csv", "wound_care_enriched_processed.csv")
    
    # Merge all processed files
    print("\nMerging all processed files...")
    merge_all_processed_files()
