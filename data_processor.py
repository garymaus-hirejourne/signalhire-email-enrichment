#!/usr/bin/env python3
"""
Cloud-based data processor for SignalHire enriched results
Fixes column headers and cleans up data format
"""

import csv
import json
import os
import sys
from pathlib import Path
from datetime import datetime

def process_signalhire_results(input_file, output_file):
    """
    Process and clean SignalHire results CSV
    - Fix column headers
    - Remove duplicate/failed records
    - Standardize data format
    - Extract only successful enriched records
    """
    
    # Target clean column headers
    clean_headers = [
        "LinkedIn Profile",
        "Status", 
        "First Name",
        "Last Name",
        "Full Name",
        "Current Position",
        "Company",
        "Country",
        "City",
        "Work Emails",
        "Personal Emails", 
        "Mobile Phone",
        "Work Phone",
        "Home Phone",
        "Skills",
        "Education"
    ]
    
    processed_records = []
    seen_profiles = set()
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Skip failed records
            if row.get('status') != 'success':
                continue
                
            # Get LinkedIn profile URL
            linkedin_url = row.get('item', '').strip()
            if not linkedin_url or 'linkedin.com' not in linkedin_url:
                continue
                
            # Skip duplicates
            if linkedin_url in seen_profiles:
                continue
            seen_profiles.add(linkedin_url)
            
            # Check if this is enriched data (has actual data in key fields)
            # The enriched data is in the same CSV but with data in later columns
            row_values = list(row.values())
            if len([v for v in row_values if v and v.strip()]) < 5:  # Skip mostly empty rows
                continue
                
            # Extract and clean data
            clean_record = {
                "LinkedIn Profile": linkedin_url,
                "Status": "Success",
                "First Name": clean_text(row.get('fullName', '').split(',')[0] if ',' in row.get('fullName', '') else row.get('fullName', '').split()[0] if row.get('fullName', '') else ''),
                "Last Name": clean_text(row.get('fullName', '').split(',')[1] if ',' in row.get('fullName', '') else ' '.join(row.get('fullName', '').split()[1:]) if len(row.get('fullName', '').split()) > 1 else ''),
                "Full Name": clean_text(row.get('fullName', '')),
                "Current Position": clean_text(get_column_value(row, ['Current Position', 'position', 'title'])),
                "Company": clean_text(get_column_value(row, ['Company', 'company'])),
                "Country": clean_text(get_column_value(row, ['Country', 'country'])),
                "City": clean_text(get_column_value(row, ['City', 'city'])),
                "Work Emails": clean_emails(get_column_value(row, ['Emails (Work)', 'work_emails', 'emails'])),
                "Personal Emails": clean_emails(get_column_value(row, ['Emails (Personal)', 'personal_emails'])),
                "Mobile Phone": clean_phone(get_column_value(row, ['Mobile Phone1', 'mobile', 'phone'])),
                "Work Phone": clean_phone(get_column_value(row, ['Work Phone1', 'work_phone'])),
                "Home Phone": clean_phone(get_column_value(row, ['Home Phone', 'home_phone'])),
                "Skills": clean_skills(get_column_value(row, ['Skills', 'skills'])),
                "Education": clean_education(get_column_value(row, ['Education', 'education']))
            }
            
            processed_records.append(clean_record)
    
    # Write cleaned results
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=clean_headers)
        writer.writeheader()
        writer.writerows(processed_records)
    
    return len(processed_records)

def get_column_value(row, possible_keys):
    """Get value from row using multiple possible column names"""
    for key in possible_keys:
        if key in row and row[key]:
            return row[key]
    return ''

def clean_text(text):
    """Clean and normalize text fields"""
    if not text:
        return ''
    return text.strip().replace('\n', ' ').replace('\r', '')

def clean_emails(emails_text):
    """Clean and format email addresses"""
    if not emails_text:
        return ''
    # Handle both semicolon and comma separated emails
    emails = []
    for email in emails_text.replace(';', ',').split(','):
        email = email.strip()
        if email and '@' in email:
            emails.append(email)
    return '; '.join(emails)

def clean_phone(phone_text):
    """Clean and format phone numbers"""
    if not phone_text:
        return ''
    # Take first phone if multiple
    phones = phone_text.replace(';', ',').split(',')
    phone = phones[0].strip() if phones else ''
    # Basic phone cleaning
    return phone.replace('(', '').replace(')', '').replace('-', ' ').strip()

def clean_skills(skills_text):
    """Clean and format skills"""
    if not skills_text:
        return ''
    # Limit to top 10 skills for readability
    skills = [s.strip() for s in skills_text.split(';')]
    return '; '.join(skills[:10])

def clean_education(education_text):
    """Clean and format education"""
    if not education_text:
        return ''
    # Limit to top 3 education entries
    education = [e.strip() for e in education_text.split(';')]
    return '; '.join(education[:3])

if __name__ == '__main__':
    input_file = sys.argv[1] if len(sys.argv) > 1 else '/data/results.csv'
    output_file = sys.argv[2] if len(sys.argv) > 2 else '/data/cleaned_results.csv'
    
    print(f"Processing SignalHire results: {input_file}")
    
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found")
        sys.exit(1)
    
    try:
        count = process_signalhire_results(input_file, output_file)
        print(f"✅ Processed {count} enriched records")
        print(f"📁 Cleaned results saved to: {output_file}")
        
        # Show preview
        if os.path.exists(output_file):
            print("\n📋 Preview of cleaned data:")
            with open(output_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for i, line in enumerate(lines[:3]):
                    print(f"  {i+1}: {line.strip()}")
                if len(lines) > 3:
                    print(f"  ... and {len(lines)-3} more records")
                    
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        sys.exit(1)
