#!/usr/bin/env python3
"""
Cloud-based SignalHire data processor
Fixes column headers and extracts enriched data properly
"""

import csv
import os
import sys
from datetime import datetime

def process_enriched_csv(input_file='/data/results.csv', output_file='/data/cleaned_results.csv'):
    """
    Process SignalHire results CSV with mixed format data
    Extract only successful enriched records with proper headers
    """
    
    # Correct column headers for enriched data
    headers = [
        "LinkedIn Profile", "Status", "First Name", "Last Name", "Full Name",
        "Current Position", "Company", "Country", "City", 
        "Work Emails", "Personal Emails", "Mobile Phone1", "Mobile Phone2",
        "Work Phone1", "Work Phone2", "Home Phone", "LinkedIn URL",
        "Skills", "Education"
    ]
    
    processed_records = []
    seen_profiles = set()
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Process each line manually since headers don't match data structure
    for line in lines[1:]:  # Skip header
        if not line.strip():
            continue
            
        # Split CSV line properly
        parts = []
        current_part = ""
        in_quotes = False
        
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                parts.append(current_part.strip())
                current_part = ""
            else:
                current_part += char
        
        if current_part:
            parts.append(current_part.strip())
        
        # Skip if not enough data or failed status
        if len(parts) < 7 or parts[1] != 'success':
            continue
            
        # Skip if not enriched data (enriched records have 19+ columns)
        if len(parts) < 15:
            continue
            
        linkedin_url = parts[0]
        if linkedin_url in seen_profiles:
            continue
        seen_profiles.add(linkedin_url)
        
        # Map enriched data to correct columns
        record = {
            "LinkedIn Profile": linkedin_url,
            "Status": "Success", 
            "First Name": parts[2] if len(parts) > 2 else "",
            "Last Name": parts[3] if len(parts) > 3 else "",
            "Full Name": parts[4] if len(parts) > 4 else "",
            "Current Position": parts[5] if len(parts) > 5 else "",
            "Company": parts[6] if len(parts) > 6 else "",
            "Country": parts[7] if len(parts) > 7 else "",
            "City": parts[8] if len(parts) > 8 else "",
            "Work Emails": parts[9] if len(parts) > 9 else "",
            "Personal Emails": parts[10] if len(parts) > 10 else "",
            "Mobile Phone1": clean_phone(parts[11]) if len(parts) > 11 else "",
            "Mobile Phone2": clean_phone(parts[12]) if len(parts) > 12 else "",
            "Work Phone1": clean_phone(parts[13]) if len(parts) > 13 else "",
            "Work Phone2": clean_phone(parts[14]) if len(parts) > 14 else "",
            "Home Phone": clean_phone(parts[15]) if len(parts) > 15 else "",
            "LinkedIn URL": parts[16] if len(parts) > 16 else linkedin_url,
            "Skills": clean_skills(parts[17]) if len(parts) > 17 else "",
            "Education": clean_education(parts[18]) if len(parts) > 18 else ""
        }
        
        processed_records.append(record)
    
    # Write cleaned results
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(processed_records)
    
    return len(processed_records)

def clean_phone(phone):
    """Clean phone number format"""
    if not phone:
        return ""
    return phone.replace('+1 ', '').replace('+1', '').strip()

def clean_skills(skills):
    """Limit skills to top 10 for readability"""
    if not skills:
        return ""
    skill_list = [s.strip() for s in skills.split(';')]
    return '; '.join(skill_list[:10])

def clean_education(education):
    """Limit education to top 3 entries"""
    if not education:
        return ""
    edu_list = [e.strip() for e in education.split(';')]
    return '; '.join(edu_list[:3])

if __name__ == '__main__':
    input_file = sys.argv[1] if len(sys.argv) > 1 else '/data/results.csv'
    output_file = sys.argv[2] if len(sys.argv) > 2 else '/data/cleaned_results.csv'
    
    print(f"Processing: {input_file}")
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        sys.exit(1)
    
    try:
        count = process_enriched_csv(input_file, output_file)
        print(f"Processed {count} enriched records")
        print(f"Saved to: {output_file}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
