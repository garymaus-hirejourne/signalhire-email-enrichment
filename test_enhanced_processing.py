#!/usr/bin/env python3
"""
Test enhanced data processing locally without Flask dependencies
"""
import csv
import re

def clean_text(text):
    """Clean text by removing quotes and extra whitespace"""
    if not text:
        return ""
    return text.replace('"', '').strip()

def extract_multi_values(field):
    """Extract multiple values from semicolon or comma separated field"""
    if not field:
        return []
    
    # Split by semicolon first, then comma
    values = []
    for item in field.split(';'):
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
        clean_email = email.strip().lower()
        if clean_email and '@' in clean_email and clean_email not in seen:
            clean_emails.append(clean_email)
            seen.add(clean_email)
    
    return clean_emails[:3]  # Limit to 3 emails

def clean_and_dedupe_phones(phones):
    """Clean and deduplicate phone numbers"""
    clean_phones = []
    seen = set()
    
    for phone in phones:
        # Remove all formatting
        clean_phone = ''.join(filter(str.isdigit, phone))
        
        # Remove leading 1 if it's 11 digits (US country code)
        if len(clean_phone) == 11 and clean_phone.startswith('1'):
            clean_phone = clean_phone[1:]
        
        # Only keep 10-digit US numbers
        if len(clean_phone) == 10 and clean_phone not in seen:
            # Format as (XXX) XXX-XXXX
            formatted = f"({clean_phone[:3]}) {clean_phone[3:6]}-{clean_phone[6:]}"
            clean_phones.append(formatted)
            seen.add(clean_phone)
    
    return clean_phones[:3]  # Limit to 3 phones

def process_enriched_csv(input_file, output_file):
    """Process and clean SignalHire enriched data with enhanced validation and multi-value splitting"""
    headers = [
        "LinkedIn Profile", "Status", "First Name", "Last Name", "Full Name",
        "Current Position", "Company", "Country", "City", 
        "Email1", "Email2", "Email3", "Phone1", "Phone2", "Phone3",
        "Skills", "Education"
    ]
    
    processed_records = []
    seen_profiles = set()
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for line in lines[1:]:  # Skip header
        if not line.strip():
            continue
            
        # Parse CSV line handling quotes
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
        
        # Skip failed or insufficient data
        if len(parts) < 15 or parts[1] != 'success':
            continue
            
        linkedin_url = parts[0]
        if linkedin_url in seen_profiles:
            continue
        seen_profiles.add(linkedin_url)
        
        # Extract and clean basic fields
        first_name = clean_text(parts[2])
        last_name = clean_text(parts[3])
        company = clean_text(parts[6])
        
        # Collect all emails and phones
        all_emails = []
        all_phones = []
        
        # Extract emails from work and personal fields
        work_emails = extract_multi_values(parts[9])
        personal_emails = extract_multi_values(parts[10])
        all_emails.extend(work_emails)
        all_emails.extend(personal_emails)
        
        # Extract phones from mobile, work, home fields
        mobile_phones = extract_multi_values(parts[11])
        work_phones = extract_multi_values(parts[13]) if len(parts) > 13 else []
        home_phones = extract_multi_values(parts[15]) if len(parts) > 15 else []
        all_phones.extend(mobile_phones)
        all_phones.extend(work_phones)
        all_phones.extend(home_phones)
        
        # Clean and deduplicate contacts
        clean_emails = clean_and_dedupe_emails(all_emails)
        clean_phones = clean_and_dedupe_phones(all_phones)
        
        # VALIDATION: Skip rows missing required fields, but try email generation first
        if not (first_name and last_name and company and (clean_emails or clean_phones)):
            # Try to generate email if missing
            if first_name and last_name and company and not clean_emails:
                generated_email = generate_email_from_company(first_name, last_name, company)
                if generated_email:
                    clean_emails = [generated_email]
                    print(f"GENERATED EMAIL: {first_name} {last_name} - {generated_email}")
                else:
                    print(f"FILTERED OUT: {first_name} {last_name} - Missing required contact info")
                    continue
            else:
                print(f"FILTERED OUT: {first_name} {last_name} - Missing required fields")
                continue
        
        # Build record with split contact fields
        record = {
            "LinkedIn Profile": linkedin_url,
            "Status": "Success", 
            "First Name": first_name,
            "Last Name": last_name,
            "Full Name": clean_text(parts[4]),
            "Current Position": clean_text(parts[5]),
            "Company": company,
            "Country": parts[7],
            "City": parts[8],
            "Email1": clean_emails[0] if len(clean_emails) > 0 else "",
            "Email2": clean_emails[1] if len(clean_emails) > 1 else "",
            "Email3": clean_emails[2] if len(clean_emails) > 2 else "",
            "Phone1": clean_phones[0] if len(clean_phones) > 0 else "",
            "Phone2": clean_phones[1] if len(clean_phones) > 1 else "",
            "Phone3": clean_phones[2] if len(clean_phones) > 2 else "",
            "Skills": parts[17][:200] if len(parts) > 17 else "",  # Limit skills
            "Education": parts[18][:200] if len(parts) > 18 else ""  # Limit education
        }
        
        processed_records.append(record)
        print(f"PROCESSED: {first_name} {last_name} - {len(clean_emails)} emails, {len(clean_phones)} phones")
    
    # Write cleaned results
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(processed_records)
    
    return len(processed_records)

def generate_email_from_company(first_name, last_name, company):
    """Generate email address using company domain and email pattern detection"""
    if not first_name or not last_name or not company:
        return None
    
    # Clean and normalize names
    first_clean = clean_name_for_email(first_name)
    last_clean = clean_name_for_email(last_name)
    
    if not first_clean or not last_clean:
        return None
    
    # Generate domain from company name
    domain = generate_domain_from_company(company)
    if not domain:
        return None
    
    # Get email pattern for domain
    pattern = get_email_pattern_for_domain(domain, company)
    
    # Generate email based on pattern
    email = generate_email_with_pattern(first_clean, last_clean, domain, pattern)
    
    return email

def clean_name_for_email(name):
    """Clean name for email generation"""
    if not name:
        return ""
    
    # Remove credentials, titles, and special characters
    name = str(name).strip().lower()
    name = name.split(',')[0]  # Remove credentials after comma
    name = name.replace("'", "").replace("-", "").replace(".", "")
    name = ''.join(c for c in name if c.isalpha())
    
    return name

def generate_domain_from_company(company):
    """Generate email domain from company name"""
    if not company:
        return None
    
    # Known domain mappings for common companies
    domain_mappings = {
        'tufts medical center': 'tuftsmedicalcenter.org',
        'tufts medicine': 'tuftsmedicalcenter.org',
        'mass general brigham': 'partners.org',
        'massachusetts general hospital': 'partners.org',
        'brigham and women\'s hospital': 'partners.org',
        'harvard medical school': 'hms.harvard.edu',
        'harvard business school': 'hbs.edu',
        'harvard university': 'harvard.edu',
        'boston university': 'bu.edu',
        'boston medical center': 'bmc.org',
        'beth israel deaconess': 'bidmc.harvard.edu',
        'dana-farber cancer institute': 'dfci.harvard.edu',
        'children\'s hospital boston': 'childrens.harvard.edu',
        'boston children\'s hospital': 'childrens.harvard.edu'
    }
    
    company_lower = company.lower().strip()
    
    # Check known mappings first
    for key, domain in domain_mappings.items():
        if key in company_lower:
            return domain
    
    # Generate domain from company name
    # Remove common words and clean
    company_clean = company_lower
    remove_words = ['inc', 'llc', 'corp', 'corporation', 'company', 'co', 'ltd', 'limited', 
                   'the', 'and', '&', 'of', 'for', 'medical', 'center', 'hospital', 'health']
    
    for word in remove_words:
        company_clean = company_clean.replace(f' {word} ', ' ')
        company_clean = company_clean.replace(f' {word}', '')
        company_clean = company_clean.replace(f'{word} ', '')
    
    # Clean and format
    company_clean = ''.join(c for c in company_clean if c.isalnum())
    
    if len(company_clean) > 3:
        return f"{company_clean}.com"
    
    return None

def get_email_pattern_for_domain(domain, company):
    """Get email pattern for domain using known patterns and heuristics"""
    
    # Known patterns for specific domains
    known_patterns = {
        'tuftsmedicalcenter.org': 'first.last',
        'partners.org': 'first.last', 
        'hms.harvard.edu': 'first_last',
        'hbs.edu': 'first.last',
        'harvard.edu': 'first.last',
        'bu.edu': 'first.last',
        'bmc.org': 'first.last',
        'bidmc.harvard.edu': 'first.last',
        'dfci.harvard.edu': 'first.last',
        'childrens.harvard.edu': 'first.last'
    }
    
    if domain in known_patterns:
        return known_patterns[domain]
    
    # Default patterns by domain type
    if '.edu' in domain or '.org' in domain:
        return 'first.last'  # Academic/non-profit typically use first.last
    elif '.gov' in domain:
        return 'first.last'  # Government typically uses first.last
    else:
        return 'first.last'  # Default to most common pattern

def generate_email_with_pattern(first_name, last_name, domain, pattern):
    """Generate email address using specified pattern"""
    if not first_name or not last_name or not domain:
        return None
    
    patterns = {
        'first.last': f"{first_name}.{last_name}@{domain}",
        'first_last': f"{first_name}_{last_name}@{domain}",
        'firstlast': f"{first_name}{last_name}@{domain}",
        'f.last': f"{first_name[0]}.{last_name}@{domain}",
        'firstl': f"{first_name}{last_name[0]}@{domain}",
        'first': f"{first_name}@{domain}"
    }
    
    return patterns.get(pattern, f"{first_name}.{last_name}@{domain}")

if __name__ == "__main__":
    print("Testing enhanced data processing with email generation...")
    result = process_enriched_csv('enriched_results_final.csv', 'test_enhanced_output.csv')
    print(f"\nProcessed {result} records with enhanced validation and contact splitting")
    print("Output saved to: test_enhanced_output.csv")
