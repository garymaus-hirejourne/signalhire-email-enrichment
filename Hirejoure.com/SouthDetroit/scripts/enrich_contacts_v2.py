import pandas as pd
import re
import logging
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def generate_email(first_name: str, last_name: str) -> str:
    """Generate a simple professional email address."""
    if not first_name or not last_name:
        return ""
    
    # Clean names
    first = first_name.lower().strip()
    last = last_name.lower().strip()
    
    # Generate simple email patterns
    patterns = [
        f"{first}.{last}@",  # john.smith@
        f"{first[0]}{last}@",  # jsmith@
        f"{first}_{last}@",  # john_smith@
        f"{last}{first[0]}@",  # smithj@
        f"{first}{last}@",  # johnsmith@
    ]
    
    # Return the first valid pattern
    for pattern in patterns:
        if pattern and " " not in pattern:
            return pattern
    return ""

def extract_company_name(text: str) -> str:
    """Extract company name from text using simple patterns."""
    if not text:
        return ""
    
    # Clean text
    text = str(text).lower().strip()
    
    # Try to extract from common patterns
    patterns = [
        r'at (.+?)\.',  # e.g., "at Google."
        r'of (.+?)\.',  # e.g., "of Apple."
        r'with (.+?)\.',  # e.g., "with Facebook."
        r'from (.+?)\.',  # e.g., "from IBM."
        r'at (.+?)\s+company',  # e.g., "at Google company"
        r'at (.+?)\s+inc',  # e.g., "at Google Inc"
        r'at (.+?)\s+llc',  # e.g., "at Google LLC"
        r'at (.+?)\s+corp',  # e.g., "at Google Corp"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            company = match.group(1)
            # Remove common suffixes
            company = re.sub(r'\b(company|inc|llc|corp|limited|co|ltd)\b', '', company)
            return company.strip().title()
    
    # If no pattern match, try splitting by common separators
    parts = re.split(r'[\s\._-]+', text)
    if len(parts) > 1:
        return ' '.join(part.title() for part in parts if part)
    
    return text.title()

def extract_phone(text: str) -> str:
    """Extract the first valid phone number from the given text using regex."""
    if not isinstance(text, str):
        return ""
    phone_patterns = [
        r'\+?\d{1,3}?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # International/US
        r'\(\d{3}\)\s?\d{3}-\d{4}',  # (123) 456-7890
        r'\d{3}\.\d{3}\.\d{4}',  # 123.456.7890
        r'\d{3}-\d{3}-\d{4}',  # 123-456-7890
        r'\d{10}'  # 1234567890
    ]
    for pattern in phone_patterns:
        match = re.search(pattern, text)
        if match:
            phone = match.group(0)
            phone = re.sub(r'[^\d]', '', phone)
            if len(phone) == 10:
                return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
            elif len(phone) == 11 and phone[0] == '1':
                return f"({phone[1:4]}) {phone[4:7]}-{phone[7:]}"
            else:
                return phone
    return ""

def validate_linkedin(url: str) -> bool:
    """Validate if a string is a plausible LinkedIn profile URL."""
    if not isinstance(url, str):
        return False
    url = url.strip()
    pattern = r"^https?://(www\.)?linkedin\.com/in/[\w\-_%]+/?$"
    return re.match(pattern, url) is not None

def validate_email_format(email: str) -> bool:
    """Basic email format validation."""
    if not isinstance(email, str):
        return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def enrich_contact_list(input_file: str, output_file: str):
    """
    Enrich contact list by adding company names, emails, phones, and validation columns.
    Handles missing or variably named columns gracefully.
    """
    try:
        df = pd.read_csv(input_file)
        logger.info(f"Input file columns: {df.columns.tolist()}")

        # Flexible column mapping
        def safe_lower(val):
            try:
                return str(val).lower()
            except Exception:
                return ''

        def safe_str(val):
            try:
                return str(val)
            except Exception:
                return ''

        email_col = next((c for c in df.columns if safe_lower(c) == 'email'), None)
        company_col = next((c for c in df.columns if safe_lower(c) == 'company'), None)
        if not company_col:
            company_col = next((c for c in df.columns if 'company name' in safe_lower(c)), None)
        linkedin_col = next((c for c in df.columns if 'linkedin' in safe_lower(c)), None)
        first_name_col = next((c for c in df.columns if 'first name' in safe_lower(c)), None)
        last_name_col = next((c for c in df.columns if 'last name' in safe_lower(c)), None)
        full_name_col = next((c for c in df.columns if 'full name' in safe_lower(c)), None)

        # Add columns if missing
        if not email_col:
            df['Email'] = ''
            email_col = 'Email'
        if not company_col:
            df['Company'] = ''
            company_col = 'Company'
        if not linkedin_col:
            df['LinkedIn'] = ''
            linkedin_col = 'LinkedIn'
        if 'Phone' not in df.columns:
            df['Phone'] = ''
        if 'LinkedIn_Valid' not in df.columns:
            df['LinkedIn_Valid'] = ''
        if 'Email_Valid' not in df.columns:
            df['Email_Valid'] = ''
        df['Original_Email'] = df[email_col].copy() if email_col in df.columns else ''
        df['Original_Company'] = df[company_col].copy() if company_col in df.columns else ''

        # Identify possible phone and LinkedIn columns
        phone_columns = [col for col in df.columns if 'phone' in col.lower() or 'mobile' in col.lower() or 'contact' in col.lower()]
        linkedin_columns = [linkedin_col] if linkedin_col else []

        for idx, row in df.iterrows():
            # Determine first/last name
            first = safe_str(row.get(first_name_col, '')) if first_name_col else ''
            last = safe_str(row.get(last_name_col, '')) if last_name_col else ''
            if not first and not last and full_name_col:
                full = safe_str(row.get(full_name_col, ''))
                if full:
                    parts = full.split()
                    first = parts[0] if len(parts) > 0 else ''
                    last = parts[-1] if len(parts) > 1 else ''

            # Generate email if empty
            email = safe_str(row.get(email_col, '')) if email_col else ''
            if not email or not email.strip():
                email = generate_email(first, last)
                if email:
                    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
                    for domain in domains:
                        if email + domain not in df[email_col].values:
                            df.at[idx, email_col] = email + domain
                            break
            # Validate email
            email_to_check = df.at[idx, email_col] if email_col else ''
            df.at[idx, 'Email_Valid'] = validate_email_format(email_to_check)

            # Extract phone
            phone_found = ''
            for pcol in phone_columns:
                val = safe_str(row.get(pcol, ''))
                phone_found = extract_phone(val)
                if phone_found:
                    break
            df.at[idx, 'Phone'] = phone_found

            # Validate LinkedIn
            linkedin_found = False
            for lcol in linkedin_columns:
                val = safe_str(row.get(lcol, ''))
                if validate_linkedin(val):
                    linkedin_found = True
                    break
            df.at[idx, 'LinkedIn_Valid'] = linkedin_found

            # Extract company if empty
            company_val = safe_str(row.get(company_col, ''))
            if not company_val or not company_val.strip():
                # Try to extract from job title or fallback to names
                title_col = next((c for c in df.columns if 'title' in safe_lower(c)), None)
                company = extract_company_name(safe_str(row.get(title_col, ''))) if title_col else ''
                if company:
                    df.at[idx, company_col] = company
                else:
                    df.at[idx, company_col] = f"{first} {last}".strip()

        df.to_csv(output_file, index=False)
        logger.info(f"Successfully enriched contact list and saved to {output_file}")
    except Exception as e:
        logger.error(f"Error enriching contact list: {str(e)}")
        raise

import argparse
from pathlib import Path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich contact CSV with phone, email, LinkedIn validation.")
    parser.add_argument("input_file", help="Path to input CSV file")
    parser.add_argument("--output", help="Optional output file path")
    args = parser.parse_args()

    input_path = Path(args.input_file)
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.parent / f"{input_path.stem} - ENRICHED{input_path.suffix}"

    try:
        enrich_contact_list(str(input_path), str(output_path))
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
