import pandas as pd
import logging
import re
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def generate_email(first_name: str, last_name: str, domain: str) -> str:
    """Generate a professional email address from first name, last name, and domain."""
    if not first_name or not last_name or not domain:
        return ""
        
    first = first_name.lower().strip()
    last = last_name.lower().strip()
    domain = domain.lower().strip()
    
    # Generate multiple email patterns
    patterns = [
        f"{first}.{last}@{domain}",
        f"{first}_{last}@{domain}",
        f"{last}.{first}@{domain}",
        f"{first[0]}{last}@{domain}",
        f"{first}{last}@{domain}",
        f"{last}{first[0]}@{domain}",
        f"{first[0]}{last[0]}@{domain}",
        f"{last[0]}{first}@{domain}"
    ]
    
    # Return the first pattern that doesn't contain spaces
    for email in patterns:
        if " " not in email:
            return email
    return ""


def extract_company_name(source: str) -> Optional[str]:
    """Extract company name from various sources."""
    if pd.isna(source) or not isinstance(source, str):
        return None
    
    try:
        # Clean the source string
        source = str(source).strip().lower()
        
        # Try to extract from email format
        if '@' in source:
            domain = source.split('@')[-1]
            # Remove common domain extensions
            domain = re.sub(r'\.(com|org|net|edu|gov|io|biz|info|me|co|us|ca|uk|au|nz|jp|de|fr|it|es|nl|be|ch)$', '', domain)
            # Remove common subdomains
            domain = re.sub(r'^(www\.|mail\.|webmail\.|email\.|support\.|contact\.)', '', domain)
            # Replace special characters with spaces
            domain = re.sub(r'[-_]', ' ', domain)
            # Split by periods and other common separators
            company_parts = re.split(r'[\.\s]+', domain)
            # Capitalize words and filter out empty parts
            company_name = ' '.join([part.capitalize() for part in company_parts if part])
            if company_name.lower() not in ['gmail', 'yahoo', 'hotmail', 'outlook']:
                return company_name

        # Try to extract from title case format
        if re.match(r'^[A-Z][a-z]+(?: [A-Z][a-z]+)*$', source):
            return source

        # Try to extract from camel case format
        if re.match(r'^[A-Z][a-z]+[A-Z][a-z]+$', source):
            return ' '.join(re.findall(r'[A-Z][^A-Z]*', source))

        # Try to extract from snake case format
        if '_' in source:
            return ' '.join(part.capitalize() for part in source.split('_'))

        # Try to extract from kebab case format
        if '-' in source:
            return ' '.join(part.capitalize() for part in source.split('-'))

        # Last resort - just capitalize the words
        return ' '.join(part.capitalize() for part in source.split())

    except Exception as e:
        logger.error(f"Error extracting company name from {source}: {str(e)}")
        return None

def enrich_contact_list(input_file: str, output_file: str):
    """
    Enrich contact list by adding company names and emails to appropriate columns.
    """
    try:
        # Read the input CSV
        df = pd.read_csv(input_file)
        
        # Map column names from input file to our expected names
        column_map = {
            'First Name': 'first_name',
            'Last Name': 'last_name',
            'Company': 'Company',
            'Email': 'Email',
            'Title': 'Title'
        }
        
        # Rename columns to match our expected names
        df = df.rename(columns=column_map)
        
        # Create any missing columns
        for col in ['first_name', 'last_name', 'Company', 'Email', 'Title']:
            if col not in df.columns:
                df[col] = ''

        # First pass: Extract company names from any available source
        company_sources = ['Company', 'Email', 'Title']
        for source_col in company_sources:
            if source_col in df.columns:
                df['Company'] = df.apply(
                    lambda row: extract_company_name(row[source_col]) or row['Company'],
                    axis=1
                )

        # Second pass: Generate fallback emails from names
        df['Email'] = df.apply(
            lambda row: (
                # Try existing email first
                row['Email'] or 
                # Try generating from name and company
                generate_email(row['first_name'], row['last_name'], row['Company'].lower().replace(' ', '')) or
                # Try generating from name and title (if contains company info)
                generate_email(row['first_name'], row['last_name'], extract_company_name(row['Title']).lower().replace(' ', ''))
            ),
            axis=1
        )

        # Third pass: Use company name from email if not already set
        df['Company'] = df.apply(
            lambda row: (
                # Try existing company name first
                row['Company'] or 
                # Try extracting from email
                extract_company_name(row['Email']) or
                # Try extracting from title
                extract_company_name(row['Title'])
            ),
            axis=1
        )

        # Fourth pass: Generate company domain for emails
        df['Company'] = df.apply(
            lambda row: (
                # If we have a company name but no email, create a domain
                row['Company'] if row['Email'] else 
                row['Company'] + '.com' if row['Company'] else ''
            ),
            axis=1
        )

        # Clean up empty values
        df['Company'] = df['Company'].fillna('')
        df['Email'] = df['Email'].fillna('')
        
        # Map back to original column names before saving
        df = df.rename(columns={v: k for k, v in column_map.items()})

        # Save enriched file
        df.to_csv(output_file, index=False)
        logger.info(f"Successfully enriched contact list and saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Error enriching contact list: {str(e)}")
        raise

if __name__ == "__main__":
    # Define input and output files
    input_file = "G:\\My Drive\\Hirejoure.com\\Hirejourne.com - Master Contact List Consolidation - Enriched v2.csv"
    output_file = "output\\Hirejourne.com - Master Contact List Consolidation - Enriched v3.csv"
    
    try:
        # Read the input file to check its structure
        df = pd.read_csv(input_file)
        logger.info(f"Input file columns: {df.columns.tolist()}")
        logger.info(f"Number of rows: {len(df)}")
        
        # Show some statistics about empty values
        empty_stats = df[['Email', 'Company']].isna().sum()
        logger.info(f"Empty values before enrichment:\n{empty_stats}")
        
        enrich_contact_list(input_file, output_file)
        
        # Read the output file to verify changes
        df_enriched = pd.read_csv(output_file)
        empty_stats_after = df_enriched[['Email', 'Company']].isna().sum()
        logger.info(f"Empty values after enrichment:\n{empty_stats_after}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
