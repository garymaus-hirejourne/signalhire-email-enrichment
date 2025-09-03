#!/usr/bin/env python3
"""
Domain Extractor Script
Extracts domain names from CSV files containing URLs and outputs them as comma-separated text file.
Removes http://, https://, www., and any path/query parameters.
"""

import pandas as pd
import re
from pathlib import Path
from urllib.parse import urlparse
import argparse

def extract_domain_from_url(url):
    """
    Extract clean domain name from URL.
    Removes http://, https://, www., and any path/query parameters.
    
    Args:
        url (str): URL to extract domain from
        
    Returns:
        str: Clean domain name or None if invalid
    """
    if pd.isna(url) or not url:
        return None
    
    # Convert to string and strip whitespace
    url = str(url).strip()
    
    # Add protocol if missing for proper parsing
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    try:
        # Parse the URL
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        
        # Remove any port numbers
        if ':' in domain:
            domain = domain.split(':')[0]
        
        return domain if domain else None
        
    except Exception as e:
        print(f"Error parsing URL '{url}': {e}")
        return None

def find_url_columns(df):
    """
    Find columns that likely contain URLs.
    
    Args:
        df (pandas.DataFrame): DataFrame to search
        
    Returns:
        list: List of column names that likely contain URLs
    """
    url_columns = []
    
    for col in df.columns:
        col_lower = str(col).lower()
        # Check column name
        if any(keyword in col_lower for keyword in ['url', 'website', 'domain', 'link', 'site']):
            url_columns.append(col)
            continue
        
        # Check sample values in column
        sample_values = df[col].dropna().head(5)
        url_count = 0
        for value in sample_values:
            if pd.isna(value):
                continue
            value_str = str(value).lower()
            if any(pattern in value_str for pattern in ['http://', 'https://', 'www.', '.com', '.org', '.net']):
                url_count += 1
        
        # If more than half the sample values look like URLs, include this column
        if len(sample_values) > 0 and url_count / len(sample_values) >= 0.5:
            url_columns.append(col)
    
    return url_columns

def extract_domains_from_csv(csv_file, output_file=None):
    """
    Extract domains from CSV file and save as comma-separated text file.
    
    Args:
        csv_file (str): Path to input CSV file
        output_file (str): Path to output text file (optional)
    """
    csv_path = Path(csv_file)
    
    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_file}")
        return
    
    # Set default output filename
    if output_file is None:
        output_file = csv_path.parent / f"{csv_path.stem}_domains.txt"
    else:
        output_file = Path(output_file)
    
    print(f"Reading CSV file: {csv_file}")
    
    try:
        # Try different encodings
        try:
            df = pd.read_csv(csv_path, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(csv_path, encoding='latin-1')
            except UnicodeDecodeError:
                df = pd.read_csv(csv_path, encoding='cp1252')
        
        print(f"Loaded {len(df)} records with columns: {', '.join(df.columns)}")
        
        # Find URL columns
        url_columns = find_url_columns(df)
        
        if not url_columns:
            print("No URL columns found in the CSV file.")
            print("Looking for columns containing: url, website, domain, link, site")
            print("Or columns with values containing: http://, https://, www., .com, .org, .net")
            return
        
        print(f"Found URL columns: {', '.join(url_columns)}")
        
        # Extract domains from all URL columns
        all_domains = set()
        
        for col in url_columns:
            print(f"\nProcessing column: {col}")
            urls = df[col].dropna()
            
            for url in urls:
                domain = extract_domain_from_url(url)
                if domain:
                    all_domains.add(domain)
                    print(f"  {url} -> {domain}")
        
        # Convert to sorted list for consistent output
        domains_list = sorted(list(all_domains))
        
        if not domains_list:
            print("No valid domains found in the CSV file.")
            return
        
        # Create comma-separated string
        domains_text = ', '.join(domains_list)
        
        # Save to text file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(domains_text)
        
        print(f"\nSUCCESS!")
        print(f"Extracted {len(domains_list)} unique domains")
        print(f"Output saved to: {output_file}")
        print(f"\nDomains found:")
        for i, domain in enumerate(domains_list, 1):
            print(f"  {i}. {domain}")
        
        print(f"\nText file content preview:")
        print(f"{domains_text[:200]}{'...' if len(domains_text) > 200 else ''}")
        
        return output_file
        
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        return None

def main():
    """Main function with command line argument parsing."""
    parser = argparse.ArgumentParser(description='Extract domains from CSV files')
    parser.add_argument('csv_file', help='Path to input CSV file')
    parser.add_argument('-o', '--output', help='Path to output text file (optional)')
    
    args = parser.parse_args()
    
    result = extract_domains_from_csv(args.csv_file, args.output)
    
    if result:
        print(f"\nDomain extraction completed successfully!")
    else:
        print(f"\nDomain extraction failed.")

if __name__ == "__main__":
    # If run directly, process the specified university domains file
    example_file = r"G:\My Drive\Hirejoure.com\Dyer, Bryan\Top 100 For-Profit Universities\Top 11-92 University Domains.csv"
    
    if Path(example_file).exists():
        print("Domain Extractor Script")
        print("=" * 50)
        print("Processing university domains file...")
        extract_domains_from_csv(example_file)
    else:
        print("No example file found. Use command line arguments:")
        print("python extract_domains.py <csv_file> [-o output_file]")
