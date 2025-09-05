#!/usr/bin/env python3
"""
SignalHire Cloud Uploader
Uploads CSV contacts to SignalHire API with webhook callback for cloud processing.
Designed to work with Northflank-hosted webhook receiver.
"""

import pandas as pd
import requests
import json
import time
import os
from pathlib import Path
import argparse
from urllib.parse import urljoin

class SignalHireCloudUploader:
    def __init__(self, api_key, webhook_url):
        self.api_key = api_key
        self.webhook_url = webhook_url
        self.base_url = "https://www.signalhire.com/api/v1/"
        self.headers = {
            "Content-Type": "application/json",
            "apikey": api_key
        }
        # Adaptive batch configuration based on dataset size
        self.batch_config = {
            'small': {'threshold': 200, 'batch_size': 50, 'delay': 2},
            'medium': {'threshold': 1000, 'batch_size': 75, 'delay': 5}, 
            'large': {'threshold': float('inf'), 'batch_size': 100, 'delay': 12}
        }
    
    def determine_batch_strategy(self, total_records):
        """
        Determine optimal batch strategy based on dataset size.
        
        Args:
            total_records (int): Total number of records to process
            
        Returns:
            dict: Batch configuration with size and delay settings
        """
        if total_records <= self.batch_config['small']['threshold']:
            strategy = 'small'
            print(f"Small dataset detected ({total_records} records) - Using fast processing")
        elif total_records <= self.batch_config['medium']['threshold']:
            strategy = 'medium'
            print(f"Medium dataset detected ({total_records} records) - Using balanced processing")
        else:
            strategy = 'large'
            print(f"Large dataset detected ({total_records} records) - Using conservative processing")
        
        config = self.batch_config[strategy]
        print(f"Strategy: {strategy} | Batch size: {config['batch_size']} | Delay: {config['delay']}s")
        return config
    
    def upload_contacts_batch(self, contacts_data, batch_id=None):
        """
        Upload a batch of contacts to SignalHire API with webhook callback.
        
        Args:
            contacts_data (list): List of contact identifiers (LinkedIn URLs, emails, phones)
            batch_id (str): Optional batch identifier
            
        Returns:
            dict: API response
        """
        endpoint = urljoin(self.base_url, "candidate/search")
        
        payload = {
            "callbackUrl": self.webhook_url,
            "items": contacts_data
        }
        
        print(f"Uploading {len(contacts_data)} contacts to SignalHire...")
        print(f"Webhook URL: {self.webhook_url}")
        
        try:
            response = requests.post(endpoint, headers=self.headers, json=payload)
            response.raise_for_status()
            
            result = response.json()
            print(f"Upload successful: {result}")
            return result
            
        except requests.exceptions.RequestException as e:
            print(f"Upload failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            return None
    
    def prepare_contact_from_row(self, row, row_index):
        """
        Convert CSV row to SignalHire contact format.
        
        Args:
            row (pandas.Series): CSV row data
            row_index (int): Row index for unique identification
            
        Returns:
            dict: Contact in SignalHire format
        """
        # Map common CSV columns to SignalHire format
        contact = {
            "item": f"contact_{row_index}",  # Unique identifier
        }
        
        # Map first name
        for col in ['First Name', 'FirstName', 'first_name', 'fname']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                contact["firstName"] = str(row[col]).strip()
                break
        
        # Map last name
        for col in ['Last Name', 'LastName', 'last_name', 'lname']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                contact["lastName"] = str(row[col]).strip()
                break
        
        # Map full name if first/last not available
        if "firstName" not in contact and "lastName" not in contact:
            for col in ['Full Name', 'FullName', 'full_name', 'name']:
                if col in row and pd.notna(row[col]) and str(row[col]).strip():
                    full_name = str(row[col]).strip()
                    name_parts = full_name.split()
                    if len(name_parts) >= 2:
                        contact["firstName"] = name_parts[0]
                        contact["lastName"] = " ".join(name_parts[1:])
                    elif len(name_parts) == 1:
                        contact["firstName"] = name_parts[0]
                    break
        
        # Map company
        for col in ['Company Name', 'Company', 'company_name', 'organization']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                contact["company"] = str(row[col]).strip()
                break
        
        # Map domain
        for col in ['Company Domain', 'Domain', 'domain', 'website']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                domain = str(row[col]).strip()
                # Clean domain format
                domain = domain.replace('https://', '').replace('http://', '').replace('www.', '')
                if '/' in domain:
                    domain = domain.split('/')[0]
                contact["domain"] = domain
                break
        
        # Map LinkedIn
        for col in ['LinkedIn Profile', 'LinkedIn', 'linkedin', 'li_url', 'LinkedIn URL']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                contact["linkedinUrl"] = str(row[col]).strip()
                break
        
        # Map job title
        for col in ['Job Title', 'Title', 'job_title', 'position']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                contact["title"] = str(row[col]).strip()
                break
        
        # Map location
        for col in ['Location', 'location', 'city', 'address']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                contact["location"] = str(row[col]).strip()
                break
        
        return contact
    
    def extract_identifiers(self, row):
        """
        Extract identifiers from CSV row for SignalHire API.
        
        Args:
            row (pandas.Series): CSV row data
            
        Returns:
            list: List of identifiers (LinkedIn URLs, emails, phones)
        """
        identifiers = []
        
        # Extract LinkedIn URL
        for col in ['LinkedIn Profile', 'LinkedIn', 'linkedin', 'li_url', 'LinkedIn URL']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                linkedin_url = str(row[col]).strip()
                if 'linkedin.com' in linkedin_url:
                    identifiers.append(linkedin_url)
                break
        
        # Extract emails
        for col in ['Email', 'email', 'work_email', 'personal_email']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                email = str(row[col]).strip()
                if '@' in email:
                    identifiers.append(email)
                break
        
        # Extract phone numbers
        for col in ['Phone', 'phone', 'mobile', 'work_phone']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                phone = str(row[col]).strip()
                if phone and len(phone) > 5:  # Basic phone validation
                    identifiers.append(phone)
                break
        
        return identifiers

    def process_csv_file(self, csv_file, batch_size=50, start_row=0, max_rows=None):
        """
        Process CSV file and upload to SignalHire in batches.
        
        Args:
            csv_file (str): Path to CSV file
            batch_size (int): Number of identifiers per batch (max 100)
            start_row (int): Starting row (for resuming)
            max_rows (int): Maximum rows to process
            
        Returns:
            dict: Processing summary
        """
        csv_path = Path(csv_file)
        if not csv_path.exists():
            print(f"❌ CSV file not found: {csv_file}")
            return None
        
        print(f"Processing CSV file: {csv_file}")
        
        # Load CSV
        df = pd.read_csv(csv_file)
        print(f"Loaded {len(df)} records")
        print(f"Columns: {', '.join(df.columns)}")
        
        # Apply row limits
        if max_rows:
            df = df.head(max_rows)
        if start_row > 0:
            df = df.iloc[start_row:]
        
        # Extract all identifiers from CSV
        all_identifiers = []
        for _, row in df.iterrows():
            identifiers = self.extract_identifiers(row)
            all_identifiers.extend(identifiers)
        
        print(f"Extracted {len(all_identifiers)} identifiers from {len(df)} records")
        
        if not all_identifiers:
            print("No valid identifiers found in CSV")
            return None
        
        # Determine adaptive batch strategy based on dataset size
        batch_strategy = self.determine_batch_strategy(len(df))
        
        # Use adaptive batch size or user override, limited to API maximum (100)
        adaptive_batch_size = batch_strategy['batch_size']
        batch_size = min(batch_size if batch_size != 100 else adaptive_batch_size, 100)
        delay_seconds = batch_strategy['delay']
        
        total_uploaded = 0
        total_batches = (len(all_identifiers) + batch_size - 1) // batch_size
        
        print(f"\nBatch Configuration:")
        print(f"- Records to process: {len(df)}")
        print(f"- Identifiers extracted: {len(all_identifiers)}")
        print(f"- Batch size: {batch_size}")
        print(f"- Total batches: {total_batches}")
        print(f"- Delay between batches: {delay_seconds}s")
        print(f"- Estimated total time: {(total_batches * delay_seconds) / 60:.1f} minutes")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(all_identifiers))
            batch_identifiers = all_identifiers[start_idx:end_idx]
            
            print(f"\nProcessing batch {batch_num + 1}/{total_batches} ({len(batch_identifiers)} identifiers)")
            
            # Upload batch
            result = self.upload_contacts_batch(batch_identifiers)
            
            if result:
                total_uploaded += len(batch_identifiers)
                print(f"Batch {batch_num + 1} uploaded successfully")
            else:
                print(f"Batch {batch_num + 1} failed")
            
            # Adaptive rate limiting based on dataset size
            if batch_num < total_batches - 1:
                print(f"Waiting {delay_seconds} seconds before next batch...")
                time.sleep(delay_seconds)
        
        summary = {
            "csv_file": str(csv_path),
            "total_rows": len(df),
            "total_uploaded": total_uploaded,
            "batches_processed": total_batches,
            "webhook_url": self.webhook_url
        }
        
        print(f"\nProcessing complete!")
        print(f"📊 Summary: {json.dumps(summary, indent=2)}")
        
        return summary

def main():
    parser = argparse.ArgumentParser(description='Upload CSV contacts to SignalHire API with cloud webhook')
    parser.add_argument('csv_file', help='Path to CSV file')
    parser.add_argument('--api-key', required=True, help='SignalHire API key')
    parser.add_argument('--webhook-url', required=True, help='Northflank webhook URL (e.g., https://your-app.northflank.app/signalhire/webhook)')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size (default: 50)')
    parser.add_argument('--start-row', type=int, default=0, help='Starting row (for resuming)')
    parser.add_argument('--max-rows', type=int, help='Maximum rows to process')
    
    args = parser.parse_args()
    
    uploader = SignalHireCloudUploader(args.api_key, args.webhook_url)
    result = uploader.process_csv_file(
        args.csv_file,
        batch_size=args.batch_size,
        start_row=args.start_row,
        max_rows=args.max_rows
    )
    
    if result:
        print("\nUpload initiated! Check your Northflank webhook for results.")
        print(f"Results will be saved to: {args.webhook_url.replace('/signalhire/webhook', '')}/data/results.csv")
    else:
        print("\nUpload failed!")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Run with command line arguments
        main()
    else:
        # Show usage examples
        example_file = r"G:\My Drive\Hirejoure.com\Dyer, Bryan\Top 100 For-Profit Universities\NEW.csv"
        
        if Path(example_file).exists():
            print("SignalHire Cloud Uploader")
            print("=" * 50)
            print("Example usage:")
            print(f"python signalhire_cloud_uploader.py \"{example_file}\" \\")
            print("  --api-key YOUR_SIGNALHIRE_API_KEY \\")
            print("  --webhook-url https://your-app.northflank.app/signalhire/webhook \\")
            print("  --batch-size 50 \\")
            print("  --max-rows 100")
            print("\nSet your API key and webhook URL to run!")
        else:
            print("Run with command line arguments:")
            print("python signalhire_cloud_uploader.py <csv_file> --api-key <key> --webhook-url <url>")
