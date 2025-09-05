#!/usr/bin/env python3
"""
SignalHire Cloud Uploader - Fixed Version
Uploads CSV contacts to SignalHire API with webhook callback for cloud processing.
Based on official SignalHire API documentation.
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
    
    def upload_contacts_batch(self, identifiers):
        """
        Upload a batch of identifiers to SignalHire API with webhook callback.
        
        Args:
            identifiers (list): List of LinkedIn URLs, emails, or phone numbers
            
        Returns:
            dict: API response
        """
        endpoint = urljoin(self.base_url, "candidate/search")
        
        payload = {
            "callbackUrl": self.webhook_url,
            "items": identifiers
        }
        
        print(f"Uploading {len(identifiers)} identifiers to SignalHire...")
        print(f"Webhook URL: {self.webhook_url}")
        
        try:
            response = requests.post(endpoint, headers=self.headers, json=payload)
            response.raise_for_status()
            
            result = response.json()
            print(f"✅ Upload successful: {result}")
            return result
            
        except requests.exceptions.HTTPError as e:
            print(f"❌ Upload failed: {e}")
            if hasattr(e.response, 'text'):
                print(f"Response: {e.response.text}")
            return None
        except Exception as e:
            print(f"❌ Upload error: {e}")
            return None
    
    def extract_identifiers(self, row):
        """
        Extract SignalHire identifiers (LinkedIn URLs, emails, phones) from CSV row.
        
        Args:
            row: Pandas Series representing a CSV row
            
        Returns:
            list: List of identifiers for SignalHire API
        """
        identifiers = []
        
        # Extract LinkedIn URL
        for col in ['LinkedIn Profile', 'LinkedIn', 'linkedin', 'li_url']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                linkedin_url = str(row[col]).strip()
                if 'linkedin.com' in linkedin_url:
                    identifiers.append(linkedin_url)
                break
        
        # Extract email
        for col in ['Email', 'email', 'email_address']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                email = str(row[col]).strip()
                if '@' in email:
                    identifiers.append(email)
                break
        
        # Extract phone
        for col in ['Phone', 'phone', 'phone_number', 'telephone']:
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
        
        print(f"📁 Processing CSV file: {csv_file}")
        
        # Load CSV
        df = pd.read_csv(csv_file)
        print(f"📊 Loaded {len(df)} records")
        print(f"📋 Columns: {', '.join(df.columns)}")
        
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
        
        print(f"🔍 Extracted {len(all_identifiers)} identifiers from {len(df)} records")
        
        if not all_identifiers:
            print("❌ No valid identifiers found in CSV")
            return None
        
        # Limit batch size to API maximum (100)
        batch_size = min(batch_size, 100)
        total_uploaded = 0
        total_batches = (len(all_identifiers) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(all_identifiers))
            batch_identifiers = all_identifiers[start_idx:end_idx]
            
            print(f"\n📦 Processing batch {batch_num + 1}/{total_batches} ({len(batch_identifiers)} identifiers)")
            
            # Upload batch
            result = self.upload_contacts_batch(batch_identifiers)
            
            if result:
                total_uploaded += len(batch_identifiers)
                print(f"✅ Batch {batch_num + 1} uploaded successfully")
            else:
                print(f"❌ Batch {batch_num + 1} failed")
            
            # Rate limiting
            if batch_num < total_batches - 1:
                print("⏳ Waiting 2 seconds before next batch...")
                time.sleep(2)
        
        summary = {
            "csv_file": str(csv_path),
            "total_rows": len(df),
            "total_identifiers": len(all_identifiers),
            "total_uploaded": total_uploaded,
            "batches_processed": total_batches,
            "webhook_url": self.webhook_url
        }
        
        print(f"\n🎉 Processing complete!")
        print(f"📊 Summary: {json.dumps(summary, indent=2)}")
        
        return summary

def main():
    parser = argparse.ArgumentParser(description='Upload CSV contacts to SignalHire API with cloud webhook')
    parser.add_argument('csv_file', help='Path to CSV file')
    parser.add_argument('--api-key', required=True, help='SignalHire API key')
    parser.add_argument('--webhook-url', required=True, help='Northflank webhook URL (e.g., https://your-app.northflank.app/signalhire/webhook)')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size (default: 50, max: 100)')
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
        print("\n🚀 Upload initiated! Check your Northflank webhook for results.")
        print(f"📁 Results will be saved to: {args.webhook_url.replace('/signalhire/webhook', '')}/data/results.csv")
    else:
        print("\n❌ Upload failed!")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Run with command line arguments
        main()
    else:
        # Show usage examples
        print("Run with command line arguments:")
        print("python signalhire_cloud_uploader_fixed.py <csv_file> --api-key <key> --webhook-url <url>")
