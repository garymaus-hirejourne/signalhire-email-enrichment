#!/usr/bin/env python3
"""
Download SignalHire results.csv from Northflank webhook server
"""

import requests
import time
from pathlib import Path

def download_results(webhook_base_url, output_file="signalhire_results.csv", max_retries=10, retry_delay=30):
    """
    Download results.csv from SignalHire webhook server with retry logic.
    
    Args:
        webhook_base_url (str): Base webhook URL (without /signalhire/webhook)
        output_file (str): Local filename to save results
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Seconds to wait between retries
    """
    results_url = f"{webhook_base_url}/results.csv"
    output_path = Path(output_file)
    
    print(f"🔍 Checking for results at: {results_url}")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(results_url)
            
            if response.status_code == 200:
                # Success! Save the file
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                print(f"✅ Results downloaded successfully!")
                print(f"📁 Saved to: {output_path.absolute()}")
                print(f"📊 File size: {len(response.text)} characters")
                
                # Show first few lines
                lines = response.text.split('\n')[:5]
                print(f"📋 Preview (first 5 lines):")
                for i, line in enumerate(lines, 1):
                    print(f"  {i}: {line}")
                
                return str(output_path.absolute())
                
            elif response.status_code == 404:
                print(f"⏳ Attempt {attempt + 1}/{max_retries}: Results not ready yet (404)")
                if attempt < max_retries - 1:
                    print(f"   Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                else:
                    print("❌ Results file not found after all retries")
                    return None
            else:
                print(f"❌ Unexpected response: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error: {e}")
            if attempt < max_retries - 1:
                print(f"   Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                return None
    
    return None

if __name__ == "__main__":
    # Your Northflank webhook base URL
    webhook_base = "https://webhook--signalhire-webhook--jgdqh2mydks5.code.run"
    
    print("SignalHire Results Downloader")
    print("=" * 50)
    
    result_file = download_results(
        webhook_base_url=webhook_base,
        output_file="signalhire_results.csv",
        max_retries=10,
        retry_delay=30
    )
    
    if result_file:
        print(f"\n🎉 Download complete: {result_file}")
    else:
        print(f"\n❌ Download failed. Check if SignalHire processing is complete.")
        print(f"   You can manually check: {webhook_base}/results.csv")
