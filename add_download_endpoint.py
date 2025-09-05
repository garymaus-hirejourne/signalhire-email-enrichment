#!/usr/bin/env python3
"""
Temporary script to add download endpoint to running Flask app
"""

import requests
import json

def check_current_endpoints():
    """Check what endpoints are currently available"""
    try:
        response = requests.get("https://webhook--signalhire-webhook--jgdqh2mydks5.code.run/")
        print("Current endpoints:", response.json())
        return response.json()
    except Exception as e:
        print(f"Error checking endpoints: {e}")
        return None

def test_download_endpoint():
    """Test if the download endpoint works"""
    try:
        response = requests.get("https://webhook--signalhire-webhook--jgdqh2mydks5.code.run/results.csv")
        if response.status_code == 200:
            print("✅ Download endpoint working!")
            with open("signalhire_results_downloaded.csv", "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"📁 Results saved to: signalhire_results_downloaded.csv")
            print(f"📊 Content preview:")
            lines = response.text.split('\n')[:10]
            for i, line in enumerate(lines, 1):
                print(f"  {i}: {line}")
            return True
        else:
            print(f"❌ Download failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error testing download: {e}")
        return False

if __name__ == "__main__":
    print("SignalHire Results Download Test")
    print("=" * 50)
    
    # Check current endpoints
    endpoints = check_current_endpoints()
    
    # Test download
    success = test_download_endpoint()
    
    if not success:
        print("\n💡 To enable download:")
        print("1. Redeploy your Flask app with the updated code")
        print("2. Or access via Northflank terminal: cat /data/results.csv")
