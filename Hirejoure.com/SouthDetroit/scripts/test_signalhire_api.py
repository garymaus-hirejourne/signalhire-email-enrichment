#!/usr/bin/env python
"""
Test SignalHire API connectivity and response format
"""

import requests
import json

# SignalHire API Configuration
SIGNALHIRE_API_KEY = "202.evaAyOWjUoheYEQ4Bb2XlSp0ZSzi"
SIGNALHIRE_ENDPOINT = "https://api.signalhire.com/v2/email-finder"

def test_signalhire_api():
    """Test SignalHire API with a sample contact"""
    
    print("Testing SignalHire API...")
    print(f"API Key: {SIGNALHIRE_API_KEY}")
    print(f"Endpoint: {SIGNALHIRE_ENDPOINT}")
    print("-" * 50)
    
    # Test parameters - using a common name/company combination
    test_params = {
        'first_name': 'John',
        'last_name': 'Smith', 
        'company_domain': 'microsoft.com',
        'api_key': SIGNALHIRE_API_KEY
    }
    
    print(f"Test Parameters:")
    for key, value in test_params.items():
        if key != 'api_key':
            print(f"  {key}: {value}")
    print()
    
    try:
        print("Making API request...")
        response = requests.get(SIGNALHIRE_ENDPOINT, params=test_params, timeout=15)
        
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print()
        
        if response.status_code == 200:
            try:
                data = response.json()
                print("Response JSON:")
                print(json.dumps(data, indent=2))
                
                # Check for expected fields
                if 'error' in data:
                    print(f"\n❌ API Error: {data['error']}")
                elif 'email' in data:
                    print(f"\n✅ Success! Found email: {data.get('email')}")
                    if data.get('phone'):
                        print(f"   Phone: {data.get('phone')}")
                    if data.get('social'):
                        print(f"   Social: {data.get('social')}")
                else:
                    print(f"\n⚠️  No email found, but API responded successfully")
                    
            except json.JSONDecodeError as e:
                print(f"❌ JSON Decode Error: {e}")
                print(f"Raw Response: {response.text[:500]}...")
                
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"Response Text: {response.text[:500]}...")
            
    except requests.exceptions.Timeout:
        print("❌ Request Timeout - API took too long to respond")
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Connection Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")

def test_alternative_endpoints():
    """Test alternative SignalHire API endpoints"""
    
    print("\n" + "="*50)
    print("Testing Alternative SignalHire Endpoints...")
    print("="*50)
    
    # Alternative endpoints to try
    alternative_endpoints = [
        "https://api.signalhire.com/v1/email-finder",
        "https://api.signalhire.com/email-finder", 
        "https://signalhire.com/api/v2/email-finder",
        "https://signalhire.com/api/email-finder"
    ]
    
    test_params = {
        'first_name': 'John',
        'last_name': 'Smith',
        'company_domain': 'microsoft.com',
        'api_key': SIGNALHIRE_API_KEY
    }
    
    for endpoint in alternative_endpoints:
        print(f"\nTesting: {endpoint}")
        try:
            response = requests.get(endpoint, params=test_params, timeout=10)
            print(f"  Status: {response.status_code}")
            if response.status_code != 404:
                print(f"  Response: {response.text[:100]}...")
        except Exception as e:
            print(f"  Error: {str(e)[:50]}...")

if __name__ == "__main__":
    test_signalhire_api()
    test_alternative_endpoints()
