#!/usr/bin/env python3
"""
SignalHire API Status Checker
Diagnoses API authentication and endpoint availability issues
"""

import requests
import os
import json
from datetime import datetime

# API Configuration
API_KEY = os.getenv("SIGNALHIRE_API_KEY", "202.evaAyOWjUoheYEQ4Bb2XlSp0ZSzi")
BASE_URL = "https://www.signalhire.com/api/v1"

# Test endpoints
ENDPOINTS = {
    "search": f"{BASE_URL}/candidate/search",
    "requests": f"{BASE_URL}/requests",
    "request_detail": f"{BASE_URL}/requests/102798363",  # Test batch from memories
    "account": f"{BASE_URL}/account",
    "credits": f"{BASE_URL}/credits"
}

def check_endpoint(name, url, method="GET", payload=None):
    """Check a single API endpoint"""
    print(f"\n{'='*50}")
    print(f"Testing {name.upper()}: {url}")
    print(f"{'='*50}")
    
    headers = {"apikey": API_KEY}
    if payload:
        headers["Content-Type"] = "application/json"
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=10)
        else:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        # Check if response is HTML (indicates login page)
        content_type = response.headers.get('content-type', '').lower()
        is_html = 'text/html' in content_type
        
        if is_html:
            print("WARNING: Response is HTML (likely login page)")
            print(f"Response length: {len(response.text)} characters")
            print("First 500 characters:")
            print(response.text[:500])
            if "login" in response.text.lower() or "sign in" in response.text.lower():
                print("AUTHENTICATION REQUIRED: Login page detected")
        else:
            print("SUCCESS: Response appears to be API data (JSON/text)")
            try:
                json_data = response.json()
                print("JSON Response:")
                print(json.dumps(json_data, indent=2)[:1000])
            except:
                print("Response Text:")
                print(response.text[:500])
        
        return response.status_code, is_html, response.text
        
    except requests.exceptions.RequestException as e:
        print(f"FAILED: Request failed: {e}")
        return None, None, str(e)

def test_authentication():
    """Test basic authentication with account endpoint"""
    print(f"\n{'='*60}")
    print("SIGNALHIRE API STATUS CHECK")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API Key: {API_KEY[:10]}...{API_KEY[-10:] if len(API_KEY) > 20 else API_KEY}")
    print(f"{'='*60}")
    
    # Test each endpoint
    results = {}
    for name, url in ENDPOINTS.items():
        status, is_html, response = check_endpoint(name, url)
        results[name] = {
            'status_code': status,
            'is_html': is_html,
            'working': status == 200 and not is_html
        }
    
    # Test a small search request
    print(f"\n{'='*50}")
    print("Testing SEARCH with sample data")
    print(f"{'='*50}")
    
    test_payload = {
        "items": ["https://www.linkedin.com/in/test-profile"],
        "callbackUrl": "https://webhook--signalhire-webhook--jgdqh2mydks5.code.run/signalhire/webhook"
    }
    
    search_status, search_html, search_response = check_endpoint(
        "search_test", 
        ENDPOINTS["search"], 
        method="POST", 
        payload=test_payload
    )
    
    results["search_test"] = {
        'status_code': search_status,
        'is_html': search_html,
        'working': search_status == 200 and not search_html
    }
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    
    working_endpoints = sum(1 for r in results.values() if r['working'])
    total_endpoints = len(results)
    
    print(f"Working endpoints: {working_endpoints}/{total_endpoints}")
    
    for name, result in results.items():
        status = "WORKING" if result['working'] else "FAILED"
        if result['is_html']:
            status += " (HTML/Login page)"
        elif result['status_code'] and result['status_code'] != 200:
            status += f" (HTTP {result['status_code']})"
        print(f"{name:15}: {status}")
    
    # Recommendations
    print(f"\n{'='*60}")
    print("RECOMMENDATIONS")
    print(f"{'='*60}")
    
    if working_endpoints == 0:
        print("CRITICAL: All endpoints failing")
        if any(r['is_html'] for r in results.values()):
            print("   - HTML responses indicate authentication issues")
            print("   - Contact SignalHire support for API key renewal")
            print("   - Check if account subscription is active")
        print("   - Consider using existing webhook data (3,144 records)")
        print("   - Wait for SignalHire system recovery")
    elif working_endpoints < total_endpoints:
        print("PARTIAL: Some endpoints working")
        print("   - Check specific endpoint issues above")
        print("   - May be temporary API issues")
    else:
        print("SUCCESS: All endpoints working normally")
        print("   - API authentication is functional")
        print("   - Ready for new enrichment requests")

if __name__ == "__main__":
    test_authentication()
