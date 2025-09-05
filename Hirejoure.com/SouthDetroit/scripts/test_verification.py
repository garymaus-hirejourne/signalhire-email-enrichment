#!/usr/bin/env python3
"""
Test script to verify external email format verification functions work correctly.
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from email_pattern_filler import get_verified_email_pattern, query_hunter_io, get_known_pattern, google_search_email_pattern

def test_verification():
    print("Testing external email format verification functions...")
    print("=" * 60)
    
    # Test domains from our CSV
    test_domains = [
        ("Bureau Veritas", "bureauveritas.com"),
        ("UL", "ul.com"),
        ("DNV GL", "dnvgl.com"),
        ("TÜV SÜD", "tuvsud.com"),
        ("Mistras Group", "mistrasgroup.com")
    ]
    
    for company_name, domain in test_domains:
        print(f"\nTesting {company_name} ({domain}):")
        print("-" * 40)
        
        # Test individual functions
        print("1. Hunter.io API:")
        hunter_result = query_hunter_io(domain)
        print(f"   Result: {hunter_result}")
        
        print("2. Known patterns database:")
        known_result = get_known_pattern(domain)
        print(f"   Result: {known_result}")
        
        print("3. Google search:")
        google_result = google_search_email_pattern(company_name, domain)
        print(f"   Result: {google_result}")
        
        print("4. Combined verification:")
        final_result = get_verified_email_pattern(company_name, domain)
        print(f"   Final pattern: {final_result}")
        
        print()

if __name__ == "__main__":
    test_verification()
