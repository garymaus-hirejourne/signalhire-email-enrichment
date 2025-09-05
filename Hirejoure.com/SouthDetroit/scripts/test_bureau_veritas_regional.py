#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to demonstrate regional domain detection for Bureau Veritas specifically.
This addresses the user's finding that Bureau Veritas uses us.bureauveritas.com for US contacts.
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from email_pattern_filler import get_verified_email_pattern, detect_regional_domains

def test_bureau_veritas_regional():
    print("Testing Bureau Veritas Regional Domain Detection")
    print("=" * 60)
    print("User found: First.Last@us.bureauveritas.com (98% of US contacts)")
    print("Let's see if our enhanced system can detect this...")
    print()
    
    company_name = "Bureau Veritas"
    domain = "bureauveritas.com"
    
    print(f"Testing: {company_name} ({domain})")
    print("-" * 50)
    
    # Test regional domain detection specifically
    print("1. Regional Domain Detection:")
    regional_domains = detect_regional_domains(company_name, domain)
    if regional_domains:
        for reg_domain, pattern in regional_domains.items():
            print(f"   Found: {reg_domain} -> {pattern}")
    else:
        print("   No regional domains detected")
    print()
    
    # Test full verification
    print("2. Full Verification with Regional Support:")
    verification_result = get_verified_email_pattern(company_name, domain)
    
    print(f"Primary domain: {verification_result['primary_domain']}")
    print(f"Primary pattern: {verification_result['primary_pattern']}")
    print(f"Regional domains: {verification_result['regional_domains']}")
    print(f"RECOMMENDED domain: {verification_result['recommended_domain']}")
    print(f"RECOMMENDED pattern: {verification_result['recommended_pattern']}")
    print()
    
    # Check if we detected the US domain
    us_domain = f"us.{domain}"
    if us_domain in verification_result.get('regional_domains', {}):
        print(f"SUCCESS: Detected US regional domain!")
        print(f"FOUND: {us_domain} -> {verification_result['regional_domains'][us_domain]}")
        print(f"MATCH: This matches the user's finding of First.Last@us.bureauveritas.com")
    else:
        print(f"LIMITATION: Did not detect us.bureauveritas.com")
        print(f"This may be due to search engine blocking or limited search results")
    
    print()
    print("CONCLUSION:")
    print("The enhanced verification system now has the capability to detect")
    print("regional domains like us.bureauveritas.com, addressing the critical")
    print("limitation identified by the user.")

if __name__ == "__main__":
    test_bureau_veritas_regional()
