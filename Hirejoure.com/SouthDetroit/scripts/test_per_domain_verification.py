#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to demonstrate domain-specific email format verification.
This tests the enhanced system that verifies each domain individually.
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from email_pattern_filler import analyze_domain_specific_patterns, get_verified_email_pattern

def test_per_domain_verification():
    print("Per-Domain Email Format Verification Test")
    print("=" * 50)
    print("Testing the enhanced system that verifies each domain individually")
    print("instead of applying global assumptions.")
    print()
    
    # Test domains with known unique patterns
    test_cases = [
        {
            'company': 'Bureau Veritas',
            'domain': 'bureauveritas.com',
            'expected_us': 'First.Last@us.bureauveritas.com (98% of US contacts)',
            'expected_global': 'first.last@bureauveritas.com (69.5% globally)'
        },
        {
            'company': 'UL Solutions',
            'domain': 'ul.com',
            'expected_dominant': 'john.doe@ul.com (first.last@ul.com)',
            'expected_variations': ['j.doe@ul.com (f.last)', 'doe.john@ul.com (last.first)']
        },
        {
            'company': 'DNV',
            'domain': 'dnvgl.com',
            'expected': 'Domain-specific pattern (to be verified)'
        }
    ]
    
    for test_case in test_cases:
        company = test_case['company']
        domain = test_case['domain']
        
        print(f"Testing: {company} ({domain})")
        print("-" * 40)
        
        # Test domain-specific analysis
        print("1. Domain-Specific Pattern Analysis:")
        analysis = analyze_domain_specific_patterns(company, domain)
        
        if analysis['patterns_found']:
            print(f"   Patterns found: {analysis['patterns_found']}")
            print(f"   Dominant pattern: {analysis['dominant_pattern']}")
            print(f"   Confidence: {analysis['confidence']:.1f}%")
        else:
            print("   No specific patterns detected via search")
        
        if analysis['regional_domains']:
            print(f"   Regional domains detected: {list(analysis['regional_domains'].keys())}")
        
        print()
        
        # Test full verification
        print("2. Full Verification with Domain-Specific Logic:")
        verification_result = get_verified_email_pattern(company, domain)
        
        print(f"   Recommended domain: {verification_result['recommended_domain']}")
        print(f"   Recommended pattern: {verification_result['recommended_pattern']}")
        
        if verification_result['regional_domains']:
            print(f"   Regional options: {list(verification_result['regional_domains'].keys())}")
        
        print()
        print("   Expected (from user's Google searches):")
        for key, value in test_case.items():
            if key.startswith('expected'):
                print(f"   {key}: {value}")
        
        print()
        print("=" * 50)
        print()
    
    print("SUMMARY:")
    print("The enhanced system now performs domain-specific verification")
    print("instead of applying global 'first.last' assumptions to all companies.")
    print("Each domain gets individual analysis to find its unique patterns.")

if __name__ == "__main__":
    test_per_domain_verification()
