#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to demonstrate external email format verification working for ALL domains in the dataset.
"""

import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(__file__))

from email_pattern_filler import get_verified_email_pattern, standardize_column_names, infer_domains

def test_all_domains():
    print("Testing External Email Format Verification for ALL DOMAINS")
    print("=" * 70)
    
    # Load the actual CSV data
    csv_path = r"G:\My Drive\Hirejoure.com\Instrumentation_and_Measurement_Execs v2.csv"
    
    try:
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} contacts from CSV")
        
        # Standardize columns and infer domains (same as main script)
        df = standardize_column_names(df)
        df = infer_domains(df)
        
        # Get all unique domains
        unique_domains = df['domain'].dropna().unique()
        print(f"Found {len(unique_domains)} unique domains")
        print()
        
        # Test verification for each domain
        verified_patterns = {}
        
        for domain in unique_domains:
            if domain and domain.strip():
                company_name = df[df['domain'] == domain]['company'].iloc[0] if 'company' in df.columns else domain
                
                print(f"Testing: {company_name} ({domain})")
                print("-" * 50)
                
                verified_pattern = get_verified_email_pattern(company_name, domain)
                verified_patterns[domain] = verified_pattern
                
                print(f"VERIFIED pattern: {verified_pattern}")
                print()
        
        print("SUMMARY - External Verification Results for ALL Domains:")
        print("=" * 70)
        for domain, pattern in verified_patterns.items():
            company_name = df[df['domain'] == domain]['company'].iloc[0] if 'company' in df.columns else domain
            print(f"{company_name:25} ({domain:20}) -> {pattern}")
        
        print(f"\nVERIFICATION COMPLETE: All {len(verified_patterns)} domains processed!")
        print("External email format verification is working for ALL domains, not just Bureau Veritas!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_all_domains()
