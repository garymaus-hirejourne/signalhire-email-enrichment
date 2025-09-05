#!/usr/bin/env python3
"""
Complete Wound Specialist Pipeline
Integrates scraping, SignalHire enrichment, and email pattern filling for 495 Beltway wound specialists.
"""

import os
import sys
import subprocess
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import argparse
import logging

# Add the project paths for imports
project_root = Path(__file__).parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "Hirejoure.com" / "SouthDetroit" / "scripts"))

try:
    from wound_specialist_scraper import WoundSpecialistScraper, BELTWAY_ZIP_CODES
    from signalhire_cloud_uploader import SignalHireCloudUploader
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure all required files are in the correct locations")
    sys.exit(1)

class WoundSpecialistPipeline:
    def __init__(self, output_dir="wound_specialist_campaign", signalhire_api_key=None, webhook_url=None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.signalhire_api_key = signalhire_api_key
        self.webhook_url = webhook_url
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging for the pipeline"""
        log_file = self.output_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def step1_scrape_specialists(self, test_mode=False):
        """Step 1: Scrape wound specialists from healthcare directories"""
        self.logger.info("STEP 1: Scraping wound care specialists...")
        
        scraper_output_dir = self.output_dir / "scraped_data"
        scraper = WoundSpecialistScraper(
            output_dir=scraper_output_dir,
            headless=True
        )
        
        # Use limited ZIP codes for testing
        zip_codes = BELTWAY_ZIP_CODES[:5] if test_mode else BELTWAY_ZIP_CODES
        
        results = scraper.run_full_scrape(
            zip_codes=zip_codes,
            include_hospitals=True
        )
        
        self.logger.info(f"Scraping complete: {results['total_specialists']} specialists found")
        return results
    
    def step2_enrich_with_signalhire(self, enrichment_file, use_cloud=True):
        """Step 2: Enrich data using SignalHire API"""
        self.logger.info("STEP 2: Enriching with SignalHire...")
        
        if not self.signalhire_api_key:
            self.logger.error("SignalHire API key not provided. Skipping SignalHire enrichment.")
            return enrichment_file
        
        if use_cloud and self.webhook_url:
            # Use cloud-based SignalHire processing
            self.logger.info("Using SignalHire cloud processing...")
            
            uploader = SignalHireCloudUploader(self.signalhire_api_key, self.webhook_url)
            result = uploader.process_csv_file(
                csv_file=str(enrichment_file),
                batch_size=25,  # Smaller batches for wound specialists
                max_rows=None
            )
            
            if result:
                self.logger.info("SignalHire cloud processing initiated")
                self.logger.info("Results will be available at your webhook endpoint")
                return enrichment_file  # Original file, results come via webhook
            else:
                self.logger.error("SignalHire cloud processing failed")
                return enrichment_file
        else:
            # Use local SignalHire API calls (would need implementation)
            self.logger.warning("Local SignalHire processing not implemented. Using cloud method.")
            return enrichment_file
    
    def step3_email_pattern_enrichment(self, input_file):
        """Step 3: Run email pattern filling using existing script"""
        self.logger.info("STEP 3: Running email pattern enrichment...")
        
        # Path to your existing email_pattern_filler.py
        email_script_path = project_root / "Hirejoure.com" / "SouthDetroit" / "scripts" / "email_pattern_filler.py"
        
        if not email_script_path.exists():
            self.logger.error(f"Email pattern filler script not found at: {email_script_path}")
            return input_file
        
        # Create output filename
        input_path = Path(input_file)
        output_file = input_path.parent / f"{input_path.stem}_ENRICHED{input_path.suffix}"
        
        # Run the email pattern filler script
        cmd = [
            sys.executable,
            str(email_script_path),
            str(input_file),
            str(output_file),
            "--fast",  # Use fast mode for better performance
            "--batch-size", "50"
        ]
        
        self.logger.info(f"Running command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1 hour timeout
            
            if result.returncode == 0:
                self.logger.info("Email pattern enrichment completed successfully")
                self.logger.info(f"Enriched file saved to: {output_file}")
                return output_file
            else:
                self.logger.error(f"Email pattern enrichment failed: {result.stderr}")
                return input_file
                
        except subprocess.TimeoutExpired:
            self.logger.error("Email pattern enrichment timed out")
            return input_file
        except Exception as e:
            self.logger.error(f"Error running email pattern enrichment: {e}")
            return input_file
    
    def step4_create_mail_merge_list(self, enriched_file):
        """Step 4: Create final mail merge list with proper formatting"""
        self.logger.info("STEP 4: Creating mail merge list...")
        
        try:
            # Load enriched data
            df = pd.read_csv(enriched_file)
            
            # Create mail merge format
            mail_merge_columns = [
                'First Name', 'Last Name', 'Full Name', 'Email', 'Phone',
                'Company Name', 'Job Title', 'Address', 'ZIP Code',
                'LinkedIn Profile', 'Company Domain', 'Source', 'Profile URL'
            ]
            
            # Ensure all columns exist
            for col in mail_merge_columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Select and reorder columns
            mail_merge_df = df[mail_merge_columns].copy()
            
            # Clean and format data
            mail_merge_df['Job Title'] = mail_merge_df['Job Title'].fillna('Wound Care Specialist')
            mail_merge_df['Full Name'] = mail_merge_df.apply(
                lambda row: f"{row['First Name']} {row['Last Name']}".strip() 
                if row['First Name'] or row['Last Name'] else row['Full Name'], axis=1
            )
            
            # Add campaign metadata
            mail_merge_df['Campaign'] = 'Wound Specialists 495 Beltway'
            mail_merge_df['Campaign Date'] = datetime.now().strftime('%Y-%m-%d')
            mail_merge_df['Geographic Area'] = '495 Beltway Massachusetts'
            
            # Save mail merge file
            mail_merge_file = self.output_dir / f"wound_specialists_mail_merge_{datetime.now().strftime('%Y%m%d')}.csv"
            mail_merge_df.to_csv(mail_merge_file, index=False)
            
            # Create summary statistics
            stats = {
                'total_contacts': len(mail_merge_df),
                'contacts_with_email': len(mail_merge_df[mail_merge_df['Email'].notna() & (mail_merge_df['Email'] != '')]),
                'contacts_with_phone': len(mail_merge_df[mail_merge_df['Phone'].notna() & (mail_merge_df['Phone'] != '')]),
                'unique_companies': mail_merge_df['Company Name'].nunique(),
                'sources': mail_merge_df['Source'].value_counts().to_dict()
            }
            
            self.logger.info(f"Mail merge list created: {mail_merge_file}")
            self.logger.info(f"Statistics: {json.dumps(stats, indent=2)}")
            
            return mail_merge_file, stats
            
        except Exception as e:
            self.logger.error(f"Error creating mail merge list: {e}")
            return None, None
    
    def run_complete_pipeline(self, test_mode=False, skip_signalhire=False):
        """Run the complete pipeline from scraping to mail merge"""
        self.logger.info("Starting complete wound specialist pipeline...")
        
        pipeline_start = datetime.now()
        
        try:
            # Step 1: Scrape specialists
            scrape_results = self.step1_scrape_specialists(test_mode=test_mode)
            if scrape_results['total_specialists'] == 0:
                self.logger.error("No specialists found during scraping. Aborting pipeline.")
                return None
            
            enrichment_file = scrape_results['enrichment_file']
            
            # Step 2: SignalHire enrichment (optional)
            if not skip_signalhire and self.signalhire_api_key and self.webhook_url:
                enrichment_file = self.step2_enrich_with_signalhire(enrichment_file)
                
                # Note: If using cloud SignalHire, you may need to wait for webhook results
                # before proceeding to step 3. For now, we'll continue with the original file.
                self.logger.info("SignalHire cloud processing initiated. Continuing with email pattern enrichment...")
            
            # Step 3: Email pattern enrichment
            enriched_file = self.step3_email_pattern_enrichment(enrichment_file)
            
            # Step 4: Create mail merge list
            mail_merge_file, stats = self.step4_create_mail_merge_list(enriched_file)
            
            pipeline_end = datetime.now()
            duration = pipeline_end - pipeline_start
            
            # Final results
            results = {
                'success': True,
                'duration': str(duration),
                'specialists_found': scrape_results['total_specialists'],
                'mail_merge_file': str(mail_merge_file) if mail_merge_file else None,
                'statistics': stats,
                'files_created': {
                    'raw_data': str(scrape_results['raw_file']),
                    'enrichment_input': str(scrape_results['enrichment_file']),
                    'enriched_data': str(enriched_file),
                    'mail_merge_list': str(mail_merge_file) if mail_merge_file else None
                }
            }
            
            self.logger.info("Pipeline completed successfully!")
            self.logger.info(f"Total duration: {duration}")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'duration': str(datetime.now() - pipeline_start)
            }

def main():
    parser = argparse.ArgumentParser(description='Complete wound specialist pipeline')
    parser.add_argument('--output-dir', default='wound_specialist_campaign', help='Output directory')
    parser.add_argument('--signalhire-api-key', help='SignalHire API key')
    parser.add_argument('--webhook-url', help='SignalHire webhook URL')
    parser.add_argument('--test-mode', action='store_true', help='Run in test mode (limited data)')
    parser.add_argument('--skip-signalhire', action='store_true', help='Skip SignalHire enrichment')
    
    args = parser.parse_args()
    
    # Get API credentials from environment if not provided
    api_key = args.signalhire_api_key or os.getenv('SIGNALHIRE_API_KEY')
    webhook_url = args.webhook_url or os.getenv('SIGNALHIRE_WEBHOOK_URL')
    
    if not args.skip_signalhire and not api_key:
        print("Warning: No SignalHire API key provided. SignalHire enrichment will be skipped.")
        print("Set SIGNALHIRE_API_KEY environment variable or use --signalhire-api-key")
    
    # Initialize and run pipeline
    pipeline = WoundSpecialistPipeline(
        output_dir=args.output_dir,
        signalhire_api_key=api_key,
        webhook_url=webhook_url
    )
    
    results = pipeline.run_complete_pipeline(
        test_mode=args.test_mode,
        skip_signalhire=args.skip_signalhire or not api_key
    )
    
    # Print results
    print("\n" + "="*80)
    print("WOUND SPECIALIST PIPELINE - FINAL RESULTS")
    print("="*80)
    
    if results and results.get('success'):
        print(f"✅ Pipeline completed successfully in {results['duration']}")
        print(f"📊 Specialists found: {results['specialists_found']}")
        print(f"📧 Mail merge file: {results['mail_merge_file']}")
        
        if results.get('statistics'):
            stats = results['statistics']
            print(f"📈 Contacts with email: {stats.get('contacts_with_email', 0)}")
            print(f"📞 Contacts with phone: {stats.get('contacts_with_phone', 0)}")
            print(f"🏢 Unique companies: {stats.get('unique_companies', 0)}")
        
        print("\n📁 Files created:")
        for file_type, file_path in results.get('files_created', {}).items():
            if file_path:
                print(f"  {file_type}: {file_path}")
        
        print("\n🚀 Next steps:")
        print("1. Review the mail merge file for accuracy")
        print("2. Import into your email marketing platform")
        print("3. Create targeted wound care specialist campaign")
        print("4. Monitor SignalHire webhook for additional enrichment results")
        
    else:
        print("❌ Pipeline failed")
        if results and results.get('error'):
            print(f"Error: {results['error']}")
    
    print("="*80)

if __name__ == "__main__":
    main()
