import pandas as pd
import os
from pathlib import Path
import shutil
import logging
import time
from typing import Dict, List, Optional

# Add parent directory to Python path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from scripts.south_detroit_phase1_team_scraper_debug import ContactScraper
from scripts.process_input_csv import clean_company_name, clean_url

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Constants
BATCH_SIZE = 10  # Number of companies to process in each batch
MAX_RETRIES = 3  # Number of retries for failed companies
RETRY_DELAY = 30  # Seconds to wait between retries

# Define local paths
PROJECT_ROOT = Path(__file__).parent.parent
input_file = PROJECT_ROOT / "input" / "NY_PE_Company_List.csv"
output_base = PROJECT_ROOT / "output"
output_csv = output_base / "team_overview.csv"
backup_dir = output_base / "backups"


def ensure_directories():
    """Ensure all required directories exist"""
    for path in [output_base, output_base / "csv", output_base / "people", backup_dir]:
        path.mkdir(parents=True, exist_ok=True)


def backup_results(df: pd.DataFrame, reason: str):
    """Create a backup of the current results"""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir / f"backup_{timestamp}_{reason}.csv"
    df.to_csv(backup_file, index=False)
    logger.info(f"Created backup: {backup_file}")


def validate_input(df: pd.DataFrame) -> bool:
    """Validate input CSV format"""
    required_columns = ["Company", "URL"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False

    # Check for empty values
    empty_rows = df[required_columns].isna().any(axis=1)
    if empty_rows.any():
        logger.warning(f"Found {empty_rows.sum()} rows with missing required data")
        df = df[~empty_rows]

    return True


def process_company_batch(
    scraper: ContactScraper, companies: List[Dict], results: List[Dict]
) -> List[Dict]:
    """Process a batch of companies with retry logic"""
    for company_data in companies:
        company = company_data["Company"]
        url = company_data["URL"]

        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Processing {company} (attempt {attempt + 1})")

                # Scrape the company page
                new_contacts = scraper.scrape_team_page(company, url)
                results.extend(new_contacts)

                # Save intermediate results
                df = pd.DataFrame(results)
                df.to_csv(output_csv, index=False)
                logger.info(f"Saved {len(df)} total contacts")

                # Create backup after successful batch
                backup_results(df, "successful_batch")

                break  # Success, break out of retry loop

            except Exception as e:
                logger.error(f"Error processing {company} (attempt {attempt + 1}): {e}")
                if attempt == MAX_RETRIES - 1:  # Last attempt
                    logger.error(
                        f"Failed to process {company} after {MAX_RETRIES} attempts"
                    )
                    # Save failed company data for later review
                    failed_file = (
                        backup_dir / f"failed_{company.replace(' ', '_')}.json"
                    )
                    with open(failed_file, "w", encoding="utf-8") as f:
                        json.dump(company_data, f, indent=2)
                    continue

                # Wait before retry
                time.sleep(RETRY_DELAY)

    return results


def process_single_file(
    input_file: Path, output_file: Path, hunter_api_key: str, serpapi_key: str
) -> None:
    """Process a single CSV file"""
    logger.info(f"Processing {input_file}...")

    try:
        # Read input CSV
        df = pd.read_csv(input_file)

        # Initialize scraper with API keys
        scraper = ContactScraper(hunter_api_key=hunter_api_key, serpapi_key=serpapi_key)

        # Process each row
        results = []
        for _, row in df.iterrows():
            company = row["Company"]
            url = row["URL"]

            try:
                logger.info(f"Scraping {company} ({url})...")
                contacts = scraper.scrape_team_page(company, url)
                results.extend(contacts)

                # Create backup after successful batch
                df = pd.DataFrame(results)
                backup_results(df, f"batch_{i//BATCH_SIZE + 1}_{input_file.stem}")

            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                # Create error backup
                backup_results(df, f"error_{input_file.stem}")
                continue

        # Final save
        if results:
            df = pd.DataFrame(results)
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(df)} contacts to {output_file}")

            # Copy to subdirectories
            for sub in ["csv", "people"]:
                out_path = output_base / sub / output_file.name
                shutil.copy(output_file, out_path)
                logger.info(f"Copied to: {out_path}")

        else:
            logger.warning(f"No data to save for {input_file}")

    except Exception as e:
        logger.error(f"Error processing {input_file}: {e}")
        # Create final error backup
        if "df" in locals():
            backup_results(df, f"final_error_{input_file.stem}")
        raise


def main(hunter_api_key: str, serpapi_key: str):
    """Main pipeline execution"""
    ensure_directories()
    processed_dir = output_base / "processed"

    try:
        logger.info("API keys loaded successfully")

        # Process each input file
        for input_file in processed_dir.glob("processed_*.csv"):
            output_file = output_base / f"{input_file.stem}.csv"
            try:
                process_single_file(
                    input_file, output_file, hunter_api_key, serpapi_key
                )
            except Exception as e:
                logger.error(f"Failed to process {input_file}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise


if __name__ == "__main__":
    # Load API keys
    import os
    from dotenv import load_dotenv

    # Load .env file from project root
    load_dotenv(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env"
        )
    )

    # Get API keys
    hunter_api_key = os.getenv("HUNTER_API_KEY")
    serpapi_key = os.getenv("SERPAPI_KEY")

    if not hunter_api_key or not serpapi_key:
        raise ValueError("API keys not found in environment variables")

    # --- CUSTOM: Directly process the user's file (undo after run) ---
    input_file = Path(r"G:/My Drive/Hirejoure.com/Hirejourne.com - MA College Career Professionals.csv")
    output_file = Path(r"G:/My Drive/Hirejoure.com/Hirejourne_ma_enriched.csv")
    process_single_file(input_file, output_file, hunter_api_key, serpapi_key)
    # --- END CUSTOM ---
    # To revert: comment above and uncomment below
    # main(hunter_api_key, serpapi_key)
