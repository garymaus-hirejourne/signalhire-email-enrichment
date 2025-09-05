import pandas as pd
import os
from pathlib import Path
import shutil
import logging
import time
from typing import Dict, List, Optional
from tqdm import tqdm
import argparse
import concurrent.futures

# Constants
BATCH_SIZE = 10  # Number of companies to process in each batch
MAX_RETRIES = 3  # Number of retries for failed companies
RETRY_DELAY = 30  # Seconds to wait between retries
MAX_BACKOFF = 300  # Maximum backoff time in seconds
MIN_REQUEST_INTERVAL = 1.5  # Minimum time between requests in seconds
THROTTLE_ERRORS = [
    "rate limit",
    "throttled",
    "too many requests",
    "blocked",
    "timeout",
    "connection reset",
    "server unavailable",
]

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


def process_company(scraper: ContactScraper, company: pd.Series) -> List[Dict]:
    """Process a single company"""
    company_name = company["Company"]
    url = company["URL"]

    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Processing {company_name} (attempt {attempt + 1})")
            results = scraper.scrape_team_page(company_name, url)
            return results
        except Exception as e:
            logger.error(
                f"Error processing {company_name} (attempt {attempt + 1}): {e}"
            )
            if attempt == MAX_RETRIES - 1:
                logger.error(
                    f"Failed to process {company_name} after {MAX_RETRIES} attempts"
                )
                return []
            time.sleep(RETRY_DELAY)
    return []


def main():
    """Main pipeline execution"""
    parser = argparse.ArgumentParser(
        description="Scrape team pages from company websites"
    )
    parser.add_argument(
        "--input", default="input/NY_PE_Company_List.csv", help="Input CSV file"
    )
    parser.add_argument(
        "--output", default="output/team_overview.csv", help="Output CSV file"
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum number of records to process",
    )
    parser.add_argument(
        "--max-workers", type=int, default=3, help="Maximum number of parallel workers"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Number of records to process in parallel",
    )
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
    )

    # Load input data
    df = pd.read_csv(args.input)
    if args.max_records:
        df = df.head(args.max_records)

    # Initialize scraper
    scraper = ContactScraper()

    # Process companies in parallel
    all_results = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.max_workers
    ) as executor:
        # Process in batches to avoid overwhelming memory
        for i in range(0, len(df), args.batch_size):
            batch = df.iloc[i : i + args.batch_size]
            futures = []

            # Submit all batch jobs
            for _, company in batch.iterrows():
                future = executor.submit(process_company, scraper, company)
                futures.append(future)

            # Wait for all batch jobs to complete
            completed_futures = concurrent.futures.wait(futures)

            # Process completed futures
            for future in completed_futures.done:
                try:
                    results = future.result()
                    all_results.extend(results)
                except Exception as e:
                    logger.error(f"Error in batch processing: {e}")

            # Rate limit between batches
            logger.info(
                f"Completed batch {i//args.batch_size + 1}, sleeping for {args.batch_size * scraper.min_request_interval:.2f}s"
            )
            time.sleep(args.batch_size * scraper.min_request_interval)

    # Save results
    if all_results:
        df_results = pd.DataFrame(all_results)
        df_results.to_csv(args.output, index=False)
        logger.info(f"Saved {len(all_results)} contacts to {args.output}")

        # Copy to other locations
        csv_dir = os.path.join("output", "csv")
        people_dir = os.path.join("output", "people")
        os.makedirs(csv_dir, exist_ok=True)
        os.makedirs(people_dir, exist_ok=True)

        df_results.to_csv(
            os.path.join(csv_dir, os.path.basename(args.output)), index=False
        )
        df_results.to_csv(
            os.path.join(people_dir, os.path.basename(args.output)), index=False
        )
        logger.info(f"Copied to: {csv_dir}")
        logger.info(f"Copied to: {people_dir}")
    else:
        logger.warning("No data to save")


if __name__ == "__main__":
    # Load API keys
    import os
    from dotenv import load_dotenv

    load_dotenv()
    hunter_api_key = os.getenv("HUNTER_API_KEY")
    serpapi_key = os.getenv("SERPAPI_KEY")

    if not hunter_api_key:
        logger.warning("HUNTER_API_KEY not found - skipping email validation")
        hunter_api_key = None
    if not serpapi_key:
        logger.warning("SERPAPI_KEY not found - skipping search functionality")
        serpapi_key = None

    main()
