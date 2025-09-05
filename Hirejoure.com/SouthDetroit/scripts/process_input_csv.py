import pandas as pd
import os
from pathlib import Path
import logging
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("processing.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Define paths
PROJECT_ROOT = Path(__file__).parent.parent
input_dir = PROJECT_ROOT / "input"
processed_dir = PROJECT_ROOT / "processed"

# Create processed directory if it doesn't exist
processed_dir.mkdir(exist_ok=True)


def clean_company_name(name: str) -> str:
    """Clean company names by removing extra whitespace and special characters"""
    if pd.isna(name):
        return ""
    name = str(name).strip()
    name = re.sub(r"\s+", " ", name)  # Replace multiple spaces with single space
    name = re.sub(r"\([^)]*\)", "", name)  # Remove parentheses and contents
    name = name.replace("-", " ").replace("_", " ").strip()
    return name


def clean_url(url: str) -> str:
    """Clean and validate URLs"""
    if pd.isna(url) or not url:
        return ""
    url = str(url).strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def process_ny_pe_list(df: pd.DataFrame) -> pd.DataFrame:
    """Process NY_PE_Company_List.csv"""
    logger.info("Processing NY_PE_Company_List.csv")
    df = df.copy()
    df.columns = ["Company", "URL", "Notes"]
    df["Company"] = df["Company"].apply(clean_company_name)
    df["URL"] = df["URL"].apply(clean_url)
    return df[["Company", "URL"]]


def process_chicago_pe_list(df: pd.DataFrame) -> pd.DataFrame:
    """Process Chicago_PE_Company_List.csv"""
    logger.info("Processing Chicago_PE_Company_List.csv")
    df = df.copy()
    df.columns = ["Company", "URL", "Description"]
    df["Company"] = df["Company"].apply(clean_company_name)
    df["URL"] = df["URL"].apply(clean_url)
    return df[["Company", "URL"]]


def process_nyc_target_list(df: pd.DataFrame) -> pd.DataFrame:
    """Process Target List - NYC List.csv"""
    logger.info("Processing Target List - NYC List.csv")
    # Skip header rows
    df = df.iloc[4:]
    # Extract company names from the first column
    df["Company"] = df.iloc[:, 0].apply(lambda x: clean_company_name(str(x)))
    # Create URLs based on company names
    df["URL"] = df["Company"].apply(
        lambda x: clean_url(f"https://www.{x.lower().replace(' ', '-')}.com")
    )
    return df[["Company", "URL"]]


def main():
    """Process all input CSV files"""
    csv_files = {
        "NY_PE_Company_List.csv": process_ny_pe_list,
        "Chicago_PE_Company_List.csv": process_chicago_pe_list,
        "Target List - NYC List.csv": process_nyc_target_list,
    }

    for filename, processor in csv_files.items():
        try:
            logger.info(f"Processing {filename}")
            input_path = input_dir / filename
            df = pd.read_csv(input_path)

            # Process the dataframe
            processed_df = processor(df)

            # Save processed file
            output_path = processed_dir / f"processed_{filename}"
            processed_df.to_csv(output_path, index=False)
            logger.info(f"Saved processed file to {output_path}")

        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            continue


if __name__ == "__main__":
    main()
