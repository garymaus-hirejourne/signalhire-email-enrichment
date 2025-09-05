import pandas as pd
import re
import logging
import unicodedata
import urllib.parse
from typing import Dict, Optional, Any
import email_validator
import os
import requests
from dotenv import load_dotenv
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from parent directory
parent_dir = Path(__file__).resolve().parent.parent.parent
env_path = parent_dir / ".env"
logger.info(f"Loading .env file from: {env_path}")
load_dotenv(env_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def is_valid_name(name: str) -> bool:
    """Check if a name is valid (not team name or empty)"""
    if not name:
        return False

    name = name.lower().strip()

    # Check for invalid names
    invalid_names = [
        "team",
        "all",
        "staff",
        "members",
        "people",
        "about",
        "contact",
        "info",
        "admin",
        "support",
    ]

    if any(invalid in name for invalid in invalid_names):
        return False

    # Check if all characters are ASCII
    return all(ord(c) < 128 for c in name)


def extract_domain(url: str) -> Optional[str]:
    """Extract domain from URL"""
    try:
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except:
        return None


def get_serpapi_email_format(company: str, domain: str) -> Optional[str]:
    """Query SerpAPI for email format"""
    try:
        serpapi_key = os.getenv("SERPAPI_KEY")
        if not serpapi_key:
            logger.error(f"No SerpAPI API key found in environment variables")
            return None

        # Construct search query
        query = f"{company} email format"

        # Query SerpAPI
        params = {"engine": "google", "q": query, "api_key": serpapi_key}

        response = requests.get("https://serpapi.com/search", params=params)
        results = response.json()

        # Look for email patterns in the results
        if "organic_results" in results:
            for result in results["organic_results"]:
                if "snippet" in result:
                    # Look for common email patterns in the snippet
                    patterns = [
                        r"\b([a-z]+)\@",  # first@
                        r"\b([a-z]+)\.([a-z]+)\@",  # first.last@
                        r"\b([a-z]+)\d+\@",  # first123@
                        r"\b([a-z]+)\.([a-z]+)\d+\@",  # first.last123@
                    ]

                    for pattern in patterns:
                        match = re.search(pattern, result["snippet"], re.IGNORECASE)
                        if match:
                            # Create format string
                            format_str = re.sub(r"[a-z]+", "{first}", match.group(0))
                            format_str = re.sub(r"\d+", "{number}", format_str)
                            return f"{format_str}{domain}"

        return None
    except Exception as e:
        logger.error(f"Error querying SerpAPI for {company}: {e}")
        return None


def infer_email_format(
    emails: list[str], company: str, source_url: str
) -> Optional[str]:
    """Infer email format from a list of emails and company domain"""
    if not emails:
        return None

    # Get domain from source URL if available
    domain = extract_domain(source_url)
    if not domain:
        return None

    # First try to find patterns in existing emails
    # Extract patterns from valid emails
    patterns = []
    for email in emails:
        try:
            if not email or "@" not in email:
                continue

            local, email_domain = email.split("@")
            if not email_domain:
                continue

            # If email domain matches company domain, use it
            if email_domain.lower() == domain.lower():
                # Normalize local part
                local = local.lower()
                local = re.sub(r"[^a-z0-9]", "", local)
                patterns.append((local, domain))
        except:
            continue

    if not patterns:
        # If no patterns found in existing emails, try SerpAPI
        return get_serpapi_email_format(company, domain)

    # Try common formats
    common_formats = [
        r"^[a-z]+$",  # first
        r"^[a-z]+[0-9]+$",  # first123
        r"^[a-z]+\.[a-z]+$",  # first.last
        r"^[a-z]+\.[a-z]+[0-9]+$",  # first.last123
        r"^[a-z]+[0-9]+\.[a-z]+$",  # first123.last
        r"^[a-z]+[0-9]+\.[a-z]+[0-9]+$",  # first123.last456
    ]

    for pattern in common_formats:
        if all(re.match(pattern, p[0]) for p in patterns):
            # Create format string
            format_str = re.sub(r"[a-z]+", "{first}", pattern)
            format_str = re.sub(r"[0-9]+", "{number}", format_str)
            return f"{format_str}@{domain}"

    return None


def generate_email(first_name: str, format_str: str) -> Optional[str]:
    """Generate email based on format string"""
    if not first_name or not format_str:
        return None

    try:
        # Clean first name
        first = first_name.lower()
        first = re.sub(r"[^a-z]", "", first)

        # Replace placeholders
        email = format_str.replace("{first}", first)

        # If there's a number placeholder, use 1
        email = email.replace("{number}", "1")

        # Validate email
        try:
            email_validator.validate_email(email)
            return email
        except:
            return None
    except:
        return None


def process_contacts(input_file: str, output_file: str) -> None:
    """Process contacts by removing duplicates, invalid names, and inferring email formats"""
    try:
        # Read the input file
        df = pd.read_csv(input_file)

        # Remove duplicates based on first name and company
        logger.info(f"Original records: {len(df)}")
        df = df.drop_duplicates(subset=["first_name", "company"])
        logger.info(f"After removing duplicates: {len(df)}")

        # Remove rows with invalid names
        df["is_valid_name"] = df["first_name"].apply(is_valid_name)
        df = df[df["is_valid_name"]]
        logger.info(f"After removing invalid names: {len(df)}")

        # Extract email patterns by company
        company_patterns = {}
        for company, group in df.groupby("company"):
            # Get a single source URL for this company
            source_url = (
                group["source_url"].dropna().iloc[0]
                if not group["source_url"].dropna().empty
                else None
            )
            if source_url:
                # Try to get email pattern from SerpAPI
                domain = extract_domain(source_url)
                if domain:
                    pattern = get_serpapi_email_format(company, domain)
                    if pattern:
                        company_patterns[company] = pattern
                        logger.info(f"Inferred pattern for {company}: {pattern}")

        # Generate emails for contacts without email
        def generate_contact_email(row):
            if pd.notna(row["email"]):
                return row["email"]

            pattern = company_patterns.get(row["company"])
            if not pattern:
                return None

            return generate_email(row["first_name"], pattern)

        df["email"] = df.apply(generate_contact_email, axis=1)

        # Remove temporary column
        df = df.drop("is_valid_name", axis=1)

        # Save processed data
        df.to_csv(output_file, index=False)
        logger.info(f"Processed data saved to: {output_file}")
        logger.info(f"Final records: {len(df)}")
        logger.info(f"Emails generated for {df['email'].count()} contacts")

    except Exception as e:
        logger.error(f"Error processing contacts: {e}")


if __name__ == "__main__":
    input_file = "output/cleaned_contacts.csv"
    output_file = "output/processed_contacts.csv"
    process_contacts(input_file, output_file)
