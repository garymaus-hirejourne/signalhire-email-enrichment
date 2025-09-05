import pandas as pd
import re
import logging
from typing import Dict, Set, Optional
import requests
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Hunter.io API configuration
HUNTER_API_KEY = "b8d028506b3e43fc38fde7f0bcff07ab323c0a33"
HUNTER_API_URL = "https://api.hunter.io/v2/email-finder"

# SerpApi configuration
SERPAPI_API_KEY = "9086f1e3c292f39ae1f4b49fc74ffa97fca2ea7ca4ddc4c12f242d51213f369c"
SERPAPI_API_URL = "https://serpapi.com/search.json"


def get_domain_from_url(url: str) -> Optional[str]:
    """Extract domain from URL"""
    try:
        parsed_url = urlparse(url)
        return parsed_url.netloc
    except:
        return None


def get_email_from_hunter(
    first_name: str, last_name: str, company: str, company_domain: str
) -> Optional[str]:
    """Get email using Hunter.io API"""
    try:
        params = {
            "api_key": HUNTER_API_KEY,
            "first_name": first_name,
            "last_name": last_name,
            "company": company,
        }

        response = requests.get(HUNTER_API_URL, params=params)
        data = response.json()

        if data.get("data", {}).get("email"):
            return data["data"]["email"]

    except Exception as e:
        logger.error(f"Error using Hunter.io API: {e}")

    return None


def get_email_from_serpapi(
    first_name: str, last_name: str, company: str, company_domain: str
) -> Optional[str]:
    """Get email using SerpApi"""
    try:
        params = {
            "api_key": SERPAPI_API_KEY,
            "engine": "google",
            "q": f"{first_name} {last_name} {company} email",
            "num": 10,
        }

        response = requests.get(SERPAPI_API_URL, params=params)
        data = response.json()

        # Look for email patterns in search results
        for result in data.get("organic_results", []):
            if result.get("snippet"):
                emails = re.findall(
                    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
                    result["snippet"],
                )
                for email in emails:
                    if company_domain in email:
                        return email

    except Exception as e:
        logger.error(f"Error using SerpApi: {e}")

    return None


def get_company_domain(company: str, source_url: str) -> Optional[str]:
    """Get company domain from company name or source URL"""
    if pd.isna(company) or pd.isna(source_url):
        return None

    # Try to extract from source URL first
    domain = get_domain_from_url(str(source_url))
    if domain:
        return domain

    # Try to extract from company name
    parts = str(company).lower().split()
    for part in parts:
        if "." in part or part.endswith(".com") or part.endswith(".net"):
            return part

    return None


def is_valid_person_name(name: str) -> bool:
    """Check if a name is likely a real person's name"""
    if pd.isna(name):
        return False

    # Normalize name
    name = str(name).strip()
    if not name:
        return False

    # Check for common non-person patterns
    non_person_patterns = [
        "team",
        "staff",
        "members",
        "people",
        "about",
        "contact",
        "info",
        "admin",
        "support",
        "sales",
        "marketing",
        "hr",
        "finance",
        "it",
        "legal",
        "operations",
        "executive",
        "management",
        "board",
        "committee",
        "council",
        "department",
        "division",
        "unit",
        "group",
        "section",
        "team",
        "office",
        "branch",
        "all",
        "debt",
        "equity",
        "private",
        "investment",
        "venture",
        "capital",
        "privateequity",
        "privateequityinvestments",
    ]

    # Convert to lowercase and check if any non-person pattern is in the name
    name_lower = name.lower()
    if any(pattern in name_lower for pattern in non_person_patterns):
        return False

    # Remove any non-alphabetic characters except spaces
    name = re.sub(r"[^a-zA-Z\s]", "", name)

    # Check if name has at least one word
    parts = name.split()
    if len(parts) < 1:
        return False

    # Check if name looks like a real name
    # Allow names with common prefixes and suffixes
    name = name.lower()
    if any(part in name for part in ["mr", "mrs", "ms", "dr", "jr", "sr", "phd"]):
        return True

    # Allow names with at least 2 characters
    if any(len(part) >= 2 for part in parts):
        return True

    return False


def analyze_email_patterns(df: pd.DataFrame) -> Dict[str, Set[str]]:
    """Analyze existing email patterns by company"""
    email_patterns = {}

    # Try to extract potential domains from company names
    for _, row in df.iterrows():
        company = row["company"].strip().lower()
        parts = company.split()
        for part in parts:
            if "." in part or part.endswith(".com") or part.endswith(".net"):
                domain = part
                if domain not in email_patterns:
                    email_patterns[domain] = set()

    # Add some common patterns if no domains found
    if not email_patterns:
        email_patterns["company.com"] = set(
            ["first.last", "f.last", "firstlast", "last.f", "lastf"]
        )
        email_patterns["example.com"] = set(
            ["first.last", "f.last", "firstlast", "last.f", "lastf"]
        )

    return email_patterns


def generate_enriched_email(
    first_name: str, last_name: str, company: str, source_url: str
) -> str:
    """Generate or enrich email using API services"""
    try:
        # Handle NaN values
        if pd.isna(first_name) or pd.isna(last_name):
            return "invalid@invalid.com"

        # Convert to strings and normalize
        first_name = str(first_name).strip()
        last_name = str(last_name).strip()
        company = str(company).strip() if not pd.isna(company) else ""
        source_url = str(source_url).strip() if not pd.isna(source_url) else ""

        # Get company domain
        company_domain = get_company_domain(company, source_url)
        if not company_domain:
            return f"{first_name.lower()}.{last_name.lower()}@example.com"

        # Try Hunter.io first
        email = get_email_from_hunter(first_name, last_name, company, company_domain)
        if email:
            return email

        # Try SerpApi if Hunter.io fails
        email = get_email_from_serpapi(first_name, last_name, company, company_domain)
        if email:
            return email

        # Generate email using common patterns
        first = first_name.lower()
        last = last_name.lower()

        # Try different patterns
        patterns = [
            f"{first}.{last}",
            f"{first[0]}{last}",
            f"{first}{last}",
            f"{last}.{first[0]}",
            f"{last}{first[0]}",
            f"{first}_{last}",
            f"{last}_{first[0]}",
            f"{first}-last",
            f"{last}-{first[0]}",
        ]

        # Return the first valid pattern
        for pattern in patterns:
            if "@" in pattern:
                continue
            return f"{pattern}@{company_domain}"

        return f"{first}.{last}@{company_domain}"

    except Exception as e:
        logger.error(f"Error enriching email for {first_name} {last_name}: {e}")
        return f"{first_name.lower()}.{last_name.lower()}@example.com"


def process_contacts(input_file: str, output_file: str) -> None:
    try:
        # Read the input file
        df = pd.read_csv(input_file)
        logger.info(f"Original records: {len(df)}")

        # Ensure required columns exist and are in correct order
        required_columns = ["first_name", "last_name", "company", "source_url"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return

        # Reorder columns if necessary
        if not all(
            df.columns[i] == required_columns[i] for i in range(len(required_columns))
        ):
            df = df[
                required_columns
                + [col for col in df.columns if col not in required_columns]
            ]
            logger.info("Reordered columns to match required order")

        # Remove duplicates based on first name, last name, and company
        df = df.drop_duplicates(subset=["first_name", "last_name", "company"])
        logger.info(f"After removing duplicates: {len(df)}")

        # Remove rows with invalid names
        df["is_valid_name"] = df["first_name"].apply(is_valid_person_name)
        valid_names_count = df["is_valid_name"].sum()
        logger.info(f"Valid names count: {valid_names_count}")
        df = df[df["is_valid_name"]]
        logger.info(f"After removing invalid names: {len(df)}")

        # If no valid names, return early
        if len(df) == 0:
            logger.info("No valid contacts found")
            return

        # Generate enriched emails for all contacts
        df["email"] = df.apply(
            lambda row: generate_enriched_email(
                row["first_name"], row["last_name"], row["company"], row["source_url"]
            ),
            axis=1,
        )

        # Check if we have any valid emails
        if len(df) > 0:
            valid_emails = df["email"].str.contains("@").sum()
            logger.info(f"Generated {valid_emails} valid email addresses")
        else:
            valid_emails = 0
            logger.info("No valid emails generated")

        # Remove temporary column
        df = df.drop("is_valid_name", axis=1)

        # Ensure output directory exists
        import os

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Save processed data
        df.to_csv(output_file, index=False)
        logger.info(f"Processed data saved to: {output_file}")
        logger.info(f"Final records: {len(df)}")

    except Exception as e:
        logger.error(f"Error processing contacts: {e}", exc_info=True)


if __name__ == "__main__":
    import os

    # Directory containing input CSV files
    input_dir = "output"
    output_dir = "debug/scripts/pre-Boston"

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Process each CSV file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".csv"):
            input_file = os.path.join(input_dir, filename)
            output_file = os.path.join(output_dir, f"enriched_{filename}")
            logger.info(f"Processing file: {filename}")
            try:
                process_contacts(input_file, output_file)
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                continue

    logger.info("Processing complete!")
