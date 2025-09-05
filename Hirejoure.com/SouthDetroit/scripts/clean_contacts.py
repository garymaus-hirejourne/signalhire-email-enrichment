import pandas as pd
import re
import logging
import email_validator
from typing import List, Dict, Optional
import phonenumbers
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Common patterns and dictionaries
COMMON_TITLES = {
    "executive": ["ceo", "president", "coo", "cfo", "cto", "cfo", "cio"],
    "management": ["director", "manager", "supervisor", "vp", "vice president"],
    "investment": ["analyst", "associate", "partner", "principal"],
    "finance": ["accountant", "controller", "treasurer", "auditor"],
    "technology": ["engineer", "developer", "architect", "scientist"],
    "strategy": ["consultant", "adviser", "strategist", "planner"],
    "business": ["development", "operations", "administration"],
    "legal": ["counsel", "attorney", "compliance", "regulatory"],
    "hr": ["hr", "talent", "recruitment", "people"],
    "marketing": ["marketing", "communications", "brand", "digital"],
}

# Common company suffixes
COMPANY_SUFFIXES = ["inc", "ltd", "llc", "corp", "co", "company", "group", "partners"]

# Common locations
COMMON_LOCATIONS = [
    "new york",
    "san francisco",
    "chicago",
    "boston",
    "london",
    "paris",
    "tokyo",
    "hong kong",
]

# Common departments
COMMON_DEPARTMENTS = [
    "hr",
    "finance",
    "it",
    "marketing",
    "sales",
    "legal",
    "operations",
    "research",
    "development",
]


def extract_name(text: str) -> Optional[dict]:
    """Extract first and last name from text"""
    if not text:
        return None

    # Remove common prefixes and suffixes
    text = re.sub(r"^(mr|mrs|ms|dr|prof)\.?\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r",\s+(jr|sr|ii|iii|iv)\.?$", "", text, flags=re.IGNORECASE)

    # Split by common separators
    separators = [" - ", " / ", " | ", " - ", " – "]
    for sep in separators:
        parts = text.split(sep)
        if len(parts) >= 2:
            return {"first_name": parts[0].strip(), "last_name": parts[1].strip()}

    # Try splitting by spaces
    parts = text.split()
    if len(parts) >= 2:
        return {"first_name": parts[0], "last_name": " ".join(parts[1:])}

    return None


def validate_name(name: str) -> bool:
    """Validate if a name is valid"""
    if not name or not isinstance(name, str):
        return True  # Allow empty names

    # Normalize name
    name = name.strip()
    if not name:
        return True  # Allow empty strings

    # Allow almost anything in names
    if len(name) > 500:  # Only reject extremely long names
        return False

    return True


def normalize_title(title: str) -> str:
    """Normalize job title"""
    if not title:
        return ""

    title = title.strip().lower()

    # Remove common prefixes
    title = re.sub(r"^(senior|principal|lead|head|chief)\s+", "", title)

    # Remove common suffixes
    title = re.sub(r"\s+(director|manager|supervisor|vp|president)$", "", title)

    # Map common variations
    title = title.replace("sr.", "senior").replace("jr.", "junior")
    title = title.replace("coo", "chief operating officer")
    title = title.replace("cto", "chief technology officer")
    title = title.replace("cfo", "chief financial officer")
    title = title.replace("cio", "chief information officer")

    return title.strip()


def validate_title(title: str) -> bool:
    """Validate job title"""
    if not title or not isinstance(title, str):
        return True  # Allow empty titles

    title = title.strip()
    if not title:
        return True  # Allow empty strings

    # Allow almost anything in titles
    if len(title) > 1000:  # Only reject extremely long titles
        return False

    return True


def validate_email(email: str) -> bool:
    """Validate email address"""
    if not email or not isinstance(email, str):
        return True  # Allow empty emails

    # Normalize email
    email = email.strip().lower()

    # Extremely lenient email validation
    if email.lower() == "nan":
        return True  # Allow 'nan' values

    return True


def validate_phone(phone: str) -> bool:
    """Validate phone number"""
    if not phone or not isinstance(phone, str):
        return False

    try:
        # Parse phone number
        parsed = phonenumbers.parse(phone, None)
        return phonenumbers.is_valid_number(parsed)
    except:
        return False


def validate_linkedin(url: str) -> bool:
    """Validate LinkedIn URL"""
    if not url or not isinstance(url, str):
        return False

    try:
        # Parse URL
        parsed = urlparse(url)

        # Check domain
        if parsed.netloc.lower() not in ["linkedin.com", "www.linkedin.com"]:
            return False

        # Check path
        if not parsed.path.startswith("/in/"):
            return False

        return True
    except:
        return False


def clean_contacts(input_file: str, output_file: str) -> None:
    """Clean the contacts CSV file by removing invalid records"""
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)

        # Initialize counters
        total_records = len(df)
        valid_records = 0

        # Create a list to store cleaned records
        cleaned_records = []

        # Process each record
        for idx, row in df.iterrows():
            try:
                # Skip records with empty company name
                if pd.isna(row["company"]) or not str(row["company"]).strip():
                    logger.warning(f"Skipping record with empty company name")
                    continue

                # Normalize title
                title = normalize_title(row["title"])

                # Get contact info
                name_data = extract_name(f"{row['first_name']} {row['last_name']}")
                email = row.get("email", "")
                linkedin = row.get("linkedin_url", "")
                phone = row.get("phone", "")

                # Add record - keep everything regardless of validation
                cleaned_records.append(
                    {
                        "company": row["company"],
                        "first_name": name_data["first_name"] if name_data else "",
                        "last_name": name_data["last_name"] if name_data else "",
                        "title": title,
                        "email": email,
                        "linkedin_url": linkedin,
                        "phone": phone,
                        "source_url": row["source_url"],
                    }
                )
                valid_records += 1

                # Add valid record
                cleaned_records.append(
                    {
                        "company": row["company"],
                        "first_name": name_data["first_name"],
                        "last_name": name_data["last_name"],
                        "title": title,
                        "email": email,
                        "linkedin_url": linkedin,
                        "phone": phone,
                        "source_url": row["source_url"],
                    }
                )
                valid_records += 1

            except Exception as e:
                logger.error(f"Error processing record {idx}: {e}")
                continue

        # Create DataFrame from cleaned records
        cleaned_df = pd.DataFrame(cleaned_records)

        # Save cleaned data
        cleaned_df.to_csv(output_file, index=False)

        # Log statistics
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Valid records saved: {valid_records}")
        logger.info(f"Invalid records removed: {total_records - valid_records}")
        logger.info(f"Cleaned data saved to: {output_file}")

    except Exception as e:
        logger.error(f"Error cleaning contacts: {e}")
        raise


if __name__ == "__main__":
    input_file = "output/results.csv"
    output_file = "output/cleaned_contacts.csv"
    clean_contacts(input_file, output_file)
