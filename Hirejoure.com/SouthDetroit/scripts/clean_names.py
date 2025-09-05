import pandas as pd
import re
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Lists of common names
common_first_names = [
    "john",
    "michael",
    "william",
    "robert",
    "james",
    "charles",
    "joseph",
    "thomas",
    "david",
    "richard",
    "martin",
    "peter",
    "mark",
    "donald",
    "george",
    "paul",
    "steven",
    "edward",
    "anthony",
    "kenneth",
    "brian",
    "kevin",
    "daniel",
    "jerry",
    "gary",
    "jason",
    "joe",
    "eric",
    "steve",
    "doug",
]

common_last_names = [
    "smith",
    "johnson",
    "williams",
    "brown",
    "jones",
    "miller",
    "davis",
    "garcia",
    "rodriguez",
    "wilson",
    "martinez",
    "anderson",
    "taylor",
    "thomas",
    "hernandez",
    "moore",
    "martin",
    "jackson",
    "thompson",
    "white",
    "lopez",
    "lee",
    "gonzalez",
    "harris",
    "clark",
    "lewis",
    "robinson",
    "walker",
    "perez",
    "hall",
    "young",
    "allen",
    "king",
    "wright",
    "scott",
    "torres",
    "nguyen",
    "hill",
    "flores",
    "green",
    "adams",
    "nelson",
    "baker",
    "rivera",
    "campbell",
    "mitchell",
    "carter",
    "roberts",
]


def is_valid_person_name(name: str) -> bool:
    """Check if a name is likely a real person's name"""
    if not name:
        return False

    # Normalize name
    name = name.strip().lower()

    # Check for common non-person patterns
    non_person_patterns = [
        r"^(team|all|staff|members|people|about|contact|info|admin|support)$",
        r"^(sales|marketing|hr|finance|it|legal|operations|executive|management)$",
        r"^(board|committee|council|department|division|unit|group|section|team|office|branch)$",
    ]

    if any(re.match(pattern, name) for pattern in non_person_patterns):
        return False

    # Check if name has valid character patterns (allow hyphens and apostrophes)
    if not re.match(r"^[a-z\s\-\']+$", name):
        return False

    # Check if name has at least one part (single names are allowed)
    parts = name.split()
    if len(parts) < 1:
        return False

    # If name has only one part, check if it's a common name
    if (
        len(parts) == 1
        and parts[0] not in common_first_names
        and parts[0] not in common_last_names
    ):
        return False

    # Check if either first or last name is common
    first_name = parts[0]
    last_name = parts[-1]

    if first_name in common_first_names or last_name in common_last_names:
        return True

    # If neither name is common, check if they have valid name-like patterns
    if re.match(r"^[a-z]{2,}$", first_name) and re.match(r"^[a-z]{2,}$", last_name):
        return True

    return False


def clean_contacts(input_file: str, output_file: str) -> None:
    """Clean contacts by removing duplicates and non-person entries"""
    try:
        # Read the input file
        df = pd.read_csv(input_file)

        # Remove duplicates based on first name, last name, and company
        logger.info(f"Original records: {len(df)}")
        df = df.drop_duplicates(subset=["first_name", "last_name", "company"])
        logger.info(f"After removing duplicates: {len(df)}")

        # Remove rows with invalid names
        df["is_valid_name"] = df["first_name"].apply(is_valid_person_name)
        df = df[df["is_valid_name"]]
        logger.info(f"After removing invalid names: {len(df)}")

        # Remove rows with invalid titles
        def is_valid_title(title: str) -> bool:
            if not title:
                return False
            title = title.lower()
            # Check for common professional titles
            professional_titles = [
                "ceo",
                "coo",
                "cfo",
                "cto",
                "president",
                "vp",
                "director",
                "manager",
                "founder",
                "partner",
                "investor",
                "analyst",
                "associate",
                "executive",
                "consultant",
                "adviser",
                "counsel",
            ]
            return any(term in title for term in professional_titles)

        df["is_valid_title"] = df["title"].apply(is_valid_title)
        df = df[df["is_valid_title"]]
        logger.info(f"After removing invalid titles: {len(df)}")

        # Remove rows with invalid emails
        def is_valid_email(email: str) -> bool:
            if pd.isna(email):
                return False
            email = str(email)  # Convert to string
            if not email.strip():  # Check for empty string
                return False
            # Basic email format validation
            if not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email):
                return False
            
            local_part, domain = email.split("@", 1)
            # Reject generic role-based prefixes (non-person mailboxes)
            generic_prefixes = {
                "info", "contact", "sales", "marketing", "support", "help",
                "admin", "webmaster", "postmaster", "hr", "jobs", "careers",
                "billing", "accounts", "office", "team", "hello", "enquiries",
                "customerservice", "service", "no-reply", "noreply"
            }
            if local_part.lower() in generic_prefixes:
                return False
            # Reject pattern like "eriez" or company name only (catch-all)
            if re.fullmatch(r"[a-z0-9\-_.]+", local_part) and len(local_part.split(".")) == 1 and len(local_part) <= 12:
                # likely a role/catch-all, not firstname.lastname
                return False
            # Check for common invalid domains
            invalid_domains = {"example.com", "test.com", "invalid.com"}
            if domain.lower() in invalid_domains:
                return False
            return True

        df["is_valid_email"] = df["email"].apply(is_valid_email)
        df = df[df["is_valid_email"]]
        logger.info(f"After removing invalid emails: {len(df)}")

        # Remove temporary columns
        df = df.drop(["is_valid_name", "is_valid_title", "is_valid_email"], axis=1)

        # Save processed data
        df.to_csv(output_file, index=False)
        logger.info(f"Processed data saved to: {output_file}")
        logger.info(f"Final records: {len(df)}")

    except Exception as e:
        logger.error(f"Error cleaning contacts: {e}")


if __name__ == "__main__":
    input_file = "output/cleaned_contacts.csv"
    output_file = "output/personal_contacts.csv"
    clean_contacts(input_file, output_file)
