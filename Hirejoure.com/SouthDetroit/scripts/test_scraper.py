import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
from .south_detroit_phase1_team_scraper_debug import ContactScraper

# Load environment variables
load_dotenv(
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
)

# Get API keys
hunter_api_key = os.getenv("HUNTER_API_KEY")
serpapi_key = os.getenv("SERPAPI_KEY")

# Initialize scraper
scraper = ContactScraper(hunter_api_key, serpapi_key)


def test_name_validation():
    """Test name validation functionality"""
    test_names = [
        ("John Smith", True),  # Valid name
        ("johnsmith", False),  # No capital letters
        ("Team Overview", False),  # Common non-name pattern
        ("Contact Us", False),  # Common non-name pattern
        ("John O'Malley-Smith", True),  # Valid name with special characters
        ("John Smith " * 20, False),  # Too long
        ("", False),  # Empty string
        ("A", False),  # Too short
    ]

    print("\nTesting name validation:")
    for name, expected in test_names:
        result = scraper.validate_name(name)
        status = "[PASS]" if result == expected else "[FAIL]"
        print(f"{status} {name}: {result}")


def test_contact_validation():
    """Test contact validation functionality"""
    test_contacts = [
        (
            {
                "Full Name": "John Smith",
                "Title": "CEO",
                "Company": "Test Company",
                "Source URL": "https://example.com",
            },
            True,
        ),  # Valid contact
        (
            {
                "Full Name": "Team Overview",
                "Title": "" * 101,  # Too long
                "Company": "Test Company",
                "Source URL": "https://example.com",
            },
            False,
        ),  # Invalid title
        (
            {
                "Full Name": "John Smith",
                "Title": "CEO",
                "Company": "Test Company",
                "Source URL": "https://example.com",
            },
            False,
        ),  # Duplicate (since we're using the same company)
    ]

    print("\nTesting contact validation:")
    for contact, expected in test_contacts:
        result = scraper.validate_contact(contact)
        status = "[PASS]" if result == expected else "[FAIL]"
        print(f"{status} {contact['Full Name']}: {result}")


def test_data_generation():
    """Test data generation with a sample URL"""
    test_url = "https://www.example.com/team"
    test_company = "Example Company"
    test_output_file = Path("test_output.csv")

    print("\nTesting data generation:")
    try:
        results = scraper.scrape_team_page(test_company, test_url)
        print(f"\nFound {len(results)} contacts:")
        for contact in results:
            print(f"- {contact['Full Name']} ({contact['Title']})")

        # Save test results to CSV
        if results:
            df = pd.DataFrame(results)
            df.to_csv(test_output_file, index=False)
            print(f"\nTest results saved to {test_output_file}")
            print("\nFirst few rows of test output:")
            print(df.head())
    except Exception as e:
        print(f"Error: {e}")

    # Clean up test file
    if test_output_file.exists():
        test_output_file.unlink()


def check_output_files():
    """Check if output files are being generated"""
    output_dir = Path(__file__).parent.parent / "output"
    print("\nChecking output files:")

    # Check main output file
    main_output = output_dir / "team_overview.csv"
    if main_output.exists():
        print(f"[PASS] Main output file exists: {main_output}")
        df = pd.read_csv(main_output)
        print(f"  - Rows: {len(df)}")
        print(f"  - Last update: {datetime.fromtimestamp(main_output.stat().st_mtime)}")
    else:
        print("[FAIL] Main output file not found")

    # Check backup files
    backup_dir = output_dir / "backups"
    if backup_dir.exists():
        print(f"[PASS] Backup directory exists")
        backups = list(backup_dir.glob("*.csv"))
        print(f"  - Backups found: {len(backups)}")
    else:
        print("[FAIL] Backup directory not found")

    # Check debug dumps
    debug_dir = output_dir / "debug_dumps"
    if debug_dir.exists():
        print(f"[PASS] Debug dumps directory exists")
        dumps = list(debug_dir.glob("*.html"))
        print(f"  - Debug dumps found: {len(dumps)}")
    else:
        print("[FAIL] Debug dumps directory not found")


if __name__ == "__main__":
    print("Running scraper tests...")
    test_name_validation()
    test_contact_validation()
    test_data_generation()
    check_output_files()
