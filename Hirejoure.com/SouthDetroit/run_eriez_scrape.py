import sys
import csv
from pathlib import Path

# Ensure scripts package is importable
sys.path.append(str(Path(__file__).parent / "scripts"))

from contact_scraper import ContactScraper


def main():
    # Replace API keys if you have them; blank is okay for basic scrape
    scraper = ContactScraper(hunter_api_key="", serpapi_key="")

    url = "https://www.eriez.com/Americas/About.htm"
    print(f"Scraping {url} …")

    contacts = scraper.scrape_team_page("Eriez Magnetics", url, limit=150)
    print(f"Found {len(contacts)} contacts")

    if contacts:
        out_path = Path("output/eriez_contacts.csv")
        out_path.parent.mkdir(exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=contacts[0].keys())
            writer.writeheader()
            writer.writerows(contacts)
        print(f"Saved contacts to {out_path.relative_to(Path.cwd())}")
    else:
        print("No contacts captured.")


if __name__ == "__main__":
    main()
