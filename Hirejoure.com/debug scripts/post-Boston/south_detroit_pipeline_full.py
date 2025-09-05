import csv
import os
from pathlib import Path
from south_detroit_phase1_team_scraper_debug import scrape_team_page
from shore_playwright_enrichment import enrich_contacts_with_profile_data
from south_detroit_phase2_profile_enricher import fallback_email_and_phone_enrichment

INPUT_FILE = "input/NY_PE_Company_List.csv"
OUTPUT_DIR = Path("output/csv")
DEBUG_DIR = Path("output/debug_dumps")
PEOPLE_DIR = Path("output/people")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)
PEOPLE_DIR.mkdir(parents=True, exist_ok=True)

def load_company_list(input_file):
    with open(input_file, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        companies = [row for row in reader if "Name" in row and "URL" in row]
    return companies

def main():
    companies = load_company_list(INPUT_FILE)
    all_contacts = []

    for idx, company in enumerate(companies[:15]):  # Limit to first 15
        name = company["Name"]
        url = company["URL"]
        print(f"🔍 Scraping [{idx + 1}/{len(companies)}]: {name} ({url})")
        try:
            contacts = scrape_team_page(name, url, debug_dir=DEBUG_DIR)
            print(f"✅ Found {len(contacts)} people at {name}")
            all_contacts.extend(contacts)
        except Exception as e:
            print(f"❌ Failed to scrape {name}: {e}")

    enriched_contacts = enrich_contacts_with_profile_data(all_contacts)
    final_contacts = fallback_email_and_phone_enrichment(enriched_contacts)

    output_file = OUTPUT_DIR / "south_detroit_full_output.csv"
    keys = final_contacts[0].keys() if final_contacts else []

    with open(output_file, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(final_contacts)

    print(f"📦 Output saved to {output_file.resolve()}")

if __name__ == "__main__":
    main()
