
import time
from playwright.sync_api import sync_playwright
import pandas as pd
from pathlib import Path

def scrape_pritzker_team():
    url = "https://www.ppcpartners.com/team"
    output_file = "pritzker_team_updated.csv"
    debug_dir = Path("bio_debug_dumps")
    debug_dir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        print(f"🔎 Loading team page: {url}")
        page.wait_for_timeout(3000)

        tiles = page.locator(".team-member")
        count = tiles.count()
        print(f"👥 Found {count} team member tiles")

        records = []

        for i in range(count):
            try:
                tile = tiles.nth(i)
                tile.scroll_into_view_if_needed()
                tile.click()
                page.wait_for_timeout(1000)

                page.wait_for_selector(".bio-slider .slide.active h3.bio-name", timeout=5000)

                name = page.locator(".bio-slider .slide.active h3.bio-name").inner_text()
                title = page.locator(".bio-slider .slide.active h4.bio-title").inner_text()
                linkedin = page.locator(".bio-slider .slide.active a.btn-linkedin").get_attribute("href")

                # Save debug HTML
                with open(debug_dir / f"bio_dump_{i}.html", "w", encoding="utf-8") as f:
                    f.write(page.content())

                # Name split
                if " " in name:
                    first_name, last_name = name.strip().split(" ", 1)
                else:
                    first_name = name.strip()
                    last_name = ""

                records.append({
                    "First Name": first_name,
                    "Last Name": last_name,
                    "Title": title.strip(),
                    "LinkedIn": linkedin,
                    "Company": "Pritzker Private Capital",
                    "Profile URL": url
                })

                print(f"✅ Parsed: {first_name} {last_name}")

            except Exception as e:
                print(f"⚠️ No name found after tile click {i}: {e}")
                with open(debug_dir / f"bio_dump_failed_{i}.html", "w", encoding="utf-8") as f:
                    f.write(page.content())

        browser.close()

    if records:
        df = pd.DataFrame(records)
        df.to_csv(output_file, index=False)
        print(f"✅ Saved {len(records)} people to {output_file}")
    else:
        print("❌ No people data extracted.")

if __name__ == "__main__":
    scrape_pritzker_team()
