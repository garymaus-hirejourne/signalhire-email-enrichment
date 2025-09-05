
import time
from pathlib import Path
from playwright.sync_api import sync_playwright
import csv

def scrape_pritzker_team():
    output_file = "pritzker_team_v2_results.csv"
    debug_dir = Path("bio_debug_dumps")
    debug_dir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto("https://www.ppcpartners.com/team")
        page.wait_for_timeout(5000)

        team_tiles = page.query_selector_all(".team-member")
        print(f"👥 Found {len(team_tiles)} team member tiles")

        results = []

        for i, tile in enumerate(team_tiles):
            try:
                tile.click()
                page.wait_for_selector(".bio-slider .slide.active h3.bio-name", timeout=7000)
                name = page.locator(".bio-slider .slide.active h3.bio-name").inner_text()
                linkedin = ""
                link_elements = page.locator(".bio-slider .slide.active a").all()
                for el in link_elements:
                    href = el.get_attribute("href")
                    if href and "linkedin.com" in href:
                        linkedin = href
                        break
                first, last = name.split(" ", 1) if " " in name else (name, "")
                results.append({
                    "First Name": first.strip(),
                    "Last Name": last.strip(),
                    "LinkedIn URL": linkedin,
                    "Email": "",
                    "Phone": ""
                })
            except Exception as e:
                print(f"⚠️ Failed to process tile {i}: {e}")
            finally:
                page.keyboard.press("Escape")
                time.sleep(1)

        browser.close()

        keys = ["First Name", "Last Name", "LinkedIn URL", "Email", "Phone"]
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)
        print(f"✅ Saved {len(results)} people to {output_file}")

if __name__ == "__main__":
    scrape_pritzker_team()
