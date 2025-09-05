import pandas as pd
from pathlib import Path
from playwright.sync_api import sync_playwright
from urllib.parse import urljoin
import time

# === HARDCODED COMPANY URL MAP ===
company_urls = {
    "GTCR": "https://www.gtcr.com",
    "Madison Dearborn Partners": "https://www.mdcp.com",
    "Thoma Bravo": "https://www.thomabravo.com",
    "Baird Capital": "https://www.bairdcapital.com",
    "Pritzker Private Capital": "https://www.ppcpartners.com",
    "Waud Capital Partners": "https://www.waudcapital.com",
    "Shore Capital Partners": "https://www.shorecp.com",
    "Chicago Pacific Founders": "https://www.chicagopacificfounders.com"
}

team_keywords = ["team", "leadership", "people", "staff", "partners", "management"]

# === LOCAL PATHS ===
base_path = Path(r"C:/Users/gary_/Dropbox/Personal/Job Search/Paul Bickford Solutions/Private Equity/Generic/")
input_csv = base_path / "companies_to_scrape.csv"
output_csv = base_path / "team_overview.csv"

companies_df = pd.read_csv(input_csv)
team_data = []

def is_valid_name(text):
    return (
        text.count(" ") >= 1 and
        len(text.split(" ")[0]) > 1 and
        not any(c in text for c in "@0123456789")
    )

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # Show browser for debugging
    context = browser.new_context()
    page = context.new_page()

    for _, row in companies_df.iterrows():
        company = row["Company"]
        base_url = company_urls.get(company, "")
        if not base_url:
            print(f"⚠️ No URL found for {company}")
            continue

        print(f"🔎 Checking {company}: {base_url}")
        try:
            page.goto(base_url, timeout=20000)
            time.sleep(2)

            team_url = None
            links = page.query_selector_all("a")
            for link in links:
                href = link.get_attribute("href")
                if href and any(k in href.lower() for k in team_keywords):
                    team_url = urljoin(base_url, href)
                    break

            if not team_url:
                print(f"❌ No team page found for {company}")
                continue

            print(f"➡️ Visiting team page: {team_url}")
            page.goto(team_url, timeout=20000)
            time.sleep(3)

            html = page.content()
            blocks = page.locator("div, li, section").all()[:150]  # Limit scanned blocks
            print(f"🔍 Scanning up to {len(blocks)} blocks...")
            found = 0

            for i, block in enumerate(blocks):
                print(f"📦 Block {i + 1}/{len(blocks)}")
                try:
                    text = block.inner_text().strip()
                    if not text or len(text.splitlines()) < 1:
                        continue
                    lines = [l.strip() for l in text.splitlines() if l.strip()]
                    if len(lines) >= 1 and is_valid_name(lines[0]):
                        first, last = lines[0].split(" ", 1)
                        title = lines[1] if len(lines) > 1 else ""
                        link = ""
                        try:
                            href = block.locator("a").first.get_attribute("href")
                            if href:
                                link = urljoin(team_url, href)
                        except:
                            pass
                        team_data.append({
                            "First Name": first,
                            "Last Name": last,
                            "Title": title,
                            "Company": company,
                            "Profile URL": link,
                            "Team Page": team_url
                        })
                        found += 1
                except Exception as e:
                    print(f"⚠️ Block error: {e}")
                    continue

            print(f"✅ Found {found} team members.")
        except Exception as e:
            print(f"❌ Error with {company}: {e}")
            continue

    browser.close()

df = pd.DataFrame(team_data)
df.to_csv(output_csv, index=False)
print(f"✅ Saved {len(df)} people to: {output_csv}")
