
import pandas as pd
from playwright.sync_api import sync_playwright
import time

INPUT_CSV = "shore_capital_final_enriched.csv"
OUTPUT_CSV = "shore_capital_final_enriched_with_contacts.csv"

df = pd.read_csv(INPUT_CSV)
df["Phone"] = ""
df["LinkedIn"] = ""

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    for i, row in df.iterrows():
        url = row["Profile URL"]
        print(f"🔎 Visiting: {url}")
        try:
            page.goto(url, timeout=15000)
            page.wait_for_timeout(2000)

            phone = ""
            linkedin = ""

            try:
                phone_elem = page.query_selector("a[href^='tel:']")
                if phone_elem:
                    phone = phone_elem.get_attribute("href").replace("tel:", "")
            except:
                pass

            try:
                linkedin_elem = page.query_selector("a[href*='linkedin.com']")
                if linkedin_elem:
                    linkedin = linkedin_elem.get_attribute("href")
            except:
                pass

            df.at[i, "Phone"] = phone
            df.at[i, "LinkedIn"] = linkedin
        except Exception as e:
            print(f"⚠️ Failed to process {url}: {e}")
        time.sleep(1)

    browser.close()

df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Enriched contact info saved to {OUTPUT_CSV}")
