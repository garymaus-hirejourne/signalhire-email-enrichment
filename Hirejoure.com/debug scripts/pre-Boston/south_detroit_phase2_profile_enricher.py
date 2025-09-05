import pandas as pd
import re
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

# === FILE PATHS ===
base_path = Path(r"C:/Users/gary_/Dropbox/Personal/Job Search/Paul Bickford Solutions/Private Equity/Generic/")
input_csv = base_path / "team_overview.csv"
output_csv = base_path / "employee_contacts.csv"

df = pd.read_csv(input_csv)
enriched = []

def extract_email_phone_linkedin(html):
    email_match = re.search(r'href=["\']mailto:([^"\']+)', html)
    phone_match = re.search(r'href=["\']tel:([^"\']+)', html)
    linkedin_match = re.search(r'href=["\'](https?://[\w./-]*linkedin\.com[^"\']*)', html)

    return (
        email_match.group(1) if email_match else "",
        phone_match.group(1) if phone_match else "",
        linkedin_match.group(1) if linkedin_match else ""
    )

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    for idx, row in df.iterrows():
        url = row.get("Profile URL", "")
        if not url or not isinstance(url, str) or url.strip() == "":
            continue

        print(f"🔎 Visiting profile: {url}")
        try:
            page.goto(url, timeout=15000)
            time.sleep(2)
            html = page.content()

            email, phone, linkedin = extract_email_phone_linkedin(html)

            enriched.append({
                "First Name": row["First Name"],
                "Last Name": row["Last Name"],
                "Company": row["Company"],
                "Title": row["Title"],
                "Profile URL": url,
                "Email": email,
                "Phone": phone,
                "LinkedIn": linkedin
            })
        except Exception as e:
            print(f"⚠️ Failed to enrich {row['First Name']} {row['Last Name']} at {url}: {e}")
            continue

    browser.close()

# Save enriched file
pd.DataFrame(enriched).to_csv(output_csv, index=False)
print(f"✅ Saved enriched contacts to {output_csv}")
