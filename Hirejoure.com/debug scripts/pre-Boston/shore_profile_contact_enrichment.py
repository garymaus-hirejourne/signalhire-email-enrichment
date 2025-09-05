
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

INPUT_CSV = "shore_capital_final_enriched.csv"
OUTPUT_CSV = "shore_capital_final_enriched_with_contacts.csv"

def extract_contact_info(url):
    try:
        response = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html"
        }, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        tel_tag = soup.find("a", href=lambda x: x and x.startswith("tel:"))
        linkedin_tag = soup.find("a", href=lambda x: x and "linkedin.com" in x)

        phone = tel_tag["href"].replace("tel:", "") if tel_tag else ""
        linkedin = linkedin_tag["href"] if linkedin_tag else ""
        return phone, linkedin
    except Exception as e:
        print(f"⚠️ Failed to fetch profile {url}: {e}")
        return "", ""

df = pd.read_csv(INPUT_CSV)
df["Phone"] = ""
df["LinkedIn"] = ""

for i, row in df.iterrows():
    profile_url = row["Profile URL"]
    print(f"🔎 Visiting: {profile_url}")
    phone, linkedin = extract_contact_info(profile_url)
    df.at[i, "Phone"] = phone
    df.at[i, "LinkedIn"] = linkedin
    time.sleep(1.2)  # Avoid hammering the server

df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Enriched profile data saved to {OUTPUT_CSV}")
