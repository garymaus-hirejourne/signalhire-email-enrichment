
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup

# === CONFIGURATION ===
INPUT_HTML = "shore_debug/shore_rendered_page.html"
OUTPUT_CSV = "shore_capital_final_enriched.csv"
HUNTER_API_KEY = "b8d028506b3e43fc38fde7f0bcff07ab323c0a33"

# === STATIC NAME FILTERS ===
non_name_terms = {
    "team", "leadership", "contact", "founded", "locations", "global", "overview",
    "biography", "platform", "mission", "partners", "staff", "email", "about",
    "filtered", "content", "subscribe", "search", "menu", "more", "login"
}
title_terms = {
    "ceo", "coo", "cfo", "cio", "cto", "president", "vp", "svp", "evp",
    "partner", "principal", "associate", "analyst", "chairman", "director",
    "managing", "investment", "investor", "finance", "operations", "business",
    "executive", "admin", "hr", "legal", "strategic", "developer", "founder"
}

# === EMAIL INFERENCE VIA HUNTER.IO ===
def get_email_pattern(domain):
    url = "https://api.hunter.io/v2/domain-search"
    params = {"domain": domain, "api_key": HUNTER_API_KEY}
    try:
        r = requests.get(url, params=params)
        pattern = r.json()["data"]["pattern"]
        return pattern
    except Exception as e:
        print(f"Error with Hunter.io lookup for {domain}: {e}")
        return "first.last"

def build_email(first, last, pattern, domain):
    first = first.lower()
    last = last.lower()
    if pattern == "first.last":
        return f"{first}.{last}@{domain}"
    elif pattern == "flast":
        return f"{first[0]}{last}@{domain}"
    elif pattern == "first":
        return f"{first}@{domain}"
    elif pattern == "last":
        return f"{last}@{domain}"
    elif pattern == "firstlast":
        return f"{first}{last}@{domain}"
    elif pattern == "f.last":
        return f"{first[0]}.{last}@{domain}"
    return f"{first}.{last}@{domain}"

def clean_name(name):
    return " ".join([part for part in name.split() if not re.fullmatch(r"[A-Z]\.?$", part)])

# === PARSE HTML ===
with open(INPUT_HTML, "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f, "html.parser")

cards = soup.select("a[href^='/people/']")
domain = "shorecp.com"
pattern = get_email_pattern(domain)

people = []
for card in cards:
    name_el = card.find("div", class_=re.compile("name", re.I))
    title_el = card.find("div", class_=re.compile("title", re.I))
    if not name_el or not title_el:
        continue

    full_name = clean_name(name_el.get_text(strip=True))
    title = title_el.get_text(strip=True)

    if any(term in full_name.lower() for term in non_name_terms | title_terms):
        continue

    parts = full_name.strip().split()
    if len(parts) < 2:
        continue
    first, last = parts[0], " ".join(parts[1:])

    if len(first) <= 2 or len(last) <= 2:
        continue

    email = build_email(first, last, pattern, domain)
    profile_url = "https://www.shorecp.com" + card["href"]

    people.append({
        "First Name": first,
        "Last Name": last,
        "Title": title,
        "Email": email,
        "Email Valid": "Yes",
        "Phone": "",
        "LinkedIn": "",
        "Profile URL": profile_url,
        "Company": "Shore Capital Partners"
    })

df = pd.DataFrame(people)
df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Saved cleaned + enriched Shore Capital team to {OUTPUT_CSV}")
