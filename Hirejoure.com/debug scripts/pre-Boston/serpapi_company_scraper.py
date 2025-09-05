
import os
import csv
from serpapi import GoogleSearch
from urllib.parse import urlparse
import pandas as pd

# === CONFIGURATION ===
SERPAPI_API_KEY = "9086f1e3c292f39ae1f4b49fc74ffa97fca2ea7ca4ddc4c12f242d51213f369c"
MAX_RESULTS = 20  # Number of results to collect

# === USER INPUT ===
city = input("📍 Enter the city you'd like to search in: ").strip()
industry = input("🏭 Enter the industry you're searching for (e.g., Bank, Private Equity, IT): ").strip()

# === SEARCH QUERIES ===
search_queries = [
    f"{industry} companies in {city}",
    f"{industry} firms in {city}",
    f"{industry} organizations based in {city}",
    f"{industry} employers in {city}",
    f"top {industry} companies in {city}"
]

# === SEARCH FUNCTION ===
def search_google_with_serpapi(query):
    search = GoogleSearch({
        "q": query,
        "location": city,
        "api_key": SERPAPI_API_KEY,
        "engine": "google",
        "num": 10
    })
    results = search.get_dict()
    return results.get("organic_results", [])

# === MAIN SCRAPE ===
seen_domains = set()
company_data = []

for query in search_queries:
    print(f"🔍 Searching: {query}")
    results = search_google_with_serpapi(query)

    for result in results:
        title = result.get("title", "")
        link = result.get("link", "")
        snippet = result.get("snippet", "")
        if link:
            domain = urlparse(link).netloc
            if domain not in seen_domains:
                seen_domains.add(domain)
                company_data.append({
                    "Company Name": title,
                    "Website": link,
                    "Description": snippet
                })
        if len(company_data) >= MAX_RESULTS:
            break
    if len(company_data) >= MAX_RESULTS:
        break

# === OUTPUT ===
if company_data:
    filename = f"company_search_{city.replace(',', '').replace(' ', '_')}_{industry.replace(' ', '_')}.csv"
    df = pd.DataFrame(company_data)
    df.to_csv(filename, index=False)
    print(f"✅ Saved {len(company_data)} company URLs to {filename}")
else:
    print("❌ No results found. Try adjusting your city or industry.")
