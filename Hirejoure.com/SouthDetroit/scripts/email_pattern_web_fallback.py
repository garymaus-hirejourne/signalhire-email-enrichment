import re
import requests
from bs4 import BeautifulSoup

def get_email_pattern_from_web(company_name, fallback_domain=None):
    """
    Search RocketReach/SignalHire for {company_name} email format and extract the most common pattern and domain.
    Returns (pattern_func, domain) where pattern_func is a lambda taking (first, last).
    """
    search_query = f"{company_name} email format site:rocketreach.co OR site:signalhire.com"
    # Use Google Custom Search API or direct search scraping
    from googlesearch import search
    try:
        for url in search(search_query, num_results=5, lang="en"):
            if "rocketreach.co" in url or "signalhire.com" in url:
                try:
                    resp = requests.get(url, timeout=10)
                    soup = BeautifulSoup(resp.text, "html.parser")
                    text = soup.get_text(" ", strip=True)
                    # Look for patterns like 'first.last@domain.com', 'first@domain.com', etc.
                    match = re.search(r"([\[\(]?first[\]\)]?[.\-_ ]?[\[\(]?last[\]\)]?@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,}))", text)
                    if match:
                        pattern_str = match.group(1)
                        domain = match.group(2)
                        if "first.last" in pattern_str:
                            patt_func = lambda f, l: f"{f}.{l}"
                        elif "first" in pattern_str and "last" in pattern_str:
                            patt_func = lambda f, l: f"{f}{l}"
                        elif "first" in pattern_str:
                            patt_func = lambda f, l: f
                        elif "last" in pattern_str:
                            patt_func = lambda f, l: l
                        else:
                            patt_func = lambda f, l: f"{f}.{l}"
                        return patt_func, domain
                except Exception:
                    continue
    except Exception:
        pass
    # Fallback
    if fallback_domain:
        return (lambda f, l: f"{f}.{l}"), fallback_domain
    return (lambda f, l: f"{f}.{l}"), ""

if __name__ == "__main__":
    # Example usage
    patt, dom = get_email_pattern_from_web("Company.com", fallback_domain="company.com")
    print(f"Pattern: {patt('john','doe')}@{dom}")
