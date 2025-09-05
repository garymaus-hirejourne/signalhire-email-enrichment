import pandas as pd
import re

# Keywords to identify relevant industries or roles
target_keywords = [
    'measurement', 'instrumentation', 'sensor', 'test equipment', 'calibration', 'precision',
    'metrology', 'analytical', 'process control', 'automation', 'flow', 'pressure', 'temperature',
    'data acquisition', 'DAQ', 'lab equipment', 'gauges', 'transducer', 'signal conditioning'
]

# Keywords to identify business expansion roles
growth_keywords = [
    'ceo', 'founder', 'owner', 'business development', 'sales', 'growth', 'vp', 'president', 'director',
    'managing', 'strategy', 'expansion', 'operations', 'entrepreneur', 'commercial'
]

def keyword_match(text, keywords):
    if pd.isna(text):
        return False
    text = str(text).lower()
    return any(re.search(rf"\\b{re.escape(k)}\\b", text) for k in keywords)

def filter_contacts(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    # Check for possible column names
    possible_cols = df.columns.str.lower()
    # Try to find columns for title, company, industry
    title_col = next((c for c in df.columns if 'title' in c.lower() or 'position' in c.lower()), None)
    company_col = next((c for c in df.columns if 'company' in c.lower()), None)
    industry_col = next((c for c in df.columns if 'industry' in c.lower()), None)

    # Filter for measurement/instrumentation industries
    mask = (
        df[title_col].apply(lambda x: keyword_match(x, target_keywords)) if title_col else False
    ) | (
        df[company_col].apply(lambda x: keyword_match(x, target_keywords)) if company_col else False
    )
    if industry_col:
        mask = mask | df[industry_col].apply(lambda x: keyword_match(x, target_keywords))
    filtered = df[mask]

    # Further flag those who may need business expansion help
    if title_col:
        filtered['needs_expansion_help'] = filtered[title_col].apply(lambda x: keyword_match(x, growth_keywords))
    else:
        filtered['needs_expansion_help'] = False

    filtered.to_csv(output_csv, index=False)
    print(f"Filtered {len(filtered)} contacts. Saved to {output_csv}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Filter LinkedIn contacts for measurement/instrumentation industry and business expansion help.")
    ap.add_argument('input_csv', help='Exported LinkedIn Connections CSV')
    ap.add_argument('output_csv', help='Filtered output CSV')
    args = ap.parse_args()
    filter_contacts(args.input_csv, args.output_csv)
