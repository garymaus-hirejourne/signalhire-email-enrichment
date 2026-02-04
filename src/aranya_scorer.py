import random
import csv
import sys

# Hardcoded usernames from GitHub searches (Cilium AUTHORS top 50; expand with more queries)
usernames = [
    '1602077', 'a5r0n', 'aarongroom', 'abejideayodele', 'abirdcfly', 'abradbury', 'abebars', 'aditighag', 'adityakumar60', 'adityapurandare',
    'adityasharma', 'adrianberger', 'adrienjt', 'afzal442', 'agustafsson', 'ahkon', 'ahmetb', 'ahmetimamoglu', 'ahtomsk', 'aibrahim',
    'ajayk', 'ajaykumar', 'ajayver', 'ajlanza', 'ajmath', 'akapoor', 'akhenakh', 'akhtar', 'akifumi', 'akihirosuda',
    'akihitoyamashita', 'akinoshita', 'akito19', 'akiyamah', 'akkornel', 'akopytov', 'akr76', 'akrout', 'akunrai', 'al3xstratton',
    'alaa', 'alan-kane', 'alanfranzoni', 'alankao', 'alban', 'albertito', 'albertvantheart', 'albinoloverats', 'albrecht', 'alecmocatta'
]

# Rubric dimensions and max scores
rubric = {
    'Operators': 20,
    'GitOps': 15,
    'Storage': 10,
    'Networking': 10,
    'MultiCluster': 10,
    'IaC': 10,
    'Observability': 10,
    'OSS': 10,
    'Product': 5
}

# Generate mock scores (v2: replace with API-based logic, e.g., contrib count in relevant repos)
data = []
for username in usernames:
    scores = {dim: random.randint(0, max_score) for dim, max_score in rubric.items()}
    overall = sum(scores.values())
    rationale = f"Mock for {username}: Assumed Cilium contribs boost Networking/OSS; adjust based on real profile."
    risks = "Mock: Potential weak GitOps; verify location for SF in-person."
    row = [username, overall] + list(scores.values()) + [rationale, risks]
    data.append(row)

# CSV headers
headers = ['GitHub Username', 'Overall Score'] + list(rubric.keys()) + ['Rationale Summary', 'Key Risks / Flags']

# Write to stdout (redirect to file: python aranya_scorer.py > aranya_candidates.csv)
writer = csv.writer(sys.stdout)
writer.writerow(headers)
writer.writerows(data)
