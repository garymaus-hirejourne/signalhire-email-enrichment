import pandas as pd
import re

def clean_name(name):
    if not isinstance(name, str) or pd.isna(name):
        return ""
    
    original_name = str(name).strip()
    if not original_name:
        return ""
    
    # Start with the original name
    name = original_name
    
    # Remove tabs and normalize whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    
    # Step 1: Remove everything after comma (credentials often follow commas)
    if ',' in name:
        name = name.split(',')[0].strip()
    
    # Step 2: Remove everything after semicolon or pipe
    if ';' in name:
        name = name.split(';')[0].strip()
    if '|' in name:
        name = name.split('|')[0].strip()
    
    # Step 3: Remove parentheses and their contents (nicknames, etc.)
    name = re.sub(r"\([^)]*\)", "", name).strip()
    
    # Step 4: Handle quotes - remove quotes but keep the content inside
    name = re.sub(r'"([^"]+)"', r'\1', name)  # Replace "Jerry" with Jerry
    name = re.sub(r"'([^']+)'", r'\1', name)   # Replace 'Jerry' with Jerry
    name = re.sub(r'["\']', "", name).strip()  # Remove remaining quotes
    
    # Step 5: Remove specific honorifics at the beginning (whole words only)
    honorifics = ['mr', 'ms', 'mrs', 'dr', 'prof', 'miss', 'sir', 'madam', 'lady', 'lord']
    words = name.split()
    if words and words[0].lower().rstrip('.') in honorifics:
        words = words[1:]  # Remove first word if it's an honorific
        name = ' '.join(words)
    
    # Step 6: Remove middle initials (single letters with periods at start or end)
    # "J. Prajzner" -> "Prajzner", "Angela C." -> "Angela"
    words = name.split()
    cleaned_words = []
    for word in words:
        # Skip single letters with or without periods (but keep regular words)
        if len(word.rstrip('.')) == 1 and word.rstrip('.').isalpha():
            continue  # Skip middle initials
        else:
            cleaned_words.append(word)
    name = ' '.join(cleaned_words)
    
    # Step 7: Clean up any remaining non-alphabetic characters at start/end
    # But be very conservative to avoid truncating valid names
    name = re.sub(r'^[^a-zA-Z\s\'\-]+', '', name)  # Remove leading non-letters
    name = re.sub(r'[^a-zA-Z\s\'\-]+$', '', name)  # Remove trailing non-letters
    
    # Final cleanup
    name = re.sub(r'\s+', ' ', name).strip()
    
    # If empty after cleaning, return empty
    if not name:
        return ""
    
    # Handle special cases for email generation
    # Remove apostrophes and hyphens for email (D'Alterio -> dalterio)
    email_name = name.replace("'", "").replace("-", "")
    
    return email_name.lower()

# Test cases from the actual data
test_cases = [
    "Meinen, MS, CISSP",
    "Guerrero, P.E., MBA, PMP, LEED AP", 
    "J. Prajzner",
    "N. Stamatakis",
    "D'Alterio",
    "Ms.",
    "Angela C.",
    "Anthony (Tony)",
    'Gennaro "Jerry"',
    "Wendy	",  # Has tab character
    "Jennifer Scanlon",
    "Shuman",
    "Williams"
]

print("Testing enhanced name cleaning logic:")
print("=" * 50)
for test_name in test_cases:
    cleaned = clean_name(test_name)
    print(f'"{test_name}" -> "{cleaned}"')
