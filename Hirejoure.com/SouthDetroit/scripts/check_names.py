import pandas as pd

# Read the CSV file
df = pd.read_csv(r'G:\My Drive\Hirejoure.com\Instrumentation_and_Measurement_Execs v2.csv')

print("Last names with potential issues:")
for i, name in enumerate(df['Last']):
    if pd.notna(name):
        name_str = str(name)
        # Check for problematic characters
        if any(char in name_str for char in [',', '.', ';', '(', ')', '"', "'"]):
            print(f'{i}: "{name_str}" - HAS ISSUES')
        else:
            print(f'{i}: "{name_str}"')
    else:
        print(f'{i}: BLANK/NaN')

print("\nFirst names with potential issues:")
for i, name in enumerate(df['First']):
    if pd.notna(name):
        name_str = str(name)
        # Check for problematic characters
        if any(char in name_str for char in [',', '.', ';', '(', ')', '"', "'"]) or name_str in ['Ms.', 'Mr.', 'Dr.', 'Mrs.']:
            print(f'{i}: "{name_str}" - HAS ISSUES')
        else:
            print(f'{i}: "{name_str}"')
    else:
        print(f'{i}: BLANK/NaN')
