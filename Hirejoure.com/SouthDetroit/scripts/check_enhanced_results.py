import pandas as pd

# Read the enhanced results
df = pd.read_csv(r'G:\My Drive\Hirejoure.com\Instrumentation_and_Measurement_Execs v2 - ENRICHED.csv')

print('Enhanced Results Summary:')
print('=' * 50)
print(f'Total records: {len(df)}')
print(f'Emails populated: {df["email"].notna().sum()}')
print(f'Phone numbers populated: {df["phone_number"].notna().sum()}')

print('\nSample of cleaned names and emails:')
for i in range(min(10, len(df))):
    first = str(df.iloc[i]["first_name"])
    last = str(df.iloc[i]["last_name"])
    email = str(df.iloc[i]["email"])
    print(f'{i+1:2d}. {first:15} {last:15} -> {email}')

print('\nProblem cases from original data (should now be fixed):')
# Check specific problem indices we identified earlier
problem_indices = [1, 11, 20, 25, 30]  # Ms., Meinen,MS, Guerrero,P.E., J.Prajzner, D'Alterio
for i in problem_indices:
    if i < len(df):
        first = str(df.iloc[i]["first_name"])
        last = str(df.iloc[i]["last_name"])
        email = str(df.iloc[i]["email"])
        print(f'{i+1:2d}. {first:15} {last:15} -> {email}')

print('\nEmail quality check:')
valid_emails = 0
for email in df["email"]:
    if pd.notna(email) and "@" in str(email) and "." in str(email):
        valid_emails += 1
print(f'Valid email format: {valid_emails}/{len(df)} ({valid_emails/len(df)*100:.1f}%)')
