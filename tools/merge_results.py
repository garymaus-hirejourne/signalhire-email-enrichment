
import pandas as pd
import sys

def main():
    if len(sys.argv) < 4:
        print("Usage: python tools/merge_results.py input.csv results.csv output.csv [linkedin_column]")
        sys.exit(2)
    input_path, results_path, out_path = sys.argv[1:4]
    li_col = sys.argv[4] if len(sys.argv) > 4 else "LinkedIn URL"

    df_in = pd.read_csv(input_path)
    res = pd.read_csv(results_path)

    key = "item"
    if "linkedin" in res.columns:
        key = "linkedin"

    right = res[[key, "emails", "phones"]].copy()
    right = right.rename(columns={key: li_col})

    merged = df_in.merge(right, on=li_col, how="left")
    if "email" not in merged.columns: merged["email"] = ""
    if "phone" not in merged.columns: merged["phone"] = ""
    merged["email"] = merged["email"].fillna("").where(merged["email"].astype(str).str.len()>0, merged["emails"].fillna(""))
    merged["phone"] = merged["phone"].fillna("").where(merged["phone"].astype(str).str.len()>0, merged["phones"].fillna(""))
    merged = merged.drop(columns=[c for c in ["emails","phones"] if c in merged.columns])

    merged.to_csv(out_path, index=False)
    print("Wrote", out_path)

if __name__ == "__main__":
    main()
