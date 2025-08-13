
import csv, time, requests, os, sys

API_KEY = os.getenv("SIGNALHIRE_API_KEY", "YOUR_SIGNALHIRE_KEY")
ENDPOINT = "https://www.signalhire.com/api/v1/candidate/search"
CALLBACK_URL = os.getenv("SIGNALHIRE_CALLBACK_URL", "https://YOUR_DOMAIN/signalhire/webhook")

def chunks(lst, n=100):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def load_identifiers(csv_path, columns=("LinkedIn URL","linkedin","linkedin_url","profile")):
    items = []
    with open(csv_path, newline='', encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            for c in columns:
                val = (row.get(c) or "").strip()
                if val:
                    items.append(val)
                    break
    return items

def submit_batch(items):
    payload = {"items": items, "callbackUrl": CALLBACK_URL}
    resp = requests.post(
        ENDPOINT,
        headers={"apikey": API_KEY},
        json=payload,
        timeout=30
    )
    if resp.status_code == 429:
        raise RuntimeError("Rate limited (429). Slow down and retry.")
    resp.raise_for_status()
    print("Submitted batch; response:", resp.text[:200])

def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/signalhire_enrich.py /path/to/input.csv [batch_size]")
        sys.exit(2)
    csv_path = sys.argv[1]
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    ids = load_identifiers(csv_path)
    print(f"Loaded {len(ids)} identifiers from {csv_path}.")
    sent = 0
    for batch in chunks(ids, batch_size):
        submit_batch(batch)
        sent += len(batch)
        time.sleep(0.25)
    print("Done. Submitted", sent, "items.")

if __name__ == "__main__":
    main()
