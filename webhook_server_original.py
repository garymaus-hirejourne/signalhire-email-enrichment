
import os, csv, datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

RESULTS_PATH = os.getenv("SIGNALHIRE_RESULTS_CSV", "/data/results.csv")
os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)

if not os.path.exists(RESULTS_PATH):
    with open(RESULTS_PATH, "w", newline='', encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["item","status","fullName","emails","phones","linkedin","received_at"])

@app.get("/health")
def health():
    return jsonify({"ok": True}), 200

@app.post("/signalhire/webhook")
def signalhire_webhook():
    data = request.get_json(force=True)
    if not isinstance(data, list):
        return jsonify({"error": "Expected a JSON array"}), 400

    rows = []
    now = datetime.datetime.utcnow().isoformat()
    for r in data:
        item = r.get("item")
        status = r.get("status")
        cand = r.get("candidate") or {}
        name = cand.get("fullName") or ""
        emails = ";".join([c.get("value","") for c in (cand.get("contacts") or []) if c.get("type")=="email"])
        phones = ";".join([c.get("value","") for c in (cand.get("contacts") or []) if c.get("type")=="phone"])
        li = ""
        for s in (cand.get("social") or []):
            if s.get("type") == "li":
                li = s.get("link","")
                break
        rows.append([item, status, name, emails, phones, li, now])

    with open(RESULTS_PATH, "a", newline='', encoding="utf-8") as f:
        csv.writer(f).writerows(rows)

    return jsonify({"ok": True, "written": len(rows)}), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
