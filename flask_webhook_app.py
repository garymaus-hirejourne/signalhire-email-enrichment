#!/usr/bin/env python3
"""
SignalHire Webhook Receiver for Northflank
Receives SignalHire API callbacks and saves enriched results to CSV
"""

from flask import Flask, request, jsonify, send_file
import csv
import json
import os
import time
import threading
from datetime import datetime
from pathlib import Path
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration - fallback to /tmp if /data not available
RESULTS_CSV = os.getenv('SIGNALHIRE_RESULTS_CSV', '/data/results.csv')
STATUS_JSON = os.getenv('SIGNALHIRE_STATUS_JSON', '/data/status.json')
DATA_DIR = Path(RESULTS_CSV).parent
write_lock = threading.Lock()

# Create fallback directory if /data is not accessible
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except PermissionError:
    # Fallback to /tmp if /data volume not mounted
    RESULTS_CSV = '/tmp/results.csv'
    STATUS_JSON = '/tmp/status.json'
    DATA_DIR = Path(RESULTS_CSV).parent
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.warning(f"Cannot access /data directory, using fallback: {RESULTS_CSV}")

# Target columns for enriched data (matching SignalHire's rich format)
COLUMNS = [
    "Item", "Status",
    "First Name", "Last Name", "Full Name",
    "Current Position", "Company",
    "Country", "City",
    "Emails (Work)", "Emails (Personal)",
    "Mobile Phone1", "Mobile Phone2",
    "Work Phone1", "Work Phone2",
    "Home Phone",
    "LinkedIn",
    "Skills", "Education",
    "Received At"
]

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

def split_name(full_name):
    """Split full name into first and last name"""
    if not full_name:
        return ("", "")
    parts = full_name.strip().split()
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))

def pick_location(locations):
    """Extract country and city from locations array"""
    if not locations:
        return ("", "")
    location = locations[0]
    name = location.get("name", "")
    country, city = "", ""
    if name and "," in name:
        parts = [p.strip() for p in name.split(",")]
        if len(parts) >= 1:
            city = parts[0]
        if len(parts) >= 2:
            country = parts[-1]
    return (country, city)

def current_experience(experiences):
    """Extract current position and company from experience"""
    if not experiences:
        return ("", "")
    # Prefer current experience
    current = None
    for exp in experiences:
        if exp.get("current"):
            current = exp
            break
    exp = current or experiences[0]
    position = exp.get("position", "") or exp.get("title", "")
    company = exp.get("company", "")
    return (position, company)

def collect_contacts(contacts):
    """Extract and categorize contact information"""
    emails_work, emails_personal = [], []
    mobile_phones, work_phones, home_phones = [], [], []
    
    for contact in contacts or []:
        contact_type = (contact.get("type", "") or "").lower()
        sub_type = (contact.get("subType", "") or "").lower()
        value = contact.get("value", "")
        
        if contact_type == "email":
            if "work" in sub_type or "business" in sub_type:
                emails_work.append(value)
            else:
                emails_personal.append(value)
        elif contact_type == "phone":
            if "mobile" in sub_type or "cell" in sub_type:
                mobile_phones.append(value)
            elif "work" in sub_type or "business" in sub_type:
                work_phones.append(value)
            elif "home" in sub_type:
                home_phones.append(value)
            else:
                # Default unknown phones to mobile
                mobile_phones.append(value)
    
    return (
        "; ".join(emails_work),
        "; ".join(emails_personal),
        mobile_phones[0] if mobile_phones else "",
        mobile_phones[1] if len(mobile_phones) > 1 else "",
        work_phones[0] if work_phones else "",
        work_phones[1] if len(work_phones) > 1 else "",
        home_phones[0] if home_phones else ""
    )

def skills_join(skills):
    """Join skills array into string"""
    if not skills:
        return ""
    if isinstance(skills, list):
        return "; ".join([str(s) for s in skills if s])
    return str(skills)

def education_join(education):
    """Join education array into formatted string"""
    if not education:
        return ""
    result = []
    for edu in education:
        parts = []
        if edu.get("university"):
            parts.append(edu["university"])
        if edu.get("faculty"):
            parts.append(edu["faculty"])
        if edu.get("degree"):
            if isinstance(edu["degree"], list):
                parts.append(", ".join(edu["degree"]))
            else:
                parts.append(str(edu["degree"]))
        if edu.get("startedYear") or edu.get("endedYear"):
            year_range = f"{edu.get('startedYear', '')}-{edu.get('endedYear', '')}".strip("-")
            if year_range:
                parts.append(year_range)
        if parts:
            result.append(" | ".join(parts))
    return "; ".join(result)

def load_status():
    """Load processing status from JSON file"""
    if os.path.exists(STATUS_JSON):
        try:
            with open(STATUS_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_status(status):
    """Save processing status to JSON file"""
    try:
        with open(STATUS_JSON, "w", encoding="utf-8") as f:
            json.dump(status, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving status: {e}")

@app.route('/signalhire/webhook', methods=['POST'])
def signalhire_webhook():
    """Receive SignalHire API callbacks and save enriched data to CSV"""
    try:
        # Get JSON payload
        payload = request.get_json(force=True, silent=True) or []
        batch_id = request.args.get("batch") or f"batch-{int(time.time())}"
        
        logger.info(f"Received SignalHire webhook: batch {batch_id}, {len(payload)} records")
        
        # Update status
        status = load_status()
        status[batch_id] = {
            "status": "running",
            "received_at": int(time.time()),
            "count_received": len(payload)
        }
        save_status(status)
        
        # Process each record and extract enriched data
        rows = []
        for item in payload:
            status_val = item.get("status", "")
            source_item = item.get("item", "")
            candidate = item.get("candidate") or {}
            
            # Extract basic info
            full_name = candidate.get("fullName", "")
            first_name, last_name = split_name(full_name)
            
            # Extract location
            country, city = pick_location(candidate.get("locations"))
            
            # Extract current experience
            position, company = current_experience(candidate.get("experience"))
            
            # Extract contacts
            contacts = candidate.get("contacts") or candidate.get("contact") or []
            emails_work, emails_personal, mob1, mob2, work1, work2, home = collect_contacts(contacts)
            
            # Extract LinkedIn from social
            linkedin_url = ""
            for social in candidate.get("social", []):
                if social.get("type") == "li" or "linkedin" in social.get("type", "").lower():
                    linkedin_url = social.get("link", "")
                    break
            
            # If no LinkedIn in social, use original item if it's a LinkedIn URL
            if not linkedin_url and "linkedin.com" in source_item:
                linkedin_url = source_item
            
            row = {
                "Item": source_item,
                "Status": status_val,
                "First Name": first_name,
                "Last Name": last_name,
                "Full Name": full_name,
                "Current Position": position,
                "Company": company,
                "Country": country,
                "City": city,
                "Emails (Work)": emails_work,
                "Emails (Personal)": emails_personal,
                "Mobile Phone1": mob1,
                "Mobile Phone2": mob2,
                "Work Phone1": work1,
                "Work Phone2": work2,
                "Home Phone": home,
                "LinkedIn": linkedin_url,
                "Skills": skills_join(candidate.get("skills")),
                "Education": education_join(candidate.get("education")),
                "Received At": datetime.now().isoformat()
            }
            rows.append(row)
        
        # Write to CSV with thread safety
        with write_lock:
            need_header = not os.path.exists(RESULTS_CSV) or os.path.getsize(RESULTS_CSV) == 0
            with open(RESULTS_CSV, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                if need_header:
                    writer.writeheader()
                for row in rows:
                    writer.writerow(row)
        
        # Update final status
        status = load_status()
        status[batch_id].update({
            "status": "completed",
            "completed_at": int(time.time()),
            "count_written": len(rows),
            "results_path": RESULTS_CSV
        })
        save_status(status)
        
        logger.info(f"Saved {len(rows)} enriched records to {RESULTS_CSV}")
        
        return jsonify({
            "ok": True,
            "batch": batch_id,
            "written": len(rows)
        })
        
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/results.csv', methods=['GET'])
def download_results():
    """Download enriched results CSV file"""
    try:
        if Path(RESULTS_CSV).exists():
            return send_file(RESULTS_CSV, as_attachment=True, download_name='signalhire_enriched_results.csv')
        else:
            return jsonify({"error": "Results file not found"}), 404
    except Exception as e:
        logger.error(f"Error downloading results: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/status', methods=['GET'])
def get_status():
    """Get processing status"""
    try:
        status = load_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/', methods=['GET'])
def root():
    """Root endpoint"""
    return jsonify({
        "service": "SignalHire Enriched Webhook Receiver",
        "status": "running",
        "version": "2.0 - Full Candidate Data Extraction",
        "endpoints": {
            "health": "/health",
            "webhook": "/signalhire/webhook",
            "results": "/results.csv",
            "status": "/status"
        },
        "columns": COLUMNS
    })

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
