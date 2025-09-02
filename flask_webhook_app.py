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

@app.route('/process', methods=['POST'])
def process_data():
    """Process and clean the results CSV data"""
    try:
        if not Path(RESULTS_CSV).exists():
            return jsonify({"error": "No results file to process"}), 404
            
        cleaned_file = RESULTS_CSV.replace('.csv', '_cleaned.csv')
        
        # Process the data
        count = process_enriched_csv(RESULTS_CSV, cleaned_file)
        
        return jsonify({
            "status": "success",
            "records_processed": count,
            "cleaned_file": cleaned_file,
            "download_url": "/cleaned.csv"
        })
        
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/cleaned.csv', methods=['GET'])
def download_cleaned():
    """Download cleaned and processed results CSV"""
    try:
        cleaned_file = RESULTS_CSV.replace('.csv', '_cleaned.csv')
        if Path(cleaned_file).exists():
            return send_file(cleaned_file, as_attachment=True, download_name='signalhire_cleaned_results.csv')
        else:
            return jsonify({"error": "Cleaned file not found. Run /process first."}), 404
    except Exception as e:
        logger.error(f"Error downloading cleaned results: {e}")
        return jsonify({"error": str(e)}), 500

def process_enriched_csv(input_file, output_file):
    """Process and clean SignalHire enriched data with enhanced validation and multi-value splitting"""
    headers = [
        "LinkedIn Profile", "Status", "First Name", "Last Name", "Full Name",
        "Current Position", "Company", "Country", "City", 
        "Email1", "Email2", "Email3", "Phone1", "Phone2", "Phone3",
        "Skills", "Education"
    ]
    
    processed_records = []
    seen_profiles = set()
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for line in lines[1:]:  # Skip header
        if not line.strip():
            continue
            
        # Parse CSV line handling quotes
        parts = []
        current_part = ""
        in_quotes = False
        
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                parts.append(current_part.strip())
                current_part = ""
            else:
                current_part += char
        
        if current_part:
            parts.append(current_part.strip())
        
        # Skip failed or insufficient data
        if len(parts) < 15 or parts[1] != 'success':
            continue
            
        linkedin_url = parts[0]
        if linkedin_url in seen_profiles:
            continue
        seen_profiles.add(linkedin_url)
        
        # Extract and clean basic fields
        first_name = clean_text(parts[2])
        last_name = clean_text(parts[3])
        company = clean_text(parts[6])
        
        # Collect all emails and phones
        all_emails = []
        all_phones = []
        
        # Extract emails from work and personal fields
        work_emails = extract_multi_values(parts[9])
        personal_emails = extract_multi_values(parts[10])
        all_emails.extend(work_emails)
        all_emails.extend(personal_emails)
        
        # Extract phones from mobile, work, home fields
        mobile_phones = extract_multi_values(parts[11])
        work_phones = extract_multi_values(parts[13]) if len(parts) > 13 else []
        home_phones = extract_multi_values(parts[15]) if len(parts) > 15 else []
        all_phones.extend(mobile_phones)
        all_phones.extend(work_phones)
        all_phones.extend(home_phones)
        
        # Clean and deduplicate contacts
        clean_emails = clean_and_dedupe_emails(all_emails)
        clean_phones = clean_and_dedupe_phones(all_phones)
        
        # VALIDATION: Skip rows missing required fields
        if not (first_name and last_name and company and (clean_emails or clean_phones)):
            continue
        
        # Build record with split contact fields
        record = {
            "LinkedIn Profile": linkedin_url,
            "Status": "Success", 
            "First Name": first_name,
            "Last Name": last_name,
            "Full Name": clean_text(parts[4]),
            "Current Position": clean_text(parts[5]),
            "Company": company,
            "Country": parts[7],
            "City": parts[8],
            "Email1": clean_emails[0] if len(clean_emails) > 0 else "",
            "Email2": clean_emails[1] if len(clean_emails) > 1 else "",
            "Email3": clean_emails[2] if len(clean_emails) > 2 else "",
            "Phone1": clean_phones[0] if len(clean_phones) > 0 else "",
            "Phone2": clean_phones[1] if len(clean_phones) > 1 else "",
            "Phone3": clean_phones[2] if len(clean_phones) > 2 else "",
            "Skills": clean_skills(parts[17]) if len(parts) > 17 else "",
            "Education": clean_education(parts[18]) if len(parts) > 18 else ""
        }
        
        processed_records.append(record)
    
    # Write cleaned results
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(processed_records)
    
    return len(processed_records)

def clean_text(text):
    """Clean text by removing quotes and extra whitespace"""
    if not text:
        return ""
    return text.replace('"', '').strip()

def extract_multi_values(field):
    """Extract multiple values from semicolon or comma separated field"""
    if not field:
        return []
    
    # Split by semicolon first, then comma
    values = []
    for item in field.split(';'):
        for subitem in item.split(','):
            clean_item = subitem.strip()
            if clean_item:
                values.append(clean_item)
    
    return values

def clean_and_dedupe_emails(emails):
    """Clean and deduplicate email addresses"""
    clean_emails = []
    seen = set()
    
    for email in emails:
        clean_email = email.strip().lower()
        if clean_email and '@' in clean_email and clean_email not in seen:
            clean_emails.append(clean_email)
            seen.add(clean_email)
    
    return clean_emails[:3]  # Limit to 3 emails

def clean_and_dedupe_phones(phones):
    """Clean and deduplicate phone numbers"""
    clean_phones = []
    seen = set()
    
    for phone in phones:
        # Remove all formatting
        clean_phone = ''.join(filter(str.isdigit, phone))
        
        # Remove leading 1 if it's 11 digits (US country code)
        if len(clean_phone) == 11 and clean_phone.startswith('1'):
            clean_phone = clean_phone[1:]
        
        # Only keep 10-digit US numbers
        if len(clean_phone) == 10 and clean_phone not in seen:
            # Format as (XXX) XXX-XXXX
            formatted = f"({clean_phone[:3]}) {clean_phone[3:6]}-{clean_phone[6:]}"
            clean_phones.append(formatted)
            seen.add(clean_phone)
    
    return clean_phones[:3]  # Limit to 3 phones

def clean_phone(phone):
    """Clean phone number format - legacy function"""
    if not phone:
        return ""
    return phone.replace('+1 ', '').replace('+1', '').replace('(', '').replace(')', '').replace('-', '').strip()

def clean_skills(skills):
    """Limit skills to top 8 for readability"""
    if not skills:
        return ""
    skill_list = [s.strip() for s in skills.split(';')]
    return '; '.join(skill_list[:8])

def clean_education(education):
    """Limit education to top 2 entries"""
    if not education:
        return ""
    edu_list = [e.strip() for e in education.split(';')]
    return '; '.join(edu_list[:2])

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
            "status": "/status",
            "process": "/process",
            "cleaned": "/cleaned.csv"
        },
        "columns": COLUMNS
    })

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
