#!/usr/bin/env python3
"""
SignalHire Webhook Receiver for Northflank - Enhanced Version
Includes per-request batch management and rich contact data extraction
Updated: September 18, 2025 - Fixing missing contact data issue
"""

from flask import Flask, request, jsonify
import pandas as pd
import json
import os
from datetime import datetime
from pathlib import Path
import logging
import hashlib

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
RESULTS_CSV = os.getenv('SIGNALHIRE_RESULTS_CSV', '/data/results.csv')
DATA_DIR = Path(RESULTS_CSV).parent

@app.route('/', methods=['GET'])
def root():
    """Root endpoint"""
    return jsonify({
        "service": "SignalHire Webhook Receiver",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "webhook": "/signalhire/webhook",
            "results": "/results.csv",
            "batches": "/batches",
            "status": "/status"
        }
    })

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/signalhire/webhook', methods=['POST'])
def signalhire_webhook():
    """Enhanced webhook endpoint with per-request file management"""
    
    try:
        # Ensure data directory exists
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        # Get JSON payload
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data received"}), 400
        
        logger.info(f"Received SignalHire webhook data: {len(data) if isinstance(data, list) else 1} records")
        
        # Process webhook data
        if isinstance(data, list):
            records = data
        else:
            records = [data]
        
        # Generate unique identifier for this enrichment batch
        timestamp = datetime.now()
        batch_id = hashlib.md5(f"{timestamp.isoformat()}_{len(records)}".encode()).hexdigest()[:8]
        
        # Create separate file for this enrichment request
        batch_filename = f"enrichment_{timestamp.strftime('%Y%m%d_%H%M%S')}_{batch_id}.csv"
        batch_filepath = DATA_DIR / batch_filename
        
        # Process each record with correct SignalHire data extraction
        csv_records = []
        for record in records:
            # Get the candidate object (contains all the rich data)
            candidate = record.get('candidate', {})
            
            # Extract basic info
            full_name = candidate.get('fullName', '')
            name_parts = full_name.split(' ', 1) if full_name else ['', '']
            first_name = name_parts[0] if len(name_parts) > 0 else ''
            last_name = name_parts[1] if len(name_parts) > 1 else ''
            
            # Extract location info
            locations = candidate.get('locations', [])
            location = locations[0].get('name', '') if locations else ''
            
            # Parse location for country/city
            location_parts = location.split(', ') if location else []
            city = location_parts[0] if len(location_parts) > 0 else ''
            country = location_parts[-1] if len(location_parts) > 0 else ''
            
            # Extract job info from experience (most recent)
            experience = candidate.get('experience', [])
            current_job = experience[0] if experience else {}
            job_title = current_job.get('position', candidate.get('headLine', ''))
            company = current_job.get('company', '')
            industry = current_job.get('industry', '')
            
            # Extract contact information
            contacts = candidate.get('contacts', [])
            emails = [c['value'] for c in contacts if c.get('type') == 'email']
            phones = [c['value'] for c in contacts if c.get('type') == 'phone']
            
            # Extract skills
            skills = candidate.get('skills', [])
            
            # Extract education
            education = candidate.get('education', [])
            
            # Build the correctly mapped record
            csv_record = {
                'batch_id': batch_id,
                'item': record.get('item', ''),
                'status': record.get('status', ''),
                'first_name': first_name,
                'last_name': last_name,
                'full_name': full_name,
                'job_title': job_title,
                'company': company,
                'location': location,
                'country': country,
                'city': city,
                'industry': industry,
                'seniority': '',  # Not directly available in SignalHire data
                'department': '',  # Not directly available in SignalHire data
                'skills': ', '.join(skills) if skills else '',
                'education': json.dumps(education) if education else '',
                'experience': json.dumps(experience) if experience else '',
                'linkedin': record.get('item', ''),
                'received_at': timestamp.isoformat(),
                'emails': ', '.join(emails) if emails else '',
                'phones': ', '.join(phones) if phones else ''
            }
            
            csv_records.append(csv_record)
        
        # Save to separate batch file
        df = pd.DataFrame(csv_records)
        df.to_csv(batch_filepath, index=False)
        
        # Also append to main accumulated file for backward compatibility
        if Path(RESULTS_CSV).exists():
            df.to_csv(RESULTS_CSV, mode='a', header=False, index=False)
        else:
            df.to_csv(RESULTS_CSV, index=False)
        
        logger.info(f"Saved {len(csv_records)} records to batch file: {batch_filename}")
        logger.info(f"Also appended to main file: {RESULTS_CSV}")
        
        return jsonify({
            "status": "success",
            "records_processed": len(csv_records),
            "batch_id": batch_id,
            "batch_file": str(batch_filename),
            "main_file": str(RESULTS_CSV),
            "timestamp": timestamp.isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/results.csv', methods=['GET'])
def get_results():
    """Endpoint to download the main accumulated results CSV file"""
    try:
        if Path(RESULTS_CSV).exists():
            with open(RESULTS_CSV, 'r', encoding='utf-8') as f:
                content = f.read()
            return content, 200, {'Content-Type': 'text/csv'}
        else:
            return "No results file found", 404
    except Exception as e:
        return f"Error reading results: {str(e)}", 500

@app.route('/batch/<batch_id>', methods=['GET'])
def get_batch_results(batch_id):
    """Endpoint to download results for a specific batch"""
    try:
        # Find batch file by ID
        batch_files = list(DATA_DIR.glob(f"enrichment_*_{batch_id}.csv"))
        if not batch_files:
            return f"No batch file found for ID: {batch_id}", 404
        
        batch_file = batch_files[0]
        with open(batch_file, 'r', encoding='utf-8') as f:
            content = f.read()
        return content, 200, {'Content-Type': 'text/csv'}
    except Exception as e:
        return f"Error reading batch results: {str(e)}", 500

@app.route('/batches', methods=['GET'])
def list_batches():
    """List all available batch files"""
    try:
        batch_files = list(DATA_DIR.glob("enrichment_*.csv"))
        batches = []
        
        for batch_file in sorted(batch_files, reverse=True):  # Most recent first
            try:
                df = pd.read_csv(batch_file)
                batch_info = {
                    "filename": batch_file.name,
                    "batch_id": df['batch_id'].iloc[0] if 'batch_id' in df.columns and len(df) > 0 else "unknown",
                    "records": len(df),
                    "timestamp": df['received_at'].iloc[0] if 'received_at' in df.columns and len(df) > 0 else "unknown",
                    "size_mb": round(batch_file.stat().st_size / 1024 / 1024, 2)
                }
                batches.append(batch_info)
            except Exception as e:
                logger.warning(f"Error reading batch file {batch_file}: {e}")
        
        return jsonify({
            "total_batches": len(batches),
            "batches": batches
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/status', methods=['GET'])
def get_status():
    """Get webhook server status and statistics"""
    try:
        stats = {
            "status": "running",
            "main_file_exists": Path(RESULTS_CSV).exists(),
            "timestamp": datetime.now().isoformat()
        }
        
        # Main file stats
        if Path(RESULTS_CSV).exists():
            df = pd.read_csv(RESULTS_CSV)
            stats.update({
                "main_file_records": len(df),
                "main_file_successful": len(df[df['status'] == 'success']) if 'status' in df.columns else 0,
                "main_file_failed": len(df[df['status'] == 'failed']) if 'status' in df.columns else 0,
                "main_file_size_mb": round(Path(RESULTS_CSV).stat().st_size / 1024 / 1024, 2)
            })
        
        # Batch files stats
        batch_files = list(DATA_DIR.glob("enrichment_*.csv"))
        stats["total_batch_files"] = len(batch_files)
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
