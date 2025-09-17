#!/usr/bin/env python3
"""
SignalHire Webhook Receiver for Northflank
Enhanced version to handle SignalHire's new rich data format
"""

from flask import Flask, request, jsonify
import pandas as pd
import json
import os
from datetime import datetime
from pathlib import Path
import logging

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
            "webhook": "/signalhire/webhook"
        }
    })

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/signalhire/webhook', methods=['POST'])
def signalhire_webhook():
    """Enhanced webhook endpoint to handle SignalHire callbacks with rich data"""
    
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
        
        # Process each record with enhanced data extraction
        csv_records = []
        for record in records:
            # Extract all available fields from SignalHire's rich data
            csv_record = {
                'item': record.get('item', record.get('linkedin', '')),
                'status': record.get('status', ''),
                'first_name': record.get('firstName', ''),
                'last_name': record.get('lastName', ''),
                'full_name': record.get('fullName', ''),
                'job_title': record.get('jobTitle', record.get('title', '')),
                'company': record.get('company', record.get('companyName', '')),
                'location': record.get('location', ''),
                'country': record.get('country', ''),
                'city': record.get('city', ''),
                'industry': record.get('industry', ''),
                'seniority': record.get('seniority', ''),
                'department': record.get('department', ''),
                'skills': json.dumps(record.get('skills', [])) if record.get('skills') else '',
                'education': json.dumps(record.get('education', [])) if record.get('education') else '',
                'experience': json.dumps(record.get('experience', [])) if record.get('experience') else '',
                'linkedin': record.get('item', record.get('linkedin', '')),
                'received_at': datetime.now().isoformat()
            }
            
            # Handle emails - can be array or string
            emails = record.get('emails', [])
            if isinstance(emails, list):
                csv_record['emails'] = json.dumps(emails) if emails else ''
            else:
                csv_record['emails'] = str(emails) if emails else ''
            
            # Handle phones - can be array or string  
            phones = record.get('phones', [])
            if isinstance(phones, list):
                csv_record['phones'] = json.dumps(phones) if phones else ''
            else:
                csv_record['phones'] = str(phones) if phones else ''
            
            csv_records.append(csv_record)
        
        # Save to CSV
        df = pd.DataFrame(csv_records)
        
        # Append to existing file or create new
        if Path(RESULTS_CSV).exists():
            df.to_csv(RESULTS_CSV, mode='a', header=False, index=False)
        else:
            df.to_csv(RESULTS_CSV, index=False)
        
        logger.info(f"Saved {len(csv_records)} records to {RESULTS_CSV}")
        
        return jsonify({
            "status": "success",
            "records_processed": len(csv_records),
            "saved_to": RESULTS_CSV
        })
        
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/results.csv', methods=['GET'])
def get_results():
    """Endpoint to download the results CSV file"""
    try:
        if Path(RESULTS_CSV).exists():
            with open(RESULTS_CSV, 'r', encoding='utf-8') as f:
                content = f.read()
            return content, 200, {'Content-Type': 'text/csv'}
        else:
            return "No results file found", 404
    except Exception as e:
        return f"Error reading results: {str(e)}", 500

@app.route('/status', methods=['GET'])
def get_status():
    """Get webhook server status and statistics"""
    try:
        stats = {
            "status": "running",
            "results_file_exists": Path(RESULTS_CSV).exists(),
            "timestamp": datetime.now().isoformat()
        }
        
        if Path(RESULTS_CSV).exists():
            df = pd.read_csv(RESULTS_CSV)
            stats.update({
                "total_records": len(df),
                "successful_enrichments": len(df[df['status'] == 'success']),
                "failed_enrichments": len(df[df['status'] == 'failed']),
                "file_size_mb": round(Path(RESULTS_CSV).stat().st_size / 1024 / 1024, 2)
            })
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
