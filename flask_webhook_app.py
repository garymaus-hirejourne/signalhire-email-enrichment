#!/usr/bin/env python3
"""
SignalHire Webhook Receiver for Northflank
Receives SignalHire API callbacks and saves results to CSV
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

# Configuration - fallback to /tmp if /data not available
RESULTS_CSV = os.getenv('SIGNALHIRE_RESULTS_CSV', '/data/results.csv')
DATA_DIR = Path(RESULTS_CSV).parent

# Create fallback directory if /data is not accessible
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except PermissionError:
    # Fallback to /tmp if /data volume not mounted
    RESULTS_CSV = '/tmp/results.csv'
    DATA_DIR = Path(RESULTS_CSV).parent
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.warning(f"Cannot access /data directory, using fallback: {RESULTS_CSV}")

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/signalhire/webhook', methods=['POST'])
def signalhire_webhook():
    """Receive SignalHire API callbacks and save to CSV"""
    try:
        # Data directory already handled in configuration section
        
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
        
        # Prepare CSV data
        csv_records = []
        for record in records:
            csv_record = {
                'item': record.get('item', ''),
                'status': record.get('status', ''),
                'fullName': record.get('fullName', ''),
                'emails': json.dumps(record.get('emails', [])) if record.get('emails') else '',
                'phones': json.dumps(record.get('phones', [])) if record.get('phones') else '',
                'linkedin': record.get('linkedin', ''),
                'received_at': datetime.now().isoformat()
            }
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
        logger.error(f"Error processing webhook: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/results.csv', methods=['GET'])
def download_results():
    """Download results CSV file"""
    try:
        if Path(RESULTS_CSV).exists():
            from flask import send_file
            return send_file(RESULTS_CSV, as_attachment=True, download_name='signalhire_results.csv')
        else:
            return jsonify({"error": "Results file not found"}), 404
    except Exception as e:
        logger.error(f"Error downloading results: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/', methods=['GET'])
def root():
    """Root endpoint"""
    return jsonify({
        "service": "SignalHire Webhook Receiver",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "webhook": "/signalhire/webhook",
            "results": "/results.csv"
        }
    })

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
