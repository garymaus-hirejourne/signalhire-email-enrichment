# Northflank Environment Setup Checklist for Wound Specialist Campaign

## Current Setup Status (Based on Memory)

Your existing SignalHire webhook infrastructure:
- **GitHub Repository**: https://github.com/garymaus-hirejourne/signalhire-email-enrichment
- **Northflank Project**: signalhire-api-email-scraper
- **Account**: https://app.northflank.com/t/garymauss-team/project/signalhire-api-email-scraper

## ✅ Required Components Checklist

### 1. Northflank Account & Project
- [ ] **Account Status**: Verify you have a PAID Northflank account (required for volume mounting)
- [ ] **Project Exists**: signalhire-api-email-scraper project is active
- [ ] **Deployment Status**: Webhook service is running and healthy

### 2. Webhook Endpoint Configuration
- [ ] **Endpoint Active**: `POST /signalhire/webhook` is responding
- [ ] **Health Check**: `GET /health` returns 200 OK
- [ ] **URL Format**: `https://your-app.northflank.app/signalhire/webhook`

### 3. Volume Mounting (CRITICAL)
- [ ] **Volume Created**: `/data` volume exists in Northflank project
- [ ] **Volume Mounted**: Volume mounted at `/data` path in container
- [ ] **Write Permissions**: Container can write to `/data/results.csv`
- [ ] **Account Upgraded**: Free account upgraded to paid for volume support

### 4. Environment Variables
- [ ] **SIGNALHIRE_RESULTS_CSV**: Set to `/data/results.csv`
- [ ] **Other Required Env Vars**: Any additional configuration variables

### 5. Application Code
- [ ] **Flask App**: Webhook receiver application deployed
- [ ] **Dependencies**: All Python packages installed (Flask, pandas, etc.)
- [ ] **Error Handling**: Proper logging and error handling in place

## 🔍 Verification Steps

### Step 1: Check Northflank Dashboard
1. Go to: https://app.northflank.com/t/garymauss-team/project/signalhire-api-email-scraper
2. Verify service is running (green status)
3. Check recent logs for any errors
4. Confirm volume is mounted

### Step 2: Test Webhook Endpoint
```bash
# Test health endpoint
curl https://your-app.northflank.app/health

# Expected response: {"status": "healthy"}
```

### Step 3: Test Volume Writing
```bash
# SSH into container or check logs to verify /data is writable
# Look for any permission errors in application logs
```

### Step 4: Verify Account Status
1. Check Northflank billing section
2. Confirm you're on a paid plan (required for persistent volumes)
3. Verify volume storage limits

## 🚨 Common Issues to Check

### Volume Mounting Issues
- **Free Account Limitation**: Northflank free accounts don't support persistent volumes
- **Mount Path**: Ensure volume is mounted at exactly `/data`
- **Permissions**: Container user must have write access to mounted volume

### Webhook Connectivity
- **URL Changes**: Northflank URLs may change if service is redeployed
- **SSL/TLS**: Ensure webhook URL uses HTTPS
- **Firewall**: No blocking of incoming webhook requests

### Application Issues
- **Dependencies**: Missing Python packages in deployment
- **Environment Variables**: Missing or incorrect configuration
- **File Permissions**: Cannot write to results file

## 🔧 Quick Fixes

### If Volume Not Working
1. Upgrade to paid Northflank account
2. Create new volume in project settings
3. Redeploy service with volume mount

### If Webhook Not Responding
1. Check service logs in Northflank dashboard
2. Verify application is listening on correct port
3. Test with simple curl request

### If CSV Not Saving
1. Check `/data` directory permissions
2. Verify SIGNALHIRE_RESULTS_CSV environment variable
3. Look for write errors in application logs

## 📋 Pre-Campaign Verification Commands

Run these before starting the wound specialist campaign:

```bash
# 1. Test webhook health
curl https://your-app.northflank.app/health

# 2. Check if previous results file exists
# (Should be accessible via your Northflank file browser or logs)

# 3. Test SignalHire API connectivity
python -c "
import requests
headers = {'Authorization': 'Bearer YOUR_API_KEY'}
response = requests.get('https://www.signalhire.com/api/v1/account', headers=headers)
print(f'API Status: {response.status_code}')
"

# 4. Verify wound specialist pipeline can connect
python wound_specialist_pipeline.py --test-mode --signalhire-api-key YOUR_KEY --webhook-url YOUR_WEBHOOK_URL
```

## 🎯 Expected Output Format

Your webhook should save results in this format to `/data/results.csv`:
```csv
item,status,fullName,emails,phones,linkedin,received_at
contact_1,success,John Smith,john.smith@clinic.com,(555) 123-4567,https://linkedin.com/in/johnsmith,2024-01-01T12:00:00Z
```

## ⚠️ Critical Requirements for Wound Specialist Campaign

1. **Paid Account**: Volume mounting requires paid Northflank subscription
2. **Webhook URL**: Must be accessible and responding to health checks
3. **Volume Storage**: Sufficient space for CSV results (typically <10MB)
4. **API Limits**: Ensure SignalHire API has sufficient credits/quota

## 🔗 Useful Links

- **Northflank Dashboard**: https://app.northflank.com/t/garymauss-team/project/signalhire-api-email-scraper
- **GitHub Repo**: https://github.com/garymaus-hirejourne/signalhire-email-enrichment
- **SignalHire API Docs**: https://www.signalhire.com/api
- **Volume Creation**: https://app.northflank.com/t/garymauss-team/project/signalhire-api-email-scraper/create/volume

---

**Next Step**: Complete this checklist, then run the wound specialist pipeline with confidence that your Northflank environment is properly configured.
