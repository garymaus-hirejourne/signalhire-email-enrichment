# Northflank Volume Setup Guide for SignalHire Webhook

## 🚨 Prerequisites

### 1. Account Upgrade (REQUIRED)
- **Free accounts cannot create persistent volumes**
- You must upgrade to a paid plan first
- Go to: https://app.northflank.com/billing
- Choose Developer ($20/month) or Team plan

### 2. Access Your Project
- Navigate to: https://app.northflank.com/t/garymauss-team/project/signalhire-api-email-scraper

## 📁 Step-by-Step Volume Creation

### Step 1: Create the Volume
1. **Go to Volumes Section**
   - In your project dashboard, click **"Volumes"** in the left sidebar
   - Or visit: https://app.northflank.com/t/garymauss-team/project/signalhire-api-email-scraper/volumes

2. **Create New Volume**
   - Click **"Create Volume"** button
   - Or use direct link: https://app.northflank.com/t/garymauss-team/project/signalhire-api-email-scraper/create/volume

3. **Volume Configuration**
   ```
   Volume Name: signalhire-results-storage
   Size: 1GB (sufficient for CSV files)
   Type: Single Read/Write (recommended for single service)
   ```

   **IMPORTANT**: 
   - **Skip "Attach to resources" section** during volume creation
   - **Leave "Select services" dropdown empty**
   - Create the volume first, then attach it separately

   **Volume Type Options:**
   - **Single Read/Write**: One service can read/write (choose this)
   - **Multi Read/Write**: Multiple services can access (not needed for webhook)

### Step 2: Mount Volume to Service

1. **Access Your Service**
   - Go to **"Services"** in left sidebar
   - Find your webhook service (likely named something like "signalhire-webhook")
   - Click on the service name

2. **Edit Service Configuration**
   - Click **"Settings"** tab
   - Scroll to **"Volumes"** section
   - Click **"Add Volume Mount"**

3. **Volume Mount Settings**
   ```
   Volume: signalhire-results-storage (select from dropdown)
   Mount Path: /data
   Read/Write: Read & Write
   ```

### Step 3: Environment Variables

1. **In Service Settings**
   - Go to **"Environment"** tab
   - Add/verify this environment variable:
   ```
   SIGNALHIRE_RESULTS_CSV=/data/results.csv
   ```

2. **Other Required Variables**
   ```
   PORT=8080
   FLASK_ENV=production
   ```

### Step 4: Redeploy Service

1. **Deploy Changes**
   - Click **"Deploy"** button to apply volume mount
   - Wait for deployment to complete (green status)

2. **Verify Deployment**
   - Check service logs for any mount errors
   - Look for successful startup messages

## 🔍 Verification Steps

### Test Volume Mount
1. **Check Service Logs**
   - Look for messages about `/data` directory
   - Verify no permission errors

2. **Test Write Access**
   - Your Flask app should be able to create files in `/data/`
   - Check logs for any "Permission denied" errors

### Test Webhook Endpoint
```bash
# Replace with your actual Northflank URL
curl https://your-actual-service-url.northflank.app/health
```

Expected response:
```json
{"status": "healthy"}
```

## 🛠️ Common Issues & Solutions

### Issue 1: "Volume not available"
**Cause**: Free account limitation
**Solution**: Upgrade to paid plan first

### Issue 2: "Permission denied" in /data
**Cause**: Container user doesn't have write permissions
**Solution**: 
- Check Dockerfile USER directive
- Ensure volume mount has Read & Write permissions

### Issue 3: Service won't start after volume mount
**Cause**: Mount path conflicts or missing directories
**Solution**:
- Verify mount path is `/data` (not `/data/`)
- Check service logs for specific errors

### Issue 4: CSV file not being created
**Cause**: Application not writing to correct path
**Solution**:
- Verify `SIGNALHIRE_RESULTS_CSV=/data/results.csv`
- Check Flask app code for file writing logic

## 📋 Final Configuration Checklist

- [ ] **Account upgraded to paid plan**
- [ ] **Volume created**: `signalhire-results-storage` (1GB)
- [ ] **Volume mounted**: at `/data` with Read & Write permissions
- [ ] **Environment variable set**: `SIGNALHIRE_RESULTS_CSV=/data/results.csv`
- [ ] **Service redeployed** successfully
- [ ] **Health endpoint responding**: `/health` returns 200 OK
- [ ] **Logs show no errors** related to volume mounting

## 🎯 Expected File Structure

After setup, your container should have:
```
/data/
├── results.csv (created by webhook when SignalHire sends data)
└── (other temporary files as needed)
```

## 🔗 Direct Links for Your Project

- **Project Dashboard**: https://app.northflank.com/t/garymauss-team/project/signalhire-api-email-scraper
- **Create Volume**: https://app.northflank.com/t/garymauss-team/project/signalhire-api-email-scraper/create/volume
- **Volumes List**: https://app.northflank.com/t/garymauss-team/project/signalhire-api-email-scraper/volumes
- **Billing/Upgrade**: https://app.northflank.com/billing

## ⚡ Quick Setup Commands

Once volume is created and mounted, test with:

```bash
# Test webhook health
curl https://your-service-url.northflank.app/health

# Test SignalHire integration (replace with your API key and webhook URL)
python wound_specialist_pipeline.py --test-mode --signalhire-api-key YOUR_KEY --webhook-url YOUR_WEBHOOK_URL
```

## 🚀 Ready for Wound Specialist Campaign

After completing these steps, your Northflank environment will be ready to:
- Receive SignalHire webhook callbacks
- Store enriched contact data in `/data/results.csv`
- Support the wound specialist scraping pipeline
- Handle batch processing of 200-500 specialists

The volume setup is the final piece needed before running your wound specialist campaign successfully.
