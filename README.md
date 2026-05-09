# SCM Network Analysis MVP Backend

Clean FastAPI backend for Supply Chain Management Network Analysis with Azure Blob Storage integration.

## Features

- **Network Analysis Endpoints**: Nearest hub, network coverage, network status
- **CSV Upload & Merge**: Upload CSV files and merge with baseline data
- **Azure Blob Storage**: Automatic download of large datasets at startup
- **MVP Configuration**: Environment-based configuration for development/production
- **Render Ready**: Configured for deployment on Render Web Service

## Quick Start

### Local Development

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Run the server:**
   ```bash
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

4. **Access API docs:** http://localhost:8000/docs

## Environment Variables

### Required for Production
```bash
# Azure Blob Storage URLs
BASELINE_DATA_URL=https://scmdata2026.blob.core.windows.net/data/combined_df.csv
MASTER_DATA_URL=https://scmdata2026.blob.core.windows.net/data/Master_data_with_pincodes.csv

# Environment
ENVIRONMENT=production
CORS_ORIGINS=https://your-frontend-domain.com
```

### Optional (with defaults)
```bash
MVP_MODE=true                    # MVP mode flag
STORAGE_TYPE=sqlite              # Storage backend
MAX_FILE_SIZE_MB=20              # Upload file size limit
REQUEST_TIMEOUT_SECONDS=25       # Request timeout
```

## Azure Blob Storage Integration

The backend automatically downloads large CSV files from Azure Blob Storage on startup:

- **Baseline Dataset**: Downloads to `./data/combined_df_baseline.csv`
- **Master Pincode Data**: Downloads to `./data/Master_data_with_pincodes.csv`

Files are only downloaded if they don't exist locally, ensuring fast subsequent startups.

## API Endpoints

### Network Analysis
- `GET /api/network/nearest-hub` - Find nearest hub for PIN code
- `GET /api/network/network-coverage` - Get network coverage analysis
- `GET /api/network/network-status` - Network status overview

### Data Management
- `POST /api/network/upload-csv` - Upload CSV file
- `POST /api/network/merge-csv` - Merge uploaded CSV with baseline
- `GET /api/network/merge-preview/{filename}` - Preview merge results

### System
- `GET /` - API root with service info
- `GET /health` - Health check endpoint

## Deployed Swagger Verification

Use `https://scm-backend-mvp.onrender.com/docs` as the source of truth for the live Render deployment.

### What should appear in Swagger
- `GET /`
- `GET /health`
- `POST /api/network/nearest-hub`
- `POST /api/network/network-coverage`
- `POST /api/network/network-optimization`
- `POST /api/network/dispatch-analysis`
- `GET /api/network/network-status`
- `POST /api/network/dispatch-compliance`
- `POST /api/network/order-risk`
- `POST /api/network/upload-combined-df`
- `POST /api/network/comprehensive-baseline`
- `POST /api/network/comprehensive-compliance`
- `POST /api/network/risk-analysis`
- `POST /api/network/upload-csv`
- `POST /api/network/merge-csv`
- `GET /api/network/merge-preview/{filename}`

### Recommended manual test order
1. Open `/docs` and confirm the `network-design` tag is present.
2. Run `GET /health` first. It should return `{"status": "healthy"}`.
3. Run `GET /` next. It should return service metadata plus the documented endpoint list.
4. Test the simple POST routes before the file-upload routes.
5. Finish with `upload-csv`, `merge-csv`, and `merge-preview/{filename}` because those depend on multipart form data or previously saved files.

### Swagger request examples
- `POST /api/network/nearest-hub`
   - JSON body: `{"pincode": "110001"}`
- `POST /api/network/network-coverage`
   - JSON body: `{"pincodes": ["110001", "560001"]}`
- `POST /api/network/order-risk`
   - JSON body: `{"order_no": "ORD001", "sku": "SKU123", "customer_pincode": "110001", "delivery_period": 2}`
- `POST /api/network/comprehensive-baseline`
   - Try `data_source=existing` first
   - Optional form fields: `limit=1000`, `data_source=custom`
- `POST /api/network/comprehensive-compliance`
   - JSON body: `{"cost_per_km": 2.5}`
- `POST /api/network/upload-csv`
   - Multipart form fields: `order_data`, `pick_data`
- `POST /api/network/merge-csv`
   - Multipart form fields: `order_data`, `pick_data`, optional `output_filename`
- `GET /api/network/merge-preview/{filename}`
   - Example: `/api/network/merge-preview/combined_df.csv`

### Important deployment note
- The live app currently registers only the network router in `app/main.py`.
- The `what-if`, `ML`, telemetry, and mock-data routes exist in source files, but they are not guaranteed to appear in Swagger on the deployed Render host unless they are explicitly included in the running app.

## Render Deployment Instructions

### 1. Create Render Web Service
- Go to https://render.com
- Click "New" → "Web Service"
- Connect GitHub repository: `okaysarith/scm-backend-mvp`
- Runtime: Python 3
- Build Command: `pip install -r requirements.txt`
- Start Command: `gunicorn app.main:app --workers 1 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT`

### 2. Set Environment Variables in Render
```
BASELINE_DATA_URL=https://scmdata2026.blob.core.windows.net/data/combined_df.csv
MASTER_DATA_URL=https://scmdata2026.blob.core.windows.net/data/Master_data_with_pincodes.csv
MVP_MODE=true
ENVIRONMENT=production
STORAGE_TYPE=sqlite
MAX_FILE_SIZE_MB=20
REQUEST_TIMEOUT_SECONDS=25
CORS_ORIGINS=https://scm-frontend-mvp.vercel.app
```

### 3. Data Storage (Free Tier)
- Uses default Render filesystem (~16 GB, ephemeral) at `./data`
- On each deploy or restart, filesystem is reset
- Startup code automatically re-downloads baseline and master CSVs from Azure Blob if they are missing
- No extra paths, no disk configuration, no paid options
- Everything stays under `Path("data")` → `/opt/render/project/src/data` on Render, fully within free limits

### 4. Test Deployment
- Health check: `https://your-app.onrender.com/health`
- API docs: `https://your-app.onrender.com/docs`
- Frontend integration: Test with https://scm-frontend-mvp.vercel.app/

## File Structure

```
SCM_Backend_Deploy/
├── app/
│   ├── main.py                 # FastAPI application with Azure download
│   ├── api/                    # API routes
│   ├── services/               # Business logic
│   ├── models/                 # Pydantic models
│   └── utils/                  # Utilities
├── data/                       # Created automatically (gitignored)
├── requirements.txt            # Python dependencies
├── Procfile                    # Render deployment configuration
├── .env.example               # Environment variables template
└── .gitignore                 # Git ignore rules
```

## Development Notes

- **No large files in repo**: All CSV files are stored in Azure Blob Storage
- **Clean git history**: Fresh repository without monorepo complications
- **MVP focused**: Only essential network analysis endpoints included
- **Production ready**: Configured CORS, timeouts, and error handling

## Health Checks

- `/health` - Returns `{"status": "healthy"}`
- `/` - Returns service information and available endpoints

## Support

This backend is designed for the SCM Network Analysis MVP. For production deployment, ensure all environment variables are properly configured in your hosting platform.
