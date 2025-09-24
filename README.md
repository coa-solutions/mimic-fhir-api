# MIMIC-IV FHIR API

FHIR R4 API serving MIMIC-IV Clinical Database Demo on FHIR (v2.1.0)

## Dataset Attribution

This API serves data from the MIMIC-IV Clinical Database Demo on FHIR dataset from PhysioNet.

**Required Citation:**
```
Bennett, A., Ulrich, H., Wiedekopf, J., Szul, P., Grimes, J., & Johnson, A. (2025).
MIMIC-IV Clinical Database Demo on FHIR (version 2.1.0). PhysioNet.
https://doi.org/10.13026/c2f9-3y63
```

**Data Source:** https://physionet.org/content/mimic-iv-fhir-demo/2.1.0/

## Quick Start

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run the API
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

API will be available at:
- http://localhost:8000 - API root
- http://localhost:8000/docs - Interactive API documentation

### Deploy to Render
1. Push this repository to GitHub
2. Connect to Render and create a new Web Service
3. Render will auto-detect the configuration from `render.yaml`

## API Endpoints

### FHIR Metadata
- `GET /` - API information
- `GET /metadata` - FHIR CapabilityStatement

### FHIR Resources
- `GET /Patient` - Search patients
- `GET /Patient/{id}` - Get specific patient
- `GET /Observation` - Search observations
- `GET /Observation/{id}` - Get specific observation
- `GET /Condition` - Search conditions
- `GET /Encounter` - Search encounters
- `GET /MedicationRequest` - Search medication requests
- `GET /Procedure` - Search procedures
- `GET /Specimen` - Search specimens

### Custom Operations
- `GET /api/patient-intelligence` - AI-powered patient risk intelligence
- `GET /patients-summary` - Enriched patient list with metadata

### Cache Management
- `GET /cache/stats` - View cache statistics
- `POST /cache/clear` - Clear all caches

## Query Parameters

### Common Parameters
- `_count` - Number of results to return (default: 100)
- `patient` or `subject` - Filter by patient ID

### Observation-specific
- `category` - Filter by observation category

## Data Overview

The dataset contains:
- 100 patients from MIMIC-IV Clinical Database
- 813,000+ observations including:
  - Laboratory results
  - Vital signs
  - Chart events
  - Microbiology results
- Medications, procedures, conditions, and encounters
- All data is de-identified and suitable for research

## License

**Dataset:** Open Database License (ODbL) - See LICENSE file
**API Code:** MIT License

## Performance

The API includes an in-memory cache layer optimized for the static MIMIC dataset:
- Never-expiring cache for unchanging data
- Sub-second response times for cached queries
- Automatic cache management

## Environment Variables

For deployment:
```
CORS_ORIGINS=*  # Configure based on your needs
PYTHON_VERSION=3.11.0
```

## Support

For issues with:
- **Dataset**: See [PhysioNet MIMIC-IV FHIR page](https://physionet.org/content/mimic-iv-fhir-demo/2.1.0/)
- **API**: Create an issue in this repository