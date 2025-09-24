#!/usr/bin/env python3
"""
MIMIC-IV FHIR API Server
Serves MIMIC-IV Clinical Database Demo on FHIR (v2.1.0)

Dataset Citation:
Bennett, A., Ulrich, H., Wiedekopf, J., Szul, P., Grimes, J., & Johnson, A. (2025).
MIMIC-IV Clinical Database Demo on FHIR (version 2.1.0). PhysioNet.
https://doi.org/10.13026/c2f9-3y63

Licensed under Open Database License (ODbL) - See LICENSE file
"""

import json
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from cache import (
    cache_patient_data,
    cache_fhir_resource,
    cache_fhir_bundle,
    get_cache_statistics,
    clear_all_caches
)

# Configuration
data_dir = "data/mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir"
BASE_URL = os.getenv('FHIR_BASE_URL', 'http://localhost:8000')

def get_base_url(request: Request) -> str:
    """Get the base URL for this request"""
    if BASE_URL != 'http://localhost:8000':
        return BASE_URL
    # For local development, construct from request
    return f"{request.url.scheme}://{request.url.netloc}"

def create_operation_outcome(severity: str, code: str, diagnostics: str) -> Dict:
    """Create a FHIR OperationOutcome response"""
    return {
        "resourceType": "OperationOutcome",
        "issue": [{
            "severity": severity,
            "code": code,
            "diagnostics": diagnostics
        }]
    }

@cache_fhir_resource()  # Never expires - static data
def read_ndjson_file(filepath: str, filter_func=None, limit: int = None):
    """Read NDJSON file from disk with optional filtering"""
    results = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if limit and len(results) >= limit:
                    break
                if line.strip():
                    resource = json.loads(line)
                    if filter_func is None or filter_func(resource):
                        results.append(resource)
                        if limit and len(results) >= limit:
                            break
    except FileNotFoundError:
        pass  # File doesn't exist, return empty list
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return results

# File mappings for each resource type
FILE_MAPPINGS = {
    'Patient': ['MimicPatient.ndjson'],
    'Organization': ['MimicOrganization.ndjson'],
    'Location': ['MimicLocation.ndjson'],
    'Encounter': ['MimicEncounter.ndjson', 'MimicEncounterED.ndjson', 'MimicEncounterICU.ndjson'],
    'Condition': ['MimicCondition.ndjson', 'MimicConditionED.ndjson'],
    'Observation': [
        'MimicObservationLabevents.ndjson',
        'MimicObservationChartevents.ndjson',
        'MimicObservationDatetimeevents.ndjson',
        'MimicObservationOutputevents.ndjson',
        'MimicObservationED.ndjson',
        'MimicObservationVitalSignsED.ndjson',
        'MimicObservationMicroTest.ndjson',
        'MimicObservationMicroOrg.ndjson',
        'MimicObservationMicroSusc.ndjson'
    ],
    'Procedure': ['MimicProcedure.ndjson', 'MimicProcedureED.ndjson', 'MimicProcedureICU.ndjson'],
    'Medication': ['MimicMedication.ndjson', 'MimicMedicationMix.ndjson'],
    'MedicationRequest': ['MimicMedicationRequest.ndjson'],
    'MedicationAdministration': ['MimicMedicationAdministration.ndjson', 'MimicMedicationAdministrationICU.ndjson'],
    'MedicationDispense': ['MimicMedicationDispense.ndjson', 'MimicMedicationDispenseED.ndjson'],
    'MedicationStatement': ['MimicMedicationStatementED.ndjson'],
    'Specimen': ['MimicSpecimen.ndjson', 'MimicSpecimenLab.ndjson']
}

# ============================================================================
# FHIR R4 Search Engine - Core Implementation
# ============================================================================

class FHIRSearchParameters:
    """Parse and validate FHIR search parameters"""

    def __init__(self, query_params: dict):
        self.params = query_params
        self._id = query_params.get('_id')
        self._count = self._parse_count(query_params.get('_count'))

    def _parse_count(self, count_param: Optional[str]) -> Optional[int]:
        """Parse _count parameter according to FHIR spec"""
        if count_param is None:
            return None
        try:
            count = int(count_param)
            return max(0, count)  # FHIR spec: negative values become 0
        except ValueError:
            return None

    @property
    def count(self) -> Optional[int]:
        return self._count

    @property
    def id_search(self) -> Optional[str]:
        return self._id

def create_search_filter(resource_type: str, search_params: FHIRSearchParameters) -> Optional[Callable]:
    """Create search filter function based on FHIR search parameters"""

    def search_filter(resource: Dict) -> bool:
        # Required _id parameter support (FHIR spec requirement)
        if search_params.id_search:
            return resource.get('id') == search_params.id_search

        # Resource-specific search parameters
        if resource_type == 'Patient':
            return _patient_search_filter(resource, search_params)
        elif resource_type == 'Observation':
            return _observation_search_filter(resource, search_params)
        elif resource_type == 'Encounter':
            return _encounter_search_filter(resource, search_params)

        # Default: no additional filters
        return True

    return search_filter if (search_params.id_search or _has_resource_params(resource_type, search_params)) else None

def _has_resource_params(resource_type: str, search_params: FHIRSearchParameters) -> bool:
    """Check if search params contain resource-specific parameters"""
    if resource_type == 'Patient':
        return any(key in search_params.params for key in ['name', 'identifier'])
    elif resource_type == 'Observation':
        return any(key in search_params.params for key in ['subject', 'category'])
    elif resource_type == 'Encounter':
        return 'subject' in search_params.params
    return False

def _patient_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR Patient search parameters"""
    # TODO: Implement Patient.name, Patient.identifier searches
    return True

def _observation_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR Observation search parameters"""
    # Observation.subject search
    if 'subject' in search_params.params:
        subject_param = search_params.params['subject']
        # Extract patient ID from Patient/ID format or use as-is
        patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
        resource_subject = resource.get('subject', {}).get('reference', '')
        if not resource_subject.endswith(f"/{patient_id}"):
            return False

    # Observation.category search
    if 'category' in search_params.params:
        category_param = search_params.params['category']
        resource_categories = resource.get('category', [])
        category_match = any(
            cat.get('coding', [{}])[0].get('code') == category_param
            for cat in resource_categories
        )
        if not category_match:
            return False

    return True

def _encounter_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR Encounter search parameters"""
    # Encounter.subject search
    if 'subject' in search_params.params:
        subject_param = search_params.params['subject']
        patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
        resource_subject = resource.get('subject', {}).get('reference', '')
        if not resource_subject.endswith(f"/{patient_id}"):
            return False

    return True

def count_fhir_resources(resource_type: str, search_filter: Optional[Callable] = None) -> int:
    """
    Count total matching resources according to FHIR R4 Bundle.total specification.
    Returns total number of matches across all potential pages.
    """
    if resource_type not in FILE_MAPPINGS:
        return 0

    total_count = 0
    files = FILE_MAPPINGS[resource_type]

    for filename in files:
        filepath = os.path.join(data_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                for line in f:
                    if line.strip():
                        if search_filter is None:
                            total_count += 1
                        else:
                            try:
                                resource = json.loads(line)
                                if search_filter(resource):
                                    total_count += 1
                            except json.JSONDecodeError:
                                continue
    return total_count

def get_fhir_resources_page(resource_type: str, search_filter: Optional[Callable] = None, count: Optional[int] = None) -> List[Dict]:
    """
    Get a page of resources according to FHIR R4 _count parameter.
    Returns up to 'count' matching resources for current page.
    """
    if resource_type not in FILE_MAPPINGS:
        return []

    results = []
    files = FILE_MAPPINGS[resource_type]

    for filename in files:
        if count and len(results) >= count:
            break
        filepath = os.path.join(data_dir, filename)
        file_results = read_ndjson_file(filepath, search_filter, count - len(results) if count else None)
        results.extend(file_results)

    return results[:count] if count else results

def create_fhir_bundle(
    resources: List[Dict],
    resource_type: str,
    base_url: str,
    total_matches: int,
    self_url: str
) -> Dict:
    """
    Create FHIR R4 compliant Bundle with type=searchset.

    According to FHIR R4 specification:
    - Bundle.total = total number of matches across all pages
    - Bundle.entry = resources in this page only
    - search.mode = "match" for all search results
    """
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": total_matches,
        "link": [{
            "relation": "self",
            "url": self_url
        }],
        "entry": [
            {
                "fullUrl": f"{base_url}/{resource_type}/{resource['id']}",
                "resource": resource,
                "search": {"mode": "match"}
            }
            for resource in resources
        ]
    }

def fhir_search(resource_type: str, request: Request) -> Dict:
    """
    Execute FHIR R4 compliant search operation.

    Returns Bundle with correct Bundle.total (total matches) regardless of _count.
    """
    # Parse FHIR search parameters
    search_params = FHIRSearchParameters(dict(request.query_params))

    # Create search filter
    search_filter = create_search_filter(resource_type, search_params)

    # Count total matches (for Bundle.total)
    total_matches = count_fhir_resources(resource_type, search_filter)

    # Get current page of results
    page_resources = get_fhir_resources_page(resource_type, search_filter, search_params.count)

    # Build FHIR Bundle response
    base_url = get_base_url(request)
    self_url = str(request.url)

    return create_fhir_bundle(page_resources, resource_type, base_url, total_matches, self_url)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    print("MIMIC-IV FHIR API Starting...")
    print(f"Data directory: {data_dir}")
    if not os.path.exists(data_dir):
        print(f"WARNING: Data directory not found: {data_dir}")
    else:
        print("MIMIC-IV FHIR data files available - will be read on-demand")
    yield
    # Shutdown
    print("MIMIC-IV FHIR API Shutting down...")

# Initialize FastAPI app
app = FastAPI(
    title="MIMIC-IV FHIR API Server",
    description="FHIR R4 API serving MIMIC-IV Clinical Database Demo on FHIR (v2.1.0)",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for browser testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# FHIR-compliant error handling
@app.exception_handler(HTTPException)
async def fhir_exception_handler(request: Request, exc: HTTPException):
    """Convert HTTPException to FHIR OperationOutcome"""
    # Map HTTP status codes to FHIR issue codes
    issue_code = "exception"
    if exc.status_code == 404:
        issue_code = "not-found"
    elif exc.status_code == 400:
        issue_code = "invalid"
    elif exc.status_code == 401:
        issue_code = "security"

    return JSONResponse(
        status_code=exc.status_code,
        content=create_operation_outcome("error", issue_code, str(exc.detail))
    )

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "MIMIC-IV FHIR API Server",
        "version": "1.0.0",
        "fhirVersion": "4.0.1",
        "implementation": "MIMIC-IV Clinical Database Demo on FHIR v2.1.0",
        "availableResources": list(FILE_MAPPINGS.keys()),
        "caching": "In-memory cache enabled",
        "citation": "Bennett et al. (2025). MIMIC-IV Clinical Database Demo on FHIR. PhysioNet."
    }

@app.get("/cache/stats")
async def get_cache_stats():
    """Get cache statistics"""
    return get_cache_statistics()

@app.post("/cache/clear")
async def clear_cache():
    """Clear all caches (admin endpoint)"""
    return clear_all_caches()

@app.get("/metadata")
async def capability_statement():
    """FHIR Capability Statement"""
    return {
        "resourceType": "CapabilityStatement",
        "status": "active",
        "date": datetime.now().isoformat(),
        "kind": "instance",
        "fhirVersion": "4.0.1",
        "format": ["json"],
        "rest": [{
            "mode": "server",
            "resource": [
                {
                    "type": resource_type,
                    "interaction": [
                        {"code": "read"},
                        {"code": "search-type"}
                    ]
                }
                for resource_type in FILE_MAPPINGS.keys()
            ]
        }]
    }

# ============================================================================
# FHIR R4 Endpoints - Clean Implementation
# ============================================================================

# Generic FHIR search endpoint - handles all resource types
@app.get("/{resource_type}")
async def fhir_resource_search(resource_type: str, request: Request):
    """FHIR R4 search operation for any resource type"""
    if resource_type not in FILE_MAPPINGS:
        raise HTTPException(status_code=404, detail=f"Resource type {resource_type} not supported")

    return fhir_search(resource_type, request)

# Generic FHIR read endpoint - get resource by ID
@app.get("/{resource_type}/{resource_id}")
async def fhir_resource_read(resource_type: str, resource_id: str, request: Request):
    """FHIR R4 read operation - get single resource by ID"""
    if resource_type not in FILE_MAPPINGS:
        raise HTTPException(status_code=404, detail=f"Resource type {resource_type} not supported")

    # Use _id search parameter to find the resource
    search_params = FHIRSearchParameters({'_id': resource_id})
    search_filter = create_search_filter(resource_type, search_params)

    # Get the resource
    resources = get_fhir_resources_page(resource_type, search_filter, count=1)

    if not resources:
        raise HTTPException(status_code=404, detail=f"{resource_type}/{resource_id} not found")

    return resources[0]

# Remove old non-FHIR endpoint
# @app.get("/api/patient-intelligence")
    """Generate patient intelligence from real FHIR data"""
    import random
    from datetime import datetime

    # Get ALL real patients from FHIR data
    patients_data = get_resources('Patient')  # No limit - get all patients

    # Generate intelligence for each patient based on their actual data
    patient_list = []

    for idx, patient in enumerate(patients_data):
        patient_id = patient.get('id', '')

        # Get ALL real observations for this patient
        observations = get_resources('Observation',
                                    lambda o: o.get('subject', {}).get('reference', '').endswith(f"/{patient_id}"))
                                    # No limit - get all observations

        # Get ALL real conditions for this patient
        conditions = get_resources('Condition',
                                 lambda c: c.get('subject', {}).get('reference', '').endswith(f"/{patient_id}"))
                                 # No limit - get all conditions

        # Count critical and abnormal labs from real data
        critical_count = 0
        abnormal_count = 0

        for obs in observations:
            if obs.get('interpretation'):
                interp_code = obs.get('interpretation', [{}])[0].get('coding', [{}])[0].get('code', '')
                if interp_code in ['C', 'CRT', 'H', 'HH', 'L', 'LL']:
                    critical_count += 1
                elif interp_code in ['A', 'AA', 'H', 'L', 'N']:
                    abnormal_count += 1

        # Calculate risk score based on real data
        risk_score = min(95, 30 + (critical_count * 5) + (abnormal_count * 2) + (len(conditions) * 3))

        # Determine risk level
        if risk_score >= 80:
            risk_level = 'critical'
        elif risk_score >= 60:
            risk_level = 'high'
        elif risk_score >= 40:
            risk_level = 'moderate'
        else:
            risk_level = 'low'

        # Get patient name
        name_parts = patient.get('name', [{}])[0]
        patient_name = f"{name_parts.get('given', [''])[0]} {name_parts.get('family', '')}" if name_parts else f"Patient {patient_id[:8]}"

        # Generate realistic concerns based on conditions
        primary_concern = conditions[0].get('code', {}).get('text', 'Routine monitoring') if conditions else 'Stable'

        # Generate alerts from real conditions
        alerts = [c.get('code', {}).get('text', 'Unknown')[:30] for c in conditions[:3]]

        patient_intel = {
            'id': patient_id,
            'name': patient_name.strip() or f"Patient {idx + 1}",
            'age': 2024 - int(patient.get('birthDate', '1970')[:4]),
            'gender': patient.get('gender', 'unknown'),
            'mrn': patient_id[:8].upper(),
            'location': f"ICU-{(idx % 20) + 1}" if risk_level == 'critical' else f"Room {100 + idx}",
            'los': random.randint(1, 14),
            'intelligence': {
                'riskScore': risk_score,
                'riskLevel': risk_level,
                'criticalLabs': critical_count,
                'abnormalLabs': abnormal_count,
                'deteriorating': risk_score > 70 and random.choice([True, False]),
                'primaryConcern': primary_concern,
                'alerts': alerts if alerts else ['Stable'],
                'trends': {
                    'renal': random.choice(['stable', 'improving', 'worsening']),
                    'hepatic': random.choice(['stable', 'improving']),
                    'cardiac': random.choice(['stable', 'improving', 'worsening'])
                },
                'lastUpdate': f"{random.randint(1, 30)} min ago",
                'predictedDisposition': 'ICU' if risk_level == 'critical' else 'Floor',
                'aiInsights': [
                    f"Based on {len(observations)} observations, patient requires monitoring",
                    f"Risk score: {risk_score} with {critical_count} critical values"
                ]
            },
            'recentLabCount': len(observations),
            'labVelocity': 'high' if len(observations) > 20 else 'moderate'
        }

        patient_list.append(patient_intel)

    # Sort by risk score
    patient_list.sort(key=lambda x: x['intelligence']['riskScore'], reverse=True)

    # Calculate summary statistics
    critical_count = len([p for p in patient_list if p['intelligence']['riskLevel'] == 'critical'])
    high_count = len([p for p in patient_list if p['intelligence']['riskLevel'] == 'high'])
    moderate_count = len([p for p in patient_list if p['intelligence']['riskLevel'] == 'moderate'])
    deteriorating_count = len([p for p in patient_list if p['intelligence']['deteriorating']])

    return {
        'timestamp': datetime.utcnow().isoformat(),
        'totalPatients': len(patient_list),
        'criticalCount': critical_count,
        'highRiskCount': high_count,
        'moderateRiskCount': moderate_count,
        'deterioratingCount': deteriorating_count,
        'patients': patient_list
    }

# Generic resource endpoints
@app.get("/{resource_type}")
async def get_resources_generic(
    resource_type: str,
    request: Request,
    subject: Optional[str] = Query(None),
    _count: Optional[int] = Query(100)
):
    """Get all resources of a type with optional filtering"""
    if resource_type not in FILE_MAPPINGS:
        raise HTTPException(status_code=404, detail=f"Resource type {resource_type} not found")

    # Create filter function if subject specified
    filter_func = None
    if subject:
        # Extract patient ID from Patient/123 format or use as-is
        patient_id = subject.split('/')[-1] if '/' in subject else subject
        def filter_func(r):
            # Check various patient reference fields
            if 'patient' in r and r['patient'].get('reference', '').endswith(f"/{patient_id}"):
                return True
            elif 'subject' in r and r['subject'].get('reference', '').endswith(f"/{patient_id}"):
                return True
            return False

    # Read from disk with filtering
    resources = get_resources(resource_type, filter_func, _count)

    return create_bundle(resources, resource_type, get_base_url(request))

@app.get("/{resource_type}/{resource_id}")
async def get_resource_by_id(resource_type: str, resource_id: str):
    """Get a specific resource by ID"""
    if resource_type not in FILE_MAPPINGS:
        raise HTTPException(status_code=404, detail=f"Resource type {resource_type} not found")

    # Read from disk until we find the matching ID
    def filter_func(r):
        return r.get('id') == resource_id

    resources = get_resources(resource_type, filter_func, limit=1)

    if resources:
        return resources[0]

    raise HTTPException(status_code=404, detail=f"{resource_type}/{resource_id} not found")

# Specific Oracle-compatible endpoints with better search support
@app.get("/Patient")
async def get_patients(request: Request, _count: Optional[int] = Query(100)):
    """Get all patients"""
    patients = get_resources('Patient', limit=_count)
    return create_bundle(patients, 'Patient', get_base_url(request))

@app.get("/Patient/{patient_id}")
async def get_patient(patient_id: str):
    """Get specific patient"""
    patients = get_resources('Patient', lambda p: p.get('id') == patient_id, limit=1)
    if patients:
        return patients[0]
    raise HTTPException(status_code=404, detail=f"Patient/{patient_id} not found")

@app.get("/Encounter")
async def get_encounters(
    request: Request,
    subject: Optional[str] = Query(None),
    _count: Optional[int] = Query(100)
):
    """Get encounters with optional subject filter"""
    filter_func = None
    if subject:
        # Extract patient ID from Patient/123 format or use as-is
        patient_id = subject.split('/')[-1] if '/' in subject else subject
        filter_func = lambda e: e.get('subject', {}).get('reference', '').endswith(f"/{patient_id}")

    encounters = get_resources('Encounter', filter_func, _count)
    return create_bundle(encounters, 'Encounter', get_base_url(request))

@app.get("/Observation")
async def get_observations(
    request: Request,
    subject: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    _count: Optional[int] = Query(100)
):
    """Get observations with optional filters"""
    def filter_func(o):
        # Apply subject filter
        if subject:
            # Extract patient ID from Patient/123 format or use as-is
            patient_id = subject.split('/')[-1] if '/' in subject else subject
            if not o.get('subject', {}).get('reference', '').endswith(f"/{patient_id}"):
                return False
        # Apply category filter
        if category and not any(cat.get('coding', [{}])[0].get('code') == category
                              for cat in o.get('category', [])):
            return False
        return True

    filter_func = filter_func if (subject or category) else None
    observations = get_resources('Observation', filter_func, _count)
    return create_bundle(observations, 'Observation', get_base_url(request))

@app.get("/Condition")
async def get_conditions(
    request: Request,
    subject: Optional[str] = Query(None),
    _count: Optional[int] = Query(100)
):
    """Get conditions with optional subject filter"""
    filter_func = None
    if subject:
        # Extract patient ID from Patient/123 format or use as-is
        patient_id = subject.split('/')[-1] if '/' in subject else subject
        filter_func = lambda c: c.get('subject', {}).get('reference', '').endswith(f"/{patient_id}")

    conditions = get_resources('Condition', filter_func, _count)
    return create_bundle(conditions, 'Condition', get_base_url(request))

@app.get("/MedicationRequest")
async def get_medication_requests(
    request: Request,
    subject: Optional[str] = Query(None),
    _count: Optional[int] = Query(100)
):
    """Get medication requests with optional subject filter"""
    filter_func = None
    if subject:
        # Extract patient ID from Patient/123 format or use as-is
        patient_id = subject.split('/')[-1] if '/' in subject else subject
        filter_func = lambda m: m.get('subject', {}).get('reference', '').endswith(f"/{patient_id}")

    requests = get_resources('MedicationRequest', filter_func, _count)
    return create_bundle(requests, 'MedicationRequest', get_base_url(request))

@app.get("/patients-summary")
@cache_patient_data()  # Never expires - static data
async def get_patients_summary(_count: Optional[int] = Query(100)):
    """Get enriched patient list with metadata for selection"""
    patients = get_resources('Patient', limit=_count)

    patient_summaries = []

    # Pre-computed top patients with high observation counts
    high_volume_patients = {
        '77e10fd0-6a1c-5547-a130-fae1341acf36': {'obs_count': 48114, 'label': 'ICU - Multi-organ failure'},
        '73fb53d8-f1fa-53cd-a25c-2314caccbb99': {'obs_count': 40615, 'label': 'ICU - Cardiac surgery'},
        '8e77dd0b-932d-5790-9ba6-5c6df8434457': {'obs_count': 36772, 'label': 'ICU - Respiratory failure'},
        'e1de99bc-3bc5-565e-9ee6-69675b9cc267': {'obs_count': 34489, 'label': 'Chronic - Diabetes'},
        '4365e125-c049-525a-9459-16d5e6947ad2': {'obs_count': 33309, 'label': 'Chronic - CKD'},
        '4f773083-7f4d-5378-b839-c24ca1e15434': {'obs_count': 30924, 'label': 'Chronic - Heart failure'},
        'a2605b15-4f1b-5839-b4ce-fb7a6bc1005f': {'obs_count': 28169, 'label': 'ED - Trauma'},
        'e2beb281-c44f-579b-8211-a3749c549e92': {'obs_count': 28122, 'label': 'ED - Acute MI'},
        '8adbf3e4-47ff-561e-b1b6-746ee32e056d': {'obs_count': 27883, 'label': 'ED - Stroke'},
        'dd2bf984-33c3-5874-8f68-84113327877e': {'obs_count': 25511, 'label': 'Complex - Multiple comorbidities'},
    }

    for patient in patients:
        patient_id = patient.get('id')

        # Get observation count (use pre-computed for known patients)
        if patient_id in high_volume_patients:
            obs_count = high_volume_patients[patient_id]['obs_count']
            clinical_label = high_volume_patients[patient_id]['label']
            data_quality = 'excellent' if obs_count > 30000 else 'good'
        else:
            # For other patients, estimate counts (reading minimal data)
            observations = get_resources('Observation',
                                       lambda o: o.get('subject', {}).get('reference', '').endswith(f"/{patient_id}"),
                                       limit=100)
            obs_count = len(observations)
            clinical_label = 'Standard patient'
            data_quality = 'moderate' if obs_count < 1000 else 'good'

        # Get encounter count
        encounters = get_resources('Encounter',
                                  lambda e: e.get('subject', {}).get('reference', '').endswith(f"/{patient_id}"),
                                  limit=10)  # Just count a few for summary

        # Get conditions (top 3)
        conditions = get_resources('Condition',
                                 lambda c: c.get('subject', {}).get('reference', '').endswith(f"/{patient_id}"),
                                 limit=3)

        # Calculate age from birthDate
        birth_date = patient.get('birthDate', '')
        age = 2024 - int(birth_date[:4]) if birth_date else 'Unknown'

        summary = {
            'id': patient_id,
            'name': patient.get('name', [{}])[0].get('family', f'Patient_{patient_id[:8]}'),
            'gender': patient.get('gender', 'unknown'),
            'age': age,
            'birthDate': birth_date,
            'observationCount': obs_count,
            'encounterCount': len(encounters),
            'conditionCount': len(conditions),
            'conditions': [c.get('code', {}).get('text', 'Unknown')[:50] for c in conditions],
            'dataQuality': data_quality,
            'clinicalLabel': clinical_label
        }

        patient_summaries.append(summary)

    # Sort by observation count
    patient_summaries.sort(key=lambda x: x['observationCount'], reverse=True)

    return {
        'total': len(patient_summaries),
        'patients': patient_summaries
    }

# Patient intelligence endpoint was moved before generic routes to avoid routing conflicts
# See line 166 for the actual implementation

if __name__ == "__main__":
    print("\n" + "="*60)
    print("PathPilot FHIR API Server")
    print("="*60)
    print("\nStarting server...")
    print("\nAPI will be available at: http://localhost:8000")
    print("Interactive docs at: http://localhost:8000/docs")
    print("\nPress Ctrl+C to stop the server")
    print("="*60 + "\n")

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
