#!/usr/bin/env python3
"""
MIMIC-IV FHIR R4 API Server
Clean, FHIR R4 compliant implementation serving MIMIC-IV Clinical Database Demo on FHIR (v2.1.0)

Dataset Citation:
Bennett, A., Ulrich, H., Wiedekopf, J., Szul, P., Grimes, J., & Johnson, A. (2025).
MIMIC-IV Clinical Database Demo on FHIR (version 2.1.0). PhysioNet.
https://doi.org/10.13026/c2f9-3y63

Licensed under Open Database License (ODbL) - See LICENSE file
"""

import json
import os
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from cache import (
    cache_fhir_resource,
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

def read_ndjson_file(filepath: str, filter_func: Optional[Callable] = None, limit: Optional[int] = None) -> List[Dict]:
    """Read NDJSON file with optional filtering and limiting"""
    results = []
    if not os.path.exists(filepath):
        return results

    with open(filepath, 'r') as f:
        for line in f:
            if limit and len(results) >= limit:
                break
            if line.strip():
                try:
                    resource = json.loads(line)
                    if filter_func is None or filter_func(resource):
                        results.append(resource)
                except json.JSONDecodeError:
                    continue
    return results

# FHIR Resource Type Mappings
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
    print("MIMIC-IV FHIR R4 API Starting...")
    print(f"Data directory: {data_dir}")
    if not os.path.exists(data_dir):
        print(f"WARNING: Data directory not found: {data_dir}")
    else:
        print("MIMIC-IV FHIR data files available - will be read on-demand")
    yield
    # Shutdown
    print("MIMIC-IV FHIR R4 API Shutting down...")

# Initialize FastAPI app
app = FastAPI(
    title="MIMIC-IV FHIR R4 API Server",
    description="Clean FHIR R4 compliant API serving MIMIC-IV Clinical Database Demo on FHIR (v2.1.0)",
    version="2.0.0",
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
    """Root endpoint - redirect to CapabilityStatement"""
    return {
        "resourceType": "Bundle",
        "type": "message",
        "entry": [{
            "resource": {
                "resourceType": "MessageHeader",
                "source": {"name": "MIMIC-IV FHIR R4 API"},
                "meta": {"lastUpdated": datetime.now().isoformat()}
            }
        }],
        "link": [{"relation": "self", "url": "/metadata"}]
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
    """FHIR R4 CapabilityStatement"""
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
                    ],
                    "searchParam": [
                        {"name": "_id", "type": "token", "documentation": "Logical id of this artifact"}
                    ] + _get_resource_search_params(resource_type)
                }
                for resource_type in FILE_MAPPINGS.keys()
            ]
        }]
    }

def _get_resource_search_params(resource_type: str) -> List[Dict]:
    """Get supported search parameters for a resource type"""
    if resource_type == "Patient":
        return [
            {"name": "name", "type": "string", "documentation": "A server defined search that may match any of the string fields in the HumanName"},
            {"name": "identifier", "type": "token", "documentation": "A patient identifier"}
        ]
    elif resource_type == "Observation":
        return [
            {"name": "subject", "type": "reference", "documentation": "The subject that the observation is about"},
            {"name": "category", "type": "token", "documentation": "The classification of the type of observation"}
        ]
    elif resource_type == "Encounter":
        return [
            {"name": "subject", "type": "reference", "documentation": "The patient or group present at the encounter"}
        ]
    return []

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

if __name__ == "__main__":
    print("\n" + "="*60)
    print("MIMIC-IV FHIR R4 API Server")
    print("="*60)
    print("\nStarting FHIR-compliant server...")
    print("\nAPI will be available at: http://localhost:8000")
    print("CapabilityStatement: http://localhost:8000/metadata")
    print("Interactive docs: http://localhost:8000/docs")
    print("\nPress Ctrl+C to stop the server")
    print("="*60 + "\n")

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)