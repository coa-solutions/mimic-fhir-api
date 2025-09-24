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
import hashlib
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from cache import (
    cache_fhir_resource,
    cache_fhir_bundle,
    resource_cache,
    bundle_cache,
    get_cache_statistics,
    clear_all_caches,
    generate_cache_key
)

# Configuration
data_dir = "data/mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir"
BASE_URL = os.getenv('FHIR_BASE_URL', 'http://localhost:8000')

def get_base_url(request: Request) -> str:
    """Get the base URL for this request"""
    if BASE_URL != 'http://localhost:8000':
        return BASE_URL
    return f"{request.url.scheme}://{request.url.netloc}"

def generate_etag(data: Any) -> str:
    """Generate ETag for resource data"""
    if isinstance(data, dict):
        # Use resource content to generate hash
        content = json.dumps(data, sort_keys=True, separators=(',', ':'))
    else:
        content = str(data)
    return hashlib.md5(content.encode('utf-8')).hexdigest()

# Cache for file line counts (populated at startup)
file_line_counts = {}

def get_last_modified(resource: Dict) -> Optional[str]:
    """Extract last modified date from FHIR resource meta"""
    meta = resource.get('meta', {})
    last_updated = meta.get('lastUpdated')
    if last_updated:
        # Ensure proper HTTP date format
        try:
            dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
            return dt.strftime('%a, %d %b %Y %H:%M:%S GMT')
        except:
            pass
    return None

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
        self._format = self._parse_format(query_params.get('_format'))
        self._since = self._parse_since(query_params.get('_since'))
        self._summary = query_params.get('_summary')  # New: support _summary parameter

    def _parse_count(self, count_param: Optional[str]) -> Optional[int]:
        """Parse _count parameter according to FHIR spec"""
        if count_param is None:
            return None
        try:
            count = int(count_param)
            return max(0, count)  # FHIR spec: negative values become 0
        except ValueError:
            return None

    def _parse_format(self, format_param: Optional[str]) -> str:
        """Parse _format parameter according to FHIR spec"""
        if format_param is None:
            return "json"

        # Normalize format parameter
        format_param = format_param.lower()
        if format_param in ["json", "application/json", "application/fhir+json"]:
            return "json"
        elif format_param in ["html", "text/html"]:
            return "html"
        else:
            # Default to JSON for unsupported formats
            return "json"

    def _parse_since(self, since_param: Optional[str]) -> Optional[datetime]:
        """Parse _since parameter according to FHIR spec"""
        if since_param is None:
            return None

        try:
            # Handle ISO format with Z suffix
            if since_param.endswith('Z'):
                since_param = since_param.replace('Z', '+00:00')
            return datetime.fromisoformat(since_param)
        except ValueError:
            # Invalid date format, ignore parameter
            return None

    @property
    def count(self) -> Optional[int]:
        return self._count

    @property
    def id_search(self) -> Optional[str]:
        return self._id

    @property
    def format(self) -> str:
        return self._format

    @property
    def since(self) -> Optional[datetime]:
        return self._since

    @property
    def summary(self) -> Optional[str]:
        return self._summary

    def get_count(self, default: int = 100, max_limit: int = 1000) -> int:
        """Get _count with default and maximum enforcement"""
        if self._count is None:
            return default
        return min(self._count, max_limit)

def create_search_filter(resource_type: str, search_params: FHIRSearchParameters) -> Optional[Callable]:
    """Create search filter function based on FHIR search parameters"""

    def search_filter(resource: Dict) -> bool:
        # Required _id parameter support (FHIR spec requirement)
        if search_params.id_search:
            return resource.get('id') == search_params.id_search

        # _since parameter support (filter by last modified)
        if search_params.since:
            meta = resource.get('meta', {})
            last_updated = meta.get('lastUpdated')
            if last_updated:
                try:
                    if last_updated.endswith('Z'):
                        last_updated = last_updated.replace('Z', '+00:00')
                    resource_date = datetime.fromisoformat(last_updated)
                    if resource_date < search_params.since:
                        return False
                except ValueError:
                    # Skip resources with invalid dates
                    pass

        # Resource-specific search parameters
        if resource_type == 'Patient':
            return _patient_search_filter(resource, search_params)
        elif resource_type == 'Observation':
            return _observation_search_filter(resource, search_params)
        elif resource_type == 'Encounter':
            return _encounter_search_filter(resource, search_params)
        elif resource_type == 'Condition':
            return _condition_search_filter(resource, search_params)
        elif resource_type == 'Procedure':
            return _procedure_search_filter(resource, search_params)
        elif resource_type == 'MedicationRequest':
            return _medication_request_search_filter(resource, search_params)
        elif resource_type == 'MedicationAdministration':
            return _medication_administration_search_filter(resource, search_params)
        elif resource_type == 'MedicationDispense':
            return _medication_dispense_search_filter(resource, search_params)
        elif resource_type == 'MedicationStatement':
            return _medication_statement_search_filter(resource, search_params)
        elif resource_type == 'Specimen':
            return _specimen_search_filter(resource, search_params)

        # Default: no additional filters
        return True

    return search_filter if (search_params.id_search or search_params.since or _has_resource_params(resource_type, search_params)) else None

def _has_resource_params(resource_type: str, search_params: FHIRSearchParameters) -> bool:
    """Check if search params contain resource-specific parameters"""
    if resource_type == 'Patient':
        return any(key in search_params.params for key in ['name', 'identifier'])
    elif resource_type == 'Observation':
        return any(key in search_params.params for key in ['subject', 'patient', 'category'])
    elif resource_type == 'Encounter':
        return any(key in search_params.params for key in ['subject', 'patient'])
    elif resource_type in ['Condition', 'Procedure', 'MedicationRequest', 'MedicationAdministration', 'MedicationDispense', 'MedicationStatement', 'Specimen']:
        return any(key in search_params.params for key in ['subject', 'patient'])
    return False

def _patient_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR Patient search parameters"""

    # Patient.name search
    if 'name' in search_params.params:
        name_param = search_params.params['name'].lower()
        patient_names = resource.get('name', [])
        name_match = False

        for name_obj in patient_names:
            # Check given names
            given_names = name_obj.get('given', [])
            if any(name_param in given.lower() for given in given_names):
                name_match = True
                break

            # Check family name
            family_name = name_obj.get('family', '')
            if name_param in family_name.lower():
                name_match = True
                break

        if not name_match:
            return False

    # Patient.identifier search
    if 'identifier' in search_params.params:
        identifier_param = search_params.params['identifier']
        patient_identifiers = resource.get('identifier', [])
        identifier_match = False

        for identifier in patient_identifiers:
            # Check identifier value
            if identifier.get('value') == identifier_param:
                identifier_match = True
                break
            # Check system|value format
            if '|' in identifier_param:
                system, value = identifier_param.split('|', 1)
                if identifier.get('system') == system and identifier.get('value') == value:
                    identifier_match = True
                    break

        if not identifier_match:
            return False

    return True

def _observation_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR Observation search parameters"""
    # Observation.subject search (handles both 'subject' and 'patient' parameters per FHIR R4 spec)
    subject_param = search_params.params.get('subject') or search_params.params.get('patient')
    if subject_param:
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
    # Encounter.subject search (handles both 'subject' and 'patient' parameters per FHIR R4 spec)
    subject_param = search_params.params.get('subject') or search_params.params.get('patient')
    if subject_param:
        patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
        resource_subject = resource.get('subject', {}).get('reference', '')
        if not resource_subject.endswith(f"/{patient_id}"):
            return False

    return True

def _condition_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR Condition search parameters"""
    # Condition.subject search (handles both 'subject' and 'patient' parameters per FHIR R4 spec)
    subject_param = search_params.params.get('subject') or search_params.params.get('patient')
    if subject_param:
        patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
        resource_subject = resource.get('subject', {}).get('reference', '')
        if not resource_subject.endswith(f"/{patient_id}"):
            return False
    return True

def _procedure_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR Procedure search parameters"""
    # Procedure.subject search (handles both 'subject' and 'patient' parameters per FHIR R4 spec)
    subject_param = search_params.params.get('subject') or search_params.params.get('patient')
    if subject_param:
        patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
        resource_subject = resource.get('subject', {}).get('reference', '')
        if not resource_subject.endswith(f"/{patient_id}"):
            return False
    return True

def _medication_request_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR MedicationRequest search parameters"""
    # MedicationRequest.subject search (handles both 'subject' and 'patient' parameters per FHIR R4 spec)
    subject_param = search_params.params.get('subject') or search_params.params.get('patient')
    if subject_param:
        patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
        resource_subject = resource.get('subject', {}).get('reference', '')
        if not resource_subject.endswith(f"/{patient_id}"):
            return False
    return True

def _medication_administration_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR MedicationAdministration search parameters"""
    # MedicationAdministration.subject search (handles both 'subject' and 'patient' parameters per FHIR R4 spec)
    subject_param = search_params.params.get('subject') or search_params.params.get('patient')
    if subject_param:
        patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
        resource_subject = resource.get('subject', {}).get('reference', '')
        if not resource_subject.endswith(f"/{patient_id}"):
            return False
    return True

def _medication_dispense_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR MedicationDispense search parameters"""
    # MedicationDispense.subject search (handles both 'subject' and 'patient' parameters per FHIR R4 spec)
    subject_param = search_params.params.get('subject') or search_params.params.get('patient')
    if subject_param:
        patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
        resource_subject = resource.get('subject', {}).get('reference', '')
        if not resource_subject.endswith(f"/{patient_id}"):
            return False
    return True

def _medication_statement_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR MedicationStatement search parameters"""
    # MedicationStatement.subject search (handles both 'subject' and 'patient' parameters per FHIR R4 spec)
    subject_param = search_params.params.get('subject') or search_params.params.get('patient')
    if subject_param:
        patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
        resource_subject = resource.get('subject', {}).get('reference', '')
        if not resource_subject.endswith(f"/{patient_id}"):
            return False
    return True

def _specimen_search_filter(resource: Dict, search_params: FHIRSearchParameters) -> bool:
    """FHIR Specimen search parameters"""
    # Specimen.subject search (handles both 'subject' and 'patient' parameters per FHIR R4 spec)
    subject_param = search_params.params.get('subject') or search_params.params.get('patient')
    if subject_param:
        patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
        resource_subject = resource.get('subject', {}).get('reference', '')
        if not resource_subject.endswith(f"/{patient_id}"):
            return False
    return True

def count_lines_with_string(filepath: str, search_string: str) -> int:
    """Count lines containing a specific string without JSON parsing"""
    count = 0
    with open(filepath, 'r') as f:
        for line in f:
            if search_string in line:
                count += 1
    return count

def count_fhir_resources_optimized(resource_type: str, search_params: Optional[FHIRSearchParameters] = None, search_filter: Optional[Callable] = None) -> int:
    """
    Optimized resource counting that avoids JSON parsing when possible.
    Uses string matching for simple filters, cached line counts for no filter.
    """
    if resource_type not in FILE_MAPPINGS:
        return 0

    files = FILE_MAPPINGS[resource_type]

    # Case 1: No filter - use cached line counts
    if search_filter is None:
        total_count = 0
        for filename in files:
            if filename in file_line_counts:
                total_count += file_line_counts[filename]
            else:
                # Fallback if cache miss
                filepath = os.path.join(data_dir, filename)
                if os.path.exists(filepath):
                    with open(filepath, 'r') as f:
                        count = sum(1 for line in f if line.strip())
                        file_line_counts[filename] = count
                        total_count += count
        return total_count

    # Case 2: Simple subject/patient filter - use string matching
    if search_params:
        # Check for subject parameter (used for patient filtering)
        subject_param = search_params.params.get('subject') or search_params.params.get('patient')
        if subject_param and len(search_params.params) == 1:  # Only this filter
            # Extract patient ID
            patient_id = subject_param.split('/')[-1] if '/' in subject_param else subject_param
            # Search strings that would appear in the JSON
            search_strings = [
                f'"reference":"Patient/{patient_id}"',  # Most common format
                f'"reference": "Patient/{patient_id}"',  # With space
                f'Patient/{patient_id}'  # Fallback
            ]

            total_count = 0
            for filename in files:
                filepath = os.path.join(data_dir, filename)
                if os.path.exists(filepath):
                    # Try each search string format
                    for search_string in search_strings:
                        count = count_lines_with_string(filepath, search_string)
                        if count > 0:
                            total_count += count
                            break  # Found matches with this format
            return total_count

    # Case 3: Complex filter - fall back to JSON parsing (current implementation)
    return count_fhir_resources_json_parse(resource_type, search_filter)

def count_fhir_resources_json_parse(resource_type: str, search_filter: Optional[Callable] = None) -> int:
    """
    Original counting implementation that parses JSON (for complex filters).
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

# Keep old name for compatibility but redirect to optimized version
def count_fhir_resources(resource_type: str, search_filter: Optional[Callable] = None) -> int:
    """Legacy wrapper - redirects to optimized counting"""
    return count_fhir_resources_json_parse(resource_type, search_filter)

def get_fhir_resources_page(resource_type: str, search_filter: Optional[Callable] = None, count: Optional[int] = None) -> List[Dict]:
    """
    Get a page of resources according to FHIR R4 _count parameter.
    Returns up to 'count' matching resources for current page.
    Implements smart caching for simple queries.
    """
    if resource_type not in FILE_MAPPINGS:
        return []

    # Try to use cache for simple queries (no filter)
    if search_filter is None:
        cache_key = f"page:{resource_type}:nofilter:{count}"
        cached_results = resource_cache.get(cache_key)
        if cached_results is not None:
            return cached_results

    results = []
    files = FILE_MAPPINGS[resource_type]

    for filename in files:
        if count and len(results) >= count:
            break
        filepath = os.path.join(data_dir, filename)
        file_results = read_ndjson_file(filepath, search_filter, count - len(results) if count else None)
        results.extend(file_results)

    final_results = results[:count] if count else results

    # Cache results for simple queries
    if search_filter is None:
        cache_key = f"page:{resource_type}:nofilter:{count}"
        resource_cache.set(cache_key, final_results)

    return final_results

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

def fhir_search(resource_type: str, request: Request):
    """
    Execute FHIR R4 compliant search operation.

    Returns Bundle with correct Bundle.total (total matches) regardless of _count.
    Supports _format parameter for content negotiation.
    Supports _summary=count for count-only responses.
    """
    # Parse FHIR search parameters
    search_params = FHIRSearchParameters(dict(request.query_params))

    # Handle _summary=count - return count-only Bundle
    if search_params.summary == "count":
        # Create search filter
        search_filter = create_search_filter(resource_type, search_params)

        # Use optimized counting for _summary=count
        total_matches = count_fhir_resources_optimized(resource_type, search_params, search_filter)

        # Return count-only Bundle per FHIR spec
        return {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": total_matches,
            "entry": []
        }

    # Generate cache key for this search
    cache_key = f"bundle:{resource_type}:{generate_cache_key(**dict(request.query_params))}"
    cached_bundle = bundle_cache.get(cache_key)

    if cached_bundle:
        # Handle format parameter for cached results
        if search_params.format == "html":
            html_content = f"""
            <html>
            <head><title>FHIR {resource_type} Search Results</title></head>
            <body>
            <h1>{resource_type} Search Results</h1>
            <p>Total matches: {cached_bundle.get('total', 0)}</p>
            <p>Resources in this page: {len(cached_bundle.get('entry', []))}</p>
            <pre>{json.dumps(cached_bundle, indent=2)}</pre>
            </body>
            </html>
            """
            return PlainTextResponse(content=html_content, media_type="text/html")
        return cached_bundle

    # Create search filter
    search_filter = create_search_filter(resource_type, search_params)

    # Count total matches (for Bundle.total)
    total_matches = count_fhir_resources(resource_type, search_filter)

    # Get current page of results with default and max limits
    count = search_params.get_count(default=100, max_limit=1000)
    page_resources = get_fhir_resources_page(resource_type, search_filter, count)

    # Build FHIR Bundle response
    base_url = get_base_url(request)
    self_url = str(request.url)

    bundle = create_fhir_bundle(page_resources, resource_type, base_url, total_matches, self_url)

    # Cache the bundle
    bundle_cache.set(cache_key, bundle)

    # Handle format parameter
    if search_params.format == "html":
        # Simple HTML representation for human readability
        html_content = f"""
        <html>
        <head><title>FHIR {resource_type} Search Results</title></head>
        <body>
        <h1>{resource_type} Search Results</h1>
        <p>Total matches: {total_matches}</p>
        <p>Resources in this page: {len(page_resources)}</p>
        <pre>{json.dumps(bundle, indent=2)}</pre>
        </body>
        </html>
        """
        return PlainTextResponse(content=html_content, media_type="text/html")

    return bundle

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
        # Pre-cache line counts for all files
        print("Pre-caching file line counts for optimized counting...")
        for resource_type, filenames in FILE_MAPPINGS.items():
            for filename in filenames:
                filepath = os.path.join(data_dir, filename)
                if os.path.exists(filepath):
                    with open(filepath, 'r') as f:
                        count = sum(1 for line in f if line.strip())
                        file_line_counts[filename] = count
                        print(f"  - {filename}: {count:,} resources")
        print(f"Cached line counts for {len(file_line_counts)} files")
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
async def root(response: Response):
    """Root endpoint - redirect to CapabilityStatement"""
    response.headers["Cache-Control"] = "public, max-age=3600"
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
async def capability_statement(response: Response):
    """FHIR R4 CapabilityStatement"""
    response.headers["Cache-Control"] = "public, max-age=86400"  # 24 hours for metadata
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
                        {"name": "_id", "type": "token", "documentation": "Logical id of this artifact"},
                        {"name": "_count", "type": "number", "documentation": "Number of resources to return (default: 100, max: 1000)"},
                        {"name": "_format", "type": "token", "documentation": "Specify response format (json, html)"},
                        {"name": "_summary", "type": "token", "documentation": "Return summary (count = return only Bundle.total)"}
                    ] + _get_resource_search_params(resource_type)
                }
                for resource_type in FILE_MAPPINGS.keys()
            ]
        }]
    }

def _get_resource_search_params(resource_type: str) -> List[Dict]:
    """Get supported search parameters for a resource type"""
    common_params = [
        {"name": "_since", "type": "date", "documentation": "Only return resources which were last updated as specified by the given range"}
    ]

    if resource_type == "Patient":
        return common_params + [
            {"name": "name", "type": "string", "documentation": "A server defined search that may match any of the string fields in the HumanName"},
            {"name": "identifier", "type": "token", "documentation": "A patient identifier"}
        ]
    elif resource_type == "Observation":
        return common_params + [
            {"name": "subject", "type": "reference", "documentation": "The subject that the observation is about"},
            {"name": "patient", "type": "reference", "documentation": "The subject that the observation is about (if patient)"},
            {"name": "category", "type": "token", "documentation": "The classification of the type of observation"}
        ]
    elif resource_type == "Encounter":
        return common_params + [
            {"name": "subject", "type": "reference", "documentation": "The patient or group present at the encounter"},
            {"name": "patient", "type": "reference", "documentation": "The patient present at the encounter"}
        ]
    elif resource_type == "Condition":
        return common_params + [
            {"name": "subject", "type": "reference", "documentation": "Who has the condition"},
            {"name": "patient", "type": "reference", "documentation": "Who has the condition (if patient)"}
        ]
    elif resource_type == "Procedure":
        return common_params + [
            {"name": "subject", "type": "reference", "documentation": "Search by subject"},
            {"name": "patient", "type": "reference", "documentation": "Search by subject (if patient)"}
        ]
    elif resource_type == "MedicationRequest":
        return common_params + [
            {"name": "subject", "type": "reference", "documentation": "The identity of a patient to list orders for"},
            {"name": "patient", "type": "reference", "documentation": "The identity of a patient to list orders for"}
        ]
    elif resource_type == "MedicationAdministration":
        return common_params + [
            {"name": "subject", "type": "reference", "documentation": "The identity of the individual or group to list administrations for"},
            {"name": "patient", "type": "reference", "documentation": "The identity of the patient to list administrations for"}
        ]
    elif resource_type == "MedicationDispense":
        return common_params + [
            {"name": "subject", "type": "reference", "documentation": "The identity of a patient to list dispenses for"},
            {"name": "patient", "type": "reference", "documentation": "The identity of a patient to list dispenses for"}
        ]
    elif resource_type == "MedicationStatement":
        return common_params + [
            {"name": "subject", "type": "reference", "documentation": "Returns statements for a specific patient"},
            {"name": "patient", "type": "reference", "documentation": "Returns statements for a specific patient"}
        ]
    elif resource_type == "Specimen":
        return common_params + [
            {"name": "subject", "type": "reference", "documentation": "The subject of the specimen"},
            {"name": "patient", "type": "reference", "documentation": "The patient the specimen came from"}
        ]
    return common_params

# ============================================================================
# FHIR R4 Endpoints - Clean Implementation
# ============================================================================

# Generic FHIR search endpoint - handles all resource types
@app.get("/{resource_type}")
async def fhir_resource_search(resource_type: str, request: Request, response: Response):
    """FHIR R4 search operation for any resource type"""
    if resource_type not in FILE_MAPPINGS:
        raise HTTPException(status_code=404, detail=f"Resource type {resource_type} not supported")

    bundle = fhir_search(resource_type, request)

    # Add ETag header for cache validation
    if isinstance(bundle, dict):
        etag = generate_etag(bundle)
        response.headers["ETag"] = f'W/"{etag}"'
        response.headers["Cache-Control"] = "public, max-age=3600"  # 1 hour cache for searches

        # Handle conditional requests
        if_none_match = request.headers.get("If-None-Match")
        if if_none_match and if_none_match.strip('W/"').strip('"') == etag:
            response.status_code = 304  # Not Modified
            return None

    return bundle

# Generic FHIR read endpoint - get resource by ID
@app.get("/{resource_type}/{resource_id}")
async def fhir_resource_read(resource_type: str, resource_id: str, request: Request, response: Response):
    """FHIR R4 read operation - get single resource by ID"""
    if resource_type not in FILE_MAPPINGS:
        raise HTTPException(status_code=404, detail=f"Resource type {resource_type} not supported")

    # Try cache first for individual resource
    cache_key = f"resource:{resource_type}:{resource_id}"
    cached_resource = resource_cache.get(cache_key)

    if cached_resource:
        resource = cached_resource
    else:
        # Use _id search parameter to find the resource
        search_params = FHIRSearchParameters({'_id': resource_id})
        search_filter = create_search_filter(resource_type, search_params)

        # Get the resource - bypass get_fhir_resources_page for efficiency
        # Direct file reading for single resource by ID
        resources = []
        files = FILE_MAPPINGS[resource_type]
        for filename in files:
            filepath = os.path.join(data_dir, filename)
            file_results = read_ndjson_file(filepath, search_filter, 1)
            if file_results:
                resources = file_results
                break

        if not resources:
            raise HTTPException(status_code=404, detail=f"{resource_type}/{resource_id} not found")

        resource = resources[0]
        # Cache the individual resource with specific key
        resource_cache.set(cache_key, resource)

    # Add ETag header
    etag = generate_etag(resource)
    response.headers["ETag"] = f'W/"{etag}"'
    response.headers["Cache-Control"] = "public, max-age=86400"  # 24 hours for individual resources

    # Handle conditional requests
    if_none_match = request.headers.get("If-None-Match")
    if if_none_match and if_none_match.strip('W/"').strip('"') == etag:
        response.status_code = 304  # Not Modified
        return None

    # Add Last-Modified header if available
    last_modified = get_last_modified(resource)
    if last_modified:
        response.headers["Last-Modified"] = last_modified

    return resource

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