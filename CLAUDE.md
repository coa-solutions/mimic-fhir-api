# CLAUDE.md - Important Instructions for Claude Code

## Critical Rules

### NEVER Modify Core Data
- **NEVER** modify the MIMIC-IV FHIR data files in the `data/` directory
- The data files are intentionally anonymized with:
  - Future birth dates (shifted 100 years for de-identification)
  - Generic patient names (Patient_XXXXX format)
  - No real MRNs (only anonymized patient IDs)
- This is standard MIMIC-IV Clinical Database Demo format per HIPAA requirements
- Any data quality issues in the source data must remain as-is
- Data transformations should only happen at the API response layer if needed

## Project Context
- This is a FHIR R4 compliant API serving MIMIC-IV Clinical Database Demo
- The dataset is publicly available research data that has been de-identified
- Temporal shifts and anonymization are intentional for privacy protection

## Development Guidelines
- When testing/linting, run: `python -m py_compile main.py`
- The API serves read-only FHIR resources from NDJSON files
- Performance optimizations use caching strategies without modifying source data