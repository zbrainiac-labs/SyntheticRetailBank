# Implementation Spec: Late Enrichment

This is a detailed implementation plan stored as a business requirements appendix. The plan will be written to [business_requirements/LATE_ENRICHMENT_IMPLEMENTATION_SPEC.md](business_requirements/LATE_ENRICHMENT_IMPLEMENTATION_SPEC.md).

## Scope

- 22 objects contain PII across 6 SQL definition files
- 3 new PII vault tables (customer, address, employee)
- 6 new late-enrichment secure views
- 1 new stored procedure (GDPR erasure)
- 1 new governance view
- 15 semantic views to update
- 9 notebooks to update
- 1 Streamlit app to update

## Phases

### Phase 1: Foundation (PII Vaults + Policies)
Create the 3 PII vault tables, masking policies, row access policies, and sensitivity tags. No breaking changes -- additive only.

### Phase 2: DT/View Refactoring (Remove PII)
Modify all 22 DT/view definitions to drop PII columns. This is the breaking change phase -- downstream consumers must switch to enriched views.

### Phase 3: Late Enrichment Views + Consumer Migration
Create secure enrichment views, update semantic views, refactor Streamlit and notebooks to use enriched views.

### Phase 4: Governance + Validation
GDPR erasure procedure, governance audit view, data sharing readiness validation, PII scan to confirm zero leakage.

## Implementation Table

All 40+ tasks listed with: phase, file, object, action, PII columns affected, and status.
