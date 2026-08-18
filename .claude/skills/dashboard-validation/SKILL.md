---
name: dashboard-validation
description: Validate universe, data, analysis and dashboard consistency across the entire pipeline.
---

# dashboard-validation

## Universe

Validate:

- `config/dashboard_universe.json` exists
- expected_count matches actual count
- symbols are unique
- all symbols are valid strings
- data pipeline does not silently add/remove universe members

## Data

Validate:
- schema
- timestamp
- provenance
- no NaN/undefined
- filters
- no fabricated values

## Analysis

Validate:
- every qualifying asset has analysis
- valid states
- finite scores
- evidence and risk flags
- raw facts unchanged

## Dashboard

Validate:
- valid HTML
- unique IDs
- every overview link resolves
- every qualifying asset has exactly one card
- ranks/counts consistent
- required metrics present
- no NaN/undefined leakage
- timestamp/source/methodology visible

## Cross-layer invariant

For the filtered set:

`data qualifying count == analysis asset count == dashboard detail-card count`

Raw displayed metrics must equal the data snapshot except for display rounding.

Return PASS/FAIL and identify the responsible layer for every failure.
