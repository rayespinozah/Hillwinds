**3) dbt System Design (in `DESIGN.md`)**  
- Outline staging → intermediate → fact models for these sources + enrichment (5–7 bullets).  
- Describe incremental merge logic, schema/column tests, freshness/anomaly checks, metadata columns, and a safe model deprecation strategy.

Everything was created on Databricks (I am including png of catalogs - schemas - jobs - queries/notebooks)

Architecture Overview
Staging Layer (models/staging/)

stg_employees: Clean employees_raw → normalize emails, validate EINs, extract notes fields
stg_plans: Dedupe plans_raw → validate date ranges, normalize carrier names
stg_claims: Clean claims_raw → validate amounts, coerce service_date
Materialization: view with source freshness checks (12h warn, 24h error)
Tests: not_null on PKs, unique on claim_id/person_id, accepted_values for enums

Intermediate Layer (models/intermediate/)

int_plans_normalized: Stitch overlapping/adjacent intervals per company+plan_type (gap detection prep)
int_claims_rolling_90d: Pre-compute rolling windows for spike detection (optimize performance)
int_employees_with_ein: Infer missing EINs from email domains using company_lookup.json
Materialization: ephemeral (reused in multiple marts)
Tests: relationships to staging, custom assert_no_cartesian_joins macro

Fact Layer (models/marts/)

fct_plan_gaps: Output from SQL #1 (gap_start, gap_end, gap_length_days, carriers)
fct_claims_cost_spikes: Output from SQL #2 (90d windows, pct_change >200%)
fct_employee_roster_mismatches: Output from SQL #3 (expected vs observed, severity levels)
Materialization: incremental with unique_key, partitioned by date
Tests: Row count stability, cost outlier detection (3σ), no future dates


Increméntal Strategy
{{ config(
  materialized='incremental',
  unique_key=['company_ein', 'window_end'],
  partition_by={'field': 'window_end', 'data_type': 'date'},
  cluster_by=['company_ein'],
  on_schema_change='append_new_columns'
) }}

SELECT * FROM {{ ref('int_claims_rolling_90d') }}
{% if is_incremental() %}
WHERE window_end > (SELECT MAX(window_end) FROM {{ this }})
  OR window_end >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
{% endif %}

**4) Deep Debugging & Incident Response (in `DESIGN.md`)**  
- Scenario: new feed adds 20M rows/day; costs triple; models slow 4m → 45m; outputs wrong despite passing dbt tests.  
- Briefly explain likely causes (partitioning/clustering, key explosion, duplicates), how you’d isolate the regression, detect logic issues beyond tests, guardrails to prevent recurrence, and your first 30-minute plan.

A.
Symptom: 11x slower runtime
Likely Cause: Missing partition keys
Evidence to Check: Query plan shows full table scan; bytes processed jumped 50x

B.
Symptom: 3x cost increase
Likely Cause: Cartesian join explosion
Evidence to Check: Intermediate table has 200M rows (expected 20M); shuffle output >500GB

C.
Symptom: Wrong outputs despite passing tests
Likely Cause: Duplicate keys in source
Evidence to Check: Same claim_id appears 5x with different amounts; incremental merge fails

D.
Symptom: Uneven cluster performance
Likely Cause: Data skew
Evidence to Check: 1 company has 18M/20M rows; single worker maxed at 100% CPU

Post-Incident:
Add uniqueness test on claim_id in staging
Add partition config to all fact tables
Enable query result caching (reduce redundant scans)
Set up cost budget alerts ($500/day threshold)


