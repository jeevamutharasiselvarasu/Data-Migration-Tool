# Source Data — Scenario Guide

The sample source files in `source/` now carry a deliberate set of edge cases on top of the
original clean baseline, so every agent and validation rule in the pipeline has something to
catch. Original rows are untouched — all scenarios were appended as new rows (or, for
`accounts.json`, replaced with a purpose-built file). Every scenario below was run through the
actual pipeline and confirmed to trigger the stated rule before being deployed.

## Recommended test order

**Households → Accounts → Sleeves → Models → Transactions.**

This is slightly different from the idealized load order in `entity_hierarchy.md` (which lists
Core Models before Households/Accounts) — here it matters for one specific reason: approving
and loading *any* batch that references `MDL_REP_C` (an advisor-built model not yet in
`core_models_registry.json`) automatically registers it, and it then stops showing up as
"pending" anywhere. Testing Accounts and Sleeves *before* Models lets you see the
`PENDING_CORE_MODEL_REGISTRATION` warning; loading Models afterward demonstrates the
pending → registered lifecycle (confirmed: re-running Accounts after Models is loaded no
longer flags it).

Two checks specifically need a parent entity already loaded in your session first:
- Orphan checks (`ORPHAN_RECORD`) compare against the most recently loaded batch of the parent
  entity — e.g. an orphan account only surfaces once Households has been loaded at least once.
- The registration lifecycle above needs Models to actually be approved & loaded.

## Households (`households.csv`) — 13 rows (10 original + 3 new)

| Key | Scenario | Expected result |
|---|---|---|
| HH013 | Unknown custodian (`MERRILL`) | `UNKNOWN_CUSTODIAN_CODE` (Compliance) |
| HH001 (2nd row) | Exact duplicate of the original HH001 row | `DUPLICATE_KEY` on household_id |
| HH015 | Missing `PrimaryClientID`, non-standard status (`PENDING_CLOSE`) | `REQUIRED_FIELD` (primary_client_id) — same rule as pre-existing HH007 |

Note: households map `TotalAUM` → `total_aum`, not `assets_under_management`, so there's no
existing rule that checks it for negative/non-numeric values — that scenario lives on Accounts
instead (see AMP-1033 / AMP-1034 below), where `AUM` maps to `assets_under_management`.

## Accounts (`accounts.csv`) — 40 rows (29 original + 11 new, tagged Wave 4)

| Key | Scenario | Expected result |
|---|---|---|
| AMP-1030 | Unknown `AccountType` (`SEP`) | `UNKNOWN_ACCOUNT_TYPE` (Compliance) |
| AMP-1031 | References `HH999` (doesn't exist) | `ORPHAN_RECORD` (household_id) — needs Households loaded first |
| AMP-1032 (both rows) | References `MDL_REP_C` (unregistered) + duplicate `AccountNumber` | `PENDING_CORE_MODEL_REGISTRATION` + `DUPLICATE_KEY` — test before Models is loaded |
| AMP-1033 | Negative AUM | `NEGATIVE_AUM` (Data Quality) — **also unconditionally blocks the pre-load guardrail**, force or not, until fixed/excluded |
| AMP-1034 | Non-numeric AUM (`"TBD"`) | `NON_NUMERIC_AUM` (Data Quality) |
| AMP-1035 | Missing `ClientID` | `REQUIRED_FIELD` (client_id) |
| AMP-1036 | Unknown custodian (`MERRILL`) + missing `DriftThreshold` | `UNKNOWN_CUSTODIAN_CODE` + drift-default assumption |
| AMP-1037 | Unparseable `InceptionDate` (`"not-a-date"`) + 3P model | Date coerced to null, visible in Transformation Review |
| AMP-1038, AMP-1039 | Clean accounts | No errors — added only to host the sleeve-only scenarios below |

Confirmed: because AMP-1033 survives the Compliance Validator with negative AUM, `approve()`
raises a `GuardrailViolation` even with `force=True` — the only way to load this batch is to
fix or exclude that row first. This is intentional and worth demonstrating as-is.

## Sleeves (`sleeves.csv`) — 103 rows (86 original + 17 new)

| Key | Scenario | Expected result |
|---|---|---|
| SLV-AMP-1010-03 | Zero-allocation row added to an already-balanced group | Excluded from load when Rule 2 (exclude zero) is on — confirmed AMP-1010 goes from 3 rows to 2 after the toggle |
| SLV-AMP-1001-01 (2nd row) | Duplicate `SleeveID` | `DUPLICATE_KEY` — **also** pushes AMP-1001's allocation sum to ~120%, so with Rule 1 (normalize) off it *also* shows up under `SLEEVE_SUM`. Good illustration of why duplicate keys corrupt downstream math, not just key integrity. |
| SLV-AMP-1038-01/02 | Allocations sum to 85%, not 100% | `SLEEVE_SUM` (Compliance) + pre-load guardrail block — **only visible with Rule 1 (normalize) turned off**; by default normalization silently rescales the group back to 100%, which is itself worth showing as what normalization does |
| SLV-AMP-1039-01 | References `MDL_GHOST` (doesn't exist in models.csv) | `ORPHAN_RECORD` (model_id) — needs Models loaded first (as a fixture, not necessarily registered) |
| SLV-AMP9999-01 | References `AMP-9999` (doesn't exist in accounts.csv) | `ORPHAN_RECORD` (account_number) — needs Accounts loaded first |
| AMP-1030–1037 | Balanced 100% sleeve sets added for the new Wave 4 accounts | So each account-level scenario above also has matching sleeve data |

## Models (`models.csv`) — 12 rows (8 original + 4 new)

| Key | Scenario | Expected result |
|---|---|---|
| MDL_3P_TACTICAL | `ModelType = 3P` | Maps to `THIRD_PARTY` — first sample data to exercise this code path |
| MDL_NONAME | Missing `ModelName` | `REQUIRED_FIELD` (model_name) |
| MDL_LEGACY | Unrecognized `ModelType` (`EXPERIMENTAL`) | Falls back to `UNKNOWN` in transformation (no formal rule, visible in Transformation Review) |
| MDL_CONS (2nd row) | Duplicate `ModelCode` | `DUPLICATE_KEY` |
| MDL_REP_C | Advisor-built, not in `core_models_registry.json` at the start | `PENDING_CORE_MODEL_REGISTRATION` — approving Models registers it, after which it stops showing as pending anywhere (confirmed) |

## Transactions (`transactions.csv`) — 119 rows (114 original + 5 new)

| Key | Scenario | Expected result |
|---|---|---|
| TX-ORPHAN01 | References `AMP-9999` (doesn't exist) | `ORPHAN_RECORD` (account_number) — needs Accounts loaded first |
| TX-680097 (2nd row) | Duplicate `TransactionID` | `DUPLICATE_KEY` |
| TX-MISSING-ACCT | Missing `AccountNumber` | `REQUIRED_FIELD` (account_number) |
| (blank TransactionID) | Missing `TransactionID` | `REQUIRED_FIELD` (transaction_id) |
| TX-CUST01 | Unknown custodian (`MERRILL`) | `UNKNOWN_CUSTODIAN_CODE` |

## Multi-file combine (`accounts.csv` + `accounts.json`)

`accounts.json` was rewritten as a clean 5-row "Wave 5 — New Business" batch (AMP-2001–AMP-2005),
deliberately free of errors. Upload it together with `accounts.csv` in the "1. Upload" tab to see
the multi-file combine feature: 45 combined rows (confirmed), each tagged with its originating
`source_file`, with no new validation noise introduced by the combine itself.

## Reference workbook

`sample_source_data.xlsx` was regenerated from the same final data and now has a **Scenario
Guide** sheet (in addition to README) listing everything above in one place.

## A note on the `model_id` required-field check

`compliance_transform()` runs `.astype(str)` on `model_id` before the Data Quality agent checks
required fields, which turns a missing value into the literal string `"nan"` instead of a true
null — so a blank `ModelCode` will *not* trigger `REQUIRED_FIELD`. This is a pre-existing quirk
in `pipeline.py`, not something introduced by this data update. The missing-required-field
scenario on Accounts (AMP-1035) uses `ClientID` instead, which isn't touched by that
normalization and correctly triggers `REQUIRED_FIELD`. Worth a look if you want the model_id
check to behave the same way.
