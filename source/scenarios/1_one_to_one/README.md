# Scenario 1 — One-to-one mapping, minimal transformation

**File:** `advisors.csv` (8 rows)

**How to run it:** Upload `advisors.csv` and select entity type **Advisors**. No combine mode
needed — single file, no join.

**What to look for:** Every source column maps straight across with a rename and nothing else —
no code crosswalks, no sleeve math, no compliance overlay. Check the "2. Schema Mapping" tab and
notice the mapping is a plain 1:1 rename (`RepCode → advisor_rep_code`, `RepName → advisor_name`,
etc.), and "3. Transformation Review" shows no assumptions. This is the simplest possible
migration shape — useful as a baseline before the join/derivation scenarios below.
