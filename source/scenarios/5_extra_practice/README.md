# Scenario 5 — Extra practice set (NOT pre-verified)

**Files:** `accounts_wave7_practice.csv` (19 rows) + `sleeves_wave7_practice.csv` (37 rows),
new AccountNumbers AMP-6001–AMP-6018 (plus one intentional duplicate row).

Unlike scenarios 1–4 and the earlier Wave 4 edge-case tour, **this set was generated
programmatically with randomized variety and was only checked to confirm it loads without
crashing** — I did not catalog which specific validation rules each row triggers, or run it
through the full pipeline myself. That's deliberate: it's meant as an open practice set for you
to upload, explore, and see what the Compliance Validator / Data Quality / guardrails surface on
your own, without a guide sheet telling you the answers in advance.

A few things seeded in (not exhaustive): a mix of account types and custodian codes (some real,
some not), occasional blank/negative/non-numeric AUM, a duplicate account row, sleeve groups that
may or may not sum to 100%, and one sleeve referencing an account number that isn't in this file.
Upload `accounts_wave7_practice.csv` as **Accounts** and `sleeves_wave7_practice.csv` as
**Sleeves** and see what turns up.
