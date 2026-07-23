# Scenario 2 — Combine files to build one entity (simple join)

**Files:** `account_core.csv` (identity/relationship fields) + `account_balances.csv`
(AUM/custodian fields), 10 accounts each (AMP-3001–AMP-3010), joined on `AccountNumber`.

**How to run it:** In "1. Upload", select both files, choose entity type **Accounts**, then set
**Combine mode → Join** with join key `AccountNumber` (inner join). The two files merge side by
side into one 10-row batch — neither file alone has every column the Accounts entity needs.

**What to look for:** This is different from the multi-file *stacking* you may have used before
(several files with the *same* shape, concatenated into more rows). Here each file has a
*different* shape, and joining merges them into wider rows. Compare the row count before/after:
10 + 10 rows in, 10 rows out (not 20) — a join, not a stack. No data-quality issues are seeded
here on purpose, to keep the join mechanic itself clean and easy to see.
