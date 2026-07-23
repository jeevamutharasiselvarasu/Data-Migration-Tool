# Scenario 4 — One source file, multiple destination entities

**File:** `new_business_intake.csv` (8 accounts across 6 households — one row per account).

**How to run it (twice, same file):**
1. Upload it once, entity type **Accounts** — produces a clean 8-row Accounts batch. Every
   required field is present, no duplicate keys (confirmed).
2. Upload the *same file* again, entity type **Households** — because the file is at
   account-grain, two households (HH030 and HH033, each with 2 accounts) appear twice. The
   "5. Data Quality" agent correctly flags this as `DUPLICATE_KEY` (confirmed: 4 rows affected
   across HH030 + HH033).

**What to look for:** This is the honest version of "derive multiple entities from one file" —
each entity type's default field map only pulls out the columns it needs and ignores the rest,
so no new code was required to make this work. But it also surfaces a real lesson: when a flat
file's grain doesn't match the entity you're deriving, you'll see duplicates on the "one" side of
a one-to-many relationship. In a production pipeline you'd de-duplicate the household slice
first (e.g. `DISTINCT` on HouseholdID) before loading Households from this file — this prototype
doesn't have a de-dupe transformation layer yet, so for now the right move is to either accept
the duplicates as informational, or only use this file for the Accounts pass and get Households
from a proper household-grain export instead.
