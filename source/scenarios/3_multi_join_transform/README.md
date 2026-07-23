# Scenario 3 — Combine files to build one entity (multiple joins + transformation)

**Files:** `accounts_identity.csv` + `accounts_balances.csv` + `accounts_fees.csv`, 8 accounts
each (AMP-4001–AMP-4008), all joined on `AccountNumber`.

**How to run it:** Select all 3 files, entity type **Accounts**, Combine mode → Join, join key
`AccountNumber`. This chains two joins under the hood (identity ⋈ balances, then that result ⋈
fees) to produce one 8-row batch with every field, including `AnnualFeeAmount` (mapped to
`annual_fee_amount` — the only field in this scenario set that isn't part of the default Accounts
mapping already used elsewhere).

**Then add a transformation:** go to the Transformation Logic tab → Rule Builder → add a
**Compute** layer: `new_column = net_aum`, `expression = assets_under_management -
annual_fee_amount`. Apply it and check "3. Transformation Review" — you'll see a derived
`net_aum` column that didn't exist in any single source file; it only became possible once all
three files were joined together and then transformed. Confirmed: `AMP-4001` → AUM 655,000 minus
a 4,550 fee → net_aum 650,450.
