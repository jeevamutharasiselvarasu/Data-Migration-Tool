# Data Migration Tool

A local, agentic **data-migration pipeline** with a guided Streamlit UI. It moves wealth-management
data from source extracts into a destination-shaped schema the way a
real migration wave would — extract, map, transform, validate, human-approve, load, reconcile, and
audit — entirely on your machine so you can rehearse and inspect every step before touching
production data.

> **Note:** "Agents" here means the multi-agent *software* pattern (named classes, each with one
> job). The core pipeline is fully deterministic Python and needs **no API key**. An optional Claude
> reasoning layer can be switched on with a key, but it only ever *suggests* and *explains* — a
> human still approves every load.

---

## What it does

An **Orchestrator (Conductor)** routes each batch of records through nine specialist agents in a
fixed order. Nothing is written to the destination until a human reviews the findings and approves.

| # | Agent | Responsibility |
|---|-------|----------------|
| 1 | **Extractor** | Reads source file(s) — CSV, Excel, or JSON |
| 2 | **Schema Mapper** | Renames source columns to destination fields per the active mapping |
| 3 | **Sleeve Math** | Applies allocation rules (sleeves entity): normalize to 100%, exclude zero-allocations |
| 4 | **Compliance Validator** | Normalizes codes (account/model type, custodian, asset class); flags unknown codes and pending Core Model registrations |
| 5 | **Data Quality** | Required fields, numeric/non-negative AUM, duplicate keys, orphan foreign keys |
| — | **HITL Gate** | Nothing loads until a human clicks Approve or Reject |
| 6 | **Loader** | Writes the approved batch; registers newly-approved Core Models |
| 7 | **Reconciler** | Compares source vs. destination record counts and total AUM drift |
| 8 | **Cutover / Rollback** | Promotes a clean load, or quarantines the file and restores the prior registry on a breach |
| 9 | **Audit & Evidence** | Writes a redacted, timestamped audit record of the whole run |

Cross-cutting **Guardrails** enforce deterministic invariants (allocations within tolerance of
100%, no negative AUM survives) that block a load regardless of any "force" flag.

---

## Key features

- **Guided single-flow UI** — a sidebar walks you through Upload → Map → Transform → Validate &
  Approve → History, with a live pipeline-stage stepper and an expandable agent activity log on
  every step.
- **Six entity types** — households, accounts, sleeves, models, transactions, and advisors — each
  with its own schema map, keys, and validation rules.
- **Multi-file combine** — stage several files and either **stack** them (union same-shape files
  into more rows) or **join** them (merge different-shape files on a shared key: inner / left /
  outer).
- **Configurable transformation logic** — three modes: deterministic **toggles**, a no-code
  **rule builder** (stacked filter / normalize / fill-default / map-values / compute layers), or a
  restricted **custom Python** rule.
- **Learning memory (offline)** — the agents evolve between runs by remembering what you approve
  and teach: accepted custodian codes, taught account-type crosswalks, and confirmed column
  mappings. Stored as plain JSON, no network required.
- **Optional Claude reasoning** — with an `ANTHROPIC_API_KEY`, a thin layer explains findings in
  plain language and proposes mappings. Off by default; the app is fully functional without it.
- **Human-in-the-loop + full audit trail** — every run (approved or rejected) writes a review
  summary, validation report, and audit log.
- **Dockerized** — `docker compose up` and open the browser.

---

## Quickstart (local)

```bash
pip install -r requirements.txt
streamlit run app.py
```

Opens at http://localhost:8501. On the **Upload** step, use the **⚡ Quickstart Scenarios** picker
to load a pre-built example with one click, or upload your own CSV/Excel/JSON.

### Run with Docker

```bash
docker compose up --build
```

The `source/`, `output/`, `audit/`, `cutover/`, `quarantine/` folders and the learning memory are
bind-mounted, so results persist on your host across container rebuilds.

---

## Configuration & secrets

The app reads its optional API key from an environment variable, so no secret ever needs to live
in the repo.

**Local:** copy the template and add your key (the file is git-ignored):

```bash
cp .env.example .env
# .env:  ANTHROPIC_API_KEY=sk-ant-...
pip install anthropic     # already in requirements.txt
```

**Streamlit Community Cloud:** in the deploy dialog under **Advanced settings → Secrets**, paste:

```toml
ANTHROPIC_API_KEY = "sk-ant-..."
MIGRATION_LLM_MODEL = "claude-sonnet-5"
```

Streamlit exposes root-level secrets as environment variables, so the same code path works in both
places. Without a key, the app runs fully offline and the local learning memory still works.

---

## Deploy to Streamlit Community Cloud

1. Push to GitHub (`git push origin main`).
2. At [share.streamlit.io](https://share.streamlit.io) → **Create app** → select this repo, branch
   `main`, main file **`app.py`**. (For a private repo, first grant private-repo access under your
   username → Settings → Linked accounts → Source control.)
3. Add your key under **Advanced settings → Secrets** (above), then **Deploy**.

**Caveats on Community Cloud:** the filesystem is ephemeral, so the learning memory, audit files,
and outputs reset on each reboot/redeploy — fine for a demo, but for durable state host the Docker
image on a VM instead. Keep the hosted instance to the synthetic sample data only.

---

## Sample data & scenarios

`source/` ships with a full synthetic dataset (households, accounts, sleeves, models, transactions)
plus five teaching scenarios under `source/scenarios/`, loadable from the Quickstart picker:

1. **One-to-one mapping** — a single file, minimal transformation (advisors).
2. **Simple join** — two files merged on one key into one Accounts batch.
3. **Multi-join + transformation** — three files merged, then a computed column via the rule builder.
4. **One file → many entities** — the same file mapped as Accounts (clean) and Households (shows a duplicate-key case).
5. **Extra practice** — a larger, randomized set left un-catalogued for open exploration.

See **`SCENARIOS.md`** for the full breakdown of every seeded edge case and the rule it triggers.

---

## How the learning memory works

On an approved & reconciled load, the Orchestrator calls the learning store, which records:

- **Custodian codes** present in the batch → the Compliance Validator stops flagging them next time.
- **Confirmed column → field mappings** → offered as suggestions when the same column reappears.

Separately, on the **Map** step you can **teach an account-type rule** (raw code → normalized
value) when an `UNKNOWN_ACCOUNT_TYPE` appears; the Compliance Validator applies it on every future
run. Everything is inspectable, resettable JSON on the **Learning Memory** page (`knowledge_store.json`).

---

## Project structure

```
app.py                     Streamlit UI (guided single-flow)
pipeline.py                Shared knowledge layer + low-level ETL primitives
agents/
  orchestrator.py          Conductor: routes the batch, runs HITL, learning, rollback
  extractor.py             1. Extract (single / stack / join)
  schema_mapper.py         2. Map
  sleeve_math.py           3. Transform (toggles / rule builder / custom)
  compliance_validator.py  4. Compliance
  data_quality.py          5. Data quality
  loader.py                6. Load
  reconciler.py            7. Reconcile
  cutover.py / rollback.py 8. Cutover or Rollback
  audit_evidence.py        9. Audit & evidence
  guardrails.py            Deterministic pre-load invariants + PII redaction
  knowledge_store.py       Offline learning memory
  llm.py                   Optional, key-gated Claude reasoning layer
  context.py               Shared MigrationContext state object
source/                    Sample data + scenarios/
requirements.txt           Dependencies
Dockerfile, docker-compose.yml
.env.example               Secrets template (copy to .env)
SCENARIOS.md               Scenario / edge-case reference
README_APP.md              Additional run notes
```

---

## Security notes

- **Never commit `.env`** — it's git-ignored. Provide the key via `.env` locally or Streamlit
  Secrets in the cloud.
- Use only the **synthetic sample data** included here; do not load real client data into a hosted
  instance.
- PII-ish columns (names, client IDs) are redacted in audit/evidence artifacts by the guardrails
  layer.

---

*Prototype for demonstration and rehearsal. Not a production migration system.*
