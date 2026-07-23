# Migration Front End

Web UI for the multi-agent data migration pipeline (`pipeline.py` +
`agents/`). An Orchestrator (Conductor) routes each batch through 9 specialist agents, gated by
a human-in-the-loop review before anything loads. See the **"0. How to Use"** tab inside the app
for a full walkthrough, glossary, and entity-relationship reference.

## Run with Docker (recommended — works the same on any machine)

```
docker compose up --build
```

Then open http://localhost:8501. The `source/`, `output/`, `audit/`, `cutover/`, and
`quarantine/` folders (plus `core_models_registry.json`) are bind-mounted, so everything the app
reads or writes lives in this project folder on your host machine and survives container
rebuilds. Stop with `Ctrl+C` or `docker compose down`.

To rebuild after changing `pipeline.py`, `app.py`, or `agents/`:

```
docker compose up --build
```

## Run without Docker

```
pip install -r requirements.txt
streamlit run app.py
```

Opens at http://localhost:8501.

## Optional: enable the Claude reasoning layer (`.env`)

The app runs fully offline by default — the local learning memory needs no key.
To additionally turn on Claude-powered explanations of validation findings and
mapping suggestions, put your Anthropic key in a **`.env`** file:

```
cp .env.example .env          # then edit .env and paste your key
# .env contains:  ANTHROPIC_API_KEY=sk-ant-...
pip install anthropic          # only needed for this layer
```

Then restart (`streamlit run app.py`) or `docker compose up`. The `.env` file
is git-ignored and docker-ignored, so the key never lands in a commit or a
built image. A plain shell `export ANTHROPIC_API_KEY=...` also works and takes
precedence over `.env`. The sidebar shows whether the layer is on or off. Even
when on, Claude only ever *suggests* and *explains* — a human still approves
every load and the deterministic checks always run.

## Using the app



Upload a source file (CSV/Excel/JSON) — or several, choosing **Stack** (union same-shape files)
or **Join** (merge different-shape files on a shared key) — review the extract/map/transform/
validate results, then Approve & Load or Reject. Approved runs write to `output/`; every run
(approved or rejected) writes a review summary, validation report, and audit log to `audit/`.
Past runs are browsable under the "Audit History" tab.

### Quickstart scenarios

The "1. Upload" tab has a **Quickstart Scenarios** picker that loads pre-built example files
straight from `source/scenarios/` with one click — no manual upload needed:

1. **One-to-one mapping** (`scenarios/1_one_to_one/`) — a single file, minimal transformation.
2. **Simple join** (`scenarios/2_simple_join/`) — two files, different shapes, merged on one key.
3. **Multi-join + transformation** (`scenarios/3_multi_join_transform/`) — three files merged on
   one key, then a computed column via the Rule Builder.
4. **One file, multiple entities** (`scenarios/4_one_to_many/`) — the same file mapped two ways
   (Accounts vs. Households), including the duplicate-key case that shows up on a grain mismatch.
5. **Extra practice, not pre-verified** (`scenarios/5_extra_practice/`) — a bigger, randomized
   set left open for you to explore.

Full details in `SCENARIOS.md` and each scenario folder's own `README.md`.
