"""
Front end for the multi-agent data migration pipeline.

Drives agents/orchestrator.py's OrchestratorAgent, which coordinates:
  1. Extractor Agent -> 2. Schema Mapper -> 3. Sleeve Math Agent ->
  4. Compliance Validator -> 5. Data Quality -> [HITL gate] ->
  6. Loader Agent -> 7. Reconciler Agent -> 8. Cutover Agent (or the
  Rollback Path on a reconciliation breach) -> 9. Audit & Evidence Agent

Tabs:
  1. Upload            - upload a source file and pick which entity it represents
  Agent Pipeline        - live run log of every agent that has executed so far
  2. Schema Mapping      - dynamically edit the source -> destination field map
  3. Transformation Review - source vs. destination data, sleeve math detail,
                             final destination data set
  4. Validate & Approve  - compliance / data-quality findings, HITL gate,
                           load -> reconcile -> cutover/rollback
  Audit History          - browse past runs, cutover records, quarantine
                           incidents, and registered Core Models

Run with:  streamlit run app.py
"""

import glob
import io
import json
import os
import tempfile
from datetime import datetime

import pandas as pd
import streamlit as st

import pipeline as pl
from agents import llm
from agents.knowledge_store import KnowledgeStore
from agents.orchestrator import OrchestratorAgent

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
AUDIT_DIR = os.path.join(BASE_DIR, "audit")
CUTOVER_DIR = os.path.join(BASE_DIR, "cutover")
QUARANTINE_DIR = os.path.join(BASE_DIR, "quarantine")
REGISTRY_PATH = os.path.join(BASE_DIR, "core_models_registry.json")
KNOWLEDGE_STORE_PATH = os.path.join(BASE_DIR, "knowledge_store.json")
SOURCE_DIR = os.path.join(BASE_DIR, "source")
SCENARIOS_DIR = os.path.join(SOURCE_DIR, "scenarios")

# Quickstart scenarios -- pre-built local files under source/scenarios/, loadable
# with one click (no manual upload needed) to demonstrate each migration pattern.
QUICKSTART_SCENARIOS = [
    {
        "id": "1",
        "label": "1. One-to-one mapping (Advisors) -- minimal transformation",
        "entity_key": "advisors",
        "files": ["scenarios/1_one_to_one/advisors.csv"],
        "combine_mode": "stack",
    },
    {
        "id": "2",
        "label": "2. Simple join -> Accounts (2 files, 1 key)",
        "entity_key": "accounts",
        "files": ["scenarios/2_simple_join/account_core.csv", "scenarios/2_simple_join/account_balances.csv"],
        "combine_mode": "join",
        "join_key": "AccountNumber",
        "join_how": "inner",
    },
    {
        "id": "3",
        "label": "3. Multi-join + transformation -> Accounts (3 files, 1 key)",
        "entity_key": "accounts",
        "files": [
            "scenarios/3_multi_join_transform/accounts_identity.csv",
            "scenarios/3_multi_join_transform/accounts_balances.csv",
            "scenarios/3_multi_join_transform/accounts_fees.csv",
        ],
        "combine_mode": "join",
        "join_key": "AccountNumber",
        "join_how": "inner",
    },
    {
        "id": "4a",
        "label": "4a. One file -> many entities: Accounts pass (clean)",
        "entity_key": "accounts",
        "files": ["scenarios/4_one_to_many/new_business_intake.csv"],
        "combine_mode": "stack",
    },
    {
        "id": "4b",
        "label": "4b. One file -> many entities: Households pass (shows duplicates)",
        "entity_key": "households",
        "files": ["scenarios/4_one_to_many/new_business_intake.csv"],
        "combine_mode": "stack",
    },
    {
        "id": "5a",
        "label": "5. Extra practice -- Accounts (not pre-verified, explore on your own)",
        "entity_key": "accounts",
        "files": ["scenarios/5_extra_practice/accounts_wave7_practice.csv"],
        "combine_mode": "stack",
    },
    {
        "id": "5b",
        "label": "5. Extra practice -- Sleeves (not pre-verified, explore on your own)",
        "entity_key": "sleeves",
        "files": ["scenarios/5_extra_practice/sleeves_wave7_practice.csv"],
        "combine_mode": "stack",
    },
]

PIPELINE_STAGES = [
    ("Extract", "1. Extractor Agent"),
    ("Map", "2. Schema Mapper"),
    ("Sleeve Math", "3. Sleeve Math Agent"),
    ("Compliance", "4. Compliance Validator"),
    ("Data Quality", "5. Data Quality"),
    ("HITL", "Orchestrator (Conductor)"),
    ("Load", "6. Loader Agent"),
    ("Reconcile", "7. Reconciler Agent"),
    ("Cutover", ["8. Cutover Agent", "Rollback Path"]),
    ("Audit", "9. Audit & Evidence Agent"),
]

DEFAULT_CUSTOM_SLEEVE_TEMPLATE = """def custom_sleeve_math(df):
    # df: the mapped pandas DataFrame for this batch, before the Compliance
    # Validator runs. Must return a DataFrame. Example below reproduces the
    # built-in rules manually -- edit freely.
    if "sleeve_allocation_pct" in df.columns and "account_number" in df.columns:
        df["sleeve_allocation_pct"] = (
            df.groupby("account_number")["sleeve_allocation_pct"]
            .transform(lambda x: x / x.sum() * 100)
        )
        df = df[df["sleeve_allocation_pct"] > 0]
    return df
"""

DEFAULT_SLEEVE_MATH_CONFIG = {
    "mode": "toggles",  # "toggles" | "layers" | "custom"
    "normalize": True,
    "exclude_zero": True,
    "zero_threshold": 0.0,
    "group_key": "account_number",
    "tolerance": 0.5,
    "custom_code": DEFAULT_CUSTOM_SLEEVE_TEMPLATE,
    "layers": [],
}

LAYER_TYPES = ["filter", "normalize", "fill_default", "map_values", "compute"]
LAYER_LABELS = {
    "filter": "Filter rows",
    "normalize": "Normalize group",
    "fill_default": "Fill default",
    "map_values": "Map values",
    "compute": "Compute column",
}
FILTER_OPERATORS = ["==", "!=", ">", "<", ">=", "<=", "is null", "is not null"]


def effective_sleeve_math_config(cfg: dict) -> dict:
    """Resolve the editor's config (which keeps custom_code/layers around even
    when not the active mode, so drafts aren't lost when switching modes)
    into the config the agents actually consume."""
    resolved = dict(cfg)
    mode = cfg.get("mode", "toggles")
    resolved["custom_code"] = cfg.get("custom_code") if mode == "custom" else None
    resolved["layers"] = cfg.get("layers", []) if mode == "layers" else []
    return resolved


STATUS_COLORS = {
    "OK": "#d4edda",
    "APPROVED": "#d4edda",
    "WARNING": "#fff3cd",
    "HITL_GATE": "#d1ecf1",
    "RUNNING": "#d1ecf1",
    "SKIPPED": "#e2e3e5",
    "BLOCKED": "#f8d7da",
    "BREACH": "#f8d7da",
    "REJECTED": "#f8d7da",
    "ROLLED_BACK": "#f8d7da",
}

st.set_page_config(page_title="Data Migration Tool", layout="wide", initial_sidebar_state="expanded")


def get_store() -> KnowledgeStore:
    if "knowledge_store" not in st.session_state:
        st.session_state["knowledge_store"] = KnowledgeStore(KNOWLEDGE_STORE_PATH)
    return st.session_state["knowledge_store"]


def style_run_log(log_df: pd.DataFrame):
    def color(val):
        return f"background-color: {STATUS_COLORS.get(val, '')}"

    return log_df.style.applymap(color, subset=["Status"])


def render_run_log(run_log: list, container=None):
    target = container if container is not None else st
    if not run_log:
        target.write("No agent activity yet.")
        return
    log_df = pd.DataFrame(run_log).rename(
        columns={"agent": "Agent", "status": "Status", "detail": "Detail", "timestamp": "Timestamp", "duration_ms": "Duration (ms)"}
    )[["Agent", "Status", "Detail", "Duration (ms)", "Timestamp"]]
    try:
        target.dataframe(style_run_log(log_df), use_container_width=True, hide_index=True)
    except Exception:
        target.dataframe(log_df, use_container_width=True, hide_index=True)


def render_stage_stepper(context):
    """Always-visible progress bar across the top of the app showing which
    pipeline stage the current batch has reached, so the flow feels like a
    guided process rather than a pile of independent tabs."""
    names_seen = {}
    if context is not None:
        for entry in context.run_log:
            names_seen.setdefault(entry["agent"], entry["status"])
            names_seen[entry["agent"]] = entry["status"]

    cols = st.columns(len(PIPELINE_STAGES))
    for col, (label, agent_names) in zip(cols, PIPELINE_STAGES):
        names = agent_names if isinstance(agent_names, list) else [agent_names]
        status = next((names_seen[n] for n in names if n in names_seen), None)
        if status in ("OK", "APPROVED"):
            badge, bg = "🟢", "#d4edda"
        elif status in ("WARNING", "HITL_GATE", "RUNNING"):
            badge, bg = "🟡", "#fff3cd"
        elif status in ("BLOCKED", "BREACH", "REJECTED", "ROLLED_BACK"):
            badge, bg = "🔴", "#f8d7da"
        elif status == "SKIPPED":
            badge, bg = "⚫", "#e2e3e5"
        else:
            badge, bg = "⚪", "#eef0f2"
        col.markdown(
            f"<div style='text-align:center;padding:6px 2px;border-radius:6px;background:{bg};'>"
            f"<div style='font-size:17px;line-height:1.2'>{badge}</div>"
            f"<div style='font-size:10.5px;line-height:1.2'>{label}</div></div>",
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Guided single-flow navigation — one step visible at a time, driven by the
# sidebar, so the pipeline reads as a sequence rather than a wall of tabs.
# ---------------------------------------------------------------------------

PAGE_HOWTO = "How to Use"
PAGE_UPLOAD = "1 · Upload"
PAGE_MAP = "2 · Map"
PAGE_TRANSFORM = "3 · Transform"
PAGE_VALIDATE = "4 · Validate & Approve"
PAGE_HISTORY = "History"
PAGE_MEMORY = "Learning Memory"
STEP_PAGES = [PAGE_UPLOAD, PAGE_MAP, PAGE_TRANSFORM, PAGE_VALIDATE]
ALL_PAGES = [PAGE_HOWTO] + STEP_PAGES + [PAGE_HISTORY, PAGE_MEMORY]

if "nav_page" not in st.session_state:
    st.session_state["nav_page"] = PAGE_HOWTO

# Apply any programmatic navigation (Back/Next buttons) requested on the last
# run, BEFORE the sidebar radio widget is instantiated — Streamlit forbids
# mutating a widget-keyed value after the widget exists.
if "_pending_nav" in st.session_state:
    st.session_state["nav_page"] = st.session_state.pop("_pending_nav")


def goto(page: str):
    st.session_state["_pending_nav"] = page
    st.rerun()


ctx_now = st.session_state.get("context")
store = get_store()

with st.sidebar:
    st.markdown("### source → destination")
    st.caption("Multi-agent migration pipeline")

    st.radio(
        "Steps",
        ALL_PAGES,
        key="nav_page",
        format_func=lambda p: {
            PAGE_HOWTO: "❓ How to Use",
            PAGE_UPLOAD: "① Upload",
            PAGE_MAP: "② Map",
            PAGE_TRANSFORM: "③ Transform",
            PAGE_VALIDATE: "④ Validate & Approve",
            PAGE_HISTORY: "🗂 History",
            PAGE_MEMORY: "🧠 Learning Memory",
        }[p],
    )

    st.divider()
    if ctx_now is not None:
        st.markdown("**Current batch**")
        st.caption(f"Entity: {pl.ENTITY_CONFIGS[ctx_now.entity_key]['label'].split(' (')[0]}")
        if ctx_now.source_df is not None:
            st.caption(f"{len(ctx_now.source_df)} rows · {len(ctx_now.source_df.columns)} cols")
        n_err = sum(1 for e in ctx_now.errors if e.get("severity") == "ERROR")
        n_warn = sum(1 for e in ctx_now.errors if e.get("severity") == "WARNING")
        decision = ctx_now.decision or ("PASSED" if ctx_now.passed else "needs review")
        st.caption(f"Findings: {n_err} error(s), {n_warn} warning(s)")
        st.caption(f"Status: **{decision}**")
    else:
        st.caption("No batch loaded yet — start on ① Upload.")

    st.divider()
    stats = store.stats()
    st.markdown("**🧠 Learning memory**")
    st.caption(
        f"{stats['runs_learned']} run(s) learned · {stats['custodian_codes']} custodian code(s) · "
        f"{stats['account_types']} taught type(s) · {stats['mapping_columns']} mapping rule(s)"
    )
    st.caption(llm.status_note())


# ---- Main header: compact title + always-on stage stepper + activity log ----
st.title("Data Migration Tool")
render_stage_stepper(ctx_now)

with st.expander("🔎 Agent activity log — what each agent did on this batch", expanded=False):
    if ctx_now is None:
        st.caption("No agent activity yet. Load a batch on the Upload step.")
    else:
        render_run_log(ctx_now.run_log)

st.markdown("&nbsp;", unsafe_allow_html=True)

page = st.session_state["nav_page"]

# ---------------------------------------------------------------------------
# 0. How to Use — orientation, glossary, scenario guide
# ---------------------------------------------------------------------------
if page == PAGE_HOWTO:
    st.subheader("How this platform works")
    st.markdown(
        """
This is a local prototype of an **agentic migration pipeline**: it moves data from a source system
source extracts into a destination-shaped schema, the same way a real
migration wave would, but entirely on your machine so you can see every step and rehearse
scenarios before touching production data.

An **Orchestrator (Conductor)** routes each batch through 9 specialist agents in a fixed order.
Use the **sidebar** on the left to move through the flow one step at a time — Upload → Map →
Transform → Validate & Approve — with Back/Next buttons at the bottom of each step. The colored
strip below the title tracks exactly where the current batch is in the sequence (grey = not
reached, yellow = running or awaiting a decision, green = passed, red = blocked), and the
**Agent activity log** expander just under it shows what every agent did, on any step.

**A note on "agents" and API keys:** these agents are deterministic Python — they rename
columns, check tolerances, and write files. None of them call an LLM, which is why the app needs
no API key and runs fully offline. The agents still *learn and evolve* between runs via a local
**Learning Memory** (see the sidebar page): they remember custodian codes you approve, account-
type rules you teach, and column mappings you confirm. Optionally, if you set an
`ANTHROPIC_API_KEY`, a thin Claude layer switches on to *explain* findings and *suggest* mappings
— but it only ever advises; a human still approves every load and the deterministic checks always
run regardless.
        """
    )

    st.markdown("#### The 9 agents, in order")
    stage_table = pd.DataFrame(
        [
            ("1", "Extractor", "Reads the source file(s) — CSV, Excel, or JSON."),
            ("2", "Schema Mapper", "Renames source columns to destination fields per the active mapping."),
            ("3", "Sleeve Math", "Applies allocation rules (toggles, no-code layers, or custom Python) — sleeves entity only."),
            ("4", "Compliance Validator", "Normalizes codes (account type, model type, custodian, asset class), flags unknown codes and pending Core Model registrations."),
            ("5", "Data Quality", "Checks required fields, numeric/non-negative AUM, duplicate keys, and orphan foreign keys against already-loaded parents."),
            ("—", "HITL Gate", "Nothing loads until a human reviews the findings and clicks Approve or Reject."),
            ("6", "Loader", "Writes the approved batch to the destination and registers any newly-approved Core Models."),
            ("7", "Reconciler", "Compares source vs. destination record counts and (for AUM-bearing entities) total value drift."),
            ("8", "Cutover / Rollback", "On a clean reconciliation, promotes the batch. On a breach, quarantines the file and restores the prior Core Models registry."),
            ("9", "Audit & Evidence", "Writes a redacted, timestamped audit record of the whole run, decision included."),
        ],
        columns=["#", "Agent", "What it does"],
    )
    st.dataframe(stage_table, use_container_width=True, hide_index=True)

    st.markdown("#### Combining multiple source files")
    st.markdown(
        """
When you stage more than one file on the **Upload** step, you choose how they combine:

- **Stack rows** — the files share the same shape (same kind of record) and you want them
  unioned into one bigger batch, e.g. several advisor-cohort extracts of the same entity. Each
  row is tagged with a `source_file` column for lineage.
- **Join on a key** — the files hold *different* slices of the *same* records (e.g. account
  identity in one file, balances in another, fees in a third) and need to be merged side by side
  on a shared key column (like `AccountNumber`) before they're a complete row. Choose inner
  (only keys present in every file), left (keep everything from the first file), or outer (keep
  every key seen anywhere).
        """
    )

    st.markdown("#### Quickstart scenarios")
    st.markdown(
        """
The **Upload** step has a **Quickstart Scenarios** picker that loads pre-built local files with
one click — no manual upload needed. They cover the four migration shapes worth knowing, plus an
open-ended practice set:

1. **One-to-one mapping** — a single file, almost every column a plain rename, no business logic.
2. **Simple join** — two files, different shapes, merged on one key into a single entity.
3. **Multi-join + transformation** — three files merged on one key, then a computed column built
   on top via the Rule Builder (Transformation Logic tab).
4. **One file, multiple entities** — the *same* file mapped two different ways (Accounts vs.
   Households), showing both the clean case and the duplicate-key case that shows up when a
   flat file's grain doesn't match the entity you're deriving from it.
5. **Extra practice (not pre-verified)** — a bigger, randomized dataset that hasn't been
   individually catalogued — upload it and see what the Compliance Validator, Data Quality
   agent, and guardrails turn up on their own.

Full details and expected results for each: `SCENARIOS.md` and the `source/scenarios/*/README.md`
files in the project folder.
        """
    )

    with st.expander("Glossary"):
        st.markdown(
            """
- **HITL (Human-in-the-Loop)** — the mandatory pause before Load; nothing writes to the
  destination without an explicit Approve.
- **Guardrails** — deterministic checks the Loader obeys no matter what (e.g. sleeve allocations
  within tolerance of 100%, no negative AUM survives) — these block even with "force" set.
- **Sleeve** — a sub-allocation within an account (e.g. 60% US equity / 40% core fixed income);
  sleeve allocations for an account should sum to 100%.
- **Core Model** — an advisor-built ("Rep-as-PM") or firm-level model; advisor-built models must
  be registered before their sleeves/accounts are considered fully compliant.
- **Reconciliation** — comparing source vs. destination record counts (and AUM total, where
  applicable) after load, to catch silent data loss.
- **Cutover / Rollback** — cutover promotes a clean load; rollback quarantines the file and
  restores the prior Core Models registry snapshot on a reconciliation breach.
- **Orphan record** — a row whose foreign key (e.g. an account's household_id) doesn't match
  anything in the most recently loaded parent batch.
            """
        )

    with st.expander("Entity relationships"):
        st.markdown(
            """
```
Household
  └── Account            (needs Household)
        └── Sleeve        (needs Account + a registered Model)
        └── Transaction    (needs Account)
Model      (independent — registers Core Models used by Sleeves/Accounts)
Advisor    (independent — the advisor/rep directory)
```
            """
        )


# ---------------------------------------------------------------------------
# 1. Upload
# ---------------------------------------------------------------------------
def _peek_columns(uploaded_file) -> list:
    """Read just enough of an uploaded file to list its columns, for the Join
    combine mode's key-column picker."""
    suffix = os.path.splitext(uploaded_file.name)[1].lower()
    data = uploaded_file.getvalue()
    try:
        if suffix == ".csv":
            return list(pd.read_csv(io.BytesIO(data), nrows=5).columns)
        if suffix in (".xlsx", ".xls"):
            return list(pd.read_excel(io.BytesIO(data), nrows=5).columns)
        if suffix == ".json":
            obj = json.loads(data)
            if isinstance(obj, list) and obj:
                return list(obj[0].keys())
            if isinstance(obj, dict):
                return list(obj.keys())
        return []
    except Exception:
        return []


def _run_quickstart(scenario: dict):
    paths = [os.path.join(SOURCE_DIR, rel) for rel in scenario["files"]]
    default_map = pl.ENTITY_CONFIGS[scenario["entity_key"]]["field_map"]
    sleeve_cfg = st.session_state.setdefault("sleeve_math_config", dict(DEFAULT_SLEEVE_MATH_CONFIG))
    if scenario["combine_mode"] == "join":
        context = OrchestratorAgent.run_to_hitl_joined(
            paths, scenario["join_key"], default_map, scenario["entity_key"], REGISTRY_PATH, OUTPUT_DIR,
            sleeve_math_config=effective_sleeve_math_config(sleeve_cfg), how=scenario.get("join_how", "inner"),
            knowledge_store=get_store(),
        )
    else:
        context = OrchestratorAgent.run_to_hitl_multi(
            paths, default_map, scenario["entity_key"], REGISTRY_PATH, OUTPUT_DIR,
            sleeve_math_config=effective_sleeve_math_config(sleeve_cfg),
            knowledge_store=get_store(),
        )
    st.session_state["context"] = context
    st.session_state["entity_key"] = scenario["entity_key"]
    st.session_state["uploaded_names"] = None  # decouple from the uploader widget's own state
    st.session_state["mapping_editor_version"] = st.session_state.get("mapping_editor_version", 0) + 1
    st.session_state["review_written"] = False


if page == PAGE_UPLOAD:
    st.subheader("Upload source file")
    st.markdown(
        """
**Instructions**

1. Export the source extract as **CSV, Excel (.xlsx), or JSON**.
2. Pick the **entity type** below.
3. Upload one file, or several — if several, choose how they combine (see
   **Combine mode** below once 2+ files are staged). The **Orchestrator**
   immediately routes the result through the Extractor, Schema Mapper, Sleeve
   Math, Compliance Validator, and Data Quality agents — watch their status
   live in the stepper above and in the **Agent Pipeline** tab. No data loads
   until you approve it in **4. Validate & Approve**.
4. Need to change how a column maps to the destination? Go to
   **2. Schema Mapping**, edit, and Apply — the Orchestrator re-runs Schema
   Mapper onward automatically.

New here? See the **0. How to Use** tab for a full walkthrough and glossary.
        """
    )

    with st.expander("⚡ Quickstart scenarios — load a pre-built example with one click", expanded=False):
        st.caption(
            "These read directly from source/scenarios/ on disk — no upload needed. Great for seeing each "
            "migration pattern (one-to-one, simple join, multi-join + transform, one-to-many, open practice) "
            "before trying your own files."
        )
        scenario_by_label = {s["label"]: s for s in QUICKSTART_SCENARIOS}
        chosen_label = st.selectbox("Scenario", list(scenario_by_label.keys()), key="quickstart_choice")
        chosen = scenario_by_label[chosen_label]
        st.caption(
            f"Entity: **{pl.ENTITY_CONFIGS[chosen['entity_key']]['label']}** | "
            f"Files: {', '.join(chosen['files'])} | "
            f"Combine mode: **{chosen['combine_mode']}**"
            + (f" on `{chosen['join_key']}` ({chosen.get('join_how', 'inner')})" if chosen["combine_mode"] == "join" else "")
        )
        if st.button("Load this scenario", key="quickstart_load"):
            _run_quickstart(chosen)
            st.rerun()

    entity_key = st.selectbox(
        "Entity type",
        options=list(pl.ENTITY_CONFIGS.keys()),
        format_func=lambda k: pl.ENTITY_CONFIGS[k]["label"],
    )
    uploaded_files = st.file_uploader(
        "Source file(s) — upload one, or several to combine into a single destination table",
        type=["csv", "xlsx", "xls", "json"],
        accept_multiple_files=True,
        help="Select several files at once in the file browser (ctrl/cmd-click), or drag a group of files "
        "in together. You can also drop more files onto this box afterward — everything currently staged "
        "here gets combined into one destination table.",
    )

    combine_mode = "stack"
    join_key = None
    join_how = "inner"
    if uploaded_files and len(uploaded_files) >= 2:
        st.markdown("**Combine mode** (2+ files staged)")
        combine_mode = st.radio(
            "Combine mode",
            ["stack", "join"],
            format_func=lambda m: "Stack rows (same-shape files, union into more rows)" if m == "stack" else "Join on a key (different-shape files, merge into wider rows)",
            horizontal=True,
            label_visibility="collapsed",
            key="combine_mode_radio",
        )
        if combine_mode == "join":
            per_file_cols = [set(_peek_columns(f)) for f in uploaded_files]
            common_cols = sorted(set.intersection(*per_file_cols)) if all(per_file_cols) else []
            jc1, jc2 = st.columns(2)
            with jc1:
                if common_cols:
                    join_key = st.selectbox("Join key (column present in every staged file)", common_cols, key="join_key_select")
                else:
                    st.warning("No column name is shared across all staged files — type one manually.")
                    join_key = st.text_input("Join key", key="join_key_text")
            with jc2:
                join_how = st.selectbox(
                    "Join type", ["inner", "left", "outer"], key="join_how_select",
                    help="inner: only keys present in every file. left: keep all rows from the first file. outer: keep every key seen anywhere.",
                )
    elif uploaded_files:
        st.caption(
            f"{len(uploaded_files)} file(s) staged: {', '.join(f.name for f in uploaded_files)}. "
            "Drop more files onto the uploader to add them to this batch, or click the ✕ on a staged file "
            "above to remove it — the batch reprocesses automatically."
        )

    if uploaded_files:
        current_names = sorted(f.name for f in uploaded_files)
        settings_key = (current_names, entity_key, combine_mode, join_key, join_how)
        if st.session_state.get("upload_settings_key") != settings_key:
            tmp_paths = []
            for f in uploaded_files:
                suffix = os.path.splitext(f.name)[1]
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(f.getvalue())
                    tmp_paths.append(tmp.name)

            default_map = pl.ENTITY_CONFIGS[entity_key]["field_map"]
            sleeve_cfg = st.session_state.setdefault("sleeve_math_config", dict(DEFAULT_SLEEVE_MATH_CONFIG))

            if combine_mode == "join" and join_key:
                context = OrchestratorAgent.run_to_hitl_joined(
                    tmp_paths, join_key, default_map, entity_key, REGISTRY_PATH, OUTPUT_DIR,
                    sleeve_math_config=effective_sleeve_math_config(sleeve_cfg), how=join_how,
                    knowledge_store=get_store(),
                )
            else:
                context = OrchestratorAgent.run_to_hitl_multi(
                    tmp_paths, default_map, entity_key, REGISTRY_PATH, OUTPUT_DIR,
                    sleeve_math_config=effective_sleeve_math_config(sleeve_cfg),
                    knowledge_store=get_store(),
                )

            st.session_state["context"] = context
            st.session_state["uploaded_names"] = current_names
            st.session_state["upload_settings_key"] = settings_key
            st.session_state["entity_key"] = entity_key
            st.session_state["mapping_editor_version"] = st.session_state.get("mapping_editor_version", 0) + 1
            st.session_state["review_written"] = False

    if "context" in st.session_state:
        context = st.session_state["context"]
        entity_label = pl.ENTITY_CONFIGS[context.entity_key]["label"]
        st.success(f"Loaded {len(context.source_files)} file(s) as **{entity_label}**")
        st.write(f"{len(context.source_df)} combined rows, {len(context.source_df.columns)} columns")
        if context.extract_notes.get("join_key"):
            jn = context.extract_notes
            st.caption(
                f"Joined on **{jn['join_key']}** ({jn['how']}) — result: {jn['result_rows']} row(s)."
                + (f" Unmatched key counts: {jn['unmatched_by_file']}." if jn.get("unmatched_by_file") else "")
            )
        if context.extract_notes.get("file_row_counts"):
            st.dataframe(
                pd.DataFrame(list(context.extract_notes["file_row_counts"].items()), columns=["Source File", "Rows"]),
                use_container_width=True,
                hide_index=True,
            )
        st.dataframe(context.source_df.head(10), use_container_width=True)
        st.caption(
            "Rows from a Stack combine are tagged with a 'source_file' column for lineage — include it in "
            "the **Map** step if you want it in the destination table. Expand **Agent activity log** at the top for the run log."
        )
    else:
        st.info("Upload one or more source files above, or load a Quickstart scenario, to start a migration run.")

# ---------------------------------------------------------------------------
# Transformation Logic — configure the Sleeve Math Agent
# ---------------------------------------------------------------------------
if page == PAGE_TRANSFORM:
    st.subheader("Sleeve Math — Transformation Logic")
    st.caption(
        "Configures the Sleeve Math Agent (step 3), which runs before the Compliance Validator and Data "
        "Quality agents. Changes apply the next time you upload a file, or immediately to the current "
        "batch via 'Apply to Current Batch' below."
    )

    cfg = st.session_state.setdefault("sleeve_math_config", dict(DEFAULT_SLEEVE_MATH_CONFIG))

    def _batch_columns():
        ctx = st.session_state.get("context")
        if ctx is None:
            return []
        df = ctx.mapped_df if ctx.mapped_df is not None else ctx.source_df
        return list(df.columns) if df is not None else []

    def _columns_before_layer(base_cols, layers_list, idx):
        cols = list(base_cols)
        for j in range(idx):
            if layers_list[j].get("type") == "compute":
                nc = layers_list[j].get("new_column")
                if nc and nc not in cols:
                    cols.append(nc)
        return cols

    def _column_picker(label, current, options, key, help=None):
        custom_label = "(custom / not in this batch — type below)"
        opts = list(options) + [custom_label]
        idx = opts.index(current) if current in options else len(opts) - 1
        choice = st.selectbox(label, opts, index=idx, key=key, help=help)
        if choice == custom_label:
            return st.text_input(
                f"{label} — custom name", value=(current if current not in options else ""),
                key=key + "_custom",
            )
        return choice

    base_cols = _batch_columns()

    mode_options = ["toggles", "layers", "custom"]
    mode_labels = {"toggles": "Toggles (Rule 1 / Rule 2)", "layers": "Rule Builder (layers)", "custom": "Custom Python (advanced)"}
    cfg["mode"] = st.radio(
        "Transformation mode",
        mode_options,
        format_func=lambda m: mode_labels[m],
        index=mode_options.index(cfg.get("mode", "toggles")),
        horizontal=True,
    )

    if base_cols:
        with st.expander(f"Available columns in this batch ({len(base_cols)})", expanded=False):
            ctx = st.session_state["context"]
            df_ref = ctx.mapped_df if ctx.mapped_df is not None else ctx.source_df
            sample = df_ref.iloc[0] if len(df_ref) else None
            st.dataframe(
                pd.DataFrame({
                    "Column": base_cols,
                    "Type": [str(df_ref[c].dtype) for c in base_cols],
                    "Sample value": [("" if sample is None else str(sample[c])) for c in base_cols],
                }),
                use_container_width=True, hide_index=True,
            )
    else:
        st.info("Load a batch on the **Upload** step to see real column names here — fields below fall back to free text until then.")

    c1, c2 = st.columns(2)
    with c1:
        cfg["group_key"] = _column_picker(
            "Grouping key column (destination field)", cfg["group_key"], base_cols, key="sm_top_group_key",
            help="The mapped/destination column rows are grouped by — normally account_number. Used by "
            "Toggles/Rule Builder normalization and by the Compliance Validator + pre-load guardrail checks.",
        )
    with c2:
        cfg["tolerance"] = st.number_input(
            "Allocation sum tolerance (percentage points from 100%)",
            value=float(cfg["tolerance"]),
            step=0.1,
            format="%.2f",
            help="Used by the Compliance Validator's no-loss check (warns) and the pre-load guardrail (blocks), "
            "regardless of which transformation mode is active.",
        )

    st.markdown("---")

    if cfg["mode"] == "toggles":
        t1, t2 = st.columns(2)
        with t1:
            cfg["normalize"] = st.checkbox(
                "Rule 1 — Normalize allocations to sum to 100% per group", value=cfg["normalize"]
            )
            cfg["exclude_zero"] = st.checkbox(
                "Rule 2 — Exclude zero/near-zero allocation sleeves", value=cfg["exclude_zero"]
            )
        with t2:
            cfg["zero_threshold"] = st.number_input(
                "Zero-allocation threshold (%)",
                value=float(cfg["zero_threshold"]),
                step=0.01,
                format="%.2f",
                help="Allocations at or below this value count as inactive, when Rule 2 is on.",
            )

    elif cfg["mode"] == "layers":
        st.markdown("**Rule Builder — stacked transformation layers**")
        st.caption(
            "Layers run top to bottom against the mapped data for this batch. Use the arrow buttons to "
            "reorder, ✕ to delete. Field pickers below are populated from the columns available at that "
            "point in the stack (including any new columns created by earlier Compute layers)."
        )
        layers = cfg.setdefault("layers", [])

        for i, layer in enumerate(layers):
            cols_here = _columns_before_layer(base_cols, layers, i)
            with st.container(border=True):
                top = st.columns([4, 1, 1, 1])
                top[0].markdown(f"**Layer {i + 1}: {LAYER_LABELS.get(layer.get('type', 'filter'), '?')}**")
                if top[1].button("↑", key=f"sm_up_{i}", disabled=(i == 0), help="Move up"):
                    layers[i - 1], layers[i] = layers[i], layers[i - 1]
                    st.rerun()
                if top[2].button("↓", key=f"sm_down_{i}", disabled=(i == len(layers) - 1), help="Move down"):
                    layers[i + 1], layers[i] = layers[i], layers[i + 1]
                    st.rerun()
                if top[3].button("✕", key=f"sm_remove_{i}", help="Remove layer"):
                    layers.pop(i)
                    st.rerun()

                layer["type"] = st.selectbox(
                    "Layer type",
                    LAYER_TYPES,
                    format_func=lambda t: LAYER_LABELS[t],
                    index=LAYER_TYPES.index(layer.get("type", "filter")),
                    key=f"sm_type_{i}",
                    label_visibility="collapsed",
                )

                if layer["type"] == "filter":
                    fc = st.columns(4)
                    with fc[0]:
                        layer["field"] = _column_picker("Field", layer.get("field", ""), cols_here, key=f"sm_field_{i}")
                    layer["operator"] = fc[1].selectbox(
                        "Operator", FILTER_OPERATORS,
                        index=FILTER_OPERATORS.index(layer.get("operator", "==")), key=f"sm_op_{i}",
                    )
                    layer["value"] = fc[2].text_input(
                        "Value", value=str(layer.get("value", "")), key=f"sm_val_{i}",
                        disabled=layer["operator"] in ("is null", "is not null"),
                    )
                    layer["action"] = fc[3].selectbox(
                        "Action", ["keep", "exclude"],
                        index=["keep", "exclude"].index(layer.get("action", "keep")), key=f"sm_action_{i}",
                    )

                elif layer["type"] == "normalize":
                    nc = st.columns(3)
                    with nc[0]:
                        layer["group_key"] = _column_picker(
                            "Group by column", layer.get("group_key", cfg["group_key"]), cols_here, key=f"sm_gk_{i}"
                        )
                    with nc[1]:
                        layer["value_field"] = _column_picker(
                            "Value column", layer.get("value_field", "sleeve_allocation_pct"), cols_here, key=f"sm_vf_{i}"
                        )
                    layer["target_sum"] = nc[2].number_input(
                        "Target sum", value=float(layer.get("target_sum", 100.0)), key=f"sm_ts_{i}"
                    )

                elif layer["type"] == "fill_default":
                    dc = st.columns(2)
                    with dc[0]:
                        layer["field"] = _column_picker("Field", layer.get("field", ""), cols_here, key=f"sm_dfield_{i}")
                    layer["default_value"] = dc[1].text_input(
                        "Default value (used where null)", value=str(layer.get("default_value", "")), key=f"sm_dval_{i}"
                    )

                elif layer["type"] == "map_values":
                    layer["field"] = _column_picker("Field", layer.get("field", ""), cols_here, key=f"sm_mfield_{i}")
                    layer["mapping_text"] = st.text_area(
                        "Mapping — one 'OLD:NEW' pair per line", value=layer.get("mapping_text", ""),
                        key=f"sm_mtext_{i}", height=100,
                    )

                elif layer["type"] == "compute":
                    cc = st.columns(2)
                    layer["new_column"] = cc[0].text_input("New column name", value=layer.get("new_column", ""), key=f"sm_ncol_{i}")
                    layer["expression"] = cc[1].text_input(
                        "Expression (e.g. amount - fee)", value=layer.get("expression", ""), key=f"sm_expr_{i}",
                        help="Column arithmetic/comparisons only, evaluated via pandas — not a general code sandbox.",
                    )
                    if cols_here:
                        st.caption(f"Columns available here: {', '.join(cols_here)}")

        if st.button("+ Add Layer"):
            layers.append({"type": "filter", "operator": "==", "action": "keep"})
            st.rerun()
        cfg["layers"] = layers

    else:  # custom
        st.warning(
            "Runs as Python on your local machine only, with a restricted set of builtins (no imports, no "
            "file or network access) — this reduces accidental mistakes but is not a full security sandbox, "
            "so only paste logic you wrote or trust. Define a function named custom_sleeve_math(df) that "
            "takes the mapped pandas DataFrame for this batch and returns the transformed DataFrame."
        )
        if base_cols:
            st.caption(f"Columns available in df: {', '.join(base_cols)}")
        cfg["custom_code"] = st.text_area("Custom rule", value=cfg["custom_code"], height=240)

    st.caption("The Compliance Validator and pre-load guardrail check the result of any mode against the tolerance set above.")
    st.session_state["sleeve_math_config"] = cfg

    b1, b2 = st.columns(2)
    apply_clicked = b1.button("Apply to Current Batch", type="primary", disabled=("context" not in st.session_state))
    reset_clicked = b2.button("Reset to Defaults")

    if reset_clicked:
        st.session_state["sleeve_math_config"] = dict(DEFAULT_SLEEVE_MATH_CONFIG)
        st.success("Reset to the default Sleeve Math configuration.")
        st.rerun()

    if apply_clicked:
        context = st.session_state["context"]
        try:
            context = OrchestratorAgent.remap(
                context, context.field_map, REGISTRY_PATH, OUTPUT_DIR,
                sleeve_math_config=effective_sleeve_math_config(cfg),
            )
            st.session_state["context"] = context
            st.session_state["review_written"] = False
            st.success(
                "Transformation logic applied — re-ran Schema Mapper onward. "
                "Expand **Agent activity log** at the top for the run log; the result appears below."
            )
        except ValueError as exc:
            st.error(f"Transformation logic failed: {exc}")


# ---------------------------------------------------------------------------
# 2. Schema Mapping — dynamic destination mapping
# ---------------------------------------------------------------------------
if page == PAGE_MAP:
    st.subheader("Source → Destination Schema Mapping")

    if "context" not in st.session_state:
        st.info("Load a batch on the **Upload** step first (or try a Quickstart scenario there).")
    else:
        context = st.session_state["context"]
        active_entity = context.entity_key
        active_config = pl.ENTITY_CONFIGS[active_entity]
        active_map = context.field_map

        st.caption(
            "Edit the **Destination Field** for any source column, check **Include** to add a currently-unmapped "
            "column, or uncheck to exclude one — then click Apply Mapping (the Orchestrator re-runs Schema Mapper "
            "onward). Renaming a field that drives a business rule (account_type, model_type, asset_class, "
            "model_id, custodian_code, drift_tolerance_pct, sleeve_allocation_pct + account_number) disables that "
            "rule's automatic transformation in the Sleeve Math / Compliance Validator agents."
        )

        # -- Suggestions from learning memory (offline) + optional Claude ----
        store_obj = getattr(context, "knowledge_store", None) or get_store()
        unmapped_now = [c0 for c0 in context.source_df.columns if c0 not in context.field_map]
        remembered = store_obj.suggested_mapping(context.entity_key, unmapped_now) if unmapped_now else {}
        if remembered:
            st.info(
                "🧠 **Learning memory** recognizes these currently-unmapped columns from past approved runs: "
                + ", ".join(f"`{k}` → `{v}`" for k, v in remembered.items())
            )
            if st.button("Apply remembered mappings", key="apply_remembered"):
                merged = dict(context.field_map)
                merged.update(remembered)
                context = OrchestratorAgent.remap(context, merged, REGISTRY_PATH, OUTPUT_DIR)
                st.session_state["context"] = context
                st.session_state["mapping_editor_version"] = st.session_state.get("mapping_editor_version", 0) + 1
                st.session_state["review_written"] = False
                st.rerun()

        if unmapped_now and llm.is_enabled():
            if st.button("💡 Ask Claude to propose mappings for unmapped columns", key="claude_map"):
                with st.spinner("Asking Claude…"):
                    proposal = llm.propose_mapping(
                        context.entity_key, unmapped_now, list(pl.ENTITY_CONFIGS[context.entity_key]["field_map"].values())
                    )
                if proposal:
                    st.success("Claude proposed: " + ", ".join(f"`{k}` → `{v}`" for k, v in proposal.items()))
                    st.caption("Review these in the editor below before applying — Claude only suggests, you decide.")
                else:
                    st.caption("Claude didn't return a confident suggestion.")

        editor_source = pd.DataFrame(
            [
                {"Source Column": col, "Destination Field": active_map.get(col, ""), "Include": col in active_map}
                for col in context.source_df.columns
            ]
        )

        editor_key = f"mapping_editor_{st.session_state.get('mapping_editor_version', 0)}"
        edited = st.data_editor(
            editor_source,
            use_container_width=True,
            num_rows="fixed",
            hide_index=True,
            column_config={
                "Source Column": st.column_config.TextColumn(disabled=True),
                "Destination Field": st.column_config.TextColumn(help="Destination field name"),
                "Include": st.column_config.CheckboxColumn(help="Include this column in the mapping"),
            },
            key=editor_key,
        )

        b1, b2 = st.columns(2)
        apply_clicked = b1.button("Apply Mapping", type="primary")
        reset_clicked = b2.button("Reset to Default Mapping")

        if reset_clicked:
            context = OrchestratorAgent.remap(context, active_config["field_map"], REGISTRY_PATH, OUTPUT_DIR)
            st.session_state["context"] = context
            st.session_state["mapping_editor_version"] = st.session_state.get("mapping_editor_version", 0) + 1
            st.session_state["review_written"] = False
            st.success("Reset to the default schema mapping.")
            st.rerun()

        if apply_clicked:
            custom_map = {}
            dest_fields_seen = []
            for _, row in edited.iterrows():
                dest = str(row["Destination Field"]).strip()
                if bool(row["Include"]) and dest:
                    dest_fields_seen.append(dest)
                    custom_map[row["Source Column"]] = dest

            dupes = sorted({d for d in dest_fields_seen if dest_fields_seen.count(d) > 1})
            if dupes:
                st.error(f"Duplicate destination field name(s): {dupes}. Each destination field must be unique.")
            elif not custom_map:
                st.error("At least one column must be included with a destination field name.")
            else:
                context = OrchestratorAgent.remap(context, custom_map, REGISTRY_PATH, OUTPUT_DIR)
                st.session_state["context"] = context
                st.session_state["review_written"] = False
                st.success("Custom schema mapping applied and the pipeline re-ran. The result appears below and on the **Transform** step.")

        st.subheader("Active Mapping")
        st.dataframe(
            pd.DataFrame(list(context.field_map.items()), columns=["Source Column", "Destination Field"]),
            use_container_width=True,
        )
        if context.unmapped_cols:
            st.warning(f"Unmapped/excluded source columns (not loaded): {context.unmapped_cols}")

        # -- Teach an account-type rule (the agents evolve from this) --------
        if context.transformed_df is not None and "account_type" in context.transformed_df.columns:
            # find the *raw* codes that fell through to UNKNOWN by reading the mapped (pre-transform) df
            raw_unknown = []
            if context.mapped_df is not None and "account_type" in context.mapped_df.columns:
                mapped_codes = set(context.mapped_df["account_type"].astype(str))
                known = set(pl.ACCOUNT_TYPE_MAP) | set(store_obj.account_type_overrides())
                raw_unknown = sorted(code for code in mapped_codes if code and code.lower() != "nan" and code not in known)
            if raw_unknown:
                with st.container(border=True):
                    st.markdown("**🧠 Teach an account-type rule**")
                    st.caption(
                        f"These raw codes aren't in the crosswalk yet: {raw_unknown}. Teach the normalized destination "
                        "value and the Compliance Validator will apply it automatically on every future run."
                    )
                    tc1, tc2, tc3 = st.columns([1, 1, 1])
                    raw_pick = tc1.selectbox("Raw code", raw_unknown, key="teach_raw")
                    norm_val = tc2.text_input("Normalized value", key="teach_norm", placeholder="e.g. SEP_IRA")
                    tc3.markdown("&nbsp;")
                    if tc3.button("Teach & re-run", key="teach_btn"):
                        if norm_val.strip():
                            store_obj.teach_account_type(raw_pick, norm_val)
                            context = OrchestratorAgent.remap(context, context.field_map, REGISTRY_PATH, OUTPUT_DIR)
                            st.session_state["context"] = context
                            st.session_state["review_written"] = False
                            st.success(f"Learned {raw_pick} → {norm_val}. Re-ran the pipeline.")
                            st.rerun()
                        else:
                            st.error("Enter a normalized value first.")

        st.subheader("Shared Knowledge Layer — Reference Code Crosswalks")
        r1, r2, r3 = st.columns(3)
        with r1:
            st.markdown("**Account Type**")
            st.dataframe(pd.DataFrame(pl.ACCOUNT_TYPE_MAP.items(), columns=["the source system", "the destination"]), use_container_width=True)
        with r2:
            st.markdown("**Model Type**")
            st.dataframe(pd.DataFrame(pl.MODEL_TYPE_MAP.items(), columns=["the source system", "the destination"]), use_container_width=True)
        with r3:
            st.markdown("**Asset Class**")
            st.dataframe(pd.DataFrame(pl.ASSET_CLASS_MAP.items(), columns=["the source system", "the destination"]), use_container_width=True)
        st.markdown("**Known Custodian Codes**")
        st.write(sorted(pl.KNOWN_CUSTODIAN_CODES))

# ---------------------------------------------------------------------------
# 3. Transformation Review
# ---------------------------------------------------------------------------
if page == PAGE_TRANSFORM:
    st.subheader("Source → Destination Transformation Review")

    if "context" not in st.session_state or st.session_state["context"].transformed_df is None:
        st.info("Load a batch on the **Upload** step to see the transformation review.")
    else:
        context = st.session_state["context"]
        mapped_df = context.mapped_df
        transformed_df = context.transformed_df

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Source data (raw, as uploaded)**")
            st.dataframe(context.source_df.head(10), use_container_width=True)
        with c2:
            st.markdown("**Destination data (mapped + transformed by Sleeve Math + Compliance Validator)**")
            st.dataframe(transformed_df.head(10), use_container_width=True)

        if context.sleeve_notes.get("assumptions") or context.compliance_notes.get("assumptions"):
            st.markdown("**Transformation assumptions applied**")
            for a in context.sleeve_notes.get("assumptions", []) + context.compliance_notes.get("assumptions", []):
                st.info(a)

        if context.compliance_notes.get("pending_model_registration"):
            st.warning(
                f"Advisor-built (Rep-as-PM) models pending Core Model registration: "
                f"{context.compliance_notes['pending_model_registration']}"
            )

        sm_cfg = context.sleeve_math_config or {}
        sm_mode = sm_cfg.get("mode", "toggles")
        group_key_display = sm_cfg.get("group_key", "account_number")

        if sm_mode == "layers" and context.sleeve_notes.get("layers_applied"):
            st.markdown("**Sleeve Math Agent — Rule Builder layers applied**")
            for line in context.sleeve_notes["layers_applied"]:
                st.caption(f"• {line}")

        if "sleeve_allocation_pct" in mapped_df.columns and group_key_display in mapped_df.columns:
            st.markdown(f"**Allocation % per '{group_key_display}', before vs. after transformation**")
            if sm_mode == "custom":
                st.caption("Custom Sleeve Math rule applied (see 'Transformation Logic' tab).")
            elif sm_mode == "layers":
                st.caption("Rule Builder layers applied (see above / 'Transformation Logic' tab).")
            else:
                st.caption(
                    f"Rule 1 (normalize to 100%): {'ON' if sm_cfg.get('normalize', True) else 'OFF'}. "
                    f"Rule 2 (exclude <= {sm_cfg.get('zero_threshold', 0.0)}%): {'ON' if sm_cfg.get('exclude_zero', True) else 'OFF'}. "
                    "Adjust these in the 'Transformation Logic' tab."
                )
            sums_before = mapped_df.groupby(group_key_display)["sleeve_allocation_pct"].sum()
            sums_after = (
                transformed_df.groupby(group_key_display)["sleeve_allocation_pct"].sum()
                if "sleeve_allocation_pct" in transformed_df.columns and group_key_display in transformed_df.columns
                else None
            )
            sleeve_compare = pd.DataFrame(
                {
                    "sum_before_normalization": sums_before,
                    "sum_after_normalization": sums_after.reindex(sums_before.index) if sums_after is not None else None,
                }
            )
            st.dataframe(sleeve_compare, use_container_width=True)
            rows_dropped = len(mapped_df) - len(transformed_df)
            if rows_dropped > 0:
                st.caption(f"{rows_dropped} zero-allocation sleeve row(s) excluded from the destination data.")

        st.subheader("Final Destination Data")
        st.caption("The complete data set as it will be written to the destination if approved.")
        st.dataframe(transformed_df, use_container_width=True)
        st.write(f"{len(transformed_df)} rows, {len(transformed_df.columns)} columns")

# ---------------------------------------------------------------------------
# 4. Validate & Approve
# ---------------------------------------------------------------------------
if page == PAGE_VALIDATE:
    if "context" not in st.session_state or st.session_state["context"].transformed_df is None:
        st.info("Upload a source file in the '1. Upload' tab first.")
    else:
        context = st.session_state["context"]
        active_config = pl.ENTITY_CONFIGS[context.entity_key]

        st.subheader("4. Compliance Validator findings")
        if context.compliance_errors:
            st.dataframe(pd.DataFrame(context.compliance_errors), use_container_width=True)
        else:
            st.success("No compliance findings")

        st.subheader("5. Data Quality findings")
        if context.dq_errors:
            st.dataframe(pd.DataFrame(context.dq_errors), use_container_width=True)
        else:
            st.success("No data quality findings")

        if context.passed:
            st.success(f"Overall: PASSED ({len(context.errors)} finding(s), none blocking)")
        else:
            st.error(f"Overall: FAILED ({len(context.errors)} finding(s)) — fix ERROR-severity findings or force approve below")

        # -- Optional Claude explanation of the findings (key-gated) ---------
        if context.errors and llm.is_enabled():
            if st.button("💡 Explain these findings with Claude"):
                with st.spinner("Asking Claude…"):
                    explanation = llm.explain_findings(context.entity_key, context.compliance_errors, context.dq_errors)
                if explanation:
                    st.markdown(explanation)
                else:
                    st.caption("Claude couldn't produce an explanation right now.")
        elif context.errors:
            st.caption(
                "Tip: set an ANTHROPIC_API_KEY (see the sidebar) to get a plain-language explanation of these "
                "findings and suggested fixes. Everything else runs fully offline."
            )

        c1, c2, c3 = st.columns(3)
        with c1:
            entity_name = st.text_input("Output filename entity name", value=context.entity_key)
        with c2:
            out_format = st.selectbox("Output format", ["csv", "xlsx", "json"])
        with c3:
            force = st.checkbox("Force approve despite ERROR findings", value=False)

        st.subheader("Reconciliation Keys (Reconciler Agent)")
        from agents.reconciler import ReconcilerAgent

        st.caption(
            f"Tolerance: key-set match required, plus Total AUM drift <= {ReconcilerAgent.AUM_TOLERANCE_PCT}% "
            "if an AUM field is present."
        )
        source_cols = list(context.source_df.columns)
        dest_cols = list(context.transformed_df.columns)
        default_source_key = active_config["source_key"] if active_config["source_key"] in source_cols else source_cols[0]
        default_loaded_key = active_config["loaded_key"] if active_config["loaded_key"] in dest_cols else dest_cols[0]
        k1, k2 = st.columns(2)
        with k1:
            source_key = st.selectbox("Source key column", options=source_cols, index=source_cols.index(default_source_key))
        with k2:
            loaded_key = st.selectbox("Destination key field", options=dest_cols, index=dest_cols.index(default_loaded_key))

        st.subheader("Human-in-the-Loop Review")
        if not st.session_state.get("review_written"):
            from agents.audit_evidence import AuditEvidenceAgent

            AuditEvidenceAgent.write_review(context, AUDIT_DIR, datetime.now().strftime("%Y%m%d_%H%M%S"))
            st.session_state["review_written"] = True

        can_approve = context.passed or force
        b1, b2, _ = st.columns(3)
        approve_clicked = b1.button("Approve & Load", disabled=not can_approve, type="primary")
        reject_clicked = b2.button("Reject")
        if not can_approve:
            st.caption("Resolve ERROR-severity findings, or check 'Force approve' to override.")

        if approve_clicked:
            try:
                context = OrchestratorAgent.approve(
                    context, OUTPUT_DIR, AUDIT_DIR, CUTOVER_DIR, QUARANTINE_DIR, REGISTRY_PATH,
                    entity_name, out_format, source_key, loaded_key, force=force,
                )
                st.session_state["context"] = context
            except Exception as exc:
                st.error(f"Approval blocked: {exc}")

        if reject_clicked:
            context = OrchestratorAgent.reject(context, AUDIT_DIR)
            st.session_state["context"] = context

        if context.decision == "REJECTED":
            st.error("Migration rejected. No data was loaded. Audit entry recorded.")
            if context.audit_path:
                with open(context.audit_path, "rb") as f:
                    st.download_button("Download audit log", f, file_name=os.path.basename(context.audit_path))

        elif context.decision == "ROLLED_BACK":
            st.error("Reconciliation breach — Rollback Path triggered. Output quarantined, Core Models registry restored.")
            st.json(context.reconciliation)
            if context.aum_reconciliation:
                st.json(context.aum_reconciliation)
            if context.incident_path:
                with open(context.incident_path, "rb") as f:
                    st.download_button("Download incident report", f, file_name=os.path.basename(context.incident_path))
            if context.audit_path:
                with open(context.audit_path, "rb") as f:
                    st.download_button("Download audit log", f, file_name=os.path.basename(context.audit_path))

        elif context.decision == "APPROVED":
            st.subheader("6. Loader Agent")
            st.success(f"Loaded {len(context.transformed_df)} row(s) → {os.path.basename(context.output_path)}")

            st.subheader("7. Reconciler Agent")
            if context.reconciliation["overall_status"] == "PASS":
                st.success("Reconciliation PASSED")
            else:
                st.error(f"Reconciliation FAILED — {context.reconciliation}")
            if context.aum_reconciliation:
                st.json(context.aum_reconciliation)

            st.subheader("8. Cutover Agent")
            st.success(f"Promoted {len(context.cutover_record['promoted_keys'])} record(s) to PRODUCTION")

            learned = getattr(context, "learned_this_run", None) or {}
            if learned.get("custodians") or learned.get("mappings"):
                bits = []
                if learned.get("custodians"):
                    bits.append(f"{len(learned['custodians'])} new custodian code(s): {learned['custodians']}")
                if learned.get("mappings"):
                    bits.append(f"{len(learned['mappings'])} mapping rule(s)")
                st.info("🧠 Learning memory evolved from this approval — " + "; ".join(bits) + ". See the **Learning Memory** page.")

            st.subheader("9. Audit & Evidence Agent")
            with open(context.audit_path) as f:
                st.json(json.load(f))

            d1, d2 = st.columns(2)
            with open(context.output_path, "rb") as f:
                d1.download_button("Download migrated file", f, file_name=os.path.basename(context.output_path))
            with open(context.audit_path, "rb") as f:
                d2.download_button("Download audit log", f, file_name=os.path.basename(context.audit_path))

# ---------------------------------------------------------------------------
# Audit History
# ---------------------------------------------------------------------------
if page == PAGE_HISTORY:
    st.subheader("Past Runs")
    audit_files = sorted(glob.glob(os.path.join(AUDIT_DIR, "audit_*.json")), reverse=True)

    if not audit_files:
        st.write("No audit runs recorded yet.")
    else:
        rows = []
        for path in audit_files:
            with open(path) as f:
                data = json.load(f)
            rows.append(
                {
                    "run_timestamp": data.get("run_timestamp"),
                    "entity": data.get("entity"),
                    "source_file": data.get("source_file"),
                    "validation_result": data.get("validation_result"),
                    "hitl_decision": data.get("hitl_decision"),
                    "reconciliation": (data.get("reconciliation") or {}).get("overall_status"),
                    "destination_file": data.get("destination_file"),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

        for path, data in zip(audit_files, [json.load(open(p)) for p in audit_files]):
            label = f"{data.get('run_timestamp')} — {data.get('entity')} — {data.get('validation_result')} — HITL: {data.get('hitl_decision')}"
            with st.expander(label):
                st.json(data)

    st.subheader("Cutover Records (production promotions)")
    cutover_files = sorted(glob.glob(os.path.join(CUTOVER_DIR, "cutover_*.json")), reverse=True)
    if cutover_files:
        for path in cutover_files:
            with open(path) as f:
                data = json.load(f)
            with st.expander(f"{data.get('run_timestamp')} — {data.get('entity')} — {len(data.get('promoted_keys', []))} record(s)"):
                st.json(data)
    else:
        st.write("No cutover records yet.")

    st.subheader("Quarantine / Rollback Incidents")
    incident_files = sorted(glob.glob(os.path.join(QUARANTINE_DIR, "incident_*.json")), reverse=True)
    if incident_files:
        for path in incident_files:
            with open(path) as f:
                data = json.load(f)
            with st.expander(f"{data.get('run_timestamp')} — {data.get('entity')} — {data.get('reason', '')[:60]}"):
                st.json(data)
    else:
        st.write("No rollback incidents yet.")

    st.subheader("Registered Core Models (Rep-as-PM advisor-built)")
    st.json(sorted(pl.load_registered_models(REGISTRY_PATH)))

    st.subheader("Output Files")
    output_files = sorted(glob.glob(os.path.join(OUTPUT_DIR, "*")), reverse=True)
    if output_files:
        for path in output_files:
            with open(path, "rb") as f:
                st.download_button(os.path.basename(path), f, file_name=os.path.basename(path), key=path)
    else:
        st.write("No output files yet.")


# ---------------------------------------------------------------------------
# Learning Memory — inspect and manage what the agents have learned
# ---------------------------------------------------------------------------
if page == PAGE_MEMORY:
    st.subheader("🧠 Learning Memory")
    st.caption(
        "The agents evolve between runs by remembering what you approve and teach — entirely on "
        "your machine, no LLM or API key required. This is that memory, stored in "
        "knowledge_store.json. Optionally, real Claude reasoning can layer on top when an API key "
        "is present (status shown in the sidebar)."
    )

    stats = store.stats()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Runs learned from", stats["runs_learned"])
    m2.metric("Custodian codes", stats["custodian_codes"])
    m3.metric("Taught account types", stats["account_types"])
    m4.metric("Mapping rules", stats["mapping_columns"])

    st.markdown("#### Accepted custodian codes")
    st.caption("Auto-learned from approved batches. Once here, the Compliance Validator stops flagging them as UNKNOWN_CUSTODIAN_CODE.")
    if store.data["custodian_codes"]:
        st.dataframe(
            pd.DataFrame(
                [{"Code": k, "Times seen": v["count"], "Last seen": v.get("last_seen", "")} for k, v in store.data["custodian_codes"].items()]
            ),
            use_container_width=True, hide_index=True,
        )
    else:
        st.caption("Nothing yet — approve a batch containing a new custodian code to teach one.")

    st.markdown("#### Taught account-type rules")
    st.caption("Explicitly taught on the Map step (raw source code → normalized destination value). Merged over the built-in crosswalk on the next run.")
    if store.data["account_types"]:
        st.dataframe(
            pd.DataFrame(
                [{"Raw code": k, "Normalized": v["normalized"], "Taught at": v.get("taught_at", "")} for k, v in store.data["account_types"].items()]
            ),
            use_container_width=True, hide_index=True,
        )
    else:
        st.caption("Nothing yet — teach one on the Map step when an UNKNOWN_ACCOUNT_TYPE shows up.")

    st.markdown("#### Confirmed column → field mappings")
    st.caption("Auto-learned from approved runs. The Map step offers these as suggestions when the same source column reappears.")
    map_rows = []
    for ent, cols in store.data["mappings"].items():
        for col, rec in cols.items():
            map_rows.append({"Entity": ent, "Source column": col, "→ Destination field": rec["dest"], "Times confirmed": rec["count"]})
    if map_rows:
        st.dataframe(pd.DataFrame(map_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("Nothing yet — apply a custom mapping and approve the batch to record it.")

    st.divider()
    if st.button("🗑 Reset all learned memory", type="secondary"):
        store.reset()
        st.success("Learning memory cleared.")
        st.rerun()


# ---------------------------------------------------------------------------
# Guided step navigation — Back / Next across the 4 pipeline steps
# ---------------------------------------------------------------------------
if page in STEP_PAGES:
    idx = STEP_PAGES.index(page)
    st.divider()
    nav_prev, nav_spacer, nav_next = st.columns([1, 3, 1])
    if idx > 0:
        if nav_prev.button(f"← {STEP_PAGES[idx - 1].split(' · ')[1]}", use_container_width=True):
            goto(STEP_PAGES[idx - 1])
    if idx < len(STEP_PAGES) - 1:
        if nav_next.button(f"{STEP_PAGES[idx + 1].split(' · ')[1]} →", use_container_width=True, type="primary"):
            goto(STEP_PAGES[idx + 1])
