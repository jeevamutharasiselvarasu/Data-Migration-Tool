"""
Agentic migration pipeline: source accounts.csv -> destination schema.

Stages: Extract -> Map -> Transform -> Validate -> HITL -> Load -> Reconcile -> Audit.
Business rules follow references/field_glossary.md, references/rep_as_pm_logic.md and
references/entity_hierarchy.md from the agentic-migration-prototype skill.

This module holds the low-level primitives and the "shared knowledge layer"
(field maps, code crosswalks, entity registry). The agents/ package wraps
these primitives into named agents (Extractor, Schema Mapper, Sleeve Math,
Compliance Validator, Data Quality, Loader, Reconciler, Cutover, Audit &
Evidence) coordinated by agents/orchestrator.py. transform()/validate() are
kept as composed wrappers for backward compatibility with the CLI below.
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime

import pandas as pd

# ---------------------------------------------------------------------------
# Step 2 — Schema mapping (source column -> destination field), per field_glossary.md
# ---------------------------------------------------------------------------

FIELD_MAP = {
    "HouseholdID": "household_id",
    "HouseholdName": "household_name",
    "ClientID": "client_id",
    "ClientFirstName": "first_name",
    "ClientLastName": "last_name",
    "AccountNumber": "account_number",
    "AccountType": "account_type",
    "ModelCode": "model_id",
    "ModelName": "model_name",
    "ModelType": "model_type",
    "AUM": "assets_under_management",
    "Custodian": "custodian_code",
    "RepCode": "advisor_rep_code",
    "AdvisorName": "advisor_name",
    "InceptionDate": "account_inception_date",
    "DriftThreshold": "drift_tolerance_pct",
    "WaveNumber": "wave_number",
    "AnnualFeeAmount": "annual_fee_amount",
}

ACCOUNT_TYPE_MAP = {
    "IRA": "IND_RETIREMENT",
    "ROTH": "ROTH_IRA",
    "BROK": "BROKERAGE",
    "401K": "RETIREMENT_PLAN",
    "TRUST": "TRUST_ACCOUNT",
    "JTWROS": "JOINT_ACCOUNT",
    "CUST": "CUSTODIAL",
    "CORP": "CORPORATE",
}

MODEL_TYPE_MAP = {
    "REP": "ADVISOR_BUILT",
    "FIRM": "FIRM_LEVEL",
    "3P": "THIRD_PARTY",
}

ASSET_CLASS_MAP = {
    "EQ_US": "US_EQUITY",
    "EQ_INT": "INTL_EQUITY",
    "FI_CORE": "CORE_FIXED_INCOME",
    "FI_HY": "HIGH_YIELD",
    "CASH": "CASH_EQUIV",
    "ALT": "ALTERNATIVES",
    "MULTI": "MULTI_ASSET",
}

KNOWN_CUSTODIAN_CODES = {"SCHW", "FIDO", "PERSH", "TDA"}

REQUIRED_FIELDS = ["household_id", "client_id", "account_number", "account_type", "model_id"]

DEFAULT_DRIFT_TOLERANCE = 5.0

# ---------------------------------------------------------------------------
# Sleeve schema mapping (source sleeves.csv -> destination account sleeves)
# Per references/rep_as_pm_logic.md — sleeve allocations must sum to 100% per
# account and zero-allocation sleeves are inactive (excluded in sleeve_math()).
# ---------------------------------------------------------------------------

SLEEVE_FIELD_MAP = {
    "AccountNumber": "account_number",
    "HouseholdID": "household_id",
    "ClientID": "client_id",
    "ModelCode": "model_id",
    "ModelType": "model_type",
    "SleeveID": "sleeve_id",
    "AssetClass": "asset_class",
    "SleeveAlloc%": "sleeve_allocation_pct",
    "DriftThreshold": "drift_tolerance_pct",
    "RepCode": "advisor_rep_code",
}

# ---------------------------------------------------------------------------
# Household schema mapping (source households.csv -> destination households)
# ---------------------------------------------------------------------------

HOUSEHOLD_FIELD_MAP = {
    "HouseholdID": "household_id",
    "HouseholdName": "household_name",
    "PrimaryClientID": "primary_client_id",
    "PrimaryClientName": "primary_client_name",
    "NumClients": "num_clients",
    "TotalAUM": "total_aum",
    "RepCode": "advisor_rep_code",
    "Custodian": "custodian_code",
    "Status": "status",
    "OnboardedDate": "onboarded_date",
}

# ---------------------------------------------------------------------------
# Model schema mapping (source models.csv -> destination Core Models)
# ---------------------------------------------------------------------------

MODEL_FIELD_MAP = {
    "ModelCode": "model_id",
    "ModelName": "model_name",
    "ModelType": "model_type",
    "TargetAlloc%": "target_allocation_pct",
    "AssetClass": "asset_class",
    "MinAUM": "min_aum",
    "DriftThreshold": "drift_tolerance_pct",
    "CreatedDate": "created_date",
    "RepCode": "advisor_rep_code",
    "Status": "status",
}

# ---------------------------------------------------------------------------
# Transaction schema mapping (source transactions.csv -> destination trade blotter)
# ---------------------------------------------------------------------------

TRANSACTION_FIELD_MAP = {
    "TransactionID": "transaction_id",
    "AccountNumber": "account_number",
    "HouseholdID": "household_id",
    "TransactionDate": "transaction_date",
    "TransactionType": "transaction_type",
    "Ticker": "ticker",
    "Quantity": "quantity",
    "Price": "price",
    "Amount": "amount",
    "Custodian": "custodian_code",
    "RepCode": "advisor_rep_code",
    "SettlementDate": "settlement_date",
}

# ---------------------------------------------------------------------------
# Advisor schema mapping (source advisors.csv -> destination advisor
# directory). This is a near 1:1 mapping -- almost every column is a plain
# rename with no business-logic transformation, unlike accounts/sleeves.
# Used as the "Scenario 1: one-to-one mapping" reference example.
# ---------------------------------------------------------------------------

ADVISOR_FIELD_MAP = {
    "RepCode": "advisor_rep_code",
    "RepName": "advisor_name",
    "Email": "email",
    "Phone": "phone",
    "Region": "region",
    "Title": "title",
    "HireDate": "hire_date",
    "Status": "status",
}

# ---------------------------------------------------------------------------
# Entity registry — each source entity has its own schema map, the source
# / destination key columns used for reconciliation, and its own required
# fields for validation. Per entity_hierarchy.md, load order is:
# Households -> Accounts -> Account Sleeves (needs Core Models) -> Trade
# Blotter (Transactions); Models must be registered before sleeves load.
# ---------------------------------------------------------------------------

ENTITY_CONFIGS = {
    "households": {
        "label": "Households (households.csv)",
        "field_map": HOUSEHOLD_FIELD_MAP,
        "source_key": "HouseholdID",
        "loaded_key": "household_id",
        "required_fields": ["household_id", "primary_client_id"],
    },
    "accounts": {
        "label": "Accounts (accounts.csv)",
        "field_map": FIELD_MAP,
        "source_key": "AccountNumber",
        "loaded_key": "account_number",
        "required_fields": ["household_id", "client_id", "account_number", "account_type", "model_id"],
    },
    "sleeves": {
        "label": "Account Sleeves (sleeves.csv) — enables sleeve math",
        "field_map": SLEEVE_FIELD_MAP,
        "source_key": "SleeveID",
        "loaded_key": "sleeve_id",
        "required_fields": ["account_number", "sleeve_id", "model_id"],
    },
    "models": {
        "label": "Models (models.csv) — Core Models registry",
        "field_map": MODEL_FIELD_MAP,
        "source_key": "ModelCode",
        "loaded_key": "model_id",
        "required_fields": ["model_id", "model_name", "model_type"],
    },
    "transactions": {
        "label": "Transactions (transactions.csv) — trade blotter",
        "field_map": TRANSACTION_FIELD_MAP,
        "source_key": "TransactionID",
        "loaded_key": "transaction_id",
        "required_fields": ["transaction_id", "account_number"],
    },
    "advisors": {
        "label": "Advisors (advisors.csv) — advisor directory, one-to-one mapping example",
        "field_map": ADVISOR_FIELD_MAP,
        "source_key": "RepCode",
        "loaded_key": "advisor_rep_code",
        "required_fields": ["advisor_rep_code", "advisor_name"],
    },
}

# Per entity_hierarchy.md dependency order — (foreign_key_field, parent_entity)
# pairs checked by the Data Quality agent's orphan detection.
ENTITY_PARENTS = {
    "households": [],
    "accounts": [("household_id", "households")],
    "sleeves": [("account_number", "accounts"), ("model_id", "models")],
    "models": [],
    "transactions": [("account_number", "accounts")],
    "advisors": [],
}


def log(msg: str) -> None:
    print(msg)


# ---------------------------------------------------------------------------
# Step 1 — Extract
# ---------------------------------------------------------------------------

def extract(source_path: str) -> pd.DataFrame:
    ext = os.path.splitext(source_path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(source_path)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(source_path)
    elif ext == ".json":
        with open(source_path) as f:
            data = json.load(f)
        df = pd.DataFrame(data if isinstance(data, list) else [data])
    else:
        raise ValueError(f"Unsupported source format: {ext}")

    log(f"[EXTRACT] Loaded {len(df)} rows, {len(df.columns)} columns from {source_path}")
    log(f"[EXTRACT] Columns: {list(df.columns)}")
    return df


def extract_many(source_paths: list[str]) -> tuple[pd.DataFrame, dict]:
    """Extract several source files for the same entity and combine them into
    one data set, cumulatively, for a single destination table. Each row is
    tagged with a 'source_file' column (its origin filename) so it flows
    through the normal dynamic schema-mapping UI like any other column —
    include it in the mapping if you want lineage in the destination table,
    leave it unmapped to drop it."""
    frames = []
    file_row_counts = {}
    for path in source_paths:
        df = extract(path)
        name = os.path.basename(path)
        df = df.copy()
        df["source_file"] = name
        file_row_counts[name] = len(df)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True, sort=False)
    log(f"[EXTRACT] Combined {len(source_paths)} file(s) into {len(combined)} total row(s): {file_row_counts}")
    return combined, {"file_count": len(source_paths), "file_row_counts": file_row_counts}


def extract_and_join(source_paths: list[str], join_key: str, how: str = "inner") -> tuple[pd.DataFrame, dict]:
    """Extract several source files that each hold a different slice of the
    same records (e.g. account identity in one file, balances in another)
    and JOIN them side by side on a shared key column, instead of stacking
    rows like extract_many(). Files are merged left-to-right in the order
    given. Column name collisions on non-key columns get a _<filename>
    suffix so nothing is silently overwritten.

    how: "inner" (only keys present in every file), "left" (keep all rows
    from the first file, filling unmatched columns with null), or "outer"
    (keep every key seen in any file).
    """
    if len(source_paths) < 2:
        raise ValueError("extract_and_join needs at least 2 source files")

    frames = []
    file_row_counts = {}
    for path in source_paths:
        df = extract(path)
        name = os.path.basename(path)
        if join_key not in df.columns:
            raise ValueError(f"Join key '{join_key}' not found in {name} (columns: {list(df.columns)})")
        file_row_counts[name] = len(df)
        frames.append((name, df))

    combined = frames[0][1]
    join_notes = {"file_row_counts": file_row_counts, "unmatched_by_file": {}}

    for name, df in frames[1:]:
        before_keys = set(combined[join_key].astype(str))
        incoming_keys = set(df[join_key].astype(str))
        if how == "inner":
            unmatched = before_keys.symmetric_difference(incoming_keys)
        elif how == "left":
            unmatched = incoming_keys - before_keys
        else:  # outer
            unmatched = set()
        if unmatched:
            join_notes["unmatched_by_file"][name] = len(unmatched)

        left_suffix = ""
        right_suffix = f"_{os.path.splitext(name)[0]}"
        combined = combined.merge(df, on=join_key, how=how, suffixes=(left_suffix, right_suffix))

    log(
        f"[EXTRACT] Joined {len(source_paths)} file(s) on '{join_key}' ({how}) into "
        f"{len(combined)} row(s): {file_row_counts}; unmatched key counts: {join_notes['unmatched_by_file']}"
    )
    join_notes["file_count"] = len(source_paths)
    join_notes["join_key"] = join_key
    join_notes["how"] = how
    join_notes["result_rows"] = len(combined)
    return combined, join_notes


# ---------------------------------------------------------------------------
# Step 2 — Map
# ---------------------------------------------------------------------------

def apply_schema_map(df: pd.DataFrame, field_map: dict) -> tuple[pd.DataFrame, list[str]]:
    unmapped = [c for c in df.columns if c not in field_map]
    if unmapped:
        log(f"[MAP] WARNING — unmapped source columns (dropped): {unmapped}")

    mapped = df.rename(columns={k: v for k, v in field_map.items() if k in df.columns})
    mapped_cols = [c for c in field_map.values() if c in mapped.columns]
    return mapped[mapped_cols], unmapped


# ---------------------------------------------------------------------------
# Step 3a — Transform: Sleeve Math (Sleeve Math Agent)
# ---------------------------------------------------------------------------

# Restricted builtins available to a user-supplied custom Sleeve Math rule.
# No imports, no file/network access, no exec/eval — enough to write simple
# pandas transformations, not enough to reach outside the DataFrame.
_SAFE_BUILTINS = {
    "len": len, "range": range, "abs": abs, "min": min, "max": max, "sum": sum,
    "sorted": sorted, "round": round, "float": float, "int": int, "str": str,
    "bool": bool, "list": list, "dict": dict, "set": set, "enumerate": enumerate,
    "zip": zip, "print": print, "True": True, "False": False, "None": None,
}


def sleeve_math(
    df: pd.DataFrame,
    normalize: bool = True,
    exclude_zero: bool = True,
    zero_threshold: float = 0.0,
    group_key: str = "account_number",
    custom_code: str = None,
) -> tuple[pd.DataFrame, dict]:
    """Rule 1: normalize sleeve allocations to sum to 100% per group_key.
    Rule 2: allocations at/below zero_threshold are inactive — exclude from load.
    Both rules, the grouping key, and the threshold are configurable from the
    front end's Transformation Logic tab. If custom_code is supplied it fully
    replaces the built-in rules: it must define a function named
    custom_sleeve_math(df) -> df."""
    t = df.copy()
    notes = {"assumptions": [], "custom_rule_used": False}

    if custom_code:
        namespace = {"pd": pd}
        try:
            exec(custom_code, {"__builtins__": _SAFE_BUILTINS}, namespace)
        except Exception as exc:
            raise ValueError(f"error while defining custom Sleeve Math rule: {exc}") from exc
        func = namespace.get("custom_sleeve_math")
        if not callable(func):
            raise ValueError("custom rule must define a function named custom_sleeve_math(df)")
        try:
            result = func(t.copy())
        except Exception as exc:
            raise ValueError(f"error while running custom Sleeve Math rule: {exc}") from exc
        if not isinstance(result, pd.DataFrame):
            raise ValueError("custom_sleeve_math(df) must return a pandas DataFrame")
        notes["custom_rule_used"] = True
        notes["assumptions"].append("Custom Sleeve Math rule applied (overrides built-in normalize/exclude-zero logic)")
        return result, notes

    if "sleeve_allocation_pct" in t.columns and group_key in t.columns:
        if normalize:
            t["sleeve_allocation_pct"] = (
                t.groupby(group_key)["sleeve_allocation_pct"].transform(lambda x: x / x.sum() * 100)
            )
        else:
            notes["assumptions"].append("Normalization to 100% was disabled for this run")

        if exclude_zero:
            dropped = int((t["sleeve_allocation_pct"] <= zero_threshold).sum())
            t = t[t["sleeve_allocation_pct"] > zero_threshold]
            if dropped:
                notes["assumptions"].append(f"{dropped} sleeve row(s) at/below the {zero_threshold} threshold excluded from load")
        else:
            notes["assumptions"].append("Zero-allocation exclusion was disabled for this run")

    return t, notes


# ---------------------------------------------------------------------------
# Step 3b — Transform: Compliance overlay (Compliance Validator Agent)
# ---------------------------------------------------------------------------

def compliance_transform(df: pd.DataFrame, registered_models: set, extra_account_type_map: dict = None) -> tuple[pd.DataFrame, dict]:
    t = df.copy()
    notes = {"assumptions": [], "pending_model_registration": [], "learned_account_types_applied": []}

    # Account type normalization. The built-in crosswalk can be extended by
    # entries the user has *taught* the learning store (raw code -> normalized
    # value), so a code that was UNKNOWN on a prior run resolves this time.
    if "account_type" in t.columns:
        effective_map = dict(ACCOUNT_TYPE_MAP)
        if extra_account_type_map:
            applicable = {k: v for k, v in extra_account_type_map.items() if k in set(t["account_type"].astype(str))}
            effective_map.update(extra_account_type_map)
            notes["learned_account_types_applied"] = sorted(applicable.keys())
        t["account_type"] = t["account_type"].map(effective_map).fillna("UNKNOWN")

    # Model type normalization (Rep-as-PM vs firm-level vs third-party)
    if "model_type" in t.columns:
        t["model_type"] = t["model_type"].map(MODEL_TYPE_MAP).fillna("UNKNOWN")

    # Model id normalization
    if "model_id" in t.columns:
        t["model_id"] = t["model_id"].astype(str).str.upper().str.strip()

    # Asset class normalization (sleeves) — per field_glossary.md Asset Class Codes
    if "asset_class" in t.columns:
        t["asset_class"] = t["asset_class"].map(ASSET_CLASS_MAP).fillna(t["asset_class"])

    # Rep-as-PM rule 3: advisor-built models must be registered as Core Models
    # before their sleeves/accounts can load. Track which ones are new this run.
    if "model_id" in t.columns and "model_type" in t.columns:
        advisor_built = t.loc[t["model_type"] == "ADVISOR_BUILT", "model_id"].unique().tolist()
        notes["pending_model_registration"] = sorted(m for m in advisor_built if m not in registered_models)

    # Custodian code — standardize to uppercase
    if "custodian_code" in t.columns:
        t["custodian_code"] = t["custodian_code"].astype(str).str.upper().str.strip()

    # AUM — ensure numeric so Data Quality can evaluate numeric/non-negative rules
    if "assets_under_management" in t.columns:
        t["assets_under_management"] = pd.to_numeric(t["assets_under_management"], errors="coerce")

    # Drift tolerance — default to 5.0 if missing, per rep_as_pm_logic.md rule 4
    if "drift_tolerance_pct" in t.columns:
        missing_drift = t["drift_tolerance_pct"].isnull().sum()
        if missing_drift:
            notes["assumptions"].append(f"{missing_drift} rows missing drift_tolerance_pct, defaulted to {DEFAULT_DRIFT_TOLERANCE}")
        t["drift_tolerance_pct"] = t["drift_tolerance_pct"].fillna(DEFAULT_DRIFT_TOLERANCE)

    # Inception date — normalize to YYYY-MM-DD
    if "account_inception_date" in t.columns:
        t["account_inception_date"] = pd.to_datetime(t["account_inception_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    return t, notes


# ---------------------------------------------------------------------------
# Step 3 (alternate) — Rule Builder: stacked, no-code transformation layers
# ---------------------------------------------------------------------------

_FILTER_OPERATORS = {"==", "!=", ">", "<", ">=", "<=", "is null", "is not null"}


def _coerce_value(raw):
    """Best-effort turn a rule-builder text value into a number, else leave as string."""
    if raw is None:
        return raw
    raw = str(raw).strip()
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except ValueError:
        return raw


def apply_rule_layers(df: pd.DataFrame, layers: list[dict]) -> tuple[pd.DataFrame, dict]:
    """Apply a stack of no-code transformation layers, in order. Each layer is
    a dict with a "type" plus type-specific fields, built by the front end's
    Rule Builder:

      filter        {field, operator, value, action: "keep"|"exclude"}
      normalize     {group_key, value_field, target_sum}
      fill_default  {field, default_value}
      map_values    {field, mapping_text: "OLD:NEW\nOLD2:NEW2"}
      compute       {new_column, expression}   -- evaluated via DataFrame.eval,
                                                    formulas only, no arbitrary code

    Raises ValueError (with the offending layer identified) on any failure,
    the same contract as the custom-code path, so the Sleeve Math Agent can
    handle both uniformly.
    """
    t = df.copy()
    notes = {"assumptions": [], "layers_applied": []}

    for i, layer in enumerate(layers, start=1):
        ltype = layer.get("type")
        label = f"Layer {i} ({ltype})"
        try:
            if ltype == "filter":
                field = layer.get("field", "")
                op = layer.get("operator", "==")
                action = layer.get("action", "keep")
                if field not in t.columns:
                    notes["assumptions"].append(f"{label}: skipped — field '{field}' not present")
                    continue
                if op not in _FILTER_OPERATORS:
                    raise ValueError(f"unknown operator '{op}'")
                if op == "is null":
                    mask = t[field].isnull()
                elif op == "is not null":
                    mask = t[field].notnull()
                else:
                    value = _coerce_value(layer.get("value"))
                    col = t[field]
                    if op in (">", "<", ">=", "<="):
                        col = pd.to_numeric(col, errors="coerce")
                    if op == "==":
                        mask = col == value
                    elif op == "!=":
                        mask = col != value
                    elif op == ">":
                        mask = col > value
                    elif op == "<":
                        mask = col < value
                    elif op == ">=":
                        mask = col >= value
                    else:
                        mask = col <= value
                before = len(t)
                t = t[mask] if action == "keep" else t[~mask]
                notes["layers_applied"].append(
                    f"{label}: {action} rows where {field} {op} {layer.get('value', '')} ({before} -> {len(t)} rows)"
                )

            elif ltype == "normalize":
                group_key = layer.get("group_key", "")
                value_field = layer.get("value_field", "")
                target = float(layer.get("target_sum", 100) or 100)
                if group_key not in t.columns or value_field not in t.columns:
                    notes["assumptions"].append(f"{label}: skipped — missing '{group_key}' or '{value_field}'")
                    continue
                t[value_field] = t.groupby(group_key)[value_field].transform(
                    lambda x: (x / x.sum() * target) if x.sum() else x
                )
                notes["layers_applied"].append(f"{label}: normalized {value_field} to sum to {target} per {group_key}")

            elif ltype == "fill_default":
                field = layer.get("field", "")
                default_value = _coerce_value(layer.get("default_value"))
                if field not in t.columns:
                    notes["assumptions"].append(f"{label}: skipped — field '{field}' not present")
                    continue
                n_missing = int(t[field].isnull().sum())
                t[field] = t[field].fillna(default_value)
                notes["layers_applied"].append(f"{label}: filled {n_missing} null value(s) in {field} with {default_value}")

            elif ltype == "map_values":
                field = layer.get("field", "")
                mapping_text = layer.get("mapping_text", "") or ""
                mapping = {}
                for line in mapping_text.splitlines():
                    if ":" in line:
                        k, v = line.split(":", 1)
                        mapping[k.strip()] = v.strip()
                if field not in t.columns:
                    notes["assumptions"].append(f"{label}: skipped — field '{field}' not present")
                    continue
                t[field] = t[field].astype(str).map(mapping).fillna(t[field])
                notes["layers_applied"].append(f"{label}: mapped {len(mapping)} value(s) in {field}")

            elif ltype == "compute":
                new_col = layer.get("new_column", "")
                expr = layer.get("expression", "")
                if not new_col or not expr:
                    notes["assumptions"].append(f"{label}: skipped — new column name and expression are both required")
                    continue
                t[new_col] = t.eval(expr, engine="python")
                notes["layers_applied"].append(f"{label}: computed {new_col} = {expr}")

            else:
                notes["assumptions"].append(f"{label}: unknown layer type, skipped")

        except Exception as exc:
            raise ValueError(f"{label} failed: {exc}") from exc

    return t, notes


def transform(df: pd.DataFrame, registered_models: set) -> tuple[pd.DataFrame, dict]:
    """Backward-compatible composed transform (sleeve math + compliance overlay), used by the CLI."""
    t, sleeve_notes = sleeve_math(df)
    t, notes = compliance_transform(t, registered_models)
    notes["assumptions"] = sleeve_notes["assumptions"] + notes["assumptions"]
    return t, notes


# ---------------------------------------------------------------------------
# Step 4a — Validate: Data Quality (Data Quality Agent) — integrity checks
# ---------------------------------------------------------------------------

def validate_data_quality(df: pd.DataFrame, required_fields: list[str] = None) -> list[dict]:
    errors = []
    required_fields = REQUIRED_FIELDS if required_fields is None else required_fields

    for field in required_fields:
        if field in df.columns:
            nulls = df[df[field].isnull()]
            if not nulls.empty:
                errors.append({
                    "rule": "REQUIRED_FIELD",
                    "field": field,
                    "rows_affected": len(nulls),
                    "severity": "ERROR",
                })

    if "assets_under_management" in df.columns:
        nan_aum = df[df["assets_under_management"].isnull()]
        if not nan_aum.empty:
            errors.append({
                "rule": "NON_NUMERIC_AUM",
                "rows_affected": len(nan_aum),
                "severity": "ERROR",
            })
        neg_aum = df[df["assets_under_management"] < 0]
        if not neg_aum.empty:
            errors.append({
                "rule": "NEGATIVE_AUM",
                "rows_affected": len(neg_aum),
                "severity": "ERROR",
            })

    return errors


def check_duplicates(df: pd.DataFrame, key_field: str) -> dict | None:
    if key_field not in df.columns:
        return None
    dupes = df[df[key_field].notna() & df[key_field].duplicated(keep=False)]
    if dupes.empty:
        return None
    return {
        "rule": "DUPLICATE_KEY",
        "field": key_field,
        "rows_affected": len(dupes),
        "values": sorted(dupes[key_field].astype(str).unique().tolist())[:20],
        "severity": "ERROR",
    }


def find_latest_output(output_dir: str, entity_key: str) -> str | None:
    files = sorted(glob.glob(os.path.join(output_dir, f"{entity_key}_migrated_*.csv")), reverse=True)
    return files[0] if files else None


def load_parent_ids(output_dir: str, entity_key: str) -> set | None:
    path = find_latest_output(output_dir, entity_key)
    if not path:
        return None
    key = ENTITY_CONFIGS[entity_key]["loaded_key"]
    df = pd.read_csv(path)
    if key not in df.columns:
        return None
    return set(df[key].dropna().astype(str))


def check_orphans(df: pd.DataFrame, fk_field: str, parent_ids: set | None, parent_label: str) -> dict | None:
    if fk_field not in df.columns or parent_ids is None:
        return None
    child_refs = set(df[fk_field].dropna().astype(str))
    orphans = sorted(child_refs - {str(p) for p in parent_ids})
    if not orphans:
        return None
    return {
        "rule": "ORPHAN_RECORD",
        "field": fk_field,
        "detail": f"{len(orphans)} {fk_field} value(s) not found in the most recently loaded {parent_label} batch",
        "values": orphans[:20],
        "severity": "WARNING",
    }


# ---------------------------------------------------------------------------
# Step 4b — Validate: Compliance (Compliance Validator Agent) — business rules
# ---------------------------------------------------------------------------

def validate_compliance(df: pd.DataFrame, pending_model_registration: list[str], group_key: str = "account_number", sum_tolerance: float = 0.5, extra_known_custodians: set = None) -> list[dict]:
    errors = []

    # No-loss check vs source: sleeve allocations must still sum to 100% after sleeve math.
    if "sleeve_allocation_pct" in df.columns and group_key in df.columns:
        sleeve_sums = df.groupby(group_key)["sleeve_allocation_pct"].sum()
        bad_accounts = sleeve_sums[abs(sleeve_sums - 100) > sum_tolerance]
        if not bad_accounts.empty:
            errors.append({
                "rule": "SLEEVE_SUM",
                "detail": f"{len(bad_accounts)} accounts with sleeve allocations != 100%",
                "accounts": bad_accounts.index.tolist(),
                "severity": "WARNING",
            })

    if "account_type" in df.columns:
        unknowns = df[df["account_type"] == "UNKNOWN"]
        if not unknowns.empty:
            errors.append({
                "rule": "UNKNOWN_ACCOUNT_TYPE",
                "rows_affected": len(unknowns),
                "severity": "WARNING",
            })

    if "custodian_code" in df.columns:
        # Codes accepted on prior approved runs (learned) count as known now.
        known_custodians = set(KNOWN_CUSTODIAN_CODES) | set(extra_known_custodians or set())
        bad_custodians = df[~df["custodian_code"].isin(known_custodians)]
        if not bad_custodians.empty:
            errors.append({
                "rule": "UNKNOWN_CUSTODIAN_CODE",
                "rows_affected": len(bad_custodians),
                "codes": sorted(bad_custodians["custodian_code"].unique().tolist()),
                "severity": "WARNING",
            })

    if pending_model_registration:
        errors.append({
            "rule": "PENDING_CORE_MODEL_REGISTRATION",
            "detail": "Advisor-built (Rep-as-PM) models not yet registered as Core Models in the destination",
            "models": pending_model_registration,
            "severity": "WARNING",
        })

    return errors


def validate(df: pd.DataFrame, pending_model_registration: list[str], required_fields: list[str] = None) -> tuple[bool, list[dict]]:
    """Backward-compatible composed validate (data quality + compliance), used by the CLI."""
    errors = validate_data_quality(df, required_fields) + validate_compliance(df, pending_model_registration)
    passed = not any(e["severity"] == "ERROR" for e in errors)
    return passed, errors


# ---------------------------------------------------------------------------
# Step 5 — Human-in-the-loop gate
# ---------------------------------------------------------------------------

def write_review_file(audit_dir: str, timestamp: str, source_df, transformed_df, errors) -> str:
    os.makedirs(audit_dir, exist_ok=True)
    path = os.path.join(audit_dir, f"review_summary_{timestamp}.txt")
    error_count = sum(1 for e in errors if e["severity"] == "ERROR")
    warning_count = sum(1 for e in errors if e["severity"] == "WARNING")

    lines = [
        "=" * 60,
        "MIGRATION REVIEW - HUMAN APPROVAL REQUIRED",
        "=" * 60,
        f"  Source rows:       {len(source_df)}",
        f"  Transformed rows:  {len(transformed_df)}",
        f"  Validation errors: {error_count}",
        f"  Warnings:          {warning_count}",
        "",
        "Findings:",
    ]
    for e in errors:
        lines.append(f"  [{e['severity']}] {e['rule']} - {e.get('detail', e.get('rows_affected', ''))}")
    lines += [
        "",
        "Sample transformed output (first 5 rows):",
        transformed_df.head(5).to_string(index=False),
        "",
    ]
    with open(path, "w") as f:
        f.write("\n".join(lines))
    log(f"[HITL] Review summary written to {path}")
    return path


def hitl_gate(source_df, transformed_df, errors, audit_dir, timestamp, approve_flag, force_flag) -> str:
    review_path = write_review_file(audit_dir, timestamp, source_df, transformed_df, errors)
    has_errors = any(e["severity"] == "ERROR" for e in errors)

    if has_errors and not force_flag:
        log("[HITL] REJECTED — validation errors present. Re-run with --force after fixing source data,")
        log("       or use --force only if you have reviewed and accept the errors.")
        return "REJECTED"

    if approve_flag:
        log("[HITL] APPROVED via --approve flag.")
        return "APPROVED"

    if sys.stdin.isatty():
        print(open(review_path).read())
        print("Actions: [A] Approve and load  [R] Reject  [S] Skip this batch")
        try:
            decision = input("Your decision: ").strip().upper()
        except EOFError:
            log(f"[HITL] PENDING — no input available. Review {review_path} and re-run with --approve.")
            return "PENDING"
        return {"A": "APPROVED", "R": "REJECTED"}.get(decision, "SKIPPED")

    log(f"[HITL] PENDING — non-interactive session. Review {review_path} and re-run with --approve.")
    return "PENDING"


# ---------------------------------------------------------------------------
# Step 6 — Load
# ---------------------------------------------------------------------------

def load_destination(df: pd.DataFrame, output_dir: str, entity_name: str, timestamp: str, fmt: str = "csv") -> str:
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{entity_name}_migrated_{timestamp}.{fmt}"
    output_path = os.path.join(output_dir, filename)

    if fmt == "csv":
        df.to_csv(output_path, index=False)
    elif fmt == "xlsx":
        df.to_excel(output_path, index=False)
    elif fmt == "json":
        df.to_json(output_path, orient="records", indent=2)
    else:
        raise ValueError(f"Unsupported output format: {fmt}")

    log(f"[LOAD] Wrote {len(df)} rows -> {output_path}")
    return output_path


def register_core_models(pending_models: list[str], registry_path: str) -> set:
    registered = set()
    if os.path.exists(registry_path):
        with open(registry_path) as f:
            registered = set(json.load(f))
    if pending_models:
        registered |= set(pending_models)
        with open(registry_path, "w") as f:
            json.dump(sorted(registered), f, indent=2)
        log(f"[LOAD] Registered {len(pending_models)} advisor-built model(s) as Core Models -> {registry_path}")
    return registered


def load_registered_models(registry_path: str) -> set:
    if os.path.exists(registry_path):
        with open(registry_path) as f:
            return set(json.load(f))
    return set()


# ---------------------------------------------------------------------------
# Step 7 — Reconcile
# ---------------------------------------------------------------------------

def reconcile(source_df: pd.DataFrame, loaded_df: pd.DataFrame, source_key: str, loaded_key: str) -> tuple[str, list, list]:
    source_keys = set(source_df[source_key].dropna().astype(str))
    loaded_keys = set(loaded_df[loaded_key].dropna().astype(str))
    missing = sorted(source_keys - loaded_keys)
    extra = sorted(loaded_keys - source_keys)

    status = "PASS" if not missing and not extra else "FAIL"
    log("[RECONCILE] Source records:      " + str(len(source_keys)))
    log("[RECONCILE] Destination records: " + str(len(loaded_keys)))
    log("[RECONCILE] Missing in dest:     " + str(len(missing)))
    log("[RECONCILE] Extra in dest:       " + str(len(extra)))
    log(f"[RECONCILE] Status: {status}")
    return status, missing, extra


def reconcile_aum(source_df: pd.DataFrame, loaded_df: pd.DataFrame, source_field: str, loaded_field: str, tolerance_pct: float = 0.5) -> dict | None:
    """Reconciler Agent — Total AUM drift check. Tolerance: <= 0.5% AUM drift by default."""
    if source_field not in source_df.columns or loaded_field not in loaded_df.columns:
        return None
    source_total = float(pd.to_numeric(source_df[source_field], errors="coerce").sum())
    loaded_total = float(pd.to_numeric(loaded_df[loaded_field], errors="coerce").sum())
    if source_total == 0:
        drift_pct = 0.0 if loaded_total == 0 else 100.0
    else:
        drift_pct = abs(loaded_total - source_total) / abs(source_total) * 100
    status = "PASS" if drift_pct <= tolerance_pct else "FAIL"
    return {
        "source_total": round(source_total, 2),
        "loaded_total": round(loaded_total, 2),
        "drift_pct": round(drift_pct, 4),
        "tolerance_pct": tolerance_pct,
        "status": status,
    }


def write_reconciliation_report(audit_dir: str, timestamp: str, status: str, missing: list, extra: list, source_count: int, loaded_count: int) -> str:
    os.makedirs(audit_dir, exist_ok=True)
    path = os.path.join(audit_dir, f"reconciliation_{timestamp}.txt")
    with open(path, "w") as f:
        f.write("RECONCILIATION REPORT\n")
        f.write(f"  Source records:      {source_count}\n")
        f.write(f"  Destination records: {loaded_count}\n")
        f.write(f"  Missing in dest:     {len(missing)} {missing}\n")
        f.write(f"  Extra in dest:       {len(extra)} {extra}\n")
        f.write(f"  Status: {status}\n")
    log(f"[RECONCILE] Report written to {path}")
    return path


def write_validation_report(audit_dir: str, timestamp: str, passed: bool, errors: list) -> str:
    os.makedirs(audit_dir, exist_ok=True)
    path = os.path.join(audit_dir, f"validation_report_{timestamp}.json")
    with open(path, "w") as f:
        json.dump({"passed": passed, "errors": errors}, f, indent=2, default=str)
    log(f"[VALIDATE] Report written to {path}")
    return path


# ---------------------------------------------------------------------------
# Step 8 — Audit
# ---------------------------------------------------------------------------

def write_audit_log(run_meta: dict, audit_dir: str, timestamp: str) -> str:
    os.makedirs(audit_dir, exist_ok=True)
    path = os.path.join(audit_dir, f"audit_{timestamp}.json")
    with open(path, "w") as f:
        json.dump(run_meta, f, indent=2, default=str)
    log(f"[AUDIT] Audit log written to {path}")
    return path


# ---------------------------------------------------------------------------
# Orchestration (CLI) — kept for scripted/non-interactive runs; the Streamlit
# app instead drives agents/orchestrator.py's OrchestratorAgent directly.
# ---------------------------------------------------------------------------

def run_pipeline(args) -> int:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    registry_path = os.path.join(os.path.dirname(args.source) or ".", "..", "core_models_registry.json")
    registry_path = os.path.normpath(registry_path)

    source_df = extract(args.source)
    mapped_df, unmapped_cols = apply_schema_map(source_df, FIELD_MAP)

    registered_models = load_registered_models(registry_path)
    transformed_df, transform_notes = transform(mapped_df, registered_models)

    passed, errors = validate(transformed_df, transform_notes["pending_model_registration"])
    log(f"[VALIDATE] {'PASSED' if passed else 'FAILED'} ({len(errors)} finding(s))")
    for e in errors:
        log(f"  [{e['severity']}] {e['rule']} - {e.get('detail', e.get('rows_affected', ''))}")
    validation_report_path = write_validation_report(args.audit_dir, timestamp, passed, errors)

    decision = hitl_gate(source_df, transformed_df, errors, args.audit_dir, timestamp, args.approve, args.force)

    output_path = None
    reconciliation_status = None
    missing, extra = [], []

    if decision == "APPROVED":
        registered_models = register_core_models(transform_notes["pending_model_registration"], registry_path)
        output_path = load_destination(transformed_df, args.output_dir, "accounts", timestamp, fmt=args.format)

        loaded_df = pd.read_csv(output_path) if args.format == "csv" else transformed_df
        reconciliation_status, missing, extra = reconcile(
            source_df, loaded_df, source_key="AccountNumber", loaded_key="account_number"
        )
        write_reconciliation_report(
            args.audit_dir, timestamp, reconciliation_status, missing, extra, len(source_df), len(loaded_df)
        )
    else:
        log(f"[LOAD] Skipped — HITL decision was {decision}.")

    audit = {
        "run_timestamp": datetime.now().isoformat(),
        "source_file": args.source,
        "source_row_count": len(source_df),
        "mapped_columns": list(mapped_df.columns),
        "unmapped_source_columns": unmapped_cols,
        "transform_assumptions": transform_notes["assumptions"],
        "pending_core_model_registrations": transform_notes["pending_model_registration"],
        "validation_result": "PASS" if passed else "FAIL",
        "validation_errors": errors,
        "validation_report": validation_report_path,
        "hitl_decision": decision,
        "destination_file": output_path,
        "reconciliation_status": reconciliation_status,
        "missing_records": missing,
        "extra_records": extra,
    }
    write_audit_log(audit, args.audit_dir, timestamp)

    return 0 if decision == "APPROVED" and reconciliation_status == "PASS" else 1


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="data migration pipeline")
    parser.add_argument("--source", default="source/accounts.csv", help="Path to source file (csv/xlsx/json)")
    parser.add_argument("--output-dir", default="output", help="Directory for migrated output files")
    parser.add_argument("--audit-dir", default="audit", help="Directory for audit/validation/reconciliation reports")
    parser.add_argument("--format", default="csv", choices=["csv", "xlsx", "json"], help="Destination file format")
    parser.add_argument("--approve", action="store_true", help="Auto-approve the HITL gate (requires no ERROR-severity findings unless --force)")
    parser.add_argument("--force", action="store_true", help="Approve despite ERROR-severity validation findings")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run_pipeline(parse_args()))
