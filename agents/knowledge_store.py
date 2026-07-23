"""Learning memory for the migration agents — a small, persisted, fully
offline rule store that lets the agents *evolve* between runs without any
LLM or API key.

It learns three things, each with clear provenance:

  1. Accepted custodian codes  — auto-learned from every APPROVED run. A code
     that shipped in an approved batch is one a human vouched for, so the
     Compliance Validator stops flagging it as UNKNOWN_CUSTODIAN_CODE next
     time.
  2. Account-type crosswalk entries — *explicitly taught* by the user in the
     Map step (raw Amplify code -> normalized Orion value). The Compliance
     Validator merges these over the built-in ACCOUNT_TYPE_MAP, so a code
     that was UNKNOWN becomes a real mapping on the next run.
  3. Confirmed column -> field mappings — auto-learned when a custom schema
     mapping is applied and that batch is then approved. The Schema Mapper
     offers the remembered destination field as a suggestion the next time
     the same source column shows up (even for a different entity file).

Everything is stored as plain JSON on disk so it is inspectable, diff-able,
resettable, and needs no network. Nothing here calls an LLM; see llm.py for
the optional, key-gated Claude reasoning layer.
"""

import json
import os
from datetime import datetime


STORE_VERSION = 1


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _empty_store() -> dict:
    return {
        "version": STORE_VERSION,
        "runs_learned": 0,
        "custodian_codes": {},   # CODE -> {count, first_seen, last_seen}
        "account_types": {},     # RAW_CODE -> {normalized, count, taught_at}
        "mappings": {},          # entity_key -> {source_col -> {dest, count, last_seen}}
    }


class KnowledgeStore:
    """Thin wrapper over a JSON file. Every mutating call persists immediately
    so a crash mid-session never loses a confirmed lesson."""

    def __init__(self, path: str):
        self.path = path
        self.data = self._load()

    # -- persistence -------------------------------------------------------

    def _load(self) -> dict:
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    data = json.load(f)
                base = _empty_store()
                base.update({k: data.get(k, base[k]) for k in base})
                return base
            except (json.JSONDecodeError, OSError):
                return _empty_store()
        return _empty_store()

    def save(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=2)
        os.replace(tmp, self.path)

    def reset(self) -> None:
        self.data = _empty_store()
        self.save()

    # -- recall (read side, consumed by the agents) ------------------------

    def known_custodians(self) -> set:
        return set(self.data["custodian_codes"].keys())

    def account_type_overrides(self) -> dict:
        """RAW_CODE -> normalized value, merged over the built-in crosswalk."""
        return {code: rec["normalized"] for code, rec in self.data["account_types"].items()}

    def suggested_mapping(self, entity_key: str, source_columns) -> dict:
        """For the given source columns, return {source_col -> dest_field} for
        any column we've seen confirmed before (same entity preferred, else
        any entity), so the Schema Mapper can offer it as a suggestion."""
        suggestions = {}
        per_entity = self.data["mappings"].get(entity_key, {})
        cross = {}
        for ent, cols in self.data["mappings"].items():
            for col, rec in cols.items():
                cross.setdefault(col, rec["dest"])
        for col in source_columns:
            if col in per_entity:
                suggestions[col] = per_entity[col]["dest"]
            elif col in cross:
                suggestions[col] = cross[col]
        return suggestions

    # -- teaching (explicit, user-initiated) -------------------------------

    def teach_account_type(self, raw_code: str, normalized: str) -> None:
        raw_code = str(raw_code).strip()
        normalized = str(normalized).strip()
        if not raw_code or not normalized:
            return
        rec = self.data["account_types"].get(raw_code, {"count": 0})
        rec["normalized"] = normalized
        rec["count"] = rec.get("count", 0) + 1
        rec["taught_at"] = _now()
        self.data["account_types"][raw_code] = rec
        self.save()

    def forget_account_type(self, raw_code: str) -> None:
        self.data["account_types"].pop(str(raw_code).strip(), None)
        self.save()

    # -- learning (automatic, on approval) ---------------------------------

    def learn_from_approval(self, context) -> dict:
        """Called by the Orchestrator right after a batch is APPROVED. Returns
        a summary of what was newly learned, for surfacing in the UI/audit."""
        learned = {"custodians": [], "mappings": []}
        df = context.transformed_df

        if df is not None and "custodian_code" in df.columns:
            for code in df["custodian_code"].dropna().astype(str).unique():
                code = code.strip()
                if not code or code.lower() == "nan":
                    continue
                rec = self.data["custodian_codes"].get(code)
                if rec is None:
                    self.data["custodian_codes"][code] = {"count": 1, "first_seen": _now(), "last_seen": _now()}
                    learned["custodians"].append(code)
                else:
                    rec["count"] += 1
                    rec["last_seen"] = _now()

        entity_key = context.entity_key
        field_map = context.field_map or {}
        if field_map:
            ent = self.data["mappings"].setdefault(entity_key, {})
            for source_col, dest_field in field_map.items():
                rec = ent.get(source_col)
                if rec is None:
                    ent[source_col] = {"dest": dest_field, "count": 1, "last_seen": _now()}
                    learned["mappings"].append((source_col, dest_field))
                else:
                    if rec["dest"] != dest_field:
                        rec["dest"] = dest_field
                        learned["mappings"].append((source_col, dest_field))
                    rec["count"] += 1
                    rec["last_seen"] = _now()

        self.data["runs_learned"] += 1
        self.save()
        return learned

    # -- introspection (for the UI) ----------------------------------------

    def stats(self) -> dict:
        return {
            "runs_learned": self.data["runs_learned"],
            "custodian_codes": len(self.data["custodian_codes"]),
            "account_types": len(self.data["account_types"]),
            "mapping_entities": len(self.data["mappings"]),
            "mapping_columns": sum(len(v) for v in self.data["mappings"].values()),
        }
