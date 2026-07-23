"""MigrationContext — the shared state object every agent reads from and
writes to, plus the immutable run-log the Orchestrator and Audit & Evidence
agent use to reconstruct exactly what happened during a run."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd


@dataclass
class MigrationContext:
    entity_key: Optional[str] = None
    field_map: dict = field(default_factory=dict)
    source_name: Optional[str] = None
    source_files: list = field(default_factory=list)
    extract_notes: dict = field(default_factory=dict)

    # Learning memory (agents/knowledge_store.KnowledgeStore) -- optional; the
    # pipeline runs fine when this is None. Not persisted with the context.
    knowledge_store: object = None
    learned_this_run: dict = field(default_factory=dict)

    source_df: Optional[pd.DataFrame] = None
    mapped_df: Optional[pd.DataFrame] = None
    unmapped_cols: list = field(default_factory=list)
    transformed_df: Optional[pd.DataFrame] = None

    sleeve_notes: dict = field(default_factory=dict)
    compliance_notes: dict = field(default_factory=lambda: {"assumptions": [], "pending_model_registration": []})
    sleeve_math_config: dict = field(
        default_factory=lambda: {
            "mode": "toggles",
            "normalize": True,
            "exclude_zero": True,
            "zero_threshold": 0.0,
            "group_key": "account_number",
            "tolerance": 0.5,
            "custom_code": None,
            "layers": [],
        }
    )

    dq_errors: list = field(default_factory=list)
    compliance_errors: list = field(default_factory=list)
    passed: bool = False

    decision: Optional[str] = None
    output_path: Optional[str] = None
    reconciliation: Optional[dict] = None
    aum_reconciliation: Optional[dict] = None
    cutover_record: Optional[dict] = None
    cutover_path: Optional[str] = None
    incident_path: Optional[str] = None
    audit_path: Optional[str] = None

    run_log: list = field(default_factory=list)  # append-only — never mutate past entries

    @property
    def errors(self) -> list:
        return self.compliance_errors + self.dq_errors

    def log(self, agent: str, status: str, detail: str, duration_ms: float = 0.0) -> None:
        self.run_log.append(
            {
                "agent": agent,
                "status": status,
                "detail": detail,
                "timestamp": datetime.now().isoformat(),
                "duration_ms": round(duration_ms, 2),
            }
        )
