"""9. Audit & Evidence Agent — captures every transformation, rule decision,
and human decision made during the run. Runs "concurrently" with every other
agent in spirit: each agent appends to the immutable run-log on the shared
context as it executes, and this agent persists that log plus the
validation/reconciliation/audit artifacts to disk."""

import time
from datetime import datetime

import pipeline as pl

from . import guardrails


class AuditEvidenceAgent:
    NAME = "9. Audit & Evidence Agent"

    @staticmethod
    def write_review(context, audit_dir: str, timestamp: str) -> str:
        t0 = time.perf_counter()
        # PII-ish columns are masked in the human-readable review sample only —
        # never in the data actually written to the destination.
        sample_df = guardrails.redact_pii(context.transformed_df)
        path = pl.write_review_file(audit_dir, timestamp, context.source_df, sample_df, context.errors)
        context.log(AuditEvidenceAgent.NAME, "OK", f"Review summary -> {path}", (time.perf_counter() - t0) * 1000)
        return path

    @staticmethod
    def finalize(context, audit_dir: str, timestamp: str, decision: str) -> tuple[dict, str]:
        t0 = time.perf_counter()
        validation_report_path = pl.write_validation_report(audit_dir, timestamp, context.passed, context.errors)

        reconciliation_report_path = None
        recon = context.reconciliation or {}
        if context.output_path or context.incident_path:
            reconciliation_report_path = pl.write_reconciliation_report(
                audit_dir,
                timestamp,
                recon.get("overall_status"),
                recon.get("missing", []),
                recon.get("extra", []),
                len(context.source_df) if context.source_df is not None else 0,
                len(context.transformed_df) if context.transformed_df is not None else 0,
            )

        audit = {
            "run_timestamp": datetime.now().isoformat(),
            "entity": context.entity_key,
            "source_file": context.source_name,
            "schema_map_used": context.field_map,
            "unmapped_source_columns": context.unmapped_cols,
            "sleeve_math_config": context.sleeve_math_config,
            "sleeve_math_notes": context.sleeve_notes,
            "compliance_notes": context.compliance_notes,
            "validation_result": "PASS" if context.passed else "FAIL",
            "validation_errors": context.errors,
            "validation_report": validation_report_path,
            "reconciliation_report": reconciliation_report_path,
            "hitl_decision": decision,
            "destination_file": context.output_path,
            "reconciliation": context.reconciliation,
            "aum_reconciliation": context.aum_reconciliation,
            "cutover_record": context.cutover_record,
            "incident_report": context.incident_path,
            "agent_run_log": context.run_log,
        }
        audit_path = pl.write_audit_log(audit, audit_dir, timestamp)
        context.audit_path = audit_path
        context.log(AuditEvidenceAgent.NAME, "OK", f"Audit log -> {audit_path}", (time.perf_counter() - t0) * 1000)
        return audit, audit_path
