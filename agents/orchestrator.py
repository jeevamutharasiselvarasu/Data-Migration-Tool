"""Orchestrator Agent (Conductor) — routes work through each specialist
agent in order, manages the MigrationContext state object, triggers the
Human-in-the-Loop gate, maintains the immutable audit/run log, and handles
rollback on reconciliation breach."""

import os
import time
from datetime import datetime

import pipeline as pl

from . import guardrails
from .audit_evidence import AuditEvidenceAgent
from .compliance_validator import ComplianceValidatorAgent
from .context import MigrationContext
from .cutover import CutoverAgent
from .data_quality import DataQualityAgent
from .extractor import ExtractorAgent
from .loader import LoaderAgent
from .reconciler import ReconcilerAgent
from .rollback import RollbackAgent
from .schema_mapper import SchemaMapperAgent
from .sleeve_math import SleeveMathAgent


class OrchestratorAgent:
    NAME = "Orchestrator (Conductor)"

    # -- Extract through Data Quality, i.e. everything up to the HITL gate --

    @staticmethod
    def run_to_hitl(
        source_path: str,
        field_map: dict,
        entity_key: str,
        registry_path: str,
        output_dir: str = None,
        sleeve_math_config: dict = None,
        knowledge_store=None,
    ) -> MigrationContext:
        context = MigrationContext(entity_key=entity_key, source_name=source_path)
        context.knowledge_store = knowledge_store
        if sleeve_math_config is not None:
            context.sleeve_math_config = sleeve_math_config
        context.log(OrchestratorAgent.NAME, "RUNNING", f"Starting run for entity '{entity_key}' from {source_path}")

        ExtractorAgent.run(context, source_path)
        return OrchestratorAgent._run_map_through_dq(context, field_map, registry_path, output_dir)

    @staticmethod
    def run_to_hitl_multi(
        source_paths: list,
        field_map: dict,
        entity_key: str,
        registry_path: str,
        output_dir: str = None,
        sleeve_math_config: dict = None,
        knowledge_store=None,
    ) -> MigrationContext:
        """Combine several source files into one destination table for the
        same entity — e.g. several advisor-cohort batches loaded together."""
        display_name = ", ".join(os.path.basename(p) for p in source_paths)
        context = MigrationContext(entity_key=entity_key, source_name=display_name)
        context.knowledge_store = knowledge_store
        if sleeve_math_config is not None:
            context.sleeve_math_config = sleeve_math_config
        context.log(
            OrchestratorAgent.NAME,
            "RUNNING",
            f"Starting run for entity '{entity_key}' from {len(source_paths)} file(s): {display_name}",
        )

        ExtractorAgent.run_many(context, source_paths)
        return OrchestratorAgent._run_map_through_dq(context, field_map, registry_path, output_dir)

    @staticmethod
    def run_to_hitl_joined(
        source_paths: list,
        join_key: str,
        field_map: dict,
        entity_key: str,
        registry_path: str,
        output_dir: str = None,
        sleeve_math_config: dict = None,
        how: str = "inner",
        knowledge_store=None,
    ) -> MigrationContext:
        """Join several source files side by side on a shared key (e.g.
        account identity + balances + fees, all keyed on AccountNumber) into
        one destination table -- as opposed to run_to_hitl_multi(), which
        stacks rows from files that share the same shape."""
        display_name = ", ".join(os.path.basename(p) for p in source_paths)
        context = MigrationContext(entity_key=entity_key, source_name=display_name)
        context.knowledge_store = knowledge_store
        if sleeve_math_config is not None:
            context.sleeve_math_config = sleeve_math_config
        context.log(
            OrchestratorAgent.NAME,
            "RUNNING",
            f"Starting run for entity '{entity_key}' joining {len(source_paths)} file(s) on '{join_key}': {display_name}",
        )

        ExtractorAgent.run_joined(context, source_paths, join_key, how)
        return OrchestratorAgent._run_map_through_dq(context, field_map, registry_path, output_dir)

    @staticmethod
    def remap(
        context: MigrationContext,
        field_map: dict,
        registry_path: str,
        output_dir: str = None,
        sleeve_math_config: dict = None,
    ) -> MigrationContext:
        """Re-run Schema Mapper onward against the already-extracted source_df —
        used when the user edits the destination mapping or the Sleeve Math
        transformation logic in the front end."""
        if sleeve_math_config is not None:
            context.sleeve_math_config = sleeve_math_config
        context.log(OrchestratorAgent.NAME, "RUNNING", "Re-running from cached extract with updated settings")
        return OrchestratorAgent._run_map_through_dq(context, field_map, registry_path, output_dir)

    @staticmethod
    def _run_map_through_dq(context: MigrationContext, field_map: dict, registry_path: str, output_dir: str) -> MigrationContext:
        SchemaMapperAgent.run(context, field_map)
        SleeveMathAgent.run(context, context.sleeve_math_config)
        registered_models = pl.load_registered_models(registry_path)
        ComplianceValidatorAgent.run(context, registered_models)
        DataQualityAgent.run(context, pl.ENTITY_CONFIGS[context.entity_key].get("required_fields"), output_dir)

        context.decision = None
        context.output_path = None
        context.reconciliation = None
        context.aum_reconciliation = None
        context.cutover_record = None
        context.cutover_path = None
        context.incident_path = None
        context.log(OrchestratorAgent.NAME, "HITL_GATE", "Awaiting human-in-the-loop decision")
        return context

    # -- Human-in-the-loop decision --

    @staticmethod
    def approve(
        context: MigrationContext,
        output_dir: str,
        audit_dir: str,
        cutover_dir: str,
        quarantine_dir: str,
        registry_path: str,
        entity_name: str,
        fmt: str,
        source_key: str,
        loaded_key: str,
        force: bool = False,
        timestamp: str = None,
    ) -> MigrationContext:
        if not context.passed and not force:
            raise ValueError("Cannot approve: unresolved ERROR-severity findings and force=False")

        timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
        context.decision = "APPROVED"
        context.log(OrchestratorAgent.NAME, "APPROVED", "Human reviewer approved the batch — proceeding to load")

        registry_snapshot = pl.load_registered_models(registry_path)

        try:
            guardrails.enforce_pre_load(context)
        except guardrails.GuardrailViolation as exc:
            context.log("Guardrails & Policies", "BLOCKED", str(exc))
            raise

        LoaderAgent.run(context, output_dir, registry_path, entity_name, fmt, timestamp)
        ReconcilerAgent.run(context, source_key, loaded_key, fmt)

        if context.reconciliation["overall_status"] != "PASS":
            reason = f"Reconciliation breach: {context.reconciliation}"
            if context.aum_reconciliation:
                reason += f"; AUM drift {context.aum_reconciliation['drift_pct']}%"
            RollbackAgent.run(context, quarantine_dir, registry_path, registry_snapshot, timestamp, reason)
        else:
            CutoverAgent.run(context, cutover_dir, loaded_key, timestamp)
            # The batch cleanly loaded and reconciled -- let the learning memory
            # evolve from what a human just vouched for (accepted custodian
            # codes + confirmed column mappings). Fully local, no API calls.
            store = getattr(context, "knowledge_store", None)
            if store is not None:
                learned = store.learn_from_approval(context)
                context.learned_this_run = learned
                if learned["custodians"] or learned["mappings"]:
                    context.log(
                        "Learning Memory", "OK",
                        f"Learned {len(learned['custodians'])} new custodian code(s) and "
                        f"{len(learned['mappings'])} mapping rule(s) from this approved batch",
                    )

        AuditEvidenceAgent.finalize(context, audit_dir, timestamp, context.decision)
        return context

    @staticmethod
    def reject(context: MigrationContext, audit_dir: str, timestamp: str = None) -> MigrationContext:
        timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
        context.decision = "REJECTED"
        context.log(OrchestratorAgent.NAME, "REJECTED", "Human reviewer rejected the batch — no data loaded")
        AuditEvidenceAgent.finalize(context, audit_dir, timestamp, context.decision)
        return context
