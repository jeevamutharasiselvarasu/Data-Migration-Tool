"""Rollback Path — triggered by a reconciliation breach (>0.5% AUM drift or
a key-set mismatch), a failed trade rehearsal, an HITL reject after load, or
a custodian feed mismatch. Action: quarantine the loaded file, roll the Core
Models registry back to its pre-run snapshot, and write an incident report."""

import json
import os
import shutil
import time
from datetime import datetime


class RollbackAgent:
    NAME = "Rollback Path"

    @staticmethod
    def run(context, quarantine_dir: str, registry_path: str, registry_snapshot: set, timestamp: str, reason: str):
        t0 = time.perf_counter()
        os.makedirs(quarantine_dir, exist_ok=True)

        quarantined_path = None
        if context.output_path and os.path.exists(context.output_path):
            quarantined_path = os.path.join(quarantine_dir, os.path.basename(context.output_path))
            shutil.move(context.output_path, quarantined_path)
            context.output_path = None

        if registry_snapshot is not None:
            with open(registry_path, "w") as f:
                json.dump(sorted(registry_snapshot), f, indent=2)

        incident = {
            "run_timestamp": datetime.now().isoformat(),
            "entity": context.entity_key,
            "reason": reason,
            "quarantined_file": quarantined_path,
            "reconciliation": context.reconciliation,
            "aum_reconciliation": context.aum_reconciliation,
        }
        incident_path = os.path.join(quarantine_dir, f"incident_{context.entity_key}_{timestamp}.json")
        with open(incident_path, "w") as f:
            json.dump(incident, f, indent=2, default=str)

        context.decision = "ROLLED_BACK"
        context.incident_path = incident_path
        context.log(
            RollbackAgent.NAME,
            "ROLLED_BACK",
            f"{reason}; quarantined -> {quarantined_path}; incident -> {incident_path}",
            (time.perf_counter() - t0) * 1000,
        )
        return context
