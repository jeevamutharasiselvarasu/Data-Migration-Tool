"""8. Cutover Agent — promotes the just-loaded batch from staging to
production, records which keys were cut over, and notes that source-side
rebalancing/write-back is disabled for those records. In this prototype
"production" is a JSON cutover record rather than a live trading system."""

import json
import os
import time
from datetime import datetime


class CutoverAgent:
    NAME = "8. Cutover Agent"

    @staticmethod
    def run(context, cutover_dir: str, loaded_key: str, timestamp: str):
        t0 = time.perf_counter()
        os.makedirs(cutover_dir, exist_ok=True)

        keys = []
        if context.transformed_df is not None and loaded_key in context.transformed_df.columns:
            keys = context.transformed_df[loaded_key].dropna().astype(str).tolist()

        record = {
            "run_timestamp": datetime.now().isoformat(),
            "entity": context.entity_key,
            "status": "PRODUCTION",
            "promoted_key_field": loaded_key,
            "promoted_keys": keys,
            "note": "Source-side rebalancing/write-back disabled for these records (simulated cutover).",
        }
        path = os.path.join(cutover_dir, f"cutover_{context.entity_key}_{timestamp}.json")
        with open(path, "w") as f:
            json.dump(record, f, indent=2, default=str)

        context.cutover_record = record
        context.cutover_path = path
        context.log(
            CutoverAgent.NAME,
            "OK",
            f"Promoted {len(keys)} record(s) to PRODUCTION -> {path}",
            (time.perf_counter() - t0) * 1000,
        )
        return context
