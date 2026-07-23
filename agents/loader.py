"""6. Loader Agent — writes approved records to the Orion staging schema
(here: output_dir) in dependency order, registering any newly-approved
Core Models first. Idempotent per timestamp; resumable since each run
produces a uniquely-named file."""

import time

import pipeline as pl


class LoaderAgent:
    NAME = "6. Loader Agent"

    @staticmethod
    def run(context, output_dir: str, registry_path: str, entity_name: str, fmt: str, timestamp: str):
        t0 = time.perf_counter()
        pl.register_core_models(context.compliance_notes.get("pending_model_registration", []), registry_path)
        output_path = pl.load_destination(context.transformed_df, output_dir, entity_name, timestamp, fmt=fmt)
        context.output_path = output_path
        context.log(
            LoaderAgent.NAME,
            "OK",
            f"Wrote {len(context.transformed_df)} row(s) -> {output_path}",
            (time.perf_counter() - t0) * 1000,
        )
        return context
