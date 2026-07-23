"""7. Reconciler Agent — compares Orion post-load vs. Amplify source by
record count (key-set diff) and Total AUM. Tolerance: <= 0.5% AUM drift.
A breach here is what the Rollback Path is triggered by."""

import time

import pandas as pd

import pipeline as pl

# (source AUM field, destination AUM field) candidates, checked in order.
AUM_FIELD_CANDIDATES = [("AUM", "assets_under_management"), ("TotalAUM", "total_aum")]


class ReconcilerAgent:
    NAME = "7. Reconciler Agent"
    AUM_TOLERANCE_PCT = 0.5

    @staticmethod
    def run(context, source_key: str, loaded_key: str, fmt: str):
        t0 = time.perf_counter()
        loaded_df = pd.read_csv(context.output_path) if fmt == "csv" else context.transformed_df

        status, missing, extra = pl.reconcile(context.source_df, loaded_df, source_key, loaded_key)

        aum_result = None
        for src_field, dest_field in AUM_FIELD_CANDIDATES:
            if src_field in context.source_df.columns and dest_field in loaded_df.columns:
                aum_result = pl.reconcile_aum(context.source_df, loaded_df, src_field, dest_field, ReconcilerAgent.AUM_TOLERANCE_PCT)
                break

        overall_status = status
        if aum_result and aum_result["status"] == "FAIL":
            overall_status = "FAIL"

        context.reconciliation = {"status": status, "missing": missing, "extra": extra, "overall_status": overall_status}
        context.aum_reconciliation = aum_result

        detail = f"key-set {status} — missing {len(missing)}, extra {len(extra)}"
        if aum_result:
            detail += f"; AUM drift {aum_result['drift_pct']}% (tolerance {aum_result['tolerance_pct']}%) -> {aum_result['status']}"
        context.log(ReconcilerAgent.NAME, "OK" if overall_status == "PASS" else "BREACH", detail, (time.perf_counter() - t0) * 1000)
        return context
