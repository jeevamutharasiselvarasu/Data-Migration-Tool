"""1. Extractor Agent — pulls a per-advisor-book snapshot from the source
file (CSV / Excel / JSON), analogous to the diagram's source connector pull."""

import time

import pipeline as pl


class ExtractorAgent:
    NAME = "1. Extractor Agent"

    @staticmethod
    def run(context, source_path: str):
        t0 = time.perf_counter()
        df = pl.extract(source_path)
        context.source_df = df
        context.source_files = [source_path]
        context.log(
            ExtractorAgent.NAME,
            "OK",
            f"Extracted {len(df)} row(s), {len(df.columns)} column(s) from {source_path}",
            (time.perf_counter() - t0) * 1000,
        )
        return context

    @staticmethod
    def run_many(context, source_paths: list[str]):
        """Extract and combine several source files into one data set for a
        single destination table — one advisor-book batch per file, tagged
        with a 'source_file' column for lineage."""
        t0 = time.perf_counter()
        df, notes = pl.extract_many(source_paths)
        context.source_df = df
        context.source_files = list(source_paths)
        context.extract_notes = notes
        detail = f"Combined {notes['file_count']} file(s) into {len(df)} row(s): {notes['file_row_counts']}"
        context.log(ExtractorAgent.NAME, "OK", detail, (time.perf_counter() - t0) * 1000)
        return context

    @staticmethod
    def run_joined(context, source_paths: list[str], join_key: str, how: str = "inner"):
        """Extract several source files that each hold a different slice of
        the same records and JOIN them side by side on a shared key column
        (e.g. account identity + balances + fees, all keyed on AccountNumber)
        -- as opposed to run_many(), which stacks rows from files that share
        the same shape."""
        t0 = time.perf_counter()
        df, notes = pl.extract_and_join(source_paths, join_key, how)
        context.source_df = df
        context.source_files = list(source_paths)
        context.extract_notes = notes
        detail = (
            f"Joined {notes['file_count']} file(s) on '{join_key}' ({how}) into {len(df)} row(s): "
            f"{notes['file_row_counts']}"
        )
        if notes["unmatched_by_file"]:
            detail += f"; unmatched key counts: {notes['unmatched_by_file']}"
        context.log(ExtractorAgent.NAME, "OK", detail, (time.perf_counter() - t0) * 1000)
        return context
