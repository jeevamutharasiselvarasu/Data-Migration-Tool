"""2. Schema Mapper — applies field-level mapping rules from the mapping
rule catalog (knowledge.py / pipeline.ENTITY_CONFIGS), flagging any source
columns left unmapped so ambiguity is surfaced rather than silently dropped."""

import time

import pipeline as pl


class SchemaMapperAgent:
    NAME = "2. Schema Mapper"

    @staticmethod
    def run(context, field_map: dict):
        t0 = time.perf_counter()
        mapped_df, unmapped = pl.apply_schema_map(context.source_df, field_map)
        context.field_map = field_map
        context.mapped_df = mapped_df
        context.unmapped_cols = unmapped

        status = "WARNING" if unmapped else "OK"
        detail = f"Mapped {len(mapped_df.columns)} field(s)"
        if unmapped:
            detail += f"; unmapped/excluded: {unmapped}"
        context.log(SchemaMapperAgent.NAME, status, detail, (time.perf_counter() - t0) * 1000)
        return context
