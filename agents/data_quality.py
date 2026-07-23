"""5. Data Quality — asserts cross-entity integrity (Great-Expectations
style): required fields, numeric/non-negative AUM, duplicate destination
keys, and orphan foreign keys against the most recently loaded parent
entity (per entity_hierarchy.md load order). Blocks load on critical (ERROR)
findings."""

import time

import pipeline as pl


class DataQualityAgent:
    NAME = "5. Data Quality"

    @staticmethod
    def run(context, required_fields=None, output_dir: str = None):
        t0 = time.perf_counter()
        df = context.transformed_df
        config = pl.ENTITY_CONFIGS[context.entity_key]

        errors = pl.validate_data_quality(df, required_fields)

        dup = pl.check_duplicates(df, config["loaded_key"])
        if dup:
            errors.append(dup)

        if output_dir:
            for fk_field, parent_entity in pl.ENTITY_PARENTS.get(context.entity_key, []):
                parent_ids = pl.load_parent_ids(output_dir, parent_entity)
                orphan = pl.check_orphans(df, fk_field, parent_ids, parent_entity)
                if orphan:
                    errors.append(orphan)

        context.dq_errors = errors
        context.passed = not any(e["severity"] == "ERROR" for e in (errors + context.compliance_errors))

        has_error = any(e["severity"] == "ERROR" for e in errors)
        status = "BLOCKED" if has_error else ("WARNING" if errors else "OK")
        detail = f"{len(errors)} data quality finding(s)" if errors else "No data quality findings"
        context.log(DataQualityAgent.NAME, status, detail, (time.perf_counter() - t0) * 1000)
        return context
