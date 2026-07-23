"""3. Sleeve Math Agent — translates UMA sleeves into Portfolio + Model
Aggregate + Composition %, and (more generally) applies whichever
transformation logic the Transformation Logic tab is configured for:

  mode="toggles"  Deterministic built-in rules — rule 1 (sum to 100% per
                  group) and rule 2 (exclude zero-allocation sleeves), each
                  independently toggleable and parameterized.
  mode="layers"   A no-code stack of Rule Builder layers (filter / normalize /
                  fill default / map values / compute), applied top to bottom.
  mode="custom"   An advanced user-supplied Python function that fully
                  replaces the above.

Layers and custom rules aren't limited to sleeve_allocation_pct — they can
touch any mapped column — so this agent doubles as the general "apply my
transformation logic" step for entities that have no sleeve data at all.
"""

import time

import pipeline as pl


class SleeveMathAgent:
    NAME = "3. Sleeve Math Agent"

    @staticmethod
    def run(context, config: dict = None):
        t0 = time.perf_counter()
        cfg = config if config is not None else context.sleeve_math_config
        context.sleeve_math_config = cfg

        df = context.mapped_df
        mode = cfg.get("mode", "toggles")
        group_key = cfg.get("group_key", "account_number")

        if mode == "toggles" and ("sleeve_allocation_pct" not in df.columns or group_key not in df.columns):
            context.transformed_df = df.copy()
            context.sleeve_notes = {"assumptions": [], "custom_rule_used": False, "layers_applied": []}
            context.log(
                SleeveMathAgent.NAME,
                "SKIPPED",
                f"No 'sleeve_allocation_pct' + '{group_key}' columns in this batch — nothing to normalize",
                (time.perf_counter() - t0) * 1000,
            )
            return context

        try:
            if mode == "custom":
                t, notes = pl.sleeve_math(df, custom_code=cfg.get("custom_code"))
            elif mode == "layers":
                t, notes = pl.apply_rule_layers(df, cfg.get("layers", []))
                notes.setdefault("custom_rule_used", False)
            else:
                t, notes = pl.sleeve_math(
                    df,
                    normalize=cfg.get("normalize", True),
                    exclude_zero=cfg.get("exclude_zero", True),
                    zero_threshold=cfg.get("zero_threshold", 0.0),
                    group_key=group_key,
                )
        except ValueError as exc:
            context.log(SleeveMathAgent.NAME, "BLOCKED", f"Transformation logic error ({mode}): {exc}", (time.perf_counter() - t0) * 1000)
            raise

        context.transformed_df = t
        context.sleeve_notes = notes

        if mode == "custom":
            detail = "Custom Sleeve Math rule applied"
        elif mode == "layers":
            detail = f"{len(notes.get('layers_applied', []))} rule-builder layer(s) applied"
        else:
            parts = [
                (
                    f"normalized to 100% across {t[group_key].nunique()} group(s)"
                    if cfg.get("normalize", True)
                    else "normalization disabled"
                ),
                (
                    f"excluded allocations <= {cfg.get('zero_threshold', 0.0)}"
                    if cfg.get("exclude_zero", True)
                    else "zero-allocation exclusion disabled"
                ),
            ]
            detail = "; ".join(parts)

        if notes.get("assumptions"):
            detail += "; " + "; ".join(notes["assumptions"])
        if notes.get("layers_applied") and mode == "layers":
            detail += " — " + "; ".join(notes["layers_applied"])

        context.log(SleeveMathAgent.NAME, "OK", detail, (time.perf_counter() - t0) * 1000)
        return context
