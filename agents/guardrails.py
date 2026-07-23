"""Guardrails & Policies — cross-cutting checks every agent run is subject
to: deterministic math invariants must hold before the Loader Agent may
write anything, HITL approval is required before load, and PII-ish columns
are masked in evidence/audit artifacts (never in the actual destination
data itself)."""

import pandas as pd


class GuardrailViolation(Exception):
    """Raised when a deterministic invariant or policy is violated. The
    Orchestrator treats this as BLOCKED and refuses to run the Loader Agent."""


def enforce_pre_load(context) -> None:
    violations = []
    df = context.transformed_df
    cfg = getattr(context, "sleeve_math_config", None) or {}
    group_key = cfg.get("group_key", "account_number")
    tolerance = cfg.get("tolerance", 0.5)

    if df is not None and "sleeve_allocation_pct" in df.columns and group_key in df.columns:
        sums = df.groupby(group_key)["sleeve_allocation_pct"].sum()
        bad = sums[(sums - 100).abs() > tolerance]
        if not bad.empty:
            violations.append(f"sleeve allocations not within tolerance ({tolerance} pts) of 100% for group(s): {bad.index.tolist()}")

    if df is not None and "assets_under_management" in df.columns:
        neg = df[df["assets_under_management"] < 0]
        if not neg.empty:
            violations.append(f"{len(neg)} row(s) with negative AUM survived the Compliance Validator")

    if context.decision != "APPROVED":
        violations.append("no HITL approval recorded on context — refusing to load")

    if violations:
        raise GuardrailViolation("; ".join(violations))


# Columns treated as PII-ish for evidence/audit redaction purposes only.
PII_COLUMNS = ["first_name", "last_name", "primary_client_name", "advisor_name", "client_id", "primary_client_id"]


def redact_pii(df: pd.DataFrame) -> pd.DataFrame:
    """Mask PII-ish columns before writing sample rows into audit/evidence
    artifacts. Never applied to the destination file that is actually loaded."""
    if df is None:
        return df
    redacted = df.copy()
    for col in PII_COLUMNS:
        if col in redacted.columns:
            redacted[col] = redacted[col].astype(str).apply(
                lambda v: (v[0] + "***") if v and v.lower() != "nan" else v
            )
    return redacted
