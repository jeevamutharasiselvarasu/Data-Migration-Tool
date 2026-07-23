"""4. Compliance Validator — reapplies the Home Office overlay (account
type / model type / asset class / custodian normalization, drift tolerance
defaults, Rep-as-PM Core Model registration rule) and runs the compliance
checks: no-loss check vs source, unknown code flags, pending registrations.
BLOCKED status stops the run before it can reach the Loader Agent."""

import time

import pipeline as pl


class ComplianceValidatorAgent:
    NAME = "4. Compliance Validator"

    @staticmethod
    def run(context, registered_models: set):
        t0 = time.perf_counter()
        base_df = context.transformed_df if context.transformed_df is not None else context.mapped_df

        store = getattr(context, "knowledge_store", None)
        extra_acct_types = store.account_type_overrides() if store else None
        extra_custodians = store.known_custodians() if store else None

        t, notes = pl.compliance_transform(base_df, registered_models, extra_account_type_map=extra_acct_types)
        context.transformed_df = t
        context.compliance_notes = notes

        cfg = getattr(context, "sleeve_math_config", None) or {}
        errors = pl.validate_compliance(
            t,
            notes["pending_model_registration"],
            group_key=cfg.get("group_key", "account_number"),
            sum_tolerance=cfg.get("tolerance", 0.5),
            extra_known_custodians=extra_custodians,
        )
        context.compliance_errors = errors

        has_error = any(e["severity"] == "ERROR" for e in errors)
        status = "BLOCKED" if has_error else ("WARNING" if errors else "OK")
        detail = f"{len(errors)} compliance finding(s)" if errors else "No compliance findings"
        if notes["pending_model_registration"]:
            detail += f"; pending Core Model registration: {notes['pending_model_registration']}"
        if notes.get("learned_account_types_applied"):
            detail += f"; applied learned account-type rule(s): {notes['learned_account_types_applied']}"
        context.log(ComplianceValidatorAgent.NAME, status, detail, (time.perf_counter() - t0) * 1000)
        return context
