"""Shared Knowledge Layer — the versioned mapping rule catalog, code
crosswalks, and Core Models registry access, sourced from pipeline.py so
there is a single copy of each business rule. Agents and the front end
should read crosswalks/field maps through this module."""

import pipeline as pl

# Mapping Rule Catalog — versioned per entity via pipeline.ENTITY_CONFIGS.
FIELD_MAPS = {key: config["field_map"] for key, config in pl.ENTITY_CONFIGS.items()}

# Code crosswalks (Amplify -> Orion Eclipse), per field_glossary.md.
CODE_CROSSWALKS = {
    "account_type": pl.ACCOUNT_TYPE_MAP,
    "model_type": pl.MODEL_TYPE_MAP,
    "asset_class": pl.ASSET_CLASS_MAP,
}

KNOWN_CUSTODIAN_CODES = pl.KNOWN_CUSTODIAN_CODES

# Entity hierarchy / load-dependency order, per entity_hierarchy.md.
ENTITY_PARENTS = pl.ENTITY_PARENTS


def entity_config(entity_key: str) -> dict:
    return pl.ENTITY_CONFIGS[entity_key]


def registered_core_models(registry_path: str) -> set:
    """Core Models registry — the security/product base that advisor-built
    (Rep-as-PM) models must be registered in before their sleeves can load."""
    return pl.load_registered_models(registry_path)
