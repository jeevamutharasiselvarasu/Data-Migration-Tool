"""
Multi-agent implementation of the Agentic Migration Pipeline
(Amplify Rep-as-PM -> Orion Eclipse), mirroring the architecture diagram:

  Orchestrator (Conductor)
    1. Extractor Agent
    2. Schema Mapper
    3. Sleeve Math Agent
    4. Compliance Validator
    5. Data Quality
    -- Human-in-the-Loop Gate --
    6. Loader Agent
    7. Reconciler Agent
    8. Cutover Agent            (on reconciliation PASS)
       Rollback Path            (on reconciliation breach)
    9. Audit & Evidence Agent

Shared knowledge (mapping rule catalog, code crosswalks, Core Models
registry) lives in knowledge.py; cross-cutting guardrails live in
guardrails.py. Everything is coordinated through orchestrator.py.
"""
