"""Optional Claude reasoning layer — the *only* place in this project that can
call an LLM, and it is entirely opt-in.

It activates when, and only when, an ``ANTHROPIC_API_KEY`` environment
variable is present AND the ``anthropic`` package is installed. With no key
(the default), every function here is a graceful no-op that returns ``None``,
so the whole app runs fully offline and deterministically — the local
KnowledgeStore does the "learning" without any network calls.

Nothing in the core pipeline depends on this module returning a value: LLM
output is only ever used to *suggest* or *explain*, never to decide. A human
still approves every load, and the deterministic guardrails still run
regardless of anything an LLM says.

Setup — put the key in a .env file (never commit it):
    1. Copy .env.example to .env
    2. Put your key in it:  ANTHROPIC_API_KEY=sk-ant-...
    3. Restart the app (or `docker compose up`)

The .env file is git-ignored and Docker-ignored, so the key never lands in a
commit or a built image. For local `streamlit run`, this module auto-loads
.env below; for Docker, docker-compose.yml reads it. A plain shell
`export ANTHROPIC_API_KEY=...` also works and takes precedence.
"""

import json
import os


def _load_dotenv() -> None:
    """Minimal, dependency-free .env loader. Reads KEY=VALUE lines from a .env
    file at the project root and sets them in os.environ WITHOUT overriding any
    variable already set in the real environment (so a shell export or Docker
    env always wins). Silently does nothing if there's no .env file."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, ".env")
    if not os.path.exists(path):
        return
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        pass


_load_dotenv()

MODEL = os.environ.get("MIGRATION_LLM_MODEL", "claude-sonnet-5")


def is_enabled() -> bool:
    """True only if a key is set and the SDK is importable."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return False
    try:
        import anthropic  # noqa: F401
        return True
    except ImportError:
        return False


def status_note() -> str:
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            import anthropic  # noqa: F401
            return "Claude reasoning: ENABLED (ANTHROPIC_API_KEY found, anthropic SDK installed)."
        except ImportError:
            return "Claude reasoning: key found but the 'anthropic' package isn't installed — running offline. `pip install anthropic` to enable."
    return "Claude reasoning: OFF (no ANTHROPIC_API_KEY). Everything runs locally; the memory store still learns."


def _client():
    import anthropic
    return anthropic.Anthropic()


def _ask(prompt: str, max_tokens: int = 600) -> str | None:
    """Single-shot completion. Returns None on any failure so callers can
    always fall back to the deterministic path."""
    if not is_enabled():
        return None
    try:
        client = _client()
        resp = client.messages.create(
            model=MODEL,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(block.text for block in resp.content if getattr(block, "type", None) == "text").strip()
    except Exception:
        return None


def explain_findings(entity_key: str, compliance_errors: list, dq_errors: list) -> str | None:
    """Plain-language explanation of what the validation findings mean and how
    to resolve them. Returns None when Claude is off."""
    if not is_enabled():
        return None
    findings = {"compliance": compliance_errors, "data_quality": dq_errors}
    prompt = (
        "You are a data-migration reviewer for a wealth-management platform "
        "(source -> destination). A batch of "
        f"'{entity_key}' records produced these validation findings:\n\n"
        f"{json.dumps(findings, indent=2, default=str)}\n\n"
        "In under 180 words, plain language, no preamble: explain what these "
        "findings mean for the migration and the concrete next step for each. "
        "Do not invent findings that aren't listed."
    )
    return _ask(prompt)


def propose_mapping(entity_key: str, unmapped_columns: list, destination_fields: list) -> dict | None:
    """Suggest source-column -> destination-field pairings for columns the
    Schema Mapper couldn't place. Returns a dict, or None when Claude is off
    or the response can't be parsed."""
    if not is_enabled() or not unmapped_columns:
        return None
    prompt = (
        f"Map these source columns from a '{entity_key}' extract to the most "
        "likely destination field. Source columns:\n"
        f"{json.dumps(list(unmapped_columns))}\n\n"
        "Available destination fields:\n"
        f"{json.dumps(list(destination_fields))}\n\n"
        "Reply with ONLY a JSON object of {source_column: destination_field} "
        "for columns you're confident about; omit any you're unsure of. No prose."
    )
    raw = _ask(prompt, max_tokens=400)
    if not raw:
        return None
    try:
        start, end = raw.find("{"), raw.rfind("}")
        if start == -1 or end == -1:
            return None
        parsed = json.loads(raw[start : end + 1])
        return {str(k): str(v) for k, v in parsed.items() if v in destination_fields}
    except (json.JSONDecodeError, ValueError):
        return None
