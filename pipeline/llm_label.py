"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` was the NAIVE version (a call per ticket per run,
writing whatever came back); it now follows the four rules above, so
`python -m scripts.bonus_llm` prints BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import json
import re

import duckdb

from .embed import text_hash

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Pull {"label": ...} out of the model's answer; None if it is not valid."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return None
    try:
        label = json.loads(m.group(0)).get("label")
    except json.JSONDecodeError:
        return None
    return label if label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """Cached, validated LLM labelling.

    Every answer (valid or not) is cached under hash(input) + model + prompt version,
    so a re-run makes 0 calls and a new prompt version re-labels everything on purpose.
    Gold only gets answers that parse to an allowed label; the rest are quarantined.
    """
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR,
        raw_answer VARCHAR, label VARCHAR)""")
    tickets = live_tickets(con)
    keyed = [(ticket_id, text, text_hash(text)) for ticket_id, text in tickets]

    cached = {h for (h,) in con.execute(
        "SELECT input_hash FROM llm_label_cache WHERE model = ? AND prompt_version = ?",
        [MODEL, PROMPT_VERSION]).fetchall()}
    todo = {h: text for _, text, h in keyed if h not in cached}

    # Rule 3: estimate the cost of what we are about to send, BEFORE sending it.
    est_tokens = estimate_tokens(list(todo.values()))
    est_usd = est_tokens / 1000 * PRICE_PER_1K_TOKENS_USD

    calls_before = llm.calls
    new_rows = []
    for h, text in sorted(todo.items()):
        raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
        new_rows.append((h, MODEL, PROMPT_VERSION, raw, parse_label(raw)))
    if new_rows:
        con.executemany("INSERT INTO llm_label_cache VALUES (?, ?, ?, ?, ?)", new_rows)

    # Rebuild both outputs from the cache for the current model + prompt version.
    con.execute("CREATE OR REPLACE TEMP TABLE _live (ticket_id VARCHAR, input_hash VARCHAR)")
    if keyed:
        con.executemany("INSERT INTO _live VALUES (?, ?)", [(t, h) for t, _, h in keyed])
    current = """FROM _live l JOIN llm_label_cache c
                 ON c.input_hash = l.input_hash AND c.model = ? AND c.prompt_version = ?"""
    con.execute(f"""CREATE OR REPLACE TABLE gold_ticket_labels AS
        SELECT l.ticket_id, c.label, c.model, c.prompt_version {current}
        WHERE c.label IS NOT NULL ORDER BY l.ticket_id""", [MODEL, PROMPT_VERSION])
    con.execute(f"""CREATE OR REPLACE TABLE llm_label_quarantine AS
        SELECT l.ticket_id, c.raw_answer, 'off-schema answer' AS reason,
               c.model, c.prompt_version {current}
        WHERE c.label IS NULL ORDER BY l.ticket_id""", [MODEL, PROMPT_VERSION])

    (labeled,) = con.execute("SELECT count(*) FROM gold_ticket_labels").fetchone()
    (quarantined,) = con.execute("SELECT count(*) FROM llm_label_quarantine").fetchone()
    return {"labeled": labeled, "quarantined": quarantined, "calls": llm.calls - calls_before,
            "cache_hits": len(keyed) - len(todo),
            "estimated_tokens": est_tokens, "estimated_usd": est_usd}
