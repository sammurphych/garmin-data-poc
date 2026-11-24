"""Ask questions about your health data with a local model (llama.cpp) and the Cube tools.

The model gets the same `query` tool the MCP server gives Claude, so the two can be
compared on the same questions. The semantic layer description goes in the system
prompt up front, which saves a slow tool round-trip on CPU-only machines.

Start the model server first (see README "Local model"), then:

    uv run --group mcp python semantic/local_chat.py                 # interactive
    uv run --group mcp python semantic/local_chat.py "question?"     # one question

LLM_BASE_URL defaults to llama-server's OpenAI-compatible API on localhost:8080.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import date
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mcp_server  # noqa: E402  (shares .env loading, auth and the Cube calls)

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://localhost:8080/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "local")  # llama-server serves one model and ignores the name
MAX_TOOL_ROUNDS = 5
MAX_TOOL_RESULT_CHARS = 4000  # keeps prompts short; CPU prompt processing is the slow part

QUERY_TOOL = {
    "type": "function",
    "function": {
        "name": "query",
        "description": "Run a query against the semantic layer and return rows as JSON.",
        "parameters": {
            "type": "object",
            "properties": {
                "measures": {"type": "array", "items": {"type": "string"},
                             "description": "e.g. [\"health.avg_resting_hr\"]"},
                "dimensions": {"type": "array", "items": {"type": "string"},
                               "description": "e.g. [\"training.activity_type\"]"},
                "time_dimension": {"type": "string", "description": "e.g. \"health.calendar_date\""},
                "granularity": {"type": "string", "enum": ["day", "week", "month", "quarter", "year"]},
                "date_range": {"description": "[\"2026-10-01\", \"2026-10-31\"] or a phrase like \"last 7 days\"",
                               "anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]},
                "filters": {"type": "array", "items": {"type": "object"},
                            "description": "e.g. [{\"member\": \"training.activity_type\", \"operator\": \"equals\", \"values\": [\"running\"]}]"},
                "order": {"type": "object", "description": "e.g. {\"health.calendar_date\": \"asc\"}"},
                "limit": {"type": "integer"},
            },
            "required": ["measures"],
        },
    },
}


def _compact_semantic_layer() -> str:
    lines = []
    for view in json.loads(mcp_server.describe_semantic_layer()):
        lines.append(f"\nview {view['view']}: {view['description']}")
        for m in view["measures"]:
            lines.append(f"  measure {m['name']}" + (f" - {m['description']}" if m.get("description") else ""))
        for d in view["dimensions"]:
            lines.append(f"  dimension {d['name']} ({d['type']})" + (f" - {d['description']}" if d.get("description") else ""))
    return "\n".join(lines)


def system_prompt() -> str:
    return (
        f"You answer questions about the user's personal health data. Today is {date.today().isoformat()}.\n"
        "Always get numbers by calling the `query` tool; never guess them. Use only the member names listed "
        "below, exactly as written. Time-based questions need time_dimension and date_range. "
        "Answer in one to three short sentences with the key numbers, rounded sensibly.\n"
        f"Semantic layer:{_compact_semantic_layer()}"
    )


def _chat(messages: list[dict]) -> dict:
    resp = httpx.post(f"{LLM_BASE_URL}/chat/completions", timeout=600, json={
        "model": LLM_MODEL, "messages": messages, "tools": [QUERY_TOOL], "temperature": 0.2,
    })
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]


def ask(question: str, messages: list[dict]) -> str:
    messages.append({"role": "user", "content": question})
    for _ in range(MAX_TOOL_ROUNDS):
        reply = _chat(messages)
        messages.append(reply)
        calls = reply.get("tool_calls") or []
        if not calls:
            return reply.get("content") or ""
        for call in calls:
            try:
                args = json.loads(call["function"]["arguments"] or "{}")
                print(f"  [query] {json.dumps(args)}", file=sys.stderr)
                result = mcp_server.query(**args)
            except Exception as exc:  # bad arguments from the model go back to it as an error
                result = json.dumps({"error": str(exc)})
            messages.append({"role": "tool", "tool_call_id": call.get("id", ""),
                             "content": result[:MAX_TOOL_RESULT_CHARS]})
    return "(stopped after too many tool calls)"


def main() -> None:
    messages = [{"role": "system", "content": system_prompt()}]
    questions = sys.argv[1:]
    if questions:
        start = time.monotonic()
        print(ask(" ".join(questions), messages))
        print(f"({time.monotonic() - start:.0f}s)", file=sys.stderr)
        return
    print("Ask about your health data (Ctrl+D to quit).")
    while True:
        try:
            question = input("\n> ").strip()
        except EOFError:
            break
        if question:
            start = time.monotonic()
            print(ask(question, messages))
            print(f"({time.monotonic() - start:.0f}s)", file=sys.stderr)


if __name__ == "__main__":
    main()
