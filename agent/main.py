"""
AI Crypto Analytics Agent — FastAPI + OpenRouter + ClickHouse tool use.

POST /chat  — accepts {message, history} → returns {response}
GET  /      — health check + single-page chat UI
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from openai import OpenAI
from pydantic import BaseModel

log = logging.getLogger("agent")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Config ───────────────────────────────────────────────────────────────────
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "crypto")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
MODEL = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini")

# ─── ClickHouse client ────────────────────────────────────────────────────────
def _ch():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        username="default",
        password="",
    )


def _get_table_schemas() -> str:
    """Return CREATE TABLE statements for all mart tables as a string."""
    try:
        ch = _ch()
        tables = ch.query("SHOW TABLES FROM crypto").result_rows
        parts = []
        for (tbl,) in tables:
            ddl = ch.query(f"SHOW CREATE TABLE crypto.{tbl}").result_rows[0][0]
            parts.append(ddl)
        return "\n\n".join(parts)
    except Exception as exc:
        log.warning("Could not fetch schemas: %s", exc)
        return "(schemas unavailable — ClickHouse may still be initializing)"


# Fetch schemas once at startup
TABLE_SCHEMAS = _get_table_schemas()

# ─── OpenAI / OpenRouter client ───────────────────────────────────────────────
openai_client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
    default_headers={
        "HTTP-Referer": "http://localhost:8000",
        "X-Title": "Crypto Analytics Agent",
    },
)

# ─── Tool definitions ─────────────────────────────────────────────────────────
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": (
                "Execute a read-only SELECT query against ClickHouse and return "
                "the result rows as a JSON array (max 500 rows). "
                "Always use fully-qualified table names: crypto.<table>."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "A valid ClickHouse SELECT statement.",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": (
                "Return the names and CREATE TABLE DDL for all tables in the "
                "crypto database. Use this to understand available data before "
                "writing queries."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]


def run_sql(query: str) -> str:
    """Execute a SELECT on ClickHouse; return JSON rows."""
    q = query.strip()
    upper = q.upper()
    # Safety: reject non-SELECT statements
    if not upper.startswith("SELECT") and not upper.startswith("WITH"):
        return json.dumps({"error": "Only SELECT queries are allowed."})
    try:
        ch = _ch()
        result = ch.query(q)
        columns = result.column_names
        rows = [dict(zip(columns, row)) for row in result.result_rows[:500]]
        return json.dumps(rows, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def list_tables() -> str:
    return TABLE_SCHEMAS or "(no tables found)"


def dispatch_tool(name: str, args: dict) -> str:
    if name == "run_sql":
        return run_sql(args.get("query", ""))
    if name == "list_tables":
        return list_tables()
    return json.dumps({"error": f"Unknown tool: {name}"})


# ─── System prompt ────────────────────────────────────────────────────────────
def build_system_prompt() -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"""You are a crypto analytics assistant with access to a ClickHouse data warehouse.
Current date/time: {now}

DATABASE SCHEMA:
{TABLE_SCHEMAS}

INSTRUCTIONS:
- Always respond in Russian language.
- Always verify your answers with a SQL query before responding.
- Use fully-qualified table names: crypto.<table_name>.
- ClickHouse uses DateTime (not TIMESTAMP) and date_trunc, toStartOfHour, etc.
- For "latest" data, use MAX(hour) or ORDER BY … DESC LIMIT n.
- Keep responses concise but include the key numbers from your query results.
- If data is not yet available (tables are empty), say so clearly.
"""


# ─── FastAPI app ──────────────────────────────────────────────────────────────
app = FastAPI(title="Crypto Analytics AI Agent")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log.error("Unhandled exception: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )


class ChatRequest(BaseModel):
    message: str
    history: list[dict[str, Any]] = []


class ChatResponse(BaseModel):
    response: str


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    if not OPENROUTER_API_KEY:
        raise HTTPException(status_code=503, detail="OPENROUTER_API_KEY not configured")
    try:
        return await _chat_impl(req)
    except Exception as exc:
        log.error("Chat error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Agent error: {exc}")


async def _chat_impl(req: ChatRequest) -> ChatResponse:
    messages = [{"role": "system", "content": build_system_prompt()}]
    # Append history (role: user/assistant, content: str)
    for h in req.history[-20:]:  # keep last 20 turns
        messages.append({"role": h.get("role", "user"), "content": h.get("content", "")})
    messages.append({"role": "user", "content": req.message})

    # Tool execution loop
    for iteration in range(10):  # max 10 tool calls per request
        completion = openai_client.chat.completions.create(
            model=MODEL,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
        )
        msg = completion.choices[0].message

        # No tool calls → final answer
        if not msg.tool_calls:
            return ChatResponse(response=msg.content or "")

        # Append assistant message with tool_calls (manual build for openai compat)
        assistant_msg: dict[str, Any] = {"role": "assistant", "content": msg.content or ""}
        assistant_msg["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {"name": tc.function.name, "arguments": tc.function.arguments},
            }
            for tc in msg.tool_calls
        ]
        messages.append(assistant_msg)

        # Execute each tool call
        for tc in msg.tool_calls:
            fn_name = tc.function.name
            try:
                fn_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                fn_args = {}

            log.info("Tool call: %s(%s)", fn_name, list(fn_args.keys()))
            result = dispatch_tool(fn_name, fn_args)

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

    return ChatResponse(response="I was unable to complete the request after multiple tool calls.")


# ─── Chat UI ──────────────────────────────────────────────────────────────────
CHAT_UI = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Crypto Analytics AI Agent</title>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Segoe UI', system-ui, sans-serif;
    background: #0f1117;
    color: #e2e8f0;
    height: 100vh;
    display: flex;
    flex-direction: column;
  }
  header {
    background: #1a1d2e;
    padding: 16px 24px;
    border-bottom: 1px solid #2d3748;
    display: flex;
    align-items: center;
    gap: 12px;
  }
  header h1 { font-size: 1.2rem; font-weight: 600; color: #63b3ed; }
  header .badge {
    background: #2d3748;
    color: #a0aec0;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 0.75rem;
  }
  #chat-container {
    flex: 1;
    overflow-y: auto;
    padding: 20px 16px;
    display: flex;
    flex-direction: column;
    gap: 12px;
    max-width: 860px;
    width: 100%;
    margin: 0 auto;
  }
  .message {
    display: flex;
    flex-direction: column;
    max-width: 80%;
    animation: fadeIn 0.2s ease;
  }
  @keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; } }
  .message.user { align-self: flex-end; align-items: flex-end; }
  .message.assistant { align-self: flex-start; align-items: flex-start; }
  .bubble {
    padding: 12px 16px;
    border-radius: 16px;
    line-height: 1.6;
    font-size: 0.95rem;
  }
  .user .bubble {
    background: #3182ce;
    color: #fff;
    border-bottom-right-radius: 4px;
  }
  .assistant .bubble {
    background: #1a1d2e;
    border: 1px solid #2d3748;
    color: #e2e8f0;
    border-bottom-left-radius: 4px;
  }
  .assistant .bubble pre {
    background: #0f1117;
    border: 1px solid #2d3748;
    border-radius: 8px;
    padding: 10px 14px;
    overflow-x: auto;
    margin: 8px 0;
    font-size: 0.85rem;
  }
  .assistant .bubble code { font-family: 'Fira Code', monospace; font-size: 0.85em; }
  .assistant .bubble p { margin: 6px 0; }
  .assistant .bubble table {
    border-collapse: collapse;
    width: 100%;
    margin: 8px 0;
  }
  .assistant .bubble th, .assistant .bubble td {
    border: 1px solid #2d3748;
    padding: 6px 10px;
    text-align: left;
  }
  .assistant .bubble th { background: #2d3748; }
  .label { font-size: 0.7rem; color: #718096; margin-bottom: 4px; }
  .spinner {
    display: flex;
    align-items: center;
    gap: 8px;
    color: #718096;
    font-size: 0.85rem;
  }
  .dots span {
    animation: blink 1.2s infinite;
    display: inline-block;
  }
  .dots span:nth-child(2) { animation-delay: 0.2s; }
  .dots span:nth-child(3) { animation-delay: 0.4s; }
  @keyframes blink { 0%,80%,100% { opacity: 0.2; } 40% { opacity: 1; } }
  #input-area {
    background: #1a1d2e;
    border-top: 1px solid #2d3748;
    padding: 16px;
  }
  #input-row {
    display: flex;
    gap: 8px;
    max-width: 860px;
    margin: 0 auto;
  }
  #user-input {
    flex: 1;
    background: #0f1117;
    border: 1px solid #2d3748;
    color: #e2e8f0;
    padding: 12px 16px;
    border-radius: 12px;
    font-size: 0.95rem;
    resize: none;
    min-height: 48px;
    max-height: 160px;
    font-family: inherit;
  }
  #user-input:focus { outline: none; border-color: #3182ce; }
  #send-btn {
    background: #3182ce;
    color: #fff;
    border: none;
    border-radius: 12px;
    padding: 12px 20px;
    cursor: pointer;
    font-size: 0.95rem;
    font-weight: 500;
    transition: background 0.15s;
    white-space: nowrap;
  }
  #send-btn:hover { background: #2b6cb0; }
  #send-btn:disabled { background: #2d3748; cursor: not-allowed; }
  .examples {
    max-width: 860px;
    margin: 0 auto 8px;
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }
  .example-chip {
    background: #1a1d2e;
    border: 1px solid #2d3748;
    color: #a0aec0;
    padding: 5px 12px;
    border-radius: 20px;
    font-size: 0.8rem;
    cursor: pointer;
    transition: border-color 0.15s, color 0.15s;
  }
  .example-chip:hover { border-color: #3182ce; color: #63b3ed; }
</style>
</head>
<body>
<header>
  <h1>&#8377; Crypto Analytics AI</h1>
  <span class="badge">DeepSeek via OpenRouter</span>
</header>

<div id="chat-container">
  <div class="message assistant">
    <div class="label">Ассистент</div>
    <div class="bubble">
      Привет! Я могу отвечать на вопросы о крипторынке в реальном времени.
      Спрашивайте о ценах, объёмах торгов, доминации или лидерах роста.
    </div>
  </div>
</div>

<div id="input-area">
  <div class="examples">
    <span class="example-chip" onclick="fillInput(this)">Какая монета выросла больше всего за 24ч?</span>
    <span class="example-chip" onclick="fillInput(this)">Какая сейчас доминация Bitcoin?</span>
    <span class="example-chip" onclick="fillInput(this)">Покажи топ-5 монет по объёму торгов</span>
    <span class="example-chip" onclick="fillInput(this)">Какая была максимальная цена Bitcoin сегодня?</span>
  </div>
  <div id="input-row">
    <textarea id="user-input" placeholder="Задайте вопрос о крипторынке…" rows="1"></textarea>
    <button id="send-btn" onclick="sendMessage()">Отправить</button>
  </div>
</div>

<script>
const history = [];
const container = document.getElementById('chat-container');
const input = document.getElementById('user-input');
const btn = document.getElementById('send-btn');

input.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
});
input.addEventListener('input', () => {
  input.style.height = 'auto';
  input.style.height = Math.min(input.scrollHeight, 160) + 'px';
});

function fillInput(el) { input.value = el.textContent; input.focus(); }

function addMessage(role, content, isSpinner = false) {
  const div = document.createElement('div');
  div.className = `message ${role}`;
  const label = document.createElement('div');
  label.className = 'label';
  label.textContent = role === 'user' ? 'Вы' : 'Ассистент';
  div.appendChild(label);
  const bubble = document.createElement('div');
  bubble.className = 'bubble';
  if (isSpinner) {
    bubble.innerHTML = '<div class="spinner">Думаю<div class="dots"><span>.</span><span>.</span><span>.</span></div></div>';
    div.dataset.spinner = 'true';
  } else {
    bubble.innerHTML = role === 'assistant' ? marked.parse(content) : escapeHtml(content);
  }
  div.appendChild(bubble);
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
  return div;
}

function escapeHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function removeSpinner() {
  const el = container.querySelector('[data-spinner]');
  if (el) el.remove();
}

async function sendMessage() {
  const text = input.value.trim();
  if (!text) return;
  input.value = '';
  input.style.height = 'auto';
  btn.disabled = true;

  addMessage('user', text);
  const spinner = addMessage('assistant', '', true);

  try {
    const res = await fetch('/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, history }),
    });
    removeSpinner();
    if (!res.ok) {
      let errMsg = res.statusText;
      try { const err = await res.json(); errMsg = err.detail || errMsg; } catch {}
      addMessage('assistant', `Error ${res.status}: ${errMsg}`);
    } else {
      const data = await res.json();
      addMessage('assistant', data.response);
      history.push({ role: 'user', content: text });
      history.push({ role: 'assistant', content: data.response });
      if (history.length > 40) history.splice(0, 2);
    }
  } catch (err) {
    removeSpinner();
    addMessage('assistant', `Network error: ${err.message}`);
  } finally {
    btn.disabled = false;
    input.focus();
  }
}
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return CHAT_UI
