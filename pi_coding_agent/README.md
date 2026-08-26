# stitcher-pi — minimal pi coding agent

An **extremely thin** pi coding agent wrapper. Pi ([@earendil-works/pi-coding-agent])
is the loop. Two small pieces wrap it:

1. **`mcp_server.py`** — a FastMCP tool server. *The only place you add tools*:
   add an `@mcp.tool` function and it is exposed to the agent automatically.
2. **`pi_extension/`** — a pi extension that (a) registers the **Stitcher LLM
   endpoint** as a provider from env vars and (b) **discovers every FastMCP tool**
   over MCP and proxies it into pi. No per-tool pi code.

```
pi (the loop) ──MCP──▶ mcp_server.py (FastMCP, thin)
   stitcher model         your tools live here
```

## Run

```bash
export STITCHER_MODEL_BASE_URL=https://app.dev.stitcher.ai/llm/v1
export STITCHER_MODEL_API_KEY=sk-...            # required
export STITCHER_MODEL_NAME=qwen3.6-27b-mtp      # optional, this is the default
./run.sh
```

One-time setup: `(cd pi_extension && npm ci)` for the MCP SDK, and `pi` on PATH
(`npm i -g @earendil-works/pi-coding-agent`). The FastMCP server runs from a venv
that has `fastmcp` (default `../stitcher_mcp_service/.venv`).

`run.sh` starts the FastMCP server over Streamable HTTP (default port 8791) and
execs `pi --model stitcher/<model> -e ./pi_extension/index.ts`. You can also run
the server for other transports:

```bash
python mcp_server.py               # stdio
python mcp_server.py --http 8791   # Streamable HTTP (what run.sh uses)
```

## Env vars

| Var | Default | Purpose |
|---|---|---|
| `STITCHER_MODEL_BASE_URL` | `https://app.dev.stitcher.ai/llm/v1` | LiteLLM / OpenAI-compatible base URL |
| `STITCHER_MODEL_API_KEY` | *(required)* | gateway key (pi interpolates its `$STITCHER_MODEL_API_KEY` reference) |
| `STITCHER_MODEL_NAME` | `qwen3.6-27b-mtp` | model id (must exist as an alias on the gateway) |
| `STITCHER_MCP_PORT` | `8791` | FastMCP HTTP port |
| `STITCHER_PY` | `../stitcher_mcp_service/.venv/bin/python` | python with `fastmcp` |

## Adding a tool

Edit `mcp_server.py`:

```python
@mcp.tool
def my_tool(arg: str) -> str:
    """What it does."""
    return something(arg)
```

That's it — the extension re-discovers tools on the next launch. All determinism
belongs server-side; pi only makes the calls.
