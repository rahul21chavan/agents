# backend/agents/llm_rule_agent_enhanced.py
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Dict, List
from datetime import datetime

import pandas as pd
from langchain_openai import AzureChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate

# ─────────────── paths & dirs ───────────────
BASE_DIR = Path(__file__).resolve().parent.parent
RULE_DIR = BASE_DIR / "rule_outputs"
RULE_DIR.mkdir(exist_ok=True)

# ─────────────── default prompt ─────────────
DEFAULT_SYSTEM_PROMPT = (
    "You are an expert migration engineer.\n"
    "Convert the given SAS code block to **equivalent PySpark** "
    "(DataFrame API or spark.sql). Keep the business logic and "
    "naming intact.\n"
    "If conversion is not possible, wrap the SAS block inside "
    "a commented  spark.sql(''' ... ''') placeholder."
)

PROMPT_TMPL = ChatPromptTemplate.from_messages(
    [
        ("system", "{system_prompt}"),
        (
            "user",
            "### SAS code (chunk {chunk_id}, type={chunk_type}) ###\n"
            "{sas_code}\n\n"
            "### PySpark equivalent ###",
        ),
    ]
)

# ─────────── token counter ───────────
def _count_tokens(model_name: str, text: str) -> int:
    try:
        import tiktoken
        enc = tiktoken.encoding_for_model(model_name)
        return len(enc.encode(text))
    except Exception:
        return len(text.split())

# ─────────── LLM init ───────────
def _init_llm(provider: str, cred: Dict):
    if provider == "azureopenai":
        return AzureChatOpenAI(
            azure_endpoint     = cred["openai_api_base"],
            openai_api_key     = cred["openai_api_key"],
            openai_api_version = cred["openai_api_version"],
            deployment_name    = cred["deployment_name"],
            model_name         = cred["model_name"],
            temperature        = 0.0,
        )
    if provider == "gemini":
        return ChatGoogleGenerativeAI(
            model          = cred["model_name"],
            google_api_key = cred["google_api_key"],
            temperature    = 0.0,
        )
    raise ValueError("Unsupported LLM provider")

# ─────────── core converter ───────────
def _convert_chunk(llm, blk: Dict, system_prompt: str, retry: bool = False) -> Dict:
    rendered = PROMPT_TMPL.format_prompt(
        system_prompt = system_prompt,
        chunk_id      = blk["id"],
        chunk_type    = blk["type"],
        sas_code      = blk["code"],
    ).to_messages()

    try:
        resp   = llm.predict_messages(rendered)
        output = resp.content.strip()
        if not output:
            raise ValueError("LLM returned empty result")

        if getattr(resp, "usage", None):
            in_tok  = resp.usage.prompt_tokens
            out_tok = resp.usage.completion_tokens
        else:
            in_txt  = rendered[-1].content
            in_tok  = _count_tokens(getattr(llm, "model_name", "gpt-4o"), in_txt)
            out_tok = _count_tokens(getattr(llm, "model_name", "gpt-4o"), output)

        return {
            "id":   blk["id"],
            "ok":   True,
            "code": output,
            "reason": "",
            "input_tokens":  in_tok,
            "output_tokens": out_tok,
            "total_tokens":  in_tok + out_tok,
        }
    except Exception as exc:
        if not retry:
            return _convert_chunk(llm, blk, system_prompt, retry=True)  # one retry
        return {
            "id":   blk["id"],
            "ok":   False,
            "code": f"# LLM ERROR: {exc}",
            "reason": str(exc),
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
        }

# ─────────── LangGraph node ───────────
def llm_rule_node(state: Dict) -> Dict:
    print("🧠  LLM‑Rule Node enhanced …")

    ast_blocks      = state["ast_blocks"]
    provider        = state["llm_provider"]
    cred            = state["llm_cred"]
    system_prompt   = state.get("system_prompt", DEFAULT_SYSTEM_PROMPT)
    dry_run         = state.get("dry_run", False)

    llm = _init_llm(provider, cred)
    trivial = set()

    records = []
    sas_lookup = {b["id"]: b["code"] for b in ast_blocks}

    for blk in ast_blocks:
        if blk["type"].lower() in trivial:
            records.append({
                "id": blk["id"],
                "ok": True,
                "code": f"# {blk['type'].upper()} skipped\n",
                "reason": "Skipped",
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            })
        elif dry_run:
            records.append({
                "id": blk["id"],
                "ok": True,
                "code": "# DRY RUN - SKIPPED",
                "reason": "Dry run mode",
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            })
        else:
            records.append(_convert_chunk(llm, blk, system_prompt))

    records.sort(key=lambda r: int(r["id"].split("_")[-1]))

    csv_path = (
        Path(state["rule_csv"])
        if state.get("rule_csv")
        else RULE_DIR / f"rule_llm_{uuid.uuid4().hex}.csv"
    )
    state["rule_csv"] = str(csv_path)

    df = pd.DataFrame([
        {
            "id": r["id"],
            "success": r["ok"],
            "reason": r.get("reason", ""),
            "input_sas_code": sas_lookup.get(r["id"], ""),
            "output_pyspark_code": r["code"],
            "input_tokens": r["input_tokens"],
            "output_tokens": r["output_tokens"],
            "total_tokens": r["total_tokens"]
        }
        for r in records
    ])
    df.to_csv(csv_path, index=False)

    successes  = [r for r in records if r["ok"]]
    failed_ids = [r["id"] for r in records if not r["ok"]]

    meta = {
        "timestamp": datetime.utcnow().isoformat(),
        "model": getattr(llm, "model_name", "unknown"),
        "total_chunks": len(records),
        "successes": len(successes),
        "failures": len(failed_ids)
    }

    return {
        **state,
        "pyspark_chunks": successes,
        "failed_chunks":  failed_ids,
        "conversion_meta": meta,
        "logs": state.get("logs", []) + [
            f"LLM‑rule converted {len(successes)} chunks; failed {len(failed_ids)} (CSV: {csv_path.name})"
        ]
    }
