# backend/agents/validate_agent_enhanced.py
"""
Enhanced Validator Node
► Rule-based validator for generated PySpark code.
► Assigns a score, failure reasons, and logs.
"""

def validate_node(state: dict) -> dict:
    print("✅ Running Enhanced Validation Node…")

    logs = state.get("logs", [])
    code = state.get("pyspark_code", "") or ""
    dry_run = state.get("dry_run", False)

    if dry_run:
        logs.append("🟡 Dry run mode – skipping validation logic.")
        return {
            **state,
            "validation_passed": True,
            "validation_score": 100,
            "validation_tags": ["dry_run"],
            "failed_chunks": [],
            "logs": logs,
        }

    failure_tags = []
    score = 100

    # ── Rule‑1: Length sanity check ────────────────
    if len(code.strip()) < 20:
        logs.append("❌ Validation failed – output too short.")
        failure_tags.append("length_check")
        score -= 40

    # ── Rule‑2: Spark keyword presence ─────────────
    spark_terms = [
        "spark.read", ".select(", ".filter(",
        ".withColumn(", ".groupBy(", ".write", ".join("
    ]
    matches = [kw for kw in spark_terms if kw in code]
    if len(matches) < 2:
        logs.append(f"❌ Validation failed – found only {len(matches)} Spark terms.")
        failure_tags.append("spark_coverage")
        score -= 30

    # ── Rule‑3: Obvious LLM artifacts ─────────────
    suspicious_lines = ["# LLM ERROR", "spark.sql(\"\"\"", "pass", "TODO", "NA"]
    if any(x in code for x in suspicious_lines):
        logs.append("❌ Validation failed – found suspicious artifacts.")
        failure_tags.append("llm_artifact")
        score -= 30

    # ── Final decision ─────────────────────────────
    passed = score >= 60 and not failure_tags
    if passed:
        logs.append(f"✅ Validation passed – score={score}")
        return {
            **state,
            "validation_passed": True,
            "validation_score": score,
            "validation_tags": [],
            "failed_chunks": [],
            "logs": logs,
        }
    else:
        logs.append(f"❌ Final Validation failed – score={score}")
        return {
            **state,
            "validation_passed": False,
            "validation_score": score,
            "validation_tags": failure_tags,
            "failed_chunks": ["validation"],
            "logs": logs,
        }
