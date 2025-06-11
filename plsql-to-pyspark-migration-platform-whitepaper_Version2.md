# PL/SQL to PySpark Migration Platform – Technical Whitepaper

**Robust Modernization with Hybrid Parsing, Logical Chunking, and LLM-based Conversion**

---

## Abstract

This whitepaper documents the technical architecture, design rationale, and implementation details of a next-generation PL/SQL to PySpark migration platform. The system leverages hybrid parsing using both regex and AST/sqlparse, advanced chunking strategies, and modular LLM-based code conversion. It is intended for practitioners seeking a production-grade, auditable, and extensible approach to legacy code modernization.

---

## 1. Motivation

- **Legacy PL/SQL**: Many enterprises have critical ETL/data logic implemented in Oracle PL/SQL.
- **Modernization Need**: Migrating to Spark-based platforms (using PySpark DataFrame API) offers scalability, cost savings, and easier ML/AI integration.
- **Challenges**: PL/SQL scripts are large, complex, and their procedural logic does not easily map to distributed DataFrame operations. Chunking, context preservation, and correctness are paramount.

---

## 2. Architecture Overview

### 2.1 Modular Pipeline

1. **Input Ingestion**: Supports both file upload and direct code paste.
2. **Parsing Agent**: Splits monolithic scripts into logical, manageable code blocks using hybrid regex and AST parsing.
3. **Block Analysis**: Each block is classified (procedure, function, DML, etc.), and statistics and AST tokens are extracted.
4. **LLM Conversion** (optional): Each block (or the whole script) is converted to PySpark using a pluggable LLM interface (supports Gemini, Azure OpenAI, etc.).
5. **Post-Processing**: Blocks are reassembled into a production-ready PySpark script; mapping tables and CSVs are generated for audit.
6. **User Interface**: Streamlit-based, with options for visual QA, CSV export, and credential management.

---

## 3. Parsing Logic: Hybrid Regex + AST

### 3.1 Why Hybrid?

- **Regex**: Fast, effective at identifying clear block boundaries (e.g., CREATE ... END;, DECLARE ... END;, single DML).
- **AST/sqlparse**: Handles nested, ambiguous, or extremely large blocks; provides token-level context and safety against false splits.
- **Combined**: Ensures robust, logical chunking even for huge or malformed scripts.

### 3.2 Parsing Algorithm (Pseudocode)

```
function split_plsql_into_blocks(plsql_code, max_chunk_size):
    regex_blocks = regex_split(plsql_code)  # Step 1: fast coarse chunking
    all_blocks = []
    for block in regex_blocks:
        if large_or_complex(block):
            ast_blocks = ast_split(block, max_chunk_size)
            all_blocks.extend(ast_blocks)
        else:
            all_blocks.append(block)
    cleaned_blocks = clean_and_merge_small(all_blocks)
    final_blocks = last_resort_split(cleaned_blocks, max_chunk_size)
    return drop_empty_or_comment_only(final_blocks)
```

### 3.3 Block Type Detection

- Simple heuristics on block headers (`CREATE OR REPLACE FUNCTION`, `PROCEDURE`, `BEGIN`, etc.).
- Enables type-aware conversion and mapping.

### 3.4 AST Token Extraction

- Uses [sqlparse](https://github.com/andialbrecht/sqlparse) to flatten statements and extract token types and values.
- Useful for QA, LLM prompt engineering, and code auditing.

---

## 4. Conversion Pipeline

- **Pluggable LLMs**: Gemini, Azure OpenAI, others via a generic interface.
- **Per-block and holistic script conversion**: User chooses between atomic block conversion (for mapping/audit) and full-script conversion (for production code).
- **Credential Management**: Runtime, secure input via UI; no hardcoded secrets.

---

## 5. Technical QA and Auditing

- **Block stats**: Lines, characters, type, snippet preview.
- **AST inspection**: Optional per-block token display.
- **CSV export**: All blocks with metadata and (optionally) conversion results.

---

## 6. Streamlit UI Features

- File upload or paste.
- LLM credential validation panel.
- Block preview (PL/SQL, type, stats, tokens).
- Conversion preview and download.
- Full PySpark script generation and download.

---

## 7. Sample Code Snippet (Parsing Logic)

```python
import re
import sqlparse

def split_plsql_into_blocks(plsql_code, max_chunk_size=1200):
    # Regex phase
    regex_blocks = _regex_chunk_blocks(plsql_code)
    all_blocks = []
    for block in regex_blocks:
        if len(block) > max_chunk_size or block.upper().startswith(('CREATE', 'DECLARE', 'BEGIN')):
            ast_blocks = _ast_chunk_blocks(block, max_chunk_size)
            all_blocks.extend(ast_blocks)
        else:
            all_blocks.append(block)
    # Merge small, split huge, drop empties/comments
    # [see implementation in plsql_chunker.py]
    return final_blocks
```

---

## 8. Extensibility & Roadmap

- **Support for more SQL dialects** (T-SQL, PL/pgSQL)
- **Automatic test generation** for migrated scripts
- **User-defined chunking policies**
- **Integrated CI/CD hooks** for migration at scale
- **Audit logging and traceability features**

---

## 9. References

- [sqlparse](https://github.com/andialbrecht/sqlparse)
- [Streamlit](https://streamlit.io/)
- [Google Gemini API](https://ai.google.dev)
- [Azure OpenAI](https://learn.microsoft.com/en-us/azure/ai-services/openai/)
- [PL/SQL Language Reference](https://docs.oracle.com/en/database/oracle/oracle-database/19/lnpls/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html)

---

**Contact:**  
*Rahul Chavan · Data Engineering Lead*  
*Version: NextGen · Date: 2025-06-11*
