import streamlit as st
import pandas as pd
import sqlparse
from typing import List
import re

def _regex_chunk_blocks(plsql_code):
    code = plsql_code.replace('\r\n', '\n').replace('\r', '\n')
    block_re = re.compile(
        r'((?:(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE|PACKAGE|TRIGGER)[\s\S]*?END\s*;)|'
        r'(?:DECLARE[\s\S]*?END\s*;)|'
        r'(?:BEGIN[\s\S]*?END\s*;)|'
        r'(?:[^;]+;)))',
        re.IGNORECASE
    )
    block_matches = block_re.findall(code)
    blocks = []
    for block in block_matches:
        block = block.strip()
        if block and block != '/':
            blocks.append(block)
    return blocks

def _ast_chunk_blocks(plsql_code, max_chunk_size=1200):
    statements = sqlparse.parse(plsql_code)
    blocks = []
    for stmt in statements:
        stmt_str = str(stmt).strip()
        if not stmt_str:
            continue
        if len(stmt_str) > max_chunk_size:
            inner_blocks = re.split(r'(?i)(?=BEGIN)', stmt_str)
            for ib in inner_blocks:
                ib = ib.strip()
                if not ib:
                    continue
                if len(ib) > max_chunk_size:
                    sub_blocks = []
                    temp = []
                    temp_len = 0
                    for part in ib.split(';'):
                        if not part.strip():
                            continue
                        temp.append(part + ';')
                        temp_len += len(part) + 1
                        if temp_len > max_chunk_size:
                            sub_blocks.append('\n'.join(temp).strip())
                            temp = []
                            temp_len = 0
                    if temp:
                        sub_blocks.append('\n'.join(temp).strip())
                    blocks.extend(sub_blocks)
                else:
                    blocks.append(ib)
        else:
            blocks.append(stmt_str)
    final_blocks = []
    for b in blocks:
        if not b.strip():
            continue
        if all(l.strip().startswith('--') or not l.strip() for l in b.split('\n')):
            continue
        final_blocks.append(b)
    return final_blocks

def split_plsql_into_blocks(plsql_code, max_chunk_size=1200):
    regex_blocks = _regex_chunk_blocks(plsql_code)
    all_blocks = []
    for block in regex_blocks:
        if len(block) > max_chunk_size or block.upper().startswith(('CREATE', 'DECLARE', 'BEGIN')):
            ast_blocks = _ast_chunk_blocks(block, max_chunk_size)
            all_blocks.extend(ast_blocks)
        else:
            all_blocks.append(block)
    cleaned_blocks = []
    temp = []
    for b in all_blocks:
        if not b.strip():
            continue
        if len(b) < 180:
            temp.append(b)
            if sum(len(x) for x in temp) > 300:
                cleaned_blocks.append('\n'.join(temp))
                temp = []
        else:
            if temp:
                cleaned_blocks.append('\n'.join(temp))
                temp = []
            cleaned_blocks.append(b)
    if temp:
        cleaned_blocks.append('\n'.join(temp))
    final_blocks = []
    for b in cleaned_blocks:
        if len(b) > max_chunk_size:
            stmts = [s+';' for s in b.split(';') if s.strip()]
            buff = []
            buff_len = 0
            for stmt in stmts:
                buff.append(stmt)
                buff_len += len(stmt)
                if buff_len > max_chunk_size:
                    final_blocks.append('\n'.join(buff))
                    buff = []
                    buff_len = 0
            if buff:
                final_blocks.append('\n'.join(buff))
        else:
            final_blocks.append(b)
    final_blocks = [b for b in final_blocks if b.strip() and not all(l.strip().startswith('--') or not l.strip() for l in b.split('\n'))]
    return final_blocks

def get_block_type(block):
    header = block.strip().splitlines()[0].upper()
    if header.startswith("CREATE OR REPLACE FUNCTION"):
        return "FUNCTION"
    elif header.startswith("CREATE OR REPLACE PROCEDURE"):
        return "PROCEDURE"
    elif header.startswith("CREATE OR REPLACE PACKAGE"):
        return "PACKAGE"
    elif header.startswith("CREATE OR REPLACE TRIGGER"):
        return "TRIGGER"
    elif header.startswith("DECLARE"):
        return "ANONYMOUS BLOCK"
    elif header.startswith("BEGIN"):
        return "ANONYMOUS BLOCK"
    elif header.startswith("UPDATE"):
        return "UPDATE"
    elif header.startswith("INSERT"):
        return "INSERT"
    elif header.startswith("DELETE"):
        return "DELETE"
    elif header.startswith("SELECT"):
        return "SELECT"
    else:
        return "OTHER"

def get_token_summary(block):
    stmts = sqlparse.parse(block)
    summary = []
    for stmt in stmts:
        tokens = [t for t in stmt.flatten() if not t.is_whitespace]
        summary.append([
            {"type": str(t.ttype) if t.ttype else str(type(t)), "value": t.value}
            for t in tokens
        ])
    return summary

def example_plsql():
    return """CREATE OR REPLACE PROCEDURE update_salary IS
  v_count NUMBER := 0;
BEGIN
  SELECT COUNT(*) INTO v_count FROM employees WHERE department_id = 10;
  IF v_count > 0 THEN
    UPDATE employees SET salary = salary * 1.1 WHERE department_id = 10;
  END IF;
END;
/

-- Standalone statement
UPDATE departments SET location_id = 2000 WHERE department_id = 20;

CREATE OR REPLACE FUNCTION get_department_name(dept_id NUMBER) RETURN VARCHAR2 IS
  dept_name VARCHAR2(50);
BEGIN
  SELECT department_name INTO dept_name FROM departments WHERE department_id = dept_id;
  RETURN dept_name;
END;
/
"""

st.set_page_config(page_title="PL/SQL Parsing Agent (Regex+AST)", layout="wide")

st.markdown(
    """
    <h1>🧑‍💻 PL/SQL Parsing Agent (Regex & AST)</h1>
    <p>
    <b>Purpose:</b> Analyze and visualize how your PL/SQL scripts are decomposed into logical, LLM-ready code blocks using both regex and AST techniques.<br>
    <b>Features:</b> Robust chunking for any kind of PL/SQL, block type/stats, AST token view, CSV export.<br>
    <span style="color:#FFD700">No model inference/conversion is performed. This tool is for technical parsing diagnostics only.</span>
    </p>
    """, unsafe_allow_html=True
)

with st.sidebar:
    st.header("Options")
    if st.button("Load Example PL/SQL"):
        st.session_state["example_sql"] = example_plsql()
    chunk_size = st.slider("Max Chunk Size (chars)", min_value=200, max_value=3000, value=1200, step=100)
    show_ast = st.checkbox("Show AST Tokens per Block", value=True)
    show_stats = st.checkbox("Show Technical Stats Table", value=True)

input_method = st.radio("Input Method", ["Upload .sql File", "Paste Code"])
if input_method == "Upload .sql File":
    uploaded_file = st.file_uploader("Upload PL/SQL file", type=["sql", "txt"])
    if uploaded_file:
        sql_code = uploaded_file.read().decode("utf-8")
    else:
        sql_code = ""
else:
    sql_code = st.text_area("Paste PL/SQL code here", height=300,
                            value=st.session_state.get("example_sql", ""))

if sql_code:
    st.markdown("#### Original PL/SQL Script:")
    st.code(sql_code, language="sql")
    blocks = split_plsql_into_blocks(sql_code, max_chunk_size=chunk_size)
    st.success(f"Detected {len(blocks)} logical blocks.")

    block_types = []
    num_lines = []
    num_chars = []
    first_line = []
    for block in blocks:
        block_types.append(get_block_type(block))
        lines = block.splitlines()
        num_lines.append(len(lines))
        num_chars.append(len(block))
        first_line.append(lines[0].strip()[:80] if lines else "")

    if show_stats:
        st.markdown("### 🧮 Block Technical Stats")
        stats_df = pd.DataFrame({
            "Block #": [f"{i+1}" for i in range(len(blocks))],
            "Type": block_types,
            "Lines": num_lines,
            "Chars": num_chars,
            "First Line Preview": first_line
        })
        st.dataframe(stats_df, use_container_width=True)

    st.markdown("### 🧩 Parsed Blocks")
    for i, block in enumerate(blocks):
        st.markdown(f"#### Block {i+1} <span style='color: #FFD700;'>[{block_types[i]}]</span>", unsafe_allow_html=True)
        st.code(block, language="sql")
        st.write(f"**Lines:** {num_lines[i]} &nbsp;|&nbsp; **Chars:** {num_chars[i]}")
        if show_ast:
            st.markdown("<details><summary>Show AST Tokens</summary>", unsafe_allow_html=True)
            token_summary = get_token_summary(block)
            for stmt_idx, tokens in enumerate(token_summary):
                st.markdown(f"<b>Statement {stmt_idx+1}:</b>", unsafe_allow_html=True)
                for t in tokens:
                    st.write(f"- <code>{t['type']}</code>: <b>{t['value'].strip()}</b>")
            st.markdown("</details>", unsafe_allow_html=True)

    df = pd.DataFrame({
        "Block Number": [f"Block {i+1}" for i in range(len(blocks))],
        "Type": block_types,
        "Lines": num_lines,
        "Chars": num_chars,
        "PL/SQL Block": blocks
    })
    import io
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    st.download_button("📥 Download Blocks as CSV", data=csv_buffer.getvalue(), file_name="plsql_blocks.csv", mime="text/csv")

else:
    st.info("Upload a file or paste PL/SQL code to see how it will be parsed.")