import os
import io
import streamlit as st
import pandas as pd
from typing import Optional
import sqlparse

# --- Robust PL/SQL Chunker (Regex + AST) ---
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

# --- LLM Credentials UI & Validation ---
def prompt_llm_credentials():
    st.markdown("### 🔑 Enter your LLM API Credentials")
    llm_type = st.selectbox("Choose LLM Provider", ["Gemini", "Azure OpenAI"], key="llm_type")
    creds = {}
    if llm_type == "Gemini":
        creds["provider"] = "Gemini"
        creds["GEMINI_API_KEY"] = st.text_input("Gemini API Key", type="password", key="gemini_key")
    elif llm_type == "Azure OpenAI":
        creds["provider"] = "Azure OpenAI"
        creds["OPENAI_API_KEY"] = st.text_input("OpenAI API Key", type="password", key="openai_key")
        creds["OPENAI_API_BASE"] = st.text_input("OpenAI API Base URL", key="openai_base")
        creds["OPENAI_API_TYPE"] = st.text_input("OpenAI API Type (e.g. azure)", value="azure", key="openai_type")
        creds["OPENAI_API_VERSION"] = st.text_input("OpenAI API Version", value="2023-05-15", key="openai_version")
        creds["DEPLOYMENT_NAME"] = st.text_input("Azure Deployment Name", key="openai_deployment")
    return creds

def validate_llm_credentials(creds):
    try:
        if creds.get("provider") == "Gemini":
            import google.generativeai as genai
            genai.configure(api_key=creds["GEMINI_API_KEY"])
            model = genai.GenerativeModel("gemini-1.5-pro")
            model.generate_content("hello")  # test simple call
        elif creds.get("provider") == "Azure OpenAI":
            import openai
            openai.api_key = creds["OPENAI_API_KEY"]
            openai.api_base = creds["OPENAI_API_BASE"]
            openai.api_type = creds["OPENAI_API_TYPE"]
            openai.api_version = creds["OPENAI_API_VERSION"]
            openai.ChatCompletion.create(
                engine=creds["DEPLOYMENT_NAME"],
                messages=[{"role": "user", "content": "hello"}],
                temperature=0.0,
                max_tokens=1
            )
        else:
            return False, "Unknown provider."
        return True, "Credentials validated!"
    except Exception as e:
        return False, f"Validation failed: {e}"

def llm_credentials_flow():
    creds = prompt_llm_credentials()
    submitted = st.button("Validate & Continue")
    if submitted:
        with st.spinner("Validating credentials..."):
            valid, msg = validate_llm_credentials(creds)
        if valid:
            st.success(msg)
            st.session_state["llm_creds"] = creds
            return creds
        else:
            st.error(msg)
            st.stop()
    if "llm_creds" in st.session_state:
        return st.session_state["llm_creds"]
    return None

# --- LLM Provider Classes ---
class LLMProvider:
    def convert(self, block: str) -> str:
        raise NotImplementedError
    def convert_optimized(self, script: str) -> str:
        raise NotImplementedError

class GeminiProvider(LLMProvider):
    def __init__(self, api_key: str):
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel("gemini-1.5-pro")
    def convert(self, block: str) -> str:
        prompt = (
            "You are a senior data engineer experienced in migrating legacy PL/SQL code to PySpark.\n\n"
            "Convert the following PL/SQL block into PySpark using the DataFrame API.\n"
            "Return only executable Python code.\n\n"
            f"PL/SQL Block:\n{block}\n"
        )
        try:
            resp = self.model.generate_content(prompt)
            return resp.text.strip()
        except Exception as e:
            return f"# Gemini Error: {e}"
    def convert_optimized(self, script: str) -> str:
        prompt = (
            "You are a senior data engineer experienced in migrating legacy PL/SQL code to PySpark.\n\n"
            "Convert the following ENTIRE PL/SQL script into a single, clean, production-ready PySpark script using the DataFrame API.\n"
            "Integrate all logic, deduplicate where possible, use idiomatic PySpark, and output only the final, unified, executable Python code. Do not simply concatenate per-block code: merge and optimize for clarity and maintainability.\n\n"
            f"Full PL/SQL Script:\n{script}\n"
        )
        try:
            resp = self.model.generate_content(prompt)
            return resp.text.strip()
        except Exception as e:
            return f"# Gemini Error: {e}"

class OpenAIProvider(LLMProvider):
    def __init__(self, creds):
        import openai
        self.openai = openai
        openai.api_key = creds["OPENAI_API_KEY"]
        openai.api_base = creds["OPENAI_API_BASE"]
        openai.api_type = creds["OPENAI_API_TYPE"]
        openai.api_version = creds["OPENAI_API_VERSION"]
        self.deployment_name = creds["DEPLOYMENT_NAME"]
    def convert(self, block: str) -> str:
        prompt = (
            "You are a data engineer. Convert the following PL/SQL code block into PySpark DataFrame API code.\n"
            "Only return valid, executable Python code. Do not include explanations, comments, or markdown.\n"
            f"PL/SQL Block:\n{block}\n"
        )
        try:
            resp = self.openai.ChatCompletion.create(
                engine=self.deployment_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            return f"# OpenAI Error: {e}"
    def convert_optimized(self, script: str) -> str:
        prompt = (
            "You are a data engineer. Convert the following ENTIRE PL/SQL script into a single, clean, production-ready PySpark script using the DataFrame API.\n"
            "Integrate all logic, deduplicate where possible, use idiomatic and maintainable PySpark, and output only the final, unified, executable Python code. Do not simply concatenate per-block code: merge and optimize for clarity and maintainability.\n\n"
            f"Full PL/SQL Script:\n{script}\n"
        )
        try:
            resp = self.openai.ChatCompletion.create(
                engine=self.deployment_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            return f"# OpenAI Error: {e}"

def get_llm_provider(creds) -> Optional[LLMProvider]:
    if creds and creds.get("provider") == "Gemini" and creds.get("GEMINI_API_KEY"):
        return GeminiProvider(creds["GEMINI_API_KEY"])
    elif creds and creds.get("provider") == "Azure OpenAI" and creds.get("OPENAI_API_KEY"):
        return OpenAIProvider(creds)
    return None

# --- UI Helper ---
def show_fake_user_profile():
    st.markdown(
        """
        <style>
        .profile-card {
            background: linear-gradient(135deg, #0f2027 0%, #2c5364 100%);
            color: #FFD700;
            border-radius: 18px;
            padding: 18px 16px 16px 16px;
            margin-bottom: 30px;
            box-shadow: 0 6px 24px 0 rgba(32,40,80,0.2);
            display: flex;
            align-items: center;
            gap: 16px;
        }
        .profile-avatar {
            border-radius: 50%;
            border: 3px solid #FFD700;
            width: 68px;
            height: 68px;
            object-fit: cover;
            box-shadow: 0 2px 8px #1a2236AA;
        }
        .profile-info {
            display: flex;
            flex-direction: column;
        }
        .profile-name {
            font-size: 1.13rem;
            font-weight: 700;
            color: #FFD700;
            margin-bottom: 2px;
        }
        .profile-role {
            font-size: 0.98rem;
            color: #e9e3c9;
            font-weight: 400;
        }
        </style>
        <div class="profile-card">
            <img src="https://ui-avatars.com/api/?name=Rahul+Chavan&background=2c5364&color=ffd700&size=128" class="profile-avatar" />
            <div class="profile-info">
                <span class="profile-name">Rahul Chavan</span>
                <span class="profile-role">🪄 Data Engineer</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

st.set_page_config(page_title="PL/SQL to PySpark • Rich UI", layout="wide")

st.markdown("""
    <style>
    body {
        background: linear-gradient(120deg,#162447 0%,#1f4068 100%);
    }
    .block-container {
        background: linear-gradient(120deg,#162447 0%,#1f4068 100%);
        padding-bottom: 16px !important;
    }
    .stApp {
        background: linear-gradient(120deg,#162447 0%,#1f4068 100%) !important;
    }
    .stButton>button {
        color: #fff !important;
        background: linear-gradient(90deg, #FFD700 0%, #FFA500 100%) !important;
        border: none;
        border-radius: 6px;
        font-weight: 600;
        padding: 0.55em 1.2em !important;
        box-shadow: 0 2px 12px #FFD70044;
        margin-bottom: 4px;
    }
    .stTextArea textarea, .stTextInput input, .stFileUploader label {
        background: #f5f3d9 !important;
        color: #29335c !important;
        border-radius: 10px !important;
        border: 1.5px solid #FFD70033 !important;
        font-family: 'Fira Mono', monospace !important;
    }
    .stCode {
        background: #1f4068 !important;
        color: #FFD700 !important;
        border-radius: 12px !important;
        border: 1px solid #FFD70033 !important;
    }
    .stTable, .stDataFrame, .stMarkdown {
        background: rgba(255,255,255,0.03) !important;
        border-radius: 14px !important;
        border: 0.5px solid #FFD70033 !important;
    }
    .stDownloadButton>button {
        background: linear-gradient(90deg, #FFD700 0%, #FFA500 100%) !important;
        color: #1f4068 !important;
        border-radius: 6px;
        font-weight: 600;
        box-shadow: 0 2px 12px #FFD70044;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown(
    "<h1>🔄 <span style='color:#FFD700;'>PL/SQL</span> to <span style='color:#FFD700;'>PySpark</span> Converter <span style='font-size:0.7em;color:#FFD700;'>✨ Luxury UI</span></h1>",
    unsafe_allow_html=True
)

with st.sidebar:
    show_fake_user_profile()
    st.markdown("<hr style='border:1px solid #FFD70033; margin:8px 0;'/>", unsafe_allow_html=True)
    creds = llm_credentials_flow()
    input_method = st.radio("Input Method", ["Upload .sql File", "Paste Code"])

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

st.markdown(
    "<div style='margin-bottom:14px'><button style='background:linear-gradient(90deg,#FFD700,#FFA500);color:#1f4068;border:none;border-radius:10px;font-weight:700;padding:7px 18px;box-shadow:0 2px 10px #FFD70044;' onclick=\"window.location.reload();\">Load Example PL/SQL</button></div>",
    unsafe_allow_html=True
)
if st.button("Load Example PL/SQL"):
    st.session_state["example_sql"] = example_plsql()

sql_code = ""
if input_method == "Upload .sql File":
    uploaded_file = st.file_uploader("Upload PL/SQL file", type=["sql", "txt"])
    if uploaded_file:
        sql_code = uploaded_file.read().decode("utf-8")
else:
    sql_code = st.text_area("Paste PL/SQL code here", height=300,
                            value=st.session_state.get("example_sql", ""))

if sql_code and creds:
    st.markdown("<div style='font-size:1.2em;font-weight:600;color:#FFD700;margin:10px 0 0 0;'>📄 Original PL/SQL Code</div>", unsafe_allow_html=True)
    st.code(sql_code, language="sql")

    blocks = split_plsql_into_blocks(sql_code, max_chunk_size=1200)
    provider = get_llm_provider(creds)
    if provider is None:
        st.error("❌ LLM provider not properly configured.")
        st.stop()

    # Chunked conversion for mapping/audit/preview
    converted_blocks = []
    progress = st.progress(0, text="Converting blocks for preview/CSV ...")
    for i, block in enumerate(blocks):
        with st.spinner(f"Converting Block {i+1}/{len(blocks)}..."):
            converted_blocks.append(provider.convert(block))
            progress.progress((i+1)/len(blocks), text=f"Converted Block {i+1}/{len(blocks)}")
    progress.empty()

    st.markdown("<div style='font-size:1.09em;font-weight:500;color:#FFD700;margin:20px 0 0 0;'>🧾 Preview: PL/SQL Block vs PySpark</div>", unsafe_allow_html=True)
    preview_df = pd.DataFrame({
        "PL/SQL Block": blocks,
        "Converted PySpark": converted_blocks
    })
    st.dataframe(preview_df, use_container_width=True)
    csv_buffer = io.StringIO()
    preview_df.to_csv(csv_buffer, index=False)
    st.download_button("📥 Download PL/SQL Blocks Mapping (.csv)", data=csv_buffer.getvalue(),
                      file_name="plsql_blocks_mapping.csv", mime="text/csv")

    # Optimized Final PySpark code for the entire script
    if st.button("🚀 Generate Final Optimized PySpark Code", key="final_optimized"):
        with st.spinner("Generating a single, clean, unified optimized PySpark code for the full script..."):
            final_output = provider.convert_optimized(sql_code)
            st.session_state["final_output"] = final_output
    else:
        final_output = st.session_state.get("final_output", "")

    if final_output:
        st.markdown("<div style='font-size:1.15em;font-weight:500;color:#FFD700;margin:22px 0 0 0;'>🐍 Final Optimized PySpark Code</div>", unsafe_allow_html=True)
        st.code(final_output, language="python")
        st.download_button("📥 Download Final PySpark Code", final_output, file_name="final_optimized_pyspark.py")
else:
    st.info("Upload a file or paste PL/SQL code to begin.")