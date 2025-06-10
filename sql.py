import streamlit as st
import os
import re
from dotenv import load_dotenv
import google.generativeai as genai
import time

# Load environment variables
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Configure Gemini API
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    st.error("Gemini API key not found. Please set GEMINI_API_KEY in your .env file.")
    st.stop()

# Streamlit page config
st.set_page_config(page_title="PL/SQL to PySpark Converter", layout="centered")
st.title("🔄 PL/SQL ➡️ PySpark Converter")

st.markdown("""
This app allows you to:
1. Upload a PL/SQL `.sql` file.
2. Parse the code into blocks.
3. Automatically convert each block to PySpark using Gemini API.
4. View and download the converted PySpark script.
""")

# Upload file
uploaded_file = st.file_uploader("📤 Upload your PL/SQL (.sql) file", type=["sql"])
if uploaded_file:
    plsql_code = uploaded_file.read().decode("utf-8")
    st.subheader("📄 Original PL/SQL Code")
    st.code(plsql_code, language="sql")

    # --- Step 1: Parse PL/SQL Code ---
    def parse_sql_into_blocks(code: str):
        """
        Splits the input SQL code into logical blocks using semicolon and line-based heuristics.
        """
        raw_blocks = [block.strip() for block in code.split(";") if block.strip()]
        cleaned_blocks = []
        for block in raw_blocks:
            # Optional: clean multi-line comments
            block = re.sub(r"/\*.*?\*/", "", block, flags=re.DOTALL)
            # Optional: remove inline comments
            block = re.sub(r"--.*", "", block)
            cleaned_blocks.append(block.strip())
        return cleaned_blocks

    # --- Step 2: Use Gemini to convert each block ---
    def convert_block_to_pyspark_with_gemini(block: str):
        try:
            model = genai.GenerativeModel("gemini-pro")
            prompt = f"""
You are a code assistant. Convert the following PL/SQL code to PySpark (Python) code.
Maintain equivalent logic and comment on important translations.

PL/SQL:
{block}

# Begin PySpark equivalent code:
"""
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            return f"# Error converting block:\n# {str(e)}"

    # --- Step 3: Convert ---
    st.subheader("🔍 Parsing PL/SQL Code into Blocks...")
    blocks = parse_sql_into_blocks(plsql_code)

    if not blocks:
        st.warning("No valid blocks found in the uploaded PL/SQL file.")
        st.stop()

    st.write(f"✅ Found `{len(blocks)}` blocks for conversion.")

    converted_blocks = []
    progress_bar = st.progress(0)
    for i, block in enumerate(blocks):
        with st.expander(f"🔹 Original Block {i+1}", expanded=False):
            st.code(block, language="sql")

        converted = convert_block_to_pyspark_with_gemini(block)
        converted_blocks.append(converted)

        with st.expander(f"🟩 Converted Block {i+1} (PySpark)", expanded=False):
            st.code(converted, language="python")

        progress_bar.progress((i + 1) / len(blocks))
        time.sleep(0.5)  # Just for effect

    final_output = "\n\n".join(converted_blocks)

    # --- Step 4: Download ---
    st.subheader("📥 Download Full PySpark Code")
    st.download_button(
        label="⬇️ Download PySpark Code",
        data=final_output,
        file_name="converted_pyspark.py",
        mime="text/x-python"
    )

    st.success("🎉 Conversion completed successfully!")

# Footer note
st.markdown("---")
st.info("Gemini API key is loaded from `.env`. Ensure it is named `GEMINI_API_KEY`.")
