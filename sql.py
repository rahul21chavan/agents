import streamlit as st
import os
import re
import time
import openai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Configure OpenAI API
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY
else:
    st.error("OpenAI API key not found. Please set OPENAI_API_KEY in your .env file.")
    st.stop()

# Streamlit page config
st.set_page_config(page_title="PL/SQL to PySpark Converter", layout="centered")
st.title("🔄 PL/SQL ➡️ PySpark Converter (via OpenAI)")

st.markdown("""
This app allows you to:
1. Upload a PL/SQL `.sql` file.
2. Parse the code into blocks.
3. Automatically convert each block to PySpark using OpenAI API.
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
        raw_blocks = [block.strip() for block in code.split(";") if block.strip()]
        cleaned_blocks = []
        for block in raw_blocks:
            block = re.sub(r"/\*.*?\*/", "", block, flags=re.DOTALL)  # Remove block comments
            block = re.sub(r"--.*", "", block)  # Remove line comments
            cleaned_blocks.append(block.strip())
        return cleaned_blocks

    # --- Step 2: Convert each block with OpenAI ---
    def convert_block_to_pyspark_with_openai(block: str):
        try:
            prompt = f"""
You are a data engineer. Convert the following PL/SQL code block into equivalent PySpark (Python) code. Maintain the logic and structure as closely as possible. Comment important translations.

PL/SQL Code:
{block}

# Begin PySpark Code:
"""
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that converts PL/SQL code to PySpark code."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            return response['choices'][0]['message']['content'].strip()
        except Exception as e:
            return f"# Error: {e}"

    # --- Step 3: Conversion ---
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

        converted = convert_block_to_pyspark_with_openai(block)
        converted_blocks.append(converted)

        with st.expander(f"🟩 Converted Block {i+1} (PySpark)", expanded=False):
            st.code(converted, language="python")

        progress_bar.progress((i + 1) / len(blocks))
        time.sleep(0.5)  # Optional delay for UX

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

# Footer
st.markdown("---")
st.info("OpenAI API key is loaded from `.env`. Ensure it is named `OPENAI_API_KEY`.")
