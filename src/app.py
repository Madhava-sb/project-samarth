# src/app.py
"""
Project Samarth — FINAL UI (NO ANY_VALUE, NO GROUP BY AGGREGATES)
"""

import streamlit as st
import duckdb
import requests
import re
from pathlib import Path

# === CONFIG ===
st.set_page_config(page_title="Project Samarth", layout="wide")
st.title("🌾 Project Samarth — Intelligent Q&A on Indian Agriculture & Climate")
st.markdown("**Live data.gov.in** → **Local LLM** → **SQL Answer** → **Citation**")

# === DUCKDB ===
con = duckdb.connect(database=':memory:')
PROC_DIR = Path("../data/processed")
con.execute(f"CREATE TABLE crop AS SELECT * FROM '{PROC_DIR}/crop_clean.parquet'")
con.execute(f"CREATE TABLE rainfall AS SELECT * FROM '{PROC_DIR}/rainfall_long.parquet'")

# === SCHEMA ===
SCHEMA = """
TABLE crop (state, district, crop, season, year, area_hectare, production_tonnes);
TABLE rainfall (subdivision, year, month, rainfall_mm, annual);
JOIN: c.year = r.year AND c.state LIKE '%' || r.subdivision || '%'
GROUP BY: c.state or c.district only
DO NOT USE ANY_VALUE IN GROUP BY
"""

# === PROMPT (FINAL) ===
PROMPT_TEMPLATE = """
DUCKDB SQL ONLY. NO ```, NO EXPLANATION.

RULES:
1. Filter FIRST: WHERE UPPER(c.crop) = 'RICE'
2. Join: ON c.year = r.year AND c.state LIKE '%' || r.subdivision || '%'
3. GROUP BY c.state or c.district ONLY
4. DO NOT use ANY_VALUE in GROUP BY
5. Annual rainfall: SUM(r.rainfall_mm)
6. ROUND(..., 2)

Schema:
{SCHEMA}

Question: {question}

SQL:
"""


# === LLM CALL ===
@st.cache_data(ttl=3600)
def llm_generate_sql(question: str) -> str:
    prompt = PROMPT_TEMPLATE.format(SCHEMA=SCHEMA, question=question)
    try:
        resp = requests.post(
            "http://localhost:11434/api/generate",
            json={"model": "llama3.1:8b", "prompt": prompt, "stream": False, "options": {"temperature": 0.0}},
            timeout=120
        )
        resp.raise_for_status()
        sql = resp.json()["response"]
        sql = re.sub(r'```.*?```', '', sql, flags=re.DOTALL)
        sql = re.sub(r'^sql\s*', '', sql, flags=re.IGNORECASE).strip()
        return sql
    except Exception as e:
        return f"-- LLM ERROR: {e}"


# === EXECUTE ===
def run_query(sql: str):
    try:
        return con.execute(sql).df()
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return None


# === UI ===
st.sidebar.header("Sample Questions")
questions = [
    "Compare average annual rice production and rainfall in Maharashtra and Punjab for 2010–2015",
    "Which district in Punjab had the highest wheat production in the latest year?",
    "Top 3 states by rice production in 2015",
    "Rainfall trend in Kerala (2010–2015)"
]

for i, q in enumerate(questions):
    if st.sidebar.button(f"Q{i + 1}: {q[:60]}...", key=f"q{i}"):
        st.session_state.question = q

question = st.text_area(
    "Your Question:",
    value=st.session_state.get("question", questions[0]),
    height=120
)

if st.button("🚀 Get Answer", type="primary"):
    with st.spinner("Generating SQL with Llama 3.1..."):
        sql = llm_generate_sql(question)

    if sql and sql.upper().startswith("SELECT"):
        st.code(sql, language="sql")

        with st.spinner("Running on DuckDB..."):
            result = run_query(sql)

        if result is not None:
            st.success("**Answer**")
            st.dataframe(result, use_container_width=True)

            st.info("**Citation**")
            col1, col2 = st.columns(2)
            with col1:
                st.caption(f"**Crop Data**: {PROC_DIR}/crop_clean.parquet.source.txt")
            with col2:
                st.caption(f"**Rainfall Data**: {PROC_DIR}/rainfall_long.parquet.source.txt")
    else:
        st.error("Failed to generate valid SQL. Is Ollama running?")

# === FOOTER ===
st.markdown("---")
st.caption("Built with **Ollama + Llama 3.1 8B** | **DuckDB** | **Streamlit** | **data.gov.in**")