# Project Samarth 🌾

**Intelligent Q&A over live data.gov.in agriculture & climate data**

## Features
- Live API ingestion (248k crop + 4k rainfall rows)
- DuckDB in-memory SQL engine
- Local LLM (Llama 3.1 8B) → SQL generation
- Streamlit web UI
- Full citation traceability

## Run Locally
```bash
# 1. Start LLM
ollama run llama3.1:8b

# 2. Run app
streamlit run src/app.py
