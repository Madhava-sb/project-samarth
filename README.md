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
```

## A loom video has been made for this project as a demo, you can find the video in the belo given link or else you can even find it in the files uploaded 

https://www.loom.com/share/8e989368c5cf4799a9e0b4c00b32352c
