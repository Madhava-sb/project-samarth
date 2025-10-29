# src/clean.py
"""
Project Samarth - Phase 1B: Data Cleaning + DuckDB SQL Engine
- Standardize state names
- Melt rainfall (wide → long)
- Load CSVs into DuckDB (in-memory)
- Run sample SQL queries
"""

import duckdb
import pandas as pd
from pathlib import Path
import pyarrow

# === PATHS ===
RAW_DIR = Path("../data/raw")
PROC_DIR = Path("../data/processed")
PROC_DIR.mkdir(exist_ok=True)

CROP_RAW = RAW_DIR / "crop_production_raw.csv"
RAIN_RAW = RAW_DIR / "rainfall_subdiv_monthly_raw.csv"

# === DUCKDB CONNECTION (in-memory) ===
con = duckdb.connect(database=':memory:')

# === 1. LOAD & CLEAN CROP DATA ===
print("Cleaning crop data...")
crop_df = pd.read_csv(CROP_RAW)

# Standardize column names
crop_df = crop_df.rename(columns={
    "State_Name": "state",
    "District_Name": "district",
    "Crop": "crop",
    "Season": "season",
    "Crop_Year": "year",
    "Area": "area_hectare",
    "Production": "production_tonnes"
})

# Clean state names
state_mapping = {
    "ANDHRA PRADESH": "Andhra Pradesh",
    "ARUNACHAL PRADESH": "Arunachal Pradesh",
    "ASSAM": "Assam",
    "BIHAR": "Bihar",
    "CHHATTISGARH": "Chhattisgarh",
    "GOA": "Goa",
    "GUJARAT": "Gujarat",
    "HARYANA": "Haryana",
    "HIMACHAL PRADESH": "Himachal Pradesh",
    "JAMMU AND KASHMIR": "Jammu & Kashmir",
    "JHARKHAND": "Jharkhand",
    "KARNATAKA": "Karnataka",
    "KERALA": "Kerala",
    "MADHYA PRADESH": "Madhya Pradesh",
    "MAHARASHTRA": "Maharashtra",
    "MANIPUR": "Manipur",
    "MEGHALAYA": "Meghalaya",
    "MIZORAM": "Mizoram",
    "NAGALAND": "Nagaland",
    "ODISHA": "Odisha",
    "ORISSA": "Odisha",
    "PUNJAB": "Punjab",
    "RAJASTHAN": "Rajasthan",
    "SIKKIM": "Sikkim",
    "TAMIL NADU": "Tamil Nadu",
    "TRIPURA": "Tripura",
    "UTTAR PRADESH": "Uttar Pradesh",
    "UTTARAKHAND": "Uttarakhand",
    "WEST BENGAL": "West Bengal",
    "ANDAMAN AND NICOBAR ISLANDS": "Andaman & Nicobar",
    "DADRA AND NAGAR HAVELI": "Dadra & Nagar Haveli",
    "DAMAN AND DIU": "Daman & Diu",
    "DELHI": "Delhi",
    "LAKSHADWEEP": "Lakshadweep",
    "PONDICHERRY": "Puducherry",
    "CHANDIGARH": "Chandigarh",
    "TELANGANA": "Telangana"
}
crop_df['state'] = crop_df['state'].str.strip().str.upper().map(state_mapping).fillna(crop_df['state'])

# Save cleaned
crop_clean_path = PROC_DIR / "crop_clean.parquet"
crop_df.to_parquet(crop_clean_path, index=False)
print(f"Cleaned crop → {crop_clean_path}")

# === 2. LOAD & MELT RAINFALL ===
print("Melting rainfall data...")
rain_df = pd.read_csv(RAIN_RAW)

# Standardize
rain_df = rain_df.rename(columns={"SUBDIVISION": "subdivision", "YEAR": "year"})
rain_df.columns = [col.lower() for col in rain_df.columns]

# Melt monthly columns
monthly_cols = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
rain_long = rain_df.melt(
    id_vars=['subdivision', 'year', 'annual'],
    value_vars=monthly_cols,
    var_name='month',
    value_name='rainfall_mm'
)

# Map month name to number
month_map = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
             'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
rain_long['month'] = rain_long['month'].map(month_map)

# Save
rain_clean_path = PROC_DIR / "rainfall_long.parquet"
rain_long.to_parquet(rain_clean_path, index=False)
print(f"Melted rainfall → {rain_clean_path}")

# === 3. LOAD INTO DUCKDB ===
print("Loading into DuckDB...")
con.execute(f"CREATE TABLE crop AS SELECT * FROM '{crop_clean_path}'")
con.execute(f"CREATE TABLE rainfall AS SELECT * FROM '{rain_clean_path}'")

# === 4. SAMPLE SQL QUERIES ===
print("\nSAMPLE QUERIES:")

# Q1: Top 5 rice-producing states (2010–2015)
q1 = """
SELECT state, ROUND(AVG(production_tonnes), 0) AS avg_production
FROM crop
WHERE crop = 'Rice' AND year BETWEEN 2010 AND 2015
GROUP BY state
ORDER BY avg_production DESC
LIMIT 5
"""
print("\nTop 5 Rice States (2010–2015):")
print(con.execute(q1).df())

# Q2: District with highest Wheat in andhara pradesh (latest year)
q2 = """
SELECT district, production_tonnes, year
FROM crop
WHERE state = 'Andhra Pradesh' AND crop = 'Wheat'
ORDER BY year DESC, production_tonnes DESC
"""
print("\nHighest Wheat District in Andhra Pradesh:")
print(con.execute(q2).df())

# Q3: Rainfall trend in Maharashtra (annual)
q3 = """
SELECT year, ROUND(AVG(rainfall_mm), 1) AS avg_rainfall_mm
FROM rainfall
WHERE subdivision LIKE '%Madhya Maharashtra%'
GROUP BY year
ORDER BY year DESC
"""
print("\nRecent Rainfall in Maharashtra:")
print(con.execute(q3).df())

print("\nPHASE 1B COMPLETE: Cleaned + DuckDB Ready")
print("Next: Phase 2 — Q&A System with LLM → SQL")