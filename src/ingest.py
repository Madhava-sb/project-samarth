# src/ingest.py
"""
Project Samarth - Phase 1A: FULL API PAGINATION DOWNLOADER (FINAL)
- Live data.gov.in REST API
- Pagination: limit=1000
- Streaming CSV → memory safe
- Fixed: io.StringIO + raise RuntimeError
- Retry + backoff
- Progress bar (rows)
"""

import time
from pathlib import Path
import pandas as pd
import requests
from tqdm import tqdm
from io import StringIO

# === CONFIG ===
API_KEY = "579b464db66ec23bdd000001b45b8df8ded0451846a16146805a60d7"
RAW_DIR = Path("../data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# === RESOURCE IDs (CONFIRMED WORKING) ===
CROP_RESOURCE_ID = "35be999b-0208-4354-b557-f6ca9a5355de"   # Crop Production
RAIN_RESOURCE_ID = "8e0bd482-4aba-4d99-9cb9-ff124f6f1c2f"   # Rainfall

LIMIT = 1000
RETRY_DELAY = 3

# === FUNCTIONS ===


def download_full_csv(resource_id: str, filename: str, name: str):
    path = RAW_DIR / filename
    if path.exists():
        print(f"Already cached: {filename}")
        return str(path)

    print(f"Downloading {name} via API (full dataset)...")
    offset = 0
    all_chunks = []
    total_rows = 0

    with tqdm(desc="Rows", unit="k", unit_scale=True) as pbar:
        while True:
            url = f"https://api.data.gov.in/resource/{resource_id}"
            params = {
                "api-key": API_KEY,
                "format": "csv",
                "offset": offset,
                "limit": LIMIT
            }
            success = False
            for attempt in range(3):
                try:
                    response = requests.get(url, params=params, timeout=90)
                    response.raise_for_status()
                    text = response.text.strip()
                    if not text or len(text.splitlines()) <= 1:
                        print("Empty response — end of data.")
                        success = True
                        break

                    chunk = pd.read_csv(StringIO(text))
                    if chunk.empty:
                        success = True
                        break

                    all_chunks.append(chunk)
                    rows = len(chunk)
                    total_rows += rows
                    pbar.update(rows)
                    offset += LIMIT
                    success = True
                    break

                except Exception as e:
                    print(f"Retry {attempt+1}/3 at offset {offset}: {e}")
                    time.sleep(RETRY_DELAY * (2 ** attempt))

            if not success:
                break
            if len(text.splitlines()) <= 1:
                break

    if not all_chunks:
        raise RuntimeError(f"No data fetched for {name}. Check Resource ID or API key.")

    # Save
    full_df = pd.concat(all_chunks, ignore_index=True)
    full_df.to_csv(path, index=False)
    print(f"SAVED: {filename} → {len(full_df):,} rows, {path.stat().st_size / (1024**2):.1f} MB")

    # Source log
    log_path = path.with_suffix(".source.txt")
    with open(log_path, "w") as f:
        f.write(f"API Resource ID: {resource_id}\n")
        f.write(f"Total Rows: {len(full_df)}\n")
        f.write(f"Pages Fetched: {offset // LIMIT}\n")
        f.write(f"Downloaded: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Method: Paginated API (limit={LIMIT})\n")

    return str(path)


# === MAIN ===
if __name__ == "__main__":
    try:
        crop_path = download_full_csv(
            CROP_RESOURCE_ID,
            "crop_production_raw.csv",
            "District-wise Crop Production"
        )
        rain_path = download_full_csv(
            RAIN_RESOURCE_ID,
            "rainfall_subdiv_monthly_raw.csv",
            "Sub-divisional Monthly Rainfall"
        )

        # Preview
        print("\nCROP SAMPLE:")
        print(pd.read_csv(crop_path, nrows=3).to_string(index=False))

        print("\nRAINFALL SAMPLE:")
        print(pd.read_csv(rain_path, nrows=3).to_string(index=False))

        print("\nPHASE 1A COMPLETE: FULL DATA VIA LIVE API")
        print("Next: Phase 1B — DuckDB + SQL Engine (tomorrow)")

    except Exception as e:
        print(f"ERROR: {e}")
        raise