import os
import requests
from datetime import datetime
from tqdm import tqdm

def download_nyc_311(year, month):
    # 1. Setup constants
    base_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
    file_name = f"nyc_311_{year}_{month:02d}.csv"
    
    # 2. Define Time Range (SoQL)
    start_date = f"{year}-{month:02d}-01T00:00:00"
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    end_date = f"{next_year}-{next_month:02d}-01T00:00:00"

    current_date = datetime.now()
    requested_date = datetime(year, month, 1)
    
    if requested_date > current_date:
        print(f"Error: {year}-{month:02d} is in the future. Data not available.")
        return


    params = {
        "$where": f"created_date >= '{start_date}' AND created_date < '{end_date}'",
        "$limit": 100000000 
    }

    # 3. Stream the download
    print(f"Connecting to NYC Open Data for {year}-{month:02d}...")
    with requests.get(base_url, params=params, stream=True) as r:
        r.raise_for_status()
        
        # SODA API doesn't always provide Content-Length for dynamic queries
        # so the progress bar will show "it/s" instead of a percentage
        with open(file_name, 'wb') as f:
            pbar = tqdm(unit='B', unit_scale=True, desc=file_name)
            for chunk in r.iter_content(chunk_size=32768):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))
            pbar.close()

    print(f"Download complete: {file_name}")

if __name__ == "__main__":
    # Run for January 2024
    download_nyc_311(2024, 3)