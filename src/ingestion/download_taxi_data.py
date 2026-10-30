from pathlib import Path
from urllib.request import urlretrieve

DATA_URL = (
    'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    'https://d37ci6vzurychx.cloudfront.net/trip-data/'
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / 'data' / 'raw'
OUTPUT_FILE = RAW_DATA_DIR / 'yellow_tripdata_2024-01.parquet'

def download_taxi_data():
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_FILE.exists():
        print('Dataset already downloaded.')
        return
    print('Downloading NYC taxi data...')
    urlretrieve(DATA_URL, OUTPUT_FILE)
    print(f'Download complete: {OUTPUT_FIILE}')

if __name__ == '__main__':
    download_taxi_data()