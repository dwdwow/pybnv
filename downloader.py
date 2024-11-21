import requests
import re

def download(url: str) -> tuple[str, bytes]:
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Get filename from URL or Content-Disposition header
        filename = url.split('/')[-1]
        if 'Content-Disposition' in response.headers:
            if d := re.findall("filename=(.+)", response.headers['Content-Disposition']):
                filename = d[0].strip('"')
        
        # Download the file data into memory
        file_data = b''
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file_data += chunk
                    
        return filename, file_data
        
    except requests.exceptions.RequestException as e:
        raise e


if __name__ == "__main__":
    # Just for testing
    filename, data = download("https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-11-19.zip")
    print(filename)
    print(len(data))
