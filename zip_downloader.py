import os
from downloader import download
from zipper import is_valid_zip, unzip
import config

def zip_download_verify_save(url: str, save_dir: str) -> None:
    """
    Download a zip file from URL, validate it, and save contents to file.
    
    Args:
        url: URL of the zip file to download
        save_path: Path where the zip file should be saved

    Raises:
        requests.exceptions.RequestException: If download fails
        ValueError: If downloaded file is not a valid zip
        zipfile.BadZipFile: If zip file is corrupted or invalid
    """
    # Download the file
    data = download(url)
    
    if not is_valid_zip(data):
        raise ValueError("Downloaded file is not a valid zip")
    
    os.makedirs(save_dir, exist_ok=True)

    filename = url.split('/')[-1]

    save_path = os.path.join(save_dir, filename)
        
    # Save to file
    with open(save_path, 'wb') as f:
        f.write(data)
        
        
if __name__ == "__main__":
    prefix = "data/spot/daily/aggTrades/BTCUSDT"
    url = f"https://data.binance.vision/{prefix}/BTCUSDT-aggTrades-2024-11-19.zip"
    save_dir = config.data_binance_vision_dir + "/" + prefix
    zip_download_verify_save(url, save_dir)
