from downloader import download
from zipper import unzip

def download_unzip(url: str) -> bytes:
    """
    Download a zip file from URL, validate it, and return unzipped contents.
    
    Args:
        url: URL of the zip file to download
        
    Returns:
        The unzipped file contents as bytes
        
    Raises:
        requests.exceptions.RequestException: If download fails
        ValueError: If downloaded file is not a valid zip
        zipfile.BadZipFile: If zip file is corrupted or invalid
    """
    # Download the file
    _, data = download(url)
        
    # Unzip and return contents
    return unzip(data)

def download_unzip_save(url: str, save_path: str) -> None:
    """
    Download a zip file from URL, unzip it, and save contents to file.
    
    Args:
        url: URL of the zip file to download
        save_path: Path where the unzipped file should be saved
        
    Raises:
        requests.exceptions.RequestException: If download fails
        ValueError: If downloaded file is not a valid zip
        zipfile.BadZipFile: If zip file is corrupted or invalid
        OSError: If there are issues writing the output file
    """
    # Download and unzip
    contents = download_unzip(url)
    
    # Save to file
    with open(save_path, 'wb') as f:
        f.write(contents)
        
if __name__ == "__main__":
    download_unzip_save("https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-11-19.zip", "BTCUSDT-aggTrades-2024-11-19.csv")
