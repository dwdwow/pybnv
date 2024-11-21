import os
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
        
def download_unzip_save_many(urls: list[str], save_paths: list[str]) -> None:
    """
    Download multiple zip files from URLs, unzip them, and save contents to files.
    
    Args:
        urls: List of URLs of zip files to download
        save_paths: List of paths where the unzipped files should be saved
        
    Raises:
        ValueError: If urls and save_paths have different lengths
        requests.exceptions.RequestException: If any download fails
        ValueError: If any downloaded file is not a valid zip
        zipfile.BadZipFile: If any zip file is corrupted or invalid
        OSError: If there are issues writing any output file
    """
    if len(urls) != len(save_paths):
        raise ValueError("Number of URLs must match number of save paths")
        
    for url, save_path in zip(urls, save_paths):
        download_unzip_save(url, save_path)

        
if __name__ == "__main__":
    # Create directory if it doesn't exist
    os.makedirs("/Users/dingwendi/test/aaa", exist_ok=True)
    download_unzip_save("https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-11-19.zip", "/Users/dingwendi/test/aaa/BTCUSDT-aggTrades-2024-11-19.csv")
