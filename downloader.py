from multiprocessing import Pool
import os
import requests
import logging

import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def download(url: str) -> bytes:
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Download the file data into memory
        file_data = b''
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file_data += chunk
                    
        return file_data
        
    except requests.exceptions.RequestException as e:
        raise e
    
def download_save(url: str, save_path: str, check_exists: bool = True) -> None:
    if check_exists and os.path.exists(save_path):
        return
    
    data = download(url)
    with open(save_path, 'wb') as f:
        f.write(data)
        
def sync_download_save_many(urls: list[str], save_paths: list[str]) -> list[str]:
    """Downloads multiple files from URLs and saves them to specified paths.

    Args:
        urls: List of URLs to download from
        save_paths: List of file paths to save the downloaded files to
        
    Returns:
        List of URLs that failed to download
        
    Raises:
        ValueError: If urls and save_paths have different lengths
    """
    if len(urls) != len(save_paths):
        raise ValueError("urls and save_paths must have the same length")
    
    undownloaded_urls = []

    for url, save_path in zip(urls, save_paths):
        filename = url.split('/')[-1]
        try:
            logger.info(f"Downloading {filename}")
            download_save(url, save_path)
            logger.info(f"Downloaded {filename}")
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            undownloaded_urls.append(url)
            
    return undownloaded_urls

def multi_thread_download_save_many(urls: list[str], save_paths: list[str]) -> list[str]:
    """Downloads multiple files from URLs and saves them to specified paths using multiple threads.

    Args:
        urls: List of URLs to download from
        save_paths: List of file paths to save the downloaded files to
        
    Returns:
        List of URLs that failed to download
        
    Raises:
        ValueError: If urls and save_paths have different lengths
    """
    if len(urls) != len(save_paths):
        raise ValueError("urls and save_paths must have the same length")

    # Group URLs and save paths into chunks based on max_workers
    url_chunks = [urls[i:i + config.max_workers] for i in range(0, len(urls), config.max_workers)]
    save_path_chunks = [save_paths[i:i + config.max_workers] for i in range(0, len(save_paths), config.max_workers)]

    undownloaded_urls = []
    
    with Pool(processes=config.max_workers) as pool:
        chunks = zip(url_chunks, save_path_chunks)
        undownloaded_urls = pool.map(sync_download_save_many, chunks)
            
    return undownloaded_urls


if __name__ == "__main__":
    # Just for testing
    data = download("https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-11-19.zip")
    print(len(data))
