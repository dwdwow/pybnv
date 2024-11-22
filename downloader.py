from multiprocessing import Pool
import os, requests, logging, config
from urllib.parse import urlparse
from typing import Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    

def download_save(
        url: str,
        save_dir: str,
        data_checker: Callable[[bytes], bool] = lambda _: True,
        check_exists: bool = True
    ) -> None:
    """Downloads a file from a URL and saves it to a specified path.

    Args:
        url: URL to download from
        save_path: File path to save the downloaded file to
        data_checker: Optional function to validate downloaded data. Takes bytes as input and returns bool.
                     Defaults to always returning True.
        check_exists: If True, skip download if file already exists at save_path. Defaults to True.

    Raises:
        requests.exceptions.RequestException: If download fails
        ValueError: If downloaded data fails validation check
    """
    parsed_url = urlparse(url)
    filename = parsed_url.path.split('/')[-1]
    save_path = os.path.join(save_dir, filename)
    
    if check_exists and os.path.exists(save_path):
        return
    
    os.makedirs(save_dir, exist_ok=True)
    
    data = download(url)

    if not data_checker(data):
        raise ValueError(f"Downloaded data is not valid")
    
    with open(save_path, 'wb') as f:
        f.write(data)
        

def download_save_many(
        urls: list[str],
        save_dir: str,
        data_checker: Callable[[bytes], bool] = lambda _: True
    ) -> list[str]:
    """Downloads multiple files from URLs and saves them to specified paths.

    Args:
        urls: List of URLs to download from
        save_dir: Directory to save the downloaded files to
        data_checker: Optional function to validate downloaded data. Takes bytes as input and returns bool.
                     Defaults to always returning True.
        
    Returns:
        List of URLs that failed to download
        
    Raises:
        ValueError: If urls and save_paths have different lengths
    """
    undownloaded_urls = []

    for url in urls:
        filename = url.split('/')[-1]
        try:
            download_save(url, save_dir, data_checker)
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            undownloaded_urls.append(url)
            
    return undownloaded_urls


def multi_proc_download_save_many(
        urls: list[str],
        save_dir: str,
        data_checker: Callable[[bytes], bool] = lambda _: True,
    ) -> list[str]:
    """Downloads multiple files from URLs and saves them to specified paths using multiple processes.
    
    Max process number is config.max_workers

    Args:
        urls: List of URLs to download from
        save_dir: Directory to save the downloaded files to
        data_checker: Optional function to validate downloaded data. Takes bytes as input and returns bool.
                     Defaults to always returning True.
        
    Returns:
        List of URLs that failed to download
        
    """
    group_num = len(urls) // config.max_workers + 1
    url_chunks = [urls[i:i + group_num] for i in range(0, len(urls), group_num)]

    undownloaded_urls = []
    
    with Pool(processes=config.max_workers) as pool:
        args = [(url_chunk, save_dir, data_checker) for url_chunk in url_chunks]
        undownloads = pool.starmap(download_save_many, args)
        for chunk in undownloads:
            undownloaded_urls.extend(chunk)
            
    return undownloaded_urls


def multi_proc_download_save_many_until_success(
        urls: list[str],
        save_dir: str,
        data_checker: Callable[[bytes], bool] = lambda _: True,
    ) -> None:
    undownloaded_urls = urls
    while undownloaded_urls:
        undownloaded_urls = multi_proc_download_save_many(undownloaded_urls, save_dir, data_checker)


if __name__ == "__main__":
    # Just for testing
    urls = ["https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-11-19.zip"]
    save_dir = config.data_binance_vision_dir + "/data/spot/daily/aggTrades/BTCUSDT/"
    undownloaded_urls = download_save_many(urls, save_dir)
    print(undownloaded_urls)
