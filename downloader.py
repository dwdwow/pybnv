from multiprocessing import Pool
import os, requests, logging, config
from urllib.parse import urlparse

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

def download(url: str) -> bytes:
    response = requests.get(url, stream=True)
    response.raise_for_status()
    file_data = b''
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            file_data += chunk
    return file_data


def download_save(url: str, save_dir: str, check_exists: bool = True) -> None:
    parsed_url = urlparse(url)
    filename = parsed_url.path.split('/')[-1]
    save_path = os.path.join(save_dir, filename)
    if check_exists and os.path.exists(save_path):
        return
    os.makedirs(save_dir, exist_ok=True)
    data = download(url)
    with open(save_path, 'wb') as f:
        f.write(data)
        

def _download_save_wrapper(url: str,save_dir: str,check_exists: bool = True) -> str:
    try:
        _logger.debug(f"Downloading {url}")
        download_save(url, save_dir, check_exists)
        _logger.debug(f"Downloaded {url}")
        return None
    except Exception:
        _logger.error(f"Failed to download {url}")
        return url
        

def multi_proc_download_save(
        urls: list[str],
        save_dir: str,
        check_exists: bool = True,
        max_workers: int = config.max_workers
    ) -> list[str]:
    with Pool(processes=max_workers) as pool:
        undownloads = pool.starmap(_download_save_wrapper, [(url, save_dir, check_exists) for url in urls])
    return [url for url in undownloads if url]


def multi_proc_download_save_until_success(
        urls: list[str],
        save_dir: str,
        check_exists: bool = True,
        max_workers: int = config.max_workers,
    ) -> None:
    undownloaded_urls = urls
    while undownloaded_urls:
        undownloaded_urls = multi_proc_download_save(undownloaded_urls, save_dir, check_exists, max_workers)


if __name__ == "__main__":
    # Just for testing
    urls = [
        "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2023-11-19.zip",
        "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2023-11-20.zip",
        "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2023-11-21.zip",
        "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2023-11-22.zip",
        "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2023-11-23.zip",
        "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2023-11-24.zip"
        ]
    multi_proc_download_save_until_success(urls, config.data_binance_vision_dir + "/data/spot/daily/aggTrades/BTCUSDT/")
