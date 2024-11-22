import os
from downloader import multi_thread_download_save_many_until_success
from xmler import query_vision_xml_file_paths
from zipper import is_valid_zip, unzip
import config

def download(prefix: str, market: str) -> None:
    file_paths = query_vision_xml_file_paths(prefix, market)
    urls = [f"https://data.binance.vision/{file_path}" for file_path in file_paths]
    save_paths = [os.path.join(config.data_binance_vision_dir, file_path) for file_path in file_paths]
    multi_thread_download_save_many_until_success(urls, save_paths, is_valid_zip)
        
        
if __name__ == "__main__":
    prefix = "data/spot/daily/aggTrades/BTCUSDT"
    market = "BTCUSDT"
    download(prefix, market)
