import logging
from downloader import multi_proc_download_save_many_until_success
from xmler import query_vision_xml_file_paths
from zipper import is_valid_zip 
import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download(prefix: str, market: str, save_dir: str) -> None:
    logger.info(f"Downloading {prefix} {market} XML, And Getting File Paths")
    file_paths = query_vision_xml_file_paths(prefix, market)
    file_paths = [path for path in file_paths if path.endswith('.zip')]
    logger.info(f"Found {len(file_paths)} files")
    urls = [f"https://data.binance.vision/{file_path.strip("/")}" for file_path in file_paths]
    logger.info(f"Downloading {prefix} {market} Files")
    multi_proc_download_save_many_until_success(urls, save_dir, is_valid_zip)
    logger.info(f"Downloaded {prefix} {market} Files")
        
        
if __name__ == "__main__":
    prefix = "data/spot/daily/aggTrades/PEPEUSDT"
    save_dir = config.data_binance_vision_dir + "/" + prefix
    download(prefix, "", save_dir)
