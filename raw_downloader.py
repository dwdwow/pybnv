import logging
from downloader import multi_proc_download_save_until_success
from xmler import query_vision_xml_file_paths
import config

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

def multi_proc_download(prefix: str, marker: str, save_dir: str, check_exists: bool = True, max_workers: int = config.max_workers) -> None:
    _logger.debug(f"Downloading {prefix} {marker} XML, And Getting File Paths")
    file_paths = query_vision_xml_file_paths(prefix, marker)
    file_paths = [path for path in file_paths if path.endswith('.zip')]
    _logger.debug(f"Found {len(file_paths)} files")
    urls = [f"https://data.binance.vision/{file_path.strip("/")}" for file_path in file_paths]
    _logger.debug(f"Downloading {prefix} {marker} Files")
    multi_proc_download_save_until_success(urls, save_dir, check_exists, max_workers    )
    _logger.debug(f"Downloaded {prefix} {marker} Files")
        
        
if __name__ == "__main__":
    prefix = "data/spot/daily/aggTrades/PEPEUSDT"
    save_dir = config.data_binance_vision_dir + "/" + prefix
    multi_proc_download(prefix, "", save_dir)
