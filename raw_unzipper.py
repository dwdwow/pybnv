import logging
from multiprocessing import Pool
import os

import zipper
import config

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


def unzip_file_to_dir(file_path: str, save_dir: str, check_exists: bool = True) -> None:
    save_path = os.path.join(save_dir, os.path.basename(file_path).replace(".zip", ".csv"))
    if check_exists and os.path.exists(save_path):
        return
    
    os.makedirs(save_dir, exist_ok=True)
    
    _logger.info(f"Unzipping {file_path} to {save_path}")
    zipper.unzip_file_save(file_path, save_path)
    _logger.info(f"Unzipped {file_path} to {save_path}")
    clear_file(file_path)


def clear_file(file_path: str) -> None:
    """
    Clear the contents of a zip file without deleting the file itself.

    Args:
        file_path: Path to the zip file to clear
    """
    _logger.info(f"Clearing contents of {file_path}")
    with open(file_path, 'wb') as f:
        f.truncate(0)
    _logger.info(f"Cleared contents of {file_path}")
    

def clear_dir(dir_path: str) -> None:
    """
    Clear all files in a directory and its subdirectories without deleting the directories themselves.

    Args:
        dir_path: Path to the directory to clear
    """
    _logger.info(f"Clearing contents of directory {dir_path}")
    for root, _, files in os.walk(dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.isfile(file_path):
                with open(file_path, 'wb') as f:
                    f.truncate(0)
                _logger.info(f"Cleared contents of {file_path}")
    

def multi_proc_unzip_one_dir_files_to_dir(zip_dir: str, save_dir: str, check_exists: bool = True, max_workers: int = config.max_workers) -> None:
    with Pool(processes=max_workers) as pool:
        pool.starmap(unzip_file_to_dir, [(os.path.join(zip_dir, name), save_dir, check_exists) for name in os.listdir(zip_dir) if name.endswith(".zip")])
        

if __name__ == "__main__":
    # path = "/data/spot/daily/aggTrades/PEPEUSDT"
    # zip_dir = config.data_binance_vision_dir + path
    # save_dir = config.unzip_binance_vision_dir + path
    # multi_proc_unzip_one_dir_files_to_dir(zip_dir, save_dir, check_exists=True)
    clear_file(config.data_binance_vision_dir)
    clear_dir(config.unzip_binance_vision_dir)

