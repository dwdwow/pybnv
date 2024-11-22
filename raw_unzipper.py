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
    

def multi_proc_unzip_one_dir_files_to_dir(zip_dir: str, save_dir: str, check_exists: bool = True, max_workers: int = config.max_workers) -> None:
    with Pool(processes=max_workers) as pool:
        pool.starmap(unzip_file_to_dir, [(os.path.join(zip_dir, name), save_dir, check_exists) for name in os.listdir(zip_dir) if name.endswith(".zip")])
        

if __name__ == "__main__":
    path = "/data/spot/daily/aggTrades/PEPEUSDT"
    zip_dir = config.data_binance_vision_dir + path
    save_dir = config.unzip_binance_vision_dir + path
    multi_proc_unzip_one_dir_files_to_dir(zip_dir, save_dir, check_exists=True)
