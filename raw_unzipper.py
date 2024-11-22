import os

import zipper
import config


def unzip_file_to_dir(file_path: str, save_dir: str, check_exists: bool = True) -> None:
    save_path = os.path.join(save_dir, os.path.basename(file_path).replace(".zip", ".csv"))
    if check_exists and os.path.exists(save_path):
        return
    
    os.makedirs(save_dir, exist_ok=True)
    
    zipper.unzip_file_save(file_path, save_path)
    

def unzip_one_dir_files_to_dir(zip_dir: str, save_dir: str, check_exists: bool = True) -> None:
    for file_path in os.listdir(zip_dir):
        unzip_file_to_dir(os.path.join(zip_dir, file_path), save_dir, check_exists)
        

if __name__ == "__main__":
    path = "/data/spot/monthly/klines/PEPEUSDT/1w"
    zip_dir = config.data_binance_vision_dir + path
    save_dir = config.unzip_binance_vision_dir + path
    unzip_one_dir_files_to_dir(zip_dir, save_dir, check_exists=True)
