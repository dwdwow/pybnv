import logging
import os
import agg_trades_checker
import config
import csv_util
from enums import SymbolType
import raw_downloader
import raw_unzipper

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

def tidy_one_symbol(
        syb_type: SymbolType,
        symbol: str,
        *,
        start_date: str = "",  # YYYY-MM-DD
        zip_root_dir: str = config.data_binance_vision_dir,
        unzip_root_dir: str = config.unzip_binance_vision_dir,
        missing_root_dir: str = config.missing_binance_vision_dir,
        tidy_root_dir: str = config.tidy_binance_vision_dir,
        max_workers: int = config.max_workers
        ):

    prefix = f"data/{syb_type.value}/daily/aggTrades/{symbol}"
    
    zip_dir = f"{zip_root_dir}/{prefix}"
    unzip_dir = f"{unzip_root_dir}/{prefix}"
    missing_dir = f"{missing_root_dir}/{prefix}"
    tidy_dir = f"{tidy_root_dir}/{prefix}"
    
    os.makedirs(zip_dir, exist_ok=True)
    os.makedirs(unzip_dir, exist_ok=True)
    os.makedirs(missing_dir, exist_ok=True)
    os.makedirs(tidy_dir, exist_ok=True)
    
    marker = ""
    if start_date:
        marker = f"{prefix}/{symbol}-aggTrades-{start_date}.zip"
        
    _logger.debug(f"Downloading {symbol} aggTrades", {"start marker": marker})
    
    raw_downloader.multi_proc_download(prefix, marker, zip_dir, max_workers=max_workers)
    
    raw_unzipper.multi_proc_unzip_one_dir_files_to_dir(zip_dir, unzip_dir, max_workers=max_workers)
    
    last_file_name = ""
    file_names = os.listdir(tidy_dir)
    if file_names:
        file_names.sort()
        last_file_name = file_names[-1]
    
    missing_ids = agg_trades_checker.multi_proc_check_one_dir_consistency(unzip_dir, csv_util.agg_trades_headers, tidy_dir=tidy_dir, start_file_name=last_file_name, max_workers=max_workers)
    
    agg_trades_checker.download_missing_trades_and_save(syb_type, symbol, missing_ids, missing_dir)
    
    agg_trades_checker.multi_proc_merge_one_symbol_raw_and_missing_trades(syb_type, symbol, max_workers=max_workers)
    
    missing_ids = agg_trades_checker.multi_proc_check_one_dir_consistency(tidy_dir, csv_util.agg_trades_headers, start_file_name=last_file_name, max_workers=max_workers)
    
    if missing_ids:
        _logger.error(f"Missing trades found for {symbol} in {tidy_dir}")
        

if __name__ == "__main__":
    coins = ['BTC', 'ETH', 'SOL', 'XRP', 'BNB', 'ADA', 'DOGE', 'PEPE', 'SUI']
    symbols_usdt = [f"{coin}USDT" for coin in coins]
    symbols_usdc = [f"{coin}USDC" for coin in coins]
    symbols = symbols_usdt + symbols_usdc
    for symbol in symbols:
        tidy_one_symbol(SymbolType.SPOT, symbol)
