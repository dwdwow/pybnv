from enum import Enum
import os
import agg_trades_checker
import config
from enums import SymbolType
import raw_downloader
import raw_unzipper

def tidy_one_symbol(
        syb_type: SymbolType,
        symbol: str,
        raw_dir: str = config.data_binance_vision_dir,
        unzip_dir: str = config.unzip_binance_vision_dir,
        missing_dir: str = config.missing_binance_vision_dir,
        tidy_dir: str = config.tidy_binance_vision_dir
        ):
    prefix = f"data/{syb_type.value}/daily/aggTrades/{symbol}"
    
    raw_downloader.download(prefix, "", raw_dir)
    
    raw_unzipper.multi_proc_unzip_one_dir_files_to_dir(raw_dir, unzip_dir)
    
    file_names = os.listdir(tidy_dir)

    last_file_name = None
    if file_names:
        file_names.sort()
        last_file_name = file_names[-1]
    
    missing_ids = agg_trades_checker.multi_proc_check_one_dir_consistency(unzip_dir, ["id"], start_file_name=last_file_name)
    
    agg_trades_checker.download_missing_trades_and_save(syb_type, symbol, missing_ids, missing_dir)
    
    agg_trades_checker.multi_proc_merge_one_symbol_raw_and_missing_trades(syb_type, symbol)
