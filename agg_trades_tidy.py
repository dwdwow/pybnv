import logging
import os
import agg_trades_checker
import config
from enums import SymbolType
import raw_downloader
import raw_unzipper

_logger = logging.getLogger(__name__)

def tidy_one_symbol(
        syb_type: SymbolType,
        symbol: str,
        raw_root_dir: str = config.data_binance_vision_dir,
        unzip_root_dir: str = config.unzip_binance_vision_dir,
        missing_root_dir: str = config.missing_binance_vision_dir,
        tidy_root_dir: str = config.tidy_binance_vision_dir
        ):
    prefix = f"data/{syb_type.value}/daily/aggTrades/{symbol}"
    
    raw_downloader.download(prefix, "", raw_root_dir)
    
    raw_unzipper.multi_proc_unzip_one_dir_files_to_dir(raw_root_dir, unzip_root_dir)
    
    file_names = os.listdir(tidy_root_dir)

    last_file_name = None
    if file_names:
        file_names.sort()
        last_file_name = file_names[-1]
    
    missing_ids = agg_trades_checker.multi_proc_check_one_dir_consistency(unzip_root_dir, config.agg_trades_headers, start_file_name=last_file_name)
    
    agg_trades_checker.download_missing_trades_and_save(syb_type, symbol, missing_ids, missing_root_dir)
    
    agg_trades_checker.multi_proc_merge_one_symbol_raw_and_missing_trades(syb_type, symbol)
    
    missing_ids = agg_trades_checker.multi_proc_check_one_dir_consistency(tidy_root_dir, config.agg_trades_headers, start_file_name=last_file_name)
    
    if missing_ids:
        _logger.error(f"Missing trades found for {symbol} in {tidy_root_dir}")
