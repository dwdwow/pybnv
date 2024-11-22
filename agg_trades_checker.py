import csv
import datetime
import logging
from multiprocessing import Pool
import os
import pandas as pd

from api_downloader import download_spot_agg_trades_by_ids
import config
import csv_util


logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


def check_consistency(frame: pd.DataFrame) -> list[int]:
    ids = frame["id"].to_numpy()

    # Check for missing ids by finding where difference is > 1
    id_diffs = ids[1:] - ids[:-1]
    missing_indices = id_diffs > 1
    
    # If no gaps found, return empty list
    if not any(missing_indices):
        return []
    
    # Get the starting ids where gaps occur
    gap_starts = ids[:-1][missing_indices]
    gap_sizes = id_diffs[missing_indices] - 1
    
    # Generate all missing ids
    missing_ids = []
    for start, size in zip(gap_starts, gap_sizes):
        missing_ids.extend(range(start + 1, start + size + 1))
        
    return missing_ids


def check_one_file_consistency(file_path: str, headers: list[str]) -> tuple[int, int, list[int]]:
    """Check consistency of IDs in a single CSV file.

    Args:
        file_path: Path to the CSV file to check
        headers: List of column headers expected in the CSV file

    Returns:
        Tuple containing:
            - start_id: First ID in the file
            - end_id: Last ID in the file 
            - missing_ids: List of any missing IDs between start and end
    """
    with open(file_path, "r") as f:
        _logger.info(f"Checking {file_path} for consistency")
        df = csv_util.csv_to_pandas(f, headers)
        if df.empty:
            _logger.warning(f"File {file_path} is empty")
            return 0, 0, []
        start_id = df["id"].min()
        end_id = df["id"].max()
        missing_ids = check_consistency(df)
        if len(missing_ids) > 0:
            _logger.info(f"Found {len(missing_ids)} missing IDs in {file_path}")
        return start_id, end_id, missing_ids
            

def multi_proc_check_one_dir_consistency(dir_path: str, headers: list[str], start_file_name: str = "", max_workers: int = config.max_workers) -> list[int]:
    """Check consistency of IDs across multiple CSV files using multiprocessing.

    Args:
        file_paths: List of paths to CSV files to check
        headers: List of column headers expected in the CSV files
        max_workers: Maximum number of worker processes to use (default: from config)

    Returns:
        List of missing IDs found across all files. The files are processed in order
        and gaps between files are included in the missing IDs list.
    """
    infos: list[tuple[int, int, list[int]]] = []
    with Pool(processes=max_workers) as pool:
        infos = pool.starmap(check_one_file_consistency, [(os.path.join(dir_path, name), headers) for name in os.listdir(dir_path) if name.endswith(".csv") and (start_file_name == "" or name >= start_file_name)])
    
    infos.sort(key=lambda x: x[0])
    
    if len(infos) <= 1:
        return infos[0][2]
    
    last_end_id = infos[0][1]
    missing_ids = [id for id in infos[0][2]]
    for info in infos[1:]:
        start_id, end_id, ids = info
        if start_id != last_end_id + 1:
            missing_ids.extend(range(last_end_id + 1, start_id))
        missing_ids.extend(ids)
        last_end_id = end_id

    return missing_ids


def group_missing_ids(missing_ids: list[int]) -> list[list[int]]:
    groups = []
    for id in missing_ids:
        if len(groups) == 0 or id != groups[-1][-1] + 1:
            groups.append([id])
        else:
            groups[-1].append(id)
    return groups


def download_missing_trades(symbol: str, missing_ids: list[int]) -> list[dict]:
    grouped_missing_ids = group_missing_ids(missing_ids)
    trades = []
    for group in grouped_missing_ids:
        trades.extend(download_spot_agg_trades_by_ids(symbol, group[0], group[-1]))
    return trades


def group_trades_by_date_save(symbol: str, trades: list[dict], save_dir: str, check_file_exists: bool = True) -> None:
    if not trades:
        return
    
    trades.sort(key=lambda x: x["a"])
        
    # Group trades by date
    trades_by_date = {}
    for trade in trades:
        # Convert timestamp to date string YYYY-MM-DD
        date = datetime.datetime.fromtimestamp(trade["T"]//1000, datetime.timezone.utc).strftime("%Y-%m-%d")
        if date not in trades_by_date:
            trades_by_date[date] = []
        trades_by_date[date].append(trade)
        
    os.makedirs(save_dir, exist_ok=True)
    
    # Save trades for each date
    for date, date_trades in trades_by_date.items():
        filename = f"{symbol}-aggTrades-{date}.csv"
        filepath = os.path.join(save_dir, filename)
        
        # Convert date_trades list to pandas DataFrame
        date_trades_df = pd.DataFrame(date_trades, columns=csv_util.agg_trades_api_data_headers)
        
        date_trades_df.columns = csv_util.agg_trades_headers.copy()

        # If file exists, read existing data and append new trades
        if check_file_exists and os.path.exists(filepath):
            _logger.info(f"File {filepath} already exists, skipping")
            continue
                
        # Sort by trade ID
        date_trades_df.sort_values(by="id", inplace=True, key=lambda x: x.astype(int)) 
        
        # Remove any duplicates based on trade ID
        date_trades_df.drop_duplicates(subset="id", keep="first", inplace=True)
        
        # Write sorted DataFrame back to CSV
        date_trades_df.to_csv(filepath, index=False)
        _logger.info(f"Saved {len(date_trades_df)} trades to {filepath}")
        return


def download_missing_trades_and_save(symbol: str, missing_ids: list[int], save_dir: str) -> None:
    trades = download_missing_trades(symbol, missing_ids)
    group_trades_by_date_save(symbol, trades, save_dir)
    

def merge_raw_and_missing_trades(file_name: str, raw_dir: str, missing_dir: str, save_dir: str, check_tidy_file_exists: bool = True) -> None:
    tidy_path = os.path.join(save_dir, file_name)
    if check_tidy_file_exists and os.path.exists(tidy_path):
        _logger.info(f"Tidy file {tidy_path} already exists, skipping")
        return

    raw_path = os.path.join(raw_dir, file_name)
    raw_df = None
    with open(raw_path, "r") as f:
        raw_df = csv_util.csv_to_pandas(f, csv_util.agg_trades_headers)

    os.makedirs(save_dir, exist_ok=True)

    missing_path = os.path.join(missing_dir, file_name)
    if not os.path.exists(missing_path):
        _logger.info(f"No missing trades file for {file_name}, copying raw file to tidy file")
        raw_df.to_csv(tidy_path, index=False)
        _logger.info(f"Saved raw trades to {tidy_path}")
        return
    
    _logger.info(f"Merging {file_name} raw and missing trades")
    
    missing_df = None
    with open(missing_path, "r") as f:
        missing_df = csv_util.csv_to_pandas(f, csv_util.agg_trades_headers)
    
    merged_df = pd.concat([raw_df, missing_df])
    merged_df.sort_values(by="id", inplace=True, key=lambda x: x.astype(int))
    

    merged_df.to_csv(tidy_path, index=False)

    _logger.info(f"Saved merged trades to {os.path.join(save_dir, file_name)}")
    

def multi_proc_merge_one_dir_raw_and_missing_trades(raw_dir: str, missing_dir: str, save_dir: str, max_workers: int = config.max_workers) -> None:
    with Pool(processes=max_workers) as pool:
        pool.starmap(merge_raw_and_missing_trades, [(file_name, raw_dir, missing_dir, save_dir) for file_name in os.listdir(raw_dir)])
        

def multi_proc_merge_one_symbol_raw_and_missing_trades(pair_type: str, symbol: str, max_workers: int = config.max_workers) -> None:
    """Merge raw and missing trades for one symbol using multiple processes.

    This function merges raw trades from unzipped files with any missing trades that were downloaded
    separately, saving the combined results to the tidy directory. It processes all files for a given
    symbol in parallel using multiple worker processes.

    Args:
        pair_type (str): The trading pair type (e.g. "spot", "futures/um", "futures/cm")
        symbol (str): The trading symbol (e.g. "BTCUSDT")
        max_workers (int, optional): Maximum number of worker processes to use. Defaults to config.max_workers.
    """
    prefix = f"data/{pair_type}/daily/aggTrades/{symbol}"
    raw_dir = os.path.join(config.unzip_binance_vision_dir, prefix)
    missing_dir = os.path.join(config.missing_binance_vision_dir, prefix)
    save_dir = os.path.join(config.tidy_binance_vision_dir, prefix)
    multi_proc_merge_one_dir_raw_and_missing_trades(raw_dir, missing_dir, save_dir, max_workers)
    

if __name__ == "__main__":
    multi_proc_merge_one_symbol_raw_and_missing_trades("spot", "PEPEUSDT")    
