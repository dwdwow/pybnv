from multiprocessing import Pool
import os
import pandas as pd

import config
import csv_util


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
        df = csv_util.csv_to_pandas(f, headers)
        if df.empty:
            return 0, 0, []
        start_id = df["id"].min()
        end_id = df["id"].max()
        missing_ids = check_consistency(df)
        return start_id, end_id, missing_ids
            

def multi_proc_check_one_dir_files_consistency(dir_path: str, headers: list[str], max_workers: int = config.max_workers) -> list[int]:
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
        infos = pool.starmap(check_one_file_consistency, [(os.path.join(dir_path, name), headers) for name in os.listdir(dir_path) if name.endswith(".csv")])
    
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

if __name__ == "__main__":
    dir_path = config.unzip_binance_vision_dir + "/data/spot/daily/aggTrades/PEPEUSDT"
    missing_ids = multi_proc_check_one_dir_files_consistency(dir_path, csv_util.agg_trades_headers)
    print(len(missing_ids))

