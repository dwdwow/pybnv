from multiprocessing import Pool
import os
import pandas as pd

import config
from enums import SymbolType


def check_one_file_klines_interval(klines_file_path: str, interval_seconds: int) -> list[int]:
    with open(klines_file_path, "r") as f:
        df = pd.read_csv(f)

        if df.empty:
            return []

        df["openTime"] = pd.to_numeric(df["openTime"])
        df["closeTime"] = pd.to_numeric(df["closeTime"])
        
        # Convert interval to milliseconds
        interval_ms = interval_seconds * 1000

        # Calculate time differences between close and open times
        time_diffs = df["closeTime"] - df["openTime"]
        
        # Expected difference is interval_ms - 1 (since closeTime is inclusive)
        expected_diff = interval_ms - 1
        
        # Find any intervals with incorrect duration
        invalid_intervals = time_diffs != expected_diff
        
        invalid_ts: list[int] = []
        
        i = 0
        for invalid in invalid_intervals:
            if invalid:
                invalid_ts.append(df.at[i, "openTime"])
            i += 1
        
        return invalid_ts
        
        
def check_one_file_klines_consistency(klines_file_path: str) -> list[int]:
    with open(klines_file_path, "r") as f:
        df = pd.read_csv(f)
        
        if df.empty:
            return []

        df["openTime"] = pd.to_numeric(df["openTime"])
        df["closeTime"] = pd.to_numeric(df["closeTime"])

        # Calculate expected open times based on previous close times
        # Shift closeTime up by 1 row to compare with next row's openTime
        expected_open_times = df["closeTime"].shift(1) + 1

        # First row has no previous close time to compare against
        expected_open_times.iloc[0] = df["openTime"].iloc[0]

        # Find any mismatches between actual and expected open times
        mismatches = df["openTime"] != expected_open_times

        # Get list of open times where mismatches occurred
        invalid_ts: list[int] = []
        
        i = 0
        for mismatch in mismatches:
            if mismatch:
                invalid_ts.append(df.at[i, "openTime"])
            i += 1

        return invalid_ts
    

def check_one_file_klines(klines_file_path: str, interval_seconds: int) -> dict:
    invalid_ts: list[int] = []
    with open(klines_file_path, "r") as f:
        df = pd.read_csv(f)
        
        if df.empty:
            return [], None, None

        df["openTime"] = pd.to_numeric(df["openTime"])
        df["closeTime"] = pd.to_numeric(df["closeTime"])

        # Calculate expected open times based on previous close times
        # Shift closeTime up by 1 row to compare with next row's openTime
        expected_open_times = df["closeTime"].shift(1) + 1

        # First row has no previous close time to compare against
        expected_open_times.iloc[0] = df["openTime"].iloc[0]

        # Find any mismatches between actual and expected open times
        mismatches = df["openTime"] != expected_open_times

        # Get list of open times where mismatches occurred
        i = 0
        for mismatch in mismatches:
            if mismatch:
                invalid_ts.append(df.at[i, "openTime"])
            i += 1

        # Convert interval to milliseconds
        interval_ms = interval_seconds * 1000

        # Calculate time differences between close and open times
        time_diffs = df["closeTime"] - df["openTime"]
        
        # Expected difference is interval_ms - 1 (since closeTime is inclusive)
        expected_diff = interval_ms - 1
        
        # Find any intervals with incorrect duration
        invalid_intervals = time_diffs != expected_diff
 
        i = 0
        for invalid in invalid_intervals:
            if invalid:
                invalid_ts.append(df.at[i, "openTime"])
            i += 1
            
        # Get first and last open times from the dataframe
        first_open_time = df["openTime"].iloc[0]
        last_open_time = df["openTime"].iloc[-1]

        return {
            "file_path": klines_file_path,
            "invalid_ts": invalid_ts,
            "first_open_time": first_open_time,
            "last_open_time": last_open_time
        }
    

def multi_proc_check_one_dir_klines(dir_path: str, interval_seconds: int, start_file_name: str | None = None, max_workers: int = config.max_workers) -> list[dict]:
    file_names = os.listdir(dir_path)
    if start_file_name:
        file_names = [f for f in file_names if f >= start_file_name]

    with Pool(max_workers) as pool:
        check_results = pool.starmap(check_one_file_klines, [(os.path.join(dir_path, f), interval_seconds) for f in file_names])

    check_results.sort(key=lambda x: x["first_open_time"])
    
    interval_ms = interval_seconds * 1000

    for i, result in enumerate(check_results[1:]):
        last_open_time = check_results[i]["last_open_time"]
        missing_num = (result["first_open_time"] - last_open_time) // interval_ms - 1
        if missing_num == 0:
            continue
        
        for j in range(missing_num):
            result["invalid_ts"].append(last_open_time + (j + 1) * interval_ms)

    return [r for r in check_results if r["invalid_ts"]]


def multi_proc_check_one_symbol_klines(
        syb_type: SymbolType, 
        symbol: str, 
        interval_seconds: int, 
        start_file_name: str | None = None, 
        klines_root_dir: str = config.diy_binance_vision_dir, 
        max_workers: int = config.max_workers
    ) -> list[dict]:
    prefix = f"data/{syb_type.value}/daily/klines/{symbol}/{interval_seconds}s"
    klines_dir = os.path.join(klines_root_dir, prefix)
    return multi_proc_check_one_dir_klines(klines_dir, interval_seconds, start_file_name, max_workers=max_workers)


if __name__ == "__main__":
    print(multi_proc_check_one_symbol_klines(SymbolType.SPOT, "PEPEUSDT", 1))
