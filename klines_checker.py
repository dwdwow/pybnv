import csv
from dataclasses import dataclass
import datetime
import logging
from multiprocessing import Pool
import os
import pandas as pd

import api_downloader
import config
from csv_util import klines_headers, csv_to_pandas
from enums import SymbolType

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


map_interval_ms_to_interval = {
    60 * 1000: "1m",
    3 * 60 * 1000: "3m",
    300 * 1000: "5m",
    900 * 1000: "15m",
    1800 * 1000: "30m",
    3600 * 1000: "1h",
    2 * 3600 * 1000: "2h",
    14400 * 1000: "4h",
    6 * 3600 * 1000: "6h",
    8 * 3600 * 1000: "8h",
    12 * 3600 * 1000: "12h",
    86400 * 1000: "1d",
}


map_interval_to_interval_ms = {
    "1m": 60 * 1000,
    "3m": 3 * 60 * 1000,
    "5m": 300 * 1000,
    "15m": 900 * 1000,
    "30m": 1800 * 1000,
    "1h": 3600 * 1000,
    "2h": 2 * 3600 * 1000,
    "4h": 14400 * 1000,
    "6h": 6 * 3600 * 1000,
    "8h": 8 * 3600 * 1000,
    "12h": 12 * 3600 * 1000,
    "1d": 86400 * 1000,
}


@dataclass
class OneKlineFileCheckResult:
    empty: bool
    file_path: str
    invalid_ts: list[int]
    first_open_time: int
    last_open_time: int
    
    
def tidy_klines_df(df: pd.DataFrame) -> pd.DataFrame:
    if len(str(int(df["openTime"].iloc[0]))) == 16:
        df["openTime"] = df["openTime"] // 1000
        df["closeTime"] = df["closeTime"] // 1000
    return df


def handle_empty_klines_file(klines_file_path: str, interval_seconds: int) -> OneKlineFileCheckResult:
    interval_ms = interval_seconds * 1000
    interval = map_interval_ms_to_interval[interval_ms]
    file_date = klines_file_path.split(f"{interval}-")[1].split(".")[0]
    file_time = datetime.datetime.strptime(file_date, "%Y-%m-%d")
    first_open_time = int(file_time.timestamp() * 1000)
    last_open_time = int((file_time + datetime.timedelta(days=1) - datetime.timedelta(seconds=interval_seconds)).timestamp() * 1000)
    invalid_ts = []
    for i in range(first_open_time, last_open_time+interval_ms, interval_ms):
        invalid_ts.append(i)
    return OneKlineFileCheckResult(
        empty=True,
        file_path=klines_file_path,
        invalid_ts=invalid_ts,
        first_open_time=first_open_time,
        last_open_time=last_open_time,
    )


def check_one_file_klines(klines_file_path: str, interval_seconds: int) -> OneKlineFileCheckResult:
    interval_ms = interval_seconds * 1000
    invalid_ts: list[int] = []
    with open(klines_file_path, "r") as f:
        df = csv_to_pandas(f, klines_headers)
        
        if df.empty:
            _logger.warning(f"File {klines_file_path} is empty")
            return handle_empty_klines_file(klines_file_path, interval_seconds)
            
        df = tidy_klines_df(df)

        df["openTime"] = pd.to_numeric(df["openTime"])
        df["closeTime"] = pd.to_numeric(df["closeTime"])

        df.sort_values(by="openTime", inplace=True)
        df.drop_duplicates(subset="openTime", keep="first", inplace=True)
        
        # Drop rows where openTime is not a multiple of the interval
        df = df[df["openTime"] % interval_ms == 0]

        # Drop rows with invalid intervals
        time_diffs = df["closeTime"] - df["openTime"]
        expected_diff = interval_ms - 1
        df = df[time_diffs == expected_diff]

        if df.empty:
            _logger.warning(f"File {klines_file_path} is empty")
            return handle_empty_klines_file(klines_file_path, interval_seconds)

        # check if openTime is consistent with closeTime
        expected_open_times = df["closeTime"].shift(1) + 1
        expected_open_times.iloc[0] = df["openTime"].iloc[0]
        diffs = df["openTime"] - expected_open_times
        for i, diff in enumerate(diffs):
            if diff == 0:
                continue
            op = int(expected_open_times.iloc[i])
            for o in range(op, op + int(diff), interval_ms):
                invalid_ts.append(int(o))

        return OneKlineFileCheckResult(
            empty=False,
            file_path=klines_file_path,
            invalid_ts=invalid_ts,
            first_open_time=int(df["openTime"].iloc[0]),
            last_open_time=int(df["openTime"].iloc[-1])
        )
        

def multi_proc_check_one_symbol_klines(
        syb_type: SymbolType, 
        symbol: str, 
        interval: str,
        start_date: str = "",
        end_date: str = "",
        klines_root_dir: str = config.unzip_binance_vision_dir, 
        max_workers: int = config.max_workers
    ) -> list[OneKlineFileCheckResult]:
    prefix = f"data/{syb_type.value}/daily/klines/{symbol}/{interval}"
    klines_dir = os.path.join(klines_root_dir, prefix)
    interval_seconds = map_interval_to_interval_ms[interval] // 1000
    if start_date:
        start_file_name = f"{symbol}-{interval}-{start_date}.csv"
    else:
        start_file_name = ""    
    if end_date:
        end_file_name = f"{symbol}-{interval}-{end_date}.csv"
    else:
        end_file_name = f"{symbol}-{interval}-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')}.csv"

    file_names = os.listdir(klines_dir)
    file_names = [f for f in file_names if start_file_name <= f <= end_file_name]
    file_names.sort()

    with Pool(max_workers) as pool:
        check_results = pool.starmap(check_one_file_klines, [(os.path.join(klines_dir, f), interval_seconds) for f in file_names])

    check_results.sort(key=lambda x: x.first_open_time)
    
    interval_ms = interval_seconds * 1000

    for i, result in enumerate(check_results[1:]):
        last_open_time = check_results[i].last_open_time
        missing_num = (result.first_open_time - last_open_time) // interval_ms - 1
        if missing_num == 0:
            continue
        
        for j in range(missing_num):
            result.invalid_ts.append(last_open_time + (j + 1) * interval_ms)

    return [r for r in check_results if r.invalid_ts]


def download_missing_klines_and_save(
    syb_type: SymbolType,
    symbol: str,
    interval: str,
    missing_ts: list[int],
    missing_root_dir: str=config.missing_binance_vision_dir,
    check_file_exists: bool=True
    ) -> None:
    if not missing_ts:
        return
    interval_ms = map_interval_to_interval_ms[interval]
    groups = []
    for i in range(len(missing_ts)):
        if i == 0 or missing_ts[i] != missing_ts[i-1] + interval_ms:
            groups.append([missing_ts[i]])
        else:
            groups[-1].append(missing_ts[i])
    klines = []
    for group in groups:
        klines.extend(api_downloader.download_klines(syb_type, symbol, interval, group[0], group[-1]))

    klines.sort(key=lambda x: x[0])

    # Remove duplicates based on open time
    seen_open_times = set()
    unique_klines = []
    for kline in klines:
        open_time = kline[0]
        if open_time not in seen_open_times:
            seen_open_times.add(open_time)
            unique_klines.append(kline)
    klines = unique_klines

    if not klines:
        return

    klines_by_date = {}
    for kline in klines:
        date = datetime.datetime.fromtimestamp(kline[0]//1000, datetime.timezone.utc).strftime("%Y-%m-%d")
        if date not in klines_by_date:
            klines_by_date[date] = []
        klines_by_date[date].append(kline)

    prefix = f"data/{syb_type.value}/daily/klines/{symbol}/{interval}"
    missing_dir = os.path.join(missing_root_dir, prefix)
    os.makedirs(missing_dir, exist_ok=True)

    for date, date_klines in klines_by_date.items():
        filename = f"{symbol}-{interval}-{date}.csv"
        filepath = os.path.join(missing_dir, filename)
        if check_file_exists and os.path.exists(filepath):
            _logger.info(f"File {filepath} already exists, skipping")
            continue
        with open(filepath, "w") as f:
            writer = csv.DictWriter(f, fieldnames=klines_headers)
            writer.writeheader()
            rows = []
            for kline in date_klines:
                if len(str(int(kline[0]))) == 16:
                    kline[0] = kline[0] // 1000
                    kline[6] = kline[6] // 1000
                rows.append({
                    klines_headers[0]: kline[0],
                    klines_headers[1]: kline[1],
                    klines_headers[2]: kline[2],
                    klines_headers[3]: kline[3],
                    klines_headers[4]: kline[4],
                    klines_headers[5]: kline[5],
                    klines_headers[6]: kline[6],
                    klines_headers[7]: kline[7],
                    klines_headers[8]: kline[8],
                    klines_headers[9]: kline[9],
                    klines_headers[10]: kline[10],
                    klines_headers[11]: kline[11]
                })
            writer.writerows(rows)
    
    
def merge_raw_and_missing_klines(
    file_name: str,
    raw_dir: str=config.unzip_binance_vision_dir,
    missing_dir: str=config.missing_binance_vision_dir,
    save_dir: str=config.tidy_binance_vision_dir,
    check_file_exists: bool = True
    ) -> None:
    tidy_path = os.path.join(save_dir, file_name)
    if check_file_exists and os.path.exists(tidy_path):
        _logger.info(f"Tidy file {tidy_path} already exists, skipping")
        return
    
    raw_path = os.path.join(raw_dir, file_name)
    with open(raw_path, "r") as f:
        raw_df = csv_to_pandas(f, klines_headers)
    raw_df = tidy_klines_df(raw_df)
        
    missing_path = os.path.join(missing_dir, file_name)
    if not os.path.exists(missing_path):
        _logger.info(f"No missing klines file for {file_name}, copying raw file to tidy file")
        raw_df.to_csv(tidy_path, index=False)
        _logger.info(f"Saved raw klines to {tidy_path}")
        return
    
    _logger.info(f"Merging {file_name} raw and missing klines")
    
    with open(missing_path, "r") as f:
        missing_df = csv_to_pandas(f, klines_headers)
    missing_df = tidy_klines_df(missing_df)
        
    merged_df = pd.concat([raw_df, missing_df])
    merged_df.sort_values(by="openTime", inplace=True)
    
    # Drop duplicates based on open time
    merged_df.drop_duplicates(subset="openTime", keep="first", inplace=True)

    merged_df.to_csv(tidy_path, index=False)

    _logger.info(f"Saved merged klines to {tidy_path}")
    
    
def multi_proc_merge_one_symbol_raw_and_missing_klines(
        syb_type: SymbolType,
        symbol: str,
        interval: str,
        start_date: str = "",
        end_date: str = "",
        unzip_root_dir: str = config.unzip_binance_vision_dir,
        missing_root_dir: str = config.missing_binance_vision_dir,
        tidy_root_dir: str = config.tidy_binance_vision_dir,
        check_file_exists: bool = True,
        max_workers: int = config.max_workers
    ) -> None:
    prefix = f"data/{syb_type.value}/daily/klines/{symbol}/{interval}"
    raw_dir = os.path.join(unzip_root_dir, prefix)
    missing_dir = os.path.join(missing_root_dir, prefix)
    save_dir = os.path.join(tidy_root_dir, prefix)
    if start_date:
        start_file_name = f"{symbol}-{interval}-{start_date}.csv"
    else:
        start_file_name = ""
    if end_date:
        end_file_name = f"{symbol}-{interval}-{end_date}.csv"
    else:
        end_file_name = f"{symbol}-{interval}-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')}.csv"
    with Pool(processes=max_workers) as pool:
        pool.starmap(merge_raw_and_missing_klines, [(file_name, raw_dir, missing_dir, save_dir, check_file_exists)
                                                    for file_name in os.listdir(raw_dir) 
                                                    if start_file_name <= file_name <= end_file_name])

    
def multi_proc_tidy_klines(
        syb_type: SymbolType,
        symbol: str,
        interval: str,
        start_date: str = "",
        end_date: str = "",
        unzip_root_dir: str = config.unzip_binance_vision_dir,
        missing_root_dir: str = config.missing_binance_vision_dir,
        tidy_root_dir: str = config.tidy_binance_vision_dir,
        check_file_exists: bool = True,
        max_workers: int = config.max_workers
    ) -> None:
    """
    Process and tidy up klines data for a symbol by:
    1. Checking for missing/invalid data points
    2. Downloading missing data from API
    3. Merging raw and missing data into tidy files

    Args:
        syb_type: Type of symbol (SymbolType)
        symbol: Trading pair symbol (e.g. "BTCUSDT") 
        interval: Kline interval (e.g. "1m", "5m", etc)
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        unzip_root_dir: Directory containing unzipped raw klines data
        missing_root_dir: Directory to store downloaded missing data
        tidy_root_dir: Directory to store final merged/tidy data
        check_file_exists: Skip if output file exists
        max_workers: Number of parallel processes to use
    """

    prefix = f"data/{syb_type.value}/daily/klines/{symbol}/{interval}"
    unzip_dir = os.path.join(unzip_root_dir, prefix)
    missing_dir = os.path.join(missing_root_dir, prefix)
    tidy_dir = os.path.join(tidy_root_dir, prefix)
    os.makedirs(unzip_dir, exist_ok=True)
    os.makedirs(missing_dir, exist_ok=True)
    os.makedirs(tidy_dir, exist_ok=True)

    check_result = multi_proc_check_one_symbol_klines(syb_type, symbol, interval, start_date, end_date, unzip_root_dir, max_workers)
    
    if check_result:
        missing_ts = []
        for r in check_result:
            missing_ts.extend(r.invalid_ts)
        missing_ts.sort()
        download_missing_klines_and_save(syb_type, symbol, interval, missing_ts, missing_root_dir, check_file_exists)
    
    multi_proc_merge_one_symbol_raw_and_missing_klines(syb_type, symbol, interval, start_date, end_date, unzip_root_dir, missing_root_dir, tidy_root_dir, check_file_exists, max_workers)


if __name__ == "__main__":
    pass
