import datetime
import os
import config
from enums import SymbolType
import klines_checker
import raw_downloader
import raw_unzipper
from loguru import logger


def download(
    syb_type: SymbolType,
    symbol: str,
    interval: str,
    *,
    start_date: str = "",
    end_date: str = "",
    zip_root_dir: str = config.data_binance_vision_dir,
    unzip_root_dir: str = config.unzip_binance_vision_dir,
    missing_root_dir: str = config.missing_binance_vision_dir,
    tidy_root_dir: str = config.tidy_binance_vision_dir,
    max_workers: int = config.max_workers,
    ) -> None:
    
    """
    Download klines data for a symbol, process and tidy it up.

    Downloads raw klines data from Binance Vision, unzips it, checks for missing/invalid data,
    downloads missing data from API, and merges everything into clean files.

    Args:
        syb_type: Type of symbol (SymbolType)
        symbol: Trading pair symbol (e.g. "BTCUSDT")
        interval: Kline interval (e.g. "1m", "5m", etc)
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        zip_root_dir: Directory to store downloaded zip files
        unzip_root_dir: Directory to store unzipped raw data
        missing_root_dir: Directory to store downloaded missing data
        tidy_root_dir: Directory to store final merged/tidy data
        max_workers: Number of parallel processes to use
    
    Raises:
        ValueError: If interval is >= 1 day
    """

    if klines_checker.map_interval_to_interval_ms[interval] >= 60 * 1000 * 60 * 24:
        raise ValueError(f"Interval {interval} is too long, it should be less than 1 day")

    prefix = f"data/{syb_type.value}/daily/klines/{symbol}/{interval}"
    zip_dir = os.path.join(zip_root_dir, prefix)
    unzip_dir = os.path.join(unzip_root_dir, prefix)
    missing_dir = os.path.join(missing_root_dir, prefix)
    tidy_dir = os.path.join(tidy_root_dir, prefix)
    os.makedirs(zip_dir, exist_ok=True)
    os.makedirs(unzip_dir, exist_ok=True)
    os.makedirs(missing_dir, exist_ok=True)
    os.makedirs(tidy_dir, exist_ok=True)
    
    # Get last file date from tidy dir
    last_file_date = None
    file_names = os.listdir(tidy_dir)
    if file_names:
        file_names.sort()
        last_file_name = file_names[-1]
        last_file_date = last_file_name.split(f"{symbol}-{interval}-")[1].split(".")[0]
        if start_date == "":
            start_date = last_file_date

    if start_date:
        if last_file_date:
            if start_date > last_file_date:
                raise ValueError(f"Start date {start_date} is greater than last file date {last_file_date}")
            start_date = last_file_date
        datetime_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        datetime_obj = datetime_obj - datetime.timedelta(days=1)
        start_date = datetime_obj.strftime("%Y-%m-%d")

    marker = ""
    if start_date:
        marker = f"{prefix}/{symbol}-{interval}-{start_date}.zip"
        
    raw_downloader.multi_proc_download(prefix, marker, zip_dir, max_workers=max_workers)
    
    raw_unzipper.multi_proc_unzip_one_dir_files_to_dir(zip_dir, unzip_dir, max_workers=max_workers)

    klines_checker.multi_proc_tidy_klines(
        syb_type, symbol, interval,
        start_date, end_date,
        unzip_root_dir, missing_root_dir, tidy_root_dir,
        max_workers
    )
    
    # binance may miss some klines, so need not to check tidied klines
    # result = klines_checker.multi_proc_check_one_symbol_klines(
    #     syb_type, symbol, interval,
    #     start_date, end_date,
    #     tidy_root_dir, max_workers
    # )
    # if result:
    #     logger.error(result)
    #     raise ValueError("Tidied klines not continuous")

    file_names = os.listdir(tidy_dir)
    if file_names:
        file_names.sort()
        last_file_name = file_names[-1]
        last_file_date = last_file_name.split(f"{symbol}-{interval}-")[1].split(".")[0]
        last_file_time = datetime.datetime.strptime(last_file_date, "%Y-%m-%d") - datetime.timedelta(days=2)
        last_file_date = last_file_time.strftime("%Y-%m-%d")
        last_file_name = f"{symbol}-{interval}-{last_file_date}.csv"
        file_names = os.listdir(unzip_dir)
        file_names = [f for f in file_names if f <= last_file_name]
        for file_name in file_names:
            raw_unzipper.clear_file(os.path.join(unzip_dir, file_name))
            logger.info(f"Cleared {os.path.join(unzip_dir, file_name)}")
    
    
if __name__ == "__main__":
    # results = klines_checker.multi_proc_check_one_symbol_klines(
    #     SymbolType.SPOT, "BTCUSDT", "1m",
    #     klines_root_dir=config.tidy_binance_vision_dir,
    # )
    # for result in results:
    #     print(datetime.datetime.fromtimestamp(result.first_open_time//1000), datetime.datetime.fromtimestamp(result.last_open_time//1000))
    #     print(len(result.invalid_ts))
    
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    intervals = ["1s", "1m", "5m", "15m", "30m", "1h", "2h", "4h"]
    for symbol in symbols:
        for interval in intervals:
            download(
                SymbolType.SPOT, symbol, interval,
            )
            if interval != "1s":
                download(
                    SymbolType.FUTURES_UM, symbol, interval,
                )

    
