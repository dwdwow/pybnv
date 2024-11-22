import csv
import logging

import pandas as pd
from typing import TextIO

import config

_logger = logging.getLogger(__name__)

klines_headers = ["openTime", "openPrice", "highPrice", "lowPrice", "closePrice", "volume", "closeTime", "quoteAssetVolume", "tradesNumber", "takerBuyBaseAssetVolume", "takerBuyQuoteAssetVolume", "unused"]
agg_trades_headers = ["id", "price", "qty", "firstTradeId", "lastTradeId", "time", "isBuyerMaker", "isBestMatch"]
agg_trades_api_data_headers = ["a", "p", "q", "f", "l", "T", "m", "M"]

def has_header(file: TextIO) -> bool:
    """
    Check if a CSV file has a header row.
    
    Args:
        file: A text file object opened in read mode
        
    Returns:
        True if the file appears to have a header row, False otherwise
        
    Note:
        This uses csv.Sniffer to detect if the first row appears to be headers.
        The file pointer will be reset to the start after checking.
    """
    try:
        # Save original position
        original_pos = file.tell()
        
        # Read sample of file
        sample = file.read(10000)
        file.seek(original_pos)
        
        # Use csv sniffer to detect header
        has_header = csv.Sniffer().has_header(sample)
        return has_header
        
    except Exception as e:
        _logger.error(f"Error checking if CSV file has header: {e}")
        return False
    

def csv_to_pandas(file: TextIO, headers: list[str]) -> pd.DataFrame:
    has_h = has_header(file)
    frame = None
    if has_h:
        frame = pd.read_csv(file)
    else:
        frame = pd.read_csv(file, header=None)
    frame.columns = headers.copy()
    return frame


if __name__ == "__main__":
    file_path = config.unzip_binance_vision_dir + "/data/spot/monthly/klines/PEPEUSDT/1w/PEPEUSDT-1w-2023-05.csv"
    with open(file_path, "r") as f:
        df = csv_to_pandas(f, klines_headers)
        print(df)
