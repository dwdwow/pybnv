import csv
import datetime
import os
import config
import csv_util
from enums import SymbolType


def merge_one_file_agg_trades_to_klines(
        interval_seconds: int,
        agg_trade_file_path: str,
        ) -> list[dict]:
    df = None
    # Read agg trades file into pandas DataFrame
    with open(agg_trade_file_path, "r") as f:
        df = csv_util.csv_to_pandas(f, csv_util.agg_trades_headers)
    
    if df is None:
        return
    
    # Get first line
    if df.empty:
        return

    first_row = df.iloc[0]
    
    first_time = first_row["time"]
    
    start_ms = int(first_time)//(24*60*60*1000)*24*60*60*1000
    interval_ms = interval_seconds*1000
    
    klines: list[dict] = []
    kline: dict = {
        "openTime": 0,
    }
    
    for row in df.iterrows():
        time = row["time"]
        open_time = (time - start_ms) // interval_ms * interval_ms + start_ms
        quote_asset_volume = row["price"] * row["qty"]
        if open_time != kline["openTime"]:
            kline = {}
            kline["openTime"] = open_time
            kline["openPrice"] = row["price"]
            kline["highPrice"] = row["price"]
            kline["lowPrice"] = row["price"]
            kline["closePrice"] = row["price"]
            kline["volume"] = row["qty"]
            kline["closeTime"] = open_time + interval_ms - 1
            kline["quoteAssetVolume"] = quote_asset_volume
            kline["tradesNumber"] = 1
            if not row["isBuyerMaker"]:
                kline["takerBuyBaseAssetVolume"] = row["qty"]
                kline["takerBuyQuoteAssetVolume"] = quote_asset_volume
            klines.append(kline)
            continue
        
        if row["price"] > kline["highPrice"]:
            kline["highPrice"] = row["price"]
        if row["price"] < kline["lowPrice"]:
            kline["lowPrice"] = row["price"]
        kline["closePrice"] = row["price"]
        kline["volume"] += row["qty"]
        kline["quoteAssetVolume"] += quote_asset_volume
        kline["tradesNumber"] += 1
        if not row["isBuyerMaker"]:
            kline["takerBuyBaseAssetVolume"] += row["qty"]
            kline["takerBuyQuoteAssetVolume"] += quote_asset_volume
            
    for k in klines:
        k["volume"] = round(k["volume"], 12)
        k["quoteAssetVolume"] = round(k["quoteAssetVolume"], 12)
        k["takerBuyBaseAssetVolume"] = round(k["takerBuyBaseAssetVolume"], 12)
        k["takerBuyQuoteAssetVolume"] = round(k["takerBuyQuoteAssetVolume"], 12)
        
    index = 1
    
    # Some intervals may be no agg trades, so we need to fill them
    while True:
        if index >= len(klines):
            break

        ck = klines[index]
        lk = klines[index-1]
        ck_ot = ck["openTime"]
        lk_ot = lk["openTime"]

        missing_num = (ck_ot - lk_ot) // interval_ms - 1
        index += 1+missing_num

        if missing_num == 0:
            continue
        
        close_price = lk["closePrice"]
        for i in range(missing_num):
            open_time = lk_ot + interval_ms * (i+1)
            klines.insert(index, {
                "openTime": open_time,
                "openPrice": close_price,
                "highPrice": close_price,
                "lowPrice": close_price,
                "closePrice": close_price,
                "volume": 0,
                "closeTime": open_time + interval_ms - 1,
                "quoteAssetVolume": 0,
                "tradesNumber": 0,
                "takerBuyBaseAssetVolume": 0,
                "takerBuyQuoteAssetVolume": 0,
            })
        
    return klines


def merge_one_dir_agg_trades_to_klines(
        syb_type: SymbolType,
        symbol: str,
        interval_seconds: int,
        start_agg_trade_file_name: str = "",
        agg_trades_root_dir: str = config.tidy_binance_vision_dir,
        klines_root_dir: str = config.diy_binance_vision_dir,
        ) -> None:

    agg_trades_dir = f"{agg_trades_root_dir}/data/{syb_type.value}/daily/aggTrades/{symbol}"
    agg_trades_file_names = os.listdir(agg_trades_dir)
    agg_trades_file_names.sort()
    if start_agg_trade_file_name:
        agg_trades_file_names = [f for f in agg_trades_file_names if f >= start_agg_trade_file_name]
        
    start_time = datetime.datetime.strptime(start_agg_trade_file_name, f"{symbol}-aggTrades-%Y-%m-%d.csv", tz=datetime.timezone.utc)

    
    