import datetime
import logging
from multiprocessing import Pool
import os

import pandas as pd
from enums import SymbolType

import config
import csv_util

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

DECIMAL_PLACES = 10
MICRO_SECONDS_20000101 = 946684800000000
ONE_DAY_MS = 24*60*60*1000


def merge_agg_trades_to_klines(
    interval_ms: int,
    raw_data: pd.DataFrame,
    pre_close_price: float,
    ) -> pd.DataFrame:
    if raw_data is None or raw_data.empty:
        return pd.DataFrame(columns=csv_util.klines_headers)
    
    raw_data["time"] = pd.to_numeric(raw_data["time"])
    raw_data["price"] = pd.to_numeric(raw_data["price"])
    raw_data["qty"] = pd.to_numeric(raw_data["qty"])
    
    first_time = int(raw_data["time"].iloc[0])
    
    ts_adjust_ratio = 1
    if first_time > MICRO_SECONDS_20000101:
        ts_adjust_ratio = 1000
    
    raw_data["time_ms"] = raw_data["time"] // ts_adjust_ratio
    
    if ONE_DAY_MS % interval_ms != 0:
        raise ValueError(f"interval_ms: {interval_ms} is not a divisor of one_day_ms: {ONE_DAY_MS}")

    
    raw_data["openTime"] = (raw_data["time_ms"] // interval_ms) * interval_ms
    
    raw_data["quoteAssetVolume"] = (raw_data["price"] * raw_data["qty"]).round(DECIMAL_PLACES)
    
    raw_data["tradesNumber"] = raw_data["lastTradeId"] - raw_data["firstTradeId"] + 1
    
    grouped = raw_data.groupby("openTime").agg({
        "price": ["first", "max", "min", "last"],  # open, high, low, close
        "qty": "sum",
        "quoteAssetVolume": "sum",
        "tradesNumber": "sum",
    })
    
    grouped.columns = ["openPrice", "highPrice", "lowPrice", "closePrice", "volume", "quoteAssetVolume", "tradesNumber"]
    
    float_cols = ["openPrice", "highPrice", "lowPrice", "closePrice", "volume", "quoteAssetVolume"]
    for col in float_cols:
        grouped[col] = grouped[col].round(DECIMAL_PLACES)
    
    taker_buy_mask = ~raw_data["isBuyerMaker"]
    taker_buy_grouped = raw_data[taker_buy_mask].groupby("openTime").agg({
        "qty": "sum",
        "quoteAssetVolume": "sum",
    })
    taker_buy_grouped.columns = ["takerBuyBaseAssetVolume", "takerBuyQuoteAssetVolume"]
    
    taker_buy_grouped["takerBuyBaseAssetVolume"] = taker_buy_grouped["takerBuyBaseAssetVolume"].round(DECIMAL_PLACES)
    taker_buy_grouped["takerBuyQuoteAssetVolume"] = taker_buy_grouped["takerBuyQuoteAssetVolume"].round(DECIMAL_PLACES)
    
    klines_df = grouped.join(taker_buy_grouped, how="left")
    klines_df["takerBuyBaseAssetVolume"] = klines_df["takerBuyBaseAssetVolume"].fillna(0.0)
    klines_df["takerBuyQuoteAssetVolume"] = klines_df["takerBuyQuoteAssetVolume"].fillna(0.0)
    
    klines_df["closeTime"] = klines_df.index + interval_ms - 1
    klines_df["unused"] = 0
    
    klines_df = klines_df.reset_index()
    
    start_ms = (raw_data["time_ms"].iloc[0] // ONE_DAY_MS) * ONE_DAY_MS
    all_open_times = pd.Series(range(start_ms, start_ms + ONE_DAY_MS, interval_ms), name="openTime")
    complete_klines_df = pd.DataFrame({"openTime": all_open_times})
    
    complete_klines_df = complete_klines_df.merge(klines_df, on="openTime", how="left")
    
    if pd.isna(complete_klines_df["closePrice"].iloc[0]):
        complete_klines_df.loc[complete_klines_df.index[0], "closePrice"] = pre_close_price
    
    complete_klines_df["closePrice"] = complete_klines_df["closePrice"].ffill()

    price_cols = ["openPrice", "highPrice", "lowPrice"]
    for col in price_cols:
        complete_klines_df[col] = complete_klines_df[col].fillna(complete_klines_df["closePrice"])
    
    volume_cols = ["volume", "quoteAssetVolume", "tradesNumber", "takerBuyBaseAssetVolume", "takerBuyQuoteAssetVolume"]
    for col in volume_cols:
        complete_klines_df[col] = complete_klines_df[col].fillna(0.0)
    
    complete_klines_df["closeTime"] = complete_klines_df["openTime"] + interval_ms - 1
    complete_klines_df["unused"] = 0
    
    complete_klines_df = complete_klines_df[csv_util.klines_headers]
    
    float_cols = ["openPrice", "highPrice", "lowPrice", "closePrice", "volume", "quoteAssetVolume", "takerBuyBaseAssetVolume", "takerBuyQuoteAssetVolume"]
    for col in float_cols:
        if col in complete_klines_df.columns:
            complete_klines_df[col] = complete_klines_df[col].round(DECIMAL_PLACES)
    
    complete_klines_df["openTime"] = complete_klines_df["openTime"].astype(int)
    complete_klines_df["closeTime"] = complete_klines_df["closeTime"].astype(int)
    complete_klines_df["tradesNumber"] = complete_klines_df["tradesNumber"].astype(int)
    complete_klines_df["unused"] = complete_klines_df["unused"].astype(int)
    
    return complete_klines_df


def merge_one_file_agg_trades_to_klines(
        interval_ms: int,
        agg_trade_file_path: str,
        ) -> pd.DataFrame:

    with open(agg_trade_file_path, "r") as f:
        raw_data = csv_util.csv_to_pandas(f, csv_util.agg_trades_headers)
    
    if raw_data is None or raw_data.empty:
        return pd.DataFrame(columns=csv_util.klines_headers)
        
    date = os.path.basename(agg_trade_file_path).strip(".csv").split("-aggTrades-")[-1]
    
    date = datetime.datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    
    pre_date = date - datetime.timedelta(days=1)
    
    pre_file = agg_trade_file_path.replace(date.strftime("%Y-%m-%d"), pre_date.strftime("%Y-%m-%d"))
    
    pre_close_price = 0.0
    
    if os.path.exists(pre_file):
        last_row_str = csv_util.get_last_row_ignore_header(pre_file)
        pre_close_price = float(last_row_str.split(",")[1])
        pre_close_price = round(pre_close_price, DECIMAL_PLACES)
    
    return merge_agg_trades_to_klines(interval_ms, raw_data, pre_close_price)
    

def multi_proc_merge_one_symbol_agg_trades_to_klines(
        syb_type: SymbolType,
        symbol: str,
        interval_milliseconds: int,
        start_date: str | None = None,
        end_date: str | None = None,
        agg_trades_root_dir: str = config.tidy_binance_vision_dir,
        klines_root_dir: str = config.diy_binance_vision_dir,
        check_exist: bool = True,
        max_workers: int = config.max_workers,
        ) -> None:

    start_agg_trade_file_name = ""
    end_agg_trade_file_name = f"{symbol}-aggTrades-9999-12-31.csv"
    if start_date:
        start_agg_trade_file_name = f"{symbol}-aggTrades-{start_date}.csv"
        cdt = datetime.datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
        cdt = cdt - datetime.timedelta(days=1)
        fn = f"{symbol}-aggTrades-{cdt.strftime('%Y-%m-%d')}.csv"
        if os.path.exists(f"{agg_trades_root_dir}/data/{syb_type.value}/daily/aggTrades/{symbol}/{fn}"):
            start_agg_trade_file_name = fn
    if end_date:
        end_agg_trade_file_name = f"{symbol}-aggTrades-{end_date}.csv"

    klines_dir = f"{klines_root_dir}/data/{syb_type.value}/daily/klines/{symbol}/{interval_milliseconds}ms"
    os.makedirs(klines_dir, mode=0o777, exist_ok=True)

    exist_klines_file_names = os.listdir(klines_dir)
    
    if check_exist:
        exist_klines_file_names = [fn.replace(f"-{interval_milliseconds}ms-", f"-aggTrades-") for fn in exist_klines_file_names]
        exist_klines_file_names = [fn.replace("parquet", "csv") for fn in exist_klines_file_names]
        
    agg_trades_dir = f"{agg_trades_root_dir}/data/{syb_type.value}/daily/aggTrades/{symbol}"
    all_agg_trades_file_names = os.listdir(agg_trades_dir)
    all_agg_trades_file_names.sort()
    all_agg_trades_file_names = [f for f in all_agg_trades_file_names
                                 if f >= start_agg_trade_file_name
                                 and f <= end_agg_trade_file_name
                                 and f not in exist_klines_file_names]
        
    if len(all_agg_trades_file_names) == 0:
        _logger.info(f"no agg trades files found")
        return
        
    _logger.info(f"agg_trades_files: {all_agg_trades_file_names[0]} ~ {all_agg_trades_file_names[-1]}")
    
    kline_dict: dict[str, pd.DataFrame] = {}
    
    with Pool(max_workers) as p:
        kss = p.starmap(merge_one_file_agg_trades_to_klines, [(interval_milliseconds, f"{agg_trades_dir}/{fn}") for fn in all_agg_trades_file_names])
        for ks_df in kss:
            if ks_df.empty:
                continue
            ot = int(ks_df["openTime"].iloc[0])
            if ot > MICRO_SECONDS_20000101:
                ot = ot // 1000
            fdt = datetime.datetime.fromtimestamp(ot//1000, tz=datetime.timezone.utc)
            kline_dict[fdt.strftime("%Y-%m-%d")] = ks_df
            
    for dt, ks_df in kline_dict.items():
        klines_file_path = f"{klines_dir}/{symbol}-{interval_milliseconds}ms-{dt}.parquet"
        _logger.debug(f"saving klines to {klines_file_path}")
        ks_df.to_parquet(klines_file_path, engine="pyarrow", index=False)
        _logger.debug(f"saved klines to {klines_file_path}")

            
if __name__ == "__main__":
    syb_type = SymbolType.FUTURES_UM
    symbol = "BTCUSDT"
    interval_milliseconds = 100
    start_date = "2025-01-01"
    end_date = "2025-01-04"
    multi_proc_merge_one_symbol_agg_trades_to_klines(syb_type, symbol, interval_milliseconds, start_date, end_date)