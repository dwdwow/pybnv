import csv
import datetime
import itertools
import logging
from multiprocessing import Pool
import os
from enums import SymbolType

import config
import csv_util

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

_decimal_places = 10


def merge_one_file_agg_trades_to_klines(
        interval_seconds: int,
        agg_trade_file_path: str,
        last_kline_before_today: dict | None = None,
        ) -> list[dict]:

    df = None

    # Read agg trades file into pandas DataFrame
    with open(agg_trade_file_path, "r") as f:
        df = csv_util.csv_to_pandas(f, csv_util.agg_trades_headers)
    
    if df is None or df.empty:
        return []

    one_day_ms = 24*60*60*1000
    interval_ms = interval_seconds*1000
    
    if one_day_ms % interval_ms != 0:
        raise ValueError(f"interval_ms: {interval_ms} is not a divisor of one_day_ms: {one_day_ms}")

    first_time = df.at[0, "time"]
    start_ms = int(first_time)//one_day_ms*one_day_ms
    
    kline_num = one_day_ms // interval_ms
    # no use, just for safety
    if one_day_ms % interval_ms != 0:
        kline_num += 1
        
    klines: list[dict] = list(itertools.islice(itertools.repeat(None), kline_num))
    
    kline: dict = {
        "openTime": 0,
    }
    
    _logger.debug(f"file: {agg_trade_file_path}, df.shape: {df.shape}, kline_num: {kline_num}, start: {datetime.datetime.fromtimestamp(start_ms//1000, tz=datetime.timezone.utc)}, interval_ms: {interval_ms}")
    
    for row in df.itertuples():
        time_ms = row.time
        open_time_ms = (time_ms - start_ms) // interval_ms * interval_ms + start_ms
        quote_asset_volume = row.price * row.qty
        if open_time_ms != kline["openTime"]:
            kline = {}
            kline["openTime"] = open_time_ms
            kline["openPrice"] = row.price
            kline["highPrice"] = row.price
            kline["lowPrice"] = row.price
            kline["closePrice"] = row.price
            kline["volume"] = 0.0
            kline["closeTime"] = open_time_ms + interval_ms - 1
            kline["quoteAssetVolume"] = 0.0
            kline["tradesNumber"] = 0
            kline["takerBuyBaseAssetVolume"] = 0.0
            kline["takerBuyQuoteAssetVolume"] = 0.0
            kline["unused"] = 0
            i = (open_time_ms - start_ms) // interval_ms
            klines[i] = kline
        
        if row.price > kline["highPrice"]:
            kline["highPrice"] = row.price
        if row.price < kline["lowPrice"]:
            kline["lowPrice"] = row.price
        kline["closePrice"] = row.price
        kline["volume"] += row.qty
        kline["quoteAssetVolume"] += quote_asset_volume
        kline["tradesNumber"] += 1
        if not row.isBuyerMaker:
            kline["takerBuyBaseAssetVolume"] += row.qty
            kline["takerBuyQuoteAssetVolume"] += quote_asset_volume
    
    # for k in klines:
    #     if k is None:
    #         continue
    #     k["volume"] = round(k["volume"], _decimal_places)
    #     k["quoteAssetVolume"] = round(k["quoteAssetVolume"], _decimal_places)
    #     k["takerBuyBaseAssetVolume"] = round(k["takerBuyBaseAssetVolume"], _decimal_places)
    #     k["takerBuyQuoteAssetVolume"] = round(k["takerBuyQuoteAssetVolume"], _decimal_places)

    last_real_kline_close_price = 0.0
    if last_kline_before_today is not None:
        last_real_kline_close_price = last_kline_before_today["closePrice"]

    # can not use enumerate, because klines is too long
    i = -1
    for k in klines:
        i += 1
        if k is not None:
            last_real_kline_close_price = k["closePrice"]
            continue
        open_time_ms = start_ms + interval_ms * i
        klines[i] = {
            "openTime": open_time_ms,
            "openPrice": last_real_kline_close_price,
            "highPrice": last_real_kline_close_price,
            "lowPrice": last_real_kline_close_price,
            "closePrice": last_real_kline_close_price,
            "volume": 0,
            "closeTime": open_time_ms + interval_ms - 1,
            "quoteAssetVolume": 0,
            "tradesNumber": 0,
            "takerBuyBaseAssetVolume": 0,
            "takerBuyQuoteAssetVolume": 0,
            "unused": 0,
        }
        
    # first_not_zero_price_index = len(klines)
    # i = -1
    # for k in klines:
    #     i += 1
    #     if k["closePrice"] != 0:
    #         first_not_zero_price_index = i
    #         break
        
    return klines


def multi_proc_merge_one_dir_agg_trades_to_klines(
        syb_type: SymbolType,
        symbol: str,
        interval_seconds: int,
        start_agg_trade_file_name: str = "",
        agg_trades_root_dir: str = config.tidy_binance_vision_dir,
        klines_root_dir: str = config.diy_binance_vision_dir,
        check_exist: bool = True,
        max_workers: int = config.max_workers,
        ) -> None:
    
    if start_agg_trade_file_name:
        cdt = datetime.datetime.strptime(start_agg_trade_file_name.lstrip(f"{symbol}-aggTrades-").rstrip(".csv"), "%Y-%m-%d")
        cdt = cdt - datetime.timedelta(days=1)
        fn = f"{symbol}-aggTrades-{cdt.strftime('%Y-%m-%d')}.csv"
        if os.path.exists(f"{agg_trades_root_dir}/data/{syb_type.value}/daily/aggTrades/{symbol}/{fn}"):
            start_agg_trade_file_name = fn
            
    agg_trades_dir = f"{agg_trades_root_dir}/data/{syb_type.value}/daily/aggTrades/{symbol}"
    agg_trades_file_names = os.listdir(agg_trades_dir)
    agg_trades_file_names.sort()
    if start_agg_trade_file_name:
        agg_trades_file_names = [f for f in agg_trades_file_names if f >= start_agg_trade_file_name]
        
    if len(agg_trades_file_names) == 0:
        _logger.info(f"no agg trades files found")
        return
        
    _logger.info(f"agg_trades_files: {agg_trades_file_names[0]} ~ {agg_trades_file_names[-1]}")
        
    klines_dir = f"{klines_root_dir}/data/{syb_type.value}/daily/klines/{symbol}/{interval_seconds}s"
    os.makedirs(klines_dir, mode=0o777, exist_ok=True)
        
    kline_dict: dict[str, list[dict]] = {}
    
    with Pool(max_workers) as p:
        kss = p.starmap(merge_one_file_agg_trades_to_klines, [(interval_seconds, f"{agg_trades_dir}/{fn}") for fn in agg_trades_file_names])
        for ks in kss:
            if len(ks) == 0:
                continue
            fdt = datetime.datetime.fromtimestamp(ks[0]["openTime"]//1000, tz=datetime.timezone.utc)
            kline_dict[fdt.strftime("%Y-%m-%d")] = ks
    
    _logger.debug(f"kline_dict_len: {len(kline_dict)}")
                
    with Pool(max_workers) as p:
        p.starmap(
            _add_leading_missing_klines_and_save, 
            [(interval_seconds, dt, klines, kline_dict, check_exist, klines_dir, symbol) for dt, klines in kline_dict.items()]
        )

                    
def _float_formater(x) -> str:
    if not isinstance(x, float):
        return str(x)
    formatted = f"{round(x, _decimal_places):.{_decimal_places}f}".rstrip("0").rstrip(".")
    return formatted

            
def _add_leading_missing_klines_and_save(
        interval_seconds: int, 
        tody_date: str, 
        klines: list[dict], 
        kline_dict: dict[str, list[dict]], 
        check_exist: bool, 
        klines_dir: str, 
        symbol: str
        ) -> None:
    if len(klines) == 0:
        return
    
    _logger.debug(f"checking klines leading missing klines, symbol: {symbol}, date: {tody_date}, klines_len: {len(klines)}")
    
    interval_ms = interval_seconds*1000

    first_not_zero_price_index = len(klines)
    for k in klines:
        if k["openPrice"] != 0.0:
            first_not_zero_price_index = klines.index(k)
            break

    if first_not_zero_price_index > 0:
        lot = datetime.datetime.strptime(tody_date, "%Y-%m-%d") - datetime.timedelta(days=1)
        ldt = lot.strftime("%Y-%m-%d")
        lklines = kline_dict.get(ldt)
        if lklines is not None and len(lklines) != 0:
            fko = klines[0]["openTime"]
            last_lk = lklines[-1]
            close_price = last_lk["closePrice"]
            for i in range(first_not_zero_price_index):            
                op = fko + interval_ms * i
                klines[i] = {
                    "openTime": op,
                    "openPrice": close_price,
                    "highPrice": close_price,
                    "lowPrice": close_price,
                    "closePrice": close_price,
                    "volume": 0,
                    "closeTime": op + interval_ms - 1,
                    "quoteAssetVolume": 0,
                    "tradesNumber": 0,
                    "takerBuyBaseAssetVolume": 0,
                    "takerBuyQuoteAssetVolume": 0,
                    "unused": 0,
                }

    klines_file_path = f"{klines_dir}/{symbol}-{interval_seconds}s-{tody_date}.csv"
    _logger.debug(f"saving klines to {klines_file_path}")
    if check_exist and os.path.exists(klines_file_path):
        _logger.debug(f"klines file already exists, skipping, {klines_file_path}")
        return
    with open(klines_file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=csv_util.klines_headers)
        writer.writeheader()
        for kline in klines:
            for k, v in kline.items():
                kline[k] = _float_formater(v)
            writer.writerow(kline)
    _logger.debug(f"saved klines to {klines_file_path}")


if __name__ == "__main__":
    syb_type = SymbolType.SPOT
    symbol = "PEPEUSDT"
    interval_seconds = 1
    multi_proc_merge_one_dir_agg_trades_to_klines(syb_type, symbol, interval_seconds)
