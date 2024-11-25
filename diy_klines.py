import csv
import datetime
from multiprocessing import Pool
import os
import config
import csv_util
from enums import SymbolType


def merge_one_file_agg_trades_to_klines(
        interval_seconds: int,
        agg_trade_file_path: str,
        last_kline_before_today: dict | None = None,
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
    
    one_day_ms = 24*60*60*1000
    
    start_ms = int(first_time)//one_day_ms*one_day_ms
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
        
    if len(klines) == 0:
        return klines
        
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
            
    # Some agg trades of last intervals may be missing at the end of the day
    last_kline = klines[-1]
    
    lko = last_kline["openTime"]
    
    missing_num = (start_ms + one_day_ms - lko) // interval_ms - 1
    
    if missing_num == 0:
        return klines
    
    close_price = last_kline["closePrice"]
    for i in range(missing_num):
        open_time = lko + interval_ms * (i+1)
        klines.append({
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
        
    if last_kline_before_today is None:
        return klines
    
    # Some agg trades of first intervals may be missing at the beginning of the day
    # We need to fill them by the last day's last kline
    lkbto = last_kline_before_today["openTime"]
    fk = klines[0]
    fko = fk["openTime"]
    missing_num = (fko - lkbto) // interval_ms - 1
    if missing_num == 0:
        return klines
    
    close_price = last_kline_before_today["closePrice"]
    for i in range(missing_num):
        open_time = lkbto + interval_ms * (i+1)
        klines.insert(0, {
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
        check_exist: bool = True,
        max_workers: int = config.max_workers,
        ) -> None:
    
    if start_agg_trade_file_name:
        dt = datetime.datetime.strptime(start_agg_trade_file_name.lstrip(f"{symbol}-aggTrades-").rstrip(".csv"), "%Y-%m-%d")
        dt = dt - datetime.timedelta(seconds=interval_seconds)
        start_agg_trade_file_name = f"{symbol}-aggTrades-{dt.strftime('%Y-%m-%d', tz=datetime.timezone.utc)}.csv"

    interval_ms = interval_seconds*1000

    agg_trades_dir = f"{agg_trades_root_dir}/data/{syb_type.value}/daily/aggTrades/{symbol}"
    agg_trades_file_names = os.listdir(agg_trades_dir)
    agg_trades_file_names.sort()
    if start_agg_trade_file_name:
        agg_trades_file_names = [f for f in agg_trades_file_names if f >= start_agg_trade_file_name]
        
    kline_dict: dict[str, list[dict]] = {}
    
    for file_name in agg_trades_file_names:
        agg_trade_file_path = f"{agg_trades_dir}/{file_name}"
        with Pool(max_workers) as p:
            kss = p.starmap(merge_one_file_agg_trades_to_klines, [(interval_seconds, agg_trade_file_path)])
            for k in kss:
                kline_dict[file_name.lstrip(f"{symbol}-aggTrades-").rstrip(".csv")] = k
                
    for dt, klines in kline_dict.items():
        if len(klines) == 0:
            continue
        st = datetime.datetime.strptime(dt, "%Y-%m-%d", tz=datetime.timezone.utc)
        stms = st.timestamp()*1000
        fk = klines[0]
        fko = fk["openTime"]
        missing_num = (fko - stms) // interval_ms
        if missing_num != 0:
            lst = st - datetime.timedelta(days=1)
            ldt = lst.strftime("%Y-%m-%d")
            lklines = kline_dict.get[ldt]
            if lklines != None and len(lklines) != 0:
                last_lk = lklines[-1]
                close_price = last_lk["closePrice"]
                for i in range(missing_num):
                    open_time = stms + interval_ms * i
                    klines.insert(0, {
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


        klines_file_path = f"{klines_root_dir}/data/{syb_type.value}/daily/klines/{symbol}/{symbol}-{interval_seconds}s-{dt}.csv"
        if check_exist and os.path.exists(klines_file_path):
            continue
        with open(klines_file_path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=csv_util.klines_headers)
            writer.writeheader()
            writer.writerows(klines)
            

    