import datetime
import logging
import math
import time
from typing import Callable, Any
from binance.spot import Spot
from binance.um_futures import UMFutures
from binance.cm_futures import CMFutures

from enums import SymbolType


logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

def download_agg_trades_by_ids(rester: Callable[[str, dict[str, Any]], list[dict]], symbol: str, start_id: int, end_id: int) -> list[dict]:
    id = start_id - 1
    trades = []
    while id < end_id:
        _logger.info(f"Downloading trades from {id+1} to {end_id}")
        try:
            ts = rester(symbol=symbol, fromId=id+1, limit=1000)
        except Exception as e:
            _logger.error(e)
            continue
        _logger.info(f"Downloaded {len(ts)} trades")
        if len(ts) == 0:
            break
        valids = [trade for trade in ts if trade["a"] <= end_id]
        if len(valids) == 0:
            break
        trades.extend(valids)
        id = trades[-1]["a"]
    return trades


def download_spot_agg_trades_by_ids(symbol: str, start_id: int, end_id: int) -> list[dict]:
    return download_agg_trades_by_ids(Spot().agg_trades, symbol, start_id, end_id)


def download_um_futures_agg_trades_by_ids(symbol: str, start_id: int, end_id: int) -> list[dict]:
    return download_agg_trades_by_ids(UMFutures().agg_trades, symbol, start_id, end_id)


def download_cm_futures_agg_trades_by_ids(symbol: str, start_id: int, end_id: int) -> list[dict]:
    return download_agg_trades_by_ids(CMFutures().agg_trades, symbol, start_id, end_id)


def download_klines_with_caller(caller: Callable[[str, str, dict[str, Any]], list[dict]], symbol: str, interval: str, start_open_time: int, end_open_time: int) -> list[list[any]]:
    open_time = start_open_time
    klines = []
    _logger.info(f"Downloading klines from {
        datetime.datetime.fromtimestamp(start_open_time//1000, datetime.timezone.utc)} to {
            datetime.datetime.fromtimestamp(end_open_time//1000, datetime.timezone.utc)}")
    while open_time <= end_open_time:
        try:
            ks = caller(symbol, interval, startTime=open_time, limit=1000)
        except Exception as e:
            time.sleep(1)
            _logger.error(e)
            continue
        valids = [k for k in ks if k[0] <= end_open_time]
        if len(valids) == 0:
            break
        klines.extend(valids)
        open_time = valids[-1][6] + 1
    _logger.info(f"Downloaded klines from {
        datetime.datetime.fromtimestamp(start_open_time//1000, datetime.timezone.utc)} to {
            datetime.datetime.fromtimestamp(end_open_time//1000, datetime.timezone.utc)}, {len(klines)} klines")
    return klines


def download_spot_klines(symbol: str, interval: str, start_open_time: int, end_open_time: int) -> list[dict]:
    return download_klines_with_caller(Spot().klines, symbol, interval, start_open_time, end_open_time)


def download_um_futures_klines(symbol: str, interval: str, start_open_time: int, end_open_time: int) -> list[dict]:
    return download_klines_with_caller(UMFutures().klines, symbol, interval, start_open_time, end_open_time)


def download_cm_futures_klines(symbol: str, interval: str, start_open_time: int, end_open_time: int) -> list[dict]:
    return download_klines_with_caller(CMFutures().klines, symbol, interval, start_open_time, end_open_time)


def download_klines(syb_type: SymbolType, symbol: str, interval: str, start_open_time: int, end_open_time: int) -> list[dict]:
    match syb_type:
        case SymbolType.SPOT:
            return download_spot_klines(symbol, interval, start_open_time, end_open_time)
        case SymbolType.FUTURES_UM:
            return download_um_futures_klines(symbol, interval, start_open_time, end_open_time)
        case SymbolType.FUTURES_CM:
            return download_cm_futures_klines(symbol, interval, start_open_time, end_open_time)
        
    raise ValueError(f"Invalid symbol type: {syb_type}")


if __name__ == "__main__":
    now = math.floor(time.time()*1000) // (60 * 1000) * (60 * 1000)
    start = now - 1000*60*10000
    end = now
    print(time.localtime(start//1000))
    print(time.localtime(end//1000))
    klines = download_spot_klines("BTCUSDT", "1m", start, end)
    print(len(klines))
    print(time.localtime(klines[0][0]//1000))
    print(time.localtime(klines[-1][0]//1000))

