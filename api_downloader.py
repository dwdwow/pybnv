import logging
from typing import Callable, Any
from binance.spot import Spot
from binance.um_futures import UMFutures
from binance.cm_futures import CMFutures


logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

def download_agg_trades_by_ids(rester: Callable[[str, dict[str, Any]], list[dict]], symbol: str, start_id: int, end_id: int) -> list[dict]:
    id = start_id - 1
    trades = []
    while id < end_id:
        ts = []
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


if __name__ == "__main__":
    trades = download_spot_agg_trades_by_ids("BTCUSDT", 1, 2101)
    print(len(trades))
    # for trade in trades:
    #     print(trade["a"])

