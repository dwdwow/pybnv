import datetime
import os
import config
from enums import SymbolType


def merge_agg_trades_to_klines(
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

    
    