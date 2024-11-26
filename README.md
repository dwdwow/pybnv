# pybnv
binance vision data by python

agg trades:
    下载原始zip数据，解压数据，检查数据缺失，下载缺失数据，整合数据
    agg_trades_tidy.py tidy_one_symbol

klines:
    将agg trades数据转换为klines数据，并检查klines数据
    diy_klines.py multi_proc_merge_one_symbol_agg_trades_to_klines
    klines_checker.py multi_proc_check_one_symbol_klines
