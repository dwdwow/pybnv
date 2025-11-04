from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.resolve()))


import asyncio
from cex.bnc import public
from klines import download, SymbolType  # pyright: ignore[reportMissingImports]


async def main():
    spsybs = await public.get_spot_symbols()
    umsybs = await public.get_um_symbols()
    cmsybs = await public.get_cm_symbols()
    
    # U USDT USDC BUSD
    
    sps = []
    for syb in spsybs:
        if syb.tradable and syb.symbol.endswith("USDT"):
            sps.append(syb.symbol)
    
    ums = []
    for syb in umsybs:
        if syb.tradable and syb.symbol.endswith("USDT"):
            ums.append(syb.symbol)
    
    cms = []
    for syb in cmsybs:
        if syb.tradable:
            cms.append(syb.symbol)
        

    intervals = ["1s", "1m", "5m", "15m", "30m", "1h", "2h", "4h", "12h"]
    # for symbol in sps:
    #     for interval in intervals:
    #         download(
    #             SymbolType.SPOT, symbol, interval,
    #         )
            
    
    intervals = intervals[1:]
    for symbol in ums:
        for interval in intervals:
            download(
                SymbolType.FUTURES_UM, symbol, interval,
            )
    
    for symbol in cms:
        for interval in intervals:
            download(
                SymbolType.FUTURES_CM, symbol, interval,
            )


if __name__ == "__main__":
    asyncio.run(main())

