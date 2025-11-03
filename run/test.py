import asyncio
from cex.bnc import public

async def main():
    symbols = await public.get_spot_symbols()
    sybs = []
    for syb in symbols:
        if syb.symbol.endswith("USDT"):
            sybs.append(syb.symbol)
    print(len(sybs))
    symbols = await public.get_um_symbols()
    sybs = []
    for syb in symbols:
        if syb.symbol.endswith("USDT"):
            sybs.append(syb.symbol)
    print(len(sybs))
    symbols = await public.get_cm_symbols()
    print(len(symbols))

if __name__ == "__main__":
    asyncio.run(main())