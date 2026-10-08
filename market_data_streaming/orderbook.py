"""L2 order-book recording (`MarketDataService.SubscribeOrderBook`); see recorder.py for the file format.

Run: python -m market_data_streaming.orderbook SYMBOL... --out-dir DIR --secrets-file FILE
"""

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from finam_trade_api import AsyncFinamClient
from finam_trade_api.market_data import SubscribeOrderBookRequest

from .auth import DEFAULT_SECRET_VAR_NAME
from .recorder import arecord, main


def _subscribe(client: AsyncFinamClient, symbol: str) -> AsyncIterator[Any]:
    return client.market_data.SubscribeOrderBook(SubscribeOrderBookRequest(symbol=symbol))


async def arecord_orderbook(
    symbols: list[str],
    out_dir: str | Path,
    secrets_file: str | Path,
    secret_var: str = DEFAULT_SECRET_VAR_NAME,
) -> None:
    await arecord(symbols, out_dir, secrets_file, secret_var, _subscribe)


if __name__ == "__main__":
    main(_subscribe, __doc__)
