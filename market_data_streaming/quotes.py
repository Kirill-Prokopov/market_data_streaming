"""Quote recording (`MarketDataService.SubscribeQuote`); see recorder.py for the file format.

Used for the MOEX indices, which have no order book or trades. One stream per symbol, so every symbol gets its own
files like in the other recorders. A message is a `SubscribeQuoteResponse`; for an index `last` is the index value
(bid and ask are empty), and the first message after a (re)subscribe has `is_data_snapshot=True`.

Run: python -m market_data_streaming.quotes SYMBOL... --out-dir DIR --secrets-file FILE
"""

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from finam_trade_api import AsyncFinamClient
from finam_trade_api.market_data import SubscribeQuoteRequest

from .auth import DEFAULT_SECRET_VAR_NAME
from .recorder import arecord, main


def _subscribe(client: AsyncFinamClient, symbol: str) -> AsyncIterator[Any]:
    return client.market_data.SubscribeQuote(SubscribeQuoteRequest(symbols=[symbol]))


async def arecord_quotes(
    symbols: list[str],
    out_dir: str | Path,
    secrets_file: str | Path,
    secret_var: str = DEFAULT_SECRET_VAR_NAME,
) -> None:
    await arecord(symbols, out_dir, secrets_file, secret_var, _subscribe)


if __name__ == "__main__":
    main(_subscribe, __doc__)
