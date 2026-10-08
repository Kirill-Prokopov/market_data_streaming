"""Trade (time & sales) recording (`MarketDataService.SubscribeLatestTrades`); see recorder.py for the file format.

The feed only goes forward from the moment you subscribe, so history exists only if it is recorded continuously.
The recording is raw. When reading it back:
* the server re-sends the latest trade flagged `is_data_snapshot=True` right after (re)subscribing and sometimes
  mid-stream, so de-duplicate on (symbol, trade_id);
* because of that first snapshot a file is not strictly ordered by time right after a subscribe, but `trade_id`
  increases monotonically, so sort on it.

Run: python -m market_data_streaming.trades SYMBOL... --out-dir DIR --secrets-file FILE
"""

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from finam_trade_api import AsyncFinamClient
from finam_trade_api.market_data import SubscribeLatestTradesRequest

from .auth import DEFAULT_SECRET_VAR_NAME
from .recorder import arecord, main


def _subscribe(client: AsyncFinamClient, symbol: str) -> AsyncIterator[Any]:
    return client.market_data.SubscribeLatestTrades(SubscribeLatestTradesRequest(symbol=symbol))


async def arecord_trades(
    symbols: list[str],
    out_dir: str | Path,
    secrets_file: str | Path,
    secret_var: str = DEFAULT_SECRET_VAR_NAME,
) -> None:
    await arecord(symbols, out_dir, secrets_file, secret_var, _subscribe)


if __name__ == "__main__":
    main(_subscribe, __doc__)
