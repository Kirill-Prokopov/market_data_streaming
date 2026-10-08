"""L2 order-book recording: raw protobuf pushes, length-prefixed.

Layout: <out_dir>/<SYMBOL>/<YYYY-MM-DD>/<HHMMSS>.bin (UTC; time = first message of the file).
Record (little-endian): 8 bytes receipt time ns | 4 bytes size | protobuf bytes.
Every file is one continuous subscription. If a stream breaks, its file is closed, a row is added to
<out_dir>/<SYMBOL>/<YYYY-MM-DD>/files.log, and the retry writes into a new file.
"""

import asyncio
import time
from pathlib import Path
from typing import BinaryIO

from finam_trade_api import AsyncFinamClient, FinamError
from finam_trade_api.market_data import SubscribeOrderBookRequest
from grpc import StatusCode
from grpc.aio import AioRpcError

from .auth import DEFAULT_SECRET_VAR_NAME, get_async_client

_DAY_S: int = 86_400
_DAY_NS: int = _DAY_S * 10**9
_RETRY_S: float = 0.1
_GIVE_UP: tuple[StatusCode, ...] = (StatusCode.NOT_FOUND, StatusCode.INVALID_ARGUMENT)
_LOG_HEADER: str = "file,first_ns,last_ns,closed_ns,msgs,reason\n"


class _Writer:
    """Writes one symbol's records; opens a file lazily on the first message (and on a new UTC day)."""

    def __init__(self, out_dir: Path, symbol: str) -> None:
        self._symbol_dir: Path = out_dir / symbol.replace("@", "_")
        self._f: BinaryIO | None = None
        self._day: int = 0
        self._first: int = 0
        self._last: int = 0
        self._msgs: int = 0

    def _open(self, t: int) -> BinaryIO:
        gm = time.gmtime(t // 10**9)
        folder = self._symbol_dir / time.strftime("%Y-%m-%d", gm)
        folder.mkdir(parents=True, exist_ok=True)
        self._day, self._first, self._msgs = t // _DAY_NS, t, 0
        self._f = open(folder / (time.strftime("%H%M%S", gm) + ".bin"), "ab", buffering=1 << 10)
        return self._f

    def write(self, t: int, data: bytes) -> None:
        f = self._f
        if f is None or t // _DAY_NS != self._day:
            self.close()
            f = self._open(t)
        f.write(t.to_bytes(8, "little") + len(data).to_bytes(4, "little") + data)
        self._last = t
        self._msgs += 1

    def close(self, reason: str | None = None) -> None:
        """Close the open file. With a `reason` (the stream broke), first log the file in files.log."""
        if self._f is None:
            return
        path = Path(self._f.name)
        self._f.close()
        self._f = None
        if reason is not None:
            log = path.parent / "files.log"
            header = "" if log.exists() else _LOG_HEADER
            with open(log, "a") as g:
                g.write(f"{header}{path.name},{self._first},{self._last},{time.time_ns()},{self._msgs},{reason}\n")


async def _record(client: AsyncFinamClient, symbol: str, writer: _Writer) -> None:
    try:
        while True:
            try:
                async for resp in client.market_data.SubscribeOrderBook(SubscribeOrderBookRequest(symbol=symbol)):
                    t: int = time.time_ns()
                    writer.write(t, resp.SerializeToString())
                reason = "stream_ended"
            except AioRpcError as e:
                reason = e.code().name
                if e.code() in _GIVE_UP:
                    writer.close(reason)
                    print(f"{symbol}: {reason}, giving up on this symbol", flush=True)
                    return
            except FinamError as e:
                reason = type(e).__name__
            writer.close(reason)
            print(f"{symbol}: {reason}, reconnecting in {_RETRY_S}s", flush=True)
            await asyncio.sleep(_RETRY_S)
    finally:
        writer.close()


async def _close_at_midnight(writers: list[_Writer]) -> None:
    """Close all files at every UTC midnight, so the previous day's files are not left open."""
    while True:
        await asyncio.sleep(_DAY_S - time.time() % _DAY_S)
        for w in writers:
            w.close()


async def arecord_orderbook(
    symbols: list[str],
    out_dir: str | Path,
    secrets_file: str | Path,
    secret_var: str = DEFAULT_SECRET_VAR_NAME,
) -> None:
    out = Path(out_dir)
    writers: dict[str, _Writer] = {s: _Writer(out, s) for s in symbols}
    midnight = asyncio.create_task(_close_at_midnight(list(writers.values())))
    try:
        async with get_async_client(secrets_file=secrets_file, var_name=secret_var) as client:
            async with asyncio.TaskGroup() as tg:
                for s, w in writers.items():
                    tg.create_task(_record(client, s, w))
    finally:
        midnight.cancel()


if __name__ == "__main__":
    symbols = ["MXZ6@RTSX", "MMZ6@RTSX"]
    out_dir = Path("~/Code/tmp_data").expanduser()
    secrets_file = Path("~/Code/.secrets_market_data_streaming").expanduser()
    asyncio.run(
        arecord_orderbook(
            symbols=symbols,
            out_dir=out_dir,
            secrets_file=secrets_file,
        )
    )
