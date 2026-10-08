"""Shared recorder for Finam server streams: raw protobuf pushes, length-prefixed.

Layout: <out_dir>/<SYMBOL>/<YYYY-MM-DD>/<HHMMSS>.bin (UTC; time = first message of the file).
Record (little-endian): 8 bytes receipt time ns | 4 bytes size | protobuf bytes.
Every file is one continuous subscription. If a stream breaks, its file is closed, a row is added to
<out_dir>/<SYMBOL>/<YYYY-MM-DD>/files.log, and the retry writes into a new file. All files are closed at UTC midnight.
"""

import argparse
import asyncio
import signal
import time
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any, BinaryIO

from finam_trade_api import AsyncFinamClient, FinamError
from grpc.aio import AioRpcError

from .auth import DEFAULT_SECRET_VAR_NAME, get_async_client

Subscribe = Callable[[AsyncFinamClient, str], AsyncIterator[Any]]

_DAY_S: int = 86_400
_DAY_NS: int = _DAY_S * 10**9
_RETRY_S: float = 5  # also keeps two sessions of one symbol from sharing a HHMMSS file name
_SUBSCRIBE_GAP_S: float = 0.6  # Finam rate-limits stream opens (~200/min, shared by all streams of the account)
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
        self._f = open(folder / (time.strftime("%H%M%S", gm) + ".bin"), "ab", buffering=1 << 20)
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


class _Pacer:
    """Lets one subscribe through every `gap_s`, so a start-up or a mass reconnect stays under the rate limit."""

    def __init__(self, gap_s: float) -> None:
        self._gap_s: float = gap_s
        self._lock: asyncio.Lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            await asyncio.sleep(self._gap_s)


async def _record(client: AsyncFinamClient, symbol: str, writer: _Writer, subscribe: Subscribe, pacer: _Pacer) -> None:
    try:
        while True:
            await pacer.wait()
            try:
                async for resp in subscribe(client, symbol):
                    t: int = time.time_ns()
                    writer.write(t, resp.SerializeToString())
                reason = "stream_ended"
            except AioRpcError as e:
                reason = e.code().name
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


async def arecord(
    symbols: list[str],
    out_dir: str | Path,
    secrets_file: str | Path,
    secret_var: str,
    subscribe: Subscribe,
) -> None:
    out = Path(out_dir)
    writers: dict[str, _Writer] = {s: _Writer(out, s) for s in symbols}
    pacer = _Pacer(_SUBSCRIBE_GAP_S)
    midnight = asyncio.create_task(_close_at_midnight(list(writers.values())))
    try:
        async with get_async_client(secrets_file=secrets_file, var_name=secret_var) as client:
            async with asyncio.TaskGroup() as tg:
                for s, w in writers.items():
                    tg.create_task(_record(client, s, w, subscribe, pacer))
    finally:
        midnight.cancel()


async def _run(args: argparse.Namespace, subscribe: Subscribe) -> None:
    task = asyncio.current_task()
    assert task is not None
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)  # systemd stop/restart closes files
    await arecord(args.symbols, args.out_dir, args.secrets_file, args.secret_var, subscribe)


def _parser(description: str | None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter, fromfile_prefix_chars="@"
    )
    parser.add_argument("symbols", nargs="+", help="e.g. MXZ6@RTSX MMZ6@RTSX, or @file with one symbol per line")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--secrets-file", required=True)
    parser.add_argument("--secret-var", default=DEFAULT_SECRET_VAR_NAME)
    return parser


def main(subscribe: Subscribe, description: str | None) -> None:
    """Command line entry point shared by `orderbook` and `trades`; runs until Ctrl+C or SIGTERM."""
    args = _parser(description).parse_args()
    print(f"recording {len(args.symbols)} symbols -> {args.out_dir} (Ctrl+C to stop)", flush=True)
    try:
        asyncio.run(_run(args, subscribe))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("stopped", flush=True)
