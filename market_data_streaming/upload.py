"""Archive, upload and clean up closed day folders (adapted from lob_streaming.upload).

For every `<out_dir>/<symbol>/<date>/` whose UTC day is over, one folder at a time:

  1. archive all its files (the .bin files and files.log) into `<symbol>/<date>.tar.zst`: tar, then zstd level 9
     with a content checksum. tar.zst rather than zip/gzip/bzip2 because Yandex Disk answers an upload of those only
     after processing the archive (~0.27 s per MB of *uncompressed* content), while a zstd upload is answered in
     about a second. Read it with `zstd -d`, `tar --zstd -xf`, Python 3.14's `compression.zstd` or `tarfile`;
  2. check the archive round-trips to the same files (sha256 of every member);
  3. upload the archive to `<remote_root>/<symbol>/<date>/<feed>/<date>.tar.zst` -- only the compressed file ever
     goes to Yandex Disk;
  4. read its metadata back from Yandex Disk and require size and checksums to match the local archive;
  5. only then delete the local files and the day folder (and the archive).

Anything that fails leaves the local folder untouched, to be retried on the next run, and makes the run exit
non-zero. Today's (UTC) folder is never touched: its files may still be open.

Needs Python 3.14+ and the sibling `y_disk` package installed separately (not a formal dependency).
"""

import datetime as dt
import hashlib
import tarfile
import time
from compression import zstd
from compression.zstd import CompressionParameter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ZSTD_OPTIONS: dict[CompressionParameter, int] = {
    CompressionParameter.compression_level: 9,
    CompressionParameter.checksum_flag: 1,
}
DEFAULT_TOKEN_NAME: str = "Y_DISK_MARKET_DATA_STREAMING_API_KEY"
CHUNK_SIZE: int = 1 << 20
MIN_AGE_S: float = 600  # skip a folder with a file modified in the last 10 minutes
UPLOAD_ATTEMPTS: int = 3
VERIFY_POLLS: int = 12  # Yandex Disk can take a moment to publish a fresh file's checksums
VERIFY_POLL_S: float = 5.0


class UploadFailed(RuntimeError):
    """One or more folders could not be uploaded and verified (they were kept locally)."""


@dataclass(frozen=True)
class Digests:
    size: int
    md5: str
    sha256: str


def file_digests(path: Path) -> Digests:
    md5 = hashlib.md5(usedforsecurity=False)
    sha256 = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            md5.update(chunk)
            sha256.update(chunk)
            size += len(chunk)
    return Digests(size, md5.hexdigest(), sha256.hexdigest())


def archive_folder(folder: Path) -> Path:
    """`<symbol>/<date>/` -> `<symbol>/<date>.tar.zst`, written under a `.part` name and renamed into place so a
    crash never leaves a truncated-but-complete-looking archive. Streams -- constant memory."""
    archive = folder.with_name(folder.name + ".tar.zst")
    part = archive.with_name(archive.name + ".part")
    part.unlink(missing_ok=True)
    try:
        with zstd.open(part, "wb", options=ZSTD_OPTIONS) as fout, tarfile.open(fileobj=fout, mode="w|") as tar:
            for f in sorted(folder.iterdir()):
                tar.add(f, arcname=f"{folder.name}/{f.name}", recursive=False)
        part.replace(archive)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    return archive


def archive_matches(archive: Path, expected: dict[str, str]) -> bool:
    """True if `archive` decompresses cleanly (its zstd checksum is verified on the way) to exactly the members
    in `expected` ({member name: sha256}). A truncated or damaged archive counts as a mismatch."""
    found: dict[str, str] = {}
    try:
        with zstd.open(archive, "rb") as fin, tarfile.open(fileobj=fin, mode="r|") as tar:
            for member in tar:
                f = tar.extractfile(member)
                if f is None:
                    return False
                sha256 = hashlib.sha256()
                while chunk := f.read(CHUNK_SIZE):
                    sha256.update(chunk)
                found[member.name] = sha256.hexdigest()
    except (zstd.ZstdError, tarfile.TarError, EOFError, OSError):
        return False
    return found == expected


def upload_verified(
    client: Any,
    archive_path: Path,
    remote_path: str,
    digests: Digests,
    attempts: int = UPLOAD_ATTEMPTS,
    polls: int = VERIFY_POLLS,
    poll_s: float = VERIFY_POLL_S,
) -> bool:
    """Upload `archive_path` (replacing any earlier upload at `remote_path`) and return True only once Yandex Disk
    reports the same size, md5 and -- when it reports one -- sha256. A missing md5 is never accepted as a match."""
    from y_disk import ResourceNotFoundError  # optional dependency, imported lazily

    for attempt in range(1, attempts + 1):
        client.upload_file(str(archive_path), remote_path, overwrite=True)
        for poll in range(polls):
            try:
                meta = client.get_meta(remote_path)
            except ResourceNotFoundError:
                meta = None
            if meta is not None and meta.md5 is not None:  # checksums published -- decide now
                if meta.size == digests.size and meta.md5 == digests.md5 and meta.sha256 in (None, digests.sha256):
                    return True
                print(
                    f"  remote mismatch (attempt {attempt}/{attempts}): "
                    f"size {meta.size} vs {digests.size}, md5 {meta.md5} vs {digests.md5}",
                    flush=True,
                )
                break
            if poll < polls - 1:
                time.sleep(poll_s)
        else:
            print(f"  no checksums from Yandex Disk after {polls} polls (attempt {attempt}/{attempts})", flush=True)
    return False


def _snapshot(folder: Path) -> dict[str, tuple[int, int]]:
    """{file name: (size, mtime_ns)}, to detect a folder that changed while it was being processed."""
    out: dict[str, tuple[int, int]] = {}
    for f in folder.iterdir():
        st = f.stat()
        out[f.name] = (st.st_size, st.st_mtime_ns)
    return out


def _process_folder(client: Any, folder: Path, remote_path: str, upload_kwargs: dict[str, Any]) -> bool:
    """Archive, verify, upload, verify remotely, then delete the folder. Returns True only if the folder was
    deleted -- i.e. a verified copy is safely remote."""
    before = _snapshot(folder)
    expected = {f"{folder.name}/{f.name}": file_digests(f).sha256 for f in sorted(folder.iterdir())}
    archive_path = archive_folder(folder)
    try:
        if not archive_matches(archive_path, expected):
            print(f"  FAILED {folder}: archive does not round-trip to the original -- keeping it", flush=True)
            return False
        archive = file_digests(archive_path)
        if not upload_verified(client, archive_path, remote_path, archive, **upload_kwargs):
            print(f"  FAILED {folder}: remote copy could not be verified -- keeping it", flush=True)
            return False
        if _snapshot(folder) != before:
            print(f"  FAILED {folder}: changed while being processed -- keeping it", flush=True)
            return False
        raw_size = sum(size for size, _ in before.values())
        for f in folder.iterdir():
            f.unlink()
        folder.rmdir()
        print(
            f"{remote_path}: {raw_size / 1e6:.1f} MB -> {archive.size / 1e6:.1f} MB "
            f"({raw_size / max(archive.size, 1):.1f}x), verified, local deleted",
            flush=True,
        )
        return True
    finally:
        archive_path.unlink(missing_ok=True)  # derived; regenerated on the next run if needed


def upload_closed_folders(
    out_dir: str | Path,
    secrets_path: str | Path,
    remote_root: str,
    feed: str,
    token_name: str = DEFAULT_TOKEN_NAME,
    dry_run: bool = False,
    min_age_s: float = MIN_AGE_S,
    client: Any = None,
    upload_attempts: int = UPLOAD_ATTEMPTS,
    verify_polls: int = VERIFY_POLLS,
    verify_poll_s: float = VERIFY_POLL_S,
) -> list[str]:
    """Process every `<out_dir>/<symbol>/<date>/` folder of a finished UTC day, as described in the module
    docstring, each uploaded to `<remote_root>/<symbol>/<date>/<feed>/<date>.tar.zst` (replacing any earlier upload of the same
    day in place, so reruns never pile up duplicates). Skips today's folder and any folder with a file modified
    within `min_age_s`. `dry_run=True` only lists what would be done. `secrets_path` is the file `token_name` is
    read from. Returns the remote paths uploaded and verified; raises `UploadFailed` at the end if any folder
    could not be (the others are still processed)."""
    if client is None and not dry_run:
        from y_disk import YandexDiskClient, load_token  # optional dependency, imported lazily

        client = YandexDiskClient(load_token(token_name, secrets_path))
    today = dt.datetime.now(dt.UTC).date().isoformat()
    upload_kwargs = dict(attempts=upload_attempts, polls=verify_polls, poll_s=verify_poll_s)

    symbol_dirs = sorted(p for p in Path(out_dir).iterdir() if p.is_dir())
    if not dry_run:
        for symbol_dir in symbol_dirs:
            for stale in symbol_dir.glob("*.tar.zst.part"):  # a previous run died mid-archive
                stale.unlink()

    uploaded: list[str] = []
    failed: list[str] = []
    for symbol_dir in symbol_dirs:
        for folder in sorted(p for p in symbol_dir.iterdir() if p.is_dir()):
            if folder.name >= today:
                continue  # today's files may still be open for writing -- skip
            mtimes = [f.stat().st_mtime for f in folder.iterdir()]
            if not mtimes:
                continue
            if time.time() - max(mtimes) < min_age_s:
                print(f"skipping {folder}: modified in the last {min_age_s:.0f}s", flush=True)
                continue
            remote_path = f"{remote_root}/{symbol_dir.name}/{folder.name}/{feed}/{folder.name}.tar.zst"
            if dry_run:
                size = sum(f.stat().st_size for f in folder.iterdir())
                print(f"[dry run] would archive, upload and delete {folder} ({size / 1e6:.1f} MB) -> {remote_path}", flush=True)
                uploaded.append(remote_path)
                continue
            try:
                ok = _process_folder(client, folder, remote_path, upload_kwargs)
            except Exception as e:  # one bad folder must not block the rest
                print(f"  FAILED {folder}: {e!r}", flush=True)
                ok = False
            (uploaded if ok else failed).append(remote_path)

    if failed:
        raise UploadFailed(f"{len(failed)} folder(s) not uploaded/verified, kept locally: {failed}")
    return uploaded


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--secrets-path", required=True)
    parser.add_argument("--remote-root", default="app:", help="Yandex Disk folder holding <symbol>/<date>/<feed>/")
    parser.add_argument("--feed", required=True, help="folder name under the date, e.g. orderbook or trades")
    parser.add_argument("--token-name", default=DEFAULT_TOKEN_NAME)
    parser.add_argument("--dry-run", action="store_true", help="only list what would be archived/uploaded/deleted")
    args = parser.parse_args()
    try:
        paths = upload_closed_folders(
            args.out_dir, args.secrets_path, args.remote_root, args.feed, token_name=args.token_name, dry_run=args.dry_run
        )
    except UploadFailed as e:
        print(f"FAILED: {e}", flush=True)
        sys.exit(1)
    print(f"done: {len(paths)} folder(s)", flush=True)
