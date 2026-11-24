# Deploy (server `first_vds_streaming`)

Daily timeline (UTC): 00:00 recorders close all files and start a new day folder | 00:01 recorders restart
(moves Finam's 24 h stream cut into quiet hours) | 00:30 order-book upload | 00:45 trades upload | 01:00 quotes upload.
Each upload archives `<symbol>/<date>/` to `<symbol>/<date>/<feed>/<date>.tar.zst` on Yandex Disk, verifies it,
then deletes the local folder.

Layout on the server: repos in `/root/Code/{market_data_streaming,y_disk}`, venv `/root/Code/.venv_market_data_streaming`,
data `/root/Code/market_data/{orderbook,trades,quotes}`, secrets `/root/Code/.secrets_market_data_streaming`
(`FINAM_STREAMING_READ_API_KEY`, `Y_DISK_MARKET_DATA_STREAMING_API_KEY`).
Symbols for the order-book and trades recorders (295): `symbols/{stocks,ofz,corp_bonds,futures,perpetuals}.txt`
(one per line, passed to the recorders as `@file`). The quotes recorder (`market_data_streaming.quotes`, one
`SubscribeQuote` stream per symbol) records the 17 MOEX indices in `symbols/indices.txt`; indices have no order book.
Stream opens are rate-limited by Finam (~200/min, shared): the recorder paces them (one per 0.6 s), so a start takes ~3 min.
Futures expire and bonds mature: Finam then answers `UNIMPLEMENTED` and the recorder retries every 5 s forever, so
remove dead symbols and refresh `futures.txt` (3 nearest contracts per underlying) before the nearest one expires.
The lists are read only at start: push and `git pull` before 00:01 UTC and the nightly restart applies them.
Upcoming: NGX6 2026-11-25, BRZ6 2026-12-01, the Z6 futures 2026-12-17/18, NGZ6 2026-12-29, BRF7 2027-01-04;
bonds SU29016RMFS1 2026-12-23, RU000A107RZ0 2027-01-24, SU26207RMFS9 2027-02-03.

Update: `git pull --ff-only` in the repo, then `systemctl restart market-data-orderbook market-data-trades`
(or wait for the nightly restart). A new feed needs no restart of the others: install its units and enable it.
Units: copy `*.service` / `*.timer` to `/etc/systemd/system/`, `systemctl daemon-reload`, enable the timers and services.
