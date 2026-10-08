# Deploy (server `first_vds_streaming`)

Daily timeline (UTC): 00:00 recorders close all files and start a new day folder | 00:01 recorders restart
(moves Finam's 24 h stream cut into quiet hours) | 00:30 order-book upload | 00:45 trades upload.
Each upload archives `<symbol>/<date>/` to `<symbol>/<date>/<feed>/<date>.tar.zst` on Yandex Disk, verifies it,
then deletes the local folder.

Layout on the server: repos in `/root/Code/{market_data_streaming,y_disk}`, venv `/root/Code/.venv_market_data_streaming`,
data `/root/Code/market_data/{orderbook,trades}`, secrets `/root/Code/.secrets_market_data_streaming`
(`FINAM_STREAMING_READ_API_KEY`, `Y_DISK_MARKET_DATA_STREAMING_API_KEY`).
Symbols: `symbols/{stocks,bonds,futures}.txt` (one per line, passed to the recorders as `@file`).
Futures expire: refresh `futures.txt` (3 nearest contracts per underlying) before the nearest one expires.

Update: `git pull --ff-only` in the repo, then `systemctl restart market-data-orderbook market-data-trades`.
Units: copy `*.service` / `*.timer` to `/etc/systemd/system/`, `systemctl daemon-reload`, enable the timers and services.
