# chunkbak

FastBack-style **incremental, chunk-level snapshots** for Minecraft Java worlds in the Anvil format
(`.mca`). It works on worlds saved by vanilla, Fabric, Paper/Spigot and Pumpkin servers, because
it only reads the files on disk. It needs no mod, no plugin and no server restart.

It's a single Python file with no dependencies. Every snapshot is a full, restorable copy of the
world, but it only stores chunks that changed since the last one.

## Quick start

You need Python 3.9 or newer. Most Linux and macOS machines already have it. On Windows, get it
from [python.org](https://www.python.org/downloads/) and tick "Add python.exe to PATH" during
setup.

```sh
pipx install https://github.com/Ethan-Poag/chunkbak/archive/main.zip   # 1. install
chunkbak init /path/to/server/world                                     # 2. set up backups + first snapshot
chunkbak backup /path/to/server/world --every 15                        # 3. keep backing up every 15 min
```

If you have git, `pipx install git+https://github.com/Ethan-Poag/chunkbak` works too. So does
`pip install --user` with either source.

That's it. `init` makes a `world-backups` folder next to your world and takes the first snapshot.
Then it prints the exact commands to schedule backups on your system (cron or Windows Task
Scheduler), so you don't have to work them out. `--every 15` keeps running and prunes old
snapshots once a day. If you'd rather not leave it running, use the printed schedule instead.

Point chunkbak at the **world folder**, the one that contains `level.dat` and `region/`. On most
servers that's `world/`. In single-player it's `.minecraft/saves/<World Name>/`.

**No pipx?** chunkbak is one file, so download it and run it with Python:

```sh
curl -O https://raw.githubusercontent.com/Ethan-Poag/chunkbak/main/chunkbak.py
python3 chunkbak.py init /path/to/server/world
python3 chunkbak.py backup /path/to/server/world --every 15
```

On Windows, save
[chunkbak.py](https://raw.githubusercontent.com/Ethan-Poag/chunkbak/main/chunkbak.py)
(right-click, then "Save link as"). Run it as `python chunkbak.py ...` in a Command Prompt.

### Everyday commands

You can give either the world folder or the backup folder. `latest` means the newest snapshot.

```sh
chunkbak list    WORLD                       # what snapshots exist
chunkbak backup  WORLD                       # one snapshot now
chunkbak restore WORLD latest NEW_FOLDER     # whole world into a new folder (server stopped)
chunkbak restore-chunks WORLD latest WORLD --chunks '-5,-4;-4,-4'   # just a few chunks (server stopped)
chunkbak prune   WORLD                       # thin out old snapshots (default policy below)
chunkbak verify  WORLD --deep                # check every stored chunk is intact
chunkbak --help
```

### Let the server save first (recommended)

If RCON is on (`enable-rcon=true` in `server.properties`, which most server hosts support),
chunkbak can ask the server to `save-all` before each snapshot. It then waits until the files stop
changing, so you get the freshest copy:

```sh
export CHUNKBAK_RCON_PASSWORD='your-rcon-password'     # Windows: set CHUNKBAK_RCON_PASSWORD=...
chunkbak backup WORLD --every 15 --rcon localhost:25575
```

For servers without RCON, `--pre-hook 'any shell command'` runs a command first instead. For
example:

- `screen -S mc -X stuff 'save-all\n'`
- `docker exec mc rcon-cli save-all`
- `echo save-all > console.fifo`

Neither option is required. chunkbak detects chunks the server is writing mid-read, retries them,
and never stores a torn chunk.

## Scheduling

### Linux / macOS (cron)

`init` prints these lines with your real paths filled in. Add them with `crontab -e`:

```cron
*/15 * * * * /home/me/.local/bin/chunkbak backup "/srv/mc/world-backups" --rcon localhost:25575 >> "/srv/mc/world-backups/chunkbak.log" 2>&1
40 4 * * *   /home/me/.local/bin/chunkbak prune  "/srv/mc/world-backups" && /home/me/.local/bin/chunkbak verify "/srv/mc/world-backups" --deep
```

Put the RCON password in the crontab as `CHUNKBAK_RCON_PASSWORD=...` on its own line above the
jobs. `backup` exits non-zero on failure, and exits with code 3 if a chunk was torn and had no
earlier copy, so wire up `|| your-alert-command` if you want alerts.

### Windows (Task Scheduler)

`init` prints a ready-made `schtasks` command. Paste it into a Command Prompt once. It looks like
this:

```bat
schtasks /Create /SC MINUTE /MO 15 /TN chunkbak /TR "\"C:\Users\Me\AppData\Local\Programs\Python\Python312\python.exe\" \"C:\Tools\chunkbak.py\" backup \"C:\MC\world-backups\""
```

You can do the same thing in the Task Scheduler app:

1. Choose Create Basic Task, then Daily.
2. Choose Start a program, with program `python` and arguments `C:\Tools\chunkbak.py backup "C:\MC\world-backups"`.
3. Open the task's Properties, then Triggers, then Edit, and set "Repeat task every 15 minutes".
4. Add a second daily task with `prune` in place of `backup`.

You can also skip scheduling. Run `chunkbak backup WORLD --every 15` in a window that stays open,
or use a task that runs it "At log on".

### Docker

Here's a sidecar container next to a server such as
[itzg/minecraft-server](https://github.com/itzg/docker-minecraft-server). See
[`examples/docker-compose.yml`](examples/docker-compose.yml):

```yaml
services:
  chunkbak:
    image: python:3.12-slim
    restart: unless-stopped
    volumes:
      - ./data/world:/world:ro          # the server's world folder (read-only is fine for backups)
      - /mnt/backups/mc:/backups        # put this on another disk
    environment:
      CHUNKBAK_RCON_PASSWORD: change-me # same as the server's RCON_PASSWORD
      TZ: America/New_York              # snapshot names and daily/weekly pruning use local time
    command: >
      sh -c "pip install -q --root-user-action=ignore https://github.com/Ethan-Poag/chunkbak/archive/main.zip &&
             chunkbak backup /backups /world --every 15 --rcon mc:25575"
```

Restores have to run with the server stopped and with the world mounted read-write. Running them
on the host is simplest.

## Requirements

- Python 3.9+, standard library only, with no other dependencies.
- **Linux:** fully supported and tested. That's where the measured numbers come from.
- **Windows and macOS:** supported, but not yet tested by CI.
  - The live-server guard for `restore-chunks` uses `/proc`, which only Linux has. On Windows and
    macOS, chunkbak only checks `session.lock` and warns you to make sure the server is stopped.
- Keep the backup folder on a different disk from the world, or copy it off the machine as well.

## Features

- **Chunk-level dedup.** Each chunk in every region file (`region/`, `entities/`, `poi/`, every
  dimension) is stored once as a content-addressed blob (sha256), so an unchanged chunk costs
  nothing in a new snapshot.
- **Fast incrementals.** A region file whose size and mtime match the previous snapshot is
  reused without being read.
- **Live-server safe reads.** Every region read is checked: the file's stamp must be unchanged
  across the read, and every chunk must decompress to an NBT compound. Reads are retried. A chunk
  that's still torn after the retries keeps its previous copy and is flagged in the manifest.
  chunkbak never stores garbage.
- **Full restore** of any snapshot into an empty folder.
- **Single-chunk or area restore** into a stopped world. It restores terrain, entities and POI
  together and keeps a `.before-restore` copy of every region file it touches. It refuses to run
  while a process has world files open (checked by inode, which also works for Docker) or
  `session.lock` is held.
- **Prune** with a retention policy: keep-last, hourly, daily, weekly or monthly (`--dry-run`
  supported). The newest snapshot is always kept. Prune runs **gc** afterwards.
- **Verify** that every blob exists and hashes right. `--deep` also decompresses every chunk.
- Atomic writes, a repo lock, and blobs synced before the manifest that references them.
  chunkbak never writes to the world during a backup.

## Measured numbers

These come from a real server's world: about 0.9 GB on disk, 121,522 chunks across 1,903 region
files, on a home server's RAID array.

| | |
|---|---|
| First snapshot | 18 s, 466 MB of blobs (~790 MB on disk) |
| Later snapshots | 0.4–4 s |
| Growth per 15-minute snapshot, one player online | ~0.1–0.7 MB |
| Unchanged re-run | 0 bytes |

Your numbers depend on disk speed, world size and how much players move around.

## Restoring

**Stop the server first.** If you write chunks under a running server, it overwrites them with
what it has in memory, and its later writes are lost. `restore-chunks` refuses to run while it
sees the server using the world. `--force` overrides that check. Only use it if you know why.

```sh
chunkbak list /srv/minecraft/world          # pick a snapshot id, or use "latest"

# whole world, into a new folder (then swap folders while the server is stopped)
chunkbak restore /srv/minecraft/world 20261109-031046 /srv/minecraft/world-restored

# just a few chunks: their terrain, mobs and POIs (Nether portals, villager beds, ...) go back
chunkbak restore-chunks /srv/minecraft/world 20261109-031046 /srv/minecraft/world --chunks '-5,-4;-4,-4'

# a chunk in the Nether (classic world layout)
chunkbak restore-chunks /srv/minecraft/world latest /srv/minecraft/world --dim DIM-1 --chunks '10,3'
```

To find chunk coordinates, press F3 in game and read the `Chunk:` line, or divide block X and Z
by 16 and round down.

Every region file `restore-chunks` changes is first copied to `<file>.before-restore`, so you can
undo it by moving the copies back. chunkbak skips those copies in later backups, so delete them
once you're happy.

By default `restore-chunks` targets the overworld: `dimensions/minecraft/overworld` in the newer
world layout, or the world root in the classic layout. Use `--dim` (for example `DIM-1`, `DIM1`,
or `dimensions/minecraft/the_nether`) or `--region dim/region/r.X.Z.mca` for anything else.

## All commands

```
init    WORLD [REPO]                    set up REPO (default: <world>-backups next to it) + first snapshot
backup  WORLD | REPO | REPO WORLD       [--every MIN] [--rcon HOST:PORT] [--rcon-password P] [--rcon-command C]
                                        [--pre-hook CMD] [--quiet-secs S] [--max-wait S] [--retries N] [--note T]
list    REPO
restore REPO SNAP DEST                  whole world into an empty or missing DEST
restore-chunks REPO SNAP WORLD --chunks 'x,z;x,z' [--dim DIR | --region REL] [--force]
verify  REPO [SNAP] [--deep]
prune   REPO [--keep-last N] [--hourly H] [--daily D] [--weekly W] [--monthly M] [--dry-run]
gc      REPO
```

Anywhere a command asks for REPO, you can pass the world folder instead, and its default backup
folder is used. SNAP is a snapshot id from `list`, or `latest`.

With no options, `prune` keeps:

- the last 8 snapshots;
- one per hour for 48 hours;
- one per day for 14 days;
- one per week for 8 weeks;
- one per month for 6 months.

## Repository layout

```
REPO/objects/<h[:2]>/<sha256>     one blob per chunk payload (as stored in the .mca) or per other file
REPO/snapshots/<id>.json.gz        manifest: files -> blob, region -> 1024 (timestamp, blob) slots
REPO/lock
```

See [DESIGN.md](DESIGN.md) for details.

## Limitations

- Blobs are stored one per file, so a repo uses about 1.7x its blob size on disk because of 4 KiB
  filesystem blocks. Pack files aren't implemented yet.
- There's no built-in off-site copy. `rsync` the repo; blobs are immutable, so it syncs cheaply.
- Chunks using LZ4 compression (type 4) are only length-checked, not decompressed, during the
  torn-read check.
- Oversized chunks stored in external `.mcc` files are backed up as whole files.
- There's no in-game command. chunkbak is a command-line tool.
- It doesn't support Bedrock (LevelDB) worlds.

## Tests

```sh
./run_tests.sh        # Windows: run each tests/test_*.py with python
```

The tests are self-contained and build tiny synthetic worlds, so they need no server or real
world data:

- a backup, incremental and restore round trip;
- the beginner path: `init`, the world-folder shortcuts, `latest` and `--every`;
- RCON save-before-backup, against a fake RCON server;
- chunk restore;
- prune, gc and verify, including corruption detection;
- a hostile-writer test that rewrites a region file out of order during 40 back-to-back backups;
- the retention policy;
- the live-server guard.

## Provenance

- **Written with AI assistance** (Claude Code / an AI agent), then reviewed and tested by the
  author on a real server. Besides the tests here, it was proven against a live server writing
  the world during back-to-back snapshots, and with a single-chunk restore into a test server.
- **Clean-room implementation.** The idea of chunk-level, deduplicated world snapshots comes from
  [FastBack](https://github.com/pcal43/fastback) (a Fabric mod, GPL-2.0). No FastBack code or
  documentation was read or copied. chunkbak works from the public Anvil region-file format.

## License

MIT. See [LICENSE](LICENSE).
