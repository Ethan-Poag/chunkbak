# chunkbak

FastBack-style **incremental, chunk-level snapshots** for Minecraft Java worlds in the Anvil format
(`.mca`). It works on worlds saved by vanilla, Fabric, Paper/Spigot and Pumpkin servers, because
it only reads the files on disk. It needs no mod, no plugin and no server restart.

It's a single Python file with no dependencies. Every snapshot is a full, restorable copy of the
world, but it only stores chunks that changed since the last one.

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
files, on a homeserver RAID.

| | |
|---|---|
| First snapshot | 18 s, 466 MB of blobs (~790 MB on disk) |
| Later snapshots | 0.4–4 s |
| Growth per 15-minute snapshot, one player online | ~0.1–0.7 MB |
| Unchanged re-run | 0 bytes |

Your numbers depend on disk speed, world size and how much players move around.

## Requirements

- Python 3.9+ (standard library only)
- Linux. The live-server check uses `/proc` and `fcntl`. Backups probably work on macOS too, but
  only Linux is tested.
- The backup repo should sit on a different disk from the world, or be copied off-site.

## Quick start

```sh
curl -O https://raw.githubusercontent.com/Ethan-Poag/chunkbak/main/chunkbak.py
chmod +x chunkbak.py

./chunkbak.py backup /backups/myworld /srv/minecraft/world --note "first"
./chunkbak.py list /backups/myworld
./chunkbak.py verify /backups/myworld --deep
```

### Commands

```
backup  REPO WORLD [--note T] [--pre-hook CMD] [--quiet-secs S] [--max-wait S] [--retries N] [--retry-delay S]
list    REPO
restore REPO SNAP DEST                         whole world into an empty or missing DEST
restore-chunks REPO SNAP WORLD --chunks 'x,z;x,z' [--dim DIR | --region REL] [--force]
verify  REPO [SNAP] [--deep]
prune   REPO [--keep-last N] [--hourly H] [--daily D] [--weekly W] [--monthly M] [--dry-run]
gc      REPO
```

`backup` exits with code 3 if a chunk was torn and had no earlier copy to fall back on. Alert on
any non-zero exit.

## Scheduling (cron)

Ask the server to flush chunks to disk first, then back up. `--pre-hook` runs any shell command,
and `--quiet-secs` waits until no world file has changed for that many seconds (up to
`--max-wait`).

```cron
# every 15 minutes
*/15 * * * * /opt/chunkbak/chunkbak.py backup /backups/myworld /srv/minecraft/world \
    --pre-hook '/usr/local/bin/mc-console save-all' --quiet-secs 3 --max-wait 30 \
    >> /var/log/chunkbak.log 2>&1 || echo "chunkbak failed" | mail -s chunkbak you@example.com

# daily prune + deep verify
40 4 * * * /opt/chunkbak/chunkbak.py prune /backups/myworld --keep-last 8 --hourly 48 --daily 14 --weekly 8 --monthly 6 \
    && /opt/chunkbak/chunkbak.py verify /backups/myworld --deep
```

`mc-console` stands for however you send a console command to your server, for example:

- **RCON:** `mcrcon -H 127.0.0.1 -P 25575 -p "$RCON_PASSWORD" save-all`
- **Docker** ([itzg/minecraft-server](https://github.com/itzg/docker-minecraft-server)):
  `docker exec mc rcon-cli save-all`
- **screen/tmux:** `screen -S mc -X stuff 'save-all\n'`
- **A server reading a FIFO on stdin:** `echo save-all > /srv/minecraft/console.fifo`

On vanilla, Fabric or Paper you can also wrap the backup in `save-off`, then `save-all flush`,
then `save-on` afterwards. That isn't required, because torn reads are detected and retried
either way.

## Restoring

**Stop the server first.** If you write chunks under a running server, it overwrites them with
what it has in memory, and its later writes are lost. `restore-chunks` refuses to run while it
sees the server using the world. `--force` overrides that check. Only use it if you know why.

```sh
# whole world, into a new folder (then swap folders while the server is stopped)
./chunkbak.py restore /backups/myworld 20261109-031046 /srv/minecraft/world-restored

# just a few chunks (chunk coordinates = block coordinates >> 4)
./chunkbak.py restore-chunks /backups/myworld 20261109-031046 /srv/minecraft/world --chunks '-5,-4;-4,-4'

# a chunk in the Nether (classic layout)
./chunkbak.py restore-chunks /backups/myworld 20261109-031046 /srv/minecraft/world --dim DIM-1 --chunks '10,3'
```

Every region file `restore-chunks` changes is first copied to `<file>.before-restore`, so you can
undo it by moving the copies back. chunkbak skips those copies in later backups, so delete them
once you're happy.

By default `restore-chunks` targets the overworld: `dimensions/minecraft/overworld` in the newer
world layout, or the world root in the classic layout. Use `--dim` (for example `DIM-1`, `DIM1`,
or `dimensions/minecraft/the_nether`) or `--region dim/region/r.X.Z.mca` for anything else.

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
./run_tests.sh
```

The tests are self-contained and build tiny synthetic worlds, so they need no server or real
world data:

- a backup, incremental and restore round trip;
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
