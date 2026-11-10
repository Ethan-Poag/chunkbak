# chunkbak design

Clean-room implementation. FastBack (GPL-2.0) was not read or copied. The only thing shared with
it is the idea of backing up per chunk and deduplicating unchanged chunks.

## Model
- The store is content-addressed: `REPO/objects/<h[:2]>/<sha256>`.
  - Each chunk payload gets one blob, kept exactly as stored in the `.mca`: the compression byte
    plus the compressed bytes.
  - Each non-region file gets one blob: `level.dat`, `playerdata/*.dat`, `data/*.dat`, `.mcc`,
    and so on.
- A snapshot is a gzipped JSON manifest:
  - `files{rel: {stamp, blob}}`
  - `regions{rel: {stamp, chunks: [1024 × [timestamp, blob] | null]}}`
  - `carried`
  - `stats`
- Every `.mca` under the world is split this way: `region/`, `entities/`, `poi/`, in every
  dimension and in both world layouts (classic `DIM-1`/`DIM1` and `dimensions/<ns>/<name>`).
- **Fast path:** if a region file's (size, mtime_ns) equals the previous snapshot's, its entries
  are reused without reading the file.
- An unchanged chunk has the same bytes, so the same hash, so it adds 0 bytes. Servers recompress
  a chunk only when they save it, so only touched chunks produce new blobs.
- Region timestamps are kept, so a restore rebuilds a region file with the same chunk content.
  Sector layout may differ, which is harmless.

## Writes
- Each blob is written to a temp file and renamed into place. One `os.sync()` runs before the
  manifest is written and one after. An fsync per blob made a 121k-chunk first snapshot take
  375 s instead of 18 s.
- A repo lock (`flock`) stops two runs from writing at once.
- A backup never writes to the world.

## Consistency with a running server
Servers write region files in place. Typically a new chunk record goes to free sectors, then the
header is rewritten, then the old sectors are freed. A read therefore usually sees a consistent
file, but a sector freed by one flush can be reused by the next while the read is happening.

chunkbak handles this as follows:
- It reads each file until its (size, mtime) is unchanged across the read **and** every chunk
  decompresses to an NBT compound. zlib and gzip checksums catch torn data.
- It retries with backoff (`--retries`, `--retry-delay`).
- A slot that's still torn keeps the previous snapshot's copy and is listed in `carried`. Its
  region's stamp is poisoned so that the region is re-read on the next run.
- A torn chunk with no earlier copy is recorded as lost, and the run exits with code 3.
- gzip `.dat` files are checked the same way. Temp files (`*.tmp*`, `*_new`, `session.lock`) are
  skipped. A file that vanishes mid-read is skipped without crashing the run.
- `--pre-hook CMD` (for example, sending `save-all` to the console) and `--quiet-secs S` (wait until
  no world file has changed for S seconds) make a fully quiet snapshot likely.

## Restore
- `restore REPO SNAP DEST` restores the full world into an empty folder.
- `restore-chunks` rolls back the chosen chunks in a **stopped** world:
  - It restores the terrain, entities and POI region files together, so mobs and points of
    interest match the blocks.
  - It brings back the `.mcc` file of an oversized chunk.
  - It writes `*.before-restore` copies first, then writes each region through a temp file,
    fsync and rename.
- **Guard:** `restore-chunks` refuses while any process has a world file open, or while
  `session.lock` is held.
  - Open files are matched by device and inode, so it also works for a server in a container,
    where paths differ. A full check needs root or the server's uid.
  - Some servers don't hold `session.lock`.
  - Restoring under a live server silently loses its later writes. That's why the guard exists.

## Retention
- `prune` keeps the newest `--keep-last` snapshots.
- It also keeps the newest snapshot in each of the last H hours, D days, W ISO weeks and M months
  (local time) that have one.
- The newest snapshot is always kept, and an empty policy is refused.
- `gc` then deletes unreferenced blobs.

## Ideas, not done
- Pack files: a repo uses about 1.7x its blob size on disk because of per-file 4 KiB blocks.
- Optional zstd on non-region files.
- A built-in off-site sync.
- An in-game `/backup` command via a server plugin or mod.
