#!/usr/bin/env python3
"""chunkbak: chunk-level incremental backups for Minecraft Anvil worlds
(vanilla, Fabric, Paper, Pumpkin, ...).

https://github.com/Ethan-Poag/chunkbak  -  MIT License
Clean-room implementation: FastBack (GPL-2.0) inspired the idea; none of its code or docs were used.

Store layout (REPO):
  objects/<h[:2]>/<h>      content-addressed blobs (sha256 of the stored bytes)
  snapshots/<id>.json.gz   manifest: files -> blob, region files -> 1024 chunk slots
  lock                     held while writing

Every .mca file (region/, entities/, poi/, any dimension) is split into its chunk
payloads; each payload (compression byte + compressed data, exactly as on disk) is one
blob, so an unchanged chunk costs nothing in a new snapshot. Header timestamps are kept
so restore rebuilds a region file with the same content. All other files are whole
blobs. A region whose (size, mtime_ns) match the previous snapshot reuses its entries
without being read.

Live-server safety: every region read is checked (file stamp unchanged across the read,
every chunk decompresses to an NBT compound) and retried; a chunk that is still torn
after the retries keeps its previous snapshot's copy and is listed in the manifest
('carried'). gzip .dat files are checked the same way. Optional --pre-hook (e.g. send
`save-all` to the server console) and --quiet-secs (wait until no world file changed
for that long) run before reading.

Commands:
  backup  REPO WORLD [--note T] [--pre-hook CMD] [--quiet-secs S] [--max-wait S] [--retries N]
  list    REPO
  restore REPO SNAP DEST                    whole world into an empty DEST
  restore-chunks REPO SNAP WORLD --chunks x,z[;x,z...] [--dim DIR | --region REL] [--force]
                                            server must be stopped (checked via /proc)
  verify  REPO [SNAP] [--deep]              blobs exist and hash right (+ chunks decompress)
  prune   REPO [--keep-last N] [--hourly H] [--daily D] [--weekly W] [--monthly M] [--dry-run]
  gc      REPO
"""
import argparse, gzip, hashlib, json, os, struct, sys, time, fcntl, shutil, subprocess, zlib, collections

SECTOR = 4096
EXTERNAL = 0x80


def h(b):
    return hashlib.sha256(b).hexdigest()


class Repo:
    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.join(path, 'objects'), exist_ok=True)
        os.makedirs(os.path.join(path, 'snapshots'), exist_ok=True)
        self.new_blobs = 0
        self.new_bytes = 0

    def obj(self, digest):
        return os.path.join(self.path, 'objects', digest[:2], digest)

    def put(self, data):
        d = h(data)
        p = self.obj(d)
        if not os.path.exists(p):
            os.makedirs(os.path.dirname(p), exist_ok=True)
            tmp = p + '.tmp%d' % os.getpid()
            with open(tmp, 'wb') as f:
                f.write(data)
            os.replace(tmp, p)
            self.new_blobs += 1
            self.new_bytes += len(data)
        return d

    def get(self, d):
        with open(self.obj(d), 'rb') as f:
            data = f.read()
        if h(data) != d:
            raise RuntimeError('corrupt object ' + d)
        return data

    def snapshots(self):
        return sorted(n[:-8] for n in os.listdir(os.path.join(self.path, 'snapshots')) if n.endswith('.json.gz'))

    def load(self, sid):
        with gzip.open(os.path.join(self.path, 'snapshots', sid + '.json.gz'), 'rt') as f:
            return json.load(f)

    def save(self, sid, man):
        p = os.path.join(self.path, 'snapshots', sid + '.json.gz')
        tmp = p + '.tmp'
        with gzip.open(tmp, 'wt') as f:
            json.dump(man, f, separators=(',', ':'))
        os.replace(tmp, p)

    def lock(self):
        self._lf = open(os.path.join(self.path, 'lock'), 'w')
        try:
            fcntl.flock(self._lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            sys.exit('another chunkbak run holds %s/lock' % self.path)


# ---------------------------------------------------------------- region format
def read_region(data, strict=True):
    """-> list of 1024 (timestamp, payload|None). payload = compression byte + data.
    strict=False marks a slot whose record is out of range as (ts, False) instead of raising."""
    if len(data) < 2 * SECTOR:
        return [(0, None)] * 1024
    out = []
    for i in range(1024):
        off = int.from_bytes(data[i * 4:i * 4 + 3], 'big')
        cnt = data[i * 4 + 3]
        ts = struct.unpack('>I', data[SECTOR + i * 4:SECTOR + i * 4 + 4])[0]
        if off == 0 or cnt == 0:
            out.append((ts, None)); continue
        o = off * SECTOR
        ln = struct.unpack('>I', data[o:o + 4])[0] if o + 5 <= len(data) else 0
        if ln < 1 or o + 4 + ln > len(data) or ln > cnt * SECTOR:
            if strict:
                raise RuntimeError('chunk %d has bad record (offset %d, length %d)' % (i, off, ln))
            out.append((ts, False)); continue
        out.append((ts, data[o + 4:o + 4 + ln]))
    return out


def write_region(slots):
    """slots: list of 1024 (timestamp, payload|None) -> region bytes."""
    header = bytearray(2 * SECTOR)
    body = bytearray()
    sector = 2
    for i, (ts, payload) in enumerate(slots):
        struct.pack_into('>I', header, SECTOR + i * 4, ts)
        if payload is None:
            continue
        rec = struct.pack('>I', len(payload)) + payload
        n = (len(rec) + SECTOR - 1) // SECTOR
        if n > 255:
            raise RuntimeError('chunk %d too large for inline storage (%d sectors)' % (i, n))
        rec += b'\0' * (n * SECTOR - len(rec))
        header[i * 4:i * 4 + 3] = sector.to_bytes(3, 'big')
        header[i * 4 + 3] = n
        body += rec
        sector += n
    return bytes(header + body)


def payload_ok(p):
    """True if a chunk payload is intact: decompresses (zlib/gzip have checksums) to an NBT compound.
    External-stream stubs (.mcc) are 1 byte; LZ4 (type 4) is only length-checked."""
    if not p:
        return False
    t = p[0]
    if t & EXTERNAL:
        return len(p) == 1
    try:
        if t == 1:
            raw = gzip.decompress(p[1:])
        elif t == 2:
            raw = zlib.decompress(p[1:])
        elif t == 3:
            raw = p[1:]
        elif t == 4:
            return len(p) > 9
        else:
            return False
    except Exception:
        return False
    return len(raw) > 3 and raw[0] == 10


def dat_ok(rel, data):
    if rel.endswith('.dat') or rel.endswith('.dat_old'):
        try:
            return gzip.decompress(data)[:1] == b'\n'
        except Exception:
            return False
    return True


def stamp_of(path):
    s = os.stat(path)
    return [s.st_size, s.st_mtime_ns]


def read_stable(path, check, retries, delay):
    """Read a file until its stamp is unchanged across the read and check(data) passes.
    -> (data, stamp, attempts, ok)."""
    data = stamp = None
    for attempt in range(1, retries + 2):
        try:
            st1 = stamp_of(path)
            with open(path, 'rb') as f:
                data = f.read()
            st2 = stamp_of(path)
        except FileNotFoundError:  # renamed/deleted while we looked (temp files, level.dat_new)
            return None, None, attempt, False
        stamp = st2
        if st1 == st2 and len(data) == st2[0] and check(data):
            return data, stamp, attempt, True
        time.sleep(delay * attempt)
    return data, stamp, retries + 1, False


def walk(world):
    for root, dirs, files in os.walk(world):
        dirs.sort()
        for n in sorted(files):
            if (n == 'session.lock' or n.endswith(('.tmp', '_new', '.cbtmp', '.before-restore'))
                    or n.rsplit('.', 1)[-1].startswith('tmp')):
                continue
            full = os.path.join(root, n)
            yield os.path.relpath(full, world), full


def wait_quiet(world, quiet, max_wait):
    """Wait until no file in the world changed for `quiet` seconds (max `max_wait`). -> seconds waited."""
    t0 = time.time()
    while True:
        newest = 0
        for _, full in walk(world):
            try:
                newest = max(newest, os.stat(full).st_mtime)
            except FileNotFoundError:
                pass
        idle = time.time() - newest
        if idle >= quiet or time.time() - t0 >= max_wait:
            return round(time.time() - t0, 1)
        time.sleep(min(1.0, quiet - idle + 0.05))


# ---------------------------------------------------------------- commands
def cmd_backup(a):
    repo = Repo(a.repo); repo.lock()
    snaps = repo.snapshots()
    prev = repo.load(snaps[-1]) if snaps else {'files': {}, 'regions': {}}
    t0 = time.time()
    if a.pre_hook:
        r = subprocess.run(a.pre_hook, shell=True)
        if r.returncode:
            print('WARN pre-hook exit %d' % r.returncode, file=sys.stderr)
    waited = wait_quiet(a.world, a.quiet_secs, a.max_wait) if a.quiet_secs else 0
    man = {'created': time.time(), 'world': os.path.abspath(a.world), 'note': a.note,
           'files': {}, 'regions': {}, 'carried': []}
    st = collections.Counter(regions_read=0, regions_reused=0, chunks=0, chunks_new=0, files=0,
                             retried_reads=0, carried_chunks=0, carried_files=0, lost_chunks=0)
    for rel, full in walk(a.world):
        try:
            stamp = stamp_of(full)
        except FileNotFoundError:
            continue
        if rel.endswith('.mca'):
            old = prev['regions'].get(rel)
            if old and old['stamp'] == stamp:
                man['regions'][rel] = old; st['regions_reused'] += 1
                st['chunks'] += sum(1 for c in old['chunks'] if c)
                continue

            def region_ok(d):
                return all(p is None or (p is not False and payload_ok(p)) for _, p in read_region(d, strict=False))
            data, stamp, tries, ok = read_stable(full, region_ok, a.retries, a.retry_delay)
            st['retried_reads'] += tries - 1
            if data is None:
                if old:
                    man['regions'][rel] = dict(old, stamp=[-1, -1])
                continue
            before = repo.new_blobs
            chunks = []
            for i, (ts, payload) in enumerate(read_region(data, strict=False)):
                if payload is None:
                    chunks.append(None); continue
                if payload is False or not payload_ok(payload):
                    oc = old['chunks'][i] if old else None
                    if oc:
                        chunks.append(oc); st['carried_chunks'] += 1; man['carried'].append([rel, i])
                    else:
                        chunks.append(None); st['lost_chunks'] += 1; man['carried'].append([rel, i, 'lost'])
                    print('WARN %s slot %d torn after %d reads; %s' % (rel, i, tries, 'kept previous copy' if oc else 'no previous copy'), file=sys.stderr)
                    continue
                chunks.append([ts, repo.put(payload)])
            st['chunks'] += sum(1 for c in chunks if c)
            st['chunks_new'] += repo.new_blobs - before
            # a region with carried slots must be re-read next time: poison its stamp
            man['regions'][rel] = {'stamp': stamp if ok else [-1, -1], 'chunks': chunks}
            st['regions_read'] += 1
        else:
            data, stamp, tries, ok = read_stable(full, lambda d: dat_ok(rel, d), a.retries, a.retry_delay)
            st['retried_reads'] += tries - 1
            if data is None and rel not in prev['files']:
                continue  # a temp file that vanished
            if (data is None or not ok) and rel in prev['files']:
                man['files'][rel] = dict(prev['files'][rel], stamp=[-1, -1])
                st['carried_files'] += 1; man['carried'].append([rel])
                print('WARN %s unreadable after %d reads; kept previous copy' % (rel, tries), file=sys.stderr)
            else:
                man['files'][rel] = {'stamp': stamp, 'blob': repo.put(data)}
            st['files'] += 1
    sid = time.strftime('%Y%m%d-%H%M%S', time.localtime(man['created']))
    while sid in snaps:
        sid += 'b'
    st.update(new_blobs=repo.new_blobs, new_bytes=repo.new_bytes, quiet_wait=waited,
              seconds=round(time.time() - t0, 2))
    man['stats'] = dict(st)
    os.sync()  # blobs durable before the manifest that references them
    repo.save(sid, man)
    os.sync()
    print(sid, json.dumps(man['stats']))
    if st['lost_chunks']:
        sys.exit(3)


def cmd_list(a):
    repo = Repo(a.repo)
    for sid in repo.snapshots():
        m = repo.load(sid)
        print(sid, m.get('note') or '', json.dumps(m.get('stats', {})))


def cmd_restore(a):
    repo = Repo(a.repo); man = repo.load(a.snap)
    if os.path.exists(a.dest) and os.listdir(a.dest):
        sys.exit('DEST must be empty or missing')
    for rel, e in man['files'].items():
        p = os.path.join(a.dest, rel); os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, 'wb') as f:
            f.write(repo.get(e['blob']))
    for rel, e in man['regions'].items():
        slots = [(c[0], repo.get(c[1])) if c else (0, None) for c in e['chunks']]
        p = os.path.join(a.dest, rel); os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, 'wb') as f:
            f.write(write_region(slots))
    print('restored', a.snap, 'to', a.dest, len(man['files']), 'files', len(man['regions']), 'regions')


def server_signs(world):
    """Reasons to believe a server is using WORLD. Checks (a) every /proc/PID/fd by device+inode, so it
    also sees servers in containers whose paths differ (needs same uid or root), and (b) a held
    session.lock (vanilla/Fabric/Paper lock it; some servers, e.g. Pumpkin, do not)."""
    inodes = set()
    for _, full in walk(world):
        try:
            s = os.stat(full); inodes.add((s.st_dev, s.st_ino))
        except OSError:
            pass
    reasons, unreadable = [], 0
    for pid in os.listdir('/proc'):
        if not pid.isdigit() or int(pid) == os.getpid():
            continue
        try:
            fds = os.listdir('/proc/%s/fd' % pid)
        except OSError:
            unreadable += 1; continue
        for fd in fds:
            try:
                s = os.stat('/proc/%s/fd/%s' % (pid, fd))
            except OSError:
                continue
            if (s.st_dev, s.st_ino) in inodes:
                reasons.append('pid %s has world files open' % pid); break
    lock = os.path.join(world, 'session.lock')
    if os.path.exists(lock):
        try:
            with open(lock, 'r+b') as f:
                fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB); fcntl.lockf(f, fcntl.LOCK_UN)
        except OSError:
            reasons.append('session.lock is held')
    return reasons, unreadable


def _restore_region_chunks(repo, man, world, region, idxs):
    """Put slots idxs of REGION back to the snapshot state. -> number of slots that changed."""
    e = man['regions'].get(region)
    full = os.path.join(world, region)
    if not e and not os.path.exists(full):
        return 0
    cur = read_region(open(full, 'rb').read()) if os.path.exists(full) else [(0, None)] * 1024
    changed = 0
    d = os.path.dirname(region)
    rx, rz = [int(v) for v in os.path.basename(region).split('.')[1:3]]
    for i in idxs:
        c = e['chunks'][i] if e else None
        new = (c[0], repo.get(c[1])) if c else (0, None)
        if new[1] and new[1][0] & EXTERNAL:  # oversized chunk: bring its .mcc back too
            mcc = os.path.join(d, 'c.%d.%d.mcc' % (rx * 32 + i % 32, rz * 32 + i // 32))
            if mcc in man['files']:
                with open(os.path.join(world, mcc), 'wb') as f:
                    f.write(repo.get(man['files'][mcc]['blob']))
        if cur[i][1] != new[1]:
            changed += 1
        cur[i] = new
    if os.path.exists(full):
        shutil.copy2(full, full + '.before-restore')
    os.makedirs(os.path.dirname(full), exist_ok=True)
    tmp = full + '.cbtmp'
    with open(tmp, 'wb') as f:
        f.write(write_region(cur)); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, full)
    return changed


def cmd_restore_chunks(a):
    """Put selected chunks back from a snapshot into a STOPPED world: the terrain region
    and the matching entities/ and poi/ regions, so mobs and POIs match the blocks."""
    repo = Repo(a.repo); man = repo.load(a.snap)
    busy, unreadable = server_signs(a.world)
    if busy and not a.force:
        sys.exit('server looks live (%s): stop it first (or --force)' % '; '.join(busy))
    if unreadable and not busy:
        print('note: %d processes could not be inspected (other users); run as root for a full check' % unreadable, file=sys.stderr)
    chunks = [tuple(int(v) for v in xz.split(',')) for xz in a.chunks.replace(' ', '').split(';') if xz]
    if a.region:
        base = os.path.dirname(os.path.dirname(a.region))
        rx, rz = [int(v) for v in os.path.basename(a.region).split('.')[1:3]]
        bad = [c for c in chunks if (c[0] >> 5, c[1] >> 5) != (rx, rz)]
        if bad:
            sys.exit('chunks %s are not in %s' % (bad, a.region))
    elif a.dim is not None:
        base = a.dim
    else:
        base = 'dimensions/minecraft/overworld' if any(r.startswith('dimensions/minecraft/overworld/') for r in man['regions']) else ''
    by_region = collections.defaultdict(list)
    for x, z in chunks:
        by_region['r.%d.%d.mca' % (x >> 5, z >> 5)].append((x & 31) + (z & 31) * 32)
    for name, idxs in sorted(by_region.items()):
        if os.path.join(base, 'region', name) not in man['regions']:
            sys.exit('region not in snapshot: ' + os.path.join(base, 'region', name))
        for layer in ('region', 'entities', 'poi'):
            rel = os.path.join(base, layer, name)
            if os.path.exists(os.path.join(a.world, rel)) or rel in man['regions']:
                n = _restore_region_chunks(repo, man, a.world, rel, idxs)
                print('restored %d chunk(s) in %s from %s (%d differed)' % (len(idxs), rel, a.snap, n))


def referenced(man):
    for e in man['files'].values():
        yield e['blob']
    for e in man['regions'].values():
        for c in e['chunks']:
            if c:
                yield c[1]


def chunk_blobs(man):
    for e in man['regions'].values():
        for c in e['chunks']:
            if c:
                yield c[1]


def cmd_verify(a):
    repo = Repo(a.repo); bad = 0; n = 0; deep = 0
    for sid in ([a.snap] if a.snap else repo.snapshots()):
        man = repo.load(sid)
        cb = set(chunk_blobs(man)) if a.deep else set()
        for d in set(referenced(man)):
            n += 1
            try:
                data = repo.get(d)
                if d in cb:
                    deep += 1
                    if not payload_ok(data):
                        raise RuntimeError('chunk payload does not decompress to NBT')
            except Exception as ex:
                bad += 1; print(sid, 'BAD', d, ex)
    print('verified', n, 'blobs,', bad, 'bad' + (', %d chunk payloads decompressed' % deep if a.deep else ''))
    sys.exit(1 if bad else 0)


def cmd_gc(a):
    repo = Repo(a.repo); repo.lock()
    live = set()
    for sid in repo.snapshots():
        live.update(referenced(repo.load(sid)))
    removed = freed = 0
    objdir = os.path.join(repo.path, 'objects')
    for sub in os.listdir(objdir):
        for n in os.listdir(os.path.join(objdir, sub)):
            if n not in live:
                p = os.path.join(objdir, sub, n); freed += os.path.getsize(p); os.remove(p); removed += 1
    print('gc removed', removed, 'blobs,', freed, 'bytes')


def retention_keep(times, keep_last=0, hourly=0, daily=0, weekly=0, monthly=0):
    """times: {sid: epoch}. Keep the newest keep_last, plus the newest snapshot in each of the
    last `hourly` hours / `daily` days / `weekly` ISO weeks / `monthly` months (local time) that
    have one. -> set of sids to keep."""
    order = sorted(times, key=lambda s: times[s], reverse=True)
    keep = set(order[:keep_last])
    for count, fmt in ((hourly, '%Y%m%d%H'), (daily, '%Y%m%d'), (weekly, '%G%V'), (monthly, '%Y%m')):
        seen = []
        for s in order:
            b = time.strftime(fmt, time.localtime(times[s]))
            if b in seen:
                continue
            if len(seen) >= count:
                break
            seen.append(b); keep.add(s)
    return keep


def cmd_prune(a):
    repo = Repo(a.repo)
    snaps = repo.snapshots()
    times = {s: repo.load(s)['created'] for s in snaps}
    keep_last = a.keep if a.keep is not None else a.keep_last
    if not (keep_last or a.hourly or a.daily or a.weekly or a.monthly):
        sys.exit('refusing to prune with an empty policy')
    keep = retention_keep(times, keep_last, a.hourly, a.daily, a.weekly, a.monthly)
    if snaps:
        keep.add(max(snaps, key=lambda s: times[s]))  # never drop the newest
    for sid in snaps:
        if sid not in keep:
            print(('would drop' if a.dry_run else 'dropped'), sid)
            if not a.dry_run:
                os.remove(os.path.join(repo.path, 'snapshots', sid + '.json.gz'))
    print('kept', len(keep), 'of', len(snaps))
    if not a.dry_run:
        cmd_gc(a)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest='cmd', required=True)
    b = sp.add_parser('backup'); b.add_argument('repo'); b.add_argument('world'); b.add_argument('--note', default='')
    b.add_argument('--pre-hook'); b.add_argument('--quiet-secs', type=float, default=0)
    b.add_argument('--max-wait', type=float, default=60); b.add_argument('--retries', type=int, default=5)
    b.add_argument('--retry-delay', type=float, default=0.2)
    l = sp.add_parser('list'); l.add_argument('repo')
    r = sp.add_parser('restore'); r.add_argument('repo'); r.add_argument('snap'); r.add_argument('dest')
    rc = sp.add_parser('restore-chunks'); rc.add_argument('repo'); rc.add_argument('snap'); rc.add_argument('world')
    rc.add_argument('--region'); rc.add_argument('--dim'); rc.add_argument('--chunks', required=True)
    rc.add_argument('--force', action='store_true')
    v = sp.add_parser('verify'); v.add_argument('repo'); v.add_argument('snap', nargs='?'); v.add_argument('--deep', action='store_true')
    g = sp.add_parser('gc'); g.add_argument('repo')
    pr = sp.add_parser('prune'); pr.add_argument('repo'); pr.add_argument('--keep', type=int)
    pr.add_argument('--keep-last', type=int, default=0)
    for k in ('hourly', 'daily', 'weekly', 'monthly'):
        pr.add_argument('--' + k, type=int, default=0)
    pr.add_argument('--dry-run', action='store_true')
    a = p.parse_args()
    {'backup': cmd_backup, 'list': cmd_list, 'restore': cmd_restore, 'restore-chunks': cmd_restore_chunks,
     'verify': cmd_verify, 'gc': cmd_gc, 'prune': cmd_prune}[a.cmd](a)


if __name__ == '__main__':
    main()
