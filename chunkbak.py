#!/usr/bin/env python3
"""chunkbak: chunk-level incremental backups for Minecraft Anvil worlds
(vanilla, Fabric, Paper, Pumpkin, ...).

https://github.com/Ethan-Poag/chunkbak  -  MIT License
Clean-room implementation: FastBack (GPL-2.0) inspired the idea; none of its code or docs were used.

Quick start:
  chunkbak init  WORLD          set up a backup folder next to the world and take the first snapshot
  chunkbak backup WORLD         take another snapshot (add --every 15 to keep running every 15 min)

Wherever REPO is asked for you can also give the WORLD folder: its default backup folder
(<world>-backups next to it) is used. SNAP can be a snapshot id from `list` or `latest`.

Commands:
  init    WORLD [REPO]                      create the backup folder, take the first snapshot
  backup  [REPO] WORLD | REPO | WORLD       [--every MIN] [--rcon HOST:PORT] [--pre-hook CMD] [--note T]
            [--only-when-players HOST:PORT] [--idle-every 24h] ...
  list    REPO
  restore REPO SNAP DEST                    whole world into an empty DEST
  restore-chunks REPO SNAP WORLD --chunks x,z[;x,z...] [--dim DIR | --region REL] [--force]
                                            server must be stopped
  verify  REPO [SNAP] [--deep]              blobs exist and hash right (+ chunks decompress)
  prune   REPO [--keep-last N] [--hourly H] [--daily D] [--weekly W] [--monthly M] [--dry-run]
          (no options = keep-last 8, hourly 48, daily 14, weekly 8, monthly 6)
  gc      REPO
"""
# Store layout (REPO):
#   objects/<h[:2]>/<h>      content-addressed blobs (sha256 of the stored bytes)
#   snapshots/<id>.json.gz   manifest: files -> blob, region files -> 1024 chunk slots
#   lock                     held while writing
#
# Every .mca file (region/, entities/, poi/, any dimension) is split into its chunk
# payloads; each payload (compression byte + compressed data, exactly as on disk) is one
# blob, so an unchanged chunk costs nothing in a new snapshot. Header timestamps are kept
# so restore rebuilds a region file with the same content. All other files are whole
# blobs. A region whose (size, mtime_ns) match the previous snapshot reuses its entries
# without being read.
#
# Live-server safety: every region read is checked (file stamp unchanged across the read,
# every chunk decompresses to an NBT compound) and retried; a chunk that is still torn
# after the retries keeps its previous snapshot's copy and is listed in the manifest
# ('carried'). gzip .dat files are checked the same way. Optional --pre-hook (e.g. send
# `save-all` to the server console) and --quiet-secs (wait until no world file changed
# for that long) run before reading.
import argparse, gzip, hashlib, json, os, posixpath, socket, struct, sys, time, shutil, subprocess, zlib, collections
try:
    import fcntl
except ImportError:  # Windows
    fcntl = None
    import msvcrt

__version__ = '1.1.0'
CONFIG = 'chunkbak.json'
DEFAULT_POLICY = dict(keep_last=8, hourly=48, daily=14, weekly=8, monthly=6)

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
            if fcntl:
                fcntl.flock(self._lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                msvcrt.locking(self._lf.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            sys.exit('another chunkbak run holds %s' % os.path.join(self.path, 'lock'))

    def unlock(self):
        lf = getattr(self, '_lf', None)
        if lf:
            if fcntl:
                fcntl.flock(lf, fcntl.LOCK_UN)
            else:
                try:
                    msvcrt.locking(lf.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            lf.close(); self._lf = None

    def resolve(self, snap):
        snaps = self.snapshots()
        if not snaps:
            sys.exit('no snapshots in ' + self.path)
        if snap in ('latest', 'last'):
            return snaps[-1]
        if snap not in snaps:
            sys.exit('no snapshot %r (see `chunkbak list`)' % snap)
        return snap


def sync():
    if hasattr(os, 'sync'):
        os.sync()


def is_repo(path):
    return os.path.isdir(os.path.join(path, 'snapshots')) and os.path.isdir(os.path.join(path, 'objects'))


def is_world(path):
    return os.path.isfile(os.path.join(path, 'level.dat'))


def default_repo(world):
    w = os.path.abspath(world).rstrip('/\\')
    return w + '-backups'


def repo_arg(path):
    """REPO argument: a backup folder, or a world folder (-> its default backup folder)."""
    if is_repo(path) or not is_world(path):
        return path
    r = default_repo(path)
    if not is_repo(r):
        sys.exit('%s is a world, and it has no backups yet at %s (run: chunkbak init %s)' % (path, r, path))
    return r


def read_config(repo):
    try:
        with open(os.path.join(repo, CONFIG)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


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
            if (n in ('session.lock', CONFIG) or n.endswith(('.tmp', '_new', '.cbtmp', '.before-restore'))
                    or n.rsplit('.', 1)[-1].startswith('tmp')):
                continue
            full = os.path.join(root, n)
            yield os.path.relpath(full, world).replace(os.sep, '/'), full


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
def rcon(addr, password, command, timeout=10):
    """Send one command over Minecraft RCON (vanilla, Paper, Fabric, Pumpkin, ...). -> reply text."""
    import socket
    host, _, port = addr.rpartition(':') if ':' in addr else (addr, '', '25575')
    host = host.strip('[]') or 'localhost'

    def pkt(rid, kind, body):
        b = struct.pack('<ii', rid, kind) + body.encode('utf-8') + b'\0\0'
        return struct.pack('<i', len(b)) + b

    def recv(sock):
        def exact(n):
            buf = b''
            while len(buf) < n:
                c = sock.recv(n - len(buf))
                if not c:
                    raise ConnectionError('RCON connection closed')
                buf += c
            return buf
        n = struct.unpack('<i', exact(4))[0]
        d = exact(n)
        return struct.unpack('<i', d[:4])[0], d[8:-2].decode('utf-8', 'replace')

    with socket.create_connection((host, int(port or 25575)), timeout=timeout) as sock:
        sock.sendall(pkt(1, 3, password))
        rid, _ = recv(sock)
        if rid == -1:
            raise PermissionError('RCON password rejected by %s' % addr)
        sock.sendall(pkt(2, 2, command))
        return recv(sock)[1]



# ---------------------------------------------------------------- server list ping / idle skip
def pack_varint(n):
    if n < 0:
        n &= 0xFFFFFFFF
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def unpack_varint(buf, i=0):
    n = shift = 0
    while True:
        if i >= len(buf):
            raise ValueError('short varint')
        b = buf[i]; i += 1
        n |= (b & 0x7F) << shift
        if not b & 0x80:
            return n, i
        shift += 7
        if shift >= 35:
            raise ValueError('varint too long')


def _recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise OSError('connection closed')
        buf += chunk
    return bytes(buf)


def _recv_varint(sock):
    buf = bytearray()
    while True:
        b = sock.recv(1)
        if not b:
            raise OSError('connection closed')
        buf += b
        if not b[0] & 0x80:
            n, _ = unpack_varint(buf)
            return n
        if len(buf) >= 5:
            raise ValueError('varint too long')


def parse_duration(s):
    """'24h', '90m', '1d', '2s' (and a bare integer as seconds) -> seconds."""
    s = str(s).strip().lower()
    if s.isdigit():
        return int(s)
    units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    if len(s) >= 2 and s[-1] in units and s[:-1].replace('.', '', 1).isdigit():
        return float(s[:-1]) * units[s[-1]]
    raise ValueError('invalid duration %r (use e.g. 24h, 90m, 1d)' % s)


def split_hostport(s):
    if s.startswith('['):
        host, rest = s[1:].split(']', 1)
        if not rest.startswith(':'):
            raise ValueError('need HOST:PORT')
        port = rest[1:]
    else:
        if ':' not in s:
            raise ValueError('need HOST:PORT')
        host, port = s.rsplit(':', 1)
    port = int(port)
    if not host or not (1 <= port <= 65535):
        raise ValueError('need HOST:PORT')
    return host, port


def status_online(hostport, timeout=3.0):
    """Minecraft Java server list ping -> players.online (int). Raises on failure."""
    host, port = split_hostport(hostport)
    addr = host.encode()
    handshake = (pack_varint(0) + pack_varint(-1) + pack_varint(len(addr)) + addr
                 + struct.pack('>H', port) + pack_varint(1))
    sock = socket.create_connection((host, port), timeout)
    try:
        sock.settimeout(timeout)
        sock.sendall(pack_varint(len(handshake)) + handshake + pack_varint(1) + pack_varint(0))
        length = _recv_varint(sock)
        if length < 1 or length > 2 * 1024 * 1024:
            raise ValueError('bad status length %d' % length)
        data = _recv_exact(sock, length)
    finally:
        sock.close()
    pid, i = unpack_varint(data, 0)
    if pid != 0:
        raise ValueError('unexpected packet id %d' % pid)
    slen, i = unpack_varint(data, i)
    info = json.loads(data[i:i + slen].decode('utf-8'))
    return int(info['players']['online'])


def player_state_path(repo):
    return os.path.join(repo, 'players.state')


def load_player_state(repo):
    try:
        with open(player_state_path(repo)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def save_player_state(repo, online):
    os.makedirs(repo, exist_ok=True)
    p = player_state_path(repo)
    tmp = p + '.tmp%d' % os.getpid()
    with open(tmp, 'w') as f:
        json.dump({'online': online, 'checked_at': time.time()}, f, separators=(',', ':'))
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, p)


def newest_snapshot_age(repo):
    """Seconds since the newest snapshot was created, or None if there isn't one."""
    snapdir = os.path.join(repo, 'snapshots')
    try:
        ids = sorted(n[:-8] for n in os.listdir(snapdir) if n.endswith('.json.gz'))
    except OSError:
        return None
    if not ids:
        return None
    path = os.path.join(snapdir, ids[-1] + '.json.gz')
    with gzip.open(path, 'rt') as f:
        created = json.load(f).get('created')
    if not created:
        created = os.path.getmtime(path)
    return time.time() - created


def format_age(secs):
    if secs >= 3600:
        return '%.1fh' % (secs / 3600.0)
    if secs >= 60:
        return '%.0fm' % (secs / 60.0)
    return '%.0fs' % secs


def skip_idle_backup(repo, hostport, idle_every):
    """True if this backup should be skipped. Never takes the repo lock.
    A missing or unreadable state file is treated as previous count > 0 (safe)."""
    try:
        idle_secs = parse_duration(idle_every)
    except ValueError as ex:
        sys.exit(str(ex))
    prev = load_player_state(repo)
    prev_online = 1 if (prev is None or not isinstance(prev.get('online'), (int, float))) else int(prev['online'])
    try:
        online = status_online(hostport)
    except Exception as ex:
        print('WARN status ping %s failed: %s; taking snapshot' % (hostport, ex), file=sys.stderr)
        save_player_state(repo, prev_online)
        return False
    save_player_state(repo, online)
    if online > 0 or prev_online > 0:
        return False
    age = newest_snapshot_age(repo)
    if age is None or age >= idle_secs:
        return False
    print('skip: 0 players, last snapshot %s ago' % format_age(age), flush=True)
    return True



def backup_targets(paths):
    """backup REPO WORLD | backup WORLD | backup REPO -> (repo, world)."""
    if len(paths) == 2:
        return paths[0], paths[1]
    if len(paths) != 1:
        sys.exit('usage: chunkbak backup [REPO] WORLD')
    p = paths[0]
    if is_repo(p):
        world = read_config(p).get('world')
        if not world:
            sys.exit('%s has no %s; use: chunkbak backup %s WORLD' % (p, CONFIG, p))
        return p, world
    if is_world(p):
        return default_repo(p), p
    sys.exit('%s is neither a world (no level.dat) nor a chunkbak backup folder' % p)


def cmd_backup(a):
    a.repo, a.world = backup_targets(a.paths)
    if not is_world(a.world) and not any(r.endswith('.mca') for r, _ in walk(a.world)):
        sys.exit('%s does not look like a Minecraft world (no level.dat, no .mca files)' % a.world)
    if not a.every:
        return backup_once(a)
    last_prune = 0
    print('backing up %s to %s every %g min (Ctrl+C to stop)' % (a.world, a.repo, a.every), flush=True)
    while True:
        t0 = time.time()
        try:
            backup_once(a)
            if time.time() - last_prune > 86400:
                prune(a.repo, DEFAULT_POLICY, False)
                last_prune = time.time()
        except SystemExit as ex:
            if ex.code not in (0, None):
                print('ERROR backup failed: %s' % ex.code, file=sys.stderr, flush=True)
        except Exception as ex:  # keep the loop alive; the next run will try again
            print('ERROR backup failed: %r' % ex, file=sys.stderr, flush=True)
        sys.stdout.flush()
        time.sleep(max(1, a.every * 60 - (time.time() - t0)))


def backup_once(a):
    if getattr(a, 'only_when_players', None) and skip_idle_backup(a.repo, a.only_when_players, a.idle_every):
        return None
    repo = Repo(a.repo); repo.lock()
    if not os.path.exists(os.path.join(a.repo, CONFIG)):
        with open(os.path.join(a.repo, CONFIG), 'w') as f:
            json.dump({'world': os.path.abspath(a.world)}, f, indent=1)
    snaps = repo.snapshots()
    prev = repo.load(snaps[-1]) if snaps else {'files': {}, 'regions': {}}
    t0 = time.time()
    if a.rcon:
        pw = a.rcon_password if a.rcon_password is not None else os.environ.get('CHUNKBAK_RCON_PASSWORD', '')
        try:
            reply = rcon(a.rcon, pw, a.rcon_command)
            print('rcon %s: %s' % (a.rcon_command, reply.strip() or 'ok'), file=sys.stderr)
        except Exception as ex:  # still back up: torn reads are caught anyway
            print('WARN rcon %s failed: %s' % (a.rcon, ex), file=sys.stderr)
    if a.pre_hook:
        r = subprocess.run(a.pre_hook, shell=True)
        if r.returncode:
            print('WARN pre-hook exit %d' % r.returncode, file=sys.stderr)
    quiet = a.quiet_secs if a.quiet_secs is not None else (3 if (a.rcon or a.pre_hook) else 0)
    waited = wait_quiet(a.world, quiet, a.max_wait) if quiet else 0
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
    sync()  # blobs durable before the manifest that references them
    repo.save(sid, man)
    sync()
    repo.unlock()
    print(sid, json.dumps(man['stats']), flush=True)
    if st['lost_chunks']:
        sys.exit(3)
    return sid


def cmd_init(a):
    world = os.path.abspath(a.world)
    if not is_world(world):
        sys.exit('%s has no level.dat. Point chunkbak at the world folder (the one with level.dat and region/).' % a.world)
    repo = os.path.abspath(a.repo or default_repo(world))
    if is_repo(repo) and repo_snaps(repo):
        print('%s already has backups; taking a new snapshot.' % repo)
    a.repo, a.world = repo, world
    sid = backup_once(a)
    if not sid:
        print('no snapshot taken (idle skip); run again or drop --only-when-players for the first snapshot')
        return
    if sys.argv[0].endswith('.py'):
        exe = '"%s" "%s"' % (sys.executable, os.path.abspath(sys.argv[0]))
    else:
        exe = shutil.which('chunkbak') or 'chunkbak'  # full path: cron/Task Scheduler have a short PATH
        exe = '"%s"' % exe if ' ' in exe else exe
    q = '"%s"' % repo
    log = '"%s"' % os.path.join(repo, 'chunkbak.log')
    if os.name == 'nt':
        tr = ('%s backup %s' % (exe, q)).replace('"', '\\"')
        sched = ('Windows Task Scheduler (paste into a Command Prompt once):\n'
                 '    schtasks /Create /SC MINUTE /MO 15 /TN chunkbak /TR "%s"' % tr)
    else:
        sched = ('cron (run `crontab -e` and add these two lines):\n'
                 '    */15 * * * * %s backup %s >> %s 2>&1\n'
                 '    40 4 * * *   %s prune %s >> %s 2>&1'
                 % (exe, q, log, exe, q, log))
    print(f"""
First snapshot {sid} saved in {repo}
Tip: keep this folder on another disk too, or copy it somewhere else now and then.

What next:
  Take another snapshot now:
    {exe} backup {q}
  Keep running and take one every 15 minutes (old ones are pruned once a day):
    {exe} backup {q} --every 15
  See your snapshots:
    {exe} list {q}
  Restore the whole world into a new folder (stop the server first):
    {exe} restore {q} latest NEW_FOLDER

Or schedule it with {sched}
""")


def repo_snaps(repo):
    try:
        return [n for n in os.listdir(os.path.join(repo, 'snapshots')) if n.endswith('.json.gz')]
    except OSError:
        return []


def cmd_list(a):
    repo = Repo(repo_arg(a.repo))
    for sid in repo.snapshots():
        m = repo.load(sid)
        print(sid, m.get('note') or '', json.dumps(m.get('stats', {})))


def cmd_restore(a):
    repo = Repo(repo_arg(a.repo)); a.snap = repo.resolve(a.snap); man = repo.load(a.snap)
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
    pids = os.listdir('/proc') if os.path.isdir('/proc/self/fd') else []
    if not pids:
        unreadable = -1  # no /proc (Windows/macOS): open-file check not available
    for pid in pids:
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
                if fcntl:
                    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB); fcntl.lockf(f, fcntl.LOCK_UN)
                else:
                    msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1); msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
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
    d = posixpath.dirname(region)
    rx, rz = [int(v) for v in os.path.basename(region).split('.')[1:3]]
    for i in idxs:
        c = e['chunks'][i] if e else None
        new = (c[0], repo.get(c[1])) if c else (0, None)
        if new[1] and new[1][0] & EXTERNAL:  # oversized chunk: bring its .mcc back too
            mcc = posixpath.join(d, 'c.%d.%d.mcc' % (rx * 32 + i % 32, rz * 32 + i // 32))
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
    repo = Repo(repo_arg(a.repo)); a.snap = repo.resolve(a.snap); man = repo.load(a.snap)
    busy, unreadable = server_signs(a.world)
    if busy and not a.force:
        sys.exit('server looks live (%s): stop it first (or --force)' % '; '.join(busy))
    if unreadable < 0 and not busy:
        print('note: cannot check for running processes on this OS; make sure the server is STOPPED', file=sys.stderr)
    elif unreadable and not busy:
        print('note: %d processes could not be inspected (other users); run as root for a full check' % unreadable, file=sys.stderr)
    chunks = [tuple(int(v) for v in xz.split(',')) for xz in a.chunks.replace(' ', '').split(';') if xz]
    if a.dim:
        a.dim = a.dim.replace('\\', '/').strip('/')
    if a.region:
        a.region = a.region.replace('\\', '/')
        base = posixpath.dirname(posixpath.dirname(a.region))
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
        if posixpath.join(base, 'region', name) not in man['regions']:
            sys.exit('region not in snapshot: ' + posixpath.join(base, 'region', name))
        for layer in ('region', 'entities', 'poi'):
            rel = posixpath.join(base, layer, name)
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
    repo = Repo(repo_arg(a.repo)); bad = 0; n = 0; deep = 0
    for sid in ([repo.resolve(a.snap)] if a.snap else repo.snapshots()):
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
    gc(repo_arg(a.repo))


def gc(path):
    repo = Repo(path); repo.lock()
    live = set()
    for sid in repo.snapshots():
        live.update(referenced(repo.load(sid)))
    removed = freed = 0
    objdir = os.path.join(repo.path, 'objects')
    for sub in os.listdir(objdir):
        for n in os.listdir(os.path.join(objdir, sub)):
            if n not in live:
                p = os.path.join(objdir, sub, n); freed += os.path.getsize(p); os.remove(p); removed += 1
    repo.unlock()
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
    policy = dict(keep_last=a.keep if a.keep is not None else a.keep_last,
                  hourly=a.hourly, daily=a.daily, weekly=a.weekly, monthly=a.monthly)
    if all(v is None for v in policy.values()):
        policy = dict(DEFAULT_POLICY)
        print('policy (default):', ' '.join('%s=%d' % kv for kv in policy.items()))
    policy = {k: v or 0 for k, v in policy.items()}
    if not any(policy.values()):
        sys.exit('refusing to prune with an empty policy')
    prune(repo_arg(a.repo), policy, a.dry_run)


def prune(path, policy, dry_run):
    repo = Repo(path)
    snaps = repo.snapshots()
    times = {s: repo.load(s)['created'] for s in snaps}
    keep = retention_keep(times, **policy)
    if snaps:
        keep.add(max(snaps, key=lambda s: times[s]))  # never drop the newest
    for sid in snaps:
        if sid not in keep:
            print(('would drop' if dry_run else 'dropped'), sid)
            if not dry_run:
                os.remove(os.path.join(repo.path, 'snapshots', sid + '.json.gz'))
    print('kept', len(keep), 'of', len(snaps))
    if not dry_run:
        gc(path)


def main():
    p = argparse.ArgumentParser(prog='chunkbak', description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('-V', '--version', action='version', version='chunkbak ' + __version__)
    sp = p.add_subparsers(dest='cmd', required=True)
    backup_opts = argparse.ArgumentParser(add_help=False)
    backup_opts.add_argument('--note', default='')
    backup_opts.add_argument('--pre-hook', help='shell command run first, e.g. one that sends save-all to the server')
    backup_opts.add_argument('--rcon', metavar='HOST:PORT', help='ask the server to save first over RCON (e.g. localhost:25575)')
    backup_opts.add_argument('--rcon-password', help='RCON password (or set CHUNKBAK_RCON_PASSWORD)')
    backup_opts.add_argument('--rcon-command', default='save-all', help='command sent over RCON (default: save-all)')
    backup_opts.add_argument('--quiet-secs', type=float, help='wait until no world file changed for this long (default 3 with --rcon/--pre-hook, else 0)')
    backup_opts.add_argument('--max-wait', type=float, default=60); backup_opts.add_argument('--retries', type=int, default=5)
    backup_opts.add_argument('--retry-delay', type=float, default=0.2)
    backup_opts.add_argument('--only-when-players', metavar='HOST:PORT',
                            help='skip the snapshot when the Java server has 0 players (one final snapshot after the last logout; still snapshot if the last one is older than --idle-every)')
    backup_opts.add_argument('--idle-every', default='24h', metavar='DURATION',
                            help='with --only-when-players, still snapshot this often when idle (e.g. 24h, 90m, 1d; default 24h)')
    i = sp.add_parser('init', parents=[backup_opts], help='set up backups for a world and take the first snapshot')
    i.add_argument('world'); i.add_argument('repo', nargs='?', help='backup folder (default: <world>-backups next to the world)')
    b = sp.add_parser('backup', parents=[backup_opts], help='take a snapshot')
    b.add_argument('paths', nargs='+', metavar='[REPO] WORLD')
    b.add_argument('--every', type=float, default=0, metavar='MIN', help='keep running: a snapshot every MIN minutes, prune daily')
    l = sp.add_parser('list'); l.add_argument('repo')
    r = sp.add_parser('restore'); r.add_argument('repo'); r.add_argument('snap'); r.add_argument('dest')
    rc = sp.add_parser('restore-chunks'); rc.add_argument('repo'); rc.add_argument('snap'); rc.add_argument('world')
    rc.add_argument('--region'); rc.add_argument('--dim'); rc.add_argument('--chunks', required=True)
    rc.add_argument('--force', action='store_true')
    v = sp.add_parser('verify'); v.add_argument('repo'); v.add_argument('snap', nargs='?'); v.add_argument('--deep', action='store_true')
    g = sp.add_parser('gc'); g.add_argument('repo')
    pr = sp.add_parser('prune'); pr.add_argument('repo'); pr.add_argument('--keep', type=int)
    pr.add_argument('--keep-last', type=int)
    for k in ('hourly', 'daily', 'weekly', 'monthly'):
        pr.add_argument('--' + k, type=int)
    pr.add_argument('--dry-run', action='store_true')
    a = p.parse_args()
    try:
        {'init': cmd_init, 'backup': cmd_backup, 'list': cmd_list, 'restore': cmd_restore, 'restore-chunks': cmd_restore_chunks,
         'verify': cmd_verify, 'gc': cmd_gc, 'prune': cmd_prune}[a.cmd](a)
    except KeyboardInterrupt:
        print('stopped', file=sys.stderr)
        sys.exit(130)


if __name__ == '__main__':
    main()
