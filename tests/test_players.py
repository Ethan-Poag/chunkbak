"""--only-when-players uses a Java server-list ping: snapshot when anyone is online,
one final snapshot after the last logout, then skip until --idle-every; a failed ping
still takes a snapshot. A missing flag leaves behaviour unchanged."""
import gzip, json, os, socket, struct, subprocess, sys, tempfile, threading, time, shutil
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..'))
import chunkbak as cb
import fixture

CB = os.path.join(HERE, '..', 'chunkbak.py')
fails = 0


def check(ok, label):
    global fails
    print('PASS' if ok else 'FAIL', label)
    fails += not ok


def snaps(repo):
    d = os.path.join(repo, 'snapshots')
    if not os.path.isdir(d):
        return []
    return sorted(n[:-8] for n in os.listdir(d) if n.endswith('.json.gz'))


class StatusServer:
    def __init__(self, online=0, mode='ok'):
        self.online = online
        self.mode = mode  # ok | close | garbage
        self.srv = socket.socket(); self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(('127.0.0.1', 0)); self.srv.listen()
        self.port = self.srv.getsockname()[1]
        self.hostport = '127.0.0.1:%d' % self.port
        self._stop = False
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while not self._stop:
            try:
                self.srv.settimeout(0.2)
                c, _ = self.srv.accept()
            except OSError:
                continue
            with c:
                if self.mode == 'close':
                    continue
                try:
                    if self.mode == 'garbage':
                        c.sendall(b'\x03wtf')
                        continue
                    # consume whatever the client sent, then reply
                    c.settimeout(1)
                    try:
                        c.recv(4096)
                    except OSError:
                        pass
                    js = json.dumps({
                        'version': {'name': 'test', 'protocol': 0},
                        'players': {'max': 20, 'online': int(self.online)},
                        'description': {'text': 't'},
                    }).encode()
                    payload = cb.pack_varint(0) + cb.pack_varint(len(js)) + js
                    c.sendall(cb.pack_varint(len(payload)) + payload)
                except OSError:
                    pass

    def close(self):
        self._stop = True
        try:
            self.srv.close()
        except OSError:
            pass


def run(repo, world, extra, hook=None):
    cmd = [sys.executable, CB, 'backup', repo, world, '--quiet-secs', '0'] + extra
    if hook:
        cmd += ['--pre-hook', hook]
    return subprocess.run(cmd, capture_output=True, text=True)


def make(T):
    w, r = T + '/w', T + '/repo'
    fixture.make_world(w)
    return w, r


# --- unit: duration and varint ---
check(cb.parse_duration('24h') == 86400 and cb.parse_duration('90m') == 5400
      and cb.parse_duration('1d') == 86400 and cb.parse_duration('2s') == 2
      and cb.parse_duration('15') == 15, 'parse_duration accepts 24h/90m/1d/2s and bare seconds')
try:
    cb.parse_duration('nope'); bad = False
except ValueError:
    bad = True
check(bad, 'parse_duration rejects garbage')
n = -1
check(cb.unpack_varint(cb.pack_varint(n))[0] == (n & 0xFFFFFFFF), 'pack/unpack_varint of -1 (protocol version)')
check(cb.unpack_varint(cb.pack_varint(0))[0] == 0 and cb.unpack_varint(cb.pack_varint(300))[0] == 300,
      'pack/unpack_varint of 0 and 300')

# --- ping against the fake server ---
srv = StatusServer(online=3)
check(cb.status_online(srv.hostport) == 3, 'status_online reads players.online=3')
srv.online = 0
check(cb.status_online(srv.hostport) == 0, 'status_online reads players.online=0')

# --- CLI scenarios ---
T = tempfile.mkdtemp(prefix='cbplayers')
w, r = make(T)
hookf = T + '/hook.ran'

# no flag: unchanged, even with nobody listening on a random closed port
dead = socket.socket(); dead.bind(('127.0.0.1', 0)); dead_port = dead.getsockname()[1]; dead.close()
res = run(r, w, [])
check(res.returncode == 0 and len(snaps(r)) == 1, 'no flag: backup still taken (1 snapshot)')
# and it does not write a players.state or talk to a port
check(not os.path.exists(os.path.join(r, 'players.state')), 'no flag: no players.state written')

# players on -> snapshot
srv.online = 3
if os.path.exists(hookf):
    os.remove(hookf)
res = run(r, w, ['--only-when-players', srv.hostport], hook='touch %s' % hookf)
st = json.loads(open(os.path.join(r, 'players.state')).read())
check(res.returncode == 0 and len(snaps(r)) == 2 and st['online'] == 3 and os.path.exists(hookf),
      'players on: snapshot taken, state.online=3, pre-hook ran')

# players off after on -> one final snapshot
srv.online = 0
if os.path.exists(hookf):
    os.remove(hookf)
res = run(r, w, ['--only-when-players', srv.hostport], hook='touch %s' % hookf)
st = json.loads(open(os.path.join(r, 'players.state')).read())
check(res.returncode == 0 and len(snaps(r)) == 3 and st['online'] == 0 and os.path.exists(hookf),
      'last player left: one final snapshot, pre-hook ran')

# still off -> skip, no hook, no new snapshot
if os.path.exists(hookf):
    os.remove(hookf)
n_before = len(snaps(r))
res = run(r, w, ['--only-when-players', srv.hostport], hook='touch %s' % hookf)
check(res.returncode == 0 and len(snaps(r)) == n_before and not os.path.exists(hookf)
      and 'skip: 0 players, last snapshot' in res.stdout,
      'still idle: skip (exit 0), no pre-hook, log line printed (%r)' % (res.stdout + res.stderr).strip())

# skip does not hold the repo lock: hold it, a skip must still finish quickly
lockf = open(os.path.join(r, 'lock'), 'w')
import fcntl
fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
t0 = time.time()
res = run(r, w, ['--only-when-players', srv.hostport])
held_ok = res.returncode == 0 and 'skip:' in res.stdout and (time.time() - t0) < 2
fcntl.flock(lockf, fcntl.LOCK_UN); lockf.close()
check(held_ok, 'skip does not take (or wait on) the repo lock')

# idle-every elapsed -> snapshot
res = run(r, w, ['--only-when-players', srv.hostport, '--idle-every', '1s'])
# newest snapshot is just from the "final" one, likely < 1s; if it already aged, either is ok.
# force age by waiting
time.sleep(1.2)
n_before = len(snaps(r))
res = run(r, w, ['--only-when-players', srv.hostport, '--idle-every', '1s'])
check(res.returncode == 0 and len(snaps(r)) == n_before + 1,
      'idle longer than --idle-every: snapshot taken')

# still 0 and fresh idle snapshot -> skip again
n_before = len(snaps(r))
res = run(r, w, ['--only-when-players', srv.hostport, '--idle-every', '1h'])
check(res.returncode == 0 and len(snaps(r)) == n_before and 'skip:' in res.stdout,
      'idle again with a fresh snapshot: skip')

# server down -> snapshot + WARN
n_before = len(snaps(r))
res = run(r, w, ['--only-when-players', '127.0.0.1:%d' % dead_port])
check(res.returncode == 0 and len(snaps(r)) == n_before + 1 and 'WARN status ping' in res.stderr,
      'server down: snapshot anyway, WARN logged')

# garbage reply -> snapshot + WARN
junk = StatusServer(mode='garbage')
n_before = len(snaps(r))
res = run(r, w, ['--only-when-players', junk.hostport])
check(res.returncode == 0 and len(snaps(r)) == n_before + 1 and 'WARN status ping' in res.stderr,
      'garbage status reply: snapshot anyway, WARN logged')
junk.close()

# immediate close -> snapshot + WARN
closer = StatusServer(mode='close')
n_before = len(snaps(r))
res = run(r, w, ['--only-when-players', closer.hostport])
check(res.returncode == 0 and len(snaps(r)) == n_before + 1 and 'WARN status ping' in res.stderr,
      'status connection closed: snapshot anyway, WARN logged')
closer.close()

# missing state + 0 online = treated as previous > 0 -> snapshot
T2 = tempfile.mkdtemp(prefix='cbplayers2')
w2, r2 = make(T2)
res = run(r2, w2, ['--only-when-players', srv.hostport])  # srv.online is 0
check(res.returncode == 0 and len(snaps(r2)) == 1,
      'missing state file + 0 players: snapshot (safe default)')
shutil.rmtree(T2, ignore_errors=True)

srv.close()
shutil.rmtree(T, ignore_errors=True)
sys.exit(1 if fails else 0)
