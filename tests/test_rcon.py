"""--rcon sends save-all to a (fake) RCON server with the password before the snapshot,
and a wrong password or a dead server only warns: the backup still happens."""
import os, socket, struct, subprocess, sys, tempfile, threading, shutil
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import fixture
CB = os.path.join(HERE, '..', 'chunkbak.py')
got = []


def serve(srv):
    while True:
        try:
            c, _ = srv.accept()
        except OSError:
            return
        with c:
            def rd():
                n = struct.unpack('<i', c.recv(4))[0]; d = b''
                while len(d) < n:
                    d += c.recv(n - len(d))
                return struct.unpack('<ii', d[:8]) + (d[8:-2].decode(),)
            def wr(rid, kind, body):
                b = struct.pack('<ii', rid, kind) + body.encode() + b'\0\0'; c.sendall(struct.pack('<i', len(b)) + b)
            rid, kind, pw = rd()
            if pw != 'hunter2':
                wr(-1, 2, ''); continue
            wr(rid, 2, '')
            rid, kind, cmd = rd(); got.append(cmd); wr(rid, 0, 'Saved the game')


srv = socket.socket(); srv.bind(('127.0.0.1', 0)); srv.listen(); port = srv.getsockname()[1]
threading.Thread(target=serve, args=(srv,), daemon=True).start()
T = tempfile.mkdtemp(prefix='cbrcon'); fixture.make_world(T + '/w')
env = dict(os.environ, CHUNKBAK_RCON_PASSWORD='hunter2')
r = subprocess.run([sys.executable, CB, 'backup', T + '/w', '--rcon', '127.0.0.1:%d' % port, '--quiet-secs', '0'],
                   capture_output=True, text=True, env=env)
ok1 = r.returncode == 0 and got == ['save-all'] and 'Saved the game' in r.stderr
print('PASS' if ok1 else 'FAIL', '--rcon: sent', got, 'before the snapshot')
r = subprocess.run([sys.executable, CB, 'backup', T + '/w', '--rcon', '127.0.0.1:%d' % port, '--rcon-password', 'nope', '--quiet-secs', '0'],
                   capture_output=True, text=True)
ok2 = r.returncode == 0 and 'password rejected' in r.stderr and len(got) == 1
print('PASS' if ok2 else 'FAIL', 'wrong RCON password -> warning, backup still taken')
dead = socket.socket(); dead.bind(('127.0.0.1', 0)); dead_port = dead.getsockname()[1]; dead.close()  # nothing listens
r = subprocess.run([sys.executable, CB, 'backup', T + '/w', '--rcon', '127.0.0.1:%d' % dead_port, '--quiet-secs', '0'],
                   capture_output=True, text=True, env=env)
ok3 = r.returncode == 0 and 'WARN rcon' in r.stderr
print('PASS' if ok3 else 'FAIL', 'RCON server down -> warning, backup still taken')
shutil.rmtree(T, ignore_errors=True)
sys.exit(0 if ok1 and ok2 and ok3 else 1)
