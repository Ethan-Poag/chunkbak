"""Hostile-writer test: a thread rewrites a region file *in place and out of order*
(header before data, data in two halves) while chunkbak backs it up 40 times.
Every stored chunk must decompress and equal a version the writer really wrote; torn
slots must be carried from the previous snapshot. Plus retention-policy unit checks."""
import os, sys, struct, zlib, threading, time, random, subprocess, shutil, json, gzip, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import chunkbak as kb
T = tempfile.mkdtemp(prefix='cbtorn'); os.makedirs(T + '/W/region')
REG = T + '/W/region/r.0.0.mca'
def payload(slot, ver):
    nbt = b'\x0a\x00\x00' + b'\x03\x00\x04xPos' + struct.pack('>i', slot) + b'\x03\x00\x03ver' + struct.pack('>i', ver) \
          + os.urandom(1) * 0 + bytes(random.Random(slot * 1000 + ver).randbytes(3000 + slot * 37)) * 0 + (b'\x08\x00\x01p' + struct.pack('>H', 2000) + bytes([65 + ver % 26]) * 2000) + b'\x00'
    return b'\x02' + zlib.compress(nbt)
SL = 64
written = {i: {payload(i, 0)} for i in range(SL)}
open(REG, 'wb').write(kb.write_region([(1, payload(i, 0)) if i < SL else (0, None) for i in range(1024)]))
stop = False; lock = threading.Lock()
def writer():
    ver = 0; rnd = random.Random(7)
    f = open(REG, 'r+b')
    while not stop:
        ver += 1
        i = rnd.randrange(SL); p = payload(i, ver)
        with lock: written[i].add(p)
        rec = struct.pack('>I', len(p)) + p
        rec += b'\0' * (-len(rec) % 4096)
        f.seek(i * 4); loc = f.read(4); off = int.from_bytes(loc[:3], 'big')
        mode = rnd.random()
        if mode < 0.5:   # in place, header untouched, data in two halves: torn window
            f.seek(off * 4096); f.write(rec[:len(rec) // 2]); f.flush(); time.sleep(0.002)
            f.write(rec[len(rec) // 2:]); f.flush()
        else:            # append, header first then data: dangling window
            f.seek(0, 2); end = f.tell() // 4096
            f.seek(i * 4); f.write(end.to_bytes(3, 'big') + bytes([len(rec) // 4096])); f.flush(); time.sleep(0.002)
            f.seek(end * 4096); f.write(rec); f.flush()
        time.sleep(rnd.random() * 0.003)
th = threading.Thread(target=writer); th.start()
B = os.path.join(os.path.dirname(__file__), '..', 'chunkbak.py')
codes = []
for n in range(40):
    r = subprocess.run([sys.executable, B, 'backup', T + '/repo', T + '/W', '--retries', '2', '--retry-delay', '0.001'],
                       capture_output=True, text=True); codes.append(r.returncode)
stop = True; th.join()
r = subprocess.run([sys.executable, B, 'backup', T + '/repo', T + '/W'], capture_output=True, text=True)
repo = kb.Repo(T + '/repo'); ok = True; carried = retried = 0
for sid in repo.snapshots():
    m = repo.load(sid); carried += m['stats']['carried_chunks']; retried += m['stats']['retried_reads']
    for i, c in enumerate(m['regions']['region/r.0.0.mca']['chunks'][:SL]):
        if not c:
            print('FAIL', sid, 'slot', i, 'missing'); ok = False; continue
        p = repo.get(c[1])
        if not kb.payload_ok(p) or p not in written[i]:
            print('FAIL', sid, 'slot', i, 'stored a payload the writer never wrote'); ok = False
last = repo.load(repo.snapshots()[-1])['regions']['region/r.0.0.mca']
final = kb.read_region(open(REG, 'rb').read())
ok2 = all(repo.get(last['chunks'][i][1]) == final[i][1] for i in range(SL))
print('PASS' if ok else 'FAIL', 'torn-writer: 41 snapshots, every stored chunk intact and a real version;',
      'retried reads', retried, 'carried slots', carried, 'exit codes', sorted(set(codes)))
print('PASS' if ok2 else 'FAIL', 'quiet snapshot after writer stopped == file on disk (carried slots re-read)')
# retention
H = 3600; now = time.mktime((2026, 11, 9, 12, 0, 0, 0, 0, -1))
times = {'s%03d' % k: now - k * H for k in range(24 * 40)}   # hourly for 40 days
keep = kb.retention_keep(times, keep_last=3, hourly=24, daily=14, weekly=8, monthly=6)
days = {time.strftime('%Y%m%d', time.localtime(times[s])) for s in keep}
r_ok = ('s000' in keep and 's001' in keep and 's002' in keep and len([s for s in keep if now - times[s] < 24 * H]) == 24
        and len(days) >= 14 + 1 and len(keep) <= 3 + 24 + 14 + 8 + 6)
print('PASS' if r_ok else 'FAIL', 'retention hourly24/daily14/weekly8/monthly6 over 960 hourly snapshots keeps', len(keep), 'distinct days', len(days))
# a file that vanishes mid-backup (level.dat_new / temp renames during a save) must not crash the run
d, st_, tries, okv = kb.read_stable(T + '/W/gone.dat', lambda b: True, 2, 0.001)
os.makedirs(T + '/W2', exist_ok=True); open(T + '/W2/level.dat_new', 'wb').write(b'x'); open(T + '/W2/a.dat.tmp123', 'wb').write(b'x')
skipped = [rel for rel, _ in kb.walk(T + '/W2')] == []
v_ok = d is None and not okv and skipped
print('PASS' if v_ok else 'FAIL', 'vanished file -> skipped, no crash; temp names (_new, .tmpNNN) not backed up')
shutil.rmtree(T, ignore_errors=True)
sys.exit(0 if ok and ok2 and r_ok and v_ok else 1)
