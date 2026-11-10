"""End-to-end on a synthetic world: backup, no-change backup (0 new blobs), modify, backup,
full restore of each snapshot equals the source, restore-chunks touches only the chosen
chunks (region + entities + poi), prune + gc, verify --deep."""
import os, shutil, subprocess, sys, tempfile, time, filecmp
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..'))
import chunkbak as cb
import fixture

CB = os.path.join(HERE, '..', 'chunkbak.py')
T = tempfile.mkdtemp(prefix='cbround')
W, R = T + '/W', T + '/repo'
fails = 0


def run(*args, check=True):
    r = subprocess.run([sys.executable, CB] + list(args), capture_output=True, text=True)
    if check and r.returncode:
        sys.exit('command failed: %s\n%s%s' % (args, r.stdout, r.stderr))
    return r


def check(ok, label):
    global fails
    print('PASS' if ok else 'FAIL', label)
    fails += not ok


def same(a, b):
    fa, fb = dict(cb.walk(a)), dict(cb.walk(b))
    if set(fa) != set(fb):
        return False
    for rel in fa:
        if rel.endswith('.mca'):
            if cb.read_region(open(fa[rel], 'rb').read()) != cb.read_region(open(fb[rel], 'rb').read()):
                return False
        elif not filecmp.cmp(fa[rel], fb[rel], shallow=False):
            return False
    return True


def stats(r):
    import json
    return json.loads(r.stdout.split(' ', 1)[1])


fixture.make_world(W, ver=0)
shutil.copytree(W, T + '/expect1')
s1 = stats(run('backup', R, W, '--note', 'first'))
check(s1['regions_read'] == 10 and s1['chunks'] == 10 * 147, 'first snapshot reads all 10 regions, %d chunks' % s1['chunks'])
s2 = stats(run('backup', R, W, '--note', 'no-change'))
check(s2['new_blobs'] == 0 and s2['regions_reused'] == 10, 'unchanged world: 0 new blobs, all regions reused by size+mtime')

time.sleep(0.05)
# "play": rewrite a few chunks in one region (terrain, entities and poi) and level.dat
for layer in ('region', 'entities', 'poi'):
    fixture.write_region(os.path.join(W, layer, 'r.0.0.mca'), 0, 0, ver=0)  # same content first
    p = os.path.join(W, layer, 'r.0.0.mca')
    slots = cb.read_region(open(p, 'rb').read())
    for i in (0, 7, 14):
        slots[i] = (2000, fixture.payload(i % 32, i // 32, 1))
    open(p, 'wb').write(cb.write_region(slots))
import gzip
open(os.path.join(W, 'level.dat'), 'wb').write(gzip.compress(fixture.nbt_chunk(0, 0, 1, 50)))
shutil.copytree(W, T + '/expect2')
s3 = stats(run('backup', R, W, '--note', 'after-play'))
check(s3['regions_read'] == 3 and s3['chunks_new'] == 3,
      'after edits: only 3 regions re-read; 9 edited chunks stored as %d new blobs (identical payloads dedupe)' % s3['chunks_new'])

snaps = [l.split()[0] for l in run('list', R).stdout.splitlines()]
check(len(snaps) == 3, 'list shows 3 snapshots')
run('restore', R, snaps[0], T + '/r1'); run('restore', R, snaps[2], T + '/r3')
check(same(T + '/expect1', T + '/r1'), 'full restore of snapshot 1 == original world (chunk by chunk)')
check(same(T + '/expect2', T + '/r3'), 'full restore of snapshot 3 == edited world (chunk by chunk)')
check(run('restore', R, snaps[0], T + '/r1', check=False).returncode != 0, 'restore refuses a non-empty DEST')

# chunk-level restore of chunks 0,0 and 7,0 back to snapshot 1; chunk 14,0 stays edited
run('restore-chunks', R, snaps[0], W, '--chunks', '0,0;7,0')
ok = True
for layer in ('region', 'entities', 'poi'):
    rel = os.path.join(layer, 'r.0.0.mca')
    w = cb.read_region(open(os.path.join(W, rel), 'rb').read())
    e1 = cb.read_region(open(os.path.join(T, 'expect1', rel), 'rb').read())
    e2 = cb.read_region(open(os.path.join(T, 'expect2', rel), 'rb').read())
    ok &= w[0] == e1[0] and w[7] == e1[7] and all(w[i] == e2[i] for i in range(1024) if i not in (0, 7))
    ok &= os.path.exists(os.path.join(W, rel + '.before-restore'))
check(ok, 'restore-chunks: 2 chunks back in region+entities+poi, other 1022 slots untouched, .before-restore kept')
other = cb.read_region(open(os.path.join(W, 'region', 'r.-1.0.mca'), 'rb').read())
check(other == cb.read_region(open(os.path.join(T, 'expect2', 'region', 'r.-1.0.mca'), 'rb').read()), 'other regions untouched')
check(run('restore-chunks', R, snaps[0], W, '--region', 'region/r.0.0.mca', '--chunks', '40,0', check=False).returncode != 0,
      'restore-chunks rejects a chunk outside --region')

v = run('verify', R, '--deep')
check('0 bad' in v.stdout, 'verify --deep: ' + v.stdout.strip())
victim = next(p for p in (os.path.join(dp, f) for dp, _, fs in os.walk(R + '/objects') for f in fs))
data = open(victim, 'rb').read(); open(victim, 'wb').write(data[:-1] + bytes([data[-1] ^ 1]))
check(run('verify', R, check=False).returncode == 1, 'verify catches a corrupted blob')
open(victim, 'wb').write(data)

check(run('prune', R, check=False).returncode != 0, 'prune refuses an empty policy')
d = run('prune', R, '--keep-last', '1', '--dry-run').stdout
check(d.count('would drop') == 2 and len(os.listdir(R + '/snapshots')) == 3, 'prune --dry-run drops nothing')
p = run('prune', R, '--keep-last', '1').stdout
check('kept 1 of 3' in p and 'gc removed' in p, 'prune --keep-last 1 + gc: ' + p.strip().splitlines()[-1])
run('restore', R, snaps[2], T + '/r3b')
check(same(T + '/expect2', T + '/r3b') and '0 bad' in run('verify', R, '--deep').stdout, 'newest snapshot still restores after prune')

shutil.rmtree(T, ignore_errors=True)
sys.exit(1 if fails else 0)
