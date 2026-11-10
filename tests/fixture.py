"""Tiny synthetic Anvil world for tests: a few region/entities/poi files in the overworld and
the Nether, a gzipped level.dat and a playerdata file. No real world data is needed."""
import gzip, os, struct, sys, zlib
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import chunkbak as cb


def nbt_chunk(x, z, ver, pad=600):
    """A minimal NBT compound: {xPos:int, zPos:int, ver:int, p:string}."""
    body = (b'\x03\x00\x04xPos' + struct.pack('>i', x) + b'\x03\x00\x04zPos' + struct.pack('>i', z)
            + b'\x03\x00\x03ver' + struct.pack('>i', ver)
            + b'\x08\x00\x01p' + struct.pack('>H', pad) + bytes([65 + (x * 7 + z * 13 + ver) % 26]) * pad)
    return b'\x0a\x00\x00' + body + b'\x00'


def payload(x, z, ver):
    return b'\x02' + zlib.compress(nbt_chunk(x, z, ver))


def write_region(path, rx, rz, ver, slots=range(0, 1024, 7), ts=1000):
    """Fill the listed slots of region rx,rz with version `ver` chunks."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = [(0, None)] * 1024
    for i in slots:
        x, z = rx * 32 + i % 32, rz * 32 + i // 32
        data[i] = (ts + ver, payload(x, z, ver))
    with open(path, 'wb') as f:
        f.write(cb.write_region(data))


def make_world(w, ver=0):
    for rx, rz in ((0, 0), (-1, 0), (0, -1)):
        for layer in ('region', 'entities', 'poi'):
            write_region(os.path.join(w, layer, 'r.%d.%d.mca' % (rx, rz)), rx, rz, ver)
    write_region(os.path.join(w, 'DIM-1', 'region', 'r.0.0.mca'), 0, 0, ver)
    os.makedirs(os.path.join(w, 'playerdata'), exist_ok=True)
    with open(os.path.join(w, 'level.dat'), 'wb') as f:
        f.write(gzip.compress(nbt_chunk(0, 0, ver, 50)))
    with open(os.path.join(w, 'playerdata', '00000000-0000-0000-0000-000000000001.dat'), 'wb') as f:
        f.write(gzip.compress(nbt_chunk(1, 1, ver, 50)))
    open(os.path.join(w, 'session.lock'), 'wb').close()
