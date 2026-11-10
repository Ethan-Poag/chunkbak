"""restore-chunks must refuse while another process has world files open (matched by inode,
so a server in a container with different paths is caught too)."""
import subprocess, sys, time, os, shutil, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..')); import chunkbak as kb
D = tempfile.mkdtemp(prefix='cbguard'); W = D + '/W'; os.makedirs(W + '/region')
open(W + '/region/r.0.0.mca', 'wb').write(kb.write_region([(0, None)] * 1024))
os.symlink(W, D + '/alias')  # the holder opens it under another path
p = subprocess.Popen([sys.executable, '-c', 'import time,sys; f=open(sys.argv[1],"rb"); time.sleep(30)', D + '/alias/region/r.0.0.mca'])
time.sleep(0.5); busy = kb.server_signs(W)[0]; p.kill(); p.wait(); idle = kb.server_signs(W)[0]
print('PASS' if busy and not idle else 'FAIL', 'guard: open by other path ->', busy, '| closed ->', idle)
shutil.rmtree(D, ignore_errors=True)
sys.exit(0 if busy and not idle else 1)
