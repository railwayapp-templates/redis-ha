#!/usr/bin/env python3
"""Run the real wrapper and three Redis/Sentinel members on isolated local ports.
Requires REDIS_BIN_DIR and a cargo-built target/debug/redis-wrapper. No Docker.
"""
import base64, json, os, pathlib, signal, socket, subprocess, tempfile, time, urllib.request
ROOT = pathlib.Path(__file__).resolve().parents[1]
OLD, NEW = 'rotation-old-local', 'rotation-new-local'
BASE = int(os.environ.get('ROTATION_TEST_PORT', '18370'))
processes = []
environments = []
WRAPPER = os.environ.get("REDIS_WRAPPER", str(ROOT/"target/debug/redis-wrapper"))

def command(port, password, *args):
    def read(f):
        line = f.readline(); typ, body = line[:1], line[1:-2]
        if typ == b'-': raise RuntimeError(body.decode())
        if typ == b'$':
            n = int(body)
            if n < 0: return None
            val = f.read(n); f.read(2); return val.decode()
        if typ == b'*': return [read(f) for _ in range(int(body))]
        if typ == b':': return int(body)
        return body.decode()
    def send(s, f, args):
        parts = [str(x).encode() for x in args]
        s.sendall(b'*%d\r\n'%len(parts) + b''.join(b'$%d\r\n'%len(p)+p+b'\r\n' for p in parts))
        return read(f)
    with socket.create_connection(('127.0.0.1', port), timeout=2) as s:
        f = s.makefile('rb'); send(s, f, ('AUTH', password)); return send(s, f, args)

def wait(fn, seconds=60):
    until=time.monotonic()+seconds; error=None
    while time.monotonic()<until:
        try:
            if any(p.poll() is not None for p, _, _ in processes): raise SystemExit('wrapper exited before readiness')
            if fn(): return
        except Exception as e: error=e
        time.sleep(.2)
    raise AssertionError(f'condition timed out: {error}')

def rotate_once(i, phase, target=NEW, previous=OLD):
    payload=json.dumps(dict(operation=phase,newPassword=target,currentPassword=previous)).encode()
    error=None
    for password in [previous,target]:
        req=urllib.request.Request(f'http://127.0.0.1:{BASE+20+i}/credentials/rotate',data=payload,
            headers={'Content-Type':'application/json','Authorization':'Basic '+base64.b64encode(f'railway:{password}'.encode()).decode()})
        try:
            with urllib.request.urlopen(req,timeout=40) as r: return json.load(r)
        except urllib.error.HTTPError as e:
            error=e
            if e.code != 401: raise
    raise error

def rotate(i, phase, target=NEW, previous=OLD):
    # Model Temporal's bounded retries while a restarted Sentinel reconnects.
    until = time.monotonic() + 120
    while True:
        try:
            return rotate_once(i, phase, target, previous)
        except urllib.error.HTTPError as error:
            if error.code != 503 or time.monotonic() >= until:
                raise
            time.sleep(.5)

def quorum():
    for i in range(3):
        for password in [NEW, OLD]:
            try:
                assert command(BASE+10+i,password,'SENTINEL','CKQUORUM','mymaster').startswith('OK')
                break
            except Exception:
                if password == OLD: raise
    return True

with tempfile.TemporaryDirectory(prefix='redis-rotation-') as temp:
    try:
        for i in range(3):
            data=pathlib.Path(temp)/str(i); data.mkdir()
            env={**os.environ,'LC_ALL':'C','LANG':'C','PATH':os.environ['REDIS_BIN_DIR']+':'+os.environ['PATH'],
                'DATA_DIR':str(data),'REDIS_PASSWORD':OLD,'HEALTH_API_PASSWORD':OLD,
                'REDIS_PORT':str(BASE+i),'SENTINEL_PORT':str(BASE+10+i),'HEALTH_PORT':str(BASE+20+i),
                'SENTINEL_ENABLED':'true','SENTINEL_HOSTS':','.join(f'127.0.0.1:{BASE+10+j}' for j in range(3)),
                'RAILWAY_PRIVATE_DOMAIN':'127.0.0.1','REPLICA_OF':'' if i==0 else f'127.0.0.1:{BASE}',
                'MAXMEMORY_MB':'64','RUST_LOG':'warn'}
            log=open(data/'wrapper.log','wb')
            p=subprocess.Popen([WRAPPER],env=env,stdout=log,stderr=log,start_new_session=True)
            processes.append((p,log,data))
            environments.append(env)
        wait(quorum,90)
        wait(lambda: command(BASE,OLD,'SET','rotation-proof','retained') == 'OK')
        wait(lambda: all(command(BASE+i,OLD,'GET','rotation-proof')=='retained' for i in range(3)))
        for i in range(3): rotate(i,'preflight',OLD,OLD)
        for i in range(3): rotate(i,'prepare'); quorum()
        p,log,data=processes[2]
        os.killpg(p.pid,signal.SIGKILL); p.wait()
        p=subprocess.Popen([WRAPPER],env=environments[2],stdout=log,stderr=log,start_new_session=True)
        processes[2]=(p,log,data)
        wait(quorum)
        # Same phases repeated model an activity response lost after mutation.
        for i in range(3): rotate(i,'prepare'); quorum()
        for i in range(3): rotate(i,'member'); quorum()
        p,log,data=processes[1]
        os.killpg(p.pid,signal.SIGKILL); p.wait()
        p=subprocess.Popen([WRAPPER],env=environments[1],stdout=log,stderr=log,start_new_session=True)
        processes[1]=(p,log,data)
        wait(quorum)
        for i in range(3): rotate(i,'verify')
        command(BASE,NEW,'SET','rotation-proof','retained')
        wait(lambda: all(command(BASE+i,NEW,'GET','rotation-proof')=='retained' for i in range(3)))
        # Compensation uses the same protocol in the opposite direction.
        for i in range(3): rotate(i,'prepare',OLD,NEW); quorum()
        for i in range(3): rotate(i,'member',OLD,NEW); quorum()
        for i in range(3): rotate(i,'verify',OLD,NEW)
        assert command(BASE,OLD,'GET','rotation-proof')=='retained'
        print('PASS: three members rotated, restarted during prepare/adoption, and compensated; quorum retained; data retained')
    except BaseException:
        for p,log,data in processes:
            log.flush(); print(f'node {data.name}:', (data/'wrapper.log').read_text()[-4000:])
        raise
    finally:
        for p,log,data in processes:
            try: os.killpg(p.pid,signal.SIGKILL)
            except (ProcessLookupError, PermissionError): p.kill()
            p.wait(); log.close()
