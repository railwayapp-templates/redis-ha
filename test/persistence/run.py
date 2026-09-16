#!/usr/bin/env python3
"""Real Redis persistence regression battery. Docker only; no production access.

Every assertion reads exact values, not just DBSIZE. Unique resource names and
finally cleanup prevent interference with other test runs. Seed and target
images are supplied by CI so data formats are checked within each Redis minor.
"""
import hashlib
import json
import os
import subprocess
import time
import traceback
import uuid
from pathlib import Path

IMAGE = os.environ.get('IMAGE', 'redis-sentinel-e2e:local')
SEED = os.environ.get('SEED_IMAGE', 'redis:8.2.1')
COUNT = int(os.environ.get('PERSISTENCE_KEYS', '57401'))
PREFIX = 'persist-' + uuid.uuid4().hex[:10]
OLD_STAMP = time.strftime('%Y%m%d%H%M.%S', time.gmtime(time.time() - 434 * 86400))
PW = 'persistence-regression-only'
ARTIFACTS = Path(os.environ.get('ARTIFACTS', 'persistence-results'))
ARTIFACTS.mkdir(parents=True, exist_ok=True)
containers, volumes, results = [], [], []


def docker(*args, input=None, check=True):
    r = subprocess.run(['docker', *map(str, args)], input=input, capture_output=True,
                       timeout=180)
    if check and r.returncode:
        raise RuntimeError(f'docker {args[:3]}: {r.stderr.decode(errors="replace")[-2000:]}')
    output = r.stdout + (r.stderr if args[0] == 'logs' else b'')
    return output.decode(errors='replace').strip()


def wait(fn, description, seconds=90):
    end = time.monotonic() + seconds
    last = None
    while time.monotonic() < end:
        try:
            if fn():
                return
        except Exception as e:
            last = e
        time.sleep(.25)
    raise AssertionError(f'timeout: {description}; {last}')


def volume():
    v = f'{PREFIX}-v{len(volumes)}'
    docker('volume', 'create', v)
    volumes.append(v)
    return v


def fs(v, script):
    return docker('run', '--rm', '-v', f'{v}:/v', 'alpine:3.22', 'sh', '-ec', script)


def cli(c, *args, db=0):
    output = docker('exec', c, 'redis-cli', '-2', '--no-auth-warning', '-a', PW,
                    '-n', db, '--json', *args)
    # redis-cli treats INFO as raw text even when --json is requested.
    return output if args[0] == 'INFO' else json.loads(output)


def start(v, target=False, nested=False, aof=False):
    c = f'{PREFIX}-c{len(containers)}'
    containers.append(c)
    mount = '/bitnami' if nested else '/data'
    path = mount + ('/redis/data' if nested else '')
    args = ['run', '-d', '--name', c, '-v', f'{v}:{mount}']
    if target:
        args += ['-e', f'RAILWAY_VOLUME_MOUNT_PATH={mount}', '-e', f'REDIS_PASSWORD={PW}',
                 '-e', 'SENTINEL_ENABLED=false', '-e', 'REDIS_AOF_ENABLED=no', IMAGE]
    else:
        fs(v, 'mkdir -p ' + ('/v/redis/data' if nested else '/v'))
        args += ['--user', '0', '--entrypoint', 'redis-server', SEED, '--dir', path,
                 '--requirepass', PW, '--appendonly', 'yes' if aof else 'no', '--save', '']
    docker(*args)
    return c


def ready(c):
    wait(lambda: cli(c, 'PING') == 'PONG', 'Redis accepts connections')


def stop(c, graceful=True):
    docker('stop' if graceful else 'kill', c)



def shutdown_nosave(c):
    # Docker can terminate the exec client as soon as Redis (PID 1) exits.
    # Check the SERVER exit instead of treating that client-side race as failure.
    docker('exec', c, 'redis-cli', '-a', PW, '--no-auth-warning', 'SHUTDOWN', 'NOSAVE', check=False)
    wait(lambda: docker('inspect', '-f', '{{.State.Running}}', c) == 'false', 'clean source shutdown')
    assert docker('inspect', '-f', '{{.State.ExitCode}}', c) == '0', 'source did not shut down cleanly'


def age(v, nested=False):
    p = '/v/redis/data' if nested else '/v'
    fs(v, f"find {p}/appendonlydir -type f -exec touch -t {OLD_STAMP} {{}} +")


def manifest(c, nested=False):
    p = '/bitnami/redis/data' if nested else '/data'
    return docker('exec', c, 'sh', '-c', f'test -s {p}/appendonlydir/appendonly.aof.manifest && echo yes', check=False) == 'yes'


def committed(c, nested=False):
    wait(lambda: 'aof_rewrite_in_progress:0' in cli(c, 'INFO', 'persistence')
         and 'aof_last_bgrewrite_status:ok' in cli(c, 'INFO', 'persistence')
         and manifest(c, nested), 'AOF durably committed')


def seed(c):
    # RESP pipeline, checking the server's error counter. Every value is later
    # read back, including >57k keys and Redis collection types in other DBs.
    payload = bytearray()
    for i in range(COUNT):
        parts = [b'SET', f'key:{i}'.encode(), hashlib.sha256(str(i).encode()).hexdigest().encode()]
        payload += f'*{len(parts)}\r\n'.encode()
        for p in parts:
            payload += f'${len(p)}\r\n'.encode() + p + b'\r\n'
    output = docker('exec', '-i', c, 'redis-cli', '-a', PW, '--no-auth-warning', '--pipe', input=bytes(payload))
    assert 'errors: 0' in output, output
    assert cli(c, 'HSET', 'hash', 'field', 'value\nwith newline', db=5) == 1
    assert cli(c, 'RPUSH', 'list', 'a', 'b', 'c', db=5) == 3
    assert cli(c, 'SADD', 'set', 'a', 'b', db=5) == 2
    assert cli(c, 'ZADD', 'zset', '1', 'a', '2', 'b', db=15) == 2
    assert cli(c, 'XADD', 'stream', '1-0', 'field', 'value', db=15) == '1-0'
    assert cli(c, 'SET', 'ttl-survives', 'yes', 'EX', '3600') == 'OK'
    assert cli(c, 'SET', 'ttl-expires', 'gone', 'PX', '1') == 'OK'
    assert cli(c, 'SAVE') == 'OK'


def verify(c):
    for i in range(0, COUNT, 512):
        ids = range(i, min(COUNT, i + 512))
        actual = cli(c, 'MGET', *[f'key:{j}' for j in ids])
        expected = [hashlib.sha256(str(j).encode()).hexdigest() for j in ids]
        assert actual == expected, f'missing/corrupt data in keys {i}..{i+len(expected)}'
    assert cli(c, 'HGET', 'hash', 'field', db=5) == 'value\nwith newline'
    assert cli(c, 'LRANGE', 'list', '0', '-1', db=5) == ['a', 'b', 'c']
    assert sorted(cli(c, 'SMEMBERS', 'set', db=5)) == ['a', 'b']
    assert cli(c, 'ZRANGE', 'zset', '0', '-1', 'WITHSCORES', db=15) == ['a', '1', 'b', '2']
    assert cli(c, 'XRANGE', 'stream', '-', '+', db=15) == [['1-0', ['field', 'value']]]
    assert cli(c, 'GET', 'ttl-expires') is None
    assert cli(c, 'GET', 'ttl-survives') == 'yes'
    assert 0 < cli(c, 'PTTL', 'ttl-survives') < 3600000


def stale_fixture(nested=False, nonempty=False):
    v = volume()
    old = start(v, nested=nested, aof=True)
    ready(old)
    if nonempty:
        assert cli(old, 'SET', 'abandoned-only', 'obsolete') == 'OK'
    committed(old, nested)
    # NOSAVE leaves only the AOF. A different, AOF-off process writes the RDB.
    shutdown_nosave(old)
    age(v, nested)
    old = start(v, nested=nested)
    ready(old)
    seed(old)
    verify(old)
    stop(old)
    return v


def migration(nested=False, nonempty=False):
    v = stale_fixture(nested, nonempty)
    n = start(v, target=True, nested=nested)
    ready(n)
    committed(n, nested)
    verify(n)
    assert cli(n, 'GET', 'abandoned-only') is None
    p = '/v/redis/data' if nested else '/v'
    assert fs(v, f'find {p} -maxdepth 1 -name "appendonlydir.superseded-*" | wc -l') == '1'
    # Acknowledged post-migration writes must survive normal and abrupt restarts.
    cli(n, 'SET', 'post-migration', 'acknowledged')
    time.sleep(2)  # appendfsync everysec durability boundary
    for graceful in [True, False, True]:
        stop(n, graceful)
        docker('start', n)
        ready(n)
        committed(n, nested)
        verify(n)
        assert cli(n, 'GET', 'post-migration') == 'acknowledged'
    assert fs(v, f'find {p} -maxdepth 1 -name "appendonlydir.superseded-*" | wc -l') == '1'
    stop(n)


def live_incremental(marker):
    v = volume()
    n = start(v)
    ready(n)
    seed(n)
    cli(n, 'CONFIG', 'SET', 'appendonly', 'yes')
    committed(n)
    # The RDB/base knows nothing about this later acknowledged write.
    cli(n, 'SET', 'aof-only', 'must-survive')
    time.sleep(2)
    shutdown_nosave(n)
    fs(v, f"find /v/appendonlydir -type f ! -name '*.incr.aof' -exec touch -t {OLD_STAMP} {{}} +")
    if marker == 'corrupt':
        fs(v, "echo invalid > /v/.rdb_owner")
    n = start(v, target=True)
    ready(n)
    verify(n)
    assert cli(n, 'GET', 'aof-only') == 'must-survive'
    assert fs(v, 'find /v -maxdepth 1 -name "appendonlydir.superseded-*" | wc -l') == '0'
    stop(n)


def inspection_failure(kind):
    v = stale_fixture()
    original = fs(v, 'sha256sum /v/dump.rdb /v/appendonlydir/*')
    if kind == 'entry':
        fs(v, 'ln -s absent /v/appendonlydir/unreadable-incremental.aof')
    else:
        fs(v, 'mv /v/appendonlydir/appendonly.aof.manifest /v/manifest.saved; ln -s absent /v/appendonlydir/appendonly.aof.manifest')
    n = start(v, target=True)
    wait(lambda: docker('inspect', '-f', '{{.State.Running}}', n) == 'false', 'unsafe boot refused', 30)
    assert docker('inspect', '-f', '{{.State.ExitCode}}', n) != '0'
    log = docker('logs', n)
    assert 'starting redis-server' not in log
    # Remove the deliberately invalid fixture link before comparing bytes.
    if kind == 'entry':
        fs(v, 'rm /v/appendonlydir/unreadable-incremental.aof')
    else:
        fs(v, 'rm /v/appendonlydir/appendonly.aof.manifest; mv /v/manifest.saved /v/appendonlydir/appendonly.aof.manifest')
    assert fs(v, 'sha256sum /v/dump.rdb /v/appendonlydir/*') == original


cases = [
    ('stale-empty-aof-root', lambda: migration()),
    ('stale-nonempty-aof-root', lambda: migration(nonempty=True)),
    ('stale-empty-aof-bitnami-nested', lambda: migration(nested=True)),
    ('stale-nonempty-aof-bitnami-nested', lambda: migration(nested=True, nonempty=True)),
    ('live-incremental-no-marker', lambda: live_incremental('missing')),
    ('live-incremental-corrupt-marker', lambda: live_incremental('corrupt')),
    ('partial-aof-inspection', lambda: inspection_failure('entry')),
    ('broken-manifest', lambda: inspection_failure('manifest')),
]
selected = os.environ.get('PERSISTENCE_CASES')
if selected:
    names = set(selected.split(','))
    assert names <= {name for name, _ in cases}, 'unknown persistence scenario'
    cases = [(name, fn) for name, fn in cases if name in names]
try:
    docker('pull', SEED)
    docker('pull', 'alpine:3.22')
    for name, fn in cases:
        start_time = time.monotonic()
        try:
            fn()
            result = {'case': name, 'status': 'passed'}
        except Exception as e:
            traceback.print_exc()
            for c in containers:
                print(docker('logs', '--tail', '40', c, check=False), flush=True)
            result = {'case': name, 'status': 'failed', 'error': str(e)}
        result['seconds'] = round(time.monotonic() - start_time, 2)
        results.append(result)
        print(json.dumps(result), flush=True)
        case_dir = ARTIFACTS / name
        case_dir.mkdir(exist_ok=True)
        for c in containers:
            (case_dir / f'{c}.log').write_text(docker('logs', c, check=False))
        # Each case is independent; release resources even after a failure.
        for c in containers:
            docker('rm', '-f', c, check=False)
        containers.clear()
        for v in volumes:
            docker('volume', 'rm', '-f', v, check=False)
        volumes.clear()
finally:
    (ARTIFACTS / 'results.json').write_text(json.dumps({'image': IMAGE, 'seed': SEED, 'keys': COUNT, 'results': results}, indent=2))
    for c in containers:
        docker('rm', '-f', c, check=False)
    for v in volumes:
        docker('volume', 'rm', '-f', v, check=False)
assert len(results) == len(cases) and all(r['status'] == 'passed' for r in results), 'persistence regressions failed'
