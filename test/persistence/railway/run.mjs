// Real image swaps on a disposable Railway volume. Deletes ONLY its own project.
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { createClient } from 'redis';

const target = process.env.TARGET_IMAGE;
assert(target, 'TARGET_IMAGE must pin the exact redis-sentinel build under test');
assert(process.env.RAILWAY_WORKSPACE_ID, 'RAILWAY_WORKSPACE_ID must identify the test workspace');
const oldImage = process.env.SEED_IMAGE || 'railwayapp/redis:8.2.1';
const count = Number(process.env.PERSISTENCE_KEYS || 57401);
const password = randomUUID();
const name = 'redis-persistence-e2e-' + randomUUID().slice(0, 8);
const output = process.env.ARTIFACTS || 'railway-persistence-results';
mkdirSync(output, { recursive: true });
const evidence = { name, target, oldImage, count, stages: [] };
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const value = i => createHash('sha256').update(String(i)).digest('hex');
let project, environment, service, proxy, client;

async function api(query, variables = {}) {
  const token = process.env.RAILWAY_API_TOKEN || readFileSync(process.env.RAILWAY_ADMIN_TOKEN_FILE, 'utf8').trim();
  const r = await fetch('https://backboard.railway.com/graphql/v2', {
    method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ query, variables }), signal: AbortSignal.timeout(60000),
  });
  assert(r.ok, `Railway API HTTP ${r.status}`);
  const result = await r.json();
  assert(!result.errors, JSON.stringify(result.errors));
  return result.data;
}
function record(stage, details = {}) {
  const row = { stage, at: new Date().toISOString(), ...details };
  evidence.stages.push(row);
  writeFileSync(`${output}/results.json`, JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify(row));
}
async function deployments() {
  const d = await api(`query($service: String!, $environment: String!) {
    deployments(first: 5, input: {serviceId: $service, environmentId: $environment}) {
      edges { node { id status } }
    }
  }`, { service, environment });
  return d.deployments.edges.map(e => e.node);
}
async function waitDeployment(previous) {
  const end = Date.now() + 12 * 60000;
  let last;
  while (Date.now() < end) {
    const d = (await deployments())[0];
    if (d && d.id !== previous) {
      if (last !== `${d.id}:${d.status}`) { record('deployment', d); last = `${d.id}:${d.status}`; }
      if (d.status === 'SUCCESS') return d.id;
      assert(!['FAILED', 'CRASHED', 'REMOVED'].includes(d.status), `Deployment ${d.id}: ${d.status}`);
    }
    await pause(10000);
  }
  throw new Error('New deployment did not reach SUCCESS');
}
async function connect() {
  if (client?.isOpen) client.disconnect();
  client = createClient({ password, socket: {
    host: proxy.domain, port: proxy.proxyPort, connectTimeout: 15000,
    reconnectStrategy: retries => retries < 30 ? 1000 : new Error('Redis connection deadline'),
  }});
  client.on('error', () => {});
  await client.connect();
  assert.equal(await client.ping(), 'PONG');
}
async function verify(stage) {
  await connect();
  for (let i = 0; i < count; i += 512) {
    const ids = Array.from({ length: Math.min(512, count - i) }, (_, j) => i + j);
    assert.deepEqual(await client.mGet(ids.map(j => `key:${j}`)), ids.map(value), `values at ${i}`);
  }
  await client.select(5);
  assert.equal(await client.hGet('hash', 'field'), 'value\nwith newline');
  assert.deepEqual(await client.lRange('list', 0, -1), ['a', 'b', 'c']);
  await client.select(15);
  assert.deepEqual(await client.zRange('zset', 0, -1), ['a', 'b']);
  await client.select(0);
  assert.equal(await client.get('ttl-expired'), null);
  assert.equal(await client.get('ttl-live'), 'survives');
  assert((await client.pTTL('ttl-live')) > 0);
  record(stage, { verifiedKeys: count, info: await client.info('persistence') });
}
async function patch(image) {
  const before = (await deployments())[0]?.id;
  await api(`mutation($environment: String!, $patch: EnvironmentConfig) {
    environmentPatchCommit(environmentId: $environment, patch: $patch, commitMessage: "Persistence E2E image swap")
  }`, { environment, patch: { services: { [service]: {
    source: { image }, deploy: { startCommand: null },
    variables: { REDIS_AOF_ENABLED: { value: 'no' } },
  } } } });
  await waitDeployment(before);
}
async function redeploy() {
  const before = (await deployments())[0]?.id;
  await api(`mutation($service: String!, $environment: String!) {
    serviceInstanceRedeploy(serviceId: $service, environmentId: $environment)
  }`, { service, environment });
  await waitDeployment(before);
}
async function ageRdb(stage) {
  // Exercise the real 10-minute discriminator, without changing file mtimes.
  for (let i = 0; i < 11; i++) { record(stage, { minutesRemaining: 11 - i }); await pause(60000); }
  assert.equal(await client.save(), 'OK');
}
try {
  const p = await api(`mutation($input: ProjectCreateInput!) {
    projectCreate(input: $input) { id environments { edges { node { id } } } }
  }`, { input: { name, workspaceId: process.env.RAILWAY_WORKSPACE_ID } });
  project = p.projectCreate.id;
  environment = p.projectCreate.environments.edges[0].node.id;
  record('project-created', { project, environment });
  const s = await api(`mutation($input: ServiceCreateInput!) { serviceCreate(input: $input) { id } }`, {
    input: { projectId: project, environmentId: environment, name: 'Redis persistence fixture',
      source: { image: oldImage }, variables: {
        REDIS_PASSWORD: password, REDIS_PORT_NUMBER: '6379', REDIS_AOF_ENABLED: 'yes',
        ALLOW_EMPTY_PASSWORD: 'no', RAILWAY_RUN_UID: '0', SENTINEL_ENABLED: 'false',
      } },
  });
  service = s.serviceCreate.id;
  await api(`mutation($input: VolumeCreateInput!) { volumeCreate(input: $input) { id } }`, {
    input: { projectId: project, environmentId: environment, serviceId: service, mountPath: '/bitnami' },
  });
  const t = await api(`mutation($input: TCPProxyCreateInput!) { tcpProxyCreate(input: $input) { domain proxyPort } }`, {
    input: { serviceId: service, environmentId: environment, applicationPort: 6379 },
  });
  proxy = t.tcpProxyCreate;
  await redeploy(); // Ensure the deployment includes the just-created volume.
  await connect();
  assert.match(await client.info('persistence'), /aof_enabled:1/);
  assert.equal(await client.configSet('appendonly', 'no'), 'OK');
  for (let i = 0; i < count; i += 512) {
    const batch = client.multi();
    for (let j = i; j < Math.min(count, i + 512); j++) batch.set(`key:${j}`, value(j));
    assert((await batch.exec()).every(result => result === 'OK'));
  }
  await client.select(5);
  assert.equal(await client.hSet('hash', 'field', 'value\nwith newline'), 1);
  assert.equal(await client.rPush('list', ['a', 'b', 'c']), 3);
  await client.select(15);
  assert.equal(await client.zAdd('zset', [{ score: 1, value: 'a' }, { score: 2, value: 'b' }]), 2);
  await client.select(0);
  await client.set('ttl-live', 'survives', { EX: 7200 });
  await client.set('ttl-expired', 'gone', { PX: 1 });
  await client.save();
  await verify('legacy-seed-verified');
  await ageRdb('aging-abandoned-aof');
  await patch(target);
  await verify('migration-verified');
  assert.match(await client.info('persistence'), /aof_enabled:1/);
  assert.equal(await client.set('post-migration', 'acknowledged'), 'OK');
  await pause(2000);
  for (let i = 1; i <= 2; i++) {
    await redeploy();
    await verify(`redeploy-${i}-verified`);
    assert.equal(await client.get('post-migration'), 'acknowledged');
  }
  await patch(oldImage);
  await verify('revert-verified');
  assert.match(await client.info('persistence'), /aof_enabled:0/);
  assert.equal(await client.set('foreign-image-write', 'survives-repatch'), 'OK');
  await ageRdb('aging-reverted-aof');
  await patch(target);
  await verify('repatch-verified');
  assert.equal(await client.get('foreign-image-write'), 'survives-repatch');
  await redeploy();
  await verify('repatch-redeploy-verified');
  assert.equal(await client.get('foreign-image-write'), 'survives-repatch');
  record('PASS');
} catch (e) {
  record('FAIL', { error: e.message });
  process.exitCode = 1;
} finally {
  if (client?.isOpen) client.disconnect();
  if (service) {
    try {
      for (const d of await deployments()) {
        const logs = await api(`query($id: String!) { deploymentLogs(deploymentId: $id, limit: 500) { timestamp message severity } }`, { id: d.id });
        writeFileSync(`${output}/${d.id}.json`, JSON.stringify(logs, null, 2));
      }
    } catch (e) { record('log-collection-failed', { error: e.message }); }
  }
  if (project) {
    try {
      const result = await api('mutation($id: String!) { projectDelete(id: $id) }', { id: project });
      assert.equal(result.projectDelete, true);
      record('project-deleted', { project });
    } catch (e) { record('CLEANUP-FAILED', { project, error: e.message }); process.exitCode = 1; }
  }
}
