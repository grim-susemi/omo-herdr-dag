import { test } from 'node:test';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import {cp,mkdir,mkdtemp,readFile,readdir,rm,writeFile,stat} from 'node:fs/promises';import {tmpdir} from 'node:os';import {join,dirname} from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';import {createHash} from 'node:crypto';
const root=fileURLToPath(new URL('../',import.meta.url)),namespace='herdr-dag',marker='// managed by omo-'+namespace;
async function snapshot(dir){try{const names=await readdir(dir,{recursive:true});const result={};for(const name of names.sort()){try{const b=await readFile(join(dir,name));result[name]=createHash('sha256').update(b).digest('hex');}catch(e){if(e.code!=='EISDIR'&&e.code!=='EPERM')throw e;result[name]='directory';}}return result;}catch(e){if(e.code==='ENOENT')return null;throw e;}}
test('installer subprocess homes and default agent are fixture-owned before any invocation',async t=>{const f=await fixture(t);assert.equal(f.env?.HOME,join(f.dir,'home'));assert.equal(f.env?.USERPROFILE,join(f.dir,'home'));for(const key of ['OMO_CODING_AGENT_DIR','SENPI_CODING_AGENT_DIR','PI_CODING_AGENT_DIR'])assert.equal(f.env?.[key],f.agent);assert.equal(f.env?.npm_config_cache,join(f.dir,'npm-cache'));console.log('INSTALL_FIXTURE_ENV '+JSON.stringify(f.env&&Object.fromEntries(['HOME','USERPROFILE','OMO_CODING_AGENT_DIR','SENPI_CODING_AGENT_DIR','PI_CODING_AGENT_DIR','npm_config_cache'].map(k=>[k,f.env[k]]))));const plan=f.install('--dry-run');assert.equal(plan.extension,join(f.agent,'extensions','omo-'+namespace+'.js'));assert.equal(await snapshot(f.agent),null);const result=f.install();assert.equal(dirname(dirname(result.integration)),join(f.agent,namespace));assert.equal(result.extension,join(f.agent,'extensions','omo-'+namespace+'.js'));});
async function fixture(t){const dir=await mkdtemp(join(tmpdir(),'package-guard-'));t.after(async()=>{await rm(dir,{recursive:true,force:true});console.log('PACKAGE_FIXTURE_CLEANED '+JSON.stringify({directory:dir}));});const source=join(dir,'source'),agent=join(dir,'agent');await mkdir(join(source,'scripts'),{recursive:true});await cp(join(root,'scripts/install.mjs'),join(source,'scripts/install.mjs'));await cp(join(root,'src'),join(source,'src'),{recursive:true});await cp(join(root,'extension.mjs'),join(source,'extension.mjs'));await cp(join(root,'LICENSE'),join(source,'LICENSE'));const env={ ...process.env, HOME: join(dir, 'home'), USERPROFILE: join(dir, 'home'), OMO_CODING_AGENT_DIR: agent, SENPI_CODING_AGENT_DIR: agent, PI_CODING_AGENT_DIR: agent, npm_config_cache: join(dir, 'npm-cache') };const install=(...args)=>{for(const key of ['HOME','USERPROFILE','OMO_CODING_AGENT_DIR','SENPI_CODING_AGENT_DIR','PI_CODING_AGENT_DIR','npm_config_cache'])assert.ok(env[key].startsWith(dir+join('/')),key+' escaped fixture');return JSON.parse(execFileSync(process.execPath,[join(source,'scripts/install.mjs'),...args],{env,encoding:'utf8',timeout:10000,stdio:['ignore','pipe','pipe']}));};return{dir,source,agent,install,env};}
test('installer validates explicit agent path and validates all arguments without writes',async t=>{const f=await fixture(t);for(const args of [['--agent-dir'],['--agent-dir',f.agent,'--unknown'],['--agent-dir',f.agent,'--lang','de'],['--agent-dir',f.agent,'--agent-dir',f.agent]]){assert.throws(()=>f.install(...args));assert.equal(await snapshot(f.agent),null);}});
test('dry run is zero-write and installed generations retain locale and runtime records',async t=>{const f=await fixture(t),command=['--agent-dir',f.agent,'--lang','ko'];const before=await snapshot(f.agent),dry=f.install(...command,'--dry-run');assert.deepEqual(f.install(...command,'--dry-run'),dry);assert.deepEqual(await snapshot(f.agent),before);const first=f.install(...command);assert.equal(first.installed,true);await writeFile(join(f.agent,namespace,'retained.json'),'retained');const after=await snapshot(f.agent);f.install('--agent-dir',f.agent,'--dry-run');assert.deepEqual(await snapshot(f.agent),after);const second=f.install('--agent-dir',f.agent);assert.notEqual(first.integration,second.integration);assert.equal(second.language,'ko');assert.equal(await readFile(join(f.agent,namespace,'retained.json'),'utf8'),'retained');assert.ok((await readFile(second.extension,'utf8')).startsWith(marker));});
test('unmanaged wrapper and directory fail without clobbering bytes',async t=>{const f=await fixture(t);const wrapper=join(f.agent,'extensions','omo-'+namespace+'.js');await mkdir(dirname(wrapper),{recursive:true});await writeFile(wrapper,'foreign');const before=await snapshot(f.agent);assert.throws(()=>f.install('--agent-dir',f.agent));assert.deepEqual(await snapshot(f.agent),before);await rm(wrapper);const container=join(f.agent,namespace,'integration');await mkdir(container,{recursive:true});await writeFile(join(container,'.installed-by'),'foreign');const dirBefore=await snapshot(f.agent);assert.throws(()=>f.install('--agent-dir',f.agent));assert.deepEqual(await snapshot(f.agent),dirBefore);});
test('missing source runtime and interrupted selected generation fail without clobber',async t=>{const f=await fixture(t);await rm(join(f.source,'src/viewer.mjs'));assert.throws(()=>f.install('--agent-dir',f.agent));assert.equal(await snapshot(f.agent),null);await cp(join(root,'src/viewer.mjs'),join(f.source,'src/viewer.mjs'));const first=f.install('--agent-dir',f.agent);await rm(join(first.integration,'src/viewer.mjs'));const before=await snapshot(f.agent);assert.throws(()=>f.install('--agent-dir',f.agent));assert.deepEqual(await snapshot(f.agent),before);});

if(process.platform==='win32') test('failed managed-wrapper promotion restores current pointer and leaves no generation or temporary files',async t=>{const f=await fixture(t);const first=f.install('--agent-dir',f.agent);const before=await snapshot(f.agent);const {chmod}=await import('node:fs/promises');await chmod(first.extension,0o444);try{assert.throws(()=>f.install('--agent-dir',f.agent));assert.deepEqual(await snapshot(f.agent),before);}finally{await chmod(first.extension,0o666);}});

async function legacyFixture(t, location = 'state') {
  const f = await fixture(t), first = f.install('--agent-dir', f.agent);
  const { install } = await import(pathToFileURL(join(f.source, 'scripts/install.mjs')));
  const directory = location === 'generation' ? join(first.integration, 'state', 'legacy') : join(f.agent, namespace, 'legacy');
  await mkdir(directory, { recursive: true });
  const recordPath = join(directory, 'dag.pane.json'), statePath = join(directory, 'dag.json');
  const shell = { ProcessId: 3101, ParentProcessId: 2101, Birth: '134000000000000001',
    CreationDate: '2026-10-14T00:00:00.0000000Z', ExecutablePath: 'C:/fixture/pwsh.exe', CommandLine: '"C:/fixture/pwsh.exe" -NoLogo' };
  const record = { schema: 'omo-herdr-dashboard/pane/1', socketPath: 'fixture-socket', parentPaneId: 'w1:parent',
    sessionId: 'legacy-session', role: 'dag', paneId: 'w1:dag', launchToken: 'legacy-token',
    processId: 4101, viewerBirth: '134000000000000002', creationProcessId: shell.ProcessId,
    creationBirth: shell.Birth, creationDate: shell.CreationDate, nodePath: 'C:/fixture/node.exe',
    viewerPath: join(first.integration, 'src/viewer.mjs'), statePath };
  const argv = [record.nodePath, record.viewerPath, '--view', 'dag', '--state', statePath,
    '--close-pane', record.paneId, '--launch-token', record.launchToken];
  const viewer = { ProcessId: record.processId, ParentProcessId: shell.ProcessId, Birth: record.viewerBirth,
    CreationDate: '2026-10-14T00:00:01.0000000Z', ExecutablePath: record.nodePath,
    CommandLine: argv.map(value => '"' + value + '"').join(' ') };
  await writeFile(recordPath, JSON.stringify(record));
  await writeFile(statePath, JSON.stringify({ schema: 'omo-herdr-dashboard/role/1', tasks: ['PRIVATE_CONVERSATION_PAYLOAD'] }));
  const rows = [viewer, shell], queries = [];
  const query = async pids => { queries.push(pids); return rows.filter(row => pids.includes(row.ProcessId)); };
  const run = (options = {}, ...args) => install(['--agent-dir', f.agent, ...args],
    { platform: 'win32', queryProcesses: query, ...options });
  return { ...f, first, run, record, recordPath, statePath, viewer, shell, rows, queries, directory };
}

test('diagnostic preflight with no legacy makes no native query and proceeds', async t => {
  const f = await fixture(t), { install } = await import(pathToFileURL(join(f.source, 'scripts/install.mjs')));
  const result = await install(['--agent-dir', f.agent], { platform: 'win32',
    queryProcesses: () => assert.fail('no recorded PID should be queried') });
  assert.equal(result.installed, true);
  assert.equal(result.preflight.status, 'no recorded live processes');
  assert.equal(result.preflight.recordedProcesses, 0);
});

test('absent recorded processes proceed without inventing identity or retirement proof', async t => {
  const f = await legacyFixture(t);
  delete f.record.viewerBirth;
  await writeFile(f.recordPath, JSON.stringify(f.record));
  const result = await f.run({ queryProcesses: async () => [] });
  const observation = JSON.parse(await readFile(result.preflight.observationPath, 'utf8'));
  assert.equal(observation.status, 'no recorded live processes');
  assert.equal(observation.processes[0].state, 'absent');
  assert.equal(observation.diagnosticOnly, true);
});

test('matching live viewer and producer are observed privately before generation promotion', async t => {
  const f = await legacyFixture(t, 'generation');
  const producer = { ProcessId: 5101, ParentProcessId: f.shell.ProcessId, Birth: '134000000000000003',
    CreationDate: '2026-10-14T00:00:02.0000000Z', ExecutablePath: 'C:/fixture/node.exe',
    CommandLine: '"C:/fixture/node.exe" "C:/fixture/cli.mjs"' };
  f.rows.push(producer);
  await writeFile(join(f.directory, 'slot.json'), JSON.stringify({ schema: 'omo-herdr-dashboard/parent-slot/2',
    canonicalSocketPath: f.record.socketPath, sessionId: f.record.sessionId,
    parent: { paneId: f.record.parentPaneId, shell: { pid: f.shell.ProcessId, birth: f.shell.Birth } },
    producer: { pid: producer.ProcessId, birth: producer.Birth, executable: producer.ExecutablePath,
      argvDigest: createHash('sha256').update(producer.CommandLine).digest('hex') }, roles: {} }));
  const before = await snapshot(f.first.integration);
  const result = await f.run();
  const observation = JSON.parse(await readFile(result.preflight.observationPath, 'utf8'));
  assert.equal(result.installed, true);
  assert.equal(observation.status, 'live observed');
  assert.equal(observation.diagnosticOnly, true);
  assert.equal(observation.previousGeneration, 'generation-000001');
  assert.equal(observation.processes.length, 2);
  assert.deepEqual(observation.processes.map(row => row.identity.pid).sort(), [4101, 5101]);
  assert.equal(dirname(result.preflight.observationPath), dirname(result.integration));
  assert.deepEqual(await snapshot(f.first.integration), before);
  assert.doesNotMatch(JSON.stringify(observation), /PRIVATE_CONVERSATION_PAYLOAD|READY|custodyBinding/);
  if (process.platform !== 'win32') assert.equal((await stat(result.preflight.observationPath)).mode & 0o777, 0o600);
});

test('live legacy dry-run observes identity with zero writes', async t => {
  const f = await legacyFixture(t), before = await snapshot(f.dir);
  const plan = await f.run({}, '--dry-run');
  assert.equal(plan.preflight.status, 'live observed');
  assert.deepEqual(await snapshot(f.dir), before);
  assert.deepEqual(f.queries.flat().sort(), [3101, 4101]);
});

test('mismatch, malformed identity and ambiguous owners reject before any writes', async t => {
  for (const reason of ['birth', 'executable', 'argv', 'parent', 'shell-birth', 'missing-birth', 'unparseable-query',
    'duplicate-query', 'ambiguous-owner', 'ambiguous-scope', 'producer-birth', 'producer-argv', 'malformed-record', 'query-error']) await t.test(reason, async t => {
    const f = await legacyFixture(t);
    let queryProcesses;
    if (reason === 'birth') f.viewer.Birth = '134000000000000099';
    if (reason === 'executable') f.viewer.ExecutablePath = 'C:/foreign/node.exe';
    if (reason === 'argv') f.viewer.CommandLine = f.viewer.CommandLine.replace('legacy-token', 'foreign-token');
    if (reason === 'parent') f.viewer.ParentProcessId = 9999;
    if (reason === 'shell-birth') f.shell.Birth = '134000000000000099';
    if (reason === 'missing-birth') { delete f.record.viewerBirth; await writeFile(f.recordPath, JSON.stringify(f.record)); }
    if (reason === 'unparseable-query') queryProcesses = async () => ({});
    if (reason === 'duplicate-query') queryProcesses = async () => [f.viewer, f.viewer];
    if (reason === 'ambiguous-owner') {
      const other = { ...f.record, socketPath: f.record.socketPath.toUpperCase(), processId: 4102, sessionId: 'other-session' };
      await writeFile(join(f.directory, 'other.pane.json'), JSON.stringify(other));
      f.rows.push({ ...f.viewer, ProcessId: other.processId });
    }
    if (['ambiguous-scope', 'producer-birth', 'producer-argv'].includes(reason)) {
      const producer = { ...f.viewer, ProcessId: 5101, Birth: '134000000000000003',
        CommandLine: '"C:/fixture/node.exe" "C:/fixture/cli.mjs"' };
      const identity = { pid: producer.ProcessId, birth: producer.Birth, executable: producer.ExecutablePath,
        argvDigest: createHash('sha256').update(producer.CommandLine).digest('hex') };
      await writeFile(join(f.directory, 'slot.json'), JSON.stringify({ schema: 'omo-herdr-dashboard/parent-slot/2',
        canonicalSocketPath: f.record.socketPath, sessionId: reason === 'ambiguous-scope' ? 'other-session' : f.record.sessionId,
        parent: { paneId: f.record.parentPaneId, shell: { pid: f.shell.ProcessId, birth: f.shell.Birth } },
        producer: identity, roles: {} }));
      if (reason === 'producer-birth') producer.Birth = '134000000000000099';
      if (reason === 'producer-argv') producer.CommandLine += ' --foreign';
      f.rows.push(producer);
    }
    if (reason === 'malformed-record') await writeFile(f.recordPath, '{');
    if (reason === 'query-error') queryProcesses = async () => { throw new Error('denied'); };
    const before = await snapshot(f.dir);
    for (const args of [[], ['--dry-run']]) {
      await assert.rejects(f.run(queryProcesses ? { queryProcesses } : {}, ...args), /Legacy identity preflight:/);
      assert.deepEqual(await snapshot(f.dir), before);
    }
  });
});

test('non-Windows preflight explicitly records unsupported observation without querying', async t => {
  const f = await legacyFixture(t), result = await f.run({ platform: 'linux',
    queryProcesses: () => assert.fail('unsupported platform must not query Windows') });
  const observation = JSON.parse(await readFile(result.preflight.observationPath, 'utf8'));
  assert.equal(observation.status, 'unsupported platform');
  assert.equal(observation.platform, 'linux');
  assert.equal(observation.recordedProcesses, 1);
  assert.deepEqual(observation.processes, []);
});
