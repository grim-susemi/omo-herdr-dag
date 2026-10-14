import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import net from 'node:net';
import { EventEmitter, once } from 'node:events';
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import * as fixedController from '../src/controller.mjs';
import { FixedDagPane, paneViewerProcess, canCleanupPane, parseLaunchCommand } from '../src/controller.mjs';
import { writeJson } from '../src/storage.mjs';
import { payload, sessionId } from './fixtures.mjs';

test('POSIX cleanup accepts the owned viewer but refuses foreign foreground siblings', () => {
  const module = new URL('../src/controller.mjs', import.meta.url).href;
  const code = `
    Object.defineProperty(process, 'platform', { value: 'linux' });
    const { canCleanupPane } = await import(${JSON.stringify(module)});
    const record = { creationProcessId: 456, processId: 123, nodePath: 'node',
      viewerPath: 'viewer.mjs', role: 'dag', statePath: 'state.json',
      paneId: 'qa:pane', launchToken: 'launch' };
    const owned = { pid: 123, argv: ['node', 'viewer.mjs', '--view', 'dag',
      '--state', 'state.json', '--close-pane', 'qa:pane', '--launch-token', 'launch'] };
    const shell = { pid: 456, argv: ['sh'] }, foreign = { pid: 789, argv: ['foreign'] };
    const results = [];
    for (const rows of [[owned], [shell], [owned, foreign], [owned, owned]]) {
      results.push(await canCleanupPane({ process_info: {
        shell_pid: 456, foreground_processes: rows } }, record));
    }
    console.log(JSON.stringify(results));
  `;
  const output = execFileSync(process.execPath, ['--input-type=module', '--eval', code],
    { encoding: 'utf8', timeout: 10000 });
  assert.deepEqual(JSON.parse(output), [true, true, false, false]);
});

function control(type, state, pid) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(state.controlOffer.pipePath);
    const abort = AbortSignal.timeout(3000);
    abort.addEventListener('abort', () => { socket.destroy(); reject(abort.reason); }, { once: true });
    socket.once('connect', () => socket.write(`${JSON.stringify({ ...state.controlOffer, type, pid })}\n`));
    socket.once('data', data => { socket.end(); resolve(JSON.parse(data.toString())); });
    socket.once('error', reject);
  });
}
test('Windows shell-only report verifies exact native PID, parent and fixed role', async () => {
  if (process.platform !== 'win32') return;
  const record = { role: 'dag', paneId: 'qa:pane', statePath: 'C:\\QA space\\dag.json',
    viewerPath: 'C:\\QA space\\viewer.mjs', nodePath: 'C:\\Program Files\\nodejs\\node.exe',
    launchToken: 'launch', processId: 123, creationProcessId: 456, creationDate: '2026-10-03T00:00:00.000Z' };
  const info = { process_info: { shell_pid: 456, foreground_processes: [{ pid: 456, argv: ['powershell.exe'] }] } };
  const shell = { ProcessId: 456, ExecutablePath: 'powershell.exe', CommandLine: 'powershell.exe',
    CreationDate: record.creationDate };
  const native = { ProcessId: 123, ParentProcessId: 456, ExecutablePath: record.nodePath,
    CreationDate: '2026-10-03T00:00:01.000Z',
    CommandLine: `"${record.nodePath}" "${record.viewerPath}" --view dag --state "${record.statePath}" --close-pane qa:pane --launch-token launch` };
  assert.equal((await paneViewerProcess(info, record, 123, async () => [shell, native])).pid, 123);
  for (const child of [null, { ...native, ProcessId: 124 }, { ...native, ParentProcessId: 457 },
    { ...native, ExecutablePath: 'C:\\foreign.exe' },
    { ...native, CommandLine: native.CommandLine.replace('--view dag', '--view tasks') },
    { ...native, CommandLine: native.CommandLine.replace('--launch-token launch', '--launch-token stale') },
    { ...native, CommandLine: native.CommandLine.replace('qa:pane', 'qa:foreign') },
    { ...native, CommandLine: native.CommandLine.replace('dag.json', 'foreign.json') }])
    assert.equal(await paneViewerProcess(info, record, 123, async () => child ? [shell, child] : [shell]), null);
  let inspected = false;
  assert.equal(await paneViewerProcess({ process_info: { ...info.process_info, shell_pid: 457 } },
    record, 123, async () => { inspected = true; return native; }), null);
  assert.equal(inspected, false);
  for (const command of [native.CommandLine + ' --once', native.CommandLine + ' --view dag',
    native.CommandLine + ' extra', native.CommandLine.replace('token launch', 'token "launchx'),
    native.CommandLine.replace('"C:\\QA space\\viewer.mjs"', '"C:\\QA space\\"viewer.mjs')]) {
    const child = { ...native, CommandLine: command };
    assert.equal(await paneViewerProcess(info, record, 123, async () => [shell, child]), null);
    assert.equal(await canCleanupPane(info, record, async () => [shell, child]), false);
  }
  assert.equal(parseLaunchCommand('--launch-token "launchx'), null);
  assert.equal(parseLaunchCommand('"node\\" --state state'), null);
  assert.equal(await canCleanupPane(info, record, async () => [shell]), true);
  assert.equal(await canCleanupPane(info, record, async () => [shell, native]), true);
  for (const rows of [[], [native], [shell, { ...native, ExecutablePath: 'foreign.exe' }],
    [shell, native, { ...native, ProcessId: 789 }], [shell, { ...native, CreationDate: undefined }],
    [{ ...shell, CreationDate: '2026-10-03T00:00:02.000Z' }, native]])
    assert.equal(await canCleanupPane(info, record, async () => rows), false);
  assert.equal(await canCleanupPane(info, record, async () => { throw new Error('CIM failed'); }), false);
  assert.equal(await paneViewerProcess(info, record, 123, async () =>
    [{ ...shell, CreationDate: '2026-10-03T00:00:02.000Z' }, native]), null);
});
async function setup(t) {
  const directory = join(tmpdir(), `fixed-controller-${process.pid}-${randomUUID()}`);
  console.log(JSON.stringify({ event: 'resource-planned', pid: process.pid, directory }));
  await mkdir(directory);
  const calls = [], warnings = [], events = new EventEmitter();
  const panes = new Map([['qa:parent', { pane_id: 'qa:parent', tab_id: 'qa:tab' }]]);
  const processes = new Map();
  let number = 0;
  let focused = 'qa:parent';
  const herdr = async (...args) => {
    calls.push(args);
    if (args[0] === 'list') return { panes: [...panes.values()].map(pane => ({ ...pane, focused: pane.pane_id === focused })) };
    if (args[0] === 'layout') return { layout: { tab_id: 'qa:tab', focused_pane_id: focused,
      panes: [...panes.values()].map((pane, index) => ({ ...pane, rect: { x: index ? 100 : 0, y: index ? index * 10 : 0, width: 50, height: 10 } })) } };
    if (args[0] === 'swap') { focused = args[2]; return {}; }
    if (args[0] === 'neighbor') return { neighbor: { neighbor_pane_id: 'qa:parent', layout: { tab_id: 'qa:tab', focused_pane_id: focused } } };
    if (args[0] === 'focus') { focused = 'qa:parent'; return {}; }
    if (args[0] === 'split') {
      const pane_id = `qa:role${++number}`;
      panes.set(pane_id, { pane_id, tab_id: 'qa:tab' }); return { pane: { pane_id } };
    }
    if (args[0] === 'process-info') return { process_info: { shell_pid: 7000 + Number(args[2].replace('qa:role', '')),
      foreground_processes: processes.get(args[2]) ?? [{ pid: 7000 + Number(args[2].replace('qa:role', '')), argv: ['powershell.exe'] }] } };
    if (args[0] === 'close') { panes.delete(args[1]); events.emit(`closed:${args[1]}`); return {}; }
    if (args[0] === 'run') {
      const argv = [...args[2].matchAll(/'((?:''|[^'])*)'/g)].map(match => match[1].replaceAll("''", "'"));
      const file = argv[argv.indexOf('--state') + 1], state = JSON.parse(await readFile(file, 'utf8'));
      processes.set(args[1], [{ pid: 6000 + number, argv }]);
      assert.equal((await control('register', state, 6000 + number)).type, 'registered');
      return {};
    }
    return {};
  };
  const options = { sessionId, parentPane: 'qa:parent', socket: 'qa:socket', stateDir: directory,
    managedStateDir: join(directory, 'no-managed-owner'),
    cwd: directory, taskStateDir: join(directory, 'native-source'), node: 'node', viewer: join(directory, 'viewer.mjs'),
    herdr, notify: message => warnings.push(message),
    inspectShell: async pid => [{ ProcessId: pid, ExecutablePath: 'powershell.exe',
      CommandLine: 'powershell.exe', CreationDate: '2026-10-03T00:00:00.000Z', Birth: '133800000000000000' },
    ...(processes.get(`qa:role${pid - 7000}`) ?? []).map(row => ({
      ProcessId: row.pid, ParentProcessId: pid, ExecutablePath: row.argv[0],
      Birth: '133800000010000000',
      CommandLine: row.argv.map(value => `"${value}"`).join(' '), CreationDate: '2026-10-03T00:00:01.000Z',
    }))] };
  const controller = new FixedDagPane(options);
  console.log(JSON.stringify({ event: 'resource-planned', pid: process.pid, controlPipe: controller.controlPath }));
  const controllers = [controller];
  t.after(async () => {
    for (const owner of controllers) await owner.stop();
    await rm(directory, { recursive: true, force: true });
    console.log(JSON.stringify({ event: 'resource-cleaned', pid: process.pid, directory,
      controlPipes: controllers.map(owner => owner.controlPath) }));
  });
  return { directory, calls, warnings, events, panes, processes, options, controller, controllers,
    get focused() { return focused; }, set focused(value) { focused = value; } };
}
for (const [empty, sameSession] of [[false, false], [true, false], [true, true]])
  test(`task5 retained fixed replacement resolves its executable before launch (${empty ? 'empty' : 'live'}, ${sameSession ? 'resume' : 'fresh'})`, async t => {
  const f = await setup(t);
  await f.controller.start();
  await f.controller.openPair();
  const before = f.calls.length;
  const old = JSON.parse(await readFile(f.controller.paneFile('dag'), 'utf8'));
  let resolutions = 0, retired = false;
  f.controller.node = async () => { resolutions++; return f.options.node; };
  f.controller.coordinator = {
    actor: {},
    async retireRetainedRole(retained, prepare) {
      assert.equal(retained.record.paneId, old.paneId);
      await prepare();
      retired = true;
    },
  };
  const herdr = f.controller.herdr;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'run') {
      assert.equal(retired, true);
      const argv = [...args[2].matchAll(/'((?:''|[^'])*)'/g)].map(match => match[1].replaceAll("''", "'"));
      assert.equal(argv[0], f.options.node);
    }
    return herdr(...args);
  };
  await f.controller.replaceRetained([{ state: 'retained', empty,
    record: { ...old, sessionId: sameSession ? f.controller.sessionId : 'prior-conversation' } }]);
  assert.equal(resolutions, 1);
  const current = JSON.parse(await readFile(f.controller.paneFile('dag'), 'utf8'));
  assert.equal(current.nodePath, f.options.node);
  assert.equal(current.paneId, old.paneId);
  assert.notEqual(current.launchToken, old.launchToken);
  assert.equal(f.calls.slice(before).filter(args => args[0] === 'split').length, 0);
});

for (const fail of [false, true]) test(`task30 launch joins its authenticated registration job (${fail ? 'failure' : 'success'})`, async t => {
  const f = await setup(t);
  await f.controller.start();
  await f.controller.openPair();
  const record = JSON.parse(await readFile(f.controller.paneFile('workers'), 'utf8'));
  const release = Promise.withResolvers();
  let registered, viewerHeld;
  const error = Object.assign(new Error('Registration refused'), { code: 'REGISTRATION_REFUSED' });
  f.controller.managedAvailable = true;
  f.controller.launchRegistrations = new Map();
  const get = f.controller.launchRegistrations.get.bind(f.controller.launchRegistrations);
  f.controller.launchRegistrations.get = token => {
    const job = get(token);
    assert.ok(job instanceof Promise);
    f.events.emit('registration-joined');
    return job;
  };
  f.controller.coordinator = {
    actor: { blocked: false },
    async mutate(role, type, target, args, effect) {
      const result = await f.options.herdr(...args);
      await effect;
      f.events.emit('run-settled');
      return result;
    },
    async registerRole(role, readTarget) {
      const target = await readTarget();
      f.events.emit('registration-held');
      await release.promise;
      if (fail) throw error;
      registered = target;
    },
    async holdRoleViewer(role, current) {
      assert.equal(role, 'workers');
      viewerHeld = current;
    },
  };
  const signal = AbortSignal.timeout(3000);
  const held = once(f.events, 'registration-held', { signal });
  const settled = once(f.events, 'run-settled', { signal });
  const joined = once(f.events, 'registration-joined', { signal });
  const launch = f.controller.launchRole(record);
  launch.catch(() => {});
  try {
    await held;
    await settled;
    assert.equal(await Promise.race([joined.then(() => 'joined'), launch.then(() => 'exposed')]), 'joined');
    assert.equal(registered, undefined);
    assert.equal(viewerHeld, undefined);
    release.resolve();
    if (fail) {
      await assert.rejects(launch, { code: error.code });
      assert.equal(viewerHeld, undefined);
    }
    else {
      const current = await launch;
      assert.equal(current.paneId, registered.paneId);
      assert.equal(current.processId, registered.viewer.pid);
      assert.equal(registered.shell.pid, record.creationProcessId);
      assert.equal(viewerHeld.paneId, current.paneId);
      assert.equal(viewerHeld.processId, current.processId);
      assert.equal(viewerHeld.launchToken, record.launchToken);
    }
    assert.equal(f.controller.launchRegistrations.has(record.launchToken), false);
  } finally {
    release.resolve();
    await Promise.allSettled([launch, ...f.controller.controlJobs]);
    f.controller.managedAvailable = false;
  }
});

for (const selected of ['qa:parent', 'qa:role2']) test(`owned reopen preserves selected input pane ${selected}`, async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('dag'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6001); await closed; f.focused = selected;
  const original = f.controller.herdr;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'neighbor') return { neighbor: { neighbor_pane_id: selected, layout: { tab_id: 'qa:tab', focused_pane_id: f.focused } } };
    if (args[0] === 'focus') { f.calls.push(args); f.focused = selected; return {}; }
    return original(...args);
  };
  await f.controller.openPair({ force: true });
  assert.equal(f.focused, selected, 'swap must not change foreground input target');
});
for (const moment of ['split', 'swap', 'neighbor']) test(`later manual selection after ${moment} is never overwritten`, async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('dag'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6001); await closed;
  const original = f.controller.herdr;
  f.controller.herdr = async (...args) => { const result = await original(...args); if (args[0] === moment) f.focused = 'qa:role2'; return result; };
  await f.controller.openPair({ force: true }).catch(error => assert.match(error.message, /layout-waiting/));
  assert.equal(f.focused, 'qa:role2', 'late manual selection is authoritative');
  assert.equal(f.calls.filter(args => args[0] === 'focus').length, 0);
  if (moment === 'split') assert.equal(f.calls.filter(args => args[0] === 'swap').length, 0);
});
test('inactive selected tab defers before any split or swap', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('dag'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6001); await closed;
  f.panes.set('foreign:selected', { pane_id: 'foreign:selected', tab_id: 'foreign:tab' }); f.focused = 'foreign:selected';
  const count = f.calls.filter(args => ['split', 'swap', 'focus'].includes(args[0])).length;
  await assert.rejects(f.controller.openPair({ force: true }), /selected owned tab/);
  assert.equal(f.calls.filter(args => ['split', 'swap', 'focus'].includes(args[0])).length, count);
  assert.equal(f.focused, 'foreign:selected');
  f.focused = 'qa:parent'; await f.controller.openPair({ force: true });
  assert.equal(f.focused, 'qa:parent');
});

test('initial source save precedes launches; concurrent opens create exactly one fixed pair', async t => {
  const f = await setup(t);
  await f.controller.start();
  await f.controller.provider().sourceReady;
  assert.equal(f.calls.length, 0);
  assert.deepEqual(JSON.parse(await readFile(f.controller.stateFile, 'utf8')).runs, []);
  await Promise.all(Array.from({ length: 5 }, () => f.controller.openPair()));
  const splits = f.calls.filter(call => call[0] === 'split');
  assert.equal(splits.length, 2);
  assert.equal(splits[0][splits[0].indexOf('--direction') + 1], 'right');
  assert.equal(splits[1][splits[1].indexOf('--direction') + 1], 'down');
  assert.equal(splits[1][splits[1].indexOf('--ratio') + 1], '0.4');
  assert.ok(splits.every(call => call.includes('--no-focus')));
  assert.ok(f.calls.filter(call => call[0] === 'run').some(call => call[2].includes("'--view' 'dag'")));
  assert.ok(f.calls.filter(call => call[0] === 'run').some(call => call[2].includes("'--view' 'tasks'")));
  assert.equal(f.calls.filter(call => ['close', 'resize', 'focus'].includes(call[0])).length, 0);
});

test('late offers serialize existing role writes without launching or emitting source revisions', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const before = f.calls.filter(call => call[0] === 'run').length;
  const sourceEvents = [];
  const off = f.controller.provider().subscribeSource(source => sourceEvents.push(source));
  const offers = Object.fromEntries(['dag', 'workers'].map(role => [role, { pipePath: '\\\\.\\pipe\\synthetic-presence',
    v: 1, token: 'presence-token', ownerKey: f.controller.key, sessionId, scopeEpoch: 2, role, sequence: 0 }]));
  await f.controller.setOffers(offers);
  off();
  for (const role of ['dag', 'workers']) {
    const state = JSON.parse(await readFile(f.controller.roleFile(role), 'utf8'));
    assert.deepEqual(state.presenceOffer, offers[role]);
    assert.equal(state.scopeEpoch, 2);
  }
  assert.equal(sourceEvents.length, 0);
  assert.equal(f.calls.filter(call => call[0] === 'run').length, before);
  await assert.rejects(f.controller.setOffers({ ...offers, dag: { ...offers.dag, scopeEpoch: 1 } }), /Stale/);
});

test('reported task metrics and DAG topology survive fixed-role source updates', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  await f.controller.receive(payload());
  await f.controller.receiveTasks({ parent_session_id: sessionId, tasks: [
    { task_id: 'st_ordinary', description: 'Ordinary', status: 'running', updated_at: '2026-09-01T00:00:00Z',
      run_stats: { output_tokens: 28087, generation_ms: 72394, tokens_per_second: 388, cost_usd: 0, cost_status: 'reported' } },
  ] });
  for (const role of ['dag', 'workers']) {
    const state = JSON.parse(await readFile(f.controller.roleFile(role), 'utf8'));
    assert.equal(state.runs[0].nodes.length, payload().runs[0].nodes.length);
    assert.equal(state.runs[0].edges.length, payload().runs[0].edges.length);
    assert.equal(state.tasks.find(task => task.id === 'st_ordinary').metrics.tokens_per_second, 388);
    assert.equal(state.tasks.find(task => task.id === 'st_ordinary').metrics.cost_usd, 0);
  }
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
});

test('manual workers close is durable, source updates do not reopen, explicit pair open reuses DAG', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('workers'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  assert.equal((await control('close', state, 6002)).type, 'closed');
  await closed;
  await f.controller.receive(payload());
  await f.controller.openPair();
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
  assert.equal(JSON.parse(await readFile(f.controller.paneFile('workers'), 'utf8')).manualClose, true);
  await f.controller.openPair({ force: true });
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 3);
  assert.equal((await f.controller.inspectPair()).dag.paneId, 'qa:role1');
});

test('a cancelled workers reopen does not inherit a surviving DAG reopen request', async t => {
  const f = await setup(t);
  await f.controller.start(); await f.controller.openPair();
  for (const [role, pid] of [['dag', 6001], ['workers', 6002]]) {
    const state = JSON.parse(await readFile(f.controller.roleFile(role), 'utf8'));
    const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
    assert.equal((await control('close', state, pid)).type, 'closed');
    await closed;
  }
  const runs = f.calls.filter(call => call[0] === 'run').length;
  const reopened = await f.controller.openPair({ force: true, forceRoles: ['dag'] });
  assert.ok(reopened.dag);
  assert.equal(reopened.workers, undefined);
  assert.equal(f.calls.filter(call => call[0] === 'run').length, runs + 1);
  assert.equal(JSON.parse(await readFile(f.controller.paneFile('workers'), 'utf8')).manualClose, true);
});

for (const role of ['dag', 'workers']) test(`task5 queued forced ${role} recovery yields to a later authenticated q`, { timeout: 7000 }, async t => {
  const f = await setup(t);
  await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile(role), 'utf8'));
  const splitCount = f.calls.filter(call => call[0] === 'split').length;
  const release = Promise.withResolvers();
  const entered = once(f.events, 'recovery-queue-held', { signal: AbortSignal.timeout(3000) });
  const blocked = f.controller.enqueue(async () => {
    f.events.emit('recovery-queue-held'); await release.promise;
  });
  await entered;
  const recovering = f.controller.openPair({ force: true, forceRoles: [role] });
  recovering.catch(() => {});
  try {
    const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
    assert.equal((await control('close', state, role === 'dag' ? 6001 : 6002)).type, 'closed');
    await closed;
  } finally { release.resolve(); }
  await blocked; await recovering;
  assert.equal(f.calls.filter(call => call[0] === 'split').length, splitCount);
  assert.equal(JSON.parse(await readFile(f.controller.paneFile(role), 'utf8')).manualClose, true);
  await f.controller.openPair({ force: true, forceRoles: [role] });
  assert.equal(f.calls.filter(call => call[0] === 'split').length, splitCount + 1);
});

test('workers reopen splits the verified surviving Todo region and places workers before Todo', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const todo = { ...f.controller.binding, role: 'todo', paneId: 'qa:todo', tabId: 'qa:tab', scopeEpoch: 1, ready: true, manualClose: false };
  f.panes.set(todo.paneId, { pane_id: todo.paneId, tab_id: todo.tabId });
  f.controller.coordinator = { binding: f.controller.binding, lifetimeId: 'owned', inspectTodo: async () => todo };
  const state = JSON.parse(await readFile(f.controller.roleFile('workers'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6002); await closed;
  await f.controller.openPair({ force: true });
  const split = f.calls.filter(call => call[0] === 'split').at(-1);
  assert.equal(split[split.indexOf('--pane') + 1], todo.paneId);
  assert.equal(split[split.indexOf('--ratio') + 1], '0.5');
  assert.deepEqual(f.calls.find(call => call[0] === 'swap'), ['swap', '--source-pane', 'qa:role3', '--target-pane', todo.paneId]);
});

test('DAG reopen splits surviving workers before them without a second right column', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('dag'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6001); await closed;
  await f.controller.openPair({ force: true });
  const split = f.calls.filter(call => call[0] === 'split').at(-1);
  assert.equal(split[split.indexOf('--pane') + 1], 'qa:role2');
  assert.equal(split[split.indexOf('--direction') + 1], 'down');
  assert.deepEqual(f.calls.find(call => call[0] === 'swap'), ['swap', '--source-pane', 'qa:role3', '--target-pane', 'qa:role2']);
});

test('both fixed roles reopen inside surviving Todo without changing its pane identity', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const todo = { ...f.controller.binding, role: 'todo', paneId: 'qa:todo', tabId: 'qa:tab', scopeEpoch: 1, ready: true, manualClose: false };
  f.panes.set(todo.paneId, { pane_id: todo.paneId, tab_id: todo.tabId });
  f.controller.coordinator = { binding: f.controller.binding, lifetimeId: 'owned', inspectTodo: async () => todo };
  for (const [role, pid] of [['dag', 6001], ['workers', 6002]]) {
    const state = JSON.parse(await readFile(f.controller.roleFile(role), 'utf8'));
    const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
    await control('close', state, pid); await closed;
  }
  await f.controller.openPair({ force: true });
  const splits = f.calls.filter(call => call[0] === 'split').slice(2);
  assert.deepEqual(splits.map(call => call[call.indexOf('--pane') + 1]), [todo.paneId, todo.paneId]);
  assert.deepEqual(splits.map(call => call[call.indexOf('--ratio') + 1]), ['0.4', '0.5']);
  assert.equal(f.panes.has(todo.paneId), true);
});

test('DAG reopen restores the actually observed 23/23 collapsed owned region before splitting', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const todo = { ...f.controller.binding, role: 'todo', paneId: 'qa:todo', tabId: 'qa:tab', scopeEpoch: 1, ready: true, manualClose: false };
  f.panes.set(todo.paneId, { pane_id: todo.paneId, tab_id: todo.tabId });
  f.controller.coordinator = { binding: f.controller.binding, lifetimeId: 'owned', inspectTodo: async () => todo };
  const state = JSON.parse(await readFile(f.controller.roleFile('dag'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6001); await closed;
  // Exact public Herdr rects from the real 18/14/14 -> 23/23 collapse probe.
  const rects = [
    { pane_id: 'qa:parent', rect: { x: 0, y: 0, width: 112, height: 46 } },
    { pane_id: 'qa:role2', rect: { x: 112, y: 0, width: 60, height: 23 } },
    { pane_id: 'qa:todo', rect: { x: 112, y: 23, width: 60, height: 23 } },
  ];
  const originalHerdr = f.controller.herdr;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'layout' && !f.panes.has('qa:role3')) { f.calls.push(args); return { layout: {
      tab_id: 'qa:tab', focused_pane_id: f.focused, panes: structuredClone(rects) } }; }
    if (args[0] === 'resize') {
      rects[1].rect.height = 32; rects[2].rect.y = 32; rects[2].rect.height = 14;
    }
    return originalHerdr(...args);
  };
  const start = f.calls.length;
  await f.controller.openPair({ force: true });
  const actions = f.calls.slice(start).filter(call => ['resize', 'split', 'swap'].includes(call[0]));
  assert.equal(actions[0]?.[0], 'resize', 'Collapsed 23/23 region must become 32/14 before inserting the DAG');
  assert.equal(actions[0][actions[0].indexOf('--pane') + 1], 'qa:role2');
  assert.equal(actions[0][actions[0].indexOf('--direction') + 1], 'down');
  assert.equal(Number(actions[0][actions[0].indexOf('--amount') + 1]), 9 / 46);
  assert.equal(actions[1][0], 'split'); assert.equal(actions[2][0], 'swap');
  assert.equal(f.panes.has(todo.paneId), true);
});

for (const mismatch of ['ownerKey', 'sessionId', 'scopeEpoch', 'role', 'tabId', 'ready']) test(`reopen rejects a ${mismatch} mismatch in the Todo anchor before pane changes`, async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const todo = { ...f.controller.binding, role: 'todo', paneId: 'qa:todo', tabId: 'qa:tab', scopeEpoch: 1, ready: true, manualClose: false, [mismatch]: 'foreign' };
  f.controller.coordinator = { binding: f.controller.binding, lifetimeId: 'owned', inspectTodo: async () => todo };
  const before = f.calls.filter(call => ['split', 'swap', 'close', 'run'].includes(call[0])).length;
  await assert.rejects(f.controller.openPair({ force: true }), /layout-waiting/);
  assert.equal(f.calls.filter(call => ['split', 'swap', 'close', 'run'].includes(call[0])).length, before);
});

test('a concurrent fixed-pair adoption cannot overwrite the workers manual close', { timeout: 7000 }, async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('workers'), 'utf8'));
  const entered = new EventEmitter();
  let resolve;
  const release = new Promise(done => { resolve = done; });
  const original = f.controller.herdr;
  let paused = false;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'process-info' && args[2] === state.paneId && !paused) {
      paused = true; entered.emit('adoption'); await release;
    }
    return original(...args);
  };
  const adopting = once(entered, 'adoption', { signal: AbortSignal.timeout(2000) });
  const updating = f.controller.openPair();
  await adopting;
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(2000) });
  try {
    assert.equal((await control('close', state, 6002)).type, 'closed');
    await closed;
  } finally { resolve(); }
  await updating;
  await f.controller.receive(payload());
  assert.equal(JSON.parse(await readFile(f.controller.paneFile('workers'), 'utf8')).manualClose, true);
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
});

for (const kind of ['legacy', 'foreign', 'ambiguous']) test(`${kind} ownership does not adopt, close or split`, async t => {
  const f = await setup(t); await f.controller.start();
  if (kind === 'legacy') await writeJson(f.controller.legacyRecordFile, { paneId: 'qa:legacy', ready: true });
  if (kind === 'foreign') await writeJson(f.controller.paneFile('dag'), { role: 'dag', ownerKey: 'foreign' });
  if (kind === 'ambiguous') f.panes.set('alias', { pane_id: 'qa:parent', tab_id: 'foreign:tab' });
  if (kind === 'ambiguous') await assert.rejects(f.controller.openPair(), /ambiguous/);
  else await f.controller.openPair();
  assert.equal(f.calls.filter(call => ['split', 'close'].includes(call[0])).length, 0);
});

test('failed launch records its exact attempt and does not spawn another orphan on source or open', async t => {
  const f = await setup(t);
  const original = f.controller.herdr;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'run') throw new Error('launch failed');
    return original(...args);
  };
  await f.controller.start();
  await assert.rejects(f.controller.openPair(), /launch failed/);
  await f.controller.receive(payload());
  await f.controller.openPair();
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 1);
  assert.equal(f.calls.filter(call => call[0] === 'close').length, 1);
  assert.equal(f.panes.size, 1, 'failed launch must leave no owned orphan pane');
});

for (const kind of ['foreign-child', 'foreign-sibling', 'missing-shell', 'query-failure', 'report-failure', 'owned-child'])
test(`failed launch ${kind} uses complete platform process observation for cleanup`, async t => {
  const f = await setup(t), original = f.controller.herdr, inspect = f.options.inspectShell;
  const launchError = new Error('launch failed'), observationError = new Error('CIM failed');
  let launched = false, argv;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'run') {
      argv = [...args[2].matchAll(/'((?:''|[^'])*)'/g)].map(match => match[1].replaceAll("''", "'"));
      launched = true; throw launchError;
    }
    if (args[0] === 'process-info' && launched && kind === 'report-failure') throw observationError;
    if (args[0] === 'process-info' && launched && process.platform !== 'win32') {
      const { process_info } = await original(...args);
      const rows = await f.controller.inspectShell(process_info.shell_pid);
      if (!rows.some(row => row.ProcessId === process_info.shell_pid))
        return { process_info: { ...process_info, shell_pid: null } };
      return { process_info: { ...process_info, foreground_processes: rows
        .filter(row => row.ProcessId !== process_info.shell_pid)
        .map(row => ({ pid: row.ProcessId, argv: parseLaunchCommand(row.CommandLine) })) } };
    }
    return original(...args);
  };
  f.controller.inspectShell = async pid => {
    const rows = await inspect(pid);
    if (!launched) return rows;
    if (kind === 'query-failure') throw observationError;
    if (kind === 'missing-shell') return [];
    const owned = { ProcessId: 6001, ParentProcessId: pid, ExecutablePath: argv[0],
      CommandLine: argv.map(value => `"${value}"`).join(' '), CreationDate: '2026-10-03T00:00:01.000Z' };
    const foreign = { ...owned, ProcessId: 8100, ExecutablePath: 'foreign.exe', CommandLine: 'foreign.exe' };
    return [...rows, ...(kind === 'foreign-child' ? [foreign] : kind === 'foreign-sibling' ? [owned, foreign] : [owned])];
  };
  await f.controller.start();
  await assert.rejects(f.controller.openPair(), error => {
    assert.equal(error, launchError);
    if (kind === 'report-failure' || kind === 'query-failure' && process.platform !== 'win32')
      assert.equal(error.cause, observationError);
    return true;
  });
  assert.equal(f.calls.filter(call => call[0] === 'close').length, kind === 'owned-child' ? 1 : 0);
  assert.equal(f.panes.size, kind === 'owned-child' ? 1 : 2);
});

test('platform child registration supports pair reuse and authenticated manual close', async t => {
  const f = await setup(t), original = f.controller.herdr;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'process-info' && process.platform === 'win32') {
      const pid = 7000 + Number(args[2].replace('qa:role', ''));
      return { process_info: { shell_pid: pid, foreground_processes: [{ pid, argv: ['powershell.exe'] }] } };
    }
    return original(...args);
  };
  await f.controller.start(); await f.controller.openPair(); await f.controller.openPair();
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
  const state = JSON.parse(await readFile(f.controller.roleFile('workers'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(2000) });
  assert.equal((await control('close', state, 6002)).type, 'closed');
  await closed;
  assert.equal(JSON.parse(await readFile(f.controller.paneFile('workers'), 'utf8')).manualClose, true);
});

test('reload verifies the old process, refreshes lifetime offers and never changes existing dimensions', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair(); await f.controller.stop();
  const next = new FixedDagPane({ ...f.options, scopeEpoch: 2 });
  console.log(JSON.stringify({ event: 'resource-planned', pid: process.pid, controlPipe: next.controlPath }));
  f.controllers.push(next);
  await next.start(); await next.openPair();
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
  assert.equal(f.calls.filter(call => call[0] === 'run').length, 2);
  assert.equal(f.calls.filter(call => ['resize', 'close', 'focus'].includes(call[0])).length, 0);
  assert.equal(JSON.parse(await readFile(next.roleFile('dag'), 'utf8')).scopeEpoch, 2);
  const before = await readFile(next.roleFile('dag'), 'utf8');
  await f.controller.receive(payload());
  assert.equal(await readFile(next.roleFile('dag'), 'utf8'), before);
});

for (const authority of ['no-actor', 'lost-lease', 'foreign-participant'])
  test(`task33 fixed ${authority} teardown preserves source and retained role files`, async t => {
    const f = await setup(t);
    await f.controller.start(); await f.controller.openPair();
    const paths = [f.controller.stateFile, ...['dag', 'workers'].flatMap(role =>
      [f.controller.roleFile(role), f.controller.paneFile(role)])];
    const before = await Promise.all(paths.map(path => readFile(path)));
    f.controller.managedAvailable = true;
    let drained = 0;
    if (authority !== 'no-actor') {
      const actor = { leaseHeld: authority !== 'lost-lease',
        snapshot: { participants: { dag: { lifetimeId: authority === 'foreign-participant'
          ? 'foreign-lifetime' : f.controller.lifetimeId } } },
        async drain() { drained++; } };
      f.controller.managedActor = actor;
      f.controller.coordinator = { actor, binding: f.controller.binding, ownsPublisherClaim: () => true,
        terminalAuthority: async () => { throw new Error('Unqualified terminal authority reached'); } };
    }
    await f.controller.stop();
    assert.deepEqual(await Promise.all(paths.map(path => readFile(path))), before);
    assert.equal(drained, 0);
  });

test('task33 fixed accepted owner publication remains connected to exact participant drain', async t => {
  const f = await setup(t);
  await f.controller.start(); await f.controller.openPair();
  f.controller.managedAvailable = true;
  let observed = 0, drained = 0;
  const actor = { leaseHeld: true,
    snapshot: { ownerNonce: 'logical-current-owner', generation: 1, sessionId: f.controller.sessionId,
      participants: { dag: { lifetimeId: f.controller.lifetimeId } } },
    async drain(participant, lifetimeId) {
      assert.equal(participant, 'dag'); assert.equal(lifetimeId, f.controller.lifetimeId);
      drained++;
    } };
  f.controller.managedActor = actor;
  // This checks the delegated contract only. W's real actor/terminalAuthority
  // integration has its own logical positive and the required native case.
  f.controller.coordinator = { actor, binding: f.controller.binding, ownsPublisherClaim: () => true,
    terminalAuthority: async (participant, lifetimeId, records) => {
      assert.equal(participant, 'dag'); assert.equal(lifetimeId, f.controller.lifetimeId);
      assert.equal(records.length, 2);
      for (const record of records) {
        const target = await f.controller.nativeTarget(record);
        assert.equal(target.paneId, record.paneId);
        assert.equal(target.viewer.pid, record.processId);
      }
      observed++;
      return true;
    } };
  await f.controller.stop();
  assert.equal(observed, 1); assert.equal(drained, 1);
  assert.equal(JSON.parse(await readFile(f.controller.stateFile, 'utf8')).connected, false);
  for (const role of ['dag', 'workers']) {
    const state = JSON.parse(await readFile(f.controller.roleFile(role), 'utf8'));
    assert.equal(state.connected, false); assert.equal(state.controlOffer, null); assert.equal(state.presenceOffer, null);
  }
});

test('failed initial source save rejects readiness and cannot authorize a fixed pair launch', async t => {
  const f = await setup(t);
  await writeFile(f.controller.directory, 'blocked directory');
  await assert.rejects(f.controller.start());
  await assert.rejects(f.controller.provider().sourceReady);
  await assert.rejects(f.controller.stop());
  assert.equal(f.controller.control.listening, false);
  assert.equal(f.calls.filter(call => ['split', 'run'].includes(call[0])).length, 0);
});

for (const kind of ['absent', 'known', 'busy']) test(`task3 canonical managed slot ${kind} controls standalone admission`, async t => {
  const f = await setup(t);
  const stateDir = join(f.directory, 'managed');
  await mkdir(stateDir);
  const socketPath = join(f.directory, 'socket');
  await writeFile(socketPath, '');
  const canonicalState = stateDir.replaceAll('\\', '/').toLowerCase();
  const canonicalSocket = socketPath.replaceAll('\\', '/').toLowerCase();
  const key = createHash('sha256').update(JSON.stringify([canonicalSocket, 'qa:parent'])).digest('hex');
  const slotDir = join(canonicalState, 'parent-slots', key);
  let db;
  if (kind !== 'absent') {
    await mkdir(slotDir, { recursive: true });
    if (kind === 'known') await writeFile(join(slotDir, 'slot.json'), '{"schema":"unknown-but-occupied"}');
    else {
      db = new DatabaseSync(join(slotDir, 'lease.sqlite'), { timeout: 0 });
      db.exec('BEGIN EXCLUSIVE');
    }
  }
  try {
    const result = await fixedController.probeManagedSlot({ stateDir, socketPath, parentPaneId: 'qa:parent' });
    assert.equal(result.blocked, kind !== 'absent');
    if (kind === 'busy') assert.equal(result.code, 'SQLITE_BUSY');
    f.controller.managedStateDir = stateDir;
    f.controller.binding.socketPath = socketPath;
    await f.controller.start();
    if (kind === 'absent') {
      await f.controller.openPair();
      assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
    } else {
      await assert.rejects(f.controller.openPair(), /BLOCKED/);
      assert.equal(f.controller.status, 'BLOCKED');
      assert.equal(f.calls.filter(call => ['split', 'run', 'close', 'swap', 'resize'].includes(call[0])).length, 0);
    }
  } finally { db?.close(); }
});

test('task3 managed availability remains blocking after the W coordinator disappears', async t => {
  const f = await setup(t);
  f.controller.managedAvailable = true;
  await f.controller.start();
  await assert.rejects(f.controller.open(), /BLOCKED/);
  assert.equal(f.controller.status, 'BLOCKED');
  assert.equal(f.calls.filter(call => ['split', 'run', 'close'].includes(call[0])).length, 0);
});

test('task20 managed arrange forwards its exact target, arguments and intended geometry to W', async t => {
  const f = await setup(t);
  const target = { paneId: 'qa:parent', tabId: 'qa:tab', shell: { pid: 7000, birth: '133800000000000000' } };
  const args = ['resize', '--pane', target.paneId, '--direction', 'right', '--amount', '.1'];
  const effect = { paneId: target.paneId, rect: { x: 0, y: 0, width: 60, height: 40 } };
  const events = new EventEmitter();
  let finish;
  const completion = new Promise(resolve => { finish = resolve; });
  const coordinator = { actor: {}, target: async paneId => {
    assert.equal(paneId, target.paneId);
    return target;
  }, mutate(role, type, observed, submitted, expected) {
    assert.equal(this, coordinator);
    assert.equal(role, 'dag');
    assert.equal(type, 'arrange');
    assert.equal(observed, target);
    assert.equal(submitted, args);
    assert.equal(expected, effect);
    events.emit('forwarded');
    return completion;
  } };
  f.controller.managedAvailable = true;
  f.controller.coordinator = coordinator;
  const forwarded = once(events, 'forwarded', { signal: AbortSignal.timeout(2000) });
  const result = f.controller.mutate('dag', 'arrange', target.paneId, args, effect);
  await forwarded;
  assert.equal(f.calls.filter(call => ['resize', 'layout'].includes(call[0])).length, 0);
  const receipt = { type: 'pane_resize', resize: { changed: true } };
  finish(receipt);
  assert.equal(await result, receipt);
});

async function placementFixture(t) {
  const f = await setup(t);
  await f.controller.start(); await f.controller.openPair();
  const ids = { parent: 'qa:parent', dag: 'qa:role1', workers: 'qa:role2', todo: 'qa:todo' };
  const todo = { ...f.controller.binding, role: 'todo', paneId: ids.todo, tabId: 'qa:tab',
    scopeEpoch: 1, ready: true, manualClose: false };
  f.panes.set(ids.todo, { pane_id: ids.todo, tab_id: 'qa:tab' });
  let tree = { direction: 'right', ratio: .6, first: ids.parent, second: {
    direction: 'right', ratio: .5, first: {
      direction: 'down', ratio: .5, first: ids.workers, second: ids.todo }, second: ids.dag } };
  const visit = (node, rect, panes = []) => {
    if (typeof node === 'string') { panes.push({ pane_id: node, rect }); return panes; }
    const axis = node.direction === 'right' ? 'width' : 'height';
    const offset = node.direction === 'right' ? 'x' : 'y';
    const size = Math.round(rect[axis] * node.ratio);
    visit(node.first, { ...rect, [axis]: size }, panes);
    visit(node.second, { ...rect, [offset]: rect[offset] + size, [axis]: rect[axis] - size }, panes);
    return panes;
  };
  const replace = (node, id, change) => typeof node === 'string' ? node === id ? change(node) : node :
    { ...node, first: replace(node.first, id, change), second: replace(node.second, id, change) };
  const remove = (node, id) => {
    if (typeof node === 'string') return node === id ? null : node;
    const first = remove(node.first, id), second = remove(node.second, id);
    return first === null ? second : second === null ? first : { ...node, first, second };
  };
  const layout = () => ({ workspace_id: 'qa', tab_id: 'qa:tab', area: { x: 0, y: 0, width: 100, height: 40 },
    focused_pane_id: f.focused, zoomed: false, panes: visit(tree, { x: 0, y: 0, width: 100, height: 40 }) });
  const targets = new Map(Object.values(ids).map((paneId, index) => [paneId, { paneId, tabId: 'qa:tab',
    shell: { pid: 8100 + index, birth: '133800000000000000' } }]));
  let number = 0;
  const original = f.controller.herdr;
  const actions = [];
  f.controller.herdr = async (...args) => {
    const option = key => args[args.indexOf(key) + 1];
    if (args[0] === 'layout') return { layout: layout() };
    if (args[0] === 'list') return { panes: [...f.panes.values()].map(pane => ({ ...pane, focused: pane.pane_id === f.focused })) };
    if (args[0] === 'move') { actions.push(args); return { type: 'pane_move', move_result: { changed: false, reason: 'same_tab' } }; }
    if (args[0] === 'split') {
      actions.push(args);
      const paneId = 'qa:temp' + ++number;
      f.panes.set(paneId, { pane_id: paneId, tab_id: 'qa:tab' });
      targets.set(paneId, { paneId, tabId: 'qa:tab', shell: { pid: 8200 + number, birth: '133800000000000000' } });
      tree = replace(tree, option('--pane'), id => ({ direction: option('--direction'), ratio: Number(option('--ratio') ?? .5),
        first: id, second: paneId }));
      return { pane: { pane_id: paneId } };
    }
    if (args[0] === 'swap') {
      actions.push(args);
      const a = option('--source-pane'), b = option('--target-pane');
      tree = replace(replace(replace(tree, a, () => 'temporary-marker'), b, () => a), 'temporary-marker', () => b);
      f.focused = a; return { type: 'pane_swap', swap: { changed: true, source_pane_id: a, target_pane_id: b, layout: layout() } };
    }
    if (args[0] === 'close' && targets.has(args[1])) {
      actions.push(args); tree = remove(tree, args[1]); f.panes.delete(args[1]); return {};
    }
    if (args[0] === 'resize') {
      actions.push(args);
      assert.equal(option('--pane'), ids.parent);
      tree.ratio = Number(tree.ratio) + (option('--direction') === 'left' ? -1 : 1) * Number(option('--amount'));
      return { type: 'pane_resize', resize: { changed: true, layout: layout() } };
    }
    return original(...args);
  };
  f.controller.managedAvailable = true;
  f.controller.claim = { ...f.controller.binding, tabId: 'qa:tab', layout: 'columns' };
  f.controller.nativeTarget = async record => targets.get(record.paneId);
  f.controller.restorePlacementFocus = async (_created, selected) => { f.focused = selected; };
  const actor = { leaseHeld: true, blocked: false, draining: false,
    snapshot: { ownerNonce: 'owned-nonce', generation: 1, operations: [] } };
  let afterMutation;
  f.controller.coordinator = { actor,
    lifetimeId: 'w-owned', binding: f.controller.binding, inspectTodo: async () => todo,
    assertCurrent() {},
    target: async id => {
      if (!f.panes.has(id)) throw new Error('Unknown native target');
      return structuredClone(targets.get(id));
    },
    mutate: async (_role, type, target, args) => {
      const result = await f.controller.herdr(...args);
      await afterMutation?.({ type, target, args, result });
      return result;
    } };
  return { ...f, ids, actions, targets, layout, actor,
    set afterMutation(value) { afterMutation = value; },
    addForeign() {
      const paneId = 'qa:foreign';
      f.panes.set(paneId, { pane_id: paneId, tab_id: 'qa:tab' });
      tree = { direction: 'right', ratio: .8, first: tree, second: paneId };
    } };
}

test('task22 conditional managed placement uses supported composition and retains live identities', async t => {
  const f = await placementFixture(t), before = structuredClone([...f.targets]);
  await f.controller.arrangeDashboard();
  assert.equal(fixedController.isColumnDashboard(f.layout(), f.ids), true);
  assert.equal(f.actions.filter(args => args[0] === 'move').length, 0);
  assert.equal(f.actions.filter(args => args[0] === 'split').length, 3);
  assert.equal(f.actions.filter(args => args[0] === 'close').length, 3);
  assert.equal([...f.panes.keys()].some(id => id.startsWith('qa:temp')), false);
  for (const [id, identity] of before) assert.deepEqual(f.targets.get(id), identity);
  const journal = JSON.parse(await readFile(join(f.controller.directory, 'placement.json'), 'utf8'));
  assert.equal(journal.phase, 'COMPLETE');
  assert.equal(journal.temporary, null);
  const actions = f.actions.length;
  await f.controller.arrangeDashboard();
  assert.equal(f.actions.length, actions);
});

test('task22 foreign region prevents any managed placement action', async t => {
  const f = await placementFixture(t);
  f.addForeign();
  await assert.rejects(f.controller.arrangeDashboard(), /unowned panes/);
  assert.equal(f.actions.length, 0);
});

test('task22 unknown applied split stays journaled and is never replayed', async t => {
  const f = await placementFixture(t);
  f.afterMutation = ({ type }) => {
    if (type === 'split') {
      f.actor.blocked = true;
      f.actor.snapshot.operations.push({ id: 'split-unknown', type: 'split', status: 'UNKNOWN' });
      throw new Error('lost split reply');
    }
  };
  await assert.rejects(f.controller.arrangeDashboard(), /lost split reply/);
  const journal = JSON.parse(await readFile(join(f.controller.directory, 'placement.json'), 'utf8'));
  assert.equal(journal.phase, 'BLOCKED');
  assert.equal(journal.operations[0].status, 'UNKNOWN');
  assert.equal(journal.beforeLayout.panes.length, 4);
  assert.equal(journal.failureLayout.panes.length, 5);
  assert.equal(f.actions.length, 1);
  await assert.rejects(f.controller.arrangeDashboard(), /unowned panes/);
  assert.equal(f.actions.length, 1);
});

test('task22 interrupted settled temporary split closes only its exact empty shell', async t => {
  const f = await placementFixture(t);
  const original = f.controller.coordinator.mutate;
  f.controller.coordinator.mutate = async (...args) => {
    if (args[1] === 'arrange') throw new Error('placement interrupted before swap');
    return original(...args);
  };
  await assert.rejects(f.controller.arrangeDashboard(), /interrupted before swap/);
  assert.deepEqual(f.actions.map(args => args[0]), ['split', 'close']);
  assert.equal([...f.panes.keys()].some(id => id.startsWith('qa:temp')), false);
  const journal = JSON.parse(await readFile(join(f.controller.directory, 'placement.json'), 'utf8'));
  assert.equal(journal.phase, 'BLOCKED');
  assert.equal(journal.cleanup, 'exact-empty-shell-closed');
  await assert.rejects(f.controller.arrangeDashboard(), /unresolved owned temporary placement/);
  assert.equal(f.actions.length, 2);
});

test('task22 changed temporary native identity blocks cleanup and further placement', async t => {
  const f = await placementFixture(t);
  const original = f.controller.coordinator.mutate;
  f.controller.coordinator.mutate = async (...args) => {
    if (args[1] === 'arrange') {
      f.targets.get('qa:temp1').shell.birth = '133800000000000001';
      throw new Error('temporary identity replaced');
    }
    return original(...args);
  };
  await assert.rejects(f.controller.arrangeDashboard(), /identity replaced/);
  assert.deepEqual(f.actions.map(args => args[0]), ['split']);
  const journal = JSON.parse(await readFile(join(f.controller.directory, 'placement.json'), 'utf8'));
  assert.equal(journal.phase, 'BLOCKED');
  assert.match(journal.cleanupError.message, /temporary native shell changed/);
  assert.notEqual(journal.temporary.shell.birth, f.targets.get('qa:temp1').shell.birth);
  await assert.rejects(f.controller.arrangeDashboard(), /unowned panes/);
  assert.equal(f.actions.length, 1);
});

test('task22 a foreign pane appearing after split blocks swap and close', async t => {
  const f = await placementFixture(t);
  f.afterMutation = ({ type }) => { if (type === 'split') f.addForeign(); };
  await assert.rejects(f.controller.arrangeDashboard(), /unowned panes/);
  assert.deepEqual(f.actions.map(args => args[0]), ['split']);
  const journal = JSON.parse(await readFile(join(f.controller.directory, 'placement.json'), 'utf8'));
  assert.equal(journal.phase, 'BLOCKED');
  assert.equal(journal.failureLayout.panes.length, 6);
  assert.equal(journal.temporary.paneId, 'qa:temp1');
});

test('task4 fixed discovery ignores records for another parent or socket', async t => {
  const f = await setup(t);
  const own = { ...f.controller.binding, role: 'dag', paneId: 'qa:legacy' };
  const paths = [join(f.directory, 'old', 'dag.pane.json'), join(f.directory, 'other', 'dag.pane.json')];
  for (const path of paths) await mkdir(join(path, '..'), { recursive: true });
  await writeFile(paths[0], JSON.stringify(own));
  await writeFile(paths[1], JSON.stringify({ ...own, parentPaneId: 'foreign:parent' }));
  assert.deepEqual((await fixedController.discoverLegacyRecords(f.directory, f.controller.binding)).map(entry => entry.path), [paths[0]]);
});

test('task4 only a typed authoritative pane_not_found proves an obsolete locator', () => {
  assert.equal(fixedController.authoritativePaneNotFound({ code: 'pane_not_found' }), true);
  for (const error of [{ message: 'pane_not_found' }, { message: 'unknown pane' },
    { code: 'connection_refused' }, { code: 'query_failed', stderr: 'pane_not_found' }])
    assert.equal(fixedController.authoritativePaneNotFound(error), false);
});
