import { createHash, randomBytes, randomUUID } from 'node:crypto';
import net from 'node:net';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { EventEmitter, once } from 'node:events';
import { readdir, realpath, stat } from 'node:fs/promises';
import { basename, join, resolve } from 'node:path';
import { homedir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { readJson, writeJson } from './storage.mjs';
import { pruneExpiredSnapshots, retentionDaysFromEnv } from './retention.mjs';
import { shellCommand } from './herdr.mjs';
import { normalizeRun, sessionRuns } from './model.mjs';
import { t, languageOf } from './i18n.mjs';
import { TaskData } from './task-data.mjs';

export function viewKey(socket, pane, session) {
  return createHash('sha256').update(JSON.stringify([socket, pane, session])).digest('hex').slice(0, 24);
}

export function isColumnDashboard(layout, ids) {
  if (!layout?.area || layout.panes?.length !== 4 || new Set(Object.values(ids)).size !== 4) return false;
  const rects = Object.fromEntries(Object.entries(ids).map(([role, id]) => {
    const matches = layout.panes.filter(pane => pane.pane_id === id);
    return [role, matches.length === 1 ? matches[0].rect : null];
  }));
  const { parent, todo, workers, dag } = rects, area = layout.area;
  if (![parent, todo, workers, dag, area].every(rect => rect &&
    ['x', 'y', 'width', 'height'].every(key => Number.isFinite(rect[key])) && rect.width > 0 && rect.height > 0)) return false;
  return parent.x === area.x && parent.y === area.y && parent.height === area.height &&
    todo.x === parent.x + parent.width && todo.y === area.y &&
    workers.x === todo.x && workers.width === todo.width && workers.y === todo.y + todo.height &&
    todo.height + workers.height === area.height && dag.x === todo.x + todo.width &&
    dag.y === area.y && dag.height === area.height && dag.x + dag.width === area.x + area.width;
}

export function dagTitle(sessionId) {
  return `DAG · ${sessionId.slice(0, 8)}`;
}

export function isDagViewerPane(pane) {
  return [pane?.label, pane?.terminal_title, pane?.terminal_title_stripped]
    .some(name => typeof name === 'string' && (name.startsWith('DAG · ') || name === 'OmO DAG'));
}

function missingPane(error) {
  return /pane_not_found|unknown pane|pane .*not found/i.test(`${error.message} ${error.stderr ?? ''}`);
}

export class DagPane {
  constructor({ sessionId, parentPane, socket, stateDir, cwd, node, viewer, herdr, notify = () => {}, language = 'en', taskStateDir, retentionDays, streamDelay = 250 }) {
    Object.assign(this, { sessionId, parentPane, stateDir, cwd, node, viewer, herdr, notify });
    this.language = languageOf(language);
    this.key = viewKey(socket, parentPane, sessionId);
    this.stateFile = join(stateDir, `${this.key}.json`);
    this.recordFile = join(stateDir, `${this.key}.pane.json`);
    this.checkpointDir = join(taskStateDir ?? join(cwd, '.omo', 'senpi-task'), 'dag', 'runs');
    // Explicit option wins so tests need not mutate the shared process environment.
    this.retentionDays = retentionDays ?? retentionDaysFromEnv();
    this.streamDelay = streamDelay;
    this.queue = Promise.resolve();
    this.runs = [];
    this.stopped = false;
    this.tasks = [];
    this.streams = new Map();
    this.taskData = new TaskData({ cwd, sessionId, stateDir: taskStateDir, notify,
      onChange: () => { if (!this.stopped) this.enqueue(() => this.save(true)); } });
  }

  enqueue(job) {
    const result = this.queue.then(job);
    this.queue = result.catch(error => this.notify(`DAG pane: ${error.message}`));
    return result;
  }

  receive(payload) {
    const runs = sessionRuns(payload, this.sessionId, this.language);
    if (runs === null || this.stopped) return Promise.resolve();
    return this.enqueue(async () => {
      // RPC replaces transient runs; durable runs omitted by a snapshot remain recoverable.
      await this.restoreRuns(runs, true);
      await this.save(true);
    });
  }

  start() {
    return this.enqueue(async () => {
      // Housekeeping precedes restore so a fresh pane never lists pruned snapshots.
      await pruneExpiredSnapshots(this.stateDir, {
        keepFiles: [basename(this.stateFile), basename(this.recordFile), `${basename(this.stateFile)}.view.json`],
        days: this.retentionDays, notify: message => this.notify(t(this.language, 'pruneFailed', { error: message })) });
      const state = await readJson(this.stateFile);
      if (state?.sessionId === this.sessionId) {
        this.runs = state.runs ?? [];
        this.taskData.restore(state.tasks);
      }
      this.taskData.start();
      await this.restoreRuns();
      await this.save(true);
    });
  }

  receiveTasks(payload) {
    if (this.stopped) return Promise.resolve();
    return this.enqueue(async () => {
      await this.taskData.refresh(this.runs);
      if (this.taskData.receive(payload)) await this.save(true);
    });
  }

  // Streamed tokens arrive far more often than task records change; coalesce
  // them and rewrite the snapshot without rescanning the task store. The
  // returned promise settles after the coalesced save, so tests need no sleeps.
  // `since` marks when the current phase began and `lastAt` the latest event,
  // so the viewer clock can show elapsed waits and gaps between tokens.
  receiveStream(update) {
    if (this.stopped) return Promise.resolve();
    if (update.active) {
      const previous = this.streams.get(update.taskId);
      const samePhase = previous?.phase === update.phase && previous.tool === update.tool;
      this.streams.set(update.taskId, { phase: update.phase,
        ...(update.tool ? { tool: update.tool } : {}), ...(update.text ? { text: update.text } : {}),
        ...(update.attempt ? { attempt: update.attempt } : {}), ...(update.maxAttempts ? { maxAttempts: update.maxAttempts } : {}),
        since: samePhase ? previous.since : update.now, lastAt: update.now });
    } else if (!this.streams.delete(update.taskId)) return Promise.resolve();
    if (this.streamTimer) return this.streamFlush;
    this.streamFlush = new Promise(resolve => {
      this.streamTimer = setTimeout(() => {
        this.streamTimer = undefined;
        this.enqueue(() => this.save(true, false)).then(resolve, resolve);
      }, this.streamDelay);
      this.streamTimer.unref?.();
    });
    return this.streamFlush;
  }

  withStreams(tasks, connected) {
    if (!connected || !this.streams.size) return tasks;
    return tasks.map(task => {
      // A finished task never regains a stale activity from a late event.
      if (task.status !== 'running') { this.streams.delete(task.id); return task; }
      const activity = this.streams.get(task.id);
      return activity ? { ...task, activity } : task;
    });
  }

  async restoreRuns(runs = this.runs, preferLive = false) {
    let files;
    try { files = await readdir(this.checkpointDir); }
    catch (error) { if (error.code !== 'ENOENT') throw error; files = []; }
    const restored = new Map(runs.map(run => [run.id, run]));
    for (const file of files.filter(file => file.endsWith('.json'))) {
      try {
        const raw = await readJson(join(this.checkpointDir, file));
        if (raw === null) continue; // A checkpoint may disappear after readdir.
        if (typeof raw.parentSessionId !== 'string' || raw.schemaVersion !== 1) throw new Error('Invalid checkpoint header');
        if (raw.parentSessionId !== this.sessionId) continue;
        const run = normalizeRun(raw);
        if (!run) throw new Error('Invalid checkpoint run');
        // Startup/open distrust cached state; an incoming RPC may be ahead of disk.
        if (!preferLive || !restored.has(run.id)) restored.set(run.id, run);
      } catch (error) { this.notify(`DAG pane: Cannot read checkpoint ${file}: ${error.message}`); }
    }
    this.runs = [...restored.values()].sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  }

  async save(connected, refresh = true) {
    if (refresh) this.tasks = await this.taskData.refresh(this.runs);
    await writeJson(this.stateFile, { version: 1, sessionId: this.sessionId, connected, language: this.language,
      updatedAt: new Date().toISOString(), runs: this.runs, tasks: this.withStreams(this.tasks, connected) });
    // All callers serialize saves through the queue, including task-only disk changes.
    if (connected && !this.stopped && (this.runs.length || this.tasks.length)) await this.ensure(false);
  }

  open() {
    return this.enqueue(async () => {
      if (!this.runs.length) {
        const state = await readJson(this.stateFile);
        if (state?.sessionId === this.sessionId) {
          this.runs = state.runs ?? [];
          this.taskData.restore(state.tasks);
        }
      }
      await this.restoreRuns();
      await this.save(true);
      return this.ensure(true);
    });
  }

  async listDagPanes() {
    try {
      const panes = (await this.herdr('list'))?.panes ?? [];
      const tab = panes.find(pane => pane.pane_id === this.parentPane)?.tab_id;
      return panes.filter(isDagViewerPane)
        .filter(pane => pane.pane_id && pane.pane_id !== this.parentPane && (!tab || pane.tab_id === tab))
        .map(pane => pane.pane_id);
    } catch (error) {
      if (error instanceof Error) return [];
      throw error;
    }
  }

  async closePane(paneId) {
    try { await this.herdr('close', paneId); }
    catch (error) {
      if (!missingPane(error)) this.notify(t(this.language, 'closeFailed', { error: error.message }));
    }
  }

  async closeDagPanes(keep) {
    for (const paneId of await this.listDagPanes()) {
      if (paneId !== keep) await this.closePane(paneId);
    }
  }

  async ensure(force) {
    const record = await readJson(this.recordFile);
    // Preserve a manually closed pane across events/reloads. /dag-pane explicitly reopens it.
    if (record && !force) return record.paneId;
    if (record?.paneId) {
      try {
        await this.herdr('get', record.paneId);
        if (record.ready) {
          await this.closeDagPanes(record.paneId);
          return record.paneId;
        }
        // Occupied leftover from a failed launch: close it, then replace.
        await this.closePane(record.paneId);
      } catch (error) {
        if (!missingPane(error)) throw error;
      }
    }
    if (typeof this.node === 'function') this.node = await this.node();
    // Record an attempt before mutation: a timeout must not create repeated orphan panes.
    await writeJson(this.recordFile, { attempted: true });
    await this.closeDagPanes();
    const result = await this.herdr('split', '--pane', this.parentPane, '--direction', 'right',
      '--ratio', '0.65', '--cwd', this.cwd, '--no-focus');
    const paneId = result?.pane?.pane_id;
    if (!paneId) throw new Error(t(this.language, 'missingPaneId'));
    await writeJson(this.recordFile, { paneId, ready: false });
    await this.herdr('rename', paneId, dagTitle(this.sessionId));
    await this.herdr('run', paneId, shellCommand([this.node, this.viewer, '--state', this.stateFile, '--close-pane', paneId]));
    await writeJson(this.recordFile, { paneId, ready: true });
    return paneId;
  }

  stop() {
    this.stopped = true;
    clearTimeout(this.streamTimer);
    this.streams.clear();
    this.taskData.stop();
    return this.enqueue(() => this.save(false));
  }
}

// The extension uses this fixed-role controller. The old standalone controller
// remains available to existing unmanaged callers, but is never a layout peer.
const sameOwner = (a, b) => a && b && ['socketPath', 'parentPaneId', 'sessionId', 'ownerKey']
  .every(key => a[key] === b[key]);
const pathKey = path => process.platform === 'win32' ? String(path).replaceAll('\\', '/').toLowerCase() : String(path);
export const authoritativePaneNotFound = error => error?.code === 'pane_not_found';
export async function discoverLegacyRecords(root, binding) {
  const directory = await realpath(resolve(root)), paths = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (entry.isFile() && entry.name.endsWith('.pane.json')) paths.push(join(directory, entry.name));
    if (entry.isDirectory()) for (const file of await readdir(join(directory, entry.name), { withFileTypes: true })) {
      if (file.isFile() && file.name.endsWith('.pane.json')) paths.push(join(directory, entry.name, file.name));
    }
  }
  const records = [];
  for (const path of paths.sort()) {
    const actual = await realpath(path);
    if (!pathKey(actual).startsWith(pathKey(directory) + '/')) throw blocked('legacy locator leaves configured root');
    const record = await readJson(actual);
    if (record?.parentPaneId === binding.parentPaneId && pathKey(record.socketPath) === pathKey(binding.socketPath))
      records.push({ path, record });
  }
  return records;
}
const controlKeys = ['token', 'ownerKey', 'sessionId', 'scopeEpoch', 'role', 'launchToken', 'paneId', 'statePath'];
const sameControl = (a, b) => a?.v === 1 && b?.v === 1 && controlKeys.every(key => a[key] === b[key]);
const canonical = path => process.platform === 'win32' ? path.replaceAll('\\', '/').toLowerCase() : path;
const blocked = cause => Object.assign(new Error(`BLOCKED: ${cause?.message ?? cause}`), { inlineFallback: true, cause });
export async function probeManagedSlot({ stateDir, socketPath, parentPaneId }) {
  let root;
  try { root = canonical(await realpath(resolve(stateDir))); }
  catch (error) { return error.code === 'ENOENT' ? { blocked: false } : { blocked: true, code: 'SLOT_UNKNOWN' }; }
  let socket;
  try {
    const pipe = socketPath.replaceAll('/', '\\');
    socket = /^\\\\[.]\\pipe\\/i.test(pipe) ? canonical(pipe) : canonical(await realpath(resolve(socketPath)));
  } catch { return { blocked: true, code: 'SLOT_UNKNOWN' }; }
  const key = createHash('sha256').update(JSON.stringify([socket, parentPaneId])).digest('hex');
  const directory = join(root, 'parent-slots', key);
  try { await stat(join(directory, 'slot.json')); return { blocked: true, code: 'MANAGED_SLOT' }; }
  catch (error) { if (error.code !== 'ENOENT') return { blocked: true, code: 'SLOT_UNKNOWN' }; }
  try { await stat(join(directory, 'lease.sqlite')); }
  catch (error) { return error.code === 'ENOENT' ? { blocked: false } : { blocked: true, code: 'SLOT_UNKNOWN' }; }
  let db;
  try {
    db = new DatabaseSync(join(directory, 'lease.sqlite'), { readOnly: true, timeout: 0 });
    db.prepare('PRAGMA schema_version').get();
    return { blocked: true, code: 'MANAGED_SLOT' };
  } catch (error) { return { blocked: true, code: error.errcode === 5 ? 'SQLITE_BUSY' : 'SLOT_UNKNOWN' }; }
  finally { db?.close(); }
}
const nativeIdentity = row => {
  if (!Number.isSafeInteger(row?.ProcessId) || !row.Birth || !row.ExecutablePath || !row.CommandLine) throw blocked('native identity unavailable');
  return { pid: row.ProcessId, birth: row.Birth, executable: row.ExecutablePath,
    argvDigest: createHash('sha256').update(row.CommandLine).digest('hex') };
};

function receiveControl(socket, callback) {
  socket.setEncoding('utf8');
  let buffer = '';
  socket.on('data', chunk => {
    buffer += chunk;
    if (buffer.length > 65536) return socket.destroy();
    let cut;
    while ((cut = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, cut); buffer = buffer.slice(cut + 1);
      try { callback(JSON.parse(line)); } catch { socket.destroy(); }
    }
  });
}
function processMatch(info, record, expectedPid) {
  const matches = (info?.process_info?.foreground_processes ?? []).filter(process => {
    const argv = process.argv ?? [];
    return argv.length === 10 && pathKey(argv[0]) === pathKey(record.nodePath) && pathKey(argv[1]) === pathKey(record.viewerPath) &&
      argv[2] === '--view' && argv[3] === (record.role === 'dag' ? 'dag' : 'tasks') &&
      argv[4] === '--state' && pathKey(argv[5]) === pathKey(record.statePath) &&
      argv[6] === '--close-pane' && argv[7] === record.paneId &&
      argv[8] === '--launch-token' && argv[9] === record.launchToken &&
      (expectedPid === undefined || expectedPid === process.pid) &&
      (record.processId === undefined || record.processId === process.pid);
  });
  return matches.length === 1 ? matches[0] : null;
}
const executeProcessQuery = promisify(execFile);
export function parseLaunchCommand(command) {
  if (typeof command !== 'string') return null;
  const argv = [];
  let at = 0;
  while (at < command.length) {
    while (/[ \t]/.test(command[at] ?? '') && at < command.length) at++;
    if (at === command.length) break;
    let value;
    if (command[at] === '"') {
      const end = command.indexOf('"', at + 1);
      if (end < 0 || command[end - 1] === '\\') return null;
      value = command.slice(at + 1, end); at = end + 1;
      if (at < command.length && !/[ \t]/.test(command[at])) return null;
    } else {
      const start = at;
      while (at < command.length && !/[ \t]/.test(command[at])) at++;
      value = command.slice(start, at);
      if (/["\r\n]/.test(value)) return null;
    }
    if (!value || /[\r\n]/.test(value)) return null;
    argv.push(value);
  }
  return argv;
}
async function nativeShellProcesses(pid) {
  const { stdout } = await executeProcessQuery('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command',
    `[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(); $ErrorActionPreference = 'Stop'; $rows = @(Get-CimInstance Win32_Process -Filter "ProcessId = ${pid} OR ParentProcessId = ${pid}" | Select-Object ProcessId,ParentProcessId,ExecutablePath,CommandLine,@{Name='CreationDate';Expression={$_.CreationDate.ToUniversalTime().ToString('o')}},@{Name='Birth';Expression={$h=[Diagnostics.Process]::GetProcessById($_.ProcessId);try{$null=$h.Handle;$h.StartTime.ToUniversalTime().ToFileTimeUtc().ToString()}finally{$h.Dispose()}}}); ConvertTo-Json -InputObject $rows -Compress`],
  { timeout: 5000, maxBuffer: 65536 });
  return JSON.parse(stdout);
}
export async function observePaneShell(info, record, inspect = nativeShellProcesses) {
  const pid = info?.process_info?.shell_pid;
  if (process.platform !== 'win32' || !Number.isSafeInteger(pid) || pid < 1 ||
    pid !== record.creationProcessId || info.process_info.pane_id && info.process_info.pane_id !== record.paneId) return null;
  let rows;
  try { rows = await inspect(pid); } catch { return null; }
  if (!Array.isArray(rows) || rows.filter(row => row.ProcessId === pid).length !== 1 ||
    new Set(rows.map(row => row.ProcessId)).size !== rows.length) return null;
  const shell = rows.find(row => row.ProcessId === pid), born = Date.parse(shell.CreationDate);
  if (!Number.isFinite(born) || !shell.ExecutablePath || !shell.CommandLine ||
    record.creationDate && shell.CreationDate !== record.creationDate) return null;
  const reported = info.process_info.foreground_processes?.find(row => row.pid === pid);
  if (reported && pathKey(reported.argv?.[0]) !== pathKey(shell.ExecutablePath)) return null;
  const children = rows.filter(row => row !== shell);
  if (children.some(child => !Number.isSafeInteger(child.ProcessId) || child.ProcessId < 1 ||
    child.ParentProcessId !== pid || !child.ExecutablePath || !child.CommandLine ||
    !Number.isFinite(Date.parse(child.CreationDate)) || Date.parse(child.CreationDate) < born)) return null;
  return { shell, children };
}
export async function legacyPaneIdentity(info, record, inspect) {
  if (!record.creationDate) return null;
  const observation = await observePaneShell(info, record, inspect);
  if (!observation || !observation.shell.Birth || record.creationBirth && observation.shell.Birth !== record.creationBirth ||
    observation.children.length > 1) return null;
  const foreground = info.process_info.foreground_processes ?? [];
  if (foreground.length !== 1 || ![record.creationProcessId, record.processId].includes(foreground[0].pid)) return null;
  const actual = { shell: nativeIdentity(observation.shell) };
  if (!observation.children.length) return record.prepared === true && !record.processId ? actual : null;
  const child = observation.children[0], argv = parseLaunchCommand(child.CommandLine);
  if (!child.Birth || record.viewerBirth && child.Birth !== record.viewerBirth ||
    pathKey(child.ExecutablePath) !== pathKey(record.nodePath) || !argv ||
    !processMatch({ process_info: { foreground_processes: [{ pid: child.ProcessId, argv }] } }, record)) return null;
  actual.viewer = nativeIdentity(child);
  return actual;
}
export async function paneViewerProcess(info, record, expectedPid, inspect) {
  const foreground = processMatch(info, record, expectedPid);
  if (foreground) return foreground;
  const shell = info?.process_info?.shell_pid, pid = expectedPid ?? record.processId;
  if (process.platform !== 'win32' || !Number.isSafeInteger(pid) || pid < 1 ||
    !record.creationDate || !Number.isSafeInteger(record.creationProcessId) || shell !== record.creationProcessId ||
    info.process_info.foreground_processes?.length !== 1 ||
    info.process_info.foreground_processes[0].pid !== shell) return null;
  const observation = await observePaneShell(info, record, inspect);
  if (!observation || observation.children.length !== 1) return null;
  const child = observation.children[0];
  if (child.ProcessId !== pid || record.viewerBirth && child.Birth !== record.viewerBirth ||
    pathKey(child.ExecutablePath) !== pathKey(record.nodePath)) return null;
  const argv = parseLaunchCommand(child.CommandLine);
  if (!argv) return null;
  return processMatch({ process_info: { foreground_processes: [{ pid, argv }] } }, record, pid);
}
export async function canCleanupPane(info, record, inspect) {
  if (info?.process_info?.shell_pid !== record.creationProcessId) return false;
  if (process.platform !== 'win32') {
    const foreground = info.process_info.foreground_processes ?? [];
    return foreground.every(row => row.pid === record.creationProcessId) ||
      foreground.length === 1 && Boolean(processMatch(info, record));
  }
  if (!record.creationDate) return false;
  const observation = await observePaneShell(info, record, inspect);
  if (!observation) return false;
  if (!observation.children.length) return true;
  if (observation.children.length !== 1) return false;
  const child = observation.children[0], argv = parseLaunchCommand(child.CommandLine);
  return pathKey(child.ExecutablePath) === pathKey(record.nodePath) && argv !== null &&
    Boolean(processMatch({ process_info: { foreground_processes: [{ pid: child.ProcessId, argv }] } }, record));
}

export class FixedDagPane extends DagPane {
  constructor(options) {
    super(options);
    this.inspectShell = options.inspectShell;
    this.managedAvailable = options.managedAvailable ?? false;
    this.managedStateDir = options.managedStateDir ?? process.env.OMO_HERDR_TODO_STATE_DIR ??
      join(homedir(), '.omo', 'agent', 'herdr-todo');
    this.binding = { socketPath: options.socket, parentPaneId: options.parentPane,
      sessionId: options.sessionId, ownerKey: this.key };
    this.legacyRecordFile = this.recordFile;
    this.directory = join(options.stateDir, this.key);
    this.stateFile = join(this.directory, 'source.json');
    this.scopeEpoch = options.scopeEpoch ?? 1;
    this.lifetimeId = randomUUID();
    this.sourceEvents = new EventEmitter();
    this.roleEvents = new EventEmitter();
    this.roleTail = Promise.resolve();
    this.stopSignal = new AbortController();
    this.offers = {};
    this.records = new Map();
    this.status = 'waiting';
    this.controlToken = randomBytes(32).toString('hex');
    this.controlPath = `\\\\.\\pipe\\omo-herdr-dag-control-${randomUUID()}`;
    this.controlSockets = new Set();
    this.controlJobs = new Set();
    this.pendingLaunches = new Set();
    this.launchRegistrations = new Map();
    this.closureVersions = new Map();
    this.sourceReady = new Promise((resolve, reject) => { this.sourceSaved = resolve; this.sourceRejected = reject; });
    this.sourceReady.catch(() => {});
  }
  roleFile(role) { return join(this.directory, `${role}.json`); }
  paneFile(role) { return join(this.directory, `${role}.pane.json`); }
  roleWrite(job) {
    const result = this.roleTail.then(job);
    this.roleTail = result.catch(() => {});
    return result;
  }
  source() {
    return { runs: structuredClone(this.runs), tasks: structuredClone(this.tasks),
      connected: !this.stopped, updatedAt: this.updatedAt };
  }
  provider() {
    return { lifetimeId: this.lifetimeId, binding: this.binding, sourceReady: this.sourceReady,
      getSource: () => this.source(), setCoordinator: coordinator => {
        if (!this.stopped && sameOwner(coordinator?.binding, this.binding)) {
          this.coordinator = coordinator;
          if (coordinator.actor) { this.managedAvailable = true; this.managedActor = coordinator.actor; }
        }
      },
      idle: async () => { await this.queue; await this.roleTail; },
      openPair: options => this.openPair(options),
      preflightRecovery: () => this.preflightRecovery(),
      replaceRetained: entries => this.replaceRetained(entries),
      arrangeDashboard: () => this.arrangeDashboard(),
      setOffers: offers => this.setOffers(offers), inspectPair: () => this.inspectPair(),
      observeTarget: target => this.nativeTarget(target),
      drain: () => this.stop(),
      subscribeSource: callback => {
        this.sourceEvents.on('source', callback);
        return () => this.sourceEvents.off('source', callback);
      } };
  }
  start() {
    return this.enqueue(async () => {
      try {
        await this.startControl();
        const state = await readJson(this.stateFile);
        if (state?.sessionId === this.sessionId) {
          this.runs = state.runs ?? []; this.taskData.restore(state.tasks);
        }
        this.taskData.start();
        await this.restoreRuns();
        await this.save(true);
        if (!this.stopped) { this.sourceSaved(); this.status = 'layout-waiting'; }
      } catch (error) { this.sourceRejected(error); throw error; }
    });
  }
  async startControl() {
    this.control = net.createServer(socket => {
      this.controlSockets.add(socket);
      socket.once('close', () => this.controlSockets.delete(socket));
      socket.on('error', () => {});
      receiveControl(socket, message => {
        const job = this.handleControl(message, socket);
        this.controlJobs.add(job);
        void job.catch(() => socket.destroy()).finally(() => this.controlJobs.delete(job));
      });
    });
    await new Promise((resolve, reject) => {
      this.control.once('error', reject); this.control.once('listening', resolve); this.control.listen(this.controlPath);
    });
  }
  async save(connected, refresh = true) {
    if (refresh) this.tasks = await this.taskData.refresh(this.runs);
    if (this.stopped && connected) return;
    const signature = JSON.stringify([this.runs, this.tasks, connected]);
    const changed = signature !== this.sourceSignature;
    if (changed) this.updatedAt = new Date().toISOString();
    await writeJson(this.stateFile, { version: 1, ...this.binding, scopeEpoch: this.scopeEpoch,
      connected, language: this.language, updatedAt: this.updatedAt, runs: this.runs, tasks: this.tasks });
    await this.roleWrite(async () => {
      for (const [role, record] of this.records) {
        if (record.paneId) {
          const persisted = await readJson(this.paneFile(role));
          const current = { ...record, scopeEpoch: this.scopeEpoch,
            manualClose: persisted?.launchToken === record.launchToken ? persisted.manualClose : record.manualClose };
          this.records.set(role, current);
          await writeJson(this.paneFile(role), current);
          await writeJson(this.roleFile(role), this.roleState(current, connected));
        }
      }
    });
    this.sourceSignature = signature;
    if (!this.stopped && changed) this.sourceEvents.emit('source', this.source());
    // A source job must not await a coordinator job which needs this queue.
  }
  roleState(record, connected = true) {
    const role = record.role, statePath = this.roleFile(role);
    return { schema: 'omo-herdr-dashboard/role/1', version: 1, ...this.binding, scopeEpoch: this.scopeEpoch,
      ...(this.managedActor ? { managedScope: { ownerNonce: this.managedActor.snapshot.ownerNonce,
        sessionId: this.sessionId, generation: this.managedActor.snapshot.generation,
        participant: 'dag', lifetimeId: this.lifetimeId } } : {}),
      role, tabId: record.tabId, launchToken: record.launchToken, paneId: record.paneId,
      connected, language: this.language, updatedAt: this.updatedAt,
      availability: connected ? this.runs.length || this.tasks.length ? 'current' : 'empty' : 'last-known',
      runs: this.runs, tasks: this.tasks, prefsPath: `${statePath}.view.json`,
      presenceOffer: connected ? this.offers[role] ?? null : null,
      controlOffer: connected ? { pipePath: this.controlPath, v: 1, token: this.controlToken,
        ownerKey: this.key, sessionId: this.sessionId, scopeEpoch: this.scopeEpoch, role,
        launchToken: record.launchToken, paneId: record.paneId, statePath } : null };
  }
  async membership(paneId) {
    const panes = (await this.herdr('list'))?.panes ?? [];
    const parents = panes.filter(pane => pane.pane_id === this.parentPane);
    const matches = panes.filter(pane => pane.pane_id === paneId);
    if (parents.length !== 1 || matches.length !== 1 || !parents[0].tab_id ||
      matches[0].tab_id !== parents[0].tab_id) throw new Error('layout-waiting: missing or ambiguous owned anchor');
    return { tabId: parents[0].tab_id, pane: matches[0] };
  }
  recordMatches(record, role) {
    return record?.schema === 'omo-herdr-dashboard/pane/1' && record.role === role &&
      sameOwner(record, this.binding) && pathKey(record.statePath) === pathKey(this.roleFile(role)) &&
      pathKey(record.viewerPath) === pathKey(this.viewer) && typeof record.launchToken === 'string';
  }
  async verify(record, pid) {
    if (!this.recordMatches(record, record?.role) || !record.paneId) return null;
    const member = await this.membership(record.paneId);
    if (record.tabId !== member.tabId) return null;
    const state = await readJson(this.roleFile(record.role));
    if (!state || !sameOwner(state, record) || state.launchToken !== record.launchToken ||
      state.paneId !== record.paneId || state.role !== record.role) return null;
    const info = await this.herdr('process-info', '--pane', record.paneId);
    if (this.managedAvailable) {
      const identity = await legacyPaneIdentity(info, { ...record, processId: pid ?? record.processId }, this.inspectShell);
      return identity?.viewer ? { pid: identity.viewer.pid } : null;
    }
    return paneViewerProcess(info, record, pid, this.inspectShell);
  }
  async nativeTarget(target) {
    const member = await this.membership(target.paneId);
    const info = await this.herdr('process-info', '--pane', target.paneId);
    const pid = info?.process_info?.shell_pid;
    const rows = await (this.inspectShell ?? nativeShellProcesses)(pid);
    const shell = rows.filter(row => row.ProcessId === pid);
    if (shell.length !== 1) throw blocked('role shell unavailable');
    const actual = { paneId: member.pane.pane_id, tabId: member.tabId,
      shell: { pid, birth: nativeIdentity(shell[0]).birth } };
    if (target.statePath) {
      const role = ['dag', 'workers'].find(role => pathKey(this.roleFile(role)) === pathKey(target.statePath));
      const record = role && await readJson(this.paneFile(role));
      const state = role && await readJson(this.roleFile(role));
      if (!record || !state || !this.recordMatches(record, role) || state.scopeEpoch !== this.scopeEpoch ||
        record.creationProcessId !== pid || record.creationDate !== shell[0].CreationDate ||
        record.creationBirth && record.creationBirth !== shell[0].Birth ||
        !sameOwner(state, this.binding) || state.launchToken !== record.launchToken) throw blocked('role source or shell changed');
      Object.assign(actual, { statePath: this.roleFile(role), sourcePath: this.stateFile, cachePath: this.roleFile(role),
        prefsPath: state.prefsPath, launchToken: state.launchToken, controlToken: state.controlOffer?.token ?? this.controlToken });
      if (record.processId) {
        const children = rows.filter(row => row.ParentProcessId === pid);
        if (children.length !== 1 || children[0].ProcessId !== record.processId ||
          record.viewerBirth && children[0].Birth !== record.viewerBirth ||
          !await paneViewerProcess(info, record, record.processId, async () => rows)) throw blocked('role viewer changed');
        actual.viewer = nativeIdentity(children[0]);
      } else if (rows.some(row => row.ParentProcessId === pid)) throw blocked('launch target is occupied');
    }
    return actual;
  }
  async mutate(role, type, paneId, args, effect) {
    if (!this.managedAvailable) return this.herdr(...args);
    if (!this.coordinator?.actor) { this.status = 'BLOCKED'; throw blocked('managed W coordinator unavailable'); }
    const record = await readJson(this.paneFile(role));
    const target = record?.paneId === paneId ? await this.nativeTarget(record) : await this.coordinator.target(paneId);
    return this.coordinator.mutate(role, type, target, args, effect);
  }
  async handleControl(message, socket) {
    if (this.stopped && !(this.managedAvailable && message?.type === 'register' && this.pendingLaunches.has(message.launchToken)) ||
      !['dag', 'workers'].includes(message?.role) ||
      !['register', 'close'].includes(message.type)) return socket.destroy();
    const role = message.role;
    const record = await readJson(this.paneFile(role));
    const state = await readJson(this.roleFile(role));
    if (!sameControl(message, state?.controlOffer) || message.scopeEpoch !== this.scopeEpoch ||
      !this.recordMatches(record, role) || record.launchToken !== message.launchToken ||
      !Number.isSafeInteger(message.pid) || !(await this.verify(record, message.pid))) return socket.destroy();
    const registration = this.roleWrite(async () => {
      const current = await readJson(this.paneFile(role));
      if (current.launchToken !== record.launchToken || current.scopeEpoch !== message.scopeEpoch ||
        message.scopeEpoch !== this.scopeEpoch) return socket.destroy();
      if (this.managedAvailable && message.type === 'register' && this.pendingLaunches.has(message.launchToken))
        this.launchRegistrations.set(message.launchToken, registration);
      if (this.stopped) {
        socket.write(`${JSON.stringify({ ...state.controlOffer, type: 'registered' })}\n`);
        this.roleEvents.emit(`created:${message.launchToken}`, record.paneId);
        return;
      }
      if (message.type === 'register') {
        const ready = { ...current, processId: message.pid, ready: true };
        if (this.managedAvailable) {
          const rows = await (this.inspectShell ?? nativeShellProcesses)(record.creationProcessId);
          ready.viewerBirth = nativeIdentity(rows.find(row => row.ProcessId === message.pid)).birth;
        }
        this.records.set(role, ready); await writeJson(this.paneFile(role), ready);
        socket.write(`${JSON.stringify({ ...state.controlOffer, type: 'registered' })}\n`);
        this.roleEvents.emit(`created:${message.launchToken}`, record.paneId);
        if (this.managedAvailable && this.coordinator.actor.blocked)
          await this.coordinator.observeRegisteredRun(role, ready);
        if (this.managedAvailable) await this.coordinator.registerRole(role, () => this.nativeTarget(ready));
        if (this.managedAvailable) await this.coordinator.holdRoleViewer(role, ready);
      } else {
        this.closureVersions.set(role, (this.closureVersions.get(role) ?? 0) + 1);
        if (this.managedAvailable) {
          this.coordinator.noteRoleClosure?.(role);
          this.coordinator.assertCurrent();
          await this.coordinator.closeRole(role, record.paneId);
        }
        const closed = { ...current, manualClose: true };
        this.records.set(role, closed); await writeJson(this.paneFile(role), closed);
        const reply = () => new Promise(resolve => socket.end(`${JSON.stringify({ ...state.controlOffer, type: 'closed' })}\n`, resolve));
        if (this.managedAvailable) {
          const target = await this.nativeTarget(current);
          await this.coordinator.mutate(role, 'close', target, ['close', record.paneId], null, reply);
        } else { await reply(); await this.herdr('close', record.paneId); }
      }
    });
    await registration;
  }
  setOffers(offers = {}) {
    return this.enqueue(async () => {
      for (const role of ['dag', 'workers']) {
        const offer = offers[role];
        if (offer && (offer.v !== 1 || offer.role !== role || !sameOwner({ ...offer,
          socketPath: this.binding.socketPath, parentPaneId: this.parentPane }, this.binding) ||
          !Number.isSafeInteger(offer.scopeEpoch) || offer.scopeEpoch < this.scopeEpoch))
          throw new Error('Stale or foreign presence offer');
      }
      const epoch = offers.dag?.scopeEpoch ?? offers.workers?.scopeEpoch;
      await this.roleWrite(() => {
        if (epoch) this.scopeEpoch = epoch;
        this.offers = { ...offers };
      });
      await this.save(true, false);
    });
  }
  async inspectPair() {
    const pair = {};
    for (const role of ['dag', 'workers']) {
      const record = await readJson(this.paneFile(role));
      if (!record || record.manualClose) continue;
      const obsolete = this.coordinator?.actor?.snapshot.closures?.some(entry => entry.role === role &&
        entry.paneId === record.paneId && entry.sessionId === record.sessionId && entry.reason === 'pane_not_found');
      if (!obsolete && await this.verify(record)) pair[role] = record;
    }
    return pair;
  }
  async preflightRecovery() {
    if (this.coordinator?.actor && this.coordinator.reconcileLegacy)
      return this.coordinator.reconcileLegacy(this.stateDir, legacyPaneIdentity, this.viewer);
    const entries = await discoverLegacyRecords(this.stateDir, this.binding);
    for (const { path, record } of entries) {
      if (path === this.paneFile(record.role) || !record.paneId || record.manualClose) continue;
      // A standalone controller has no managed retirement/lease authority.
      this.status = 'BLOCKED';
      throw blocked('unverified exact-parent legacy prevents another standalone pair');
    }
  }
  async replaceRetained(entries = []) {
    for (const retained of entries) {
      if (retained.state !== 'retained' || retained.record?.sessionId === this.sessionId && !retained.empty && !retained.upgrade) continue;
      const role = retained.record.role;
      if (!['dag', 'workers'].includes(role) || !this.coordinator?.actor)
        throw blocked('retained fixed replacement requires the held coordinator');
      if (typeof this.node === 'function') this.node = await this.node();
      const record = { ...retained.record, ...this.binding, scopeEpoch: this.scopeEpoch,
        statePath: this.roleFile(role), viewerPath: this.viewer, nodePath: this.node, launchToken: randomBytes(24).toString('hex'),
        prepared: true, ready: false, manualClose: false, layoutClaim: this.claim };
      delete record.processId; delete record.viewerBirth;
      await this.coordinator.retireRetainedRole(retained, () => this.roleWrite(async () => {
        this.records.set(role, record);
        await writeJson(this.paneFile(role), record);
        await writeJson(this.roleFile(role), this.roleState(record));
      }));
      await this.launchRole(record);
    }
  }
  openPair({ force = false, forceRoles, forceCurrent = () => true, offers, claim } = {}) {
    const closureVersions = new Map(this.closureVersions);
    return this.enqueue(async () => {
      if (this.stopped) return {};
      if (this.managedAvailable) {
        if (!this.coordinator?.actor) { this.status = 'BLOCKED'; throw blocked('managed W coordinator unavailable'); }
        try { this.coordinator.assertCurrent(); }
        catch (error) { this.status = 'BLOCKED'; throw blocked(error); }
        await this.coordinator.join(this.lifetimeId);
      } else if (!this.coordinator) {
        const probe = await probeManagedSlot({ stateDir: this.managedStateDir, socketPath: this.binding.socketPath,
          parentPaneId: this.parentPane });
        if (probe.blocked) { this.status = 'BLOCKED'; throw blocked(probe.code); }
      }
      if (claim && (!sameOwner(claim, this.binding) || this.coordinator &&
        claim.lifetimeId !== this.coordinator.lifetimeId)) return {};
      await this.preflightRecovery();
      if (offers) {
        for (const role of ['dag', 'workers']) {
          if (offers[role]?.ownerKey !== this.key || offers[role]?.sessionId !== this.sessionId ||
            offers[role]?.role !== role || offers[role]?.scopeEpoch < this.scopeEpoch)
            throw new Error('Stale or foreign presence offer');
        }
        await this.roleWrite(() => {
          this.scopeEpoch = offers.dag.scopeEpoch;
          this.offers = offers;
        });
      }
      await this.save(true, false);
      const parent = await this.membership(this.parentPane);
      if (claim && claim.tabId !== parent.tabId) return {};
      this.claim = claim ?? { ...this.binding, tabId: parent.tabId, lifetimeId: this.lifetimeId };
      await writeJson(join(this.directory, 'layout.json'), this.claim);
      // Legacy records without exact state/process/owner identity cannot be adopted
      // or title-cleaned. They also cannot authorize a second hidden layout.
      if (await readJson(this.legacyRecordFile) && !await readJson(this.paneFile('dag'))) {
        this.status = 'layout-waiting'; return {};
      }
      const todo = await this.coordinator?.inspectTodo?.();
      if (todo && (!sameOwner(todo, this.binding) || todo.role !== 'todo' || todo.tabId !== parent.tabId ||
        todo.scopeEpoch !== this.scopeEpoch || todo.ready !== true || todo.manualClose || todo.paneId === this.parentPane))
        throw new Error('layout-waiting: stale or foreign Todo anchor');
      if (todo) await this.membership(todo.paneId);
      const existing = await this.inspectPair();
      const forceRole = role => force && (forceRoles === undefined || forceRoles.includes(role)) && forceCurrent(role) &&
        (closureVersions.get(role) ?? 0) === (this.closureVersions.get(role) ?? 0);
      if (this.claim.layout === 'columns') {
        const dag = await this.ensureRole('dag', existing.workers?.paneId ?? todo?.paneId ?? this.parentPane,
          'right', existing.workers || todo ? '0.5' : '0.4', parent.tabId, () => forceRole('dag'));
        if (!dag) return {};
        const workers = await this.ensureRole('workers', todo?.paneId ?? dag.paneId,
          todo ? 'down' : 'right', '0.5', parent.tabId, () => forceRole('workers'), todo ? null : dag.paneId);
        this.status = workers ? 'current' : 'layout-waiting';
        return { dag, workers };
      }
      if (!existing.dag && existing.workers && todo) {
        // Closing the top DAG collapses the right-hand tree to workers/Todo
        // at 50/50. Restore their owned boundary before inserting the DAG.
        const geometry = async () => {
          const layout = (await this.herdr('layout', '--pane', existing.workers.paneId))?.layout;
          const rect = id => layout?.panes?.filter(pane => pane.pane_id === id);
          const rows = [this.parentPane, existing.workers.paneId, todo.paneId].map(rect);
          if (rows.some(values => values?.length !== 1)) throw new Error('layout-waiting: ambiguous owned geometry');
          const [parentRect, workerRect, todoRect] = rows.map(values => values[0].rect);
          if ([parentRect, workerRect, todoRect].some(value => !value ||
            !['x', 'y', 'width', 'height'].every(key => Number.isFinite(value[key])) || value.width <= 0 || value.height <= 0) ||
            workerRect.x !== parentRect.x + parentRect.width || workerRect.x !== todoRect.x ||
            workerRect.width !== todoRect.width || workerRect.y !== parentRect.y ||
            workerRect.y + workerRect.height !== todoRect.y || workerRect.height + todoRect.height !== parentRect.height)
            throw new Error('layout-waiting: non-contiguous owned region');
          return { parentRect, workerRect, todoRect };
        };
        const before = await geometry(), total = before.parentRect.height, desired = Math.round(total * .7);
        if (before.workerRect.height !== desired) {
          await this.membership(existing.workers.paneId); await this.membership(todo.paneId);
          await this.mutate('workers', 'arrange', existing.workers.paneId,
            ['resize', '--pane', existing.workers.paneId, '--direction', before.workerRect.height < desired ? 'down' : 'up',
              '--amount', String(Math.abs(desired - before.workerRect.height) / total)],
            { paneId: existing.workers.paneId, rect: { ...before.workerRect, height: desired } });
          const after = await geometry();
          if (after.workerRect.height !== desired || JSON.stringify(after.parentRect) !== JSON.stringify(before.parentRect) ||
            after.workerRect.x !== before.workerRect.x || after.workerRect.width !== before.workerRect.width)
            throw new Error('layout-waiting: owned boundary resize mismatch');
        }
      }
      const dagAnchor = existing.workers?.paneId ?? todo?.paneId ?? this.parentPane;
      const sibling = Boolean(existing.workers || todo);
      const dag = await this.ensureRole('dag', dagAnchor, sibling ? 'down' : 'right',
        existing.workers && todo ? String(4 / 7) : sibling ? '0.4' : '0.65', parent.tabId, () => forceRole('dag'),
        sibling ? dagAnchor : null);
      if (!dag) return {};
      const workers = await this.ensureRole('workers', todo?.paneId ?? dag.paneId, 'down',
        todo ? '0.5' : '0.4', parent.tabId, () => forceRole('workers'), todo?.paneId);
      this.status = workers ? 'current' : 'layout-waiting';
      return { dag, workers };
    });
  }
  async ensureRole(role, anchor, direction, ratio, tabId, force, beforePane) {
    if (force()) await this.roleTail;
    let record = await readJson(this.paneFile(role));
    // Actor tombstones retain their evidence files. Revalidate the held owner's
    // absence proof before replacing a locator from an obsolete generation.
    if (this.managedAvailable && record && !record.manualClose &&
      this.recordMatches({ ...record, viewerPath: this.viewer }, role) && typeof record.nodePath === 'string' &&
      this.coordinator?.actor?.snapshot.closures.some(entry => entry.role === role && entry.paneId === record.paneId &&
        entry.sessionId === record.sessionId && entry.reason === 'pane_not_found')) {
      if (typeof this.node === 'function') this.node = await this.node();
      if (pathKey(record.viewerPath) !== pathKey(this.viewer) || pathKey(record.nodePath) !== pathKey(this.node)) {
        await this.coordinator.obsoleteLocator(record);
        record = null;
      }
    }
    if (record && !this.recordMatches(record, role)) {
      if (this.managedAvailable) throw blocked('retained fixed record identity changed');
      return;
    }
    if (record?.manualClose && !force()) return;
    if (record?.paneId && record.prepared && !record.processId) {
      const member = await this.membership(record.paneId);
      const identity = member.tabId === record.tabId &&
        await legacyPaneIdentity(await this.herdr('process-info', '--pane', record.paneId), record, this.inspectShell);
      const state = await readJson(this.roleFile(role));
      if (!identity || identity.viewer || !state || !sameOwner(state, record) ||
        state.paneId !== record.paneId || state.launchToken !== record.launchToken)
        throw blocked('PREPARED fixed shell or source changed or occupied');
      record = { ...record, scopeEpoch: this.scopeEpoch, layoutClaim: this.claim };
      this.records.set(role, record);
      await this.roleWrite(async () => {
        await writeJson(this.paneFile(role), record);
        await writeJson(this.roleFile(role), this.roleState(record));
      });
      return this.launchRole(record);
    }
    if (record?.paneId) {
      let process;
      try { process = await this.verify(record); } catch (error) {
        if (!/missing or ambiguous/.test(error.message)) throw error;
      }
      if (process) {
        record = await this.roleWrite(async () => {
          const current = await readJson(this.paneFile(role));
          if (this.stopped || current.launchToken !== record.launchToken || current.manualClose && !force()) return null;
          const next = { ...current, scopeEpoch: this.scopeEpoch, layoutClaim: this.claim,
            processId: process.pid, ready: true, manualClose: false };
          this.records.set(role, next);
          await writeJson(this.paneFile(role), next);
          await writeJson(this.roleFile(role), this.roleState(next));
          return next;
        });
        if (!record) return;
        if (this.managedAvailable) await this.coordinator.registerRole(role, () => this.nativeTarget(record));
        return record;
      }
      const panes = (await this.herdr('list'))?.panes ?? [];
      if (panes.some(pane => pane.pane_id === record.paneId)) {
        const death = this.managedAvailable && this.coordinator.viewerDeath(role);
        if (death && death.record.launchToken === record.launchToken &&
          death.identity.pid === record.processId && death.identity.birth === record.viewerBirth) {
          const observed = await observePaneShell(await this.herdr('process-info', '--pane', record.paneId),
            record, this.inspectShell);
          if (!observed || observed.children.length || observed.shell.Birth !== death.target.shell.birth)
            throw blocked('dead fixed viewer did not leave the original empty shell');
          const next = { ...record, launchToken: randomBytes(24).toString('hex'), prepared: true, ready: false };
          delete next.processId; delete next.viewerBirth;
          await this.roleWrite(async () => {
            const current = await readJson(this.paneFile(role));
            if (current.manualClose) throw blocked('manual closure superseded fixed viewer repair');
            this.records.set(role, next);
            await writeJson(this.paneFile(role), next); await writeJson(this.roleFile(role), this.roleState(next));
          });
          this.coordinator.repairedViewer(role);
          return this.launchRole(next);
        }
        if (this.managedAvailable) throw blocked('retained fixed native identity unverified');
        return;
      }
      if (this.managedAvailable) {
        await this.coordinator.reconcileLegacy(this.stateDir, legacyPaneIdentity);
        const closure = this.coordinator.actor.snapshot.closures.find(entry => entry.role === role &&
          entry.paneId === record.paneId && entry.sessionId === record.sessionId && entry.reason === 'pane_not_found');
        if (!closure) throw blocked('fixed pane absence unverified');
        if (!force()) return;
      }
      if (!force()) {
        record = { ...record, manualClose: true }; this.records.set(role, record);
        await this.roleWrite(() => writeJson(this.paneFile(role), record)); return;
      }
    } else if (record?.attempted && !force()) return;
    await this.membership(anchor);
    const focusBefore = beforePane ? await this.placementFocus(tabId, beforePane) : null;
    if (typeof this.node === 'function') this.node = await this.node();
    const launchToken = randomBytes(24).toString('hex');
    record = { schema: 'omo-herdr-dashboard/pane/1', ...this.binding, scopeEpoch: this.scopeEpoch,
      role, tabId, statePath: this.roleFile(role), viewerPath: this.viewer, launchToken,
      nodePath: this.node, layoutClaim: this.claim, attempted: true, ready: false, manualClose: false };
    const prepared = await this.roleWrite(async () => {
      const current = await readJson(this.paneFile(role));
      if (current?.manualClose && !force()) return false;
      await writeJson(this.paneFile(role), record);
      return true;
    });
    if (!prepared) return;
    if (this.stopped) return;
    const result = await this.mutate(role, 'split', anchor,
      ['split', '--pane', anchor, '--direction', direction, '--ratio', ratio, '--cwd', this.cwd, '--no-focus']);
    if (!result?.pane?.pane_id) throw new Error('Owned fixed-role split returned no pane ID');
    record.paneId = result.pane.pane_id;
    await this.membership(record.paneId);
    const createdInfo = await this.herdr('process-info', '--pane', record.paneId);
    record.creationProcessId = createdInfo?.process_info?.shell_pid;
    const shell = await observePaneShell(createdInfo, record, this.inspectShell);
    if (shell) { record.creationDate = shell.shell.CreationDate; if (this.managedAvailable) record.creationBirth = shell.shell.Birth; }
    record.prepared = true;
    if (beforePane) {
      try {
        const current = await this.placementFocus(tabId, beforePane);
        if (current !== focusBefore) throw new Error('layout-waiting: input selection changed before placement');
        await this.mutate(role, 'arrange', record.paneId, ['swap', '--source-pane', record.paneId, '--target-pane', beforePane]);
        await this.restorePlacementFocus(record.paneId, focusBefore, tabId, beforePane);
      } catch (error) {
        if (this.managedAvailable) throw error;
        const selected = (await this.herdr('list'))?.panes?.filter(pane => pane.focused);
        if (selected?.length === 1 && selected[0].pane_id !== record.paneId &&
          await canCleanupPane(await this.herdr('process-info', '--pane', record.paneId), record, this.inspectShell)) {
          await this.membership(record.paneId); await this.herdr('close', record.paneId); record.paneId = null;
        }
        record.failed = true;
        await this.roleWrite(() => writeJson(this.paneFile(role), record));
        throw error;
      }
    }
    this.records.set(role, record);
    await this.roleWrite(async () => {
      await writeJson(this.paneFile(role), record);
      await writeJson(this.roleFile(role), this.roleState(record));
    });
    return this.launchRole(record);
  }
  async launchRole(record) {
    const role = record.role, launchToken = record.launchToken;
    const abort = new AbortController();
    const creation = once(this.roleEvents, `created:${launchToken}`,
      { signal: AbortSignal.any([abort.signal, ...(this.managedAvailable ? [] : [this.stopSignal.signal]), AbortSignal.timeout(10000)]) });
    creation.catch(() => {});
    this.pendingLaunches.add(launchToken);
    try {
      await this.mutate(role, 'run', record.paneId, ['run', record.paneId, shellCommand([this.node, this.viewer, '--view',
        role === 'dag' ? 'dag' : 'tasks', '--state', this.roleFile(role), '--close-pane', record.paneId,
        '--launch-token', launchToken])], creation);
      await creation;
      if (this.managedAvailable) {
        const registration = this.launchRegistrations.get(launchToken);
        if (!registration) throw blocked('authenticated launch registration missing');
        await registration;
      }
      return this.records.get(role);
    } catch (error) {
      if (this.managedAvailable) throw error;
      const current = await readJson(this.paneFile(role));
      if (!this.stopped && current?.launchToken === launchToken && Number.isSafeInteger(record.creationProcessId)) {
        let info;
        try {
          await this.membership(record.paneId);
          info = await this.herdr('process-info', '--pane', record.paneId);
        } catch (cleanupError) {
          error.cause = cleanupError;
          throw error;
        }
        if (await canCleanupPane(info, record, this.inspectShell)) {
          await this.herdr('close', record.paneId);
          const failed = { ...current, paneId: null, ready: false, failed: true };
          this.records.set(role, failed);
          await this.roleWrite(() => writeJson(this.paneFile(role), failed));
        }
      }
      throw error;
    } finally { this.pendingLaunches.delete(launchToken); this.launchRegistrations.delete(launchToken); abort.abort(); }
  }
  arrangeDashboard() {
    return this.enqueue(async () => {
      if (this.stopped || this.claim?.layout !== 'columns') return;
      const pair = await this.inspectPair(), todo = await this.coordinator?.inspectTodo?.();
      if (!pair.dag || !pair.workers || !todo) return;
      const tabId = this.claim.tabId;
      if (!sameOwner(todo, this.binding) || todo.scopeEpoch !== this.scopeEpoch || todo.ready !== true ||
        todo.manualClose || todo.tabId !== tabId) throw new Error('layout-waiting: invalid Todo placement owner');
      const ids = { parent: this.parentPane, todo: todo.paneId, workers: pair.workers.paneId, dag: pair.dag.paneId };
      const geometry = async () => (await this.herdr('layout', '--pane', this.parentPane))?.layout;
      const layout = await geometry();
      if (isColumnDashboard(layout, ids)) return;
      // Moving a pane collapses its old branch. A foreign pane must never be
      // resized as an incidental result of that collapse.
      if (layout?.tab_id !== tabId || layout.panes?.length !== 4 ||
        layout.panes.some(pane => !Object.values(ids).includes(pane.pane_id)))
        throw new Error('layout-waiting: dashboard region contains unowned panes');
      for (const id of Object.values(ids)) await this.membership(id);
      const selected = await this.placementFocus(tabId, todo.paneId);
      let temporaryPaneId;
      const guard = async () => {
        if (this.stopped || await this.placementFocus(tabId, todo.paneId) !== selected)
          throw new Error('layout-waiting: input selection changed during placement');
        if (!await this.verify(pair.dag) || !await this.verify(pair.workers) ||
          (await this.coordinator.inspectTodo())?.paneId !== todo.paneId)
          throw new Error('layout-waiting: dashboard owner changed during placement');
        const current = await geometry(), owned = [...Object.values(ids), ...(temporaryPaneId ? [temporaryPaneId] : [])];
        if (current?.tab_id !== tabId || current.panes?.length !== owned.length ||
          current.panes.some(pane => !owned.includes(pane.pane_id)))
          throw new Error('layout-waiting: dashboard region contains unowned panes');
      };
      const move = async (id, target, direction) => {
        await guard();
        if (!this.managedAvailable) {
          await this.mutate('dag', 'arrange', id, ['move', id, '--tab', tabId, '--target-pane', target, '--split', direction, '--no-focus']);
          await guard();
          return;
        }
        const journalPath = join(this.directory, 'placement.json');
        const previous = await readJson(journalPath);
        if (previous && previous.phase !== 'COMPLETE') {
          this.status = 'BLOCKED';
          throw blocked('unresolved owned temporary placement');
        }
        const source = await this.coordinator.target(id), anchor = await this.coordinator.target(target);
        const actor = this.coordinator.actor, lifetimeId = this.lifetimeId;
        let temporary, stage = 'PREPARED';
        const journal = { schema: 'omo-herdr-dashboard/temporary-placement/1', ...this.binding,
          lifetimeId, ownerNonce: actor.snapshot.ownerNonce, generation: actor.snapshot.generation,
          source, anchor, direction, selected, phase: stage, beforeLayout: await geometry(), temporary: null };
        const saveJournal = async phase => {
          journal.phase = phase;
          journal.temporary = temporary ?? null;
          journal.operations = actor.snapshot.operations;
          await this.roleWrite(() => writeJson(journalPath, journal));
        };
        const identities = async () => {
          if (this.coordinator.actor !== actor || this.lifetimeId !== lifetimeId ||
            actor.snapshot.ownerNonce !== journal.ownerNonce || actor.snapshot.generation !== journal.generation)
            throw blocked('placement owner or lifetime changed');
          this.coordinator.assertCurrent();
          if (JSON.stringify(await this.coordinator.target(id)) !== JSON.stringify(source) ||
            JSON.stringify(await this.coordinator.target(target)) !== JSON.stringify(anchor))
            throw blocked('placement native source or anchor changed');
        };
        const closeTemporary = async () => {
          // target() accepts an unregistered temporary shell only when its exact
          // native membership is current and it has no foreground child.
          const current = await this.coordinator.target(temporary.paneId);
          if (JSON.stringify(current) !== JSON.stringify(temporary))
            throw blocked('temporary native shell changed');
          await saveJournal('CLOSE_PREPARED');
          await this.coordinator.mutate('dag', 'close', temporary, ['close', temporary.paneId]);
          temporary = undefined;
          temporaryPaneId = undefined;
        };
        await identities(); await saveJournal(stage);
        try {
          const created = await this.mutate('dag', 'split', target,
            ['split', '--pane', target, '--direction', direction, '--ratio', '0.5', '--cwd', this.cwd, '--no-focus']);
          if (!created?.pane?.pane_id) throw blocked('temporary split has no native receipt');
          temporaryPaneId = created.pane.pane_id;
          journal.createdPaneId = temporaryPaneId;
          journal.splitReceipt = created;
          await saveJournal('SPLIT_APPLIED');
          temporary = await this.coordinator.target(created.pane.pane_id);
          stage = 'SPLIT_SETTLED'; await saveJournal(stage);
          await guard(); await identities();
          await saveJournal('SWAP_PREPARED');
          await this.mutate('dag', 'arrange', id,
            ['swap', '--source-pane', id, '--target-pane', temporary.paneId]);
          stage = 'SWAP_SETTLED'; await saveJournal(stage);
          await this.restorePlacementFocus(id, selected, tabId, todo.paneId);
          await guard(); await identities();
          await closeTemporary();
          stage = 'COMPLETE'; await saveJournal(stage);
        } catch (error) {
          this.status = 'BLOCKED';
          journal.failedStage = stage;
          journal.error = { code: error.code, message: error.message };
          try { journal.failureLayout = await geometry(); }
          catch (layoutError) { journal.layoutError = { code: layoutError.code, message: layoutError.message }; }
          // Never replay or mutate after an UNKNOWN actor effect. A known,
          // settled temporary split can be closed only through the current
          // capability and the same independently verified empty-shell identity.
          if (temporary && !actor.blocked && actor.leaseHeld && !actor.draining) {
            try { await guard(); await identities(); await closeTemporary(); journal.cleanup = 'exact-empty-shell-closed'; }
            catch (cleanupError) { journal.cleanupError = { code: cleanupError.code, message: cleanupError.message }; }
          }
          await saveJournal('BLOCKED');
          throw error;
        }
        await guard();
      };
      const swap = async (id, target) => {
        await guard();
        await this.mutate('dag', 'arrange', id, ['swap', '--source-pane', id, '--target-pane', target]);
        await this.restorePlacementFocus(id, selected, tabId, todo.paneId);
        await guard();
      };
      await move(ids.dag, ids.parent, 'right');
      await move(ids.workers, ids.dag, 'right');
      await swap(ids.workers, ids.dag);
      await move(ids.todo, ids.workers, 'down');
      await swap(ids.todo, ids.workers);
      const placed = await geometry();
      if (!isColumnDashboard(placed, ids)) throw new Error('layout-waiting: dashboard column placement mismatch');
      const parent = placed.panes.find(pane => pane.pane_id === ids.parent).rect;
      const desired = Math.round(placed.area.width * .4);
      if (parent.width !== desired) {
        await guard();
        await this.mutate('dag', 'arrange', ids.parent, ['resize', '--pane', ids.parent, '--direction', parent.width > desired ? 'left' : 'right',
          '--amount', String(Math.abs(parent.width - desired) / placed.area.width)],
          { paneId: ids.parent, rect: { ...parent, width: desired } });
      }
      const final = await geometry();
      if (!isColumnDashboard(final, ids)) throw new Error('layout-waiting: dashboard resize mismatch');
    });
  }
  async placementFocus(tabId, anchor) {
    const panes = (await this.herdr('list'))?.panes ?? [];
    const selected = panes.filter(pane => pane.focused);
    const layout = (await this.herdr('layout', '--pane', anchor))?.layout;
    if (selected.length !== 1 || selected[0].tab_id !== tabId || layout?.tab_id !== tabId ||
      selected[0].pane_id !== layout.focused_pane_id)
      throw new Error('layout-waiting: placement requires the selected owned tab');
    const id = selected[0].pane_id;
    await this.membership(id);
    await this.verifyInputPane(id, anchor, tabId);
    return id;
  }
  async verifyInputPane(id, anchor, tabId) {
    if (id === this.parentPane || id === anchor) return;
    const record = [...this.records.values()].find(value => value.paneId === id && !value.manualClose);
    if (record && await this.verify(record)) return;
    const todo = await this.coordinator?.inspectTodo?.();
    if (todo && sameOwner(todo, this.binding) && todo.role === 'todo' && todo.paneId === id &&
      todo.tabId === tabId && todo.scopeEpoch === this.scopeEpoch && todo.ready === true && !todo.manualClose) return;
    throw new Error('layout-waiting: unverified input pane');
  }
  async restorePlacementFocus(created, original, tabId, anchor) {
    let source = created;
    for (let step = 0; step < 3 && source !== original; step++) {
      const panes = (await this.herdr('list'))?.panes ?? [];
      const selected = panes.filter(pane => pane.focused);
      if (selected.length !== 1 || selected[0].pane_id !== source || selected[0].tab_id !== tabId) return;
      await this.membership(source); await this.membership(original);
      await this.verifyInputPane(original, anchor, tabId);
      const layout = (await this.herdr('layout', '--pane', source))?.layout;
      const rect = id => layout?.panes?.filter(pane => pane.pane_id === id);
      const from = rect(source), to = rect(original);
      if (layout?.tab_id !== tabId || layout.focused_pane_id !== source || from?.length !== 1 || to?.length !== 1)
        throw new Error('layout-waiting: ambiguous focus restoration geometry');
      const a = from[0].rect, b = to[0].rect;
      if (![a, b].every(value => value && ['x', 'y', 'width', 'height'].every(key => Number.isFinite(value[key]))))
        throw new Error('layout-waiting: invalid focus restoration geometry');
      const direction = b.x < a.x ? 'left' : b.x > a.x ? 'right' : b.y < a.y ? 'up' : 'down';
      const neighbor = (await this.herdr('neighbor', '--pane', source, '--direction', direction))?.neighbor;
      const next = neighbor?.neighbor_pane_id;
      if (!next || neighbor.layout?.tab_id !== tabId || neighbor.layout.focused_pane_id !== source)
        throw new Error('layout-waiting: missing focus restoration neighbor');
      await this.membership(next);
      await this.verifyInputPane(next, anchor, tabId);
      const fresh = ((await this.herdr('list'))?.panes ?? []).filter(pane => pane.focused);
      if (fresh.length !== 1 || fresh[0].pane_id !== source || fresh[0].tab_id !== tabId) return;
      await this.mutate('dag', 'arrange', source, ['focus', '--pane', source, '--direction', direction]);
      source = next;
    }
  }
  open() {
    if (this.managedAvailable && !this.coordinator?.actor) return this.enqueue(() => {
      this.status = 'BLOCKED'; throw blocked('managed W coordinator unavailable');
    });
    return this.coordinator ? this.coordinator.schedule({ forceRoles: ['dag', 'workers', 'todo'] }) : this.openPair({ force: true });
  }
  rebase() {
    return this.enqueue(async () => {
      await this.roleWrite(() => {
        this.scopeEpoch++;
        this.offers = {};
      });
      await this.save(true, false);
    });
  }
  async stop() {
    if (this.stopping) return this.stopping;
    if (this.stopped) return;
    this.stopped = true; this.taskData.stop();
    const coordinator = this.coordinator, actor = this.managedActor;
    this.stopSignal.abort();
    this.roleEvents.emit('stopped');
    this.stopping = (async () => {
    await this.queue;
    await Promise.allSettled([...this.controlJobs]);
    await this.roleTail;
    let failure;
    const ownedClaim = this.managedAvailable && actor?.leaseHeld && coordinator?.actor === actor &&
      sameOwner(coordinator.binding, this.binding) &&
      actor.snapshot.participants.dag?.lifetimeId === this.lifetimeId && coordinator.ownsPublisherClaim?.() === true;
    try {
      if (!this.managedAvailable || ownedClaim &&
        await coordinator.terminalAuthority('dag', this.lifetimeId,
          [...this.records.values()].filter(record => record.paneId && !record.manualClose)))
        await this.save(false, false);
    }
    catch (error) { failure = error; }
    finally {
      for (const socket of this.controlSockets) socket.destroy();
      if (this.control?.listening) await new Promise(resolve => this.control.close(resolve));
      this.sourceEvents.removeAllListeners();
    }
    if (this.managedAvailable && ownedClaim && actor?.leaseHeld &&
      actor.snapshot.participants.dag?.lifetimeId === this.lifetimeId)
      await actor.drain('dag', this.lifetimeId);
    if (failure) throw failure;
    })();
    void this.stopping.finally(() => { this.stopping = undefined; }).catch(() => {});
    return this.stopping;
  }
}
