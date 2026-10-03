import { createHash, randomBytes, randomUUID } from 'node:crypto';
import net from 'node:net';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { EventEmitter, once } from 'node:events';
import { readdir } from 'node:fs/promises';
import { basename, join } from 'node:path';
import { readJson, writeJson } from './storage.mjs';
import { pruneExpiredSnapshots, retentionDaysFromEnv } from './retention.mjs';
import { shellCommand } from './herdr.mjs';
import { normalizeRun, sessionRuns } from './model.mjs';
import { t, languageOf } from './i18n.mjs';
import { TaskData } from './task-data.mjs';

export function viewKey(socket, pane, session) {
  return createHash('sha256').update(JSON.stringify([socket, pane, session])).digest('hex').slice(0, 24);
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
const controlKeys = ['token', 'ownerKey', 'sessionId', 'scopeEpoch', 'role', 'launchToken', 'paneId', 'statePath'];
const sameControl = (a, b) => a?.v === 1 && b?.v === 1 && controlKeys.every(key => a[key] === b[key]);

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
    `[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(); $ErrorActionPreference = 'Stop'; $rows = @(Get-CimInstance Win32_Process -Filter "ProcessId = ${pid} OR ParentProcessId = ${pid}" | Select-Object ProcessId,ParentProcessId,ExecutablePath,CommandLine,@{Name='CreationDate';Expression={$_.CreationDate.ToUniversalTime().ToString('o')}}); ConvertTo-Json -InputObject $rows -Compress`],
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
  if (child.ProcessId !== pid ||
    pathKey(child.ExecutablePath) !== pathKey(record.nodePath)) return null;
  const argv = parseLaunchCommand(child.CommandLine);
  if (!argv) return null;
  return processMatch({ process_info: { foreground_processes: [{ pid, argv }] } }, record, pid);
}
export async function canCleanupPane(info, record, inspect) {
  if (info?.process_info?.shell_pid !== record.creationProcessId) return false;
  if (process.platform !== 'win32') return (info.process_info.foreground_processes ?? [])
    .every(row => row.pid === record.creationProcessId) || Boolean(processMatch(info, record));
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
        if (!this.stopped && sameOwner(coordinator?.binding, this.binding)) this.coordinator = coordinator;
      },
      openPair: options => this.openPair(options),
      setOffers: offers => this.setOffers(offers), inspectPair: () => this.inspectPair(),
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
        void this.handleControl(message, socket).catch(() => socket.destroy());
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
    return paneViewerProcess(await this.herdr('process-info', '--pane', record.paneId), record, pid, this.inspectShell);
  }
  async handleControl(message, socket) {
    if (this.stopped || !['dag', 'workers'].includes(message?.role) ||
      !['register', 'close'].includes(message.type)) return socket.destroy();
    const role = message.role;
    const record = await readJson(this.paneFile(role));
    const state = await readJson(this.roleFile(role));
    if (!sameControl(message, state?.controlOffer) || message.scopeEpoch !== this.scopeEpoch ||
      !this.recordMatches(record, role) || record.launchToken !== message.launchToken ||
      !Number.isSafeInteger(message.pid) || !(await this.verify(record, message.pid))) return socket.destroy();
    await this.roleWrite(async () => {
      if (this.stopped) return socket.destroy();
      const current = await readJson(this.paneFile(role));
      if (current.launchToken !== record.launchToken || current.scopeEpoch !== message.scopeEpoch ||
        message.scopeEpoch !== this.scopeEpoch) return socket.destroy();
      if (message.type === 'register') {
        const ready = { ...current, processId: message.pid, ready: true };
        this.records.set(role, ready); await writeJson(this.paneFile(role), ready);
        socket.write(`${JSON.stringify({ ...state.controlOffer, type: 'registered' })}\n`);
        this.roleEvents.emit(`created:${message.launchToken}`, record.paneId);
      } else {
        const closed = { ...current, manualClose: true };
        this.records.set(role, closed); await writeJson(this.paneFile(role), closed);
        await new Promise((resolve, reject) => {
          socket.end(`${JSON.stringify({ ...state.controlOffer, type: 'closed' })}\n`, () => {
            this.herdr('close', record.paneId).then(resolve, reject);
          });
        });
      }
    });
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
      if (record && await this.verify(record)) pair[role] = record;
    }
    return pair;
  }
  openPair({ force = false, offers, claim } = {}) {
    return this.enqueue(async () => {
      if (this.stopped) return {};
      if (claim && (!sameOwner(claim, this.binding) || this.coordinator &&
        claim.lifetimeId !== this.coordinator.lifetimeId)) return {};
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
      const dag = await this.ensureRole('dag', this.parentPane, 'right', '0.65', parent.tabId, force);
      if (!dag) return {};
      const workers = await this.ensureRole('workers', dag.paneId, 'down', '0.4', parent.tabId, force);
      this.status = workers ? 'current' : 'layout-waiting';
      return { dag, workers };
    });
  }
  async ensureRole(role, anchor, direction, ratio, tabId, force) {
    if (force) await this.roleTail;
    let record = await readJson(this.paneFile(role));
    if (record && !this.recordMatches(record, role)) return;
    if (record?.manualClose && !force) return;
    if (record?.paneId) {
      let process;
      try { process = await this.verify(record); } catch (error) {
        if (!/missing or ambiguous/.test(error.message)) throw error;
      }
      if (process) {
        record = await this.roleWrite(async () => {
          const current = await readJson(this.paneFile(role));
          if (this.stopped || current.launchToken !== record.launchToken || current.manualClose && !force) return null;
          const next = { ...current, scopeEpoch: this.scopeEpoch, layoutClaim: this.claim,
            processId: process.pid, ready: true, manualClose: false };
          this.records.set(role, next);
          await writeJson(this.paneFile(role), next);
          await writeJson(this.roleFile(role), this.roleState(next));
          return next;
        });
        if (!record) return;
        return record;
      }
      const panes = (await this.herdr('list'))?.panes ?? [];
      if (panes.some(pane => pane.pane_id === record.paneId)) return;
      if (!force) {
        record = { ...record, manualClose: true }; this.records.set(role, record);
        await this.roleWrite(() => writeJson(this.paneFile(role), record)); return;
      }
    } else if (record?.attempted && !force) return;
    await this.membership(anchor);
    if (typeof this.node === 'function') this.node = await this.node();
    const launchToken = randomBytes(24).toString('hex');
    record = { schema: 'omo-herdr-dashboard/pane/1', ...this.binding, scopeEpoch: this.scopeEpoch,
      role, tabId, statePath: this.roleFile(role), viewerPath: this.viewer, launchToken,
      nodePath: this.node, layoutClaim: this.claim, attempted: true, ready: false, manualClose: false };
    await this.roleWrite(() => writeJson(this.paneFile(role), record));
    if (this.stopped) return;
    const result = await this.herdr('split', '--pane', anchor, '--direction', direction, '--ratio', ratio,
      '--cwd', this.cwd, '--no-focus');
    if (!result?.pane?.pane_id) throw new Error('Owned fixed-role split returned no pane ID');
    record.paneId = result.pane.pane_id;
    await this.membership(record.paneId);
    const createdInfo = await this.herdr('process-info', '--pane', record.paneId);
    record.creationProcessId = createdInfo?.process_info?.shell_pid;
    const shell = await observePaneShell(createdInfo, record, this.inspectShell);
    if (shell) record.creationDate = shell.shell.CreationDate;
    this.records.set(role, record);
    await this.roleWrite(async () => {
      await writeJson(this.paneFile(role), record);
      await writeJson(this.roleFile(role), this.roleState(record));
    });
    const abort = new AbortController();
    const creation = once(this.roleEvents, `created:${launchToken}`,
      { signal: AbortSignal.any([abort.signal, this.stopSignal.signal, AbortSignal.timeout(10000)]) });
    creation.catch(() => {});
    try {
      await this.herdr('run', record.paneId, shellCommand([this.node, this.viewer, '--view',
        role === 'dag' ? 'dag' : 'tasks', '--state', this.roleFile(role), '--close-pane', record.paneId,
        '--launch-token', launchToken]));
      await creation;
      return this.records.get(role);
    } catch (error) {
      const current = await readJson(this.paneFile(role));
      if (!this.stopped && current?.launchToken === launchToken && Number.isSafeInteger(record.creationProcessId)) {
        await this.membership(record.paneId);
        const info = await this.herdr('process-info', '--pane', record.paneId);
        if (await canCleanupPane(info, record, this.inspectShell)) {
          await this.herdr('close', record.paneId);
          const failed = { ...current, paneId: null, ready: false, failed: true };
          this.records.set(role, failed);
          await this.roleWrite(() => writeJson(this.paneFile(role), failed));
        }
      }
      throw error;
    } finally { abort.abort(); }
  }
  open() {
    return this.coordinator ? this.coordinator.schedule({ forceRoles: ['dag', 'workers'] }) : this.openPair({ force: true });
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
    if (this.stopped) return;
    this.stopped = true; this.taskData.stop();
    this.stopSignal.abort();
    this.roleEvents.emit('stopped');
    await this.queue;
    try { await this.save(false, false); }
    finally {
      for (const socket of this.controlSockets) socket.destroy();
      if (this.control?.listening) await new Promise(resolve => this.control.close(resolve));
      this.sourceEvents.removeAllListeners();
    }
  }
}
