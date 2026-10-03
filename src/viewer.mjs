import { watch } from 'node:fs';
import net from 'node:net';
import { createHash } from 'node:crypto';
import { basename, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { emitKeypressEvents } from 'node:readline';
import { createHerdr } from './herdr.mjs';
import { t } from './i18n.mjs';
import { fit, renderFrame, runningTarget, standaloneTasks } from './render.mjs';
import { readJson } from './storage.mjs';
import { TASK_SCOPE, emptyViewState, isExpanded, loadViewState, saveViewState, setExpanded } from './view-state.mjs';

export function renderFixedFrame(state, options, view) {
  const columns = Math.max(1, (options.columns ?? 54) - 1), rows = Math.max(1, options.rows ?? 48);
  if (view === 'dag' && !state?.runs?.length) {
    const head = ['OMO  /  DAG', `DAG (0) - ${t(state?.language, 'none')}`];
    const body = [options.error || '', state?.connected ? t(state?.language, 'waiting') : t(state?.language, 'disconnected')];
    const foot = [options.error ? t(state?.language, 'readError', { error: options.error }) :
      `f ${t(state?.language, options.follow && state?.connected ? 'followOn' : 'followOff')}`,
      t(state?.language, 'nodeControls'), t(state?.language, 'toggleControls'), t(state?.language, 'controls')];
    const lines = [...head, ...body];
    while (lines.length < Math.max(0, rows - foot.length)) lines.push('');
    return { text: [...lines, ...foot].slice(0, rows).map(line => fit(line, columns)).join('\n'),
      scroll: 0, nodeRanges: {}, taskRanges: {}, graphRanges: {} };
  }
  const result = renderFrame(state, { ...options, view });
  const lines = result.text.split('\n');
  lines[0] = fit(`OMO  /  ${view === 'dag' ? 'DAG' : t(state?.language, 'tasks')}`, columns);
  // Ordinary workers have no run selector. No fixed-role footer invites a mode switch.
  if (view === 'tasks' && lines.length === rows)
    lines[lines.length - 1] = fit('j/k Scroll  q Close', columns);
  return { ...result, text: lines.join('\n') };
}

const controlKeys = ['token', 'ownerKey', 'sessionId', 'scopeEpoch', 'role', 'launchToken', 'paneId', 'statePath'];
const sameControl = (a, b) => a?.v === 1 && b?.v === 1 && controlKeys.every(key => a[key] === b[key]);
const sameScope = (a, b) => a && b && ['ownerKey', 'sessionId', 'scopeEpoch'].every(key => a[key] === b[key]);
function receive(socket, callback) {
  socket.setEncoding('utf8');
  let buffer = '';
  socket.on('data', chunk => {
    buffer += chunk;
    if (buffer.length > 4 * 1024 * 1024) return socket.destroy();
    let cut;
    while ((cut = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, cut); buffer = buffer.slice(cut + 1);
      try { callback(JSON.parse(line)); } catch { socket.destroy(); }
    }
  });
}
function roleControl(type, state, paneId, launchToken) {
  const offer = state?.controlOffer;
  if (!offer?.pipePath?.startsWith('\\\\.\\pipe\\') || paneId !== state.paneId ||
    launchToken !== state.launchToken || !sameScope(offer, state)) return Promise.resolve(false);
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(offer.pipePath);
    const timer = setTimeout(() => { socket.destroy(); reject(new Error('Role control response timed out')); }, 10000);
    socket.once('connect', () => socket.write(`${JSON.stringify({ ...offer, type, pid: process.pid })}\n`));
    receive(socket, message => {
      clearTimeout(timer); socket.end();
      resolve(sameControl(message, offer) && message.type === (type === 'register' ? 'registered' : 'closed'));
    });
    socket.once('error', error => { clearTimeout(timer); reject(error); });
    socket.once('close', () => { clearTimeout(timer); resolve(false); });
  });
}

export async function runViewer() {
const file = process.argv[process.argv.indexOf('--state') + 1];
if (!process.argv.includes('--state') || !file) throw new Error('Usage: node viewer.mjs --state PATH');
const fixedView = process.argv.includes('--view') ? process.argv[process.argv.indexOf('--view') + 1] : undefined;
if (fixedView && !['dag', 'tasks'].includes(fixedView)) throw new Error('Managed --view must be dag or tasks');
const role = fixedView === 'dag' ? 'dag' : 'workers';
const paneId = process.argv.includes('--close-pane') ? process.argv[process.argv.indexOf('--close-pane') + 1] : undefined;
const launchToken = process.argv.includes('--launch-token') ? process.argv[process.argv.indexOf('--launch-token') + 1] : undefined;
let state = await readJson(file);
function validManaged(next) {
  return next?.schema === 'omo-herdr-dashboard/role/1' && next.role === role &&
    next.paneId === paneId && next.launchToken === launchToken &&
    Number.isSafeInteger(next.scopeEpoch) && next.scopeEpoch > 0 &&
    ['socketPath', 'parentPaneId', 'tabId', 'sessionId'].every(key => typeof next[key] === 'string' && next[key]) &&
    next.ownerKey === createHash('sha256').update(JSON.stringify([next.socketPath, next.parentPaneId, next.sessionId]))
      .digest('hex').slice(0, 24);
}
if (fixedView && launchToken && !validManaged(state)) throw new Error('Managed viewer launch identity mismatch');
let selectedId = state?.runs?.[0]?.id;
let view = fixedView ?? (state?.runs?.length ? 'dag' : 'tasks'), selectedTaskId;
let scroll = 0, error = '', timer, drawing = false, again = false;
let viewState = emptyViewState(state?.sessionId), viewError = '', saving = Promise.resolve(), closing = false;
const selectedNodes = new Map();
let revealSelection = false, verbose = false, detailSelection, completedExpanded = false;
let follow = true, target;
let presenceClient, presenceMessage, presenceKey, controlKey, presenceNotice = '';
function bindPresence() {
  if (!fixedView || !interactive || !launchToken || !validManaged(state)) return;
  const control = JSON.stringify(state.controlOffer);
  if (state.controlOffer && control !== controlKey) {
    controlKey = control;
    void roleControl('register', state, paneId, launchToken).catch(cause => { error = cause.message; draw(); });
  }
  const offer = state.presenceOffer;
  const key = JSON.stringify(offer);
  if (key === presenceKey) return;
  presenceKey = key;
  presenceClient?.destroy(); presenceClient = undefined; presenceMessage = undefined;
  if (!offer) { presenceNotice = 'Waiting for presence'; return; }
  if (offer.v !== 1 || offer.role !== role || !sameScope(offer, state) ||
    !offer.pipePath?.startsWith('\\\\.\\pipe\\')) { presenceNotice = 'Invalid presence offer'; return; }
  const client = net.createConnection(offer.pipePath);
  presenceClient = client;
  const binding = message => message?.v === 1 && message.token === offer.token &&
    message.role === role && sameScope(message, offer);
  client.once('connect', () => client.write(`${JSON.stringify({ ...offer, type: 'hello' })}\n`));
  receive(client, message => {
    if (presenceClient !== client || !binding(message)) return client.destroy();
    if (message.type === 'snapshot') {
      if (!sameScope(message.snapshot, offer) || message.sequence !== message.snapshot?.sequence ||
        !Number.isSafeInteger(message.sequence) || message.sequence < 1 ||
        presenceMessage && message.sequence <= presenceMessage.sequence) return client.destroy();
      presenceMessage = message;
      presenceNotice = message.snapshot.availability === 'waiting' ? 'Waiting for source' : '';
      draw();
    } else if (message.type === 'status') {
      presenceMessage = undefined; presenceNotice = message.label ?? message.availability; draw();
    }
  });
  client.on('error', cause => { if (presenceClient === client) { presenceNotice = cause.message; draw(); } });
  client.once('close', () => {
    if (presenceClient !== client) return;
    presenceMessage = undefined; presenceNotice = 'Disconnected; last-known; not live'; draw();
  });
}
function isActive(run) { return run.status === 'running' || run.nodes.some(node => node.state === 'running'); }
function selectableRuns() { return (state?.runs ?? []).filter(run => isActive(run) || completedExpanded); }
async function restorePreferences() {
  const scope = fixedView ? state : undefined;
  try { viewState = await loadViewState(file, state?.sessionId, scope); viewError = ''; }
  catch (cause) { viewState = emptyViewState(state?.sessionId, scope); viewError = cause.message; }
  const navigation = viewState.navigation;
  follow = navigation?.follow ?? true;
  scroll = navigation?.scroll ?? 0;
  view = fixedView ?? navigation?.view ?? view;
  selectedId = navigation?.selectedId ?? selectedId;
  selectedTaskId = navigation?.selectedTaskId;
  selectedNodes.clear();
  for (const pair of navigation?.selectedNodes ?? []) selectedNodes.set(...pair);
  completedExpanded = navigation?.completedExpanded ?? false;
}
await restorePreferences();
const interactive = Boolean(process.stdout.isTTY && process.stdin.isTTY);
function draw() {
  if (closing) return;
  if (!error) target = runningTarget(!fixedView ? state : fixedView === 'dag' ?
    { ...state, tasks: [] } : { ...state, runs: [], tasks: standaloneTasks(state) }, target);
  const running = !error && follow && state?.connected ? target : undefined;
  if (running) {
    if (view !== running.view || running.view === 'dag' && selectedId !== running.runId) scroll = 0;
    view = fixedView ?? running.view;
    if (view === 'dag') selectedId = running.runId;
  }
  let runIndex = state?.runs?.findIndex(run => run.id === selectedId) ?? 0;
  if (runIndex < 0) { runIndex = 0; selectedId = state?.runs?.[0]?.id; }
  const run = state?.runs?.[runIndex];
  if (!fixedView && !state?.runs?.length) view = 'tasks';
  const roots = standaloneTasks(state);
  if (!roots.some(task => task.id === selectedTaskId)) selectedTaskId = roots[0]?.id;
  if (!run?.nodes.some(node => node.id === selectedNodes.get(selectedId))) selectedNodes.set(selectedId, run?.nodes[0]?.id);
  const selection = JSON.stringify([state?.sessionId, view, view === 'tasks' ? null : selectedId,
    view === 'tasks' ? selectedTaskId : selectedNodes.get(selectedId)]);
  if (selection !== detailSelection) { verbose = false; detailSelection = selection; }
  const options = { columns: process.stdout.columns ?? 54, rows: process.stdout.rows ?? 48, runIndex, scroll, color: interactive,
    error, notice: viewError ? t(state?.language, 'viewError', { error: viewError }) : presenceNotice, completedExpanded,
    selectedNodeId: selectedNodes.get(selectedId), selectedTaskId, view, viewState, verbose, revealSelection, follow,
    runningNodeId: running?.nodeId, runningTaskId: running?.taskId };
  const result = fixedView ? renderFixedFrame(state, options, fixedView) : renderFrame(state, options);
  scroll = result.scroll;
  const frame = result.text;
  const client = presenceClient, message = presenceMessage, paintedScope = state;
  process.stdout.write(interactive ? `\x1b[H${frame.replaceAll('\n', '\x1b[K\r\n')}\x1b[K` : `${frame}\n`, cause => {
    if (cause || closing || !client || presenceClient !== client || presenceMessage !== message ||
      !message || !sameScope(state, paintedScope) || !sameScope(message, state) || !state.connected ||
      !['current', 'empty'].includes(message.snapshot.availability) || client.destroyed) return;
    const offer = state.presenceOffer;
    client.write(`${JSON.stringify({ type: 'ack', v: offer.v, token: offer.token, ownerKey: offer.ownerKey,
      sessionId: offer.sessionId, scopeEpoch: offer.scopeEpoch, role, sequence: message.sequence })}\n`);
  });
}
async function refresh() {
  if (drawing) { again = true; return; }
  drawing = true;
  try {
    const next = await readJson(file);
    if (next) {
      if (fixedView && launchToken && !validManaged(next)) throw new Error('Managed viewer launch identity changed');
      const sessionChanged = next.sessionId !== state?.sessionId || fixedView && !sameScope(next, state);
      state = next; error = '';
      if (sessionChanged) {
        await saving; selectedNodes.clear(); selectedTaskId = undefined; scroll = 0;
        selectedId = state?.runs?.[0]?.id; completedExpanded = false;
        follow = true; target = undefined; revealSelection = false;
        view = fixedView ?? (state?.runs?.length ? 'dag' : 'tasks');
        await restorePreferences();
      }
      bindPresence();
    }
    else error = t(state?.language, 'stateMissing');
  } catch (cause) { error = cause.message; }
  draw(); drawing = false;
  if (again) { again = false; void refresh(); }
}
if (process.argv.includes('--once') || !interactive) { draw(); process.exit(0); }
process.stdout.write('\x1b[?1049h\x1b[?25l\x1b]0;OmO DAG\x07');
process.stdin.setRawMode(true);
process.stdin.resume();
process.stdin.setEncoding('utf8');
emitKeypressEvents(process.stdin);
const watcher = watch(dirname(file), (_event, name) => {
  if (name && String(name) !== basename(file)) return;
  clearTimeout(timer); timer = setTimeout(() => { void refresh(); }, 40);
});
watcher.on('error', cause => { error = cause.message; draw(); });
const resize = () => draw();
process.stdout.on('resize', resize);
// Advance only the display clock. Snapshot reads remain driven by fs.watch.
const clock = setInterval(() => {
  if (fixedView && presenceMessage || state?.connected && (state.tasks?.some(task => task.status === 'running') ||
      state.runs?.some(run => run.nodes.some(node => node.state === 'running')))) draw();
}, 1000);
async function close(closePane = false) {
  if (closing) return;
  closing = true;
  clearTimeout(timer); clearInterval(clock); watcher.close(); process.stdout.off('resize', resize);
  presenceClient?.destroy();
  await saving;
  process.stdin.setRawMode(false);
  process.stdout.write('\x1b[?25h\x1b[?1049l');
  if (viewError) process.stderr.write(`${t(state?.language, 'viewError', { error: viewError })}\n`);
  if (closePane && process.argv.includes('--close-pane')) {
    const pane = process.argv[process.argv.indexOf('--close-pane') + 1];
    try {
      if (fixedView) await roleControl('close', state, pane, launchToken);
      else await createHerdr()('close', pane);
    }
    catch (error) { process.stderr.write(`${t(state?.language, 'closeFailed', { error: error.message })}\n`); }
  }
  process.exit(0);
}
process.stdin.on('keypress', (text, pressed) => {
  if (closing) return;
  const key = pressed.sequence || pressed.name || text;
  if (key === 'q' || key === '\x03' || key === '\x04') return void close(true);
  let manual = false, movedScroll = false, movedSelection = false;
  if (key === 'f') { follow = !follow; revealSelection = false; }
  if (['\x1b[B', 'j', '\x1b[A', 'k', '\x1b[6~', '\x1b[5~'].includes(key)) {
    follow = false; revealSelection = false;
    manual = movedScroll = true;
  }
  if (key === '\x1b[B' || key === 'j') scroll++;
  if (key === '\x1b[A' || key === 'k') scroll = Math.max(0, scroll - 1);
  if (key === '\x1b[6~') scroll += Math.max(1, (process.stdout.rows ?? 48) - 8);
  if (key === '\x1b[5~') scroll = Math.max(0, scroll - Math.max(1, (process.stdout.rows ?? 48) - 8));
  if (!fixedView && key === 't' && state?.runs?.length) {
    follow = false;
    manual = movedSelection = true;
    view = view === 'dag' ? 'tasks' : 'dag';
    scroll = 0; revealSelection = true;
  }
  if ((key === 'c' || pressed.name === 'c') && (state?.runs?.length ?? 0) > 1) {
    follow = false;
    manual = movedSelection = true;
    completedExpanded = !completedExpanded;
    scroll = 0; revealSelection = true;
  }
  if ((!fixedView || view === 'dag') && (key === '\x1b[C' || key === '\x1b[D')) {
    follow = false;
    manual = movedSelection = true;
    const runs = selectableRuns();
    const current = Math.max(0, runs.findIndex(run => run.id === selectedId));
    selectedId = runs[(current + (key === '\x1b[C' ? 1 : -1) + runs.length) % runs.length]?.id;
    if (runs.length) view = fixedView ?? 'dag';
    scroll = 0; revealSelection = false;
  }
  const run = state?.runs?.find(run => run.id === selectedId);
  const items = view === 'tasks' ? standaloneTasks(state) : run?.nodes ?? [];
  const scope = view === 'tasks' ? TASK_SCOPE : run?.id;
  const itemId = view === 'tasks' ? selectedTaskId : selectedNodes.get(selectedId);
  if (items.length && (pressed.name === 'tab' || key === 'n' || key === 'p')) {
    follow = false;
    manual = movedSelection = true;
    const current = Math.max(0, items.findIndex(item => item.id === itemId));
    const direction = pressed.shift || key === 'p' ? -1 : 1;
    const nextId = items[(current + direction + items.length) % items.length].id;
    if (view === 'tasks') selectedTaskId = nextId;
    else selectedNodes.set(selectedId, nextId);
    revealSelection = true;
  }
  if (items.length && key === 'd') {
    follow = false;
    manual = true;
    // A temporary detail peek never writes or replaces the saved fold state.
    verbose = !verbose;
    revealSelection = true;
  }
  if (items.length && (key === ' ' || key === '\r' || key === '\n')) {
    follow = false;
    manual = true;
    verbose = false;
    const item = items.find(item => item.id === itemId);
    const status = view === 'tasks' ? item.status : item.state;
    setExpanded(viewState, scope, itemId, !isExpanded(viewState, scope, itemId, status));
    revealSelection = true;
  }
  draw();
  if (manual || key === 'f') {
    viewState.navigation ??= { follow };
    viewState.navigation.follow = follow;
    if (movedScroll || movedSelection) viewState.navigation.scroll = scroll;
    if (movedSelection) {
      viewState.navigation.view = view;
      viewState.navigation.selectedId = selectedId;
      viewState.navigation.selectedTaskId = selectedTaskId;
      viewState.navigation.selectedNodes = [...selectedNodes].filter(pair => pair.every(value => typeof value === 'string'));
      viewState.navigation.completedExpanded = completedExpanded;
    }
    // A transient follow reveal/detail peek never becomes saved manual scroll.
    if (!viewError) {
      const snapshot = structuredClone(viewState);
      saving = saving.then(() => saveViewState(file, snapshot)).catch(cause => {
        viewError = cause.message; draw();
      });
    }
  }
});
process.on('SIGTERM', () => { void close(); }); process.on('SIGINT', () => { void close(); }); process.on('SIGHUP', () => { void close(); });
bindPresence();
draw();
// Recover a replacement that raced with watcher registration.
void refresh();
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await runViewer();
