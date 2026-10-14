import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { FixedDagPane } from './src/controller.mjs';
import { createHerdr, herdrSession } from './src/herdr.mjs';
import { resolveViewerNode } from './src/runtime.mjs';
import { t, languageOf } from './src/i18n.mjs';

export default function extension(pi, { createController = options => new FixedDagPane(options) } = {}) {
  let language = languageOf(process.env.OMO_HERDR_DAG_LANG ?? 'en');
  let controller;
  let unsubscribe;
  let offDashboard = [];
  let generation = 0;
  const channels = { query: 'omo.herdr.dashboard.query', reply: 'omo.herdr.dashboard.reply',
    provider: 'omo.herdr.dashboard.provider', retired: 'omo.herdr.dashboard.retired' };
  let managedService, managedAvailable = false;
  let managedStateDir = process.env.OMO_HERDR_TODO_STATE_DIR ?? join(homedir(), '.omo', 'agent', 'herdr-todo');
  const staticBinding = { socketPath: process.env.HERDR_SOCKET_PATH, parentPaneId: process.env.HERDR_PANE_ID };
  const availability = event => {
    if (event?.v !== 2 || event.kind !== 'managed-availability' ||
      event.socketPath !== staticBinding.socketPath || event.parentPaneId !== staticBinding.parentPaneId) return;
    managedAvailable = true;
    managedService = event.service;
    managedStateDir = event.stateDir;
    if (controller) controller.managedAvailable = true;
  };
  // This load-time handshake precedes every session callback in either order.
  pi.events?.on(channels.provider, availability);
  pi.events?.on(channels.reply, availability);
  pi.events?.emit(channels.query, { v: 2, kind: 'managed-availability', ...staticBinding, requestId: randomUUID() });
  const starts = new WeakMap();
  let lifecycle = Promise.resolve();
  const eligible = ctx => {
    if (/[/\\]senpi-task[/\\]children[/\\]/.test(ctx?.sessionManager?.getSessionFile?.() ?? '')) return false;
    return ctx?.hasUI === true && ctx.mode === 'tui' && herdrSession();
  };
  const viewer = join(dirname(fileURLToPath(import.meta.url)), 'src/viewer.mjs');
  const stateDir = process.env.OMO_HERDR_DAG_STATE_DIR ?? join(homedir(), '.omo', 'agent', 'herdr-dag');

  async function stop() {
    generation++;
    unsubscribe?.();
    unsubscribe = undefined;
    for (const off of offDashboard) off();
    offDashboard = [];
    const old = controller;
    controller = undefined;
    if (old) pi.events.emit(channels.retired, { v: 1, ...old.binding, lifetimeId: old.lifetimeId });
    await old?.stop();
  }

  async function start(event, ctx) {
    if (!eligible(ctx)) return;
    if (starts.has(event)) return starts.get(event);
    const retiring = stop(); // Invalidate this publisher before any await.
    const wanted = generation;
    const starting = lifecycle.then(async () => {
    await retiring;
    if (wanted !== generation) return;
    const todo = await managedService?.start(event, ctx);
    if (wanted !== generation) return;
    let installedLanguage = 'en';
    try { installedLanguage = JSON.parse(readFileSync(new URL('./locale.json', import.meta.url), 'utf8')).language; }
    catch (error) { if (error.code !== 'ENOENT') console.warn(`DAG pane: Cannot read locale configuration: ${error.message}`); }
    language = languageOf(process.env.OMO_HERDR_DAG_LANG ?? installedLanguage);
    const current = generation;
    controller = createController({ sessionId: ctx.sessionManager.getSessionId(),
      parentPane: process.env.HERDR_PANE_ID, socket: process.env.HERDR_SOCKET_PATH,
      stateDir, cwd: pi.cwd, node: () => resolveViewerNode({ language }), viewer, herdr: createHerdr(), language,
      taskStateDir: process.env.OMO_HERDR_DAG_TASK_STATE_DIR,
      scopeEpoch: todo?.scopeEpoch, managedAvailable, managedStateDir,
      notify: message => ctx.ui.notify(message, 'warning') });
    const owner = controller;
    if (todo) owner.provider().setCoordinator(todo.coordinator());
    // Confirmed in senpi/dist/core/event-bus.js and extensions/loader.js:
    // pi.rpc.emit forwards {name, data} through this shared event channel.
    unsubscribe = pi.events.on('senpi:extension-rpc-event', event => {
      if (event?.name !== 'omo.dag.updated' && event?.name !== 'omo.task.updated') return;
      // DagPane.enqueue reports rejected jobs; the event bus cannot await them.
      if (controller !== owner || owner.stopped) return;
      try { void (event.name === 'omo.dag.updated' ? owner.receive(event.data) : owner.receiveTasks(event.data))?.catch(() => {}); }
      catch (error) { ctx.ui.notify(`DAG pane: ${error.message}`, 'warning'); }
    });
    const matches = event => event?.v === 1 && ['socketPath', 'parentPaneId', 'sessionId', 'ownerKey']
      .every(key => event[key] === owner.binding[key]);
    const accept = event => {
      if (controller !== owner || owner.stopped || !matches(event) || event.kind !== 'todo-owner') return;
      owner.provider().setCoordinator(event.provider);
      void owner.coordinator?.schedule().catch(() => {});
    };
    offDashboard = [pi.events.on(channels.provider, accept), pi.events.on(channels.reply, accept),
      pi.events.on(channels.query, event => {
        if (controller === owner && !owner.stopped && matches(event) && event.kind === 'dag-provider')
          pi.events.emit(channels.reply, { v: 1, requestId: event.requestId, kind: 'dag-provider',
            ...owner.binding, provider: owner.provider() });
      }),
      pi.events.on(channels.retired, event => {
        if (matches(event) && event.lifetimeId === owner.coordinator?.lifetimeId) {
          owner.coordinator = undefined;
          owner.status = 'BLOCKED';
        }
      })];
    try { await owner.start(); }
    catch {
      if (controller === owner) controller = undefined;
      unsubscribe?.(); unsubscribe = undefined;
      for (const off of offDashboard) off();
      offDashboard = [];
      await owner.stop();
      return;
    }
    if (generation !== current || controller !== owner || owner.stopped) return;
    pi.events.emit(channels.provider, { v: 1, kind: 'dag-provider', ...owner.binding, provider: owner.provider() });
    pi.events.emit(channels.query, { v: 1, requestId: randomUUID(), kind: 'todo-owner', ...owner.binding });
    // Own source readiness is recorded. Peer handlers and viewer creation are
    // scheduled outside native lifecycle dispatch, in either registration order.
    void (owner.coordinator ? owner.coordinator.schedule() : owner.openPair()).catch(() => {});
    });
    starts.set(event, starting);
    lifecycle = starting.catch(() => {});
    return starting;
  }
  pi.on('session_start', start);
  pi.on('session_tree', start);
  pi.on('session_shutdown', async () => { await stop(); await lifecycle; });
  pi.registerCommand('dashboard', {
    description: 'Open or reopen the Todo, worker and DAG dashboard.',
    handler: async (_args, ctx) => {
      if (!eligible(ctx) || !controller) return;
      await controller.open().catch(() => {});
    },
  });
}
