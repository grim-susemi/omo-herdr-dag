import assert from 'node:assert/strict';
import { test } from 'node:test';
import extension from '../extension.mjs';

test('child lifecycle and explicit command return before UI availability or ownership checks', async () => {
  const handlers = new Map(), commands = new Map();
  const pi = { on(type, callback) { handlers.set(type, callback); return () => {}; },
    registerCommand(name, command) { commands.set(name, command); } };
  extension(pi);
  const child = { sessionManager: { getSessionFile: () => '/synthetic/senpi-task/children/child/native.jsonl' },
    get hasUI() { throw new Error('Child UI was inspected'); } };
  await handlers.get('session_start')({}, child);
  await handlers.get('session_tree')({}, child);
  await commands.get('dag-pane').handler('', child);
  await handlers.get('session_shutdown')();
  assert.equal(commands.size, 1);
});

test('noninteractive lifecycle and commands never create a controller or read a parent session ID', async () => {
  const handlers = new Map(), commands = new Map();
  extension({ on(type, callback) { handlers.set(type, callback); return () => {}; },
    registerCommand(name, command) { commands.set(name, command); } });
  for (const ctx of [{ hasUI: false, mode: 'tui' }, { hasUI: true, mode: 'rpc' }]) {
    ctx.sessionManager = { getSessionFile: () => '/synthetic/parent.jsonl',
      getSessionId() { throw new Error('Noninteractive ownership was inspected'); } };
    await handlers.get('session_start')({}, ctx);
    await commands.get('dag-pane').handler('', ctx);
  }
});
