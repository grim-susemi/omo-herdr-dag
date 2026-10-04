import assert from 'node:assert/strict';
import { test } from 'node:test';
import { FixedDagPane, isColumnDashboard } from '../src/controller.mjs';

const columns = {
  area: { x: 0, y: 0, width: 172, height: 46 },
  panes: [
    { pane_id: 'parent', rect: { x: 0, y: 0, width: 68, height: 46 } },
    { pane_id: 'todo', rect: { x: 68, y: 0, width: 52, height: 23 } },
    { pane_id: 'workers', rect: { x: 68, y: 23, width: 52, height: 23 } },
    { pane_id: 'dag', rect: { x: 120, y: 0, width: 52, height: 46 } },
  ],
};
const ids = { parent: 'parent', todo: 'todo', workers: 'workers', dag: 'dag' };
test('column dashboard requires Todo above workers and full-height DAG to their right', () => {
  assert.equal(isColumnDashboard(columns, ids), true);
  for (const role of ['todo', 'workers', 'dag']) {
    const broken = structuredClone(columns);
    broken.panes.find(p => p.pane_id === role).rect.y++;
    assert.equal(isColumnDashboard(broken, ids), false);
  }
  const reversed = structuredClone(columns);
  [reversed.panes[1].pane_id, reversed.panes[2].pane_id] = [reversed.panes[2].pane_id, reversed.panes[1].pane_id];
  assert.equal(isColumnDashboard(reversed, ids), false);
  const foreign = structuredClone(columns);
  foreign.panes.push({ pane_id: 'foreign', rect: columns.panes[0].rect });
  assert.equal(isColumnDashboard(foreign, ids), false);
});

test('owned stacked migration keeps pane identities, preserves input selection and is idempotent', async () => {
  const binding = { socketPath: 'qa:socket', parentPaneId: 'parent', sessionId: 'qa:session', ownerKey: 'qa:owner' };
  const todo = { ...binding, paneId: 'todo', tabId: 'qa:tab', scopeEpoch: 1, ready: true };
  const pair = { dag: { ...binding, paneId: 'dag' }, workers: { ...binding, paneId: 'workers' } };
  const final = structuredClone(columns);
  final.tab_id = 'qa:tab';
  final.panes[0].rect.width = 69;
  final.panes[1].rect.x = final.panes[2].rect.x = 69;
  final.panes[1].rect.width = final.panes[2].rect.width = 51;
  let layout = { tab_id: 'qa:tab', area: final.area, panes: Object.values(ids).map(pane_id => ({ pane_id, rect: {} })) };
  let selected = 'parent';
  const mutations = [];
  const owner = {
    binding, parentPane: 'parent', scopeEpoch: 1, claim: { layout: 'columns', tabId: 'qa:tab' },
    enqueue: job => job(), inspectPair: async () => pair,
    coordinator: { inspectTodo: async () => todo }, membership: async () => {}, verify: async () => true,
    placementFocus: async () => selected,
    restorePlacementFocus: async (_created, original) => { selected = original; },
    herdr: async (...args) => {
      if (args[0] === 'layout') return { layout };
      mutations.push(args);
      if (args[0] === 'swap') selected = args[2];
      if (mutations.length === 5) layout = final;
      return {};
    },
  };
  await FixedDagPane.prototype.arrangeDashboard.call(owner);
  assert.deepEqual(mutations.map(args => args[0]), ['move', 'move', 'swap', 'move', 'swap']);
  assert.equal(selected, 'parent');
  assert.equal(isColumnDashboard(layout, ids), true);
  await FixedDagPane.prototype.arrangeDashboard.call(owner);
  assert.equal(mutations.length, 5);
  layout = { ...final, panes: [...final.panes, { pane_id: 'foreign', rect: final.panes[0].rect }] };
  await assert.rejects(FixedDagPane.prototype.arrangeDashboard.call(owner), /unowned panes/);
  assert.equal(mutations.length, 5);
});
