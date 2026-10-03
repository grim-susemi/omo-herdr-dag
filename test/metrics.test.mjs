import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TaskData, normalizeTask } from '../src/task-data.mjs';
import { DagPane } from '../src/controller.mjs';
import { readJson, writeJson } from '../src/storage.mjs';
import { render, width } from '../src/render.mjs';

const report = '2026-10-03T00:00:00.000Z';
const stats = { input_tokens: 50000, output_tokens: 28087, cache_read_tokens: 730000,
  cache_write_tokens: 3410, total_tokens: 811496, generation_ms: 72394, runtime_ms: 102917,
  tokens_per_second: 388, cost_usd: 0, cost_status: 'reported', token_status: 'complete',
  duration_status: 'monotonic' };
const record = (run_stats = stats, extra = {}) => ({ task_id: 'st_metrics', parent_session_id: 'metrics',
  status: 'running', description: 'METRIC_WORKER 한글', updated_at: report, run_stats, ...extra });
const state = task => ({ connected: false, sessionId: 'metrics', tasks: [task], runs: [] });
const frame = (task, options = {}) => render(state(task), {
  columns: 100, rows: 100, color: false, selectedTaskId: task.id, ...options });

test('metrics whitelist keeps native facts and quality without retaining private fields', () => {
  const task = normalizeTask(record({ ...stats, prompt: 'PRIVATE', output: 'PRIVATE', auth: 'PRIVATE' }));
  assert.deepEqual(task.metrics, { ...stats, source: 'run_stats', reportedAt: report });
  assert.equal(task.reportedAt, report);
  assert.doesNotMatch(JSON.stringify(task), /PRIVATE|prompt|auth/);
  const live = normalizeTask(record(undefined, { run_stats: undefined, live_progress: {
    output_tokens: 3, total_tokens: 10, tokens_per_second: 2, cost_usd: 0, generation_ms: 1 } }));
  assert.deepEqual(live.metrics, { output_tokens: 3, total_tokens: 10, tokens_per_second: 2,
    source: 'live_progress', reportedAt: report });
  assert.equal(live.metrics.cost_usd, undefined);
  const mixed = normalizeTask(record({ cost_usd: 1, cost_status: 'reported' },
    { live_progress: { output_tokens: 3, tokens_per_second: 2 } }));
  assert.equal(mixed.metrics.source, 'run_stats+live_progress');
});

test('negative nonfinite fractional token counts and arbitrary quality are not measurements', () => {
  for (const invalid of [-1, NaN, Infinity, -Infinity, '4', null]) {
    const task = normalizeTask(record(Object.fromEntries(Object.keys(stats)
      .filter(key => typeof stats[key] === 'number').map(key => [key, invalid]))));
    assert.equal(task.metrics, undefined);
    const text = frame(task, { verbose: true });
    assert.doesNotMatch(text, /\$0\.0000|NaN|Infinity|273 tok\/s|0 tok\/s/);
  }
  const task = normalizeTask(record({ input_tokens: 1.5, cost_status: 'PRIVATE', duration_status: 'PRIVATE' }));
  assert.equal(task.metrics, undefined);
});

test('compact cost distinguishes unknown estimated and reported including genuine zero and tiny positives', () => {
  for (const [cost, quality, expected] of [
    [0, 'reported', '$0.0000 [reported]'], [0.4493, 'reported', '$0.4493 [reported]'],
    [0.000001, 'reported', '<$0.0001 [reported]'], [1, 'estimated', '$1.0000 [estimated]'],
    [1, undefined, '$1.0000 [Unknown]'] ]) {
    const text = frame(normalizeTask(record({ ...stats, cost_usd: cost, cost_status: quality })));
    assert.ok(text.includes(expected), text);
    assert.ok(text.includes('388 tok/s'));
    assert.doesNotMatch(text, /free|273 tok\/s/i);
  }
  for (const quality of ['unavailable', 'invalid']) {
    const text = frame(normalizeTask(record({ ...stats, cost_status: quality })));
    assert.match(text, /Cost: Unknown/);
    assert.doesNotMatch(text, /\$0\.0000/);
  }
});

test('generation rate prefers native values and only falls back to positive generation time', () => {
  const fallback = { ...stats, tokens_per_second: undefined };
  assert.match(frame(normalizeTask(record(fallback))), /388 tok\/s \[estimated\]/);
  assert.match(frame(normalizeTask(record({ ...stats, tokens_per_second: 21 }))), /21 tok\/s/);
  assert.match(frame(normalizeTask(record({ ...stats, tokens_per_second: 2.5 }))), /2\.5 tok\/s/);
  assert.match(frame(normalizeTask(record({ ...stats, tokens_per_second: 0 }))), /0 tok\/s/);
  for (const generation_ms of [0, -1, NaN, Infinity, undefined]) {
    const text = frame(normalizeTask(record({ ...fallback, generation_ms })));
    assert.match(text, /tok\/s: Unknown/);
    assert.doesNotMatch(text, /273 tok\/s|388 tok\/s|0 tok\/s/);
  }
  assert.match(frame(normalizeTask(record({ ...fallback, output_tokens: 0 }))), /0 tok\/s \[estimated\]/);
  assert.match(frame(normalizeTask(record({ runtime_ms: 102917, output_tokens: 28087 }))), /tok\/s: Unknown/);
});

test('ordinary and DAG details expose the same recorded metrics and report quality while disconnected', () => {
  const task = normalizeTask(record());
  const options = { columns: 100, rows: 100, color: true, verbose: true, selectedTaskId: task.id };
  const ordinary = render(state(task), options);
  const dag = render({ ...state(task), runs: [{ id: 'metrics-run', name: 'Metrics DAG', status: 'running',
    nodes: [{ id: 'metric-node', label: 'METRIC_NODE', state: 'running', taskId: task.id }], edges: [] }] },
  { ...options, selectedNodeId: 'metric-node' });
  for (const text of [ordinary, dag]) {
    for (const expected of ['Input tokens: 50000', 'Output tokens: 28087', 'Cache read tokens: 730000',
      'Cache write tokens: 3410', 'Total tokens: 811496', 'Generation ms: 72394', 'Runtime ms: 102917',
      'Token quality: complete', 'Cost quality: reported', 'Duration source: monotonic',
      'Metrics source: run_stats', `Metrics last report: ${report}`, `Native last report: ${report}`]) {
      assert.ok(text.includes(expected), expected);
    }
    assert.match(text, /Disconnected/);
    assert.doesNotMatch(text, /realtime|live metrics|Lost|stalled/);
  }
  assert.equal(task.status, 'running');
  assert.match(frame(normalizeTask(record(undefined, { run_stats: undefined })), { verbose: true }), /Collection pending/);
  for (const columns of [1, 12, 24, 40, 80]) for (const language of ['en', 'ko', 'zh-cn']) {
    const text = frame(task, { columns, rows: 24, language, verbose: true });
    assert.ok(text.split('\n').every(line => width(line) <= Math.max(1, columns - 1)));
  }
});

test('metrics-only disk and RPC changes invalidate snapshot signatures and survive actual viewer cache roundtrip', async t => {
  const cwd = await mkdtemp(join(tmpdir(), 'dag-metrics-'));
  const data = new TaskData({ cwd, sessionId: 'metrics' });
  t.after(async () => { data.stop(); await rm(cwd, { recursive: true, force: true }); });
  const file = join(cwd, '.omo', 'senpi-task', 'tasks', 'st_metrics.json');
  await writeJson(file, record());
  const before = await data.refresh();
  await writeJson(file, record({ ...stats, cost_usd: 0.4493 }));
  const changed = await data.refresh();
  assert.notDeepEqual(before, changed);
  assert.notEqual(frame(before[0]), frame(changed[0]));
  data.receive({ parent_session_id: 'metrics', tasks: [record({ ...stats, output_tokens: 7 })] });
  assert.equal(data.snapshot()[0].metrics.output_tokens, 7);
  const pane = new DagPane({ cwd, sessionId: 'metrics', parentPane: 'none', socket: 'none',
    stateDir: join(cwd, 'cache'), herdr: () => { throw new Error('No pane actions permitted'); } });
  t.after(() => pane.taskData.stop());
  pane.taskData.restore(data.snapshot());
  await rm(file);
  await pane.save(false);
  const cached = await readJson(pane.stateFile);
  const restored = new TaskData({ cwd, sessionId: 'metrics' });
  t.after(() => restored.stop());
  restored.restore(cached.tasks);
  assert.deepEqual(restored.snapshot(), data.snapshot());
  const metricsReport = restored.snapshot()[0].metrics.reportedAt;
  restored.receive({ parent_session_id: 'metrics', tasks: [{ task_id: 'st_metrics', status: 'running',
    updated_at: '2026-10-03T00:01:00Z' }] });
  assert.equal(restored.snapshot()[0].metrics.reportedAt, metricsReport);
  assert.equal(restored.snapshot()[0].reportedAt, '2026-10-03T00:01:00.000Z');
  assert.equal(restored.snapshot()[0].status, 'running');
  restored.receive({ parent_session_id: 'metrics', tasks: [record(stats)] });
  assert.equal(restored.snapshot()[0].metrics.output_tokens, 7, 'older native report stays rejected after restore');
});
