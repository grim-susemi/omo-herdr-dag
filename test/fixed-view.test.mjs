import assert from 'node:assert/strict';
import { test } from 'node:test';
import { renderFixedFrame } from '../src/viewer.mjs';
import { width } from '../src/render.mjs';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { EventEmitter, once } from 'node:events';
import { mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { stripVTControlCharacters } from 'node:util';
import { writeJson } from '../src/storage.mjs';

const state = { sessionId: 'fixed-source', connected: true, language: 'en',
  runs: [{ id: 'run', name: 'ACTUAL_DAG', status: 'running', nodes: [
    { id: 'first', label: 'ACTUAL_FIRST_NODE', state: 'running', taskId: 'st_linked' },
    { id: 'second', label: 'ACTUAL_SECOND_NODE', state: 'pending' },
  ], edges: [{ from: 'first', to: 'second' }] }],
  tasks: [{ id: 'st_linked', status: 'running', description: 'DAG_LINKED_DETAIL' },
    { id: 'st_ordinary', status: 'running', description: 'ORDINARY_ROOT',
      metrics: { cost_usd: 0.4493, tokens_per_second: 21, cost_status: 'reported', source: 'run_stats' } }] };
for (const view of ['dag', 'tasks']) test(`managed ${view} has a fixed header, actual membership and metrics`, () => {
  const frame = renderFixedFrame(state, { columns: 80, rows: 60, color: false,
    viewState: { expanded: { '[null,"st_ordinary"]': true } } }, view).text;
  assert.doesNotMatch(frame, /t DAG|t Tasks/);
  if (view === 'dag') {
    for (const marker of ['ACTUAL_DAG', 'ACTUAL_FIRST_NODE', 'ACTUAL_SECOND_NODE', 'first → second', 'DAG_LINKED_DETAIL'])
      assert.ok(frame.includes(marker), marker);
    assert.doesNotMatch(frame, /ORDINARY_ROOT/);
  } else {
    assert.match(frame, /ORDINARY_ROOT/);
    assert.match(frame, /\$0\.4493.*21 tok\/s/);
    assert.doesNotMatch(frame, /ACTUAL_DAG|DAG_LINKED_DETAIL|Runs/);
  }
});
for (const [columns, rows] of [[80, 24], [40, 16], [24, 8], [12, 4], [1, 1]])
  test(`empty fixed DAG at ${columns}x${rows} never falls back to ordinary tasks`, () => {
    const empty = { ...state, runs: [] };
    const frame = renderFixedFrame(empty, { columns, rows, color: false }, 'dag').text;
    assert.ok(frame.split('\n').length <= rows);
    assert.ok(frame.split('\n').every(line => width(line) < columns || columns === 1 && width(line) <= 1));
    assert.doesNotMatch(frame, /ORDINARY_ROOT|Tasks|t DAG/);
    if (columns >= 24 && rows >= 8) assert.match(frame, /DAG \(0\)/);
    assert.deepEqual(empty.runs, []);
  });

for (const view of ['dag', 'tasks']) for (const columns of [80, 24])
  test(`fixed ${view} read errors stay visible at ${columns} columns while scrolled`, () => {
    const options = { columns, rows: 14, color: false, scroll: 100, follow: true, error: 'SOURCE_UNAVAILABLE' };
    const frame = renderFixedFrame(state, options, view).text;
    assert.match(frame.split('\n').at(-4), /^Read error:/);
    assert.doesNotMatch(frame, /Follow ON.*Connected/);
    assert.ok(frame.split('\n').every(line => width(line) < columns));
    if (view === 'dag') {
      const empty = renderFixedFrame({ ...state, runs: [] }, options, view).text;
      assert.match(empty.split('\n').at(-4), /^Read error:/);
      assert.doesNotMatch(empty, /ORDINARY_ROOT|Tasks|Follow ON/);
    }
  });

const fixedPosixBridge = String.raw`

import os, sys, pty, subprocess, selectors, json, fcntl, termios, struct, signal
master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 26, 80, 0, 0))
child = subprocess.Popen(sys.argv[1:], stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
os.close(slave)
selector = selectors.DefaultSelector()
selector.register(master, selectors.EVENT_READ)
selector.register(sys.stdin, selectors.EVENT_READ)
commands = b''
output = b''
height = 26
try:
  while True:
    for key, _ in selector.select():
      if key.fileobj == master:
        try: data = os.read(master, 65536)
        except OSError: data = b''
        if not data: sys.exit(child.wait())
        output += data
        start, end = b'\x1b[H', b'\x1b[K'
        while start in output:
          # A queued old-size frame must not consume rows from a resized frame.
          # Cursor-home starts a replacement screen, so keep the latest one.
          output = output[output.rindex(start):]
          if output.count(end) < height: break
          parts = output.split(end, height)
          frame = end.join(parts[:height]) + end
          output = parts[height]
          print(json.dumps({'frame': frame.decode('utf8')}), flush=True)
      else:
        data = os.read(sys.stdin.fileno(), 65536)
        if not data: sys.exit(0)
        commands += data
        while b'\n' in commands:
          command, commands = commands.split(b'\n', 1)
          value = json.loads(command)
          if 'keys' in value: os.write(master, value['keys'].encode('utf8'))
          if 'resize' in value:
            rows, columns = value['resize']
            height = rows
            fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack('HHHH', rows, columns, 0, 0))
            os.kill(child.pid, signal.SIGWINCH)
finally:
  if child.poll() is None:
    child.terminate()
    child.wait(timeout=5)
  os.close(master)
`;

for (const view of ['dag', 'tasks']) test(`real fixed ${view} PTY ignores t and never follows into the other role`, { timeout: 15000 }, async t => {
  const directory = join(tmpdir(), `fixed-view-${process.pid}-${randomUUID()}`);
  console.log(JSON.stringify({ event: 'resource-planned', pid: process.pid, directory }));
  await mkdir(directory);
  const file = join(directory, 'role.json');
  const managed = { ...structuredClone(state), schema: 'omo-herdr-dashboard/role/1', role: view === 'dag' ? 'dag' : 'workers',
    ownerKey: 'synthetic-owner', socketPath: 'qa:socket', parentPaneId: 'qa:parent', tabId: 'qa:tab',
    scopeEpoch: 1, launchToken: 'synthetic-launch', paneId: 'qa:owned-fixture',
    presenceOffer: null, controlOffer: null };
  managed.ownerKey = createHash('sha256').update(JSON.stringify([managed.socketPath, managed.parentPaneId, managed.sessionId]))
    .digest('hex').slice(0, 24);
  await writeJson(file, managed);
  const viewerArgs = [fileURLToPath(new URL('../src/viewer.mjs', import.meta.url)), '--view', view, '--state', file,
    '--close-pane', managed.paneId, '--launch-token', managed.launchToken];
  const helper = fileURLToPath(new URL('./windows-pty.mjs', import.meta.url));
  // This case uses the repository's actual Windows ConPTY bridge, not a mock
  // viewer or a substitute Todo surface.
  console.log(JSON.stringify({ event: 'resource-planned', helper, argv: viewerArgs }));
  const child = process.platform === 'win32'
    ? spawn(process.execPath, [helper, ...viewerArgs], { stdio: ['pipe', 'pipe', 'pipe'] })
    : spawn('python3', ['-u', '-c', fixedPosixBridge, process.execPath, ...viewerArgs], { stdio: ['pipe', 'pipe', 'pipe'] });
  console.log(JSON.stringify({ event: 'resource-bound', workerPid: child.pid, helper, statePath: file }));
  const events = new EventEmitter();
  let buffer = '', lastFrame = '', stderr = '';
  child.stderr.on('data', data => { stderr += data; });
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', data => {
    buffer += data;
    let cut;
    while ((cut = buffer.indexOf('\n')) >= 0) {
      const message = JSON.parse(buffer.slice(0, cut)); buffer = buffer.slice(cut + 1);
      lastFrame = stripVTControlCharacters(message.frame).replaceAll('\r', '');
      events.emit('frame', lastFrame);
    }
  });
  t.after(async () => {
    if (child.exitCode === null) {
      const exited = once(child, 'exit', { signal: AbortSignal.timeout(5000) });
      child.stdin.end(); await exited;
    }
    await rm(directory, { recursive: true, force: true });
    console.log(JSON.stringify({ event: 'resource-cleaned', workerPid: child.pid, directory }));
  });
  const send = keys => child.stdin.write(`${JSON.stringify({ keys })}\n`);
  const frame = (predicate, action = () => {}) => new Promise((resolve, reject) => {
    const abort = AbortSignal.timeout(5000);
    const failed = () => finish(new Error(`Fixed PTY frame timeout\n${lastFrame}\n${stderr}`));
    const onFrame = text => { if (predicate(text)) finish(null, text); };
    const onExit = code => finish(new Error(`Fixed viewer exited ${code}\n${stderr}`));
    function finish(error, text) {
      abort.removeEventListener('abort', failed); events.off('frame', onFrame); child.off('exit', onExit);
      if (error) reject(error); else resolve(text);
    }
    abort.addEventListener('abort', failed, { once: true });
    events.on('frame', onFrame); child.once('exit', onExit);
    Promise.resolve().then(action).catch(error => finish(error));
  });
  const title = view === 'dag' ? 'OMO  /  DAG' : 'OMO  /  Tasks';
  await frame(text => text.includes(title) && text.includes('Follow ON'));
  // f gives this otherwise-no-op t action an exact observable completion;
  // a clock repaint cannot satisfy Follow OFF before the key sequence.
  const locked = await frame(text => text.includes(title) && text.includes('Follow OFF'), () => send('tf'));
  assert.doesNotMatch(locked, /t DAG|t Tasks/);
  await frame(text => text.includes(title) && text.includes('Follow ON'), () => send('f'));
  if (view === 'dag') {
    managed.runs = [];
    const empty = await frame(text => text.includes('DAG (0)'), () => writeJson(file, managed));
    assert.doesNotMatch(empty, /ORDINARY_ROOT|Tasks/);
  } else {
    managed.tasks.find(task => task.id === 'st_ordinary').description = 'UPDATED_ORDINARY';
    managed.runs[0].name = 'OTHER_ROLE_ACTIVE';
    const updated = await frame(text => text.includes('UPDATED_ORDINARY'), () => writeJson(file, managed));
    assert.match(updated, /OMO  \/  Tasks/);
    assert.doesNotMatch(updated, /OTHER_ROLE_ACTIVE|ACTUAL_FIRST_NODE/);
  }
  const exited = once(child, 'exit', { signal: AbortSignal.timeout(5000) });
  send('q');
  assert.equal((await exited)[0], 0, stderr);
});
