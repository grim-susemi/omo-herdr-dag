import assert from 'node:assert/strict';

for(const language of ['en','ko','zh-cn']) for(const view of ['dag','tasks']) for(const columns of [40,24])
  test(`narrow ${view} keeps current target, live/follow and role controls at ${columns} in ${language}`,()=>{
    const frame=renderFixedFrame({...state,language},{columns,rows:8,color:false,
      follow:true,runningNodeId:'first',selectedNodeId:'first',runningTaskId:'st_ordinary'},view);
    const lines=frame.text.split('\n'), foot=lines.slice(-4).join('\n');
    assert.match(foot,/ON/);assert.match(foot,/LIVE|연결|在线/);
    for(const key of ['n/p','Space/Enter','j/k','c','d','q'])assert.ok(foot.includes(key),key+' '+foot);
    if(view==='dag')assert.ok(lines.slice(0,-4).some(line=>line.includes('ACTUAL_FIRST')||line.includes('ACTUAL_…')));
    const offline=renderFixedFrame({...state,language},{columns,rows:8,color:false,offline:true,follow:false},view);
    assert.match(offline.text.split('\n').slice(-4).join('\n'),/OFFLINE|오프라인|离线/);
    assert.ok(lines.every(line=>width(line)<columns));
  });


for (const view of ['dag', 'tasks']) for (const columns of [80, 29, 24])
  test(`fixed ${view} keeps read error and offline truth pinned at ${columns} columns`, () => {
    const frame = renderFixedFrame(state, {columns, rows:14, color:false, error:'BAD_READ',
      offline:true, scroll:5, follow:false, completedExpanded:true}, view);
    const foot = frame.text.split('\n').slice(-5).join('\n');
    assert.match(foot, /Read error/);
    assert.match(foot, /OFFLINE/);
    assert.ok(frame.text.split('\n').every(line => width(line) < columns));
  });
test('workers viewport suppresses a border-only card edge without changing task ranges', () => {
  const source = {...state, runs:[], tasks:[
    {id:'active',status:'running',description:'ACTIVE',progress:'PROGRESS'},
    {id:'next',status:'pending',description:'NEXT'}]};
  const frame = renderFixedFrame(source,{columns:80,rows:14,color:false,
    selectedTaskId:'active'},'tasks');
  assert.deepEqual(frame.taskRanges.active,{start:0,end:7});
  assert.deepEqual(frame.taskRanges.next,{start:7,end:10});
  const body=frame.text.split('\n').slice(2,10);
  assert.match(body[6], /^╰/);
  assert.equal(body[7], '');
  const colored = renderFixedFrame(source,{columns:80,rows:14,color:true,
    selectedTaskId:'active'},'tasks');
  assert.equal(colored.text.split('\n')[9], '');
  const bottomOnly=renderFixedFrame(source,{columns:80,rows:7,color:false,scroll:6},'tasks');
  assert.equal(bottomOnly.text.split('\n')[2],'');
  assert.deepEqual(bottomOnly.taskRanges,frame.taskRanges);
});

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
    assert.ok(frame.split('\n').slice(-4).some(line => /^Read error:/.test(line)));
    assert.doesNotMatch(frame, /Follow ON.*Connected/);
    assert.ok(frame.split('\n').every(line => width(line) < columns));
    if (view === 'dag') {
      const empty = renderFixedFrame({ ...state, runs: [] }, options, view).text;
      assert.match(empty.split('\n').at(-4), /^Read error:/);
      assert.doesNotMatch(empty, /ORDINARY_ROOT|Tasks|Follow ON/);
    }
  });



test('completed ordinary roots shrink to unboxed summaries while manual expansion remains visible', () => {
  const finished = { id: 'done', status: 'completed', description: 'COMPACT_DONE', progress: 'DETAIL_PROGRESS' };
  const sample = { connected: true, runs: [], tasks: [finished, { id: 'live', status: 'running', description: 'LIVE_ROOT' }] };
  const folded = renderFixedFrame(sample, { columns: 80, rows: 60, color: false, selectedTaskId: 'done' }, 'tasks');
  assert.equal(folded.taskRanges.done.end - folded.taskRanges.done.start, 1);
  assert.doesNotMatch(folded.text, /DETAIL_PROGRESS/);
  const expanded = renderFixedFrame(sample, { columns: 80, rows: 60, color: false,
    viewState: { expanded: { '[null,"done"]': true } } }, 'tasks');
  assert.match(expanded.text, /DETAIL_PROGRESS/);
  assert.equal(expanded.taskRanges.done.end - expanded.taskRanges.done.start, 7);
});

test('completed group keeps canonical counts and selected hidden summary reachable', () => {
  const sample = { connected: true, runs: [], tasks: Array.from({ length: 9 }, (_, i) => ({ id: String(i), status: 'completed', description: 'DONE_'+i })) };
  const original = structuredClone(sample);
  const folded = renderFixedFrame(sample, { columns: 80, rows: 60, color: false }, 'tasks');
  const selected = renderFixedFrame(sample, { columns: 80, rows: 60, color: false, selectedTaskId: '8', revealSelection: true }, 'tasks');
  const opened = renderFixedFrame(sample, { columns: 80, rows: 60, color: false, completedExpanded: true }, 'tasks');
  assert.equal(Object.keys(folded.taskRanges).length, 0);
  assert.equal(Object.keys(selected.taskRanges).length, 1);
  assert.match(selected.text, /DONE_8/);
  assert.equal(Object.keys(opened.taskRanges).length, 9);
  for (const r of Object.values(opened.taskRanges)) assert.equal(r.end-r.start,1);
  assert.deepEqual(sample, original);
});

for (const status of ['running','pending','blocked','paused','error','lost']) test('completed parent stays visible with '+status+' descendants', () => {
  const sample = { connected: true, runs: [], tasks: [{ id:'parent',status:'completed',description:'PROTECTED_PARENT' },
    { id:'child',parentTaskId:'parent',status,description:'PROTECTED_CHILD' }] };
  const frame=renderFixedFrame(sample,{columns:80,rows:60,color:false},'tasks');
  assert.match(frame.text,/PROTECTED_CHILD/);
  assert.ok(frame.taskRanges.parent.end-frame.taskRanges.parent.start>1);
});

test('14-row workers fit the entire selected running card including metrics and bottom border', () => {
  const frame = renderFixedFrame(state,{columns:80,rows:14,color:false,selectedTaskId:'st_ordinary'},'tasks');
  assert.match(frame.text,/\$0\.4493.*21 tok\/s/);
  const lines=frame.text.split('\n');
  assert.ok(lines.findIndex(line=>line.startsWith('╰'))>lines.findIndex(line=>line.includes('21 tok/s')));
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
