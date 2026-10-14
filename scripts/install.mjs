import { cp, mkdir, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { createHash, randomUUID } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { homedir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { languageOf, t } from '../src/i18n.mjs';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const source = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const execute = promisify(execFile);
const preflightError = message => new Error('Legacy identity preflight: ' + message);
const pathKey = value => String(value).replaceAll('\\', '/').toLowerCase();
const positive = value => Number.isSafeInteger(value) && value > 0;

async function queryNativeProcesses(pids) {
  const filter = pids.map(pid => `ProcessId = ${pid}`).join(' OR ');
  const script = `[Console]::OutputEncoding=[Text.UTF8Encoding]::new(); $ErrorActionPreference='Stop'
$rows=@(Get-CimInstance Win32_Process -Filter '${filter}' | Select-Object ProcessId,ParentProcessId,ExecutablePath,CommandLine,@{Name='CreationDate';Expression={$_.CreationDate.ToUniversalTime().ToString('o')}},@{Name='Birth';Expression={$p=[Diagnostics.Process]::GetProcessById($_.ProcessId);try{$p.StartTime.ToUniversalTime().ToFileTimeUtc().ToString()}finally{$p.Dispose()}}})
ConvertTo-Json -InputObject $rows -Compress`;
  const { stdout } = await execute('powershell.exe', ['-NoProfile', '-NonInteractive', '-EncodedCommand',
    Buffer.from(script, 'utf16le').toString('base64')], { windowsHide: true, timeout: 10000, maxBuffer: 1024 * 1024 });
  return JSON.parse(stdout);
}

function parseArgv(command) {
  if (typeof command !== 'string' || !command.trim() || /[\r\n]/.test(command)) return null;
  const argv = [];
  let at = 0;
  while (at < command.length) {
    while (at < command.length && /[ \t]/.test(command[at])) at++;
    if (at === command.length) break;
    const quoted = command[at] === '"', start = quoted ? ++at : at;
    if (quoted) {
      at = command.indexOf('"', at);
      if (at < 0 || command[at - 1] === '\\') return null;
    } else while (at < command.length && !/[ \t]/.test(command[at])) at++;
    const value = command.slice(start, at);
    if (!value || value.includes('"')) return null;
    argv.push(value);
    if (quoted && ++at < command.length && !/[ \t]/.test(command[at])) return null;
  }
  return argv;
}

// Diagnostic only: no pipe connection, native holder, capture, adoption or kill.
// The runtime must still establish its own recovery authority after activation.
export async function observeInstalledProcesses(roots, { queryProcesses = queryNativeProcesses,
  platform = process.platform } = {}) {
  const candidates = [], records = [];
  async function scan(directory) {
    let entries;
    try { entries = await readdir(directory, { withFileTypes: true }); }
    catch (error) { if (error.code === 'ENOENT') return; throw error; }
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      if (['integration', 'src'].includes(entry.name)) continue;
      const path = join(directory, entry.name);
      if (entry.isSymbolicLink()) throw preflightError('ambiguous linked state: ' + path);
      if (entry.isDirectory()) await scan(path);
      else if (entry.name.endsWith('.pane.json') || ['slot.json', 'todo.json', 'dag.json', 'workers.json'].includes(entry.name)) {
        let record;
        try { record = JSON.parse(await readFile(path, 'utf8')); }
        catch (error) { throw preflightError('unparseable record: ' + path + ': ' + error.message); }
        if (!record || typeof record !== 'object' || Array.isArray(record)) throw preflightError('unparseable record: ' + path);
        records.push({ path, record });
      }
    }
  }
  for (const root of new Set(roots)) await scan(root);
  function add(path, record, kind, identity, parent, argv) {
    if (identity?.pid === undefined) return;
    if (!positive(identity.pid)) throw preflightError('unparseable recorded PID: ' + path);
    const socket = record.canonicalSocketPath ?? record.socketPath;
    const layout = JSON.stringify([typeof socket === 'string' ? pathKey(socket) : socket,
      record.parent?.paneId ?? record.parentPaneId]);
    candidates.push({ path, kind, identity, parent, argv,
      owner: JSON.stringify([layout, kind]), layout, scope: record.sessionId });
  }
  for (const { path, record } of records) {
    if (path.endsWith('.pane.json') && (record.schema === 'omo-herdr-dashboard/pane/1' || record.nodePath && record.viewerPath)) {
      const argv = [record.nodePath, record.viewerPath,
        ...(record.role && record.role !== 'todo' ? ['--view', record.role === 'dag' ? 'dag' : 'tasks'] : []),
        '--state', record.statePath, '--close-pane', record.paneId, '--launch-token', record.launchToken];
      add(path, record, record.role ?? 'viewer', { pid: record.processId, birth: record.viewerBirth,
        executable: record.nodePath, parentPid: record.creationProcessId },
      { pid: record.creationProcessId, birth: record.creationBirth, creationDate: record.creationDate }, argv);
    }
    add(path, record, 'producer', record.producer ?? record.controlOffer?.producer ?? record.presenceOffer?.producer, record.parent?.shell);
    if (record.schema === 'omo-herdr-dashboard/parent-slot/2') {
      for (const [role, state] of Object.entries(record.roles ?? {})) add(path, { ...record, sessionId: state.sessionId },
        role, { ...state.viewer, parentPid: state.shell?.pid }, state.shell);
    }
  }
  if (platform !== 'win32') return { diagnosticOnly: true, status: 'unsupported platform', platform,
    recordedProcesses: candidates.length, processes: [] };
  const rows = new Map(), processes = [], owners = new Map(), scopes = new Map();
  async function lookup(pids) {
    const missing = [...new Set(pids)].filter(pid => !rows.has(pid));
    if (!missing.length) return;
    let result;
    try { result = await queryProcesses(missing); }
    catch (error) { throw preflightError('native query failed: ' + error.message); }
    if (!Array.isArray(result) || result.some(row => !positive(row?.ProcessId) ||
      !missing.includes(row.ProcessId)) || new Set(result.map(row => row.ProcessId)).size !== result.length)
      throw preflightError('ambiguous or unparseable native query');
    for (const pid of missing) rows.set(pid, result.find(row => row.ProcessId === pid) ?? null);
  }
  function native(row) {
    const argv = parseArgv(row?.CommandLine);
    if (!row || !positive(row.ParentProcessId) || !/^\d+$/.test(row.Birth ?? '') ||
      !Number.isFinite(Date.parse(row.CreationDate)) || typeof row.ExecutablePath !== 'string' || !row.ExecutablePath || !argv)
      throw preflightError('unparseable native identity');
    return { pid: row.ProcessId, parentPid: row.ParentProcessId, birth: row.Birth,
      creationDate: row.CreationDate, executable: row.ExecutablePath, argv,
      argvDigest: createHash('sha256').update(row.CommandLine).digest('hex') };
  }
  await lookup(candidates.map(candidate => candidate.identity.pid));
  for (const candidate of candidates) {
    const row = rows.get(candidate.identity.pid);
    if (!row) { processes.push({ recordPath: candidate.path, kind: candidate.kind, pid: candidate.identity.pid, state: 'absent' }); continue; }
    if (typeof candidate.scope !== 'string' || !candidate.scope ||
      JSON.parse(candidate.layout).some(value => typeof value !== 'string' || !value))
      throw preflightError('unparseable recorded owner: ' + candidate.path);
    const actual = native(row), expected = candidate.identity;
    const argvMatches = candidate.argv ? candidate.argv.every(value => typeof value === 'string' && value) &&
      candidate.argv.length === actual.argv.length && candidate.argv.every((value, i) =>
        [0, 1, candidate.argv.indexOf('--state') + 1].includes(i) ? pathKey(value) === pathKey(actual.argv[i]) : value === actual.argv[i])
      : expected.argvDigest === actual.argvDigest;
    if (typeof expected.birth !== 'string' || expected.birth !== actual.birth ||
      typeof expected.executable !== 'string' || pathKey(expected.executable) !== pathKey(actual.executable) ||
      expected.parentPid !== undefined && expected.parentPid !== actual.parentPid || !argvMatches)
      throw preflightError('identity mismatch or incomplete identity: ' + candidate.path);
    if (!positive(candidate.parent?.pid) || !candidate.parent.birth && !candidate.parent.creationDate)
      throw preflightError('unparseable recorded parent: ' + candidate.path);
    let ancestor = actual.parentPid;
    const seen = new Set([actual.pid]);
    while (ancestor !== candidate.parent.pid) {
      if (candidate.kind !== 'producer' || seen.has(ancestor) || seen.size > 8)
        throw preflightError('parent identity mismatch: ' + candidate.path);
      seen.add(ancestor); await lookup([ancestor]);
      ancestor = native(rows.get(ancestor)).parentPid;
    }
    await lookup([ancestor]);
    const parent = native(rows.get(ancestor));
    if (candidate.parent.birth && candidate.parent.birth !== parent.birth ||
      candidate.parent.creationDate && candidate.parent.creationDate !== parent.creationDate)
      throw preflightError('parent identity mismatch: ' + candidate.path);
    const owner = JSON.stringify([actual.pid, candidate.scope]);
    if (owners.has(candidate.owner) && owners.get(candidate.owner) !== owner)
      throw preflightError('multiple ambiguous live owners: ' + candidate.path);
    if (scopes.has(candidate.layout) && scopes.get(candidate.layout) !== candidate.scope)
      throw preflightError('multiple ambiguous live owners: ' + candidate.path);
    owners.set(candidate.owner, owner);
    scopes.set(candidate.layout, candidate.scope);
    processes.push({ recordPath: candidate.path, kind: candidate.kind, state: 'matching', identity: actual, parent });
  }
  return { diagnosticOnly: true, status: processes.some(process => process.state === 'matching') ? 'live observed' : 'no recorded live processes',
    platform, recordedProcesses: candidates.length, processes };
}

export async function install(args, observationOptions = {}) {
const seen = new Set();
for (let i = 0; i < args.length; i++) {
  const key = args[i];
  if (!['--agent-dir', '--lang', '--dry-run'].includes(key) || seen.has(key)) throw new Error('Invalid or duplicate option: '+key);
  seen.add(key);
  if (key !== '--dry-run') {
    if (!args[i+1] || args[i+1].startsWith('-')) throw new Error(key+' requires a value.');
    if (key === '--lang' && !['en','ko','zh-cn'].includes(args[i+1])) throw new Error('--lang must be en, ko, or zh-cn.');
    i++;
  }
}
const agentDir = resolve(seen.has('--agent-dir') ? args[args.indexOf('--agent-dir')+1] :
  process.env.OMO_CODING_AGENT_DIR || process.env.SENPI_CODING_AGENT_DIR || join(homedir(),'.omo','agent'));
const required = ['extension.mjs', 'LICENSE', "src/viewer.mjs", "src/view-state.mjs", "src/task-data.mjs", "src/stream-tap.mjs", "src/storage.mjs", "src/runtime.mjs", "src/retention.mjs", "src/render.mjs", "src/model.mjs", "src/i18n.mjs", "src/herdr.mjs", "src/controller.mjs"];
for (const file of required) await readFile(join(source,file));
// A packaged build carries sha256 of every runtime file; verify before any write.
try {
  const { files } = JSON.parse(await readFile(join(source, 'runtime-manifest.json'), 'utf8'));
  for (const [file, hash] of Object.entries(files)) {
    if (createHash('sha256').update(await readFile(join(source, file))).digest('hex') !== hash) throw new Error('Runtime integrity mismatch: ' + file);
  }
} catch (error) { if (error.code !== 'ENOENT' || error.path !== join(source, 'runtime-manifest.json')) throw error; }
const container = join(agentDir, 'herdr-dag', 'integration');
const entry = 'omo-herdr-dag.js';
const wrapper = join(agentDir, 'extensions', entry);
const legacyWrapper = join(agentDir, 'extensions', 'herdr-dag.js');
const marker = '// managed by omo-herdr-dag';
async function optionalText(path) {
  try { return await readFile(path, 'utf8'); }
  catch (error) { if (error.code !== 'ENOENT') throw error; return undefined; }
}
let entries;
try { entries = await readdir(container); }
catch (error) { if (error.code !== 'ENOENT') throw error; }
const currentText = await optionalText(join(container, 'current.json'));
const current = currentText === undefined ? undefined : JSON.parse(currentText).generation;
if (currentText !== undefined && (typeof current !== 'string' || !/^generation-\d+$/.test(current))) {
  throw new Error(`Invalid installation generation: ${container}`);
}
const previous = current ? join(container, current) : container;
if (current) {
  if ((await optionalText(join(previous,'.installed-by'))) !== marker) throw new Error('Interrupted installation generation: '+previous);
  for (const file of required) await readFile(join(previous,file));
}
let savedLanguage;
try { savedLanguage = JSON.parse(await readFile(join(previous, 'locale.json'), 'utf8')).language; }
catch (error) { if (error.code !== 'ENOENT') throw error; }
const languageArg = args.includes('--lang') ? args[args.indexOf('--lang') + 1] : undefined;
if (args.includes('--lang') && !['en', 'ko', 'zh-cn'].includes(languageArg)) throw new Error('--lang must be en, ko, or zh-cn.');
const language = languageArg ?? languageOf(savedLanguage);
try {
  const current = await readFile(wrapper, 'utf8');
  if (!current.startsWith(marker)) throw new Error(t(language, 'existingFile', { path: wrapper }));
} catch (error) { if (error.code !== 'ENOENT') throw error; }
if (entries && (await optionalText(join(container, '.installed-by'))) !== marker) {
  throw new Error(t(language, 'unmanagedDirectory', { path: container }));
}
const legacyText = await optionalText(legacyWrapper);
const legacyExtension = legacyText?.startsWith(marker) ? legacyWrapper : undefined;
// Retain each prior generation in place as the update backup. Every transitive
// module URL changes on reinstall; clearing Senpi's factory cache is insufficient.
const numbers = (entries ?? []).map(name => /^generation-(\d+)$/.exec(name)).filter(Boolean).map(match => Number(match[1]));
const generation = `generation-${String(Math.max(0, ...numbers) + 1).padStart(6, '0')}`;
const integration = join(container, generation);
const backup = entries ? (current ? previous : `${container}.backup-${generation}`) : undefined;
const observation = await observeInstalledProcesses([dirname(container), previous], observationOptions);
const observationPath = join(container, '.observation-' + generation + '.json');
const plan = { integration, extension: wrapper, source, entry, language,
  preflight: { diagnosticOnly: true, status: observation.status, recordedProcesses: observation.recordedProcesses, observationPath },
  ...(backup ? { backup } : {}), ...(legacyExtension ? { legacyExtension } : {}), activation: t(language, 'activation') };
if (args.includes('--dry-run')) return plan;
await mkdir(dirname(container), { recursive: true, mode: 0o700 });
await mkdir(dirname(wrapper), { recursive: true, mode: 0o700 });
const stamp = randomUUID();
const staged = `${container}.stage-${stamp}`;
const wrapperBefore = await optionalText(wrapper);
let migrated = false, promoted = false, currentReplaced = false, wrapperReplaced = false, observationWritten = false;
async function restore(path, text) {
  if (text === undefined) return rm(path, { force: true });
  const temporary = `${path}.rollback-${stamp}`;
  try { await writeFile(temporary, text, { mode: 0o600 }); await rename(temporary, path); }
  finally { await rm(temporary, { force: true }); }
}
await mkdir(staged, { mode: 0o700 });
try {
  await cp(join(source, 'src'), join(staged, 'src'), { recursive: true });
  await cp(join(source, 'extension.mjs'), join(staged, 'extension.mjs'));
  await cp(join(source, 'LICENSE'), join(staged, 'LICENSE'));
  await writeFile(join(staged, 'locale.json'), JSON.stringify({ language }) + '\n', { mode: 0o600 });
  await writeFile(join(staged, '.installed-by'), marker, { mode: 0o600 });
  // Migrate the original flat installation without touching sibling runtime files.
  if (entries && !current) { await rename(container, backup); migrated = true; }
  await mkdir(container, { recursive: true, mode: 0o700 });
  await writeFile(join(container, '.installed-by'), marker, { mode: 0o600 });
  await writeFile(observationPath, JSON.stringify({ schema: 'omo-herdr-dag/activation-observation/1',
    observedAt: new Date().toISOString(), previousGeneration: current ?? null, ...observation }, null, 2) + '\n', { mode: 0o600 });
  observationWritten = true;
  await rename(staged, integration); promoted = true;
  await writeFile(join(container, `current.json.tmp-${stamp}`), JSON.stringify({ generation }) + '\n', { mode: 0o600 });
  await rename(join(container, `current.json.tmp-${stamp}`), join(container, 'current.json')); currentReplaced = true;
  const text = `${marker}\nexport { default } from '../herdr-dag/integration/${generation}/extension.mjs';\n`;
  await writeFile(`${wrapper}.tmp-${stamp}`, text, { mode: 0o600 });
  await rename(`${wrapper}.tmp-${stamp}`, wrapper); wrapperReplaced = true;
  if (legacyExtension) await rm(legacyExtension);
} catch (error) {
  if (wrapperReplaced) await restore(wrapper, wrapperBefore);
  if (currentReplaced) await restore(join(container, 'current.json'), currentText);
  if (observationWritten) await rm(observationPath, { force: true });
  if (promoted) await rm(integration, { recursive: true, force: true });
  if (migrated) { await rm(container, { recursive: true, force: true }); await rename(backup, container); }
  else if (!entries) await rm(container, { recursive: true, force: true });
  throw error;
} finally {
  await rm(`${wrapper}.tmp-${stamp}`, { force: true });
  await rm(join(container, `current.json.tmp-${stamp}`), { force: true });
  await rm(staged, { recursive: true, force: true });
}
return { installed: true, ...plan };
}

if (process.argv[2] === 'install' || process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  console.log(JSON.stringify(await install(process.argv.slice(process.argv[2] === 'install' ? 3 : 2)), null, 2));
