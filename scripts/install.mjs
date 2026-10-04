import { cp, mkdir, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { homedir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { languageOf, t } from '../src/i18n.mjs';

const source = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(process.argv[2] === 'install' ? 3 : 2);
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
const languageArg = process.argv.includes('--lang') ? process.argv[process.argv.indexOf('--lang') + 1] : undefined;
if (process.argv.includes('--lang') && !['en', 'ko', 'zh-cn'].includes(languageArg)) throw new Error('--lang must be en, ko, or zh-cn.');
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
const plan = { integration, extension: wrapper, source, entry, language,
  ...(backup ? { backup } : {}), ...(legacyExtension ? { legacyExtension } : {}), activation: t(language, 'activation') };
if (process.argv.includes('--dry-run')) { console.log(JSON.stringify(plan, null, 2)); process.exit(0); }
await mkdir(dirname(container), { recursive: true, mode: 0o700 });
await mkdir(dirname(wrapper), { recursive: true, mode: 0o700 });
const stamp = randomUUID();
const staged = `${container}.stage-${stamp}`;
const wrapperBefore = await optionalText(wrapper);
let migrated = false, promoted = false, currentReplaced = false, wrapperReplaced = false;
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
  if (promoted) await rm(integration, { recursive: true, force: true });
  if (migrated) { await rm(container, { recursive: true, force: true }); await rename(backup, container); }
  else if (!entries) await rm(container, { recursive: true, force: true });
  throw error;
} finally {
  await rm(`${wrapper}.tmp-${stamp}`, { force: true });
  await rm(join(container, `current.json.tmp-${stamp}`), { force: true });
  await rm(staged, { recursive: true, force: true });
}
console.log(JSON.stringify({ installed: true, ...plan }, null, 2));
