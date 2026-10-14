import { cp, mkdir, mkdtemp, readdir, readFile, rename, rm, stat, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const stage = await mkdtemp(join(root, '.dist-stage-'));
try {
  await cp(join(root, 'src'), join(stage, 'src'), { recursive: true });
  await cp(join(root, 'extension.mjs'), join(stage, 'extension.mjs'));
  await cp(join(root, 'LICENSE'), join(stage, 'LICENSE'));
  await mkdir(join(stage, 'scripts'));
  await cp(join(root, 'scripts/install.mjs'), join(stage, 'scripts/install.mjs'));
  const hashes = {};
  for (const name of (await readdir(stage, { recursive: true })).map(n => n.split('\\').join('/')).sort()) {
    if ((await stat(join(stage, name))).isFile()) hashes[name] = createHash('sha256').update(await readFile(join(stage, name))).digest('hex');
  }
  await writeFile(join(stage, 'runtime-manifest.json'), JSON.stringify({ schema: 'runtime-manifest/1', files: hashes }, null, 2) + '\n');
  const files = await readdir(stage, { recursive: true });
  for (const file of files.filter(name => name.endsWith('.mjs'))) {
    execFileSync(process.execPath, ['--check', join(stage, file)], { stdio: 'pipe' });
  }
  execFileSync(process.execPath, ['--check', join(root, 'bin/omo-herdr-dag.mjs')], { stdio: 'pipe' });
  // Replace only the generated output after all source files pass syntax checks.
  await rm(join(root, 'dist'), { recursive: true, force: true });
  await rename(stage, join(root, 'dist'));
  console.log('Built dist/: extension, viewer, installer, and MIT license. JavaScript syntax checks passed.');
} finally {
  await rm(stage, { recursive: true, force: true });
}
