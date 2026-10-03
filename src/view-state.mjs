import { readJson, writeJson } from './storage.mjs';

export function emptyViewState(sessionId, scope) {
  return { version: 1, sessionId: sessionId ?? null, expanded: {},
    ...(scope ? { ownerKey: scope.ownerKey, scopeEpoch: scope.scopeEpoch } : {}) };
}

export async function loadViewState(stateFile, sessionId, scope) {
  const saved = await readJson(`${stateFile}.view.json`);
  if (!saved) return emptyViewState(sessionId, scope);
  if (saved.version !== 1 || !saved.expanded || typeof saved.expanded !== 'object' ||
      Array.isArray(saved.expanded) || Object.values(saved.expanded).some(value => typeof value !== 'boolean')) {
    throw new Error('Invalid DAG view preferences.');
  }
  const navigation = saved.navigation;
  if (navigation !== undefined && (!navigation || typeof navigation !== 'object' || Array.isArray(navigation) ||
      typeof navigation.follow !== 'boolean' ||
      navigation.scroll !== undefined && (!Number.isSafeInteger(navigation.scroll) || navigation.scroll < 0) ||
      navigation.view !== undefined && !['dag', 'tasks'].includes(navigation.view) ||
      ['selectedId', 'selectedTaskId'].some(key => navigation[key] !== undefined && typeof navigation[key] !== 'string') ||
      navigation.completedExpanded !== undefined && typeof navigation.completedExpanded !== 'boolean' ||
      navigation.selectedNodes !== undefined && (!Array.isArray(navigation.selectedNodes) ||
        navigation.selectedNodes.some(pair => !Array.isArray(pair) || pair.length !== 2 ||
          pair.some(value => typeof value !== 'string'))))) throw new Error('Invalid DAG navigation preferences.');
  return saved.sessionId === (sessionId ?? null) && (!scope ||
    saved.ownerKey === scope.ownerKey && saved.scopeEpoch === scope.scopeEpoch)
    ? saved : emptyViewState(sessionId, scope);
}

const key = (runId, nodeId) => JSON.stringify([runId, nodeId]);
// DAG run IDs are strings; null reserves a collision-free, stable task scope.
export const TASK_SCOPE = null;
export function isExpanded(view, runId, nodeId, status) {
  const saved = view?.expanded?.[key(runId, nodeId)];
  return typeof saved === 'boolean' ? saved : status === 'running';
}
export function setExpanded(view, runId, nodeId, expanded) {
  view.expanded[key(runId, nodeId)] = expanded;
}
export const saveViewState = (stateFile, view) => writeJson(`${stateFile}.view.json`, view);
