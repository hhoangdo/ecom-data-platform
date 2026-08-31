const fs = require('fs');
const path = require('path');

const graphPath = process.argv[2];
const outputPath = process.argv[3];
const nodeTypes = new Set(['file','function','class','module','concept','config','document','service','table','endpoint','pipeline','schema','resource','domain','flow','step']);
const edgeTypes = new Set(['imports','exports','contains','inherits','implements','calls','subscribes','publishes','middleware','reads_from','writes_to','transforms','validates','depends_on','tested_by','configures','related','similar_to','deploys','serves','migrates','documents','provisions','routes','defines_schema','triggers','contains_flow','flow_step','cross_domain']);
const fileTypes = new Set(['file','config','document','service','pipeline','table','schema','resource','endpoint']);
const excluded = /^(evidence|tmp|sample_design|data|tests|analysis-data)(\/|$)/;
function fail(list, message) { list.push(message); }
function asArray(value) { return Array.isArray(value) ? value : []; }
function countBy(values, key) { return values.reduce((out, value) => { const k = value && value[key]; if (typeof k === 'string') out[k] = (out[k] || 0) + 1; return out; }, {}); }
function idPath(id) { return typeof id === 'string' ? id.slice(id.indexOf(':') + 1) : ''; }
function fileNodePath(node) { return idPath(node.id); }
function isEnglish(text) { return typeof text === 'string' && /[A-Za-z]{3}/.test(text) && !/[\u0400-\u04ff\u4e00-\u9fff]/.test(text); }
function loadJson(p) { return JSON.parse(fs.readFileSync(p, 'utf8')); }

try {
  const g = loadJson(graphPath);
  const issues = [], warnings = [];
  const nodes = asArray(g.nodes), edges = asArray(g.edges), layers = asArray(g.layers);
  const tour = asArray(g.tour || g.tourSteps);
  const ids = new Set();
  const nodeIndex = new Map();
  const nodeCounts = countBy(nodes, 'type');
  const edgeCounts = countBy(edges, 'type');
  const domainGraph = nodes.some(n => n && ['domain','flow','step'].includes(n.type));
  const ua = path.resolve(path.dirname(graphPath), '..');
  const scan = loadJson(path.join(ua, 'intermediate', 'scan-result.json'));
  const scanned = new Set(asArray(scan.files).map(f => f && f.path).filter(p => typeof p === 'string' && !excluded.test(p)));
  const scannedByLength = [...scanned].sort((a, b) => b.length - a.length);
  const sourcePath = n => { const value = fileNodePath(n); return scannedByLength.find(p => value === p || value.startsWith(`${p}:`)); };
  const isFileLevel = n => n && fileTypes.has(n.type) && Boolean(sourcePath(n));
  nodes.forEach((n, i) => {
    if (!n || typeof n !== 'object') { fail(issues, `Node at index ${i} is not an object`); return; }
    const required = [['id','string'],['type','string'],['name','string'],['summary','string'],['tags','array'],['complexity','string']];
    for (const [key, type] of required) if (!(key in n) || (type === 'array' ? !Array.isArray(n[key]) : typeof n[key] !== type)) fail(issues, `Node at index ${i} has invalid or missing '${key}'`);
    if (typeof n.id === 'string') { if (!n.id) fail(issues, `Node at index ${i} has an empty id`); if (!/^(file|function|class|module|concept|config|document|service|table|endpoint|pipeline|schema|resource|domain|flow|step):/.test(n.id)) fail(issues, `Node '${n.id}' at index ${i} has an invalid ID prefix`); if (ids.has(n.id)) fail(issues, `Duplicate node ID '${n.id}' at indices ${nodeIndex.get(n.id)} and ${i}`); else { ids.add(n.id); nodeIndex.set(n.id, i); } }
    if (!nodeTypes.has(n.type)) fail(issues, `Node '${n.id || `at index ${i}`}' has invalid type '${n.type}'`);
    if (typeof n.name === 'string' && !n.name.trim()) fail(issues, `Node '${n.id || `at index ${i}`}' has an empty name`);
    if (typeof n.summary === 'string') { const base = path.posix.basename(idPath(n.id)); if (!n.summary.trim()) fail(issues, `Node '${n.id || `at index ${i}`}' has an empty summary`); else if (n.summary.trim().toLowerCase() === String(n.name).trim().toLowerCase() || n.summary.trim().toLowerCase() === base.toLowerCase()) warnings.push(`Node '${n.id}' has a generic summary`); else if (!isEnglish(n.summary)) warnings.push(`Node '${n.id}' summary does not appear to be English`); }
    if (Array.isArray(n.tags) && (n.tags.length === 0 || n.tags.some(t => typeof t !== 'string' || !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(t)))) fail(issues, `Node '${n.id || `at index ${i}`}' has invalid tags; expected nonempty lowercase hyphenated strings`);
    if (!['simple','moderate','complex'].includes(n.complexity)) fail(issues, `Node '${n.id || `at index ${i}`}' has invalid complexity '${n.complexity}'`);
    if (typeof n.type === 'string' && typeof n.id === 'string' && !n.id.startsWith(`${n.type}:`)) warnings.push(`Node '${n.id}' type '${n.type}' does not match its ID prefix`);
  });
  if (!nodes.length) fail(issues, 'Graph has zero nodes');
  if (!edges.length) fail(issues, 'Graph has zero edges');
  if (!layers.length) (domainGraph ? warnings : issues).push('Graph has zero layers');
  if (!tour.length) (domainGraph ? warnings : issues).push('Graph has zero tour steps');
  const degree = new Map(nodes.map(n => [n && n.id, 0]));
  edges.forEach((e, i) => {
    if (!e || typeof e !== 'object') { fail(issues, `Edge at index ${i} is not an object`); return; }
    for (const key of ['source','target','type','direction','weight']) if (!(key in e)) fail(issues, `Edge at index ${i} is missing '${key}'`);
    if (typeof e.source !== 'string' || !e.source) fail(issues, `Edge at index ${i} has invalid source`); else if (!ids.has(e.source)) fail(issues, `Edge at index ${i} references non-existent source node '${e.source}'`);
    if (typeof e.target !== 'string' || !e.target) fail(issues, `Edge at index ${i} has invalid target`); else if (!ids.has(e.target)) fail(issues, `Edge at index ${i} references non-existent target node '${e.target}'`);
    if (!edgeTypes.has(e.type)) fail(issues, `Edge at index ${i} has invalid type '${e.type}'`);
    if (!['forward','backward','bidirectional'].includes(e.direction)) fail(issues, `Edge at index ${i} has invalid direction '${e.direction}'`);
    if (typeof e.weight !== 'number' || e.weight < 0 || e.weight > 1) fail(issues, `Edge at index ${i} has invalid weight '${e.weight}'`);
    if (e.source === e.target) warnings.push(`Edge at index ${i} is self-referencing for '${e.source}'`);
    if (ids.has(e.source)) degree.set(e.source, (degree.get(e.source) || 0) + 1); if (ids.has(e.target)) degree.set(e.target, (degree.get(e.target) || 0) + 1);
  });
  const layerPaths = new Map();
  layers.forEach((layer, i) => { const ns = asArray(layer && layer.nodeIds); if (!ns.length) fail(issues, `Layer at index ${i} has an empty nodeIds array`); ns.forEach((id, j) => { if (!ids.has(id)) fail(issues, `Layer ${i} nodeIds[${j}] references non-existent node '${id}'`); const n = nodes[nodeIndex.get(id)]; const p = sourcePath(n || {}); if (p) { if (!layerPaths.has(p)) layerPaths.set(p, new Set()); layerPaths.get(p).add(i); } }); });
  for (const p of scanned) { const assigned = layerPaths.get(p) || new Set(); if (assigned.size === 0) fail(issues, `Scanned in-scope file '${p}' is missing from all layers`); if (assigned.size > 1) fail(issues, `Scanned in-scope file '${p}' appears in ${assigned.size} layers`); }
  const orders = new Set();
  tour.forEach((step, i) => { if (!step || typeof step !== 'object') { warnings.push(`Tour step at index ${i} is not an object`); return; } if (step.order !== i + 1) warnings.push(`Tour step at index ${i} has non-sequential order '${step.order}' (expected ${i + 1})`); if (orders.has(step.order)) warnings.push(`Tour has duplicate order '${step.order}'`); orders.add(step.order); const ns = asArray(step.nodeIds); if (!ns.length) warnings.push(`Tour step ${step.order || i + 1} has no node IDs`); ns.forEach((id, j) => { if (!ids.has(id)) fail(issues, `Tour step ${step.order || i + 1} nodeIds[${j}] references non-existent node '${id}'`); }); });
  if (tour.length < 5 || tour.length > 15) warnings.push(`Tour has ${tour.length} steps; expected 5 through 15`);
  if (tour.length && (!tour[0] || !asArray(tour[0].nodeIds).includes('document:README.md'))) warnings.push(`Tour does not start at 'document:README.md'`);
  for (const [id, d] of degree) if (id && d === 0) warnings.push(`Node '${id}' has no connecting edges`);
  const expected = {document:['documents'],service:['deploys','depends_on'],pipeline:['triggers'],table:['migrates','defines_schema'],schema:['defines_schema'],domain:['contains_flow'],flow:['flow_step']};
  for (const n of nodes) if (n && expected[n.type] && !edges.some(e => e && (e.source === n.id || e.target === n.id) && expected[n.type].includes(e.type))) warnings.push(`${n.type[0].toUpperCase()+n.type.slice(1)} node '${n.id}' lacks an expected ${expected[n.type].join(' or ')} edge`);
  const covered = new Set(nodes.filter(isFileLevel).map(sourcePath));
  if (scanned.size !== 322) fail(issues, `Scoped scan inventory contains ${scanned.size} files; expected 322`);
  for (const p of scanned) if (!covered.has(p)) fail(issues, `Scanned in-scope file '${p}' lacks a file-level graph node`);
  const importMap = scan.importMap || {};
  let importTotal = 0;
  for (const [src, targets] of Object.entries(importMap)) for (const tgt of asArray(targets)) { importTotal++; const source = `file:${src}`, target = `file:${tgt}`; if (!edges.some(e => e && e.source === source && e.target === target && e.type === 'imports' && e.direction === 'forward' && e.weight === 0.7)) fail(issues, `Deterministic import '${source}' -> '${target}' lacks an exact forward imports edge with weight 0.7`); }
  const imports = edges.filter(e => e && e.type === 'imports');
  if (imports.length !== importTotal) fail(issues, `Graph has ${imports.length} imports edges but scan importMap specifies exactly ${importTotal}`);
  const assemble = loadJson(path.join(ua, 'intermediate', 'assemble-review.json'));
  const schema = loadJson(path.join(ua, 'intermediate', 'schema-validation.json'));
  if (!schema.success) fail(issues, 'Upstream schema-validation.json reports unsuccessful validation');
  if (assemble.finalCounts && (assemble.finalCounts.nodes !== nodes.length || assemble.finalCounts.edges !== edges.length)) fail(issues, 'Assemble-review counts do not match assembled graph counts');
  const result = {scriptCompleted:true, issues:[...new Set(issues)], warnings:[...new Set(warnings)], stats:{totalNodes:nodes.length,totalEdges:edges.length,totalLayers:layers.length,tourSteps:tour.length,nodeTypes:nodeCounts,edgeTypes:edgeCounts}};
  fs.writeFileSync(outputPath, JSON.stringify(result, null, 2));
} catch (error) { console.error(error.stack || error.message); process.exit(1); }
