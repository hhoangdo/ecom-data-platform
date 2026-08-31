const fs = require('fs');
const path = require('path');

function fail(message) { console.error(message); process.exit(1); }
if (process.argv.length !== 4) fail('Usage: node ua-arch-analyze.js <assembled-graph.json> <results.json>');

const [inputPath, outputPath] = process.argv.slice(2);
let graph, scan;
try {
  graph = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
  scan = JSON.parse(fs.readFileSync(path.join(path.dirname(inputPath), 'scan-result.json'), 'utf8'));
} catch (error) { fail(`Unable to read JSON inputs: ${error.message}`); }

const scannedPaths = new Set(scan.files.map(f => f.path));
const candidates = graph.nodes.filter(n => scannedPaths.has(n.filePath) && !n.lineRange && !['function', 'class', 'method', 'property', 'interface', 'enum'].includes(n.type));
const byPath = new Map();
for (const n of candidates) {
  if (!byPath.has(n.filePath) || (byPath.get(n.filePath).type === 'file' && n.type !== 'file')) byPath.set(n.filePath, n);
}
const missing = scan.files.map(f => f.path).filter(p => !byPath.has(p));
if (missing.length) fail(`No top-level graph node for ${missing.length} scanned paths: ${missing.join(', ')}`);
const fileNodes = scan.files.map(f => byPath.get(f.path));
const fileIds = new Set(fileNodes.map(n => n.id));
const allEdges = graph.edges.filter(e => fileIds.has(e.source) && fileIds.has(e.target));
const importEdges = allEdges.filter(e => e.type === 'imports');

const split = p => p.replace(/\\/g, '/').split('/');
const paths = fileNodes.map(n => split(n.filePath));
let prefixLength = 0;
while (paths.every(parts => parts.length > prefixLength && parts[prefixLength] === paths[0][prefixLength])) prefixLength++;
function groupFor(n) {
  const parts = split(n.filePath);
  const next = parts[prefixLength];
  return next && parts.length - prefixLength > 1 ? next : (next || 'root');
}
const directoryGroups = {}, nodeTypeGroups = {}, groupOf = {};
for (const n of fileNodes) {
  const group = groupFor(n); groupOf[n.id] = group;
  (directoryGroups[group] ??= []).push(n.id);
  (nodeTypeGroups[n.type] ??= []).push(n.id);
}
const fanIn = Object.fromEntries(fileNodes.map(n => [n.id, 0]));
const fanOut = Object.fromEntries(fileNodes.map(n => [n.id, 0]));
const adjacency = Object.fromEntries(fileNodes.map(n => [n.id, []]));
for (const e of importEdges) { fanOut[e.source]++; fanIn[e.target]++; adjacency[e.source].push(e.target); }
const interCounts = new Map(), groupImports = {}, groupImportedBy = {};
for (const e of importEdges) {
  const a = groupOf[e.source], b = groupOf[e.target];
  (groupImports[a] ??= new Set()).add(b); (groupImportedBy[b] ??= new Set()).add(a);
  const key = `${a}\u0000${b}`; interCounts.set(key, (interCounts.get(key) || 0) + 1);
}
const interGroupImports = [...interCounts].map(([key,count]) => { const [from,to] = key.split('\u0000'); return {from,to,count}; });
const intraGroupDensity = {};
for (const group of Object.keys(directoryGroups)) {
  let internalEdges = 0, totalEdges = 0;
  for (const e of importEdges) if (groupOf[e.source] === group || groupOf[e.target] === group) { totalEdges++; if (groupOf[e.source] === group && groupOf[e.target] === group) internalEdges++; }
  intraGroupDensity[group] = {internalEdges, totalEdges, density: totalEdges ? Number((internalEdges / totalEdges).toFixed(3)) : 0};
}
const labels = [
  [/^(routes|api|controllers|endpoints|handlers|serializers|routers|blueprints)$/, 'api'],
  [/^(services|core|lib|domain|logic|signals|composables|mailers|jobs|channels|internal)$/, 'service'],
  [/^(models|db|data|persistence|repository|entities|migrations|sql|database|schema|entity)$/, 'data'],
  [/^(components|views|pages|ui|layouts|screens)$/, 'ui'], [/^(middleware|plugins|interceptors|guards)$/, 'middleware'],
  [/^(utils|helpers|common|shared|tools|templatetags|pkg)$/, 'utility'], [/^(config|constants|env|settings|management|commands)$/, 'config'],
  [/^(__tests__|test|tests|spec|specs)$/, 'test'], [/^(types|interfaces|schemas|contracts|dtos|dto|request|response)$/, 'types'],
  [/^(docs|documentation|wiki)$/, 'documentation'], [/^(deploy|deployment|infra|infrastructure|k8s|kubernetes|helm|charts|terraform|tf|docker)$/, 'infrastructure'], [/^(\.github|\.gitlab|\.circleci)$/, 'ci-cd']
];
const patternMatches = {};
for (const g of Object.keys(directoryGroups)) patternMatches[g] = (labels.find(([r]) => r.test(g)) || [null, 'unclassified'])[1];
const cross = new Map();
for (const e of allEdges) { const key = `${fileNodes.find(n=>n.id===e.source).type}\u0000${fileNodes.find(n=>n.id===e.target).type}\u0000${e.type}`; cross.set(key,(cross.get(key)||0)+1); }
const crossCategoryEdges = [...cross].map(([key,count]) => {const [fromType,toType,edgeType]=key.split('\u0000'); return {fromType,toType,edgeType,count};});
const infraRe = /(^|\/)(Dockerfile|docker-compose[^/]*|.*\.(tf|tfvars)|Makefile|\.github\/workflows\/|.*\/(k8s|kubernetes|helm|charts|terraform|docker)\/)/i;
const pathsList = fileNodes.map(n=>n.filePath);
const deploymentTopology = {hasDockerfile: pathsList.some(p=>/(^|\/)Dockerfile/i.test(p)),hasCompose:pathsList.some(p=>/docker-compose/i.test(p)),hasK8s:pathsList.some(p=>/(^|\/)(k8s|kubernetes|helm|charts)\//i.test(p)),hasTerraform:pathsList.some(p=>/\.tf(vars)?$/i.test(p)),hasCI:pathsList.some(p=>/(^|\/)(\.github\/workflows|\.gitlab-ci|Jenkinsfile)/i.test(p)),infraFiles:pathsList.filter(p=>infraRe.test(p))};
const selectPaths = pred => fileNodes.filter(pred).map(n=>n.filePath);
const dataPipeline = {schemaFiles:selectPaths(n=>/\.(sql|graphql|gql|proto|prisma)$/i.test(n.filePath)),migrationFiles:selectPaths(n=>/(^|\/)(migrations?)\//i.test(n.filePath)),dataModelFiles:selectPaths(n=>/(^|\/)(models?|db|data|schema)\//i.test(n.filePath)),apiHandlerFiles:selectPaths(n=>/(^|\/)(api|routes|handlers|endpoints|controllers)\//i.test(n.filePath))};
const groupDocs = Object.fromEntries(Object.keys(directoryGroups).map(g=>[g,false]));
for (const n of fileNodes) if (/\.(md|rst)$/i.test(n.filePath)) groupDocs[groupOf[n.id]] = true;
const groups = Object.keys(directoryGroups), documented = groups.filter(g=>groupDocs[g]);
const docCoverage = {groupsWithDocs:documented.length,totalGroups:groups.length,coverageRatio:Number((documented.length/groups.length).toFixed(3)),undocumentedGroups:groups.filter(g=>!groupDocs[g])};
const dependencyDirection = [];
for (const {from,to,count} of interGroupImports) { const reverse = interCounts.get(`${to}\u0000${from}`) || 0; if (count > reverse) dependencyDirection.push({dependent:from,dependsOn:to}); }
const out = {scriptCompleted:true,fileNodes,directoryGroups,nodeTypeGroups,importEdges,allEdges,crossCategoryEdges,interGroupImports,intraGroupDensity,patternMatches,deploymentTopology,dataPipeline,docCoverage,dependencyDirection,fileStats:{totalFileNodes:fileNodes.length,filesPerGroup:Object.fromEntries(groups.map(g=>[g,directoryGroups[g].length])),nodeTypeCounts:Object.fromEntries(Object.entries(nodeTypeGroups).map(([k,v])=>[k,v.length]))},fileFanIn:fanIn,fileFanOut:fanOut,adjacency,groupImports:Object.fromEntries(Object.entries(groupImports).map(([k,v])=>[k,[...v]])),groupImportedBy:Object.fromEntries(Object.entries(groupImportedBy).map(([k,v])=>[k,[...v]]))};
fs.writeFileSync(outputPath, JSON.stringify(out, null, 2));
