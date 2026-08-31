import fs from 'node:fs';

const root = process.cwd();
const extracted = JSON.parse(fs.readFileSync('.ua/tmp/ua-file-extract-results-2.json', 'utf8'));
const batch = JSON.parse(fs.readFileSync('.ua/tmp/batch-2-object.json', 'utf8'));
const summaries = {
  'bronze.py': 'Builds MinIO object keys and upload commands for landing batch snapshot files in the bronze layer.',
  'evidence.py': 'Captures Spark master and history-server status into timestamped evidence artifacts for batch runs.',
  'executive_mart.py': 'Exports selected Trino gold tables into a DuckDB executive mart and records its manifest and evidence.',
  'maintenance.py': 'Validates, rewrites, and benchmarks Iceberg table data files through Trino maintenance operations.',
  'parity.py': 'Runs DuckDB and Trino parity checks to compare local executive-mart results with gold-layer queries.',
  'runner.py': 'Builds Spark and dbt commands and coordinates batch-stage execution with persisted run summaries.',
  'trino.py': 'Executes Trino queries and performs smoke checks against the gold-layer tables.',
  'window.py': 'Defines UTC parsing and an immutable time-window value object used by batch workflows.',
  'datahub_ingestion.py': 'Coordinates DataHub ingestion, quality-report discovery, and custom lineage emission for a coursework run.',
  'hourly_batch.py': 'Orchestrates hourly lakehouse processing, supporting evidence capture and quality-document preparation.',
  'local_evidence.py': 'Runs the local evidence build command within the generated coursework run directory.',
  'mini_coursework_pipeline.py': 'Implements the end-to-end mini-coursework pipeline from bronze ingestion through quality validation and offline features.',
  'paths.py': 'Creates normalized, timestamped coursework run directories and writes run metadata safely.',
  'pinot_bootstrap.py': 'Bootstraps Pinot schemas and tables for a coursework run while recording command results.',
  'quality_helpers.py': 'Converts dataframe validation outcomes into policy-aware validation reports and documentation artifacts.',
  'reconciliation.py': 'Runs Pinot-versus-Trino reconciliation queries and writes the resulting quality report.',
  'subprocess_helpers.py': 'Provides shared command execution, working-directory, and JSON-fetching utilities for orchestration jobs.',
  'query_examples.py': 'Executes Pinot dashboard and reconciliation examples, compares them with Trino, and writes evidence reports.',
  'policies.py': 'Defines validation severities and layer-specific gating decisions for orchestration quality checks.',
  'reports.py': 'Defines validation-report data and renders JSON and HTML documentation for quality results.'
};
const tagsFor = (path) => {
  if (path.includes('/orchestration/')) return ['orchestration','pipeline','data-quality'];
  if (path.includes('/quality/')) return ['data-quality','validation','reporting'];
  if (path.includes('/pinot/')) return ['pinot','analytics','reconciliation'];
  if (path.includes('/lakehouse/')) return ['lakehouse','spark','data-pipeline'];
  return ['python','utility','data-engineering'];
};
const complexity = (length) => length > 200 ? 'complex' : length >= 50 ? 'moderate' : 'simple';
const fileNodes = new Map();
const allNodes = [];
const allEdges = [];
for (const result of extracted.results) {
  const base = result.path.split('/').at(-1);
  const fileId = `file:${result.path}`;
  const node = {id:fileId,type:'file',name:base,filePath:result.path,summary:summaries[base] ?? `Provides ${base} implementation for the project.`,tags:tagsFor(result.path),complexity:complexity(result.nonEmptyLines)};
  fileNodes.set(result.path, node); allNodes.push(node);
  const exported = new Set((result.exports ?? []).map(x => x.name));
  for (const fn of result.functions ?? []) {
    if ((fn.endLine - fn.startLine + 1) < 10 && !exported.has(fn.name)) continue;
    const id = `function:${result.path}:${fn.name}`;
    const n = {id,type:'function',name:fn.name,filePath:result.path,lineRange:[fn.startLine,fn.endLine],summary:`Implements ${fn.name.replace(/^_/, '').replaceAll('_', ' ')} for ${base}.`,tags:['python','function','data-engineering'],complexity:complexity(fn.endLine-fn.startLine+1)};
    allNodes.push(n);
    allEdges.push({source:fileId,target:id,type:'contains',direction:'forward',weight:1.0});
    if (exported.has(fn.name)) allEdges.push({source:fileId,target:id,type:'exports',direction:'forward',weight:0.8});
  }
  for (const cls of result.classes ?? []) {
    if ((cls.endLine - cls.startLine + 1) < 20 && (cls.methods ?? []).length < 2 && !exported.has(cls.name)) continue;
    const id = `class:${result.path}:${cls.name}`;
    const n = {id,type:'class',name:cls.name,filePath:result.path,lineRange:[cls.startLine,cls.endLine],summary:`Defines the ${cls.name} data structure used by ${base}.`,tags:['python','data-model','type-definition'],complexity:complexity(cls.endLine-cls.startLine+1)};
    allNodes.push(n);
    allEdges.push({source:fileId,target:id,type:'contains',direction:'forward',weight:1.0});
    if (exported.has(cls.name)) allEdges.push({source:fileId,target:id,type:'exports',direction:'forward',weight:0.8});
  }
  for (const targetPath of batch.batchImportData[result.path] ?? []) allEdges.push({source:fileId,target:`file:${targetPath}`,type:'imports',direction:'forward',weight:0.7});
}
const paths = [...fileNodes.keys()].sort();
const partCount = Math.ceil(Math.max(allNodes.length/60, allEdges.length/120));
const chunkSize = Math.ceil(paths.length/partCount);
for (let i=0; i<partCount; i++) {
  const partPaths = new Set(paths.slice(i*chunkSize,(i+1)*chunkSize));
  const nodes = allNodes.filter(n => partPaths.has(n.filePath));
  const ids = new Set(nodes.map(n => n.id));
  const edges = allEdges.filter(e => ids.has(e.source));
  fs.writeFileSync(`.ua/intermediate/batch-2-part-${i+1}.json`, JSON.stringify({nodes,edges},null,2)+'\n');
}
console.log(JSON.stringify({partCount,nodes:allNodes.length,edges:allEdges.length,imports:allEdges.filter(e=>e.type==='imports').length,files:extracted.results.length},null,2));
