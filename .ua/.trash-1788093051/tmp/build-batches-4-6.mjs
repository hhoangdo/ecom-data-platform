import fs from 'node:fs';
import path from 'node:path';

const root = 'C:/Users/hhoangdo/Documents/Personal Projects/FSDS/coursework';
const ua = path.join(root, '.ua');
const fileSummaries = {
  'smoke.py': 'Builds deterministic streaming smoke-test events and publishes them to the configured Kafka topics.',
  'verification.py': 'Compatibility module that exposes the Flink verification package through its legacy namespace.',
  '__init__.py': 'Initializes the Flink verification package and preserves its legacy namespace compatibility.',
  '_constants.py': 'Defines shared constants used by Flink verification probes, assertions, and cleanup workflows.',
  '_probes.py': 'Provides runtime probes for Kafka, MinIO, Docker, Flink, and Pinot verification evidence.',
  'assertions.py': 'Builds verification state and evaluates ADR-04 and Pinot readiness assertions.',
  'cleanroom.py': 'Resets cleanroom state and removes runtime artifacts before verification runs.',
  'runner.py': 'Coordinates cleanroom verification, preflight evidence collection, and publish phases.',
  'publisher.py': 'Publishes JSON event collections to Kafka topics for streaming workflows.',
  'bootstrap.py': 'Applies Pinot schema and table assets and waits for them to become ready.',
  'evidence.py': 'Captures evidence from Pinot endpoints for the project verification artifacts.',
  'refresh_evidence.py': 'Refreshes Pinot evidence after checking HTTP payload health and query results.',
  'alerts.py': 'Normalizes source alert records and builds payment-failure alerts for the Flink pipeline.',
  'baseline_experiment.py': 'Loads and validates Flink experiment profiles, then runs and compares baseline variants.',
  'commerce_job.py': 'Defines and runs the Flink commerce window pipeline with metrics and correction handling.',
  'config.py': 'Loads the streaming configuration and derives payment alert thresholds.',
  'corrections.py': 'Builds versioned correction records for late or revised streaming metrics.',
  'metrics.py': 'Deduplicates event streams and produces windowed commerce metric snapshots.',
  'ops_job.py': 'Defines and runs the Flink operations-alert pipeline.',
  'runtime.py': 'Loads Flink runtime settings and configures Kafka sources, sinks, timestamps, and checkpointing.',
  'bronze_sink.py': 'Registers the Kafka Connect bronze sink from the project template configuration.',
  'cleanup.py': 'Cleans Kafka topics and related state used by smoke and cleanroom workflows.',
  'consumer_smoke.py': 'Consumes and validates Kafka smoke-test messages against topic schemas.',
  'producer_smoke.py': 'Generates and publishes Kafka smoke-test events.',
  'schema_registry.py': 'Registers project Kafka schema subjects with Schema Registry.',
  'topics.py': 'Loads Kafka topic configuration and exposes source, derived, and combined topic names.',
  'kafka_bootstrap.py': 'Orchestrates Kafka topic, schema, and bronze-sink bootstrap steps for a run.'
};
function domain(p) { return p.includes('/flink/') ? 'flink' : p.includes('/kafka/') ? 'kafka' : p.includes('/pinot/') ? 'pinot' : 'orchestration'; }
function complexity(f) { return f.nonEmptyLines > 200 ? 'complex' : f.nonEmptyLines >= 50 ? 'moderate' : 'simple'; }
function functionSummary(name, p) { return `Implements ${name.replaceAll('_', ' ')} behavior for the ${domain(p)} workflow.`; }
function classSummary(name, p) { return `Models ${name} state and behavior used by the ${domain(p)} workflow.`; }
function fnode(p, f) { return { id:`function:${p}:${f.name}`, type:'function', name:f.name, filePath:p, lineRange:[f.startLine,f.endLine], summary:functionSummary(f.name,p), tags:[domain(p),'function','exported'], complexity:(f.endLine-f.startLine+1)>50?'moderate':'simple' }; }
function cnode(p, c) { return { id:`class:${p}:${c.name}`, type:'class', name:c.name, filePath:p, lineRange:[c.startLine,c.endLine], summary:classSummary(c.name,p), tags:[domain(p),'data-model','class'], complexity:(c.endLine-c.startLine+1)>50?'moderate':'simple' }; }
for (const index of [4,5,6]) {
  const input = JSON.parse(fs.readFileSync(path.join(ua,'tmp',`ua-file-analyzer-input-${index}.json`),'utf8'));
  const extract = JSON.parse(fs.readFileSync(path.join(ua,'tmp',`ua-file-extract-results-${index}.json`),'utf8'));
  const nodes=[]; const edges=[]; const byPath=new Map(extract.results.map(r=>[r.path,r]));
  for (const file of input.batchFiles) {
    const r=byPath.get(file.path); if (!r) throw new Error(`No extraction result for ${file.path}`);
    const filename=file.path.split('/').at(-1);
    const base={id:`file:${file.path}`,type:'file',name:filename,filePath:file.path,summary:fileSummaries[filename] ?? `Provides ${domain(file.path)} workflow support.`,tags:[domain(file.path),'python','workflow'],complexity:complexity(r)};
    nodes.push(base);
    const exports=new Set((r.exports??[]).map(x=>x.name));
    for (const f of r.functions??[]) if (exports.has(f.name)||(f.endLine-f.startLine+1)>=10) { const n=fnode(file.path,f); nodes.push(n); edges.push({source:base.id,target:n.id,type:'contains',direction:'forward',weight:1.0}); if(exports.has(f.name)) edges.push({source:base.id,target:n.id,type:'exports',direction:'forward',weight:.8}); }
    for (const c of r.classes??[]) if (exports.has(c.name)||(c.methods?.length>=2)||(c.endLine-c.startLine+1)>=20) { const n=cnode(file.path,c); nodes.push(n); edges.push({source:base.id,target:n.id,type:'contains',direction:'forward',weight:1.0}); if(exports.has(c.name)) edges.push({source:base.id,target:n.id,type:'exports',direction:'forward',weight:.8}); }
    for (const target of input.batchImportData[file.path]??[]) edges.push({source:base.id,target:`file:${target}`,type:'imports',direction:'forward',weight:.7});
  }
  const fileCount=input.batchFiles.length;
  const partCount=Math.ceil(Math.max(nodes.length/60,edges.length/120));
  const files=[...input.batchFiles].sort((a,b)=>a.path.localeCompare(b.path));
  const per=Math.ceil(fileCount/partCount);
  for(let part=0;part<partCount;part++) {
    const paths=new Set(files.slice(part*per,(part+1)*per).map(f=>f.path));
    const partNodes=nodes.filter(n=>paths.has(n.filePath)); const ids=new Set(partNodes.map(n=>n.id));
    const partEdges=edges.filter(e=>ids.has(e.source));
    const dest=path.join(ua,'intermediate',partCount===1?`batch-${index}.json`:`batch-${index}-part-${part+1}.json`);
    fs.writeFileSync(dest,JSON.stringify({nodes:partNodes,edges:partEdges},null,2)+'\n');
  }
  console.log(JSON.stringify({index,nodes:nodes.length,edges:edges.length,parts:partCount,imports:edges.filter(e=>e.type==='imports').length,expectedImports:Object.values(input.batchImportData).reduce((n,a)=>n+a.length,0)}));
}
