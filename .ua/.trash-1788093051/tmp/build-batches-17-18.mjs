import fs from 'node:fs';
import path from 'node:path';
const root='C:/Users/hhoangdo/Documents/Personal Projects/FSDS/coursework';
const ua=path.join(root,'.ua');
function rawSummary(name) {
  const entity=name.replace(/^raw_/, '').replace(/_/g,' ');
  if (name==='raw_bad_events') return 'Defines the bronze quarantine model for malformed Kafka and dead-letter event payloads.';
  if (name==='raw_bad_snapshots') return 'Defines the bronze quarantine model for malformed periodic table-state snapshot records.';
  if (name.includes('kafka_')) return `Loads raw Kafka ${entity.replace('kafka ','')} event envelopes and adds an ingestion timestamp.`;
  return `Loads raw ${entity} records from Parquet into the bronze layer and adds an ingestion timestamp.`;
}
const macroSummaries={
  'business_logic.sql':'Provides reusable dbt SQL macros for category cost rates and typed JSON value extraction.',
  'generate_schema_name.sql':'Overrides dbt schema naming to preserve the target schema when no custom schema is supplied.',
  'generic_tests.sql':'Defines a generic dbt test that returns rows whose supplied expression is false.',
  'reset_gold_schema.sql':'Provides a dbt macro that recreates the gold schema for build and run commands.'
};
for(const index of [17,18]) {
  const input=JSON.parse(fs.readFileSync(path.join(ua,'tmp',`ua-file-analyzer-input-${index}.json`),'utf8'));
  const extract=JSON.parse(fs.readFileSync(path.join(ua,'tmp',`ua-file-extract-results-${index}.json`),'utf8'));
  const metrics=new Map(extract.results.map(r=>[r.path,r])); const nodes=[]; const edges=[];
  for(const f of input.batchFiles) {
    const name=f.path.split('/').at(-1); const r=metrics.get(f.path); if(!r)throw new Error(`Missing extraction for ${f.path}`);
    if(name==='schema.yml') {
      nodes.push({id:`config:${f.path}`,type:'config',name,filePath:f.path,summary:'Defines dbt bronze-model descriptions and not-null or accepted-value data-quality tests.',tags:['configuration','dbt','data-quality','bronze-layer'],complexity:'moderate'});
    } else if(index===17) {
      nodes.push({id:`file:${f.path}`,type:'file',name,filePath:f.path,summary:macroSummaries[name],tags:['dbt','sql-macro','analytics','transformation'],complexity:'simple'});
    } else {
      nodes.push({id:`file:${f.path}`,type:'file',name,filePath:f.path,summary:rawSummary(name.replace('.sql','')),tags:['dbt','bronze-layer','raw-ingestion','sql'],complexity:'simple'});
    }
  }
  if(index===18) {
    const source='config:infra/analytics/dbt/models/bronze/schema.yml';
    for(const f of input.batchFiles.filter(f=>f.path.endsWith('.sql'))) edges.push({source,target:`file:${f.path}`,type:'configures',direction:'forward',weight:.6});
  }
  fs.writeFileSync(path.join(ua,'intermediate',`batch-${index}.json`),JSON.stringify({nodes,edges},null,2)+'\n');
  console.log(JSON.stringify({index,nodes:nodes.length,edges:edges.length,imports:edges.filter(e=>e.type==='imports').length,skipped:extract.filesSkipped.length}));
}
