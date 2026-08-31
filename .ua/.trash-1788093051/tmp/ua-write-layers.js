const fs = require('fs');
const input = process.argv[2], output = process.argv[3];
if (!input || !output) throw new Error('Usage: node ua-write-layers.js <analysis.json> <layers.json>');
const analysis = JSON.parse(fs.readFileSync(input, 'utf8'));
const layers = [
  { id: 'layer:documentation-governance', name: 'Documentation & Governance', description: 'Architecture diagrams, domain and delivery documentation, and rendered quality guidance that define the Vina Bim Shop platform contracts and evidence.', nodeIds: [] },
  { id: 'layer:project-configuration', name: 'Project Configuration & Tooling', description: 'Repository-level Python, environment, generator, streaming, local-runtime, and developer command configuration for operating the coursework project.', nodeIds: [] },
  { id: 'layer:data-modeling', name: 'Data Contracts & Analytics Modeling', description: 'Kafka and Pinot contracts plus dbt bronze, silver, gold, reconciliation, and lakehouse SQL assets that define the platform data products.', nodeIds: [] },
  { id: 'layer:infrastructure', name: 'Platform Infrastructure', description: 'Docker Compose profiles, service images, storage and query-engine settings, Airflow deployment assets, and runtime bootstrap resources for the local data platform.', nodeIds: [] },
  { id: 'layer:ingestion', name: 'Data Generation & Ingestion', description: 'Synthetic source-data generation and Kafka publication, topic, schema-registration, bronze-sink, and smoke-test application workflows.', nodeIds: [] },
  { id: 'layer:processing-serving', name: 'Batch, Streaming & Serving Processing', description: 'Flink, Spark lakehouse, and Pinot application modules that transform platform data, validate it, and expose realtime or executive analytics.', nodeIds: [] },
  { id: 'layer:orchestration-operations', name: 'Orchestration, Operations & Quality', description: 'Airflow-facing orchestration, DataHub lineage, quality-reporting, and operational automation scripts that coordinate and verify platform runs.', nodeIds: [] }
];
const byId = new Map(layers.map(l => [l.id, l]));
function destination(n) {
  const p = n.filePath;
  if (n.type === 'document' || p.startsWith('architecture/') || p === 'infra/orchestration/gx_docs/default_index.html') return 'layer:documentation-governance';
  if (['.env.example', '.gitattributes', '.python-version', 'pyproject.toml', 'Makefile'].includes(p) || (p.startsWith('configs/') && !p.endsWith('README.md'))) return 'layer:project-configuration';
  if (p.startsWith('infra/analytics/dbt/') || p.startsWith('infra/kafka/schemas/') || p.startsWith('infra/pinot/schemas/') || p.startsWith('infra/pinot/tables/') || p.startsWith('infra/pinot/sql/') || p.startsWith('infra/lakehouse/postgres/') || p.startsWith('infra/lakehouse/trino/sql/')) return 'layer:data-modeling';
  if (p.startsWith('compose/') || p === 'docker-compose.yml' || p.startsWith('infra/')) return 'layer:infrastructure';
  if (p.startsWith('src/vina_bim_shop/generators/') || p.startsWith('src/vina_bim_shop/kafka/')) return 'layer:ingestion';
  if (p.startsWith('src/vina_bim_shop/flink/') || p.startsWith('src/vina_bim_shop/lakehouse/') || p.startsWith('src/vina_bim_shop/pinot/')) return 'layer:processing-serving';
  if (p.startsWith('scripts/') || p === 'src/vina_bim_shop/__init__.py' || p.startsWith('src/vina_bim_shop/datahub_lineage/') || p.startsWith('src/vina_bim_shop/orchestration/') || p.startsWith('src/vina_bim_shop/quality/')) return 'layer:orchestration-operations';
  throw new Error(`No architectural layer for ${p}`);
}
for (const n of analysis.fileNodes) byId.get(destination(n)).nodeIds.push(n.id);
const assigned = layers.flatMap(l => l.nodeIds);
const expected = analysis.fileNodes.map(n => n.id);
const duplicates = assigned.filter((id, index) => assigned.indexOf(id) !== index);
const missing = expected.filter(id => !assigned.includes(id));
const unexpected = assigned.filter(id => !expected.includes(id));
if (layers.some(l => !l.nodeIds.length) || layers.length < 3 || layers.length > 10 || assigned.length !== expected.length || duplicates.length || missing.length || unexpected.length) {
  throw new Error(JSON.stringify({layerCount: layers.length, assigned: assigned.length, expected: expected.length, duplicates, missing, unexpected}));
}
fs.writeFileSync(output, JSON.stringify(layers, null, 2) + '\n');
console.log(JSON.stringify(layers.map(l => ({id:l.id,count:l.nodeIds.length}))));
