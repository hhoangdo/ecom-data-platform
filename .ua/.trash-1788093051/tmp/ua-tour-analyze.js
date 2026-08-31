const fs = require('fs');

function fail(message) { console.error(message); process.exit(1); }

try {
  const [inputPath, outputPath] = process.argv.slice(2);
  if (!inputPath || !outputPath) fail('Usage: node ua-tour-analyze.js <input> <output>');
  const input = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
  const graph = input.graphPath
    ? JSON.parse(fs.readFileSync(input.graphPath, 'utf8'))
    : input;
  const layers = input.layersPath
    ? JSON.parse(fs.readFileSync(input.layersPath, 'utf8'))
    : (graph.layers || []);
  const nodes = graph.nodes || [];
  const edges = graph.edges || [];
  const byId = new Map(nodes.map(n => [n.id, n]));
  const fanIn = Object.fromEntries(nodes.map(n => [n.id, 0]));
  const fanOut = Object.fromEntries(nodes.map(n => [n.id, 0]));
  for (const edge of edges) {
    if (byId.has(edge.source) && byId.has(edge.target)) {
      fanOut[edge.source]++;
      fanIn[edge.target]++;
    }
  }
  const rank = (counts, key) => nodes.map(n => ({id:n.id, [key]:counts[n.id], name:n.name})).sort((a,b) => b[key] - a[key] || a.id.localeCompare(b.id)).slice(0,20);
  const topOutThreshold = [...Object.values(fanOut)].sort((a,b)=>b-a)[Math.max(0, Math.ceil(nodes.length * .1)-1)] || 0;
  const lowInThreshold = [...Object.values(fanIn)].sort((a,b)=>a-b)[Math.max(0, Math.ceil(nodes.length * .25)-1)] || 0;
  const basename = n => (n.filePath || n.name || '').split('/').pop();
  const codeNames = new Set(['index.ts','index.js','main.ts','main.js','app.ts','app.js','server.ts','server.js','mod.rs','main.go','main.py','main.rs','manage.py','app.py','wsgi.py','asgi.py','run.py','__main__.py','Application.java','Main.java','Program.cs','config.ru','index.php','App.swift','Application.kt','main.cpp','main.c']);
  const candidates = nodes.map(n => {
    const path = n.filePath || '';
    let score = 0;
    if (n.type === 'document' && path === 'README.md') score += 5;
    else if (n.type === 'document' && path.endsWith('.md') && !path.includes('/')) score += 2;
    if (n.type === 'file') {
      if (codeNames.has(basename(n))) score += 3;
      if (path.split('/').length <= 2) score++;
      if (fanOut[n.id] >= topOutThreshold) score++;
      if (fanIn[n.id] <= lowInThreshold) score++;
    }
    return {id:n.id, score, name:n.name, summary:n.summary};
  }).filter(x => x.score > 0).sort((a,b) => b.score-a.score || a.id.localeCompare(b.id)).slice(0,5);
  const start = candidates.find(c => byId.get(c.id)?.type === 'file');
  const queue = start ? [start.id] : [], depthMap = {}, order = [];
  if (start) depthMap[start.id] = 0;
  for (let i=0; i<queue.length; i++) {
    const id = queue[i]; order.push(id);
    for (const e of edges) if (e.source === id && ['imports','calls'].includes(e.type) && byId.has(e.target) && depthMap[e.target] === undefined) {
      depthMap[e.target] = depthMap[id] + 1; queue.push(e.target);
    }
  }
  const byDepth = {};
  for (const id of order) (byDepth[depthMap[id]] ||= []).push(id);
  const inv = {documentation:[], infrastructure:[], data:[], config:[]};
  for (const n of nodes) {
    const item={id:n.id,name:n.name,type:n.type,summary:n.summary};
    if(n.type==='document') inv.documentation.push(item);
    if(['service','pipeline','resource'].includes(n.type)) inv.infrastructure.push(item);
    if(['table','schema','endpoint'].includes(n.type)) inv.data.push(item);
    if(n.type==='config') inv.config.push(item);
  }
  const pairCounts = new Map();
  for (const e of edges) if(['imports','calls'].includes(e.type)) pairCounts.set(`${e.source}\u0000${e.target}`, 1);
  const clusters = [];
  for (const e of edges) if(['imports','calls'].includes(e.type) && pairCounts.has(`${e.target}\u0000${e.source}`) && e.source < e.target) clusters.push({nodes:[e.source,e.target],edgeCount:2});
  const index = Object.fromEntries(nodes.map(n=>[n.id,{name:n.name,type:n.type,summary:n.summary}]));
  const result={scriptCompleted:true,entryPointCandidates:candidates,fanInRanking:rank(fanIn,'fanIn'),fanOutRanking:rank(fanOut,'fanOut'),bfsTraversal:{startNode:start?.id||null,order,depthMap,byDepth},nonCodeFiles:inv,clusters:clusters.slice(0,10),layers:{count:layers.length,list:layers.map(({id,name,description})=>({id,name,description}))},nodeSummaryIndex:index,totalNodes:nodes.length,totalEdges:edges.length};
  fs.writeFileSync(outputPath, JSON.stringify(result,null,2));
} catch (error) { fail(error.stack || String(error)); }
