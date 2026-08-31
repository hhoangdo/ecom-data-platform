import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const projectRoot = process.cwd();
const intermediateDir = join(projectRoot, ".ua", "intermediate");
const corePath = "C:/Users/hhoangdo/.understand-anything/repo/understand-anything-plugin/packages/core/dist/index.js";
const { KnowledgeGraphSchema, validateGraph } = await import(pathToFileURL(corePath).href);

const readJson = (name) => JSON.parse(readFileSync(join(intermediateDir, name), "utf8"));
const fragments = readJson("assembled-graph.json");
const scan = readJson("scan-result.json");
const layers = readJson("layers.json");
const tour = readJson("tour.json");
const gitCommitHash = execFileSync("git", ["rev-parse", "HEAD"], {
  cwd: projectRoot,
  encoding: "utf8",
}).trim();

let graph = {
  version: "1.0.0",
  kind: "codebase",
  project: {
    name: scan.name,
    languages: scan.languages,
    frameworks: scan.frameworks,
    description: scan.description,
    analyzedAt: new Date().toISOString(),
    gitCommitHash,
  },
  nodes: fragments.nodes,
  edges: fragments.edges,
  layers,
  tour,
};

const normalized = validateGraph(graph);
if (!normalized.success) {
  throw new Error(`Core graph validation failed: ${normalized.fatal ?? "unknown error"}`);
}
if (normalized.issues.length > 0) {
  graph = { ...normalized.data, kind: "codebase" };
}

const schema = KnowledgeGraphSchema.safeParse(graph);
if (!schema.success) {
  throw new Error(`KnowledgeGraph schema validation failed: ${schema.error.message}`);
}

const nodeIds = graph.nodes.map((node) => node.id);
const nodeIdSet = new Set(nodeIds);
if (nodeIdSet.size !== nodeIds.length) {
  throw new Error(`Duplicate node IDs: ${nodeIds.length - nodeIdSet.size}`);
}

const danglingEdges = graph.edges.filter(
  (edge) => !nodeIdSet.has(edge.source) || !nodeIdSet.has(edge.target),
);
if (danglingEdges.length > 0) {
  throw new Error(`Dangling edges: ${danglingEdges.length}`);
}

const topLevelNodeByPath = new Map();
for (const node of graph.nodes) {
  if (node.filePath && node.lineRange === undefined && !topLevelNodeByPath.has(node.filePath)) {
    topLevelNodeByPath.set(node.filePath, node.id);
  }
}
const fileNodeIds = scan.files.map((file) => topLevelNodeByPath.get(file.path));
const missingFileNodes = scan.files
  .filter((file) => !topLevelNodeByPath.has(file.path))
  .map((file) => file.path);
if (missingFileNodes.length > 0) {
  throw new Error(`Missing file-level nodes: ${missingFileNodes.join(", ")}`);
}

const layerAssignments = graph.layers.flatMap((layer) => layer.nodeIds);
const assignmentCounts = new Map();
for (const id of layerAssignments) {
  assignmentCounts.set(id, (assignmentCounts.get(id) ?? 0) + 1);
}
const missingLayerAssignments = fileNodeIds.filter((id) => !assignmentCounts.has(id));
const duplicateLayerAssignments = fileNodeIds.filter((id) => assignmentCounts.get(id) !== 1);
const emptyLayers = graph.layers.filter((layer) => layer.nodeIds.length === 0);
if (missingLayerAssignments.length || duplicateLayerAssignments.length || emptyLayers.length) {
  throw new Error(
    `Layer coverage failed: missing=${missingLayerAssignments.length}, duplicates=${duplicateLayerAssignments.length}, empty=${emptyLayers.length}`,
  );
}

const expectedOrders = graph.tour.map((_, index) => index + 1);
const actualOrders = graph.tour.map((step) => step.order);
const invalidTourRefs = graph.tour.flatMap((step) => step.nodeIds).filter((id) => !nodeIdSet.has(id));
if (
  graph.tour.length < 5 ||
  graph.tour.length > 15 ||
  actualOrders.some((order, index) => order !== expectedOrders[index]) ||
  invalidTourRefs.length > 0 ||
  !graph.tour[0].nodeIds.includes("document:README.md")
) {
  throw new Error("Tour validation failed");
}

writeFileSync(join(intermediateDir, "assembled-graph.json"), `${JSON.stringify(graph, null, 2)}\n`);
writeFileSync(
  join(intermediateDir, "schema-validation.json"),
  `${JSON.stringify({
    success: true,
    autoFixIssues: normalized.issues,
    nodes: graph.nodes.length,
    edges: graph.edges.length,
    imports: graph.edges.filter((edge) => edge.type === "imports").length,
    layers: graph.layers.length,
    assignedFileNodes: fileNodeIds.length,
    tourSteps: graph.tour.length,
  }, null, 2)}\n`,
);

process.stdout.write(
  `Validated graph: ${graph.nodes.length} nodes, ${graph.edges.length} edges, ${graph.layers.length} layers, ${graph.tour.length} tour steps; auto-fixes=${normalized.issues.length}\n`,
);
