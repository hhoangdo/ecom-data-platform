import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const projectRoot = process.cwd();
const uaDir = join(projectRoot, ".ua");
const intermediateDir = join(uaDir, "intermediate");
const corePath = "C:/Users/hhoangdo/.understand-anything/repo/understand-anything-plugin/packages/core/dist/index.js";
const { KnowledgeGraphSchema, validateGraph, saveGraph } = await import(pathToFileURL(corePath).href);

const graph = JSON.parse(readFileSync(join(intermediateDir, "assembled-graph.json"), "utf8"));
const scan = JSON.parse(readFileSync(join(intermediateDir, "scan-result.json"), "utf8"));
const review = JSON.parse(readFileSync(join(intermediateDir, "review.json"), "utf8"));

if (review.approved !== true || (review.issues?.length ?? 0) !== 0) {
  throw new Error(`LLM review is not clean: approved=${review.approved}, issues=${review.issues?.length ?? 0}`);
}

const schema = KnowledgeGraphSchema.safeParse(graph);
if (!schema.success) {
  throw new Error(`KnowledgeGraph schema validation failed: ${schema.error.message}`);
}
const validation = validateGraph(graph);
if (!validation.success || validation.issues.length > 0) {
  throw new Error(
    `Core validation failed or would alter the graph: fatal=${validation.fatal ?? "none"}, issues=${validation.issues.length}`,
  );
}

const nodeIds = graph.nodes.map((node) => node.id);
const nodeIdSet = new Set(nodeIds);
if (nodeIdSet.size !== nodeIds.length) throw new Error("Node IDs are not unique");
if (graph.edges.some((edge) => !nodeIdSet.has(edge.source) || !nodeIdSet.has(edge.target))) {
  throw new Error("At least one edge has a dangling reference");
}

const excludedRoots = ["evidence/", "tmp/", "sample_design/", "data/", "tests/"];
const excludedPath = (filePath) =>
  excludedRoots.some((root) => filePath.startsWith(root)) ||
  filePath.startsWith(".ua/") ||
  filePath.startsWith(".understand-anything/");
const excludedScanPaths = scan.files.map((file) => file.path).filter(excludedPath);
const excludedGraphPaths = graph.nodes.map((node) => node.filePath).filter(Boolean).filter(excludedPath);
if (excludedScanPaths.length || excludedGraphPaths.length) {
  throw new Error(
    `Excluded paths leaked into artifacts: scan=${excludedScanPaths.length}, graph=${excludedGraphPaths.length}`,
  );
}

const scanPaths = new Set(scan.files.map((file) => file.path));
const unexpectedGraphPaths = graph.nodes
  .map((node) => node.filePath)
  .filter(Boolean)
  .filter((filePath) => !scanPaths.has(filePath));
if (unexpectedGraphPaths.length) {
  throw new Error(`Graph contains ${unexpectedGraphPaths.length} paths outside the scan inventory`);
}

const fileNodeByPath = new Map();
for (const node of graph.nodes) {
  if (node.filePath && node.lineRange === undefined && !fileNodeByPath.has(node.filePath)) {
    fileNodeByPath.set(node.filePath, node.id);
  }
}
const missingFileNodes = [...scanPaths].filter((filePath) => !fileNodeByPath.has(filePath));
if (missingFileNodes.length) throw new Error(`Missing ${missingFileNodes.length} file-level nodes`);

const expectedImports = [];
for (const [sourcePath, targetPaths] of Object.entries(scan.importMap)) {
  for (const targetPath of targetPaths) {
    expectedImports.push(`${fileNodeByPath.get(sourcePath)}\u0000${fileNodeByPath.get(targetPath)}`);
  }
}
const actualImports = graph.edges
  .filter((edge) => edge.type === "imports")
  .map((edge) => `${edge.source}\u0000${edge.target}`);
const expectedImportCounts = new Map();
const actualImportCounts = new Map();
for (const key of expectedImports) expectedImportCounts.set(key, (expectedImportCounts.get(key) ?? 0) + 1);
for (const key of actualImports) actualImportCounts.set(key, (actualImportCounts.get(key) ?? 0) + 1);
const importMismatch =
  expectedImports.length !== actualImports.length ||
  [...expectedImportCounts].some(([key, count]) => actualImportCounts.get(key) !== count);
if (importMismatch) {
  throw new Error(`Import mismatch: expected=${expectedImports.length}, actual=${actualImports.length}`);
}

const assigned = graph.layers.flatMap((layer) => layer.nodeIds);
const assignmentCounts = new Map();
for (const id of assigned) assignmentCounts.set(id, (assignmentCounts.get(id) ?? 0) + 1);
const fileNodeIds = [...fileNodeByPath.values()];
const invalidAssignments = fileNodeIds.filter((id) => assignmentCounts.get(id) !== 1);
if (invalidAssignments.length || graph.layers.some((layer) => layer.nodeIds.length === 0)) {
  throw new Error(`Invalid layer assignments: ${invalidAssignments.length}`);
}

const tourOrdersValid = graph.tour.every((step, index) => step.order === index + 1);
const tourRefsValid = graph.tour.every((step) => step.nodeIds.every((id) => nodeIdSet.has(id)));
if (
  graph.tour.length < 5 ||
  graph.tour.length > 15 ||
  !tourOrdersValid ||
  !tourRefsValid ||
  !graph.tour[0].nodeIds.includes("document:README.md")
) {
  throw new Error("Guided tour validation failed");
}

saveGraph(projectRoot, graph);
writeFileSync(
  join(intermediateDir, "fingerprint-input.json"),
  `${JSON.stringify({
    projectRoot,
    sourceFilePaths: scan.files.map((file) => file.path),
    gitCommitHash: graph.project.gitCommitHash,
  }, null, 2)}\n`,
);

process.stdout.write(
  `Final graph saved: ${graph.nodes.length} nodes, ${graph.edges.length} edges, ${actualImports.length} imports, ${graph.layers.length} layers, ${graph.tour.length} tour steps, ${scan.files.length} files\n`,
);
