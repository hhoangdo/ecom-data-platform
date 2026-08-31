import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const projectRoot = process.cwd();
const uaDir = join(projectRoot, ".ua");
const readJson = (...parts) => JSON.parse(readFileSync(join(uaDir, ...parts), "utf8"));
const graph = readJson("knowledge-graph.json");
const scan = readJson("intermediate", "scan-result.json");
const review = readJson("intermediate", "review.json");
const fingerprints = readJson("fingerprints.json");
const meta = readJson("meta.json");
const config = readJson("config.json");
const corePath = "C:/Users/hhoangdo/.understand-anything/repo/understand-anything-plugin/packages/core/dist/index.js";
const { KnowledgeGraphSchema, validateGraph } = await import(pathToFileURL(corePath).href);

const failures = [];
const check = (condition, message) => {
  if (!condition) failures.push(message);
};

const schema = KnowledgeGraphSchema.safeParse(graph);
const coreValidation = validateGraph(graph);
check(schema.success, "KnowledgeGraph schema validation failed");
check(coreValidation.success, `Core validation failed: ${coreValidation.fatal ?? "unknown"}`);
check(coreValidation.issues.length === 0, `Core validation found ${coreValidation.issues.length} repair issues`);
check(graph.version === "1.0.0", `Unexpected graph version ${graph.version}`);
check(graph.kind === "codebase", `Unexpected graph kind ${graph.kind}`);

const branch = execFileSync("git", ["branch", "--show-current"], { cwd: projectRoot, encoding: "utf8" }).trim();
const commit = execFileSync("git", ["rev-parse", "HEAD"], { cwd: projectRoot, encoding: "utf8" }).trim();
check(branch === "feature/understand-anything", `Unexpected branch ${branch}`);
check(commit === "94236b9b99203e677248aeb4b8617164933fc110", `Unexpected commit ${commit}`);
check(graph.project.gitCommitHash === commit, "Graph commit does not match HEAD");

const nodeIds = graph.nodes.map((node) => node.id);
const nodeIdSet = new Set(nodeIds);
check(nodeIdSet.size === nodeIds.length, "Node IDs are not unique");
check(
  graph.edges.every((edge) => nodeIdSet.has(edge.source) && nodeIdSet.has(edge.target)),
  "An edge has a dangling reference",
);

const excludedRoots = ["evidence/", "tmp/", "sample_design/", "data/", "tests/"];
const excludedPath = (filePath) =>
  excludedRoots.some((root) => filePath.startsWith(root)) ||
  filePath.startsWith(".ua/") ||
  filePath.startsWith(".understand-anything/");
const scanPaths = scan.files.map((file) => file.path);
const scanPathSet = new Set(scanPaths);
const graphPaths = graph.nodes.map((node) => node.filePath).filter(Boolean);
check(scan.totalFiles === 322 && scanPaths.length === 322, "Scan inventory is not 322 files");
check(!scanPaths.some(excludedPath), "Excluded path found in scan inventory");
check(!graphPaths.some(excludedPath), "Excluded path found in graph");
check(graphPaths.every((filePath) => scanPathSet.has(filePath)), "Graph path not present in scan inventory");

const fileNodeByPath = new Map();
for (const node of graph.nodes) {
  if (node.filePath && node.lineRange === undefined && !fileNodeByPath.has(node.filePath)) {
    fileNodeByPath.set(node.filePath, node.id);
  }
}
check(scanPaths.every((filePath) => fileNodeByPath.has(filePath)), "Not every scanned file has a file-level node");

const pairCounts = (pairs) => {
  const counts = new Map();
  for (const pair of pairs) counts.set(pair, (counts.get(pair) ?? 0) + 1);
  return counts;
};
const expectedImports = [];
for (const [sourcePath, targets] of Object.entries(scan.importMap)) {
  for (const targetPath of targets) {
    expectedImports.push(`${fileNodeByPath.get(sourcePath)}\u0000${fileNodeByPath.get(targetPath)}`);
  }
}
const actualImports = graph.edges
  .filter((edge) => edge.type === "imports")
  .map((edge) => `${edge.source}\u0000${edge.target}`);
const expectedImportCounts = pairCounts(expectedImports);
const actualImportCounts = pairCounts(actualImports);
check(expectedImports.length === actualImports.length, "Deterministic import count mismatch");
check(
  [...expectedImportCounts].every(([pair, count]) => actualImportCounts.get(pair) === count),
  "Deterministic import edge set mismatch",
);

const assignmentCounts = new Map();
for (const id of graph.layers.flatMap((layer) => layer.nodeIds)) {
  assignmentCounts.set(id, (assignmentCounts.get(id) ?? 0) + 1);
}
check(graph.layers.length >= 3 && graph.layers.length <= 10, "Layer count outside 3–10");
check(graph.layers.every((layer) => layer.nodeIds.length > 0), "Empty layer found");
check(
  [...fileNodeByPath.values()].every((id) => assignmentCounts.get(id) === 1),
  "A file-level node is missing from layers or assigned more than once",
);

check(graph.tour.length >= 5 && graph.tour.length <= 15, "Tour step count outside 5–15");
check(graph.tour.every((step, index) => step.order === index + 1), "Tour orders are not sequential");
check(graph.tour.every((step) => step.nodeIds.every((id) => nodeIdSet.has(id))), "Tour has dangling references");
check(graph.tour[0].nodeIds.includes("document:README.md"), "Tour does not begin at README.md");

const fingerprintPaths = Object.keys(fingerprints.files);
check(fingerprints.version === "1.0.0", "Unexpected fingerprint version");
check(fingerprints.gitCommitHash === commit, "Fingerprint commit does not match HEAD");
check(fingerprintPaths.length === 322, `Expected 322 fingerprints, found ${fingerprintPaths.length}`);
check(fingerprintPaths.every((filePath) => scanPathSet.has(filePath)), "Fingerprint path outside scan inventory");
check(meta.gitCommitHash === commit, "Metadata commit does not match HEAD");
check(meta.version === "1.0.0" && meta.analyzedFiles === 322, "Metadata values are inconsistent");
check(config.autoUpdate === false && config.outputLanguage === "en", "Configuration is inconsistent");
check(review.approved === true && (review.issues?.length ?? 0) === 0, "LLM review has critical issues");

if (failures.length > 0) {
  for (const failure of failures) process.stderr.write(`FAIL: ${failure}\n`);
  process.exit(1);
}

process.stdout.write(`${JSON.stringify({
  success: true,
  branch,
  commit,
  files: scan.totalFiles,
  nodes: graph.nodes.length,
  edges: graph.edges.length,
  imports: actualImports.length,
  layers: graph.layers.length,
  tourSteps: graph.tour.length,
  fingerprints: fingerprintPaths.length,
  reviewCriticalIssues: review.issues.length,
  reviewWarnings: review.warnings.length,
}, null, 2)}\n`);
