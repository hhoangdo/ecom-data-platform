import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const projectRoot = process.cwd();
const uaDir = join(projectRoot, ".ua");
const graph = JSON.parse(readFileSync(join(uaDir, "knowledge-graph.json"), "utf8"));
const scan = JSON.parse(readFileSync(join(uaDir, "intermediate", "scan-result.json"), "utf8"));
const fingerprintsPath = join(uaDir, "fingerprints.json");
if (!existsSync(fingerprintsPath)) {
  throw new Error("fingerprints.json must exist before meta.json is written");
}

const corePath = "C:/Users/hhoangdo/.understand-anything/repo/understand-anything-plugin/packages/core/dist/index.js";
const { saveMeta } = await import(pathToFileURL(corePath).href);
saveMeta(projectRoot, {
  lastAnalyzedAt: graph.project.analyzedAt,
  gitCommitHash: graph.project.gitCommitHash,
  version: graph.version,
  analyzedFiles: scan.totalFiles,
});

process.stdout.write(`Metadata saved after fingerprints: ${scan.totalFiles} files at ${graph.project.gitCommitHash}\n`);
