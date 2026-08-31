const fs = require('fs');
const input = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const report = {
  approved: input.issues.length === 0,
  issues: input.issues,
  warnings: input.warnings,
  stats: input.stats
};
fs.writeFileSync(process.argv[3], JSON.stringify(report, null, 2));
