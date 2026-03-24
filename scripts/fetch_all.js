const path = require("path");

const fetchJjm     = require("./schemes/jjm");
const fetchMgnrega = require("./schemes/mgnrega");
const fetchPmfby   = require("./schemes/pmfby");
const fetchPmay    = require("./schemes/pmay");
const fetchPmKisan = require("./schemes/pmkisan");
const fetchSbm     = require("./schemes/sbm");

async function runSequentially() {
  const tasks = [
    { label: "JJM",      run: fetchJjm },
    { label: "MGNREGA",  run: fetchMgnrega },
    { label: "PMFBY",    run: fetchPmfby },
    { label: "PMAY-G",   run: fetchPmay },
    { label: "PM Kisan", run: fetchPmKisan },
    { label: "SBM-G",    run: fetchSbm },
  ];

  const results = [];

  for (const task of tasks) {
    try {
      console.log(`\n[${task.label}] Starting fetch...`);
      const result = await task.run();
      results.push({ label: task.label, ...result });
    } catch (error) {
      results.push({
        label: task.label,
        ok: false,
        districts: 0,
        outputPath: null,
        payload: null,
        error: error.message,
      });
    }
  }

  console.log("\nSummary");
  console.log("-------");
  for (const result of results) {
    if (result.ok) {
      console.log(`✓ ${result.label} — ${result.districts} districts fetched`);
    } else {
      const reason =
        result.error ||
        result.payload?.districts?.[0]?.metrics?.error ||
        "Unknown failure";
      console.log(`✗ ${result.label} — FAILED (${reason})`);
    }
  }

  console.log("\nOutput files");
  console.log("------------");
  for (const result of results) {
    if (result.outputPath) {
      console.log(path.resolve(result.outputPath));
    }
  }
}

runSequentially().catch((error) => {
  console.error("Fatal runner failure:", error);
  process.exitCode = 1;
});
