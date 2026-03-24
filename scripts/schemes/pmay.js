const cheerio = require("cheerio");
const {
  buildPayload,
  cleanText,
  deriveStatus,
  emptyPayload,
  fetchWithRetry,
  normalizeDistrictName,
  pct,
  toNumber,
  validateDistrictData,
  writeJsonFile,
} = require("./common");

const OUTPUT_FILE = "pmay_gramin.json";
const SOURCE_URL =
  "https://rhreporting.nic.in/netiay/PhysicalProgressReport/physicalProgressMainReport.aspx";
const FALLBACK_URL =
  "https://rhreporting.nic.in/netiay/DataAnalytics/PhysicalProgressRpt.aspx";

function parseTable(html) {
  const $ = cheerio.load(html);
  const tables = $("table").toArray();
  for (const table of tables) {
    const rows = $(table).find("tr").toArray();
    if (rows.length < 2) continue;
    const headers = $(rows[0])
      .find("th,td")
      .toArray()
      .map((cell) => cleanText($(cell).text()).toLowerCase());
    if (!headers.some((header) => header.includes("district"))) continue;
    if (!headers.some((header) => header.includes("sanction"))) continue;
    if (!headers.some((header) => header.includes("complete"))) continue;

    const districts = [];
    for (const row of rows.slice(1)) {
      const cells = $(row)
        .find("td,th")
        .toArray()
        .map((cell) => cleanText($(cell).text()));
      if (cells.length !== headers.length) continue;
      const districtName = normalizeDistrictName(
        cells[headers.findIndex((header) => header.includes("district"))]
      );
      if (!districtName || /total|rajasthan/i.test(districtName)) continue;
      const sanctioned = toNumber(
        cells[headers.findIndex((header) => header.includes("sanction"))]
      );
      const completed = toNumber(
        cells[headers.findIndex((header) => header.includes("complete"))]
      );
      const completionPct = pct(completed, sanctioned);
      districts.push({
        district_name: districtName,
        district_code: null,
        metrics: {
          houses_sanctioned: sanctioned,
          houses_completed: completed,
          completion_pct: completionPct,
        },
        coverage_pct: completionPct,
        status: deriveStatus(completionPct),
      });
    }
    if (districts.length > 0) return districts;
  }
  return [];
}

async function fetchPmay() {
  try {
    let initial;
    let activeSourceUrl = SOURCE_URL;
    try {
      initial = await fetchWithRetry(SOURCE_URL, { method: "GET" });
    } catch (_error) {
      initial = await fetchWithRetry(FALLBACK_URL, { method: "GET" });
      activeSourceUrl = FALLBACK_URL;
    }

    if (String(initial.data).includes("Report is not available from 12:00 AM to 04:00 AM")) {
      const payload = emptyPayload({
        schemeId: "PMAYG",
        schemeName: "PM Awas Yojana Gramin — Rajasthan",
        sourceUrl: activeSourceUrl,
        dataYear: "latest",
        accessMethod: "direct_fetch",
        error:
          "The PMAY-G analytics report is officially unavailable on the source portal between 12:00 AM and 04:00 AM.",
        notes:
          "Re-run this fetch outside the source maintenance window or switch to a different PMAY-G public report path.",
      });
      const outputPath = writeJsonFile(OUTPUT_FILE, payload);
      return { ok: false, districts: 0, outputPath, payload };
    }

    const viewState =
      String(initial.data).match(/id="__VIEWSTATE" value="([^"]+)"/)?.[1] || "";

    const stateCodes = ["09", "RJ", "27"];
    let districts = [];

    for (const stateCode of stateCodes) {
      const form = new URLSearchParams({
        __VIEWSTATE: viewState,
        ddlState: stateCode,
        btnSubmit: "Submit",
      }).toString();
      const response = await fetchWithRetry(activeSourceUrl, {
        method: "POST",
        data: form,
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
      });
      districts = parseTable(response.data);
      if (districts.length > 0) break;
    }

    if (districts.length === 0) {
      districts = parseTable(initial.data);
    }

    if (districts.length === 0) {
      const payload = emptyPayload({
        schemeId: "PMAYG",
        schemeName: "PM Awas Yojana Gramin — Rajasthan",
        sourceUrl: activeSourceUrl,
        dataYear: "latest",
        accessMethod: "direct_fetch",
        error:
          "Could not resolve a Rajasthan district progress table with sanctioned and completed house counts.",
        notes: "The page may require a different form parameter set or report path.",
      });
      const outputPath = writeJsonFile(OUTPUT_FILE, payload);
      return { ok: false, districts: 0, outputPath, payload };
    }

    validateDistrictData(districts, "PMAYG");
    const payload = buildPayload({
      schemeId: "PMAYG",
      schemeName: "PM Awas Yojana Gramin — Rajasthan",
      sourceUrl: activeSourceUrl,
      dataYear: "latest",
      accessMethod: "direct_fetch",
      reliability: "confirmed_real",
      notes:
        "Fetched from the PMAY-G physical progress report using the ASP.NET form workflow and district-table parsing.",
      districts,
      stateTotals: {
        houses_sanctioned: districts.reduce(
          (sum, district) => sum + (district.metrics.houses_sanctioned || 0),
          0
        ),
        houses_completed: districts.reduce(
          (sum, district) => sum + (district.metrics.houses_completed || 0),
          0
        ),
      },
    });
    const outputPath = writeJsonFile(OUTPUT_FILE, payload);
    return { ok: true, districts: districts.length, outputPath, payload };
  } catch (error) {
    const payload = emptyPayload({
      schemeId: "PMAYG",
      schemeName: "PM Awas Yojana Gramin — Rajasthan",
      sourceUrl: FALLBACK_URL,
      dataYear: "latest",
      accessMethod: "direct_fetch",
      error: error.message,
      notes: "GET/POST PMAY-G progress flow failed.",
    });
    const outputPath = writeJsonFile(OUTPUT_FILE, payload);
    return { ok: false, districts: 0, outputPath, payload };
  }
}

module.exports = fetchPmay;
