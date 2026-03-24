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

const OUTPUT_FILE = "jal_jeevan_mission.json";
const PRIMARY_URL = "https://ejalshakti.gov.in/jjmreport/JJMIndia.aspx";
const FALLBACK_URL =
  "https://ejalshakti.gov.in/jjmreport/JJMByDistrict.aspx?state=Rajasthan&stateCode=08";

function mapHeader(header) {
  const normalized = cleanText(header).toLowerCase();
  if (normalized.includes("district")) return "district_name";
  if (normalized.includes("total hh") || normalized.includes("total household")) {
    return "total_rural_households";
  }
  if (
    normalized.includes("tap connection") ||
    normalized.includes("fhtc") ||
    normalized.includes("household tap")
  ) {
    return "tap_connections_given";
  }
  return normalized.replace(/[^a-z0-9]+/g, "_");
}

function extractDistrictsFromHtml(html) {
  const $ = cheerio.load(html);
  const tables = $("table").toArray();

  for (const table of tables) {
    const rows = $(table).find("tr").toArray();
    if (rows.length < 2) continue;

    const headers = $(rows[0])
      .find("th,td")
      .toArray()
      .map((cell) => mapHeader($(cell).text()));

    const hasDistrict = headers.includes("district_name");
    const hasTotalHH = headers.includes("total_rural_households");
    const hasConnections = headers.includes("tap_connections_given");
    if (!hasDistrict || !hasTotalHH || !hasConnections) continue;

    const districts = [];
    for (const row of rows.slice(1)) {
      const cells = $(row)
        .find("td,th")
        .toArray()
        .map((cell) => cleanText($(cell).text()));
      if (cells.length !== headers.length) continue;
      const entry = Object.fromEntries(headers.map((header, idx) => [header, cells[idx]]));
      const districtName = normalizeDistrictName(entry.district_name);
      if (!districtName || /rajasthan|total/i.test(districtName)) continue;

      const totalHH = toNumber(entry.total_rural_households);
      const tapConnections = toNumber(entry.tap_connections_given);
      const coveragePct = pct(tapConnections, totalHH);
      districts.push({
        district_name: districtName,
        district_code: null,
        metrics: {
          total_rural_households: totalHH,
          tap_connections_given: tapConnections,
        },
        coverage_pct: coveragePct,
        status: deriveStatus(coveragePct),
      });
    }
    if (districts.length > 0) {
      return districts;
    }
  }
  return [];
}

async function fetchJjm() {
  try {
    let districts = [];
    const primary = await fetchWithRetry(PRIMARY_URL, { method: "GET" });
    districts = extractDistrictsFromHtml(primary.data);

    if (districts.length === 0) {
      const fallback = await fetchWithRetry(FALLBACK_URL, { method: "GET" });
      districts = extractDistrictsFromHtml(fallback.data);
    }

    if (districts.length === 0) {
      const eventTargetPost = new URLSearchParams({
        __EVENTTARGET: "ddlState",
        __EVENTARGUMENT: "",
        ddlState: "RAJASTHAN",
      }).toString();
      const formResponse = await fetchWithRetry(PRIMARY_URL, {
        method: "POST",
        data: eventTargetPost,
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
      });
      districts = extractDistrictsFromHtml(formResponse.data);
    }

    if (districts.length === 0) {
      const payload = emptyPayload({
        schemeId: "JJM",
        schemeName: "Jal Jeevan Mission — Har Ghar Jal",
        sourceUrl: PRIMARY_URL,
        dataYear: "latest",
        accessMethod: "direct_fetch",
        error: "Could not find a Rajasthan district table with Total HH and Tap Connections columns.",
        notes: "Portal HTML changed or district table is now rendered dynamically.",
      });
      const outputPath = writeJsonFile(OUTPUT_FILE, payload);
      return { ok: false, districts: 0, outputPath, payload };
    }

    validateDistrictData(districts, "JJM");
    const payload = buildPayload({
      schemeId: "JJM",
      schemeName: "Jal Jeevan Mission — Har Ghar Jal",
      sourceUrl: PRIMARY_URL,
      dataYear: "latest",
      accessMethod: "direct_fetch",
      reliability: "confirmed_real",
      notes:
        "Parsed from the public JJM dashboard using HTML table extraction with GET and form-post fallbacks.",
      districts,
      stateTotals: {
        total_rural_households: districts.reduce(
          (sum, district) => sum + (district.metrics.total_rural_households || 0),
          0
        ),
        tap_connections_given: districts.reduce(
          (sum, district) => sum + (district.metrics.tap_connections_given || 0),
          0
        ),
      },
    });
    const outputPath = writeJsonFile(OUTPUT_FILE, payload);
    return { ok: true, districts: districts.length, outputPath, payload };
  } catch (error) {
    const payload = emptyPayload({
      schemeId: "JJM",
      schemeName: "Jal Jeevan Mission — Har Ghar Jal",
      sourceUrl: PRIMARY_URL,
      dataYear: "latest",
      accessMethod: "direct_fetch",
      error: error.message,
      notes: "GET, fallback GET, and form-post attempts all failed.",
    });
    const outputPath = writeJsonFile(OUTPUT_FILE, payload);
    return { ok: false, districts: 0, outputPath, payload };
  }
}

module.exports = fetchJjm;
