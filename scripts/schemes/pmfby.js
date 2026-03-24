const { chromium } = require("playwright");
const {
  DATA_GOV_API_KEY,
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

const OUTPUT_FILE = "pmfby.json";
const SEARCH_URL =
  "https://api.data.gov.in/catalog/search?q=PMFBY+Rajasthan+district&format=json&api-key=";
const PMFBY_HOME = "https://www.pmfby.gov.in/";

function resourceMatches(resource) {
  const haystack = JSON.stringify(resource).toLowerCase();
  return (
    haystack.includes("pmfby") &&
    haystack.includes("district") &&
    (haystack.includes("rajasthan") ||
      haystack.includes("2022-23") ||
      haystack.includes("2023-24"))
  );
}

function extractResourceId(searchResponse) {
  const stack = [];
  const walk = (value) => {
    if (Array.isArray(value)) {
      value.forEach(walk);
      return;
    }
    if (value && typeof value === "object") {
      stack.push(value);
      Object.values(value).forEach(walk);
    }
  };
  walk(searchResponse);
  const match = stack.find(
    (item) =>
      item.resource_id &&
      resourceMatches(item)
  );
  return match || null;
}

function parseDatasetRecords(records) {
  const districts = [];
  let dataYear = "latest";

  for (const record of records || []) {
    const districtName = normalizeDistrictName(
      record.District ||
        record.district ||
        record.District_Name ||
        record.district_name
    );
    if (!districtName || /total/i.test(districtName)) continue;

    const farmersInsured = toNumber(
      record.Farmer_Applications_Insured ||
        record.Farmers_Enrolled ||
        record.farmer_applications_insured ||
        record.farmers_enrolled
    );
    const areaInsuredHa = toNumber(
      record.Area_Insured_Ha || record.area_insured_ha || record.Area_Insured
    );
    const sumInsuredCrore = toNumber(
      record.Sum_Insured_Crore || record.sum_insured_crore || record.Sum_Insured
    );
    const claimsPaidCrore = toNumber(
      record.Claims_Paid_Crore || record.claims_paid_crore || record.Claims_Paid
    );
    const year =
      record.Year || record.year || record.Season || record.season || record.Crop_Year;
    if (year) dataYear = cleanText(year);

    const coveragePct = pct(claimsPaidCrore, sumInsuredCrore);
    districts.push({
      district_name: districtName,
      district_code: null,
      metrics: {
        farmer_applications_insured: farmersInsured,
        area_insured_ha: areaInsuredHa,
        sum_insured_crore: sumInsuredCrore,
        claims_paid_crore: claimsPaidCrore,
      },
      coverage_pct: coveragePct,
      status: deriveStatus(coveragePct),
    });
  }

  return { districts, dataYear };
}

async function fetchViaDataGov() {
  const search = await fetchWithRetry(`${SEARCH_URL}${DATA_GOV_API_KEY}`, { method: "GET" });
  const resource = extractResourceId(search.data);
  if (!resource?.resource_id) {
    return null;
  }

  const filters = new URLSearchParams({
    "api-key": DATA_GOV_API_KEY,
    format: "json",
    limit: "100",
    offset: "0",
    "filters[State]": "RAJASTHAN",
  });
  const datasetUrl = `https://api.data.gov.in/resource/${resource.resource_id}?${filters.toString()}`;
  const dataset = await fetchWithRetry(datasetUrl, { method: "GET" });
  const { districts, dataYear } = parseDatasetRecords(dataset.data?.records || []);
  if (districts.length === 0) return null;
  return {
    districts,
    dataYear,
    sourceUrl: datasetUrl,
    notes: `Fetched from data.gov.in resource ${resource.resource_id}.`,
  };
}

async function fetchViaPlaywrightFallback() {
  let browser;
  const capturedPayloads = [];
  try {
    browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
    });
    const context = await browser.newContext({
      userAgent:
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      viewport: { width: 1280, height: 800 },
    });
    const page = await context.newPage();

    page.on("response", async (response) => {
      const url = response.url();
      if (!/district|getstate|analytics|dashboard/i.test(url)) return;
      try {
        const json = await response.json();
        capturedPayloads.push({ url, json });
      } catch (_error) {
        // Ignore non-JSON responses.
      }
    });

    await page.goto(PMFBY_HOME, { waitUntil: "networkidle", timeout: 45000 });
    await page.waitForTimeout(3000);

    const candidate = capturedPayloads.find((payload) => {
      const asText = JSON.stringify(payload.json).toLowerCase();
      return asText.includes("rajasthan") && asText.includes("district");
    });
    if (!candidate) return null;

    const districts = [];
    const maybeArray = Array.isArray(candidate.json)
      ? candidate.json
      : candidate.json?.data || candidate.json?.records || candidate.json?.result || [];
    for (const record of maybeArray) {
      const districtName = normalizeDistrictName(
        record.District ||
          record.district ||
          record.districtName ||
          record.district_name
      );
      if (!districtName || /total/i.test(districtName)) continue;
      const sumInsuredCrore = toNumber(
        record.Sum_Insured_Crore || record.sumInsured || record.sum_insured_crore
      );
      const claimsPaidCrore = toNumber(
        record.Claims_Paid_Crore || record.claimPaid || record.claims_paid_crore
      );
      const farmersInsured = toNumber(
        record.Farmer_Applications_Insured ||
          record.farmerApplications ||
          record.farmers_enrolled
      );
      const areaInsuredHa = toNumber(
        record.Area_Insured_Ha || record.areaInsured || record.area_insured_ha
      );
      const coveragePct = pct(claimsPaidCrore, sumInsuredCrore);

      districts.push({
        district_name: districtName,
        district_code: null,
        metrics: {
          farmer_applications_insured: farmersInsured,
          area_insured_ha: areaInsuredHa,
          sum_insured_crore: sumInsuredCrore,
          claims_paid_crore: claimsPaidCrore,
        },
        coverage_pct: coveragePct,
        status: deriveStatus(coveragePct),
      });
    }

    if (districts.length === 0) return null;
    return {
      districts,
      dataYear: "2023-24",
      sourceUrl: PMFBY_HOME,
      notes: "Captured from PMFBY dashboard network responses using Playwright interception.",
    };
  } finally {
    if (browser) await browser.close();
  }
}

async function fetchPmfby() {
  try {
    const apiResult = await fetchViaDataGov();
    const fallbackResult = apiResult || (await fetchViaPlaywrightFallback());
    if (!fallbackResult) {
      const payload = emptyPayload({
        schemeId: "PMFBY",
        schemeName: "Pradhan Mantri Fasal Bima Yojana 2023-24 Kharif",
        sourceUrl: PMFBY_HOME,
        dataYear: "2023-24",
        accessMethod: "api",
        error:
          "No Rajasthan district PMFBY dataset could be resolved from data.gov.in, and Playwright fallback did not capture usable district JSON.",
        notes:
          "Check for a newer PMFBY resource on data.gov.in or inspect the live PMFBY dashboard API manually.",
      });
      const outputPath = writeJsonFile(OUTPUT_FILE, payload);
      return { ok: false, districts: 0, outputPath, payload };
    }

    validateDistrictData(fallbackResult.districts, "PMFBY");
    const payload = buildPayload({
      schemeId: "PMFBY",
      schemeName: "Pradhan Mantri Fasal Bima Yojana 2023-24 Kharif",
      sourceUrl: fallbackResult.sourceUrl,
      dataYear: fallbackResult.dataYear,
      accessMethod: apiResult ? "api" : "playwright",
      reliability: "confirmed_real",
      notes: fallbackResult.notes,
      districts: fallbackResult.districts,
      stateTotals: {
        farmer_applications_insured: fallbackResult.districts.reduce(
          (sum, district) => sum + (district.metrics.farmer_applications_insured || 0),
          0
        ),
        area_insured_ha: fallbackResult.districts.reduce(
          (sum, district) => sum + (district.metrics.area_insured_ha || 0),
          0
        ),
        sum_insured_crore: fallbackResult.districts.reduce(
          (sum, district) => sum + (district.metrics.sum_insured_crore || 0),
          0
        ),
        claims_paid_crore: fallbackResult.districts.reduce(
          (sum, district) => sum + (district.metrics.claims_paid_crore || 0),
          0
        ),
      },
    });
    const outputPath = writeJsonFile(OUTPUT_FILE, payload);
    return { ok: true, districts: fallbackResult.districts.length, outputPath, payload };
  } catch (error) {
    const payload = emptyPayload({
      schemeId: "PMFBY",
      schemeName: "Pradhan Mantri Fasal Bima Yojana 2023-24 Kharif",
      sourceUrl: PMFBY_HOME,
      dataYear: "2023-24",
      accessMethod: "api",
      error: error.message,
      notes: "Both data.gov.in and PMFBY fallback workflows failed.",
    });
    const outputPath = writeJsonFile(OUTPUT_FILE, payload);
    return { ok: false, districts: 0, outputPath, payload };
  }
}

module.exports = fetchPmfby;
