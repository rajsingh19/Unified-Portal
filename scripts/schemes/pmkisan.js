const {
  DATA_GOV_API_KEY,
  buildPayload,
  cleanText,
  deriveStatus,
  emptyPayload,
  fetchWithRetry,
  normalizeDistrictName,
  validateDistrictData,
  writeJsonFile,
  toNumber,
} = require("./common");

const OUTPUT_FILE = "pm_kisan.json";
const SEARCH_URL =
  "https://api.data.gov.in/catalog/search?q=PM+Kisan+Rajasthan+district+beneficiary&format=json&api-key=";
const ESTIMATED_TOTAL_FARMERS = 7500000;

function resourceMatches(resource) {
  const haystack = JSON.stringify(resource).toLowerCase();
  return haystack.includes("pm kisan") || haystack.includes("pm-kisan");
}

function extractResource(searchResponse) {
  const objects = [];
  const walk = (value) => {
    if (Array.isArray(value)) return value.forEach(walk);
    if (value && typeof value === "object") {
      objects.push(value);
      Object.values(value).forEach(walk);
    }
  };
  walk(searchResponse);
  return objects.find((item) => item.resource_id && resourceMatches(item)) || null;
}

async function fetchPmKisan() {
  try {
    const search = await fetchWithRetry(`${SEARCH_URL}${DATA_GOV_API_KEY}`, { method: "GET" });
    const resource = extractResource(search.data);
    if (!resource?.resource_id) {
      const payload = emptyPayload({
        schemeId: "PMKISAN",
        schemeName: "PM Kisan Samman Nidhi",
        sourceUrl: "https://api.data.gov.in/",
        dataYear: "latest",
        accessMethod: "api",
        error: "No matching PM-Kisan district resource was found in the data.gov.in catalog search response.",
        notes: "Try a broader data.gov.in catalog search or a different beneficiary dataset.",
      });
      const outputPath = writeJsonFile(OUTPUT_FILE, payload);
      return { ok: false, districts: 0, outputPath, payload };
    }

    const params = new URLSearchParams({
      "api-key": DATA_GOV_API_KEY,
      format: "json",
      limit: "100",
      "filters[State_Name]": "RAJASTHAN",
    });
    const datasetUrl = `https://api.data.gov.in/resource/${resource.resource_id}?${params.toString()}`;
    const dataset = await fetchWithRetry(datasetUrl, { method: "GET" });
    const records = dataset.data?.records || [];

    const districts = records
      .map((record) => {
        const districtName = normalizeDistrictName(
          record.District_Name || record.district_name || record.District
        );
        if (!districtName || /total/i.test(districtName)) return null;
        const beneficiaries = toNumber(
          record.No_of_Beneficiaries ||
            record.Registered_Farmers ||
            record.no_of_beneficiaries ||
            record.registered_farmers
        );
        const amountReleasedCrore = toNumber(
          record.Amount_Released_Crore ||
            record.amount_released_crore ||
            record.Installment_Count
        );
        const coveragePct =
          beneficiaries == null
            ? null
            : Number(((beneficiaries / ESTIMATED_TOTAL_FARMERS) * 100).toFixed(4));
        return {
          district_name: districtName,
          district_code: null,
          metrics: {
            no_of_beneficiaries: beneficiaries,
            amount_released_crore: amountReleasedCrore,
          },
          coverage_pct: coveragePct,
          status: deriveStatus(coveragePct),
        };
      })
      .filter(Boolean);

    if (districts.length === 0) {
      const payload = emptyPayload({
        schemeId: "PMKISAN",
        schemeName: "PM Kisan Samman Nidhi",
        sourceUrl: datasetUrl,
        dataYear: "latest",
        accessMethod: "api",
        error:
          "The resolved PM-Kisan dataset did not contain parseable Rajasthan district beneficiary rows.",
        notes: "Check the returned field names or try a different district beneficiary resource.",
      });
      const outputPath = writeJsonFile(OUTPUT_FILE, payload);
      return { ok: false, districts: 0, outputPath, payload };
    }

    validateDistrictData(districts, "PMKISAN");
    const payload = buildPayload({
      schemeId: "PMKISAN",
      schemeName: "PM Kisan Samman Nidhi",
      sourceUrl: datasetUrl,
      dataYear: cleanText(records[0]?.Year || records[0]?.year || "latest"),
      accessMethod: "api",
      reliability: "confirmed_real",
      notes:
        "Fetched from data.gov.in. Coverage percentage uses the provided Rajasthan farmer estimate denominator of 75 lakh when no district denominator exists in the dataset.",
      districts,
      stateTotals: {
        no_of_beneficiaries: districts.reduce(
          (sum, district) => sum + (district.metrics.no_of_beneficiaries || 0),
          0
        ),
        amount_released_crore: districts.reduce(
          (sum, district) => sum + (district.metrics.amount_released_crore || 0),
          0
        ),
      },
    });
    const outputPath = writeJsonFile(OUTPUT_FILE, payload);
    return { ok: true, districts: districts.length, outputPath, payload };
  } catch (error) {
    const payload = emptyPayload({
      schemeId: "PMKISAN",
      schemeName: "PM Kisan Samman Nidhi",
      sourceUrl: "https://api.data.gov.in/",
      dataYear: "latest",
      accessMethod: "api",
      error: error.message,
      notes: "data.gov.in lookup for PM-Kisan failed.",
    });
    const outputPath = writeJsonFile(OUTPUT_FILE, payload);
    return { ok: false, districts: 0, outputPath, payload };
  }
}

module.exports = fetchPmKisan;
