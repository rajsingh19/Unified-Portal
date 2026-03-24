const fs = require("fs");
const path = require("path");
const axios = require("axios");

const OUTPUT_DIR = path.join(process.cwd(), "data", "schemes");
const DATA_GOV_API_KEY =
  process.env.DATA_GOV_API_KEY ||
  "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b";

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-IN,en;q=0.9",
  Connection: "keep-alive",
};

const RAJASTHAN_DISTRICTS = [
  "Ajmer",
  "Alwar",
  "Banswara",
  "Baran",
  "Barmer",
  "Bharatpur",
  "Bhilwara",
  "Bikaner",
  "Bundi",
  "Chittorgarh",
  "Churu",
  "Dausa",
  "Dholpur",
  "Dungarpur",
  "Sri Ganganagar",
  "Hanumangarh",
  "Jaipur",
  "Jaisalmer",
  "Jalore",
  "Jhalawar",
  "Jhunjhunu",
  "Jodhpur",
  "Karauli",
  "Kota",
  "Nagaur",
  "Pali",
  "Pratapgarh",
  "Rajsamand",
  "Sawai Madhopur",
  "Sikar",
  "Sirohi",
  "Tonk",
  "Udaipur",
];

const DISTRICT_ALIASES = new Map(
  [
    ["sriganganagar", "Sri Ganganagar"],
    ["shriganganagar", "Sri Ganganagar"],
    ["ganganagar", "Sri Ganganagar"],
    ["sri ganga nagar", "Sri Ganganagar"],
    ["sawai madhopur", "Sawai Madhopur"],
    ["sawai madhapur", "Sawai Madhopur"],
    ["swaimadhopur", "Sawai Madhopur"],
    ["balotra", "Balotra"],
    ["deeg", "Deeg"],
    ["phalodi", "Phalodi"],
    ["didwana kuchaman", "Didwana Kuchaman"],
    ["gangapur city", "Gangapur City"],
    ["kotputli behror", "Kotputli Behror"],
    ["khairthal tijara", "Khairthal Tijara"],
    ["salumber", "Salumber"],
    ["anuppgarh", "Anupgarh"],
  ].map(([key, value]) => [key.toLowerCase(), value])
);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function randomDelay() {
  await sleep(1000 + Math.random() * 2000);
}

async function fetchWithRetry(url, options = {}, retries = 3) {
  let lastError = null;
  for (let i = 0; i < retries; i += 1) {
    try {
      const response = await axios({
        url,
        timeout: 20000,
        headers: {
          ...HEADERS,
          ...(options.headers || {}),
        },
        ...options,
      });
      await randomDelay();
      return response;
    } catch (error) {
      lastError = error;
      if (i === retries - 1) {
        throw error;
      }
      await sleep(2000);
    }
  }
  throw lastError;
}

function toNumber(value) {
  if (value == null) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const cleaned = String(value)
    .replace(/₹/g, "")
    .replace(/,/g, "")
    .replace(/%/g, "")
    .trim();
  if (!cleaned) return null;
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function pct(numerator, denominator, digits = 2) {
  const n = toNumber(numerator);
  const d = toNumber(denominator);
  if (n == null || d == null || d === 0) return null;
  return Number(((n / d) * 100).toFixed(digits));
}

function deriveStatus(coveragePct) {
  const value = toNumber(coveragePct);
  if (value == null) return "critical";
  if (value >= 65) return "on_track";
  if (value >= 40) return "needs_push";
  return "critical";
}

function cleanText(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function slugify(value) {
  return cleanText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function titleCase(value) {
  return cleanText(value)
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function normalizeDistrictName(name) {
  const raw = cleanText(name)
    .replace(/^\d+\.?\s*/, "")
    .replace(/\(.*?\)/g, "")
    .trim();
  const key = slugify(raw);
  if (!key) return raw;
  if (DISTRICT_ALIASES.has(key)) return DISTRICT_ALIASES.get(key);

  const exact = RAJASTHAN_DISTRICTS.find((district) => slugify(district) === key);
  if (exact) return exact;

  const partial = RAJASTHAN_DISTRICTS.find((district) => {
    const districtKey = slugify(district);
    return districtKey.startsWith(key) || key.startsWith(districtKey);
  });
  if (partial) return partial;

  return titleCase(raw);
}

function failureDistrict(error, fallbackSuggestion) {
  return {
    district_name: "FETCH_FAILED",
    district_code: null,
    metrics: {
      error: cleanText(error),
      fallback_suggestion: cleanText(fallbackSuggestion),
    },
    coverage_pct: null,
    status: "critical",
  };
}

function emptyPayload({
  schemeId,
  schemeName,
  sourceUrl,
  dataYear,
  accessMethod,
  error,
  notes,
}) {
  return {
    scheme_id: schemeId,
    scheme_name: schemeName,
    source_url: sourceUrl,
    fetched_at: new Date().toISOString(),
    state: "Rajasthan",
    data_year: dataYear,
    districts: [failureDistrict(error, notes)],
    state_totals: {},
    metadata: {
      access_method: accessMethod,
      data_reliability: "estimated",
      notes: cleanText(notes),
    },
  };
}

function ensureOutputDir() {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

function sumMetric(districts, key) {
  return districts.reduce((sum, district) => {
    const value = toNumber(district?.metrics?.[key]);
    return sum + (value || 0);
  }, 0);
}

function buildPayload({
  schemeId,
  schemeName,
  sourceUrl,
  dataYear,
  accessMethod,
  reliability,
  notes,
  districts,
  stateTotals = {},
}) {
  return {
    scheme_id: schemeId,
    scheme_name: schemeName,
    source_url: sourceUrl,
    fetched_at: new Date().toISOString(),
    state: "Rajasthan",
    data_year: dataYear,
    districts,
    state_totals: stateTotals,
    metadata: {
      access_method: accessMethod,
      data_reliability: reliability,
      notes: cleanText(notes),
    },
  };
}

function validateDistrictData(districts, schemeName) {
  console.log(`[${schemeName}] Districts fetched: ${districts.length}/33`);

  const pcts = districts.map((d) => d.coverage_pct).filter((p) => p != null);
  const allSame = pcts.length > 0 && pcts.every((p) => p === pcts[0]);
  if (allSame) {
    console.warn(
      `[${schemeName}] WARNING: All coverage_pct values are identical — likely default/fake data`
    );
  }

  const nullMetrics = districts.filter(
    (d) => !d.metrics || Object.keys(d.metrics).length === 0
  );
  if (nullMetrics.length > 0) {
    console.warn(
      `[${schemeName}] WARNING: ${nullMetrics.length} districts have empty metrics`
    );
  }

  const fetchedNames = districts.map((d) => String(d.district_name || ""));
  const missing = RAJASTHAN_DISTRICTS.filter(
    (name) =>
      !fetchedNames.some((fetchedName) =>
        fetchedName.toLowerCase().includes(name.toLowerCase().slice(0, 5))
      )
  );
  if (missing.length > 0) {
    console.warn(`[${schemeName}] Missing districts: ${missing.join(", ")}`);
  }
}

function writeJsonFile(filename, payload) {
  ensureOutputDir();
  const fullPath = path.join(OUTPUT_DIR, filename);
  fs.writeFileSync(fullPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return fullPath;
}

module.exports = {
  DATA_GOV_API_KEY,
  HEADERS,
  OUTPUT_DIR,
  RAJASTHAN_DISTRICTS,
  buildPayload,
  cleanText,
  deriveStatus,
  emptyPayload,
  failureDistrict,
  fetchWithRetry,
  normalizeDistrictName,
  pct,
  randomDelay,
  sumMetric,
  toNumber,
  validateDistrictData,
  writeJsonFile,
};
