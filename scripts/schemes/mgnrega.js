const { chromium } = require("playwright");
const {
  buildPayload,
  cleanText,
  deriveStatus,
  emptyPayload,
  normalizeDistrictName,
  pct,
  randomDelay,
  toNumber,
  validateDistrictData,
  writeJsonFile,
} = require("./common");

const OUTPUT_FILE = "mgnrega.json";
const DIRECT_URL =
  "https://nreganarep.nic.in/netnrega/MISreport4.aspx?fin_year=2024-2025&flag=S&state_code=27&state_name=RAJASTHAN&district_code=0&district_name=ALL+DISTRICTS&block_code=0&block_name=ALL+BLOCKS&panchayat_code=0&panchayat_name=ALL+PANCHAYATS&unit=lakhs&Digest=x";
const FALLBACK_URL = "https://nreganarep.nic.in/netnrega/nregahome.aspx";

function mapHeaders(headers) {
  return headers.map((header) => {
    const text = cleanText(header).toLowerCase();
    if (text.includes("district")) return "district_name";
    if (text.includes("job card")) return "job_cards_issued";
    if (text.includes("household") && text.includes("worked")) return "households_worked";
    if (text.includes("person day")) return "person_days_generated";
    if (text.includes("wage") && text.includes("paid")) return "wages_paid_lakhs";
    if (text.includes("works") && text.includes("completed")) return "works_completed";
    return text.replace(/[^a-z0-9]+/g, "_");
  });
}

async function extractTableRows(page) {
  return page.evaluate(() => {
    const tables = Array.from(document.querySelectorAll("table"));
    return tables.map((table) => {
      const rows = Array.from(table.querySelectorAll("tr")).map((row) =>
        Array.from(row.querySelectorAll("th,td")).map((cell) =>
          (cell.textContent || "").replace(/\s+/g, " ").trim()
        )
      );
      return rows.filter((row) => row.some(Boolean));
    });
  });
}

function parseDistricts(tableRows) {
  for (const rows of tableRows) {
    if (rows.length < 2) continue;
    const headers = mapHeaders(rows[0]);
    if (!headers.includes("district_name")) continue;
    if (!headers.includes("job_cards_issued") && !headers.includes("households_worked")) continue;

    const districts = [];
    for (const row of rows.slice(1)) {
      if (row.length < headers.length) continue;
      const entry = Object.fromEntries(headers.map((header, index) => [header, row[index]]));
      const districtName = normalizeDistrictName(entry.district_name);
      if (!districtName || /total|rajasthan/i.test(districtName)) continue;

      const jobCardsIssued = toNumber(entry.job_cards_issued);
      const householdsWorked = toNumber(entry.households_worked);
      const personDaysGenerated = toNumber(entry.person_days_generated);
      const wagesPaidLakhs = toNumber(entry.wages_paid_lakhs);
      const worksCompleted = toNumber(entry.works_completed);
      const coveragePct = pct(householdsWorked, jobCardsIssued);

      districts.push({
        district_name: districtName,
        district_code: null,
        metrics: {
          job_cards_issued: jobCardsIssued,
          households_worked: householdsWorked,
          person_days_generated: personDaysGenerated,
          wages_paid_lakhs: wagesPaidLakhs,
          works_completed: worksCompleted,
        },
        coverage_pct: coveragePct,
        status: deriveStatus(coveragePct),
      });
    }
    if (districts.length > 0) return districts;
  }
  return [];
}

async function fetchMgnrega() {
  let browser;
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

    await page.goto(DIRECT_URL, { waitUntil: "networkidle", timeout: 45000 });
    await page.waitForSelector("table", { timeout: 30000 });
    let rows = await extractTableRows(page);
    let districts = parseDistricts(rows);

    if (districts.length === 0) {
      await page.goto(FALLBACK_URL, { waitUntil: "networkidle", timeout: 45000 });
      const rajasthanLink = page.locator("a", { hasText: /Rajasthan/i }).first();
      if (await rajasthanLink.count()) {
        await rajasthanLink.click();
        await page.waitForLoadState("networkidle");
        await randomDelay();
        rows = await extractTableRows(page);
        districts = parseDistricts(rows);
      }
    }

    if (districts.length === 0) {
      const payload = emptyPayload({
        schemeId: "MGNREGA",
        schemeName: "Mahatma Gandhi NREGA — Rajasthan 2024-25",
        sourceUrl: DIRECT_URL,
        dataYear: "2024-25",
        accessMethod: "playwright",
        error:
          "The Rajasthan district table did not expose parseable Job Cards / Households Worked columns.",
        notes: "The portal may have changed table markup or moved the data behind a report navigation step.",
      });
      const outputPath = writeJsonFile(OUTPUT_FILE, payload);
      return { ok: false, districts: 0, outputPath, payload };
    }

    validateDistrictData(districts, "MGNREGA");
    const payload = buildPayload({
      schemeId: "MGNREGA",
      schemeName: "Mahatma Gandhi NREGA — Rajasthan 2024-25",
      sourceUrl: DIRECT_URL,
      dataYear: "2024-25",
      accessMethod: "playwright",
      reliability: "confirmed_real",
      notes:
        "Captured from the public MGNREGA Rajasthan report page using Playwright because the ASP.NET report flow depends on a rendered page state.",
      districts,
      stateTotals: {
        job_cards_issued: districts.reduce(
          (sum, district) => sum + (district.metrics.job_cards_issued || 0),
          0
        ),
        households_worked: districts.reduce(
          (sum, district) => sum + (district.metrics.households_worked || 0),
          0
        ),
        person_days_generated: districts.reduce(
          (sum, district) => sum + (district.metrics.person_days_generated || 0),
          0
        ),
        wages_paid_lakhs: districts.reduce(
          (sum, district) => sum + (district.metrics.wages_paid_lakhs || 0),
          0
        ),
        works_completed: districts.reduce(
          (sum, district) => sum + (district.metrics.works_completed || 0),
          0
        ),
      },
    });
    const outputPath = writeJsonFile(OUTPUT_FILE, payload);
    return { ok: true, districts: districts.length, outputPath, payload };
  } catch (error) {
    const payload = emptyPayload({
      schemeId: "MGNREGA",
      schemeName: "Mahatma Gandhi NREGA — Rajasthan 2024-25",
      sourceUrl: DIRECT_URL,
      dataYear: "2024-25",
      accessMethod: "playwright",
      error: error.message,
      notes: "Playwright navigation or district-table parsing failed.",
    });
    const outputPath = writeJsonFile(OUTPUT_FILE, payload);
    return { ok: false, districts: 0, outputPath, payload };
  } finally {
    if (browser) await browser.close();
  }
}

module.exports = fetchMgnrega;
