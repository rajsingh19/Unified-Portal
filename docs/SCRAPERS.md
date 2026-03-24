# Scrapers Data README

This document explains what each scraper in `backend/scrapers/` collects, from which website, and the type of data returned by each scraper.

## Quick Summary

| Scraper | Source website | Main data scraped | Output shape |
| --- | --- | --- | --- |
| `igod_scraper.py` | `igod.gov.in` | Rajasthan government portal directory entries | List of portal records |
| `jansoochna_scraper.py` | `jansoochna.rajasthan.gov.in` | Jan Soochna scheme listing | List of basic scheme records |
| `jansoochna_full_scraper.py` | `jansoochna.rajasthan.gov.in` | Detailed Jan Soochna scheme dataset | List of enriched scheme records |
| `rajras_scraper.py` | `rajras.in` | Rajasthan scheme list with article details | List of basic scheme records |
| `rajras_full_scraper.py` | `rajras.in` | Detailed RajRAS scheme dataset | List of enriched scheme records |
| `myscheme_scraper.py` | `myscheme.gov.in` | Rajasthan-relevant government schemes | List of scheme records |
| `jjm_scraper.py` | `ejalshakti.gov.in` | District-wise tap water coverage in Rajasthan | List of district coverage records |
| `pmksy_scraper.py` | `rajas.rajasthan.gov.in` | District irrigation coverage derived from agriculture statistics PDF | List of district irrigation records |
| `sparkline_scraper.py` | `prsindia.org`, `ejalshakti.gov.in` | 6-year budget and coverage trends | Dictionary with arrays and metadata |
| `budget_scraper.py` | `prsindia.org`, `finance.rajasthan.gov.in`, `rajras.in`, `ejalshakti.gov.in` | Rajasthan budget KPIs, districts, and trend data | Single merged dictionary |

## 1. `igod_scraper.py`

**Website scraped:** `https://igod.gov.in/sg/RJ/SPMA/organizations`

**What data is scraped:**
- Rajasthan government organization / portal name
- Department / ministry label
- Portal category
- Website URL
- Domain name
- Status and scrape metadata

**Returned data type:** `List[Dict]`

**Fields:**

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `string` | Unique scraper record id |
| `organization_name` | `string` | Name of the portal / organization |
| `department` | `string` | Department label |
| `ministry` | `string` | Ministry label |
| `category` | `string` | Category inferred from name/domain |
| `website_url` | `string` | Portal URL |
| `domain` | `string` | Parsed domain from URL |
| `status` | `string` | Current status, usually `Active` |
| `source` | `string` | Source label |
| `scraped_at` | `string` | ISO timestamp |

## 2. `jansoochna_scraper.py`

**Website scraped:** `https://jansoochna.rajasthan.gov.in`

**What data is scraped:**
- Scheme name
- Department
- Scheme category
- Scheme URL
- Short description
- Beneficiary count if available
- Status and scrape timestamp

**Returned data type:** `List[Dict]`

**Fields:**

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `string` | Unique scheme id |
| `name` | `string` | Scheme name |
| `category` | `string` | Scheme category |
| `department` | `string` | Department name |
| `url` | `string` | Scheme page URL |
| `description` | `string` | Short scheme summary |
| `beneficiary_count` | `string` | Raw beneficiary count text |
| `status` | `string` | Scheme status |
| `source` | `string` | Source label |
| `scraped_at` | `string` | ISO timestamp |

## 3. `jansoochna_full_scraper.py`

**Website scraped:** `https://jansoochna.rajasthan.gov.in`

**What data is scraped:**
- Scheme master list from API or HTML
- Scheme id and URL
- Detailed description
- Benefits
- Eligibility criteria
- Required documents
- Beneficiary count and beneficiary text
- Budget / assistance amount text
- Progress percentage and progress source text

**Returned data type:** `List[Dict]`

**Fields:**

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `string` | Unique record id |
| `scheme_id` | `string \| null` | Scheme id from portal/API |
| `name` | `string` | Scheme name |
| `category` | `string` | Inferred category |
| `department` | `string \| null` | Department name |
| `description` | `string \| null` | Scheme summary |
| `benefits` | `list[string] \| null` | Benefits extracted from API/page |
| `eligibility` | `list[string] \| null` | Eligibility points |
| `documents_required` | `list[string] \| null` | Document list |
| `beneficiary_count` | `integer \| null` | Parsed numeric beneficiary count |
| `beneficiaries` | `string \| null` | Beneficiary coverage text |
| `budget` | `string \| null` | Extracted amount text like `₹...` |
| `progress_pct` | `float \| null` | Progress percentage |
| `progress` | `string \| null` | Progress as display text |
| `progress_source` | `string \| null` | Sentence used for progress |
| `progress_updated_at` | `string \| null` | ISO timestamp when progress was set |
| `source` | `string` | Source label |
| `url` | `string` | Scheme detail URL |
| `scraped_at` | `string` | ISO timestamp |

## 4. `rajras_scraper.py`

**Website scraped:** `https://rajras.in/ras/pre/rajasthan/adm/schemes/`

**What data is scraped:**
- Scheme names from the RajRAS schemes index
- Sector and subsection
- Article URL
- Description / objective
- Eligibility
- Benefit
- Apply link if found

**Returned data type:** `List[Dict]`

**Fields:**

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `string` | Unique scheme id |
| `name` | `string` | Scheme name |
| `category` | `string` | Sector category |
| `subcategory` | `string` | Subsection heading |
| `url` | `string` | RajRAS article URL |
| `description` | `string` | Description or objective text |
| `objective` | `string` | Objective section text |
| `eligibility` | `string` | Eligibility text |
| `benefit` | `string` | Benefit text |
| `apply_url` | `string` | External apply link if found |
| `has_article` | `boolean` | Whether article URL exists |
| `status` | `string` | Scheme status |
| `source` | `string` | Source label |
| `scraped_at` | `string` | ISO timestamp |

## 5. `rajras_full_scraper.py`

**Website scraped:** `https://rajras.in/ras/pre/rajasthan/adm/schemes/` and individual RajRAS scheme pages

**What data is scraped:**
- All discovered scheme article links
- Scheme name
- Category
- Description
- Headings from the article
- Benefits
- Eligibility
- Required documents
- Beneficiaries text
- Launch year
- Budget / assistance text
- District coverage text
- Progress percentage and supporting source sentence

**Returned data type:** `List[Dict]`

**Fields:**

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `string` | Unique record id |
| `name` | `string` | Scheme name |
| `category` | `string \| null` | Inferred scheme category |
| `description` | `string \| null` | Scheme summary |
| `headings` | `list[string] \| null` | Article section headings |
| `benefits` | `list[string] \| null` | Benefits list |
| `eligibility` | `list[string] \| null` | Eligibility list |
| `documents_required` | `list[string] \| null` | Required documents |
| `beneficiaries` | `string \| null` | Beneficiary group / coverage text |
| `launch_year` | `integer \| null` | Detected launch year |
| `budget` | `string \| null` | Budget or assistance amount |
| `districts` | `string \| null` | District coverage text like `All 33` |
| `progress_pct` | `float \| null` | Progress percentage |
| `progress` | `string \| null` | Progress display string |
| `progress_source` | `string \| null` | Source sentence for progress |
| `progress_updated_at` | `string \| null` | ISO timestamp when progress was set |
| `source` | `string` | Source label |
| `url` | `string` | Scheme page URL |

## 6. `myscheme_scraper.py`

**Website scraped:** `https://www.myscheme.gov.in`

**What data is scraped:**
- Scheme name
- Category
- Ministry / department
- Scheme page link
- Description
- Benefits
- Eligibility
- Launch date if present
- State relevance

**Returned data type:** `List[Dict]`

**Fields:**

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `string` | Unique scheme id |
| `scheme_name` | `string` | Scheme name |
| `category` | `string` | Inferred category |
| `ministry` | `string` | Ministry name |
| `department` | `string` | Department name |
| `application_link` | `string` | Scheme page/application link |
| `url` | `string` | Scheme URL |
| `description` | `string` | Scheme summary |
| `benefits` | `string` | Benefits text |
| `eligibility` | `string` | Eligibility text |
| `launched` | `string` | Launch date text |
| `state` | `string` | Usually `Rajasthan` |
| `status` | `string` | Scheme status |
| `source` | `string` | Source label |
| `scraped_at` | `string` | ISO timestamp |

## 7. `jjm_scraper.py`

**Website scraped:** `https://ejalshakti.gov.in`

**What data is scraped:**
- Rajasthan district name
- District population display value
- Tap water coverage percentage
- Source information
- Live/fallback status

**Returned data type:** `List[Dict]`

**Fields:**

| Field | Type | Meaning |
| --- | --- | --- |
| `name` | `string` | District name |
| `pop` | `string` | Population display, usually in lakh |
| `coverage` | `float` | Tap water coverage percentage |
| `source` | `string` | Data source label |
| `scraped_at` | `string` | ISO timestamp |
| `live` | `boolean` | Whether data came from live scrape |

## 8. `pmksy_scraper.py`

**Website scraped:** Rajasthan agriculture statistics PDF at `https://rajas.rajasthan.gov.in`

**What data is scraped / derived:**
- District name
- Net area sown
- Net irrigated area
- Irrigation coverage percentage
- State average
- Status label and tone for dashboard
- Source metadata

**Returned data type:** `List[Dict]`

**Fields:**

| Field | Type | Meaning |
| --- | --- | --- |
| `name` | `string` | District name |
| `net_area_sown_ha` | `integer` | Net area sown in hectares |
| `net_area_sown_lakh_ha` | `float` | Net area sown in lakh hectares |
| `net_area_sown_display` | `string` | Formatted net area sown |
| `net_irrigated_area_ha` | `integer` | Net irrigated area in hectares |
| `net_irrigated_area_lakh_ha` | `float` | Net irrigated area in lakh hectares |
| `net_irrigated_area_display` | `string` | Formatted irrigated area |
| `coverage_pct` | `float` | Irrigation coverage percentage |
| `source` | `string` | Source domain |
| `source_title` | `string` | Source publication title |
| `source_url` | `string` | Source PDF URL |
| `report_label` | `string` | Report label |
| `scraped_at` | `string` | ISO timestamp |
| `live` | `boolean` | Whether data was parsed from source successfully |
| `state_average` | `float` | Average district coverage across the dataset |
| `status` | `string` | Dashboard status like `On track` |
| `status_tone` | `string` | Tone like `good`, `watch`, `critical` |

## 9. `sparkline_scraper.py`

**Website scraped:** `https://prsindia.org` and JJM history from `ejalshakti.gov.in`

**What data is scraped:**
- Year-wise health budget
- Education share
- Fiscal deficit percentage
- Capital outlay
- Social security budget
- Total expenditure
- JJM coverage trend

**Returned data type:** `Dict`

**Top-level fields:**

| Field | Type | Meaning |
| --- | --- | --- |
| `sparklines` | `dict[string, list[float \| null]]` | Trend arrays for dashboard cards |
| `years` | `list[string]` | Financial years in order |
| `year_data` | `dict[string, dict]` | Per-year merged figures |
| `live_years` | `integer` | Number of years scraped live |
| `total_years` | `integer` | Total years in trend |
| `scraped_at` | `string` | ISO timestamp |
| `source` | `string` | Source label |
| `note` | `string` | Live/fallback note |

**Inside `sparklines`:**
- `health_cr`: `list[float | null]`
- `education_pct`: `list[float | null]`
- `fiscal_deficit_pct`: `list[float | null]`
- `capital_outlay_cr`: `list[float | null]`
- `social_security_cr`: `list[float | null]`
- `total_expenditure_cr`: `list[float | null]`
- `jjm_coverage_pct`: `list[float | null]`

## 10. `budget_scraper.py`

**Websites scraped:**
- `https://prsindia.org`
- `https://finance.rajasthan.gov.in`
- `https://rajras.in`
- `https://ejalshakti.gov.in`

**What data is scraped / merged:**
- Rajasthan budget headline figures
- Fiscal indicators
- Health, education, and social security values
- JJM state coverage
- District JJM records
- 6-year sparkline trend arrays
- Display-ready formatted values
- Scrape metadata

**Returned data type:** `Dict`

**Main fields:**

| Field | Type | Meaning |
| --- | --- | --- |
| `year` | `string` | Budget year |
| `total_expenditure_cr` | `float \| integer \| null` | Total expenditure in crore |
| `capital_outlay_cr` | `float \| integer \| null` | Capital outlay in crore |
| `fiscal_deficit_cr` | `float \| integer \| null` | Fiscal deficit in crore |
| `fiscal_deficit_pct_gsdp` | `float \| null` | Fiscal deficit as % of GSDP |
| `gsdp_cr` | `float \| integer \| null` | GSDP in crore |
| `health_cr` | `float \| integer \| null` | Health budget |
| `education_cr` | `float \| integer \| null` | Education budget |
| `education_pct` | `float \| null` | Education share |
| `social_security_cr` | `float \| integer \| null` | Social security budget |
| `agriculture_cr` | `float \| integer \| null` | Agriculture allocation |
| `jjm_coverage_pct` | `float \| null` | Rajasthan JJM coverage |
| `economy_target_bn_usd` | `float \| integer \| null` | Target economy size |
| `green_budget` | `boolean` | Whether green budget flag is set |
| `source` | `string` | Source summary |
| `source_url` | `string` | Primary source URL |
| `scraped_at` | `string` | ISO timestamp |
| `health_pct` | `float \| null` | Health share of total expenditure |
| `display` | `dict[string, string]` | Formatted dashboard labels |
| `sparklines` | `dict[string, list[float \| null]]` | Trend arrays |
| `sparkline_meta` | `dict` | Sparkline scrape metadata |
| `jjm_districts` | `list[dict]` | District coverage dataset |
| `jjm_districts_live` | `boolean` | Whether district data is live |
| `scrape_meta` | `dict` | Summary of live sources and fallback usage |

## Notes

- Some scrapers return **basic listing data** while the `*_full_scraper.py` files return **enriched structured datasets**.
- Many fields are optional, so missing values are stored as `null`, empty string, or omitted depending on the scraper.
- Some values are not directly copied from the website and are instead **derived** from scraped text, for example:
  - `category`
  - `progress_pct`
  - `budget`
  - `beneficiaries`
  - `coverage_pct`
  - `state_average`
