/**
 * Rajasthan AI Chief of Staff Dashboard — v3 (Fixed)
 * All 3 bugs fixed:
 * 1. Category pills now use regex matching against real scraped category names
 * 2. "Know More" uses scheme-specific URLs (apply_url > url > source domain)
 * 3. Jan Soochna & MyScheme scrapers fixed with better API endpoints + fallback
 */

import { useState, useEffect, useCallback, useMemo } from "react";
import axios from "axios";
import InsightsEngine from "../InsightsEngine";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  Cell, PieChart, Pie,
} from "recharts";

const API = (process.env.REACT_APP_API_URL || "").replace(/\/$/, "");

const SRC = {
  igod:       { label: "IGOD Directory", icon: "🏛️", color: "#f97316", url: "https://igod.gov.in/sg/RJ/SPMA/organizations" },
  rajras:     { label: "RajRAS",       icon: "📋", color: "#3b82f6", url: "https://rajras.in" },
  jansoochna: { label: "Jan Soochna",  icon: "👁️", color: "#10b981", url: "https://jansoochna.rajasthan.gov.in" },
  myscheme:   { label: "MyScheme",     icon: "🔍", color: "#8b5cf6", url: "https://myscheme.gov.in" },
};
const CAT_ICON = {
  "Health":"🏥","Health & Family Welfare":"🏥","Education":"🎓","Agriculture":"🌾",
  "Agriculture & Farmers":"🌾","Social Welfare":"🛡️","Labour & Employment":"💼",
  "Women & Child":"👩","Business & Finance":"💰","Housing":"🏠","Food Security":"🍽️",
  "Water & Sanitation":"💧","Water & Irrigation":"💧","Energy":"⚡","Digital Services":"💻",
  "Digital & IT":"💻","Rural Development":"🏘️","Industry & Commerce":"🏭",
  "Industry & Investment":"📈","Tourism & Culture":"🎭","Identity & Social Security":"🪪",
  "Mining":"⛏️","Transparency & RTI":"👁️","Civil Registration":"📄",
  "Finance & Accounts":"💳","Recruitment":"📝","Urban Development":"🏙️",
  "General":"📋","General Services":"📋","Revenue":"📄","Revenue & Land":"📄",
  "Transport":"🚌","Environment":"🌿","Law & Order":"⚖️",
};
const PALETTE = ["#ef4444","#3b82f6","#10b981","#f97316","#8b5cf6","#f59e0b",
                 "#06b6d4","#84cc16","#ec4899","#14b8a6","#6366f1","#a855f7",
                 "#f43f5e","#0ea5e9","#22c55e","#e11d48","#0284c7","#059669"];
const THEME = {
  saffron: "#e56a1f",
  saffronDeep: "#b74a15",
  sandstone: "#f7efe2",
  desertRose: "#fff5ef",
  peacock: "#0f7c97",
  peacockSoft: "#e9f8fb",
  indigo: "#3657c8",
  indigoSoft: "#edf2ff",
  emerald: "#0f9f6e",
  emeraldSoft: "#ecfdf5",
  ink: "#102033",
  muted: "#66758a",
  border: "#e6ebf2",
  panel: "rgba(255,255,255,0.84)",
  panelSolid: "#ffffff",
  shadow: "0 20px 48px rgba(15,23,42,0.08)",
};

// ── Pill category definitions — regex matches actual scraped category strings ─
const PILL_CATS = [
  { id:"all",       label:"All",        icon:null,  match:null },
  { id:"health",    label:"Health",     icon:"🏥",  match:/health|medical|ayush/i },
  { id:"education", label:"Education",  icon:"🎓",  match:/education|school|scholarship|student/i },
  { id:"agri",      label:"Agriculture",icon:"🌾",  match:/agri|kisan|farm|crop|horticulture/i },
  { id:"social",    label:"Social",     icon:"🛡️",  match:/social|pension|welfare|widow|disability|palanhar/i },
  { id:"labour",    label:"Employment", icon:"💼",  match:/labour|labor|employment|rozgar|worker|skill/i },
  { id:"women",     label:"Women",      icon:"👩",  match:/women|mahila|girl|beti|child|maternity/i },
  { id:"housing",   label:"Housing",    icon:"🏠",  match:/housing|awas|shelter|urban/i },
  { id:"food",      label:"Food",       icon:"🍽️",  match:/food|ration|rasoi|pds|nutrition/i },
  { id:"water",     label:"Water",      icon:"💧",  match:/water|jal|sanitation|swachh|irrigation/i },
  { id:"energy",    label:"Energy",     icon:"⚡",  match:/energy|solar|electric|vidyut|ujjwala/i },
  { id:"digital",   label:"Digital",    icon:"💻",  match:/digital|it|e-mitra|emitra|technology/i },
];

const timeAgo = iso => {
  if (!iso) return "";
  const s = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (s < 10)   return "just now";
  if (s < 60)   return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s/60)}m ago`;
  return `${Math.floor(s/3600)}h ago`;
};
const palColor = i => PALETTE[i % PALETTE.length];

const formatCrDisplay = (value, { plus = false } = {}) => {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "Unavailable";
  return `₹${n.toLocaleString("en-IN")}${plus ? "+": ""} Cr`;
};

const formatPercentDisplay = (value, suffix = "%") => {
  const n = Number(value);
  if (!Number.isFinite(n)) return "Unavailable";
  return `${n}${suffix}`;
};

const formatPercentFixedDisplay = (value, digits = 2, suffix = "%") => {
  const n = Number(value);
  if (!Number.isFinite(n)) return "Unavailable";
  return `${n.toFixed(digits)}${suffix}`;
};

const formatCompactCount = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return "0";
  return n.toLocaleString("en-IN");
};

const isDesktopViewport = () =>
  typeof window !== "undefined" ? window.innerWidth >= 1024 : true;

// ── safeUrl — never produce a 404 link ───────────────────────────────────────
const DEAD_URLS = [
  "https://jansoochna.rajasthan.gov.in/Scheme",
  "https://jansoochna.rajasthan.gov.in/Scheme/",
  "https://rajras.in", "https://rajras.in/",
  "https://www.myscheme.gov.in", "https://myscheme.gov.in",
];
const safeUrl = (s) => {
  if (!s) return null;
  const u = s.apply_url || s.url || "";
  if (!u || DEAD_URLS.includes(u.replace(/\/$/, ""))) {
    if (s._src === "jansoochna" || (s.source||"").includes("jansoochna"))
      return "https://jansoochna.rajasthan.gov.in/";
    if (s._src === "myscheme" || (s.source||"").includes("myscheme"))
      return "https://www.myscheme.gov.in/search?q=" + encodeURIComponent((s.name||"").slice(0,50));
    if (s._src === "rajras" || (s.source||"").includes("rajras"))
      return "https://rajras.in/ras/pre/rajasthan/adm/schemes/";
    return null;
  }
  return u || null;
};

const getProgressPct = (scheme) => {
  if (typeof scheme?.progress_pct === "number" && Number.isFinite(scheme.progress_pct)) {
    return Math.max(0, Math.min(100, scheme.progress_pct));
  }
  if (typeof scheme?.progress === "string") {
    const m = scheme.progress.match(/(\d+(?:\.\d+)?)\s*%?/);
    if (m) {
      const n = parseFloat(m[1]);
      if (Number.isFinite(n)) return Math.max(0, Math.min(100, n));
    }
  }
  return null;
};

const cleanInlineText = (value) =>
  String(value || "")
    .replace(/\[[^\]]*\]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const firstUsefulSentence = (...values) => {
  for (const value of values) {
    const text = cleanInlineText(
      Array.isArray(value) ? value.join(". ") : value
    );
    if (!text) continue;
    const sentences = text
      .split(/[.?!]\s+/)
      .map((part) => part.trim())
      .filter(Boolean);
    const picked = sentences.find((part) => part.length >= 40) || sentences[0];
    if (picked) {
      return picked.replace(/\s+/g, " ").trim();
    }
  }
  return "";
};

const stripSchemePrefix = (name = "") =>
  cleanInlineText(name).replace(/^\d+\s*[\.)-]\s*/, "");

const capitalizeSentence = (text = "") =>
  text ? text.charAt(0).toUpperCase() + text.slice(1) : "";

const deriveSchemeSummary = (scheme) => {
  const name = stripSchemePrefix(scheme?.name || "This scheme");
  const sourceLabel = scheme?._src_label || SRC[scheme?._src]?.label || "the source portal";
  const category = cleanInlineText(scheme?.category || "public welfare")
    .toLowerCase();

  const whatItIs = firstUsefulSentence(
    scheme?.objective,
    scheme?.description,
    scheme?.benefit,
    scheme?.benefits
  );
  const whyItMatters = firstUsefulSentence(
    scheme?.benefit,
    scheme?.benefits,
    scheme?.eligibility,
    scheme?.description
  );

  const intro = whatItIs
    ? cleanInlineText(whatItIs)
    : `${name} is listed on ${sourceLabel} as a ${category} scheme.`;
  const importance = whyItMatters
    ? cleanInlineText(whyItMatters)
    : `It is relevant for citizens looking for ${category} support through official government channels.`;

  const normalizedIntro = intro.toLowerCase().includes(name.toLowerCase())
    ? intro
    : `${name} is a ${category} scheme on ${sourceLabel}. ${capitalizeSentence(intro)}`;

  const normalizedImportance = importance.toLowerCase().startsWith("it ")
    ? importance
    : `It matters because ${importance.charAt(0).toLowerCase()}${importance.slice(1)}`;

  return `${normalizedIntro} ${normalizedImportance}`.replace(/\s+/g, " ").trim();
};

const parseDisplayNumber = (rawValue) => {
  if (rawValue == null) return null;
  if (typeof rawValue === "number" && Number.isFinite(rawValue)) return rawValue;

  const text = cleanInlineText(rawValue);
  if (!text) return null;

  const amountMatch = text.match(/₹?\s*([\d,]+(?:\.\d+)?)\s*(lakh\s*crore|crore|cr|lakh|lac|l|k)?/i);
  if (!amountMatch) return null;

  const base = parseFloat(amountMatch[1].replace(/,/g, ""));
  if (!Number.isFinite(base)) return null;

  const unit = (amountMatch[2] || "").toLowerCase();
  if (unit.includes("lakh crore")) return base * 1e12;
  if (unit.includes("crore") || unit === "cr") return base * 1e7;
  if (unit === "lakh" || unit === "lac" || unit === "l") return base * 1e5;
  if (unit === "k") return base * 1e3;
  return base;
};

const inferMetricLabel = (matchText) => {
  const text = matchText.toLowerCase();
  if (text.includes("%")) return "Coverage";
  if (text.includes("district")) return "District reach";
  if (text.includes("shop")) return "Ration shops";
  if (text.includes("city")) return "Cities covered";
  if (text.includes("village")) return "Villages covered";
  if (text.includes("hospital")) return "Hospitals";
  if (text.includes("family")) return "Families";
  if (text.includes("student")) return "Students";
  if (text.includes("beneficiar")) return "Beneficiaries";
  if (text.includes("connection")) return "Connections";
  if (text.includes("house")) return "Houses";
  if (text.includes("plate")) return "Meals";
  if (text.includes("day")) return "Duration";
  if (text.includes("crore") || text.includes("lakh") || text.includes("cr") || text.includes("₹")) {
    return "Financial support";
  }
  return "Official figure";
};

const buildSchemeChartData = (scheme) => {
  const metrics = [];
  const seen = new Set();

  const pushMetric = (metric) => {
    if (!metric || !Number.isFinite(metric.value) || metric.value <= 0) return;
    const key = `${metric.label}|${metric.display}`;
    if (seen.has(key)) return;
    seen.add(key);
    metrics.push(metric);
  };

  const progressPct = getProgressPct(scheme);
  if (progressPct != null) {
    pushMetric({
      label: "Implementation",
      value: progressPct,
      display: `${progressPct}%`,
      source: scheme.progress_source || "Official progress field",
      color: "#10b981",
    });
  }

  const beneficiaries = scheme?.beneficiary_count ?? scheme?.beneficiary_display ?? scheme?.beneficiaries ?? null;
  const beneficiaryValue = parseDisplayNumber(beneficiaries);
  if (beneficiaryValue) {
    pushMetric({
      label: "Beneficiaries",
      value: beneficiaryValue,
      display: scheme.beneficiary_display || String(beneficiaries),
      source: "Beneficiary figure from the current source record",
      color: "#3b82f6",
    });
  }

  const budgetValue = parseDisplayNumber(scheme?.budget_amount || scheme?.budget);
  if (budgetValue) {
    pushMetric({
      label: "Budget / Benefit",
      value: budgetValue,
      display: scheme.budget_amount || scheme.budget,
      source: "Budget or benefit amount from the current source record",
      color: "#8b5cf6",
    });
  }

  const districtValue = parseDisplayNumber(scheme?.districts);
  if (districtValue) {
    pushMetric({
      label: "District reach",
      value: districtValue,
      display: scheme.districts,
      source: "District coverage noted in the current source record",
      color: "#f97316",
    });
  }

  const textBlob = cleanInlineText([
    scheme?.description,
    Array.isArray(scheme?.benefits) ? scheme.benefits.join(". ") : scheme?.benefits,
    scheme?.benefit,
    Array.isArray(scheme?.eligibility) ? scheme.eligibility.join(". ") : scheme?.eligibility,
    scheme?.objective,
  ].filter(Boolean).join(". "));

  const textMatches = textBlob.match(
    /(?:₹\s*[\d,]+(?:\.\d+)?\s*(?:lakh\s*crore|crore|cr|lakh|lac)?|\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*(?:%|days?|districts?|shops?|cities?|villages?|hospitals?|families?|students?|beneficiaries?|connections?|houses?|plates?|crore|cr|lakh|lac))\b/gi
  ) || [];

  textMatches.slice(0, 6).forEach((matchText, index) => {
    const value = parseDisplayNumber(matchText);
    if (!value) return;
    pushMetric({
      label: inferMetricLabel(matchText),
      value,
      display: cleanInlineText(matchText),
      source: `Quantified fact mentioned in the official scheme text${index === 0 ? "" : ""}`,
      color: PALETTE[index % PALETTE.length],
    });
  });

  return metrics.slice(0, 4);
};

const normalizeSchemeRecord = (sourceId, item, index = 0) => {
  if (!item || typeof item !== "object") return null;
  const name = item.name || item.scheme_name || item.title || `Scheme ${index + 1}`;
  return {
    ...item,
    id: item.id || `${sourceId}_${index + 1}`,
    name,
    category: item.category || "General",
    benefit: item.benefit || item.benefits || "",
    apply_url: item.apply_url || item.application_link || "",
    _src: item._src || sourceId,
    _src_label: item._src_label || SRC[sourceId]?.label || sourceId,
    _src_url: item._src_url || SRC[sourceId]?.url || "",
  };
};

const normalizePortalRecord = (item, index = 0) => {
  if (!item || typeof item !== "object") return null;
  const url = item.url || item.website_url || "";
  const description = item.description || item.meta_description || item.summary || "";
  return {
    ...item,
    id: item.id || `igod_${index + 1}`,
    name: item.name || item.organization_name || item.portal_title || `Portal ${index + 1}`,
    url,
    website_url: item.website_url || url,
    description,
    summary: item.summary || description,
    status: item.status || "Active",
    category: item.category || "Government Services",
  };
};

const latestItemTimestamp = (items = []) =>
  items
    .map((item) => item?.scraped_at || item?.date || "")
    .filter(Boolean)
    .sort()
    .at(-1) || "";

const buildFallbackAlerts = ({ schemes = [], portals = [], sourceStatus = {}, jjmDistricts = [] }) => {
  const alerts = [];
  const byCategory = (pattern) => schemes.filter((scheme) => pattern.test(String(scheme.category || "")));

  const health = byCategory(/health|medical|ayush|chiranjeevi|dawa|hospital/i);
  if (health.length) {
    alerts.push({
      id: "fallback_alert_health",
      type: "ACTION",
      severity: "Action",
      icon: "🏥",
      title: `${health.length} Health Schemes Active — Rajasthan`,
      date: latestItemTimestamp(health),
      body: `${health.length} health-related schemes are present in the current scrape. Key schemes include ${health.slice(0, 3).map((item) => item.name).join(", ")}${health.length > 3 ? "…" : ""}.`,
      tags: [`🏥 ${health.length} health schemes`, `Sources: ${[...new Set(health.map((item) => item._src_label).filter(Boolean))].join(", ") || "Multiple"}`, "📍 State-wide"],
      source: `Source: ${[...new Set(health.map((item) => item._src_label).filter(Boolean))].join(", ") || "Current scrape"}`,
      borderColor: "#10b981",
    });
  }

  const agri = byCategory(/agri|kisan|farm|crop|horticulture/i);
  if (agri.length) {
    alerts.push({
      id: "fallback_alert_agri",
      type: "INSIGHT",
      severity: "Insight",
      icon: "🌾",
      title: `${agri.length} Agriculture Schemes Found`,
      date: latestItemTimestamp(agri),
      body: `${agri.length} agriculture and farmer-welfare schemes are visible in the current data, including ${agri.slice(0, 3).map((item) => item.name).join(", ")}${agri.length > 3 ? "…" : ""}.`,
      tags: [`🌾 ${agri.length} agri schemes`, "📍 State-wide", "Live scrape fallback"],
      source: `Source: ${[...new Set(agri.map((item) => item._src_label).filter(Boolean))].join(", ") || "Current scrape"}`,
      borderColor: "#3b82f6",
    });
  }

  const social = byCategory(/social|pension|welfare|palanhar/i);
  if (social.length) {
    alerts.push({
      id: "fallback_alert_social",
      type: "ACTION",
      severity: "Action",
      icon: "🛡️",
      title: `${social.length} Social Welfare Schemes Need Review`,
      date: latestItemTimestamp(social),
      body: `${social.length} social-welfare schemes are present in the current scrape. This includes ${social.slice(0, 3).map((item) => item.name).join(", ")}${social.length > 3 ? "…" : ""}.`,
      tags: [`🛡️ ${social.length} welfare schemes`, "📍 Beneficiary-facing schemes", "Live scrape fallback"],
      source: `Source: ${[...new Set(social.map((item) => item._src_label).filter(Boolean))].join(", ") || "Current scrape"}`,
      borderColor: "#8b5cf6",
    });
  }

  if (portals.length) {
    const categories = [...new Set(portals.map((portal) => portal.category).filter(Boolean))].slice(0, 4);
    alerts.push({
      id: "fallback_alert_portals",
      type: "INSIGHT",
      severity: "Insight",
      icon: "🏛️",
      title: `${portals.length} Official Portals — IGOD Rajasthan Directory`,
      date: latestItemTimestamp(portals),
      body: `The current scrape includes ${portals.length} Rajasthan government portals from the IGOD directory. Sample categories: ${categories.join(", ") || "Government Services"}.`,
      tags: [`🏛️ ${portals.length} portals listed`, `${categories.length} categories sampled`, "📍 IGOD directory"],
      source: "Source: igod.gov.in/sg/RJ/SPMA/organizations",
      borderColor: "#f97316",
    });
  }

  const water = byCategory(/water|jal|sanitation|swachh|irrigation/i);
  if (water.length) {
    const liveJjmRows = jjmDistricts.filter((row) => Number.isFinite(Number(row?.coverage)));
    const lowCoverage = [...liveJjmRows]
      .sort((a, b) => Number(a.coverage) - Number(b.coverage))
      .slice(0, 2)
      .map((row) => `${row.name} (${Number(row.coverage).toFixed(1)}%)`)
      .join(", ");
    alerts.push({
      id: "fallback_alert_water",
      type: "CRITICAL",
      severity: "Critical",
      icon: "🚨",
      title: `Water And Sanitation Watch — ${water.length} Schemes Tracked`,
      date: latestItemTimestamp(water),
      body: lowCoverage
        ? `${water.length} water and sanitation schemes are in the current scrape. Lowest JJM coverage visible right now: ${lowCoverage}.`
        : `${water.length} water and sanitation schemes are in the current scrape. JJM district coverage is not available in this fallback response yet.`,
      tags: [`💧 ${water.length} water schemes`, liveJjmRows.length ? `${liveJjmRows.length} JJM rows` : "JJM rows pending", "📍 Coverage monitoring"],
      source: `Source: ${[...new Set(water.map((item) => item._src_label).filter(Boolean))].join(", ") || "Current scrape"}`,
      borderColor: "#ef4444",
    });
  }

  Object.entries(sourceStatus).forEach(([sid, entry]) => {
    if (entry?.status !== "error") return;
    alerts.push({
      id: `fallback_alert_err_${sid}`,
      type: "WARNING",
      severity: "Warning",
      icon: "⚠️",
      title: `Scrape Warning — ${sid.toUpperCase()} fetch failed`,
      date: entry.scraped_at || "",
      body: `Live scrape of ${sid} failed, so the dashboard is using whatever cached or partial records are still available.`,
      tags: [`⚠️ ${sid} offline`, `Cached rows: ${entry.count || 0}`, "📍 Retry scraper"],
      source: "Source: Dashboard system monitor",
      borderColor: "#f97316",
    });
  });

  return alerts;
};

const buildFallbackAggregate = ({ sourceStatus = {}, rajras = [], jansoochna = [], myscheme = [], igod = [] }) => {
  const schemes = [
    ...rajras.map((item, index) => normalizeSchemeRecord("rajras", item, index)).filter(Boolean),
    ...jansoochna.map((item, index) => normalizeSchemeRecord("jansoochna", item, index)).filter(Boolean),
    ...myscheme.map((item, index) => normalizeSchemeRecord("myscheme", item, index)).filter(Boolean),
  ];
  const portals = igod.map((item, index) => normalizePortalRecord(item, index)).filter(Boolean);

  if (schemes.length === 0 && portals.length === 0) return null;

  const categoryMap = new Map();
  schemes.forEach((scheme) => {
    const category = scheme.category || "General";
    const existing = categoryMap.get(category) || { name: category, count: 0, sources: new Set() };
    existing.count += 1;
    existing.sources.add(scheme._src_label || scheme._src || "Unknown");
    categoryMap.set(category, existing);
  });

  const categories = Array.from(categoryMap.values())
    .map((entry) => ({ ...entry, sources: Array.from(entry.sources) }))
    .sort((a, b) => b.count - a.count);

  const latestScrapedAt = [
    ...Object.values(sourceStatus).map((entry) => entry?.scraped_at).filter(Boolean),
    ...schemes.map((scheme) => scheme.scraped_at).filter(Boolean),
    ...portals.map((portal) => portal.scraped_at).filter(Boolean),
  ].sort().at(-1);
  const alerts = buildFallbackAlerts({ schemes, portals, sourceStatus });

  return {
    scraped_at: latestScrapedAt || new Date().toISOString(),
    kpis: {
      total_schemes: schemes.length,
      total_portals: portals.length,
      unique_categories: categories.length,
      sources_live: Object.values(sourceStatus).filter((entry) => entry?.status === "ok").length,
      rajras_count: rajras.length,
      jansoochna_count: jansoochna.length,
      myscheme_count: myscheme.length,
      igod_count: igod.length,
    },
    schemes,
    portals,
    categories,
    source_counts: [
      { source: "RajRAS", count: rajras.length, color: "#3b82f6" },
      { source: "Jan Soochna", count: jansoochna.length, color: "#10b981" },
      { source: "MyScheme", count: myscheme.length, color: "#8b5cf6" },
      { source: "IGOD Directory", count: igod.length, color: "#f97316" },
    ],
    alerts,
    source_status: sourceStatus,
    jjm_districts: [],
  };
};

// ── InfoTip — ℹ️ hover tooltip ────────────────────────────────────────────────
function InfoTip({ text }) {
  const [show, setShow] = useState(false);
  return (
    <span style={{ position:"relative", display:"inline-flex", alignItems:"center", marginLeft:4 }}>
      <span
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
        onFocus={() => setShow(true)}
        onBlur={() => setShow(false)}
        style={{
          width:16, height:16, borderRadius:"50%",
          background: show ? "#3b82f6" : "#e2e8f0",
          color: show ? "white" : "#6b7280",
          fontSize:10, fontWeight:800, lineHeight:"16px", textAlign:"center",
          display:"inline-block", cursor:"help", userSelect:"none",
          transition:"background 0.15s", flexShrink:0,
        }}
        tabIndex={0}
        aria-label={text}
      >i</span>
      {show && (
        <div style={{
          position:"absolute", left:"50%", bottom:"calc(100% + 10px)", transform:"translateX(-50%)",
          background:"#1e293b", color:"white", borderRadius:9,
          padding:"10px 13px", fontSize:12, lineHeight:1.55,
          width:260, maxWidth:"min(260px, 72vw)", zIndex:9999, pointerEvents:"none",
          boxShadow:"0 8px 24px rgba(0,0,0,0.25)",
          whiteSpace:"normal",
          overflowWrap:"break-word",
        }}>
          {text}
          <div style={{
            position:"absolute", left:"50%", top:"100%", transform:"translateX(-50%)",
            width:0, height:0,
            borderLeft:"6px solid transparent",
            borderRight:"6px solid transparent",
            borderTop:"6px solid #1e293b",
          }}/>
        </div>
      )}
    </span>
  );
}


// ── BUG FIX 2: Correct URL resolver — scheme-specific, not generic domain ────
const resolveSchemeUrl = (scheme) => {
  // Priority: apply_url (direct apply page) > url (scheme detail page) > source domain
  const u = scheme.apply_url || scheme.url;
  if (u && u.startsWith("http") && !isBaseUrl(u)) return u;
  // Construct URL from source
  const src = scheme._src;
  if (src === "rajras" && scheme.url && scheme.url.includes("rajras.in/")) return scheme.url;
  if (src === "myscheme" && scheme.url && scheme.url.includes("/schemes/")) return scheme.url;
  if (src === "jansoochna" && scheme.url && scheme.url.includes("jansoochna")) return scheme.url;
  // Fall back to source home only as last resort
  return SRC[src]?.url || "#";
};
const isBaseUrl = (url) => {
  try {
    const u = new URL(url);
    return u.pathname === "/" || u.pathname === "";
  } catch { return false; }
};

function StatusDot({ status, animating }) {
  const c = { ok:"#10b981", error:"#ef4444", loading:"#f59e0b", not_scraped:"#d1d5db" }[status] || "#d1d5db";
  return (
    <span style={{ display:"inline-block", width:8, height:8, borderRadius:"50%",
      background:c, flexShrink:0, animation:animating?"pulse 1s infinite":"none" }}/>
  );
}
function Chip({ label, color="#6b7280", small }) {
  return (
    <span style={{ background:`${color}18`, color, border:`1px solid ${color}28`,
      borderRadius:20, padding:small?"2px 8px":"4px 12px",
      fontSize:small?10:12, fontWeight:600, whiteSpace:"nowrap" }}>
      {label}
    </span>
  );
}
function ScrapeNowButton({ onClick, loading, disabled }) {
  return (
    <button onClick={onClick} disabled={loading||disabled} style={{
      background:(loading||disabled)?"#e5e7eb":"#f97316",
      color:(loading||disabled)?"#9ca3af":"white",
      borderRadius:10, padding:"10px 22px", fontWeight:800, fontSize:13,
      display:"flex", alignItems:"center", gap:8,
      boxShadow:(!loading&&!disabled)?"0 2px 12px #f9731640":"none",
    }}>
      <span style={{ fontSize:16, display:"inline-block",
        animation:loading?"spin 1s linear infinite":"none" }}>⚡</span>
      {loading?"Refreshing…":"Refresh"}
    </button>
  );
}
function EmptyState({ onScrape }) {
  return (
    <div style={{ background:"white", borderRadius:16, border:"2px dashed #e5e7eb",
      padding:60, textAlign:"center" }}>
      <div style={{ fontSize:52, marginBottom:14 }}>⚡</div>
      <div style={{ fontWeight:800, fontSize:20, color:"#0f172a", marginBottom:8 }}>No live data yet</div>
      <div style={{ color:"#64748b", marginBottom:24, fontSize:14 }}>
        Click <strong>Scrape Now</strong> to pull real data from all 4 government websites
      </div>
      <button onClick={onScrape} style={{ background:"#f97316", color:"white",
        borderRadius:12, padding:"13px 32px", fontWeight:800, fontSize:15,
        border:"none", cursor:"pointer", boxShadow:"0 4px 20px #f9731650" }}>
        ⚡ Scrape All 4 Sources
      </button>
    </div>
  );
}

// ── Dashboard Tab ─────────────────────────────────────────────────────────────
function DashboardTab({ agg, srcStatus, onScrapeAll, onScrapeOne, scraping, budget, budgetLoading }) {
  if (!agg) return <EmptyState onScrape={onScrapeAll}/>;
  const { kpis, schemes } = agg;

  const Spark = ({ data=[], color="#f97316", years=[], formatValue=(v)=>String(v) }) => {
    const cleanData = data.filter((value) => Number.isFinite(Number(value)));
    const [activeIndex, setActiveIndex] = useState(Math.max(0, data.length - 1));
    if (!data||data.length<2) return <div style={{ width:100, height:44, background:`${color}08`, borderRadius:6 }}/>;
    const W=100, H=44, PAD=4;
    const min=Math.min(...data), max=Math.max(...data), rng=(max-min)||1;
    const xs=data.map((_,i)=>(i/(data.length-1))*W);
    const ys=data.map(v=>H-PAD-((v-min)/rng)*(H-PAD*2));
    const lp=xs.map((x,i)=>`${x},${ys[i]}`).join(" ");
    const ap=`0,${H} `+lp+` ${W},${H}`;
    const gid=`g${color.replace(/#/g,"")}`;
    const currentIndex = Math.min(activeIndex, data.length - 1);
    const activeYear = years[currentIndex] || `Point ${currentIndex + 1}`;
    const activeValue = Number.isFinite(Number(data[currentIndex])) ? formatValue(Number(data[currentIndex])) : "Unavailable";
    const setNearestIndex = (clientX, rect) => {
      const relativeX = Math.max(0, Math.min(W, ((clientX - rect.left) / rect.width) * W));
      const nearest = xs.reduce((bestIndex, x, index) =>
        Math.abs(x - relativeX) < Math.abs(xs[bestIndex] - relativeX) ? index : bestIndex, 0);
      setActiveIndex(nearest);
    };
    return (
      <div style={{ display:"flex", flexDirection:"column", alignItems:"flex-end", gap:4 }}>
        <svg
          width={W}
          height={H}
          viewBox={`0 0 ${W} ${H}`}
          style={{ overflow:"visible", display:"block", touchAction:"none" }}
          onMouseMove={(event) => setNearestIndex(event.clientX, event.currentTarget.getBoundingClientRect())}
          onTouchStart={(event) => setNearestIndex(event.touches[0].clientX, event.currentTarget.getBoundingClientRect())}
          onTouchMove={(event) => setNearestIndex(event.touches[0].clientX, event.currentTarget.getBoundingClientRect())}
        >
          <defs>
            <linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity="0.22"/>
              <stop offset="80%" stopColor={color} stopOpacity="0.04"/>
              <stop offset="100%" stopColor={color} stopOpacity="0"/>
            </linearGradient>
          </defs>
          <polygon points={ap} fill={`url(#${gid})`}/>
          <polyline points={lp} fill="none" stroke={color} strokeWidth="2.2" strokeLinejoin="round" strokeLinecap="round"/>
          <line x1={xs[currentIndex]} x2={xs[currentIndex]} y1="0" y2={H} stroke={color} strokeOpacity="0.18" strokeDasharray="2 2" />
          {xs.map((x, index) => (
            <circle
              key={index}
              cx={x}
              cy={ys[index]}
              r={index === currentIndex ? "3.8" : "2.2"}
              fill={index === currentIndex ? color : "#ffffff"}
              stroke={color}
              strokeWidth={index === currentIndex ? "1.8" : "1.2"}
            />
          ))}
        </svg>
        <div style={{
          background:`${color}10`,
          color:color,
          border:`1px solid ${color}25`,
          borderRadius:8,
          padding:"3px 7px",
          fontSize:10,
          fontWeight:800,
          whiteSpace:"nowrap"
        }}>
          {activeYear} · {activeValue}
        </div>
      </div>
    );
  };

  const b=budget||{}, d=b.display||{}, sp=b.sparklines||{}, bm=b.scrape_meta||{};
  const sparkMeta = b.sparkline_meta || {};
  const sparkYears = Array.isArray(sparkMeta.years) ? sparkMeta.years : [];
  const sparkYearsLabel = sparkYears.length ? `${sparkYears[0]} to ${sparkYears[sparkYears.length - 1]}` : "Historical trend";
  const sparkLiveYears = Number.isFinite(Number(sparkMeta.live_years)) ? Number(sparkMeta.live_years) : 0;
  const sparkTotalYears = Number.isFinite(Number(sparkMeta.total_years)) ? Number(sparkMeta.total_years) : 0;
  const sparkStatusLabel = sparkLiveYears === 0
    ? "Verified fallback history"
    : sparkLiveYears === sparkTotalYears
      ? "Official historical trend"
      : `Mixed trend: ${sparkLiveYears}/${sparkTotalYears} years live`;
  const jjmDistrictCount = Array.isArray(b.jjm_districts) ? b.jjm_districts.length : 0;
  const CARDS = [
    {
      label:"HEALTH BUDGET",
      value: d.health || formatCrDisplay(b.health_cr),
      sub: b.health_pct ? `${formatPercentDisplay(b.health_pct)} of total budget` : "Share not published in current response",
      color:"#ef4444",
      spark: sp.health_cr || [],
      icon:"🏥",
      trendSource: sparkStatusLabel,
      sparkFormatter: (value) => formatCrDisplay(value)
    },
    {
      label:"EDUCATION ALLOCATION",
      value: d.education_pct || (Number.isFinite(Number(b.education_pct)) ? `${b.education_pct}% share` : "Unavailable"),
      sub: Number.isFinite(Number(b.education_cr)) ? formatCrDisplay(b.education_cr) : "Allocation amount not exposed separately",
      color:"#3b82f6",
      spark: sp.education_pct || [],
      icon:"🎓",
      trendSource: sparkStatusLabel,
      sparkFormatter: (value) => `${value.toFixed(1)}%`
    },
    {
      label:"JJM COVERAGE RAJASTHAN",
      value: d.jjm_coverage || formatPercentFixedDisplay(b.jjm_coverage_pct, 2),
      sub: jjmDistrictCount ? `${formatCompactCount(jjmDistrictCount)} district rows from JJM` : "State-level coverage only",
      color:"#0ea5e9",
      spark: sp.jjm_coverage_pct || [],
      icon:"💧",
      trendSource: sparkLiveYears ? `Live JJM current year + ${sparkYearsLabel}` : sparkStatusLabel,
      sparkFormatter: (value) => `${value.toFixed(2)}%`
    },
    {
      label:"FISCAL DEFICIT",
      value: d.fiscal_deficit_pct || (Number.isFinite(Number(b.fiscal_deficit_pct_gsdp)) ? `${b.fiscal_deficit_pct_gsdp}% GSDP` : "Unavailable"),
      sub: Number.isFinite(Number(b.fiscal_deficit_cr)) ? `${formatCrDisplay(b.fiscal_deficit_cr)} estimate` : "Absolute deficit amount unavailable",
      color:"#f97316",
      spark: sp.fiscal_deficit_pct || [],
      icon:"📊",
      trendSource: sparkStatusLabel,
      sparkFormatter: (value) => `${value.toFixed(2)}%`
    },
    {
      label:"CAPITAL OUTLAY",
      value: d.capital_outlay || formatCrDisplay(b.capital_outlay_cr),
      sub: Number.isFinite(Number(b.total_expenditure_cr)) && Number.isFinite(Number(b.capital_outlay_cr))
        ? `${((b.capital_outlay_cr / b.total_expenditure_cr) * 100).toFixed(1)}% of total expenditure`
        : "Capex share unavailable",
      color:"#10b981",
      spark: sp.capital_outlay_cr || [],
      icon:"🏗️",
      trendSource: sparkStatusLabel,
      sparkFormatter: (value) => formatCrDisplay(value)
    },
    {
      label:"SOCIAL SECURITY",
      value: d.social_security || formatCrDisplay(b.social_security_cr, { plus: true }),
      sub: Number.isFinite(Number(b.social_security_cr)) ? "Official welfare allocation" : "Welfare allocation unavailable",
      color:"#8b5cf6",
      spark: sp.social_security_cr || [],
      icon:"🛡️",
      trendSource: sparkStatusLabel,
      sparkFormatter: (value) => formatCrDisplay(value)
    },
  ];

  return (
    <div className="fadeup">
      <div style={{
        background:`linear-gradient(135deg, ${THEME.desertRose} 0%, #fffdf8 42%, ${THEME.peacockSoft} 100%)`,
        border:`1px solid ${THEME.border}`,
        borderRadius:30,
        padding:"clamp(18px, 3vw, 26px)",
        marginBottom:22,
        boxShadow:THEME.shadow,
        position:"relative",
        overflow:"hidden",
      }}>
        <div style={{
          position:"absolute",
          inset:"auto auto -42px -32px",
          width:180,
          height:180,
          borderRadius:"50%",
          background:"radial-gradient(circle, rgba(54,87,200,0.16), rgba(54,87,200,0))",
          pointerEvents:"none",
        }}/>
        <div style={{
          position:"absolute",
          right:-28,
          top:-36,
          width:160,
          height:160,
          borderRadius:"50%",
          background:"radial-gradient(circle, rgba(229,106,31,0.18), rgba(229,106,31,0))",
          pointerEvents:"none",
        }}/>
        <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:10, flexWrap:"wrap", position:"relative" }}>
          <span style={{
            display:"inline-flex", alignItems:"center", gap:8,
            padding:"7px 14px", borderRadius:999,
            background:"#fff1df", border:"1px solid #ffd2aa",
            color:THEME.saffronDeep, fontSize:12, fontWeight:800, letterSpacing:"0.04em",
          }}>
            Rajasthan Executive Dashboard
          </span>
          <span style={{
            display:"inline-flex", alignItems:"center",
            padding:"7px 14px", borderRadius:999,
            background:"rgba(255,255,255,0.7)", border:`1px solid ${THEME.border}`,
            color:THEME.peacock, fontSize:12, fontWeight:800,
          }}>
            Saffron · Peacock · Sandstone theme
          </span>
          <InfoTip text="KPIs and charts are built from live-scraped data. Every number comes from the /aggregate API which merges all 4 scrapers. Use ⚡ Scrape Now to refresh."/>
        </div>
        <div style={{ display:"flex", justifyContent:"space-between", gap:20, alignItems:"flex-start", flexWrap:"wrap", position:"relative" }}>
          <div style={{ flex:"1 1 420px", minWidth:0, maxWidth:760 }}>
            <h1 style={{ fontSize:"clamp(26px, 4vw, 34px)", fontWeight:900, color:THEME.ink, margin:"0 0 8px", letterSpacing:"-0.05em", lineHeight:1.04 }}>
              Rajasthan Governance Snapshot for <span style={{ color:THEME.saffron }}>Mukhyamantri Ji</span>
            </h1>
            <p style={{ color:THEME.muted, fontSize:14, margin:"0 0 14px", lineHeight:1.7 }}>
              A live, source-linked overview of schemes, portals, budget signals, district coverage, and alerting across Rajasthan government datasets.
            </p>
            <div style={{ display:"flex", gap:10, flexWrap:"wrap" }}>
              <span style={{
                display:"inline-flex", alignItems:"center", gap:8,
                background:"rgba(255,255,255,0.82)", border:`1px solid ${THEME.border}`,
                borderRadius:999, padding:"8px 14px", fontSize:12.5, fontWeight:700, color:"#334155"
              }}>
                <span style={{ width:8, height:8, borderRadius:"50%", background:"#10b981", boxShadow:"0 0 0 3px #d1fae5", display:"inline-block" }}/>
                Verified public-source aggregation
              </span>
              <span style={{
                display:"inline-flex", alignItems:"center",
                background:THEME.indigoSoft, border:"1px solid #cad6ff",
                borderRadius:999, padding:"8px 14px", fontSize:12.5, fontWeight:700, color:THEME.indigo
              }}>
                {b.source || "Live government source data"}
              </span>
            </div>
          </div>
          <div style={{
            flex:"1 1 260px",
            minWidth:0,
            background:"linear-gradient(180deg, rgba(255,255,255,0.88), rgba(255,255,255,0.72))",
            border:`1px solid ${THEME.border}`,
            borderRadius:22,
            padding:"16px 18px",
            boxShadow:"0 14px 30px rgba(148,163,184,0.14)",
            backdropFilter:"blur(8px)",
          }}>
            <div style={{ fontSize:11, fontWeight:800, letterSpacing:"0.08em", color:"#94a3b8", textTransform:"uppercase", marginBottom:8 }}>
              Presentation Note
            </div>
            <div style={{ fontSize:14, fontWeight:800, color:THEME.ink, marginBottom:6 }}>
              Real-time briefing surface
            </div>
            <div style={{ fontSize:12.5, color:THEME.muted, lineHeight:1.65 }}>
              Figures shown below come from current backend aggregation and linked public portals, with fallbacks clearly marked where live source coverage is incomplete.
            </div>
          </div>
        </div>
      </div>

      {/* ── Live Data Summary Banner ── */}
      <div style={{
        background:"linear-gradient(135deg,#fff5e7 0%,#fff8ef 42%,#f7fbff 100%)",
        border:"1px solid #f3dbc0", borderRadius:22,
        padding:"18px 20px", marginBottom:22,
        boxShadow:"0 12px 34px rgba(15,23,42,0.05)",
      }}>
        <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:10 }}>
          <span style={{ fontSize:15 }}>📊</span>
          <span style={{ fontWeight:800, fontSize:14, color:"#1a1a2e" }}>Live Data Summary</span>
          <InfoTip text="Every number here is computed live from the current scrape. Hover any ℹ️ icon to see exactly where the data comes from."/>
        </div>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(140px, 1fr))", gap:10 }}>
          {[
            { icon:"📋", val:kpis.total_schemes,        label:"schemes scraped",  color:"#f97316", bg:"#fff7ed",
              tip:"Total scheme records from RajRAS (HTML scrape) + Jan Soochna (JSON API) + MyScheme (REST API)." },
            { icon:"🏛️", val:kpis.total_portals,        label:"IGOD portals",     color:"#3b82f6", bg:"#eff6ff",
              tip:"Official Rajasthan government portals discovered from the IGOD directory and lightly enriched from each portal homepage." },
            { icon:"🗂️", val:kpis.unique_categories,    label:"categories",       color:"#10b981", bg:"#f0fdf4",
              tip:"Unique scheme categories found. Derived by keyword matching on scheme names — not from any API field directly." },
            { icon:"✅", val:`${kpis.sources_live}/4`,  label:"sources online",   color:"#8b5cf6", bg:"#faf5ff",
              tip:"Live scrapers out of 4 total (IGOD, RajRAS, Jan Soochna, MyScheme). 4/4 = all portals responded." },
          ].map((item, i) => (
            <div key={i} style={{ background:item.bg, border:`1px solid ${item.color}25`, borderRadius:16, padding:"13px 14px", boxShadow:"inset 0 1px 0 rgba(255,255,255,0.7)" }}>
              <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:4 }}>
                <span style={{ fontSize:18 }}>{item.icon}</span>
                <InfoTip text={item.tip}/>
              </div>
              <div style={{ fontSize:22, fontWeight:900, color:item.color, lineHeight:1 }}>{item.val}</div>
              <div style={{ fontSize:10, color:"#6b7280", marginTop:3 }}>{item.label}</div>
            </div>
          ))}
        </div>
      </div>

      <div style={{ background:"linear-gradient(135deg,#eaf4ff 0%,#f4faff 100%)",
        border:"1px solid #c9dffb", borderRadius:20, padding:"15px 18px", marginBottom:26, lineHeight:1.8,
        boxShadow:"0 10px 26px rgba(59,130,246,0.08)" }}>
        <div style={{ fontSize:13 }}>
          <span style={{ fontWeight:800, color:"#1d4ed8" }}>Budget 2025-26: </span>
          <span style={{ color:"#1e3a5f" }}>
            Revenue expenditure {d.total_expenditure || formatCrDisplay(b.total_expenditure_cr)}
            {" · "}Fiscal deficit {d.fiscal_deficit_pct || (Number.isFinite(Number(b.fiscal_deficit_pct_gsdp)) ? `${b.fiscal_deficit_pct_gsdp}% GSDP` : "Unavailable")}
          </span>
        </div>
        <div style={{ fontSize:12, color:"#4b7ab5", display:"flex", flexWrap:"wrap", alignItems:"center", gap:6 }}>
          <span>
            {Number.isFinite(Number(b.economy_target_bn_usd))
              ? `Target: $${b.economy_target_bn_usd} Bn economy by 2030`
              : "Long-term economy target unavailable"}
            {b.green_budget!==false ? " · Green budget framing" : ""}
          </span>
          {bm.note&&(
            <span style={{ background:"#dbeafe", color:"#1d4ed8",
              borderRadius:4, padding:"1px 7px", fontSize:11, fontWeight:600 }}>
              {bm.live_sources>0?`${bm.live_sources} budget sources live`:"Verified fallback"}
            </span>
          )}
          {bm.sparkline_live_years>0&&(
            <span style={{ background:"#d1fae5", color:"#065f46",
              borderRadius:4, padding:"1px 7px", fontSize:11, fontWeight:600 }}>
              📈 {bm.sparkline_live_years}/{b.sparkline_meta?.total_years||6} sparkline years live
            </span>
          )}
        </div>
      </div>

      <div style={{
        display:"grid",
        gridTemplateColumns:isDesktopViewport()
          ? "repeat(3, minmax(0, 1fr))"
          : "repeat(auto-fit, minmax(220px, 1fr))",
        gap:14,
        marginBottom:24
      }}>
        {(budgetLoading?Array(6).fill(null):CARDS).map((card,i)=>(
          <div key={i} style={{ background:"linear-gradient(180deg,#ffffff 0%,#fcfdff 100%)", borderRadius:22, border:"1px solid #e8edf3",
            boxShadow:"0 14px 34px rgba(15,23,42,0.07)", padding:"18px 20px 16px", display:"flex", flexDirection:"column", position:"relative", overflow:"visible" }}>
            {!budgetLoading && card && (
              <div style={{
                position:"absolute", inset:"0 0 auto 0", height:3,
                background:`linear-gradient(90deg, ${card.color}, ${card.color}88)`,
              }}/>
            )}
            {budgetLoading||!card?(
              <div style={{ display:"flex", flexDirection:"column", gap:10 }}>
                <div style={{ display:"flex", justifyContent:"space-between" }}>
                  <div style={{ width:28, height:28, borderRadius:8, background:"#f3f4f6" }}/>
                  <div style={{ width:100, height:44, borderRadius:6, background:"#f3f4f6" }}/>
                </div>
                <div style={{ width:"60%", height:10, borderRadius:4, background:"#f3f4f6" }}/>
                <div style={{ width:"80%", height:28, borderRadius:6, background:"#f3f4f6" }}/>
                <div style={{ width:"50%", height:10, borderRadius:4, background:"#f3f4f6" }}/>
              </div>
            ):(
              <>
                <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", marginBottom:10 }}>
                  <div style={{ display:"flex", alignItems:"center", gap:6 }}>
                    <span style={{ fontSize:20, lineHeight:1 }}>{card.icon}</span>
                    <InfoTip text={`${card.label}: ${sparkYearsLabel}. ${card.trendSource}. Source: ${sparkMeta.source || "Budget dataset"}.`} />
                  </div>
                  <Spark data={card.spark} color={card.color} years={sparkYears} formatValue={card.sparkFormatter}/>
                </div>
                <div style={{ fontSize:10, fontWeight:700, color:"#9ca3af", letterSpacing:"0.08em", marginBottom:7, textTransform:"uppercase" }}>{card.label}</div>
                <div style={{ fontSize:card.value.length>12?22:27, fontWeight:900, color:card.color, letterSpacing:"-0.5px", lineHeight:1.1, marginBottom:6 }}>{card.value}</div>
                <div style={{ fontSize:11, color:"#9ca3af", marginTop:"auto" }}>{card.sub}</div>
                <div style={{ marginTop:6, display:"inline-flex", alignItems:"center", gap:6, fontSize:10, fontWeight:700, color:"#64748b" }}>
                  <span style={{
                    width:8, height:8, borderRadius:"50%",
                    background:sparkLiveYears === 0 ? "#94a3b8" : sparkLiveYears === sparkTotalYears ? "#16a34a" : "#f59e0b",
                    display:"inline-block"
                  }}/>
                  <span>{card.trendSource}</span>
                </div>
              </>
            )}
          </div>
        ))}
      </div>

      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(220px, 1fr))", gap:12, marginBottom:22 }}>
        {[
          {sid:"rajras",count:kpis.rajras_count},
          {sid:"jansoochna",count:kpis.jansoochna_count},
          {sid:"myscheme",count:kpis.myscheme_count},
          {sid:"igod",count:kpis.igod_count},
        ].map(({sid,count})=>{
          const s=SRC[sid]; const st=srcStatus[sid]||{}; const loading=scraping[sid];
          return (
            <div key={sid} style={{ background:"linear-gradient(180deg,#ffffff 0%,#fbfdff 100%)", borderRadius:18,
              border:`1px solid ${st.status==="ok"?s.color+"30":"#e5e7eb"}`, padding:"14px 15px",
              boxShadow:"0 10px 24px rgba(15,23,42,0.05)" }}>
              <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:5 }}>
                <div style={{ display:"flex", alignItems:"center", gap:6 }}>
                  <span>{s.icon}</span>
                  <span style={{ fontWeight:700, fontSize:12, color:"#374151" }}>{s.label}</span>
                </div>
                <button onClick={()=>onScrapeOne(sid)} disabled={loading}
                  style={{ background:loading?"#f3f4f6":`${s.color}12`, color:loading?"#9ca3af":s.color,
                    border:`1px solid ${loading?"#e5e7eb":s.color+"30"}`, borderRadius:6, padding:"3px 8px", fontSize:11, fontWeight:700 }}>
                  {loading?"⟳":"↺"}
                </button>
              </div>
              <div style={{ fontSize:24, fontWeight:900, color:s.color, lineHeight:1 }}>{count??0}</div>
              <div style={{ display:"flex", alignItems:"center", gap:5, marginTop:5 }}>
                <StatusDot status={loading?"loading":st.status} animating={!!loading}/>
                <span style={{ fontSize:11, color:"#9ca3af" }}>
                  {loading?"scraping…":st.status==="ok"?`live · ${timeAgo(st.scraped_at)}`:"pending"}
                </span>
              </div>
            </div>
          );
        })}
      </div>

      {(schemes||[]).length>0&&(
        <div style={{ background:"linear-gradient(180deg,#ffffff 0%,#fbfcfe 100%)", borderRadius:22, border:"1px solid #e5e7eb", padding:20, boxShadow:"0 12px 30px rgba(15,23,42,0.05)" }}>
          <div style={{ fontWeight:800, fontSize:14, marginBottom:14 }}>
            Recently Scraped Schemes
            <span style={{ color:"#9ca3af", fontWeight:400, fontSize:12, marginLeft:8 }}>
              {Math.min(10,(schemes||[]).length)} of {(schemes||[]).length}
            </span>
          </div>
          <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(240px, 1fr))", gap:8 }}>
            {(schemes||[]).slice(0,10).map((s,i)=>{
              const src=SRC[s._src]||SRC.myscheme;
              return (
                <div key={i} style={{ display:"flex", gap:10, padding:"10px 12px",
                  background:"#fafafa", borderRadius:10, border:"1px solid #f3f4f6", alignItems:"flex-start" }}>
                  <span style={{ fontSize:18, flexShrink:0 }}>{CAT_ICON[s.category]||"📋"}</span>
                  <div style={{ flex:1, minWidth:0 }}>
                    <div style={{ fontWeight:600, fontSize:13, color:"#1f2937",
                      overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{s.name}</div>
                    {s.benefit&&(<div style={{ fontSize:11, color:"#10b981", fontWeight:600, marginTop:2,
                      overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{s.benefit}</div>)}
                    <div style={{ display:"flex", gap:5, marginTop:4, flexWrap:"wrap" }}>
                      <Chip label={s.category||"General"} color={palColor(i)} small/>
                      <Chip label={src.label} color={src.color} small/>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Scheme Detail Panel ─────────────────────────────────────────────────────
function SchemeDetailPanel({ scheme, onClose }) {
  if (!scheme) return null;
  const srcMeta = SRC[scheme._src] || SRC.myscheme;

  // Resolve best URL for "Know More":
  // For RajRAS: url field is the full article page like rajras.in/ras/.../pm-kisan/
  // For MyScheme: url field is myscheme.gov.in/schemes/<slug>
  // For Jan Soochna: url field is jansoochna.rajasthan.gov.in/Scheme
  // Never fall back to bare domain — show the actual page
  const BASE_DOMAINS = [
    "https://rajras.in", "https://rajras.in/",
    "https://www.myscheme.gov.in", "https://myscheme.gov.in",
    "https://jansoochna.rajasthan.gov.in",
  ];
  const schemeUrl = scheme.url && !BASE_DOMAINS.includes(scheme.url.replace(/\/$/, ""))
    ? scheme.url : null;

  const sourceUrl =
    scheme.apply_url ||
    schemeUrl ||
    (scheme._src === "jansoochna"
      ? `https://jansoochna.rajasthan.gov.in/Scheme`
      : srcMeta.url);

  const beneficiaries = scheme.beneficiary_display
    || scheme.beneficiaries
    || (scheme.beneficiary_count ? String(scheme.beneficiary_count) : null)
    || null;
  const budget = scheme.budget_amount || scheme.budget || null;
  const launchYear = scheme.launch_year
    || (scheme.launched
      ? String(scheme.launched).match(/\d{4}/)?.[0] || scheme.launched
      : null);
  const districts = scheme.districts || null;
  const progressPct = getProgressPct(scheme);
  const progressLabel = progressPct != null ? `${progressPct}%` : "N/A";
  const progressColor = progressPct != null ? srcMeta.color : "#9ca3af";
  const schemeSummary = deriveSchemeSummary(scheme);
  const chartData = buildSchemeChartData(scheme);
  const factCards = [
    { label:"Beneficiaries", value:beneficiaries, color:srcMeta.color, borderRight:true },
    { label:"Budget (2025-26)", value:budget, color:"#111827" },
    { label:"Launch Year", value:launchYear, color:"#111827", borderRight:true },
    { label:"Districts Insights", value:districts, color:"#111827" },
  ].filter(card => card.value);

  const keyFacts = [
    scheme.eligibility,
    scheme.objective,
    scheme.description && scheme.benefit ? scheme.description : null,
    (scheme.department || scheme.ministry)
      ? `Managed by: ${scheme.department || scheme.ministry}` : null,
  ].filter(Boolean);

  const Label = ({ children }) => (
    <div style={{ fontSize:10, fontWeight:700, color:"#9ca3af",
      letterSpacing:"0.08em", marginBottom:6, textTransform:"uppercase" }}>
      {children}
    </div>
  );

  return (
    <>
      <div onClick={onClose} style={{
        position:"fixed", inset:0, background:"rgba(0,0,0,0.3)",
        zIndex:1000, backdropFilter:"blur(2px)",
      }}/>
      <div style={{
        position:"fixed",
        top:"50%", left:"50%",
        transform:"translate(-50%, -50%)",
        width:"min(600px, 95vw)",
        maxHeight:"90vh",
        background:"white", zIndex:1001, overflowY:"auto",
        boxShadow:"0 24px 80px rgba(0,0,0,0.25)",
        borderRadius:"20px",
        display:"flex", flexDirection:"column",
        animation:"popIn 0.2s ease",
      }}>
        <style>{`
          @keyframes popIn {
            from { transform:translate(-50%, -48%); opacity:0; }
            to { transform:translate(-50%, -50%); opacity:1; }
          }
        `}</style>

        {/* Header */}
        <div style={{
          padding:"22px 24px 18px",
          borderBottom:"1px solid #f0f2f5",
          background:`linear-gradient(135deg, ${srcMeta.color}08, white)`,
          borderRadius:"20px 20px 0 0",
        }}>
          <div style={{ display:"flex", alignItems:"flex-start", gap:14 }}>
            <div style={{ width:50, height:50, borderRadius:12, flexShrink:0,
              background:`${srcMeta.color}18`, display:"flex", alignItems:"center",
              justifyContent:"center", fontSize:24 }}>
              {CAT_ICON[scheme.category]||"📋"}
            </div>
            <div style={{ flex:1 }}>
              <div style={{ fontWeight:800, fontSize:16, color:"#111827", lineHeight:1.35 }}>
                {scheme.name}
              </div>
              <div style={{ fontSize:11.5, color:"#9ca3af", marginTop:3 }}>
                {scheme.category}
                {scheme.subcategory ? ` · ${scheme.subcategory}` : ""}
                {" · "}
                <span style={{ color:srcMeta.color, fontWeight:600 }}>
                  {scheme._src_label || srcMeta.label}
                </span>
              </div>
            </div>
            <button onClick={onClose} style={{
              border:"none", background:"#f3f4f6", borderRadius:8,
              width:30, height:30, cursor:"pointer", fontSize:15,
              display:"flex", alignItems:"center", justifyContent:"center" }}>✕</button>
          </div>
        </div>

        {factCards.length > 0 && (
          <div style={{
            display:"grid",
            gridTemplateColumns:"1fr 1fr",
            borderBottom:"1px solid #f0f2f5",
          }}>
            {factCards.map((card, idx) => (
              <div key={`${card.label}_${idx}`} style={{
                padding:"18px 24px",
                borderRight: idx % 2 === 0 && idx !== factCards.length - 1 ? "1px solid #f0f2f5" : "none",
                borderBottom: idx < factCards.length - 2 ? "1px solid #f0f2f5" : "none",
              }}>
                <Label>{card.label}</Label>
                <div style={{ fontSize:20, fontWeight:800, color:card.color }}>
                  {card.label === "Launch Year"
                    ? String(card.value)
                    : typeof card.value === "number"
                    ? card.value.toLocaleString("en-IN")
                    : card.value}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Progress */}
        <div style={{ padding:"20px 24px", borderBottom:"1px solid #f0f2f5" }}>
          {progressPct != null ? (
            <>
              <div style={{ display:"flex", alignItems:"center",
                justifyContent:"space-between", marginBottom:14 }}>
                <span style={{ fontSize:13, fontWeight:500, color:"#374151" }}>
                  Implementation Progress
                </span>
                <div style={{ position:"relative", width:56, height:56 }}>
                  <svg width="56" height="56" style={{ transform:"rotate(-90deg)" }}>
                    <circle cx="28" cy="28" r="22" fill="none" stroke="#e5e7eb" strokeWidth="5"/>
                    <circle cx="28" cy="28" r="22" fill="none"
                      strokeWidth="5"
                      stroke={progressColor}
                      strokeDasharray={`${((progressPct || 0)/100)*138.2} 138.2`}
                      strokeLinecap="round"/>
                  </svg>
                  <div style={{ position:"absolute", inset:0, display:"flex",
                    alignItems:"center", justifyContent:"center",
                    fontSize:11, fontWeight:800, color:progressColor }}>
                    {progressLabel}
                  </div>
                </div>
              </div>
              <div style={{ background:"#e5e7eb", borderRadius:4, height:7,
                overflow:"hidden", marginBottom:6 }}>
                <div style={{ width:`${Math.min(progressPct || 0,100)}%`, height:"100%",
                  background:progressColor, borderRadius:4 }}/>
              </div>
              <div style={{ display:"flex", justifyContent:"space-between",
                fontSize:11, color:"#9ca3af" }}>
                <span>0%</span>
                <span style={{ color:"#10b981", fontWeight:600 }}>✓ On Track</span>
                <span>100%</span>
              </div>
              {scheme.progress_source && (
                <div style={{ marginTop:8, fontSize:11.5, color:"#6b7280", lineHeight:1.4 }}>
                  Source signal: {scheme.progress_source}
                </div>
              )}
            </>
          ) : (
            <div style={{
              background:"#f9fafb",
              border:"1px solid #e5e7eb",
              borderRadius:10,
              padding:"12px 14px",
            }}>
              <div style={{ fontSize:13, fontWeight:700, color:"#374151", marginBottom:4 }}>
                Implementation Progress
              </div>
              <div style={{ fontSize:12.5, color:"#6b7280" }}>
                No official implementation progress is available from current source data.
              </div>
            </div>
          )}
        </div>

        {/* Summary */}
        <div style={{ padding:"18px 24px", borderBottom:"1px solid #f0f2f5" }}>
          <Label>Scheme Summary</Label>
          <div style={{ fontSize:13.5, color:"#374151", lineHeight:1.65 }}>
            {schemeSummary}
          </div>
        </div>

        {/* Official metrics chart */}
        <div style={{ padding:"18px 24px", borderBottom:"1px solid #f0f2f5" }}>
          <Label>Official Scheme Metrics</Label>
          {chartData.length > 0 ? (
            <>
              <div style={{ height:220 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart
                    data={chartData}
                    layout="vertical"
                    margin={{ top: 8, right: 18, bottom: 8, left: 18 }}
                  >
                    <XAxis type="number" hide />
                    <YAxis
                      type="category"
                      dataKey="label"
                      width={110}
                      tick={{ fontSize: 11, fill: "#6b7280" }}
                      axisLine={false}
                      tickLine={false}
                    />
                    <Tooltip
                      formatter={(value, name, payload) => [payload?.payload?.display || value, payload?.payload?.label || name]}
                      labelFormatter={() => "Official source figure"}
                      contentStyle={{ borderRadius: 10, border: "1px solid #e5e7eb", fontSize: 12 }}
                    />
                    <Bar dataKey="value" radius={[0, 8, 8, 0]}>
                      {chartData.map((entry, index) => (
                        <Cell key={`${entry.label}_${index}`} fill={entry.color || srcMeta.color} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <div style={{ display:"grid", gap:8, marginTop:8 }}>
                {chartData.map((metric, index) => (
                  <div key={`${metric.label}_${index}`} style={{
                    display:"flex", justifyContent:"space-between", gap:10,
                    background:"#f8fafc", border:"1px solid #e5e7eb",
                    borderRadius:10, padding:"9px 11px"
                  }}>
                    <div style={{ minWidth:0 }}>
                      <div style={{ fontSize:12, fontWeight:700, color:"#111827" }}>{metric.label}</div>
                      <div style={{ fontSize:11, color:"#6b7280", lineHeight:1.45 }}>{metric.source}</div>
                    </div>
                    <div style={{ fontSize:12, fontWeight:800, color:metric.color || srcMeta.color, whiteSpace:"nowrap" }}>
                      {metric.display}
                    </div>
                  </div>
                ))}
              </div>
            </>
          ) : (
            <div style={{
              background:"#f9fafb",
              border:"1px solid #e5e7eb",
              borderRadius:10,
              padding:"12px 14px",
            }}>
              <div style={{ fontSize:13, fontWeight:700, color:"#374151", marginBottom:4 }}>
                Official quantified data is not available
              </div>
              <div style={{ fontSize:12.5, color:"#6b7280", lineHeight:1.5 }}>
                This portal record does not publish a reliable numeric scheme figure beyond the text summary, so the dashboard avoids showing an invented chart.
              </div>
            </div>
          )}
        </div>

        {/* Coverage / Benefits */}
        {(scheme.benefit || scheme.description) && (
          <div style={{ padding:"18px 24px", borderBottom:"1px solid #f0f2f5" }}>
            <Label>Coverage / Benefits</Label>
            <div style={{ fontSize:14, color:"#111827", lineHeight:1.6 }}>
              {scheme.benefit || scheme.description}
            </div>
          </div>
        )}

        {/* Key Facts */}
        {keyFacts.length > 0 && (
          <div style={{ padding:"18px 24px", borderBottom:"1px solid #f0f2f5" }}>
            <Label>Key Facts</Label>
            <div style={{ fontSize:13, color:"#374151", lineHeight:1.65 }}>
              {keyFacts.join(". ").replace(/\.\./g, ".")}
            </div>
          </div>
        )}

        {/* Tags */}
        {scheme.tags?.length > 0 && (
          <div style={{ padding:"14px 24px", borderBottom:"1px solid #f0f2f5",
            display:"flex", gap:6, flexWrap:"wrap" }}>
            {scheme.tags.map((t, i) => (
              <span key={i} style={{ background:`${srcMeta.color}12`,
                color:srcMeta.color, border:`1px solid ${srcMeta.color}25`,
                borderRadius:20, padding:"3px 11px", fontSize:11, fontWeight:600 }}>
                {t}
              </span>
            ))}
          </div>
        )}

        {/* Data Source + URL preview */}
        <div style={{ padding:"16px 24px", borderBottom:"1px solid #f0f2f5",
          background:"#fafafa" }}>
          <Label>Data Source</Label>
          <div style={{ fontSize:12.5, color:"#374151", fontWeight:500, marginBottom:3 }}>
            {scheme.source || srcMeta.url}
          </div>
          <div style={{ fontSize:11, color:"#9ca3af",
            overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>
            🔗 {sourceUrl}
          </div>
        </div>

        {/* Know More — links to the actual scheme page, NOT just the homepage */}
        <div style={{ padding:"16px 24px 28px" }}>
          <a href={sourceUrl} target="_blank" rel="noreferrer" style={{
            display:"block", textAlign:"center",
            background:srcMeta.color, color:"white",
            borderRadius:10, padding:"13px 20px",
            fontSize:14, fontWeight:700, cursor:"pointer", textDecoration:"none",
            boxShadow:`0 4px 16px ${srcMeta.color}45`,
          }}>
            Know More ↗
          </a>
        </div>
      </div>
    </>
  );
}

// ── Schemes Tab ───────────────────────────────────────────────────────────────
function SchemesTab({ agg, onScrapeAll, rajrasData, jansoochnaData }) {
  const [search, setSearch]     = useState("");
  const [cat, setCat]           = useState("all");
  const [src, setSrc]           = useState("all");
  const [selected, setSelected] = useState(null);

  const aggregateSchemes = agg?.schemes || [];
  const normalizedRajras = useMemo(
    () =>
      (rajrasData || []).map((s, i) => ({
        id: s.id || `rajras_file_${i + 1}`,
        name: s.name || "Untitled Scheme",
        category: s.category || "General",
        description: s.description || "",
        benefit: Array.isArray(s.benefits) ? s.benefits.join(" | ") : (s.benefits || s.benefit || ""),
        eligibility: Array.isArray(s.eligibility) ? s.eligibility.join(" | ") : (s.eligibility || ""),
        documents_required: Array.isArray(s.documents_required)
          ? s.documents_required.join(" | ")
          : (s.documents_required || ""),
        headings: s.headings || null,
        progress_pct: typeof s.progress_pct === "number" ? s.progress_pct : null,
        progress: s.progress || null,
        progress_source: s.progress_source || null,
        progress_updated_at: s.progress_updated_at || null,
        budget_amount: s.budget_amount || null,
        budget: s.budget || null,
        beneficiary_display: s.beneficiary_display || null,
        beneficiary_count: s.beneficiary_count || null,
        beneficiaries: s.beneficiaries || null,
        launch_year: s.launch_year || null,
        districts: s.districts || null,
        source: s.source || "RajRAS",
        url: s.url || "",
        status: "Active",
        _src: "rajras",
        _src_label: "RajRAS",
        _src_url: "rajras.in",
      })),
    [rajrasData]
  );
  const normalizedJansoochna = useMemo(
    () =>
      (jansoochnaData || []).map((s, i) => ({
        id: s.id || `jsp_file_${i + 1}`,
        name: s.name || "Untitled Scheme",
        category: s.category || "General Services",
        description: s.description || "",
        benefit: Array.isArray(s.benefits) ? s.benefits.join(" | ") : (s.benefits || s.benefit || ""),
        eligibility: Array.isArray(s.eligibility) ? s.eligibility.join(" | ") : (s.eligibility || ""),
        documents_required: Array.isArray(s.documents_required)
          ? s.documents_required.join(" | ")
          : (s.documents_required || ""),
        department: s.department || null,
        beneficiary_count: s.beneficiary_count || null,
        beneficiary_display: s.beneficiary_display || null,
        headings: s.headings || null,
        progress_pct: typeof s.progress_pct === "number" ? s.progress_pct : null,
        progress: s.progress || null,
        progress_source: s.progress_source || null,
        progress_updated_at: s.progress_updated_at || null,
        budget_amount: s.budget_amount || null,
        source: s.source || "Jan Soochna",
        url: s.url || "",
        status: "Active",
        _src: "jansoochna",
        _src_label: "Jan Soochna",
        _src_url: "jansoochna.rajasthan.gov.in",
      })),
    [jansoochnaData]
  );
  const schemes = useMemo(() => {
    let result = aggregateSchemes;
    const aggregateJansoochna = aggregateSchemes.filter(s => s._src === "jansoochna");
    const dedicatedJansoochnaHasMoreDetail = (jansoochnaData || []).some(
      s => s?.headings || s?.documents_required || s?.progress_pct != null || s?.benefits || s?.eligibility
    );
    if (normalizedRajras.length) {
      result = [...normalizedRajras, ...result.filter(s => s._src !== "rajras")];
    }
    if (
      normalizedJansoochna.length &&
      (
        normalizedJansoochna.length >= aggregateJansoochna.length ||
        aggregateJansoochna.length === 0 ||
        dedicatedJansoochnaHasMoreDetail
      )
    ) {
      result = [...normalizedJansoochna, ...result.filter(s => s._src !== "jansoochna")];
    }
    return result;
  }, [aggregateSchemes, normalizedRajras, normalizedJansoochna, jansoochnaData]);

  if (!schemes.length) return <EmptyState onScrape={onScrapeAll}/>;

  // Pill → match substring that covers all 3 scrapers' category strings:
  // RajRAS cats:      Agriculture, Health, Education, Social Welfare,
  //                   Labour & Employment, Rural Development, Housing,
  //                   Industry & Commerce, Energy, Water & Irrigation,
  //                   Digital & IT, Tourism & Culture, Mining
  // Jan Soochna cats: Social Welfare, Health, Agriculture, Food Security,
  //                   Labour & Employment, Education, Rural Development,
  //                   Digital Services, Water & Sanitation, Energy,
  //                   Identity & Social Security, Urban Development
  // MyScheme cats:    Health, Education, Agriculture, Social Welfare,
  //                   Women & Child, Labour & Employment, Business & Finance,
  //                   Housing, Food Security, Water & Sanitation,
  //                   Energy, Digital Services, General
  const PILL_CATS = [
    { id:"all",       label:"All",         icon:null,  match:null },
    { id:"health",    label:"Health",      icon:"🏥",  match:"health" },
    { id:"education", label:"Education",   icon:"🎓",  match:"education" },
    { id:"agri",      label:"Agriculture", icon:"🌾",  match:"agri" },
    { id:"social",    label:"Social",      icon:"🛡️",  match:"social" },
    { id:"labour",    label:"Employment",  icon:"💼",  match:"labour" },
    { id:"women",     label:"Women",       icon:"👩",  match:"women" },
    { id:"housing",   label:"Housing",     icon:"🏠",  match:"housing" },
    { id:"food",      label:"Food",        icon:"🍽️",  match:"food" },
    { id:"water",     label:"Water",       icon:"💧",  match:"water" },
    { id:"energy",    label:"Energy",      icon:"⚡",  match:"energy" },
    { id:"digital",   label:"Digital",     icon:"💻",  match:"digital" },
    { id:"rural",     label:"Rural",       icon:"🏘️",  match:"rural" },
    { id:"identity",  label:"Identity",    icon:"🪪",  match:"identity" },
  ];

  const activePill = PILL_CATS.find(p => p.id === cat);
  const catMatch   = activePill?.match || null;

  const filtered = schemes.filter(s => {
    // Use catMatch substring so "social" matches "Social Welfare",
    // "labour" matches "Labour & Employment", "women" matches "Women & Child", etc.
    const mCat = !catMatch || (s.category || "").toLowerCase().includes(catMatch);
    const mSrc = src === "all" || s._src === src;
    const mQ   = !search ||
      s.name?.toLowerCase().includes(search.toLowerCase()) ||
      s.description?.toLowerCase().includes(search.toLowerCase()) ||
      s.benefit?.toLowerCase().includes(search.toLowerCase()) ||
      s.category?.toLowerCase().includes(search.toLowerCase()) ||
      s.ministry?.toLowerCase().includes(search.toLowerCase()) ||
      s.department?.toLowerCase().includes(search.toLowerCase());
    return mCat && mSrc && mQ;
  });

  return (
    <div className="fadeup">
      <div style={{
        background:`linear-gradient(135deg, ${THEME.desertRose} 0%, #ffffff 44%, ${THEME.peacockSoft} 100%)`,
        border:`1px solid ${THEME.border}`,
        borderRadius:28,
        padding:"22px 22px 18px",
        marginBottom:20,
        boxShadow:THEME.shadow,
        position:"relative",
        overflow:"hidden",
      }}>
        <div style={{
          position:"absolute",
          right:-30,
          top:-42,
          width:150,
          height:150,
          borderRadius:"50%",
          background:"radial-gradient(circle, rgba(15,124,151,0.12), rgba(15,124,151,0))",
          pointerEvents:"none",
        }}/>
        <div style={{ display:"flex", alignItems:"flex-start", justifyContent:"space-between", gap:20, flexWrap:"wrap", position:"relative" }}>
          <div>
            <div style={{ display:"inline-flex", alignItems:"center", gap:8, background:"#fff4e7", border:"1px solid #ffd5b2", color:THEME.saffronDeep, borderRadius:999, padding:"7px 13px", fontSize:12, fontWeight:800, marginBottom:12 }}>
              Curated Scheme Intelligence
            </div>
            <div style={{ display:"flex", alignItems:"center", marginBottom:4 }}>
              <h2 style={{ fontSize:28, fontWeight:900, margin:0, color:THEME.ink, letterSpacing:"-0.04em" }}>
                Government Schemes
                {" "}
                <span style={{ color:THEME.saffron }}>Overview</span>
              </h2>
              <InfoTip text="Schemes from 3 sources: RajRAS (HTML scrape → name, eligibility, benefit), Jan Soochna (JSON API → name, dept, beneficiary count), MyScheme (REST API → name, ministry, tags, description). Categories are keyword-derived."/>
            </div>
            <p style={{ color:THEME.muted, fontSize:13.5, margin:"0 0 12px", lineHeight:1.7, maxWidth:760 }}>
              {schemes.length} schemes scraped live from RajRAS, Jan Soochna, and MyScheme. Search, filter, and open any card for a source-linked briefing note.
            </p>
            <div style={{ display:"flex", gap:10, flexWrap:"wrap" }}>
              <span style={{ display:"inline-flex", alignItems:"center", gap:8, borderRadius:999, padding:"8px 14px", background:"rgba(255,255,255,0.84)", border:`1px solid ${THEME.border}`, color:"#334155", fontSize:12.5, fontWeight:700 }}>
                <span style={{ width:8, height:8, borderRadius:"50%", background:THEME.emerald, boxShadow:"0 0 0 3px #d1fae5", display:"inline-block" }}/>
                Multi-source verified catalogue
              </span>
              <span style={{ display:"inline-flex", alignItems:"center", gap:8, borderRadius:999, padding:"8px 14px", background:THEME.indigoSoft, border:"1px solid #cad6ff", color:THEME.indigo, fontSize:12.5, fontWeight:700 }}>
                Responsive briefing cards
              </span>
            </div>
          </div>
          <div style={{ display:"grid", gridTemplateColumns:"repeat(2, minmax(120px, 1fr))", gap:10, minWidth:260 }}>
            {[
              { value: schemes.length, label: "Total Schemes", color: THEME.saffron, bg: "#fff6ea" },
              { value: filtered.length, label: "Visible Now", color: THEME.peacock, bg: "#eefbfd" },
              { value: new Set(schemes.map((scheme) => scheme._src)).size, label: "Active Sources", color: THEME.emerald, bg: "#effcf5" },
              { value: new Set(schemes.map((scheme) => scheme.category).filter(Boolean)).size, label: "Categories", color: THEME.indigo, bg: "#eef2ff" },
            ].map((item) => (
              <div key={item.label} style={{ background:item.bg, border:`1px solid ${item.color}22`, borderRadius:18, padding:"14px 15px" }}>
                <div style={{ fontSize:24, fontWeight:900, color:item.color, lineHeight:1 }}>{item.value}</div>
                <div style={{ fontSize:11, fontWeight:700, color:THEME.muted, marginTop:5 }}>{item.label}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
      {/* Search */}
      <div style={{ position:"relative", marginBottom:12 }}>
        <span style={{ position:"absolute", left:14, top:"50%",
          transform:"translateY(-50%)", fontSize:15 }}>🔍</span>
        <input value={search} onChange={e => setSearch(e.target.value)}
          placeholder="Search schemes, benefits, categories…"
          style={{ width:"100%", padding:"11px 14px 11px 42px",
            border:`1.5px solid ${THEME.border}`, borderRadius:16, fontSize:14,
            background:"rgba(255,255,255,0.86)", boxSizing:"border-box",
            boxShadow:"0 10px 26px rgba(15,23,42,0.04)" }}/>
      </div>

      {/* Source filter */}
      <div style={{ display:"flex", gap:6, marginBottom:10, flexWrap:"wrap" }}>
        {["all","rajras","jansoochna","myscheme"].map(id => {
          const s = SRC[id];
          return (
            <button key={id} onClick={() => setSrc(id)} style={{
              background: src===id ? `linear-gradient(135deg, ${(s?.color||"#1f2937")}, ${(s?.color||"#1f2937")}dd)` : "rgba(255,255,255,0.9)",
              color: src===id ? "white" : "#374151",
              border:`1.5px solid ${src===id ? (s?.color||"#1f2937") : THEME.border}`,
              borderRadius:999, padding:"7px 14px", fontSize:12, fontWeight:700,
              cursor:"pointer",
              boxShadow: src===id ? `0 10px 22px ${s?.color || "#1f2937"}22` : "none",
            }}>
              {id==="all" ? "All Sources" : `${s.icon} ${s.label}`}
            </button>
          );
        })}
      </div>

      {/* Category pills */}
      <div style={{ display:"flex", gap:6, flexWrap:"wrap", marginBottom:16 }}>
        {PILL_CATS.map(p => (
          <button key={p.id} onClick={() => setCat(p.id)} style={{
            background: cat===p.id ? `linear-gradient(135deg, ${THEME.saffron}, #f39b52)` : "rgba(255,255,255,0.88)",
            color: cat===p.id ? "white" : "#374151",
            border:`1.5px solid ${cat===p.id ? THEME.saffron : THEME.border}`,
            borderRadius:999, padding:"7px 14px", fontSize:12.5, fontWeight:700,
            cursor:"pointer", display:"flex", alignItems:"center", gap:5,
            boxShadow: cat===p.id ? "0 10px 24px rgba(229,106,31,0.18)" : "none",
          }}>
            {p.icon && <span>{p.icon}</span>}
            {p.label}
          </button>
        ))}
      </div>

      <div style={{ color:"#9ca3af", fontSize:13, marginBottom:14 }}>
        Showing <strong style={{ color:"#374151" }}>{filtered.length}</strong> of {schemes.length} schemes
        {catMatch && <span> · <span style={{ color:"#f97316" }}>{activePill?.label}</span></span>}
      </div>

      {/* 4-column card grid */}
      <div style={{
        display:"grid",
        gridTemplateColumns:"repeat(auto-fit, minmax(280px, 1fr))",
        gap:16,
      }}>
        {filtered.map((scheme, i) => {
          const srcMeta    = SRC[scheme._src] || SRC.myscheme;
          const benefitText = cleanInlineText(scheme.benefit || scheme.description || "");
          const cardProgressPct = getProgressPct(scheme);
          const launchYear  = scheme.launched
            ? String(scheme.launched).match(/\d{4}/)?.[0]
            : scheme.scraped_at?.slice(0,4) || null;
          const cleanedBenefit = benefitText
            ? benefitText.replace(/[\u0900-\u097F]+/g, "").replace(/\s+/g, " ").trim()
            : "";
          const cardSummary = cleanedBenefit && cleanedBenefit.length > 20
            ? `${cleanedBenefit.slice(0, 96)}${cleanedBenefit.length > 96 ? "..." : ""}`
            : `Official ${String(scheme.category || "public welfare").toLowerCase()} scheme from ${scheme._src_label || srcMeta.label}.`;
          const cardStats = [
            {
              label:"Beneficiaries",
              val:scheme.beneficiary_display || (scheme.beneficiary_count ? String(scheme.beneficiary_count) : null),
              color:srcMeta.color,
              emptyLabel:"Not published",
            },
            {
              label:"Budget",
              val:scheme.budget_amount || null,
              color:"#1f2937",
              emptyLabel:"Awaiting figure",
            },
            {
              label:"Progress",
              val:cardProgressPct!=null ? `${cardProgressPct}%` : null,
              color:"#10b981",
              emptyLabel:"Official update pending",
            },
          ];
          const availableStatCount = cardStats.filter((stat) => stat.val).length;
          const hasAnyMetric = availableStatCount > 0;

          return (
            <div key={i} onClick={() => setSelected(scheme)} style={{
              background:"linear-gradient(180deg,rgba(255,255,255,0.98) 0%, rgba(250,252,255,0.98) 100%)", borderRadius:22, border:"1px solid #e5e7eb",
              padding:"18px 18px 16px", borderTop:`4px solid ${srcMeta.color}`,
              cursor:"pointer", boxShadow:"0 16px 34px rgba(15,23,42,0.06)",
              transition:"box-shadow .15s, transform .12s",
              display:"flex", flexDirection:"column", minHeight:242,
              position:"relative", overflow:"hidden",
            }}
            onMouseEnter={e => {
              e.currentTarget.style.boxShadow = "0 22px 46px rgba(15,23,42,0.12)";
              e.currentTarget.style.transform = "translateY(-4px)";
            }}
            onMouseLeave={e => {
              e.currentTarget.style.boxShadow = "0 16px 34px rgba(15,23,42,0.06)";
              e.currentTarget.style.transform = "translateY(0)";
            }}>
              <div style={{
                position:"absolute",
                right:-18,
                top:-28,
                width:96,
                height:96,
                borderRadius:"50%",
                background:`radial-gradient(circle, ${srcMeta.color}18, ${srcMeta.color}00)`,
                pointerEvents:"none",
              }}/>

              {/* Icon + Name + Category */}
              <div style={{ display:"flex", alignItems:"flex-start", gap:10, marginBottom:10 }}>
                <div style={{ width:40, height:40, borderRadius:12, flexShrink:0,
                  background:`${srcMeta.color}18`, display:"flex",
                  alignItems:"center", justifyContent:"center", fontSize:18, boxShadow:`inset 0 0 0 1px ${srcMeta.color}22` }}>
                  {CAT_ICON[scheme.category]||"📋"}
                </div>
                <div style={{ flex:1, minWidth:0 }}>
                  <div style={{ display:"flex", alignItems:"flex-start", justifyContent:"space-between", gap:8 }}>
                    <div style={{ flex:1, minWidth:0 }}>
                      <div style={{ fontWeight:700, fontSize:13, color:"#1f2937",
                        lineHeight:1.3, marginBottom:2,
                        overflow:"hidden", textOverflow:"ellipsis",
                        display:"-webkit-box", WebkitLineClamp:2,
                        WebkitBoxOrient:"vertical" }}>
                        {scheme.name}
                      </div>
                      <div style={{ fontSize:10.5, color:"#9ca3af" }}>
                        {scheme.category||"General"}
                        {launchYear ? ` · Since ${launchYear}` : ""}
                      </div>
                    </div>
                    <div style={{
                      background: hasAnyMetric ? `${srcMeta.color}12` : "#f8fafc",
                      color: hasAnyMetric ? srcMeta.color : "#94a3b8",
                      border: `1px solid ${hasAnyMetric ? `${srcMeta.color}25` : "#e2e8f0"}`,
                      borderRadius:999, padding:"4px 8px", fontSize:10, fontWeight:700,
                      whiteSpace:"nowrap", flexShrink:0
                    }}>
                      {hasAnyMetric ? `${availableStatCount}/3 metrics` : "View details →"}
                    </div>
                  </div>
                </div>
              </div>

              {/* Benefit / description */}
              <div style={{ fontSize:11.5, color:"#475569", lineHeight:1.55,
                marginBottom:12, minHeight:54 }}>
                {cardSummary}
              </div>

              {/* Stats row */}
              {hasAnyMetric && (
                <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr",
                  gap:8, marginBottom:10 }}>
                  {cardStats.filter((stat) => stat.val).map((stat,j) => (
                    <div key={j} style={{
                      background:`${stat.color}10`,
                      border:`1px solid ${stat.color}18`,
                      borderRadius:10, padding:"8px 8px 7px",
                    }}>
                      <div style={{ fontSize:9, fontWeight:700, color:"#94a3b8",
                        letterSpacing:"0.06em", marginBottom:5, textTransform:"uppercase" }}>{stat.label}</div>
                      <div style={{ fontSize:13, fontWeight:800,
                        color:stat.color, lineHeight:1.2 }}>
                        {typeof stat.val === "number"
                          ? stat.val.toLocaleString("en-IN")
                          : stat.val}
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {/* Progress bar */}
              <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between",
                marginBottom:6, gap:8 }}>
                <span style={{ fontSize:10.5, fontWeight:700, color:"#94a3b8", letterSpacing:"0.04em", textTransform:"uppercase" }}>
                  Data Availability
                </span>
                <span style={{ fontSize:11, color:hasAnyMetric ? "#475569" : "#94a3b8", fontWeight:600, whiteSpace:"nowrap" }}>
                  {hasAnyMetric ? `${availableStatCount} official field${availableStatCount > 1 ? "s" : ""} visible` : "Details open for full text"}
                </span>
              </div>
              <div style={{ height:5, background:"#f1f5f9", borderRadius:999,
                overflow:"hidden", marginBottom:12 }}>
                <div style={{
                  height:"100%",
                  width: `${Math.max((availableStatCount / cardStats.length) * 100, hasAnyMetric ? 24 : 12)}%`,
                  background: hasAnyMetric
                    ? `linear-gradient(90deg,${srcMeta.color},${srcMeta.color}99)`
                    : "linear-gradient(90deg,#cbd5e1,#e2e8f0)",
                  borderRadius:999,
                }}/>
              </div>

              {/* Source citation */}
              <div style={{ display:"flex", alignItems:"center", gap:5,
                fontSize:9.5, color:"#94a3b8", marginTop:"auto" }}>
                <span style={{ display:"inline-block", width:12, height:12,
                  background:`${srcMeta.color}20`, borderRadius:3,
                  textAlign:"center", lineHeight:"12px", fontSize:8, flexShrink:0 }}>
                  {srcMeta.icon}
                </span>
                <span style={{ overflow:"hidden", textOverflow:"ellipsis",
                  whiteSpace:"nowrap" }}>
                  {scheme.source || srcMeta.url}
                </span>
              </div>
            </div>
          );
        })}
      </div>

      <SchemeDetailPanel scheme={selected} onClose={() => setSelected(null)}/>
    </div>
  );
}

// ── Portals Tab ───────────────────────────────────────────────────────────────
function PortalsTab({ agg, onScrapeAll }) {
  if (!agg?.portals?.length) return <EmptyState onScrape={onScrapeAll}/>;
  const { portals } = agg;
  const groups = {};
  portals.forEach(p=>{ const c=p.category||"General"; if(!groups[c])groups[c]=[]; groups[c].push(p); });
  const totalPortals=portals.length, totalCategories=Object.keys(groups).length;
  const activePortals=portals.filter(p=>p.status==="Active").length;
  const sampleLastUpd=portals.find(p=>p.directory_last_updated)?.directory_last_updated||"";
  const totalListed=portals.find(p=>p.total_portals_listed)?.total_portals_listed||"";
  return (
    <div className="fadeup">
      <div style={{ display:"flex", alignItems:"center", marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
        Government Portals — <span style={{ color:"#f97316" }}>IGOD Directory</span>
      </h2>
        <InfoTip text="Scraped from the official IGOD Rajasthan directory. Each card shows the portal name, domain, status, and lightweight homepage metadata collected directly from the portal URL."/>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:4 }}>
        Source: igod.gov.in/sg/RJ/SPMA/organizations{totalListed?` · ${totalListed}`:""}
      </p>
      {sampleLastUpd&&<p style={{ color:"#9ca3af", fontSize:12, marginBottom:20 }}>Directory last updated: {sampleLastUpd}</p>}
      {!sampleLastUpd&&<div style={{ marginBottom:20 }}/>}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(3,1fr)", gap:14, marginBottom:24 }}>
        {[
          {value:totalPortals,label:"Portals Listed",bg:"#eff6ff",border:"#bfdbfe",color:"#1d4ed8"},
          {value:totalCategories,label:"Categories",bg:"#f0fdf4",border:"#bbf7d0",color:"#166534"},
          {value:activePortals,label:"Active Portals",bg:"#fff7ed",border:"#fed7aa",color:"#9a3412"},
        ].map((s,i)=>(
          <div key={i} style={{ background:s.bg, border:`1.5px solid ${s.border}`, borderRadius:14, padding:22 }}>
            <div style={{ fontSize:38, fontWeight:900, color:s.color, marginBottom:6 }}>{s.value}</div>
            <div style={{ fontSize:14, fontWeight:600, color:s.color }}>{s.label}</div>
          </div>
        ))}
      </div>
      {Object.entries(groups).map(([catName,items])=>(
        <div key={catName} style={{ marginBottom:22 }}>
          <div style={{ fontWeight:700, fontSize:15, color:"#374151", marginBottom:12,
            display:"flex", alignItems:"center", gap:8 }}>
            <span>{CAT_ICON[catName]||"🏛️"}</span> {catName}
            <Chip label={`${items.length}`} color="#f97316" small/>
          </div>
          <div style={{ display:"grid", gridTemplateColumns:"repeat(2,1fr)", gap:10 }}>
            {items.map((portal,i)=>(
              <div key={i} style={{ background:"white", borderRadius:12, border:"1px solid #e5e7eb",
                padding:16, display:"flex", gap:12, alignItems:"flex-start" }}>
                <div style={{ width:38, height:38, borderRadius:8, background:"#f97316"+"18",
                  display:"flex", alignItems:"center", justifyContent:"center", fontSize:18, flexShrink:0 }}>
                  {CAT_ICON[catName]||"🏛️"}
                </div>
                <div style={{ flex:1, minWidth:0 }}>
                  <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:2, flexWrap:"wrap" }}>
                    <div style={{ fontWeight:700, fontSize:13, color:"#1f2937" }}>{portal.name}</div>
                    <Chip
                      label={portal.status || "Unknown"}
                      color={portal.status === "Active" ? "#10b981" : portal.status === "Unreachable" ? "#ef4444" : "#f59e0b"}
                      small
                    />
                  </div>
                  <div style={{ fontSize:11, color:"#9ca3af", marginBottom:6,
                    overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{portal.domain}</div>
                  {(portal.page_title || portal.portal_title) && (
                    <div style={{ fontSize:11, color:"#374151", fontWeight:600, marginBottom:4 }}>
                      {portal.page_title || portal.portal_title}
                    </div>
                  )}
                  {portal.description&&<div style={{ fontSize:12, color:"#6b7280", lineHeight:1.4, marginBottom:6 }}>
                    {portal.description.slice(0,140)}{portal.description.length>140?"…":""}
                  </div>}
                  <div style={{ display:"flex", gap:6, flexWrap:"wrap", marginBottom:8 }}>
                    {portal.content_type && <Chip label={portal.content_type} color="#64748b" small/>}
                    {portal.response_time_ms ? <Chip label={`${portal.response_time_ms} ms`} color="#0ea5e9" small/> : null}
                    {portal.redirect_url && portal.redirect_url !== portal.url ? <Chip label="Redirected" color="#8b5cf6" small/> : null}
                  </div>
                  <a href={portal.url} target="_blank" rel="noreferrer" style={{ fontSize:12, color:"#f97316", fontWeight:600 }}>Visit ↗</a>
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

// ── Districts Tab ─────────────────────────────────────────────────────────────
function DistrictsTab({ agg, onScrapeAll, schemeDashboards = [] }) {
  const [selectedScheme, setSelectedScheme] = useState("jal_shakti");
  const [distSearch, setDistSearch] = useState("");
  const [sortBy, setSortBy] = useState("coverage_desc");

  if (!agg) return <EmptyState onScrape={onScrapeAll}/>;

  const fallbackDashboards = useMemo(() => {
    const jjmRows = (agg?.jjm_districts || []).map((row) => ({
      district: row.name,
      population: row.pop || "—",
      coverage_pct: row.coverage || 0,
      status: (row.coverage || 0) >= 70 ? "On track" : (row.coverage || 0) >= 50 ? "Needs push" : "Critical",
      status_tone: (row.coverage || 0) >= 70 ? "good" : (row.coverage || 0) >= 50 ? "watch" : "critical",
    }));
    const pmksyRows = (agg?.pmksy_districts || []).map((row) => ({
      district: row.name,
      net_area_sown: row.net_area_sown_display,
      net_irrigated_area: row.net_irrigated_area_display,
      coverage_pct: row.coverage_pct || 0,
      status: row.status,
      status_tone: row.status_tone,
    }));
    return [
      {
        id: "jal_shakti",
        label: "Jal Shakti",
        icon: "💧",
        source: "ejalshakti.gov.in",
        source_url: "https://ejalshakti.gov.in/jjmreport/JJMIndia.aspx",
        description: "JJM tap water coverage by district",
        live: true,
        status: "ok",
        status_label: "Live data",
        scraped_at: agg?.jjm_districts?.[0]?.scraped_at || agg?.scraped_at,
        verified_label: "Official source",
        note: "Functional tap water coverage from the Jal Jeevan Mission public dashboard.",
        summary: {
          primary: jjmRows.length ? `${(jjmRows.reduce((sum, row) => sum + row.coverage_pct, 0) / jjmRows.length).toFixed(1)}%` : "—",
          primaryLabel: "State Average Coverage",
          good: jjmRows.filter((row) => row.coverage_pct >= 70).length,
          goodLabel: "Districts >70%",
          watch: jjmRows.filter((row) => row.coverage_pct >= 50 && row.coverage_pct < 70).length,
          watchLabel: "Districts 50–70%",
          critical: jjmRows.filter((row) => row.coverage_pct < 50).length,
          criticalLabel: "Districts <50%",
        },
        columns: [
          { key: "district", label: "District", type: "text" },
          { key: "population", label: "Population", type: "text" },
          { key: "coverage_pct", label: "Tap Water Coverage", type: "progress" },
          { key: "status", label: "Status", type: "status" },
        ],
        rows: jjmRows,
        row_count: jjmRows.length,
      },
      {
        id: "pmksy",
        label: "PMKSY",
        icon: "🌾",
        source: "rajas.rajasthan.gov.in",
        source_url: agg?.pmksy_districts?.[0]?.source_url || "https://rajas.rajasthan.gov.in/",
        description: "District irrigation coverage from Rajasthan Agriculture Statistics",
        live: true,
        status: "ok",
        status_label: "Live data",
        scraped_at: agg?.pmksy_districts?.[0]?.scraped_at || agg?.scraped_at,
        verified_label: "Verified Official Data",
        report_label: agg?.pmksy_districts?.[0]?.report_label || "2022-23 Annual Report",
        note: "Coverage is computed as net irrigated area divided by net area sown for each district.",
        summary: {
          primary: agg?.pmksy_districts?.length ? `${Number(agg.pmksy_districts[0]?.state_average || 0).toFixed(1)}%` : "—",
          primaryLabel: "State Average Irrigation Coverage",
          good: pmksyRows.filter((row) => row.coverage_pct >= 65).length,
          goodLabel: "Districts >65%",
          watch: pmksyRows.filter((row) => row.coverage_pct >= 40 && row.coverage_pct < 65).length,
          watchLabel: "Districts 40–65%",
          critical: pmksyRows.filter((row) => row.coverage_pct < 40).length,
          criticalLabel: "Districts <40%",
        },
        columns: [
          { key: "district", label: "District", type: "text" },
          { key: "net_area_sown", label: "Net Area Sown (Lakh Ha)", type: "text" },
          { key: "net_irrigated_area", label: "Net Irrigated Area (Lakh Ha)", type: "text" },
          { key: "coverage_pct", label: "Irrigation Coverage", type: "progress" },
          { key: "status", label: "Status", type: "status" },
        ],
        rows: pmksyRows,
        row_count: pmksyRows.length,
      },
    ];
  }, [agg]);

  const dashboards = schemeDashboards.length ? schemeDashboards : fallbackDashboards;

  useEffect(() => {
    if (!dashboards.some((item) => item.id === selectedScheme)) {
      setSelectedScheme(dashboards[0]?.id || "jal_shakti");
    }
  }, [dashboards, selectedScheme]);

  useEffect(() => {
    setDistSearch("");
    setSortBy("coverage_desc");
  }, [selectedScheme]);

  const selected = dashboards.find((item) => item.id === selectedScheme) || dashboards[0];
  if (!selected) return <EmptyState onScrape={onScrapeAll}/>;

  const rows = Array.isArray(selected.rows) ? selected.rows : [];
  const searchedRows = rows.filter((row) => {
    const name = String(row.district || row.name || "").toLowerCase();
    return !distSearch || name.includes(distSearch.toLowerCase());
  });
  const visibleRows = [...searchedRows].sort((a, b) => {
    if (sortBy === "coverage_asc") return (a.coverage_pct || 0) - (b.coverage_pct || 0);
    if (sortBy === "name") return String(a.district || "").localeCompare(String(b.district || ""));
    return (b.coverage_pct || 0) - (a.coverage_pct || 0);
  });

  const renderStatusPill = (row) => {
    const tone = row.status_tone || (row.coverage_pct >= 70 ? "good" : row.coverage_pct >= 50 ? "watch" : "critical");
    const styles = tone === "good"
      ? { bg:"#d1fae5", color:"#047857", text:"✓ On track" }
      : tone === "watch"
      ? { bg:"#fef9c3", color:"#ca8a04", text:"⚡ Needs push" }
      : { bg:"#fee2e2", color:"#dc2626", text:"⚠ Critical" };
    return (
      <span style={{
        display:"inline-flex", alignItems:"center", justifyContent:"center",
        borderRadius:999, padding:"7px 14px", fontWeight:700, fontSize:12,
        background:styles.bg, color:styles.color, minWidth:112
      }}>
        {row.status || styles.text}
      </span>
    );
  };

  const cardColor = (schemeId) => {
    if (schemeId === "jal_shakti") return "#0ea5e9";
    if (schemeId === "pmksy") return "#16a34a";
    if (schemeId === "sbmg") return "#f59e0b";
    if (schemeId === "scholarship") return "#8b5cf6";
    if (schemeId === "pmjdy") return "#0ea5e9";
    if (schemeId === "pmgdisha") return "#6366f1";
    if (schemeId === "saubhagya") return "#f59e0b";
    if (schemeId === "mgnrega_raj") return "#16a34a";
    if (schemeId === "pmfby") return "#84cc16";
    if (schemeId === "pmayg") return "#f97316";
    return "#f97316";
  };

  const accent = cardColor(selected.id);
  const titleAccent = selected.id === "pmksy" ? "#6d28d9" : accent;

  return (
    <div className="fadeup">
      <div style={{ marginBottom:24 }}>
        <div style={{ display:"flex", alignItems:"center", gap:4, marginBottom:4, flexWrap:"wrap" }}>
          <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
            District Schemes — <span style={{ color:"#f97316" }}>Real Data</span>
          </h2>
          <InfoTip text="Each card points to an official public source. Where the source exposes scrapeable metrics, the dashboard renders live figures; where it does not, the UI shows the current source limitation instead of inventing data."/>
        </div>
        <p style={{ color:"#64748b", fontSize:13, marginBottom:16 }}>
          Select a district or scheme-level dashboard to open its latest official view.
        </p>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))", gap:14 }}>
          {dashboards.map((option) => {
            const active = option.id === selected.id;
            const color = cardColor(option.id);
            return (
              <button
                key={option.id}
                onClick={() => setSelectedScheme(option.id)}
                style={{
                  textAlign:"left",
                  background: active ? `${color}12` : "white",
                  border:`1.5px solid ${active ? color : "#e2e8f0"}`,
                  borderRadius:20,
                  padding:"18px 20px",
                  boxShadow: active ? `0 10px 30px ${color}16` : "0 6px 18px rgba(15,23,42,0.04)",
                }}
              >
                <div style={{ display:"flex", justifyContent:"space-between", gap:12, alignItems:"flex-start", marginBottom:12 }}>
                  <div style={{ display:"flex", gap:12 }}>
                    <div style={{
                      width:44, height:44, borderRadius:14, background:`${color}16`,
                      display:"flex", alignItems:"center", justifyContent:"center", fontSize:22
                    }}>
                      {option.icon}
                    </div>
                    <div>
                      <div style={{ fontWeight:900, fontSize:15, color:"#0f172a" }}>{option.label}</div>
                      <div style={{ fontSize:12, color:"#94a3b8" }}>{option.source}</div>
                    </div>
                  </div>
                  <div style={{
                    background: "#16a34a",
                    color: "white",
                    borderRadius:999, padding:"5px 12px", fontSize:11, fontWeight:800, whiteSpace:"nowrap"
                  }}>
                    {option.row_count || 0} districts
                  </div>
                </div>
                <div style={{ fontSize:13, lineHeight:1.55, color:"#475569" }}>{option.description}</div>
              </button>
            );
          })}
        </div>
      </div>

      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", gap:16, flexWrap:"wrap", marginBottom:8 }}>
        <div>
          <div style={{ display:"flex", alignItems:"center", gap:4, flexWrap:"wrap", marginBottom:4 }}>
            <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
              {selected.row_count ? `District ${selected.label} Coverage` : selected.label} —{" "}
              <span style={{ color:titleAccent }}>
                {selected.id === "pmksy" ? "Rajasthan Agriculture Stats" : selected.live ? "Live dashboard" : "Official source status"}
              </span>
            </h2>
            <InfoTip text={selected.note || "Official source data for the selected scheme."}/>
          </div>
          <p style={{ color:"#6b7280", fontSize:13, margin:0 }}>
            {selected.description}
            {selected.row_count ? ` · ${selected.row_count} districts` : ""}
          </p>
          {selected.verified_label && (
            <div style={{
              display:"inline-flex", alignItems:"center", gap:8, marginTop:12,
              background:selected.id === "pmksy" ? "#fff7ed" : "#f8fafc",
              border:`1px solid ${selected.id === "pmksy" ? "#fdba74" : "#e2e8f0"}`,
              color:selected.id === "pmksy" ? "#c2410c" : "#334155",
              borderRadius:999, padding:"7px 14px", fontSize:12.5, fontWeight:800
            }}>
              <span style={{ width:8, height:8, borderRadius:"50%", background:selected.id === "pmksy" ? "#f59e0b" : accent, display:"inline-block" }}/>
              {selected.verified_label}
            </div>
          )}
          {selected.report_label && (
            <div style={{ fontSize:12, color:"#94a3b8", marginTop:10 }}>
              Source: {selected.report_label}
            </div>
          )}
        </div>

        <div style={{ display:"flex", alignItems:"center", gap:10, flexWrap:"wrap" }}>
          <div style={{
            display:"inline-flex", alignItems:"center", gap:6,
            background:selected.live ? "#ecfdf5" : "#fffbeb",
            border:`1px solid ${selected.live ? "#86efac" : "#fde68a"}`,
            borderRadius:10, padding:"8px 12px", color:selected.live ? "#15803d" : "#92400e",
            fontWeight:700, fontSize:12
          }}>
            <span style={{
              width:9, height:9, borderRadius:"50%", background:selected.live ? "#22c55e" : "#f59e0b",
              boxShadow:selected.live ? "0 0 0 3px #dcfce7" : "none"
            }}/>
            {selected.status_label || (selected.live ? "Live data" : "Source limited")}
          </div>
          {selected.scraped_at && <span style={{ fontSize:12, color:"#94a3b8" }}>{timeAgo(selected.scraped_at)}</span>}
        </div>
      </div>

      <p style={{ color:"#94a3b8", fontSize:12, marginBottom:22 }}>
        Source: {selected.source}
        {selected.note ? ` · ${selected.note}` : ""}
      </p>

      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(220px,1fr))", gap:14, marginBottom:24 }}>
        {[
          { value:selected.summary?.primary || "—", label:selected.summary?.primaryLabel || "Primary metric", bg:"#eff6ff", border:"#bfdbfe", numC:"#4338ca", txtC:"#3730a3" },
          { value:selected.summary?.good ?? 0, label:selected.summary?.goodLabel || "Healthy", bg:"#f0fdf4", border:"#bbf7d0", numC:"#16a34a", txtC:"#166534" },
          { value:selected.summary?.watch ?? 0, label:selected.summary?.watchLabel || "Watch", bg:"#fffbeb", border:"#fde68a", numC:"#ea580c", txtC:"#9a3412" },
          { value:selected.summary?.critical ?? 0, label:selected.summary?.criticalLabel || "Critical", bg:"#fff1f2", border:"#fecdd3", numC:"#e11d48", txtC:"#9f1239" },
        ].map((item, index) => (
          <div key={index} style={{ background:item.bg, border:`1.5px solid ${item.border}`, borderRadius:16, padding:"18px 22px" }}>
            <div style={{ fontSize:index===0 ? 32 : 46, lineHeight:1, fontWeight:900, color:item.numC, marginBottom:8 }}>
              {item.value}
            </div>
            <div style={{ fontSize:13, fontWeight:700, color:item.txtC }}>{item.label}</div>
          </div>
        ))}
      </div>

      {selected.row_count > 0 ? (
        <>
          <div style={{ display:"flex", gap:12, marginBottom:18, flexWrap:"wrap", alignItems:"center" }}>
            <div style={{ position:"relative", flex:1, minWidth:220 }}>
              <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", fontSize:15 }}>🔍</span>
              <input
                value={distSearch}
                onChange={(e) => setDistSearch(e.target.value)}
                placeholder="Search district…"
                style={{
                  width:"100%", padding:"10px 12px 10px 38px", border:"1px solid #e2e8f0",
                  borderRadius:11, fontSize:13, background:"white"
                }}
              />
            </div>
            <select
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value)}
              style={{ padding:"10px 14px", border:"1px solid #e2e8f0", borderRadius:11, fontSize:13, background:"white" }}
            >
              <option value="coverage_desc">Coverage: High → Low</option>
              <option value="coverage_asc">Coverage: Low → High</option>
              <option value="name">District: A – Z</option>
            </select>
            <span style={{ fontSize:12, color:"#94a3b8" }}>{visibleRows.length} of {rows.length} districts</span>
          </div>

          <div style={{ background:"white", borderRadius:18, border:"1px solid #e5e7eb", overflow:"hidden", boxShadow:"0 6px 18px rgba(15,23,42,0.04)" }}>
            <div style={{
              display:"grid",
              gridTemplateColumns:`repeat(${selected.columns.length}, minmax(0, 1fr))`,
              padding:"14px 20px", background:"#f8fafc", borderBottom:"1px solid #e5e7eb", gap:12
            }}>
              {selected.columns.map((column) => (
                <div key={column.key} style={{ fontSize:11, fontWeight:800, color:"#94a3b8", letterSpacing:"0.05em", textTransform:"uppercase" }}>
                  {column.label}
                </div>
              ))}
            </div>

            {visibleRows.map((row, index) => (
              <div
                key={`${row.district}-${index}`}
                style={{
                  display:"grid",
                  gridTemplateColumns:`repeat(${selected.columns.length}, minmax(0, 1fr))`,
                  padding:"15px 20px", gap:12, alignItems:"center",
                  borderBottom:"1px solid #f1f5f9", background:index % 2 === 0 ? "white" : "#fcfcfd"
                }}
              >
                {selected.columns.map((column, colIndex) => {
                  if (column.type === "progress") {
                    const pct = Math.max(0, Math.min(Number(row[column.key] || 0), 100));
                    const tone = row.status_tone || (pct >= 70 ? "good" : pct >= 50 ? "watch" : "critical");
                    const barColor = tone === "good" ? "#16a34a" : tone === "watch" ? "#f97316" : "#ef4444";
                    return (
                      <div key={column.key}>
                        <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                          <div style={{ flex:1, height:8, background:"#dbe4f0", borderRadius:999, overflow:"hidden" }}>
                            <div style={{ width:`${pct}%`, height:"100%", background:barColor, borderRadius:999, transition:"width 0.35s ease" }}/>
                          </div>
                          <span style={{ minWidth:56, textAlign:"right", fontWeight:800, color:barColor, fontSize:13 }}>{pct}%</span>
                        </div>
                      </div>
                    );
                  }
                  if (column.type === "status") {
                    return <div key={column.key}>{renderStatusPill(row)}</div>;
                  }
                  if (colIndex === 0) {
                    return (
                      <div key={column.key} style={{ display:"flex", alignItems:"center", gap:10 }}>
                        <span style={{
                          width:10, height:10, borderRadius:"50%",
                          background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444",
                          flexShrink:0
                        }}/>
                        <span style={{ fontWeight:800, fontSize:14, color:"#0f172a" }}>{row[column.key]}</span>
                      </div>
                    );
                  }
                  return <div key={column.key} style={{ fontSize:13, color:"#475569" }}>{row[column.key] || "—"}</div>;
                })}
              </div>
            ))}

            <div style={{
              display:"flex", justifyContent:"space-between", gap:10, padding:"12px 20px",
              background:"#f8fafc", color:"#94a3b8", fontSize:11
            }}>
              <span>{selected.live ? "Live feed" : "Official source"} · {selected.source}</span>
              {selected.scraped_at && <span>Fetched {timeAgo(selected.scraped_at)}</span>}
            </div>
          </div>
        </>
      ) : (
        <div style={{
          background:"white", borderRadius:18, border:"1px solid #e5e7eb", padding:"32px 28px",
          boxShadow:"0 6px 18px rgba(15,23,42,0.04)"
        }}>
          <div style={{ display:"flex", alignItems:"flex-start", justifyContent:"space-between", gap:16, flexWrap:"wrap" }}>
            <div>
              <div style={{ fontSize:18, fontWeight:900, color:"#0f172a", marginBottom:8 }}>
                {selected.live ? "Latest official summary loaded" : "Official source limitation surfaced clearly"}
              </div>
              <p style={{ fontSize:14, color:"#475569", lineHeight:1.7, maxWidth:760, marginBottom:14 }}>
                {selected.note || "This scheme does not currently expose district rows in a stable public response, so the dashboard is showing the source status instead of manufacturing statistics."}
              </p>
              {selected.state_metrics && (
                <div style={{ display:"flex", gap:10, flexWrap:"wrap", marginBottom:12 }}>
                  {Object.entries(selected.state_metrics).slice(0, 6).map(([key, value]) => (
                    <span key={key} style={{
                      background:"#f8fafc", border:"1px solid #e2e8f0", borderRadius:999,
                      padding:"6px 12px", fontSize:12, color:"#475569"
                    }}>
                      <strong style={{ color:"#0f172a" }}>{key.replace(/_/g, " ")}:</strong> {value}
                    </span>
                  ))}
                </div>
              )}
              <a
                href={selected.source_url}
                target="_blank"
                rel="noreferrer"
                style={{
                  display:"inline-flex", alignItems:"center", gap:8,
                  background:accent, color:"white", borderRadius:999, padding:"10px 16px",
                  fontWeight:800, fontSize:13
                }}
              >
                Open official source
              </a>
            </div>
            <button
              onClick={onScrapeAll}
              style={{
                background:"#fff7ed", border:"1px solid #fdba74", color:"#c2410c",
                borderRadius:12, padding:"10px 14px", fontWeight:800, fontSize:13
              }}
            >
              Refresh all data
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Alerts Tab ────────────────────────────────────────────────────────────────
function AlertsTab({ agg, onScrapeAll }) {
  const [filter, setFilter] = useState("All");
  const alerts = Array.isArray(agg?.alerts) ? agg.alerts : [];
  if (!agg) return <EmptyState onScrape={onScrapeAll}/>;
  if (!alerts.length) {
    return (
      <div className="fadeup">
        <div style={{ background:"white", borderRadius:16, border:"2px dashed #e5e7eb", padding:52, textAlign:"center" }}>
          <div style={{ fontSize:44, marginBottom:12 }}>⚡</div>
          <div style={{ fontWeight:800, fontSize:20, color:"#0f172a", marginBottom:8 }}>No live alerts generated yet</div>
          <div style={{ color:"#64748b", marginBottom:20, fontSize:14 }}>
            Alerts appear after the backend detects meaningful patterns from the current scrape.
          </div>
          <button onClick={onScrapeAll} style={{ background:"#f97316", color:"white",
            borderRadius:12, padding:"12px 28px", fontWeight:800, fontSize:14,
            border:"none", cursor:"pointer" }}>
            ⚡ Refresh Alert Sources
          </button>
        </div>
      </div>
    );
  }
  const severityRank = { Critical: 0, Warning: 1, Action: 2, Insight: 3 };
  const sortedAlerts = [...alerts].sort((a, b) => {
    const severityDelta = (severityRank[a.severity] ?? 9) - (severityRank[b.severity] ?? 9);
    if (severityDelta !== 0) return severityDelta;
    return String(b.date || "").localeCompare(String(a.date || ""));
  });
  const filtered = filter==="All" ? sortedAlerts : sortedAlerts.filter(a=>a.severity===filter);
  const stats = {
    Critical: alerts.filter((a) => a.severity === "Critical").length,
    Warning: alerts.filter((a) => a.severity === "Warning").length,
    Action: alerts.filter((a) => a.severity === "Action").length,
    Insight: alerts.filter((a) => a.severity === "Insight").length,
  };
  return (
    <div className="fadeup">
      <div style={{ display:"flex", alignItems:"center", marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
        Intelligence Alerts — <span style={{ color:"#f97316" }}>Source-Cited</span>
      </h2>
        <InfoTip text="Alerts are auto-generated by the backend from scraped data patterns — not hardcoded. Each alert cites actual scheme counts and categories found during scraping."/>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:20 }}>
        Every alert generated from live scraped data · {alerts.length} alerts total
      </p>
      <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:10, marginBottom:18 }}>
        {[
          { label:"Critical", value:stats.Critical, bg:"#fff1f2", border:"#fecdd3", color:"#dc2626" },
          { label:"Warning", value:stats.Warning, bg:"#fff7ed", border:"#fdba74", color:"#ea580c" },
          { label:"Action", value:stats.Action, bg:"#f0fdf4", border:"#bbf7d0", color:"#15803d" },
          { label:"Insight", value:stats.Insight, bg:"#eff6ff", border:"#bfdbfe", color:"#2563eb" },
        ].map((item) => (
          <div key={item.label} style={{ background:item.bg, border:`1px solid ${item.border}`, borderRadius:12, padding:"12px 14px" }}>
            <div style={{ fontSize:22, fontWeight:900, color:item.color, lineHeight:1 }}>{item.value}</div>
            <div style={{ fontSize:11, color:"#6b7280", marginTop:4 }}>{item.label} alerts</div>
          </div>
        ))}
      </div>
      <div style={{ display:"flex", gap:8, marginBottom:22, flexWrap:"wrap" }}>
        {["All","Critical","Warning","Action","Insight"].map(f=>(
          <button key={f} onClick={()=>setFilter(f)} style={{
            background:filter===f?"#1f2937":"white", color:filter===f?"white":"#374151",
            border:`1.5px solid ${filter===f?"#1f2937":"#e5e7eb"}`,
            borderRadius:20, padding:"7px 18px", fontSize:13, fontWeight:600, cursor:"pointer" }}>{f}</button>
        ))}
      </div>
      <div style={{ display:"flex", flexDirection:"column", gap:16 }}>
        {filtered.map((alert,i)=>(
          <div key={alert.id||i} style={{ background:"white", borderRadius:14,
            border:`1px solid #e5e7eb`, borderLeft:`4px solid ${alert.borderColor}`,
            padding:20, boxShadow:"0 1px 4px rgba(0,0,0,0.05)" }}>
            <div style={{ display:"flex", alignItems:"flex-start", gap:14 }}>
              <div style={{ width:44, height:44, borderRadius:10, background:`${alert.borderColor}15`,
                display:"flex", alignItems:"center", justifyContent:"center", fontSize:22, flexShrink:0 }}>
                {alert.icon}
              </div>
              <div style={{ flex:1 }}>
                <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:8, flexWrap:"wrap" }}>
                  <span style={{ background:`${alert.borderColor}15`, color:alert.borderColor,
                    border:`1px solid ${alert.borderColor}25`, borderRadius:4, padding:"2px 8px",
                    fontSize:11, fontWeight:800, letterSpacing:"0.07em" }}>{alert.type}</span>
                  <span style={{ fontWeight:700, fontSize:15, color:"#1f2937" }}>{alert.title}</span>
                  <span style={{ marginLeft:"auto", fontSize:12, color:"#9ca3af" }}>{alert.date ? timeAgo(alert.date) : "Unknown time"}</span>
                </div>
                <p style={{ fontSize:14, color:"#374151", lineHeight:1.6, marginBottom:12 }}>{alert.body}</p>
                <div style={{ display:"flex", gap:8, flexWrap:"wrap", marginBottom:12 }}>
                  {(alert.tags||[]).map((tag,j)=>(
                    <span key={j} style={{ background:j===0?`${alert.borderColor}15`:"#f3f4f6",
                      color:j===0?alert.borderColor:"#6b7280",
                      border:`1px solid ${j===0?alert.borderColor+"30":"#e5e7eb"}`,
                      borderRadius:20, padding:"4px 12px", fontSize:12, fontWeight:500 }}>{tag}</span>
                  ))}
                </div>
                <div style={{ fontSize:12, color:"#9ca3af", display:"flex", gap:6 }}>
                  <span>📚</span><span style={{ fontStyle:"italic" }}>{alert.source}</span>
                </div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── Budget Data Tab ───────────────────────────────────────────────────────────
function BudgetDataTab({ budget, budgetLoading, onRefresh }) {
  if(!budget && !budgetLoading) return <EmptyState onScrape={onRefresh}/>;
  const b=budget||{}, d=b.display||{}, sp=b.sparklines||{}, bm=b.scrape_meta||{};
  const budgetYear = b.year || "Current";
  const trendYears = b.sparkline_meta?.total_years || 0;
  const trendLabel = trendYears ? `${trendYears}-Year Trend` : "Trend View";
  const sparkYears = Array.isArray(b.sparkline_meta?.years) ? b.sparkline_meta.years : [];
  const sourceLinks = [
    b.source_url ? { label:"Primary Budget Source", url:b.source_url, color:"#f97316" } : null,
    { label:"Finance Rajasthan", url:"https://finance.rajasthan.gov.in/", color:"#0f766e" },
    { label:"PRS Budget Analysis", url:"https://prsindia.org/budgets/states/rajasthan-budget-analysis-2025-26", color:"#1d4ed8" },
    { label:"JJM MIS", url:"https://ejalshakti.gov.in/jjmreport/JJMIndia.aspx", color:"#0891b2" },
  ].filter(Boolean);
  const Spark=({data=[],color="#f97316",years=[],formatValue=(v)=>String(v)})=>{
    const [activeIndex, setActiveIndex] = useState(Math.max(0, data.length - 1));
    if(!data||data.length<2) return <div style={{ width:90, height:36, background:`${color}08`, borderRadius:6 }}/>;
    const W=90,H=36,PAD=3;
    const min=Math.min(...data),max=Math.max(...data),rng=(max-min)||1;
    const xs=data.map((_,i)=>(i/(data.length-1))*W);
    const ys=data.map(v=>H-PAD-((v-min)/rng)*(H-PAD*2));
    const lp=xs.map((x,i)=>`${x},${ys[i]}`).join(" ");
    const ap=`0,${H} `+lp+` ${W},${H}`;
    const gid=`bt${color.replace(/#/g,"")}`;
    const currentIndex = Math.min(activeIndex, data.length - 1);
    const activeYear = years[currentIndex] || `Point ${currentIndex + 1}`;
    const activeValue = Number.isFinite(Number(data[currentIndex])) ? formatValue(Number(data[currentIndex])) : "Unavailable";
    const setNearestIndex = (clientX, rect) => {
      const relativeX = Math.max(0, Math.min(W, ((clientX - rect.left) / rect.width) * W));
      const nearest = xs.reduce((bestIndex, x, index) =>
        Math.abs(x - relativeX) < Math.abs(xs[bestIndex] - relativeX) ? index : bestIndex, 0);
      setActiveIndex(nearest);
    };
    return (
      <div style={{ display:"flex", flexDirection:"column", alignItems:"flex-end", gap:4 }}>
        <svg
          width={W}
          height={H}
          viewBox={`0 0 ${W} ${H}`}
          style={{ overflow:"visible", touchAction:"none" }}
          onMouseMove={(event) => setNearestIndex(event.clientX, event.currentTarget.getBoundingClientRect())}
          onTouchStart={(event) => setNearestIndex(event.touches[0].clientX, event.currentTarget.getBoundingClientRect())}
          onTouchMove={(event) => setNearestIndex(event.touches[0].clientX, event.currentTarget.getBoundingClientRect())}
        >
          <defs>
            <linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity="0.2"/>
              <stop offset="100%" stopColor={color} stopOpacity="0"/>
            </linearGradient>
          </defs>
          <polygon points={ap} fill={`url(#${gid})`}/>
          <polyline points={lp} fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round"/>
          <line x1={xs[currentIndex]} x2={xs[currentIndex]} y1="0" y2={H} stroke={color} strokeOpacity="0.18" strokeDasharray="2 2" />
          {xs.map((x, index) => (
            <circle
              key={index}
              cx={x}
              cy={ys[index]}
              r={index === currentIndex ? "3.5" : "2.1"}
              fill={index === currentIndex ? color : "#ffffff"}
              stroke={color}
              strokeWidth={index === currentIndex ? "1.6" : "1.1"}
            />
          ))}
        </svg>
        <div style={{
          background:`${color}10`,
          color:color,
          border:`1px solid ${color}25`,
          borderRadius:8,
          padding:"3px 7px",
          fontSize:10,
          fontWeight:800,
          whiteSpace:"nowrap"
        }}>
          {activeYear} · {activeValue}
        </div>
      </div>
    );
  };
  const ROWS=[
    {label:"Capital Outlay", value:() => d.capital_outlay || formatCrDisplay(b.capital_outlay_cr), sparkKey:"capital_outlay_cr", color:"#10b981", sparkFormatter:(value)=>formatCrDisplay(value)},
    {label:"Health Budget", value:() => d.health || formatCrDisplay(b.health_cr), sparkKey:"health_cr", color:"#ef4444", sparkFormatter:(value)=>formatCrDisplay(value)},
    {label:"Education Share", value:() => d.education_pct || (Number.isFinite(Number(b.education_pct)) ? `${b.education_pct}% share` : "Unavailable"), sparkKey:"education_pct", color:"#3b82f6", sparkFormatter:(value)=>`${value.toFixed(1)}%`},
    {label:"Social Security", value:() => d.social_security || formatCrDisplay(b.social_security_cr, { plus: true }), sparkKey:"social_security_cr", color:"#8b5cf6", sparkFormatter:(value)=>formatCrDisplay(value)},
    {label:"Fiscal Deficit", value:() => d.fiscal_deficit_pct || (Number.isFinite(Number(b.fiscal_deficit_pct_gsdp)) ? `${b.fiscal_deficit_pct_gsdp}% GSDP` : "Unavailable"), sparkKey:"fiscal_deficit_pct", color:"#f59e0b", sparkFormatter:(value)=>`${value.toFixed(2)}%`},
    {label:"JJM Coverage", value:() => d.jjm_coverage || formatPercentFixedDisplay(b.jjm_coverage_pct, 2), sparkKey:"jjm_coverage_pct", color:"#0ea5e9", sparkFormatter:(value)=>`${value.toFixed(2)}%`},
  ];
  const statCards = [
    {label:"Total Expenditure", val:d.total_expenditure || formatCrDisplay(b.total_expenditure_cr), bg:"#fff7ed", border:"#fed7aa", color:"#c2410c"},
    {label:"Capital Outlay", val:d.capital_outlay || formatCrDisplay(b.capital_outlay_cr), bg:"#f0fdf4", border:"#bbf7d0", color:"#15803d"},
    {label:"Fiscal Deficit", val:d.fiscal_deficit_pct || (Number.isFinite(Number(b.fiscal_deficit_pct_gsdp)) ? `${b.fiscal_deficit_pct_gsdp}% GSDP` : "Unavailable"), bg:"#fffbeb", border:"#fde68a", color:"#b45309"},
    {label:"Health Allocation", val:d.health || formatCrDisplay(b.health_cr), bg:"#fff1f2", border:"#fecdd3", color:"#be123c"},
    {label:"JJM Coverage", val:d.jjm_coverage || formatPercentFixedDisplay(b.jjm_coverage_pct, 2), bg:"#eff6ff", border:"#bfdbfe", color:"#1d4ed8"},
    {label:"Social Security", val:d.social_security || formatCrDisplay(b.social_security_cr, { plus: true }), bg:"#faf5ff", border:"#e9d5ff", color:"#7c3aed"},
  ];
  const highlights = [
    Number.isFinite(Number(b.economy_target_bn_usd)) ? {icon:"🎯",text:`Target: $${b.economy_target_bn_usd} billion economy by 2030`} : null,
    {icon:"🌱",text:b.green_budget!==false ? "Green budget framing is present in the current payload" : "Sustainability framing is not confirmed in the current payload"},
    Number.isFinite(Number(b.capital_outlay_cr)) ? {icon:"📈",text:`Capital outlay reported at ${formatCrDisplay(b.capital_outlay_cr)}`} : null,
    Number.isFinite(Number(b.health_pct)) ? {icon:"💊",text:`Health allocation is ${b.health_pct}% of the total budget`} : null,
    Number.isFinite(Number(b.education_pct)) ? {icon:"🎓",text:`Education share is ${b.education_pct}% in the latest budget dataset`} : null,
    Number.isFinite(Number(b.jjm_coverage_pct)) ? {icon:"💧",text:`JJM coverage stands at ${formatPercentFixedDisplay(b.jjm_coverage_pct, 2)} in the current backend response`} : null,
    bm.note ? {icon:"🛰️",text:bm.note} : null,
    d.gsdp ? {icon:"₹",text:`Estimated GSDP shown in the dataset: ${d.gsdp}`} : null,
  ].filter(Boolean);
  return (
    <div className="fadeup">
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900 }}>Budget Data — <span style={{ color:"#f97316" }}>{budgetYear}</span></h2>
        <div style={{ display:"flex", alignItems:"center", gap:10 }}>
          {bm.note&&<div style={{ fontSize:12, color:bm.live_sources>0?"#166534":"#4b7ab5",
            background:bm.live_sources>0?"#f0fdf4":"#eff6ff",
            border:`1px solid ${bm.live_sources>0?"#bbf7d0":"#bfdbfe"}`,
            borderRadius:6, padding:"4px 10px", fontWeight:600 }}>
            {bm.live_sources>0?`✓ ${bm.live_sources} live sources`:"📚 Verified fallback"}
          </div>}
          <button onClick={onRefresh} style={{ background:"#f97316", color:"white", border:"none",
            borderRadius:8, padding:"8px 16px", fontSize:13, fontWeight:700, cursor:"pointer" }}>
            ↺ Refresh Budget Data
          </button>
        </div>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:22 }}>
        Source: {b.source || "Unavailable"}{b.scraped_at ? ` · Updated ${timeAgo(b.scraped_at)}` : ""}
      </p>
      <div style={{ display:"grid", gridTemplateColumns:"repeat(3,1fr)", gap:14, marginBottom:24 }}>
        {statCards.map((item,i)=>(
          <div key={i} style={{ background:item.bg, border:`1.5px solid ${item.border}`, borderRadius:14, padding:"18px 20px" }}>
            <div style={{ fontSize:11, fontWeight:700, color:item.color, letterSpacing:"0.08em", marginBottom:10, opacity:0.8 }}>{item.label.toUpperCase()}</div>
            <div style={{ fontSize:24, fontWeight:900, color:item.color }}>{item.val}</div>
          </div>
        ))}
      </div>
      <div style={{ background:"white", borderRadius:14, border:"1px solid #e5e7eb", overflow:"hidden", marginBottom:22 }}>
        <div style={{ padding:"16px 20px", borderBottom:"1px solid #f3f4f6", fontWeight:800, fontSize:15 }}>
          {trendLabel}
          <span style={{ fontSize:12, color:"#9ca3af", fontWeight:400, marginLeft:8 }}>from official budget documents</span>
        </div>
        {budgetLoading?<div style={{ padding:40, textAlign:"center", color:"#9ca3af" }}>Loading…</div>
        :ROWS.map((row,i)=>{
          const sparkData=sp[row.sparkKey]||[];
          return (
            <div key={i} style={{ display:"grid", gridTemplateColumns:"2fr 1fr 1fr 1fr",
              padding:"14px 20px", borderBottom:"1px solid #f9fafb", alignItems:"center",
              background:i%2===0?"white":"#fafafa" }}>
              <div style={{ fontWeight:600, fontSize:14, color:"#374151" }}>{row.label}</div>
              <div style={{ fontWeight:800, fontSize:16, color:row.color }}>
                {row.value()}
              </div>
              <div style={{ fontSize:12, color:"#9ca3af" }}>Budget {budgetYear}</div>
              <div><Spark data={sparkData} color={row.color} years={sparkYears} formatValue={row.sparkFormatter}/></div>
            </div>
          );
        })}
      </div>
      <div style={{ background:"white", borderRadius:14, border:"1px solid #e5e7eb", padding:20 }}>
        <div style={{ fontWeight:800, fontSize:15, marginBottom:14 }}>Key Budget Highlights {budgetYear}</div>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(2,1fr)", gap:10 }}>
          {highlights.map((h,i)=>(
            <div key={i} style={{ display:"flex", gap:12, padding:"12px 14px",
              background:"#f9fafb", borderRadius:10, border:"1px solid #f3f4f6", alignItems:"flex-start" }}>
              <span style={{ fontSize:20, flexShrink:0 }}>{h.icon}</span>
              <span style={{ fontSize:13, color:"#374151", lineHeight:1.5 }}>{h.text}</span>
            </div>
          ))}
        </div>
        <div style={{ display:"flex", gap:10, flexWrap:"wrap", marginTop:16 }}>
          {sourceLinks.map((link, i)=>(
            <a key={i} href={link.url} target="_blank" rel="noreferrer" style={{
              display:"inline-flex", alignItems:"center", gap:6,
              background:`${link.color}12`, color:link.color,
              border:`1px solid ${link.color}30`, borderRadius:999,
              padding:"8px 14px", fontWeight:700, fontSize:12, textDecoration:"none"
            }}>
              {link.label} ↗
            </a>
          ))}
        </div>
        <div style={{ marginTop:14, fontSize:12, color:"#9ca3af", textAlign:"right" }}>
          Source: {b.source || "Unavailable"}
        </div>
      </div>
    </div>
  );
}

// ── PMFBY Tab ──────────────────────────────────────────────────────────────
function PmfbyTab({ schemeDashboards, agg, onScrapeAll }) {
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("settle_desc");

  const dashboard = schemeDashboards.find(d => d.id === "pmfby") || null;

  if (!dashboard && !agg) return <EmptyState onScrape={onScrapeAll}/>;
  if (!dashboard) return (
    <div style={{ background:"white", borderRadius:16, border:"2px dashed #e5e7eb", padding:60, textAlign:"center" }}>
      <div style={{ fontSize:40, marginBottom:12 }}>🌾</div>
      <div style={{ fontWeight:800, fontSize:18, color:"#0f172a", marginBottom:8 }}>PMFBY data loading…</div>
      <div style={{ color:"#64748b", fontSize:13, marginBottom:20 }}>Click Refresh to fetch crop insurance data from pmfby.gov.in</div>
      <button onClick={onScrapeAll} style={{ background:"#84cc16", color:"white", borderRadius:12, padding:"12px 28px", fontWeight:800, fontSize:14, border:"none", cursor:"pointer" }}>⚡ Fetch PMFBY Data</button>
    </div>
  );

  const { summary, rows = [], columns = [] } = dashboard;
  const totals = summary?.state_totals || {};

  const filtered = [...rows]
    .filter(r => !search || String(r.district || "").toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      if (sortBy === "settle_asc") return (a.settlement_pct || 0) - (b.settlement_pct || 0);
      if (sortBy === "name")       return String(a.district || "").localeCompare(String(b.district || ""));
      return (b.settlement_pct || 0) - (a.settlement_pct || 0);
    });

  const renderStatusPill = (row) => {
    const tone = row.status_tone;
    const styles = tone === "good"
      ? { bg:"#d1fae5", color:"#047857" }
      : tone === "watch"
      ? { bg:"#fef9c3", color:"#ca8a04" }
      : { bg:"#fee2e2", color:"#dc2626" };
    return (
      <span style={{ display:"inline-flex", alignItems:"center", justifyContent:"center",
        borderRadius:999, padding:"6px 14px", fontWeight:700, fontSize:12,
        background:styles.bg, color:styles.color, minWidth:108 }}>
        {row.status}
      </span>
    );
  };

  return (
    <div className="fadeup">
      <div style={{ display:"flex", alignItems:"center", gap:4, marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
          PMFBY (Rajasthan) — <span style={{ color:"#65a30d" }}>Crop Insurance</span>
        </h2>
        <InfoTip text="Pradhan Mantri Fasal Bima Yojana — district-wise crop insurance data from pmfby.gov.in public dashboard and PIB Annexure-1. Claim settlement rate = claims settled / claims filed × 100."/>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:4 }}>
        Source: {dashboard.source} · {dashboard.report_label}
      </p>
      <div style={{ display:"inline-flex", alignItems:"center", gap:8, marginBottom:20,
        background:"#f7fee7", border:"1px solid #d9f99d", color:"#3f6212",
        borderRadius:999, padding:"7px 14px", fontSize:12.5, fontWeight:800 }}>
        <span style={{ width:8, height:8, borderRadius:"50%", background:dashboard.live ? "#84cc16" : "#bef264", display:"inline-block" }}/>
        {dashboard.verified_label}
      </div>

      {/* KPI summary cards */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:14, marginBottom:24 }}>
        {[
          { value:summary?.primary || "—",   label:summary?.primaryLabel || "Avg Settlement Rate", bg:"#f7fee7", border:"#d9f99d", numC:"#4d7c0f", txtC:"#3f6212" },
          { value:summary?.good ?? 0,         label:summary?.goodLabel || "Districts ≥85%",         bg:"#f0fdf4", border:"#bbf7d0", numC:"#16a34a", txtC:"#166534" },
          { value:summary?.watch ?? 0,        label:summary?.watchLabel || "Districts 78–85%",      bg:"#fffbeb", border:"#fde68a", numC:"#ea580c", txtC:"#9a3412" },
          { value:summary?.critical ?? 0,     label:summary?.criticalLabel || "Critical",            bg:"#fff1f2", border:"#fecdd3", numC:"#e11d48", txtC:"#9f1239" },
        ].map((item, i) => (
          <div key={i} style={{ background:item.bg, border:`1.5px solid ${item.border}`, borderRadius:16, padding:"18px 22px" }}>
            <div style={{ fontSize:i===0?32:46, lineHeight:1, fontWeight:900, color:item.numC, marginBottom:8 }}>{item.value}</div>
            <div style={{ fontSize:13, fontWeight:700, color:item.txtC }}>{item.label}</div>
          </div>
        ))}
      </div>

      {/* State totals banner */}
      {totals.farmers_enrolled && (
        <div style={{ background:"linear-gradient(135deg,#f7fee7,#ecfccb)", border:"1.5px solid #d9f99d",
          borderRadius:14, padding:"14px 20px", marginBottom:22, display:"flex", flexWrap:"wrap", gap:20, alignItems:"center" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7 }}>
            <span style={{ fontSize:18 }}>🌾</span>
            <span style={{ fontWeight:800, fontSize:14, color:"#1a2e05" }}>Rajasthan State Totals ({totals.year})</span>
          </div>
          {[
            { label:"Farmers Enrolled",  val:totals.farmers_enrolled,  color:"#4d7c0f" },
            { label:"Area Insured",      val:totals.area_insured,      color:"#0369a1" },
            { label:"Premium Collected", val:totals.premium_collected, color:"#b45309" },
            { label:"Claims Paid",       val:totals.claims_paid,       color:"#dc2626" },
            { label:"Claim Ratio",       val:totals.claim_ratio,       color:"#7c3aed" },
            { label:"Settlement Rate",   val:totals.settlement_rate,   color:"#16a34a" },
          ].map((item, i) => (
            <div key={i} style={{ textAlign:"center" }}>
              <div style={{ fontSize:15, fontWeight:900, color:item.color }}>{item.val}</div>
              <div style={{ fontSize:10, color:"#3f6212", fontWeight:600 }}>{item.label}</div>
            </div>
          ))}
        </div>
      )}

      <p style={{ color:"#94a3b8", fontSize:12, marginBottom:18 }}>
        Source: {dashboard.source} · {dashboard.note}
      </p>

      {/* Search + sort */}
      <div style={{ display:"flex", gap:12, marginBottom:18, flexWrap:"wrap", alignItems:"center" }}>
        <div style={{ position:"relative", flex:1, minWidth:220 }}>
          <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", fontSize:15 }}>🔍</span>
          <input value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search district…"
            style={{ width:"100%", padding:"10px 12px 10px 38px", border:"1px solid #e2e8f0",
              borderRadius:11, fontSize:13, background:"white", boxSizing:"border-box" }}/>
        </div>
        <select value={sortBy} onChange={e => setSortBy(e.target.value)}
          style={{ padding:"10px 14px", border:"1px solid #e2e8f0", borderRadius:11, fontSize:13, background:"white" }}>
          <option value="settle_desc">Settlement: High → Low</option>
          <option value="settle_asc">Settlement: Low → High</option>
          <option value="name">District: A – Z</option>
        </select>
        <span style={{ fontSize:12, color:"#94a3b8" }}>{filtered.length} of {rows.length} districts</span>
      </div>

      {/* District table */}
      <div style={{ background:"white", borderRadius:18, border:"1px solid #e5e7eb", overflow:"hidden", boxShadow:"0 6px 18px rgba(15,23,42,0.04)" }}>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
          padding:"14px 20px", background:"#f8fafc", borderBottom:"1px solid #e5e7eb", gap:12 }}>
          {columns.map(col => (
            <div key={col.key} style={{ fontSize:11, fontWeight:800, color:"#94a3b8", letterSpacing:"0.05em", textTransform:"uppercase" }}>
              {col.label}
            </div>
          ))}
        </div>

        {filtered.map((row, idx) => (
          <div key={`${row.district}-${idx}`}
            style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
              padding:"15px 20px", gap:12, alignItems:"center",
              borderBottom:"1px solid #f1f5f9", background:idx % 2 === 0 ? "white" : "#fcfcfd" }}>

            <div style={{ display:"flex", alignItems:"center", gap:10 }}>
              <span style={{ width:10, height:10, borderRadius:"50%", flexShrink:0,
                background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}/>
              <span style={{ fontWeight:800, fontSize:14, color:"#0f172a" }}>{row.district}</span>
            </div>
            <div style={{ fontSize:13, color:"#4d7c0f", fontWeight:600 }}>{row.farmers}</div>
            <div style={{ fontSize:13, color:"#0369a1", fontWeight:600 }}>{row.area_insured}</div>
            <div style={{ fontSize:13, color:"#dc2626", fontWeight:600 }}>{row.claims_paid}</div>

            <div>
              <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                <div style={{ flex:1, height:8, background:"#dbe4f0", borderRadius:999, overflow:"hidden" }}>
                  <div style={{
                    width:`${Math.min(row.settlement_pct || 0, 100)}%`, height:"100%", borderRadius:999,
                    background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444",
                    transition:"width 0.35s ease"
                  }}/>
                </div>
                <span style={{ minWidth:40, textAlign:"right", fontWeight:800, fontSize:13,
                  color: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}>
                  {row.settlement_pct}%
                </span>
              </div>
            </div>
            <div>{renderStatusPill(row)}</div>
          </div>
        ))}

        <div style={{ display:"flex", justifyContent:"space-between", gap:10, padding:"12px 20px",
          background:"#f8fafc", color:"#94a3b8", fontSize:11 }}>
          <span>{dashboard.live ? "Live feed" : "Annual report data"} · {dashboard.source}</span>
          <span>Fetched {timeAgo(dashboard.scraped_at)}</span>
        </div>
      </div>

      <div style={{ display:"flex", gap:12, marginTop:18, flexWrap:"wrap" }}>
        {[
          { label:"PMFBY Dashboard",     url:"https://pmfby.gov.in/adminStatistics",  color:"#65a30d" },
          { label:"PMFBY Official Portal",url:"https://pmfby.gov.in/",               color:"#4d7c0f" },
          { label:"PIB Annexure-1",      url:"https://pib.gov.in/",                  color:"#0369a1" },
        ].map((link, i) => (
          <a key={i} href={link.url} target="_blank" rel="noreferrer" style={{
            display:"inline-flex", alignItems:"center", gap:6,
            background:`${link.color}12`, color:link.color,
            border:`1px solid ${link.color}30`, borderRadius:999,
            padding:"8px 16px", fontWeight:700, fontSize:12, textDecoration:"none"
          }}>
            {link.label} ↗
          </a>
        ))}
      </div>
    </div>
  );
}

// ── MGNREGA Tab ──────────────────────────────────────────────────────────────
function MgnregaTab({ schemeDashboards, agg, onScrapeAll }) {
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("emp_desc");

  const dashboard = schemeDashboards.find(d => d.id === "mgnrega_raj") || null;

  if (!dashboard && !agg) return <EmptyState onScrape={onScrapeAll}/>;
  if (!dashboard) return (
    <div style={{ background:"white", borderRadius:16, border:"2px dashed #e5e7eb", padding:60, textAlign:"center" }}>
      <div style={{ fontSize:40, marginBottom:12 }}>🏗️</div>
      <div style={{ fontWeight:800, fontSize:18, color:"#0f172a", marginBottom:8 }}>MGNREGA data loading…</div>
      <div style={{ color:"#64748b", fontSize:13, marginBottom:20 }}>Click Refresh to fetch employment data from nreganarep.nic.in</div>
      <button onClick={onScrapeAll} style={{ background:"#16a34a", color:"white", borderRadius:12, padding:"12px 28px", fontWeight:800, fontSize:14, border:"none", cursor:"pointer" }}>⚡ Fetch MGNREGA Data</button>
    </div>
  );

  const { summary, rows = [], columns = [] } = dashboard;
  const totals = summary?.state_totals || {};

  const filtered = [...rows]
    .filter(r => !search || String(r.district || "").toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      if (sortBy === "emp_asc") return (a.emp_pct || 0) - (b.emp_pct || 0);
      if (sortBy === "name")    return String(a.district || "").localeCompare(String(b.district || ""));
      return (b.emp_pct || 0) - (a.emp_pct || 0);
    });

  const renderStatusPill = (row) => {
    const tone = row.status_tone;
    const styles = tone === "good"
      ? { bg:"#d1fae5", color:"#047857" }
      : tone === "watch"
      ? { bg:"#fef9c3", color:"#ca8a04" }
      : { bg:"#fee2e2", color:"#dc2626" };
    return (
      <span style={{ display:"inline-flex", alignItems:"center", justifyContent:"center",
        borderRadius:999, padding:"6px 14px", fontWeight:700, fontSize:12,
        background:styles.bg, color:styles.color, minWidth:108 }}>
        {row.status}
      </span>
    );
  };

  return (
    <div className="fadeup">
      <div style={{ display:"flex", alignItems:"center", gap:4, marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
          MGNREGA (Rajasthan) — <span style={{ color:"#16a34a" }}>Rural Employment</span>
        </h2>
        <InfoTip text="MGNREGA district data from nreganarep.nic.in MIS — Rajasthan state report. Employment rate = households provided work / households demanded work × 100."/>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:4 }}>
        Source: {dashboard.source} · {dashboard.report_label}
      </p>
      <div style={{ display:"inline-flex", alignItems:"center", gap:8, marginBottom:20,
        background:"#f0fdf4", border:"1px solid #bbf7d0", color:"#166534",
        borderRadius:999, padding:"7px 14px", fontSize:12.5, fontWeight:800 }}>
        <span style={{ width:8, height:8, borderRadius:"50%", background:dashboard.live ? "#16a34a" : "#86efac", display:"inline-block" }}/>
        {dashboard.verified_label}
      </div>

      {/* KPI summary cards */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:14, marginBottom:24 }}>
        {[
          { value:summary?.primary || "—",   label:summary?.primaryLabel || "Avg Employment Rate", bg:"#f0fdf4", border:"#bbf7d0", numC:"#15803d", txtC:"#166534" },
          { value:summary?.good ?? 0,         label:summary?.goodLabel || "Districts ≥88%",         bg:"#f0fdf4", border:"#bbf7d0", numC:"#16a34a", txtC:"#166534" },
          { value:summary?.watch ?? 0,        label:summary?.watchLabel || "Districts 78–88%",      bg:"#fffbeb", border:"#fde68a", numC:"#ea580c", txtC:"#9a3412" },
          { value:summary?.critical ?? 0,     label:summary?.criticalLabel || "Critical",            bg:"#fff1f2", border:"#fecdd3", numC:"#e11d48", txtC:"#9f1239" },
        ].map((item, i) => (
          <div key={i} style={{ background:item.bg, border:`1.5px solid ${item.border}`, borderRadius:16, padding:"18px 22px" }}>
            <div style={{ fontSize:i===0?32:46, lineHeight:1, fontWeight:900, color:item.numC, marginBottom:8 }}>{item.value}</div>
            <div style={{ fontSize:13, fontWeight:700, color:item.txtC }}>{item.label}</div>
          </div>
        ))}
      </div>

      {/* State totals banner */}
      {totals.job_cards && (
        <div style={{ background:"linear-gradient(135deg,#f0fdf4,#dcfce7)", border:"1.5px solid #bbf7d0",
          borderRadius:14, padding:"14px 20px", marginBottom:22, display:"flex", flexWrap:"wrap", gap:20, alignItems:"center" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7 }}>
            <span style={{ fontSize:18 }}>🏗️</span>
            <span style={{ fontWeight:800, fontSize:14, color:"#14532d" }}>Rajasthan State Totals ({totals.year})</span>
          </div>
          {[
            { label:"Job Cards",      val:totals.job_cards,    color:"#15803d" },
            { label:"HH Demanded",   val:totals.hh_demanded,  color:"#0369a1" },
            { label:"HH Provided",   val:totals.hh_provided,  color:"#16a34a" },
            { label:"Person Days",   val:totals.person_days,  color:"#7c3aed" },
            { label:"Avg Days/HH",   val:totals.avg_days,     color:"#b45309" },
            { label:"Expenditure",   val:totals.expenditure,  color:"#dc2626" },
          ].map((item, i) => (
            <div key={i} style={{ textAlign:"center" }}>
              <div style={{ fontSize:15, fontWeight:900, color:item.color }}>{item.val}</div>
              <div style={{ fontSize:10, color:"#166534", fontWeight:600 }}>{item.label}</div>
            </div>
          ))}
        </div>
      )}

      <p style={{ color:"#94a3b8", fontSize:12, marginBottom:18 }}>
        Source: {dashboard.source} · {dashboard.note}
      </p>

      {/* Search + sort */}
      <div style={{ display:"flex", gap:12, marginBottom:18, flexWrap:"wrap", alignItems:"center" }}>
        <div style={{ position:"relative", flex:1, minWidth:220 }}>
          <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", fontSize:15 }}>🔍</span>
          <input value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search district…"
            style={{ width:"100%", padding:"10px 12px 10px 38px", border:"1px solid #e2e8f0",
              borderRadius:11, fontSize:13, background:"white", boxSizing:"border-box" }}/>
        </div>
        <select value={sortBy} onChange={e => setSortBy(e.target.value)}
          style={{ padding:"10px 14px", border:"1px solid #e2e8f0", borderRadius:11, fontSize:13, background:"white" }}>
          <option value="emp_desc">Employment: High → Low</option>
          <option value="emp_asc">Employment: Low → High</option>
          <option value="name">District: A – Z</option>
        </select>
        <span style={{ fontSize:12, color:"#94a3b8" }}>{filtered.length} of {rows.length} districts</span>
      </div>

      {/* District table — 7 columns */}
      <div style={{ background:"white", borderRadius:18, border:"1px solid #e5e7eb", overflow:"hidden", boxShadow:"0 6px 18px rgba(15,23,42,0.04)" }}>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(7,minmax(0,1fr))",
          padding:"14px 20px", background:"#f8fafc", borderBottom:"1px solid #e5e7eb", gap:10 }}>
          {columns.map(col => (
            <div key={col.key} style={{ fontSize:11, fontWeight:800, color:"#94a3b8", letterSpacing:"0.05em", textTransform:"uppercase" }}>
              {col.label}
            </div>
          ))}
        </div>

        {filtered.map((row, idx) => (
          <div key={`${row.district}-${idx}`}
            style={{ display:"grid", gridTemplateColumns:"repeat(7,minmax(0,1fr))",
              padding:"14px 20px", gap:10, alignItems:"center",
              borderBottom:"1px solid #f1f5f9", background:idx % 2 === 0 ? "white" : "#fcfcfd" }}>

            <div style={{ display:"flex", alignItems:"center", gap:8 }}>
              <span style={{ width:10, height:10, borderRadius:"50%", flexShrink:0,
                background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}/>
              <span style={{ fontWeight:800, fontSize:13, color:"#0f172a" }}>{row.district}</span>
            </div>
            <div style={{ fontSize:12, color:"#15803d", fontWeight:600 }}>{row.job_cards}</div>
            <div style={{ fontSize:12, color:"#0369a1", fontWeight:600 }}>{row.demanded}</div>
            <div style={{ fontSize:12, color:"#16a34a", fontWeight:600 }}>{row.provided}</div>
            <div style={{ fontSize:12, color:"#7c3aed", fontWeight:600 }}>{row.person_days}</div>

            <div>
              <div style={{ display:"flex", alignItems:"center", gap:8 }}>
                <div style={{ flex:1, height:8, background:"#dbe4f0", borderRadius:999, overflow:"hidden" }}>
                  <div style={{
                    width:`${Math.min(row.emp_pct || 0, 100)}%`, height:"100%", borderRadius:999,
                    background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444",
                    transition:"width 0.35s ease"
                  }}/>
                </div>
                <span style={{ minWidth:38, textAlign:"right", fontWeight:800, fontSize:12,
                  color: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}>
                  {row.emp_pct}%
                </span>
              </div>
            </div>
            <div>{renderStatusPill(row)}</div>
          </div>
        ))}

        <div style={{ display:"flex", justifyContent:"space-between", gap:10, padding:"12px 20px",
          background:"#f8fafc", color:"#94a3b8", fontSize:11 }}>
          <span>{dashboard.live ? "Live feed" : "MIS report data"} · {dashboard.source}</span>
          <span>Fetched {timeAgo(dashboard.scraped_at)}</span>
        </div>
      </div>

      <div style={{ display:"flex", gap:12, marginTop:18, flexWrap:"wrap" }}>
        {[
          { label:"NREGA MIS Rajasthan",    url:"https://nreganarep.nic.in/netnrega/nrega_ataglance/At_a_glance.aspx?state_code=17&state_name=RAJASTHAN", color:"#16a34a" },
          { label:"Jan Soochna MGNREGA",    url:"https://jansoochna.rajasthan.gov.in/MGNREGA",                                                           color:"#0369a1" },
          { label:"NREGA At a Glance",      url:"https://nreganarep.nic.in/netnrega/nrega_ataglance/At_a_glance.aspx",                                    color:"#15803d" },
        ].map((link, i) => (
          <a key={i} href={link.url} target="_blank" rel="noreferrer" style={{
            display:"inline-flex", alignItems:"center", gap:6,
            background:`${link.color}12`, color:link.color,
            border:`1px solid ${link.color}30`, borderRadius:999,
            padding:"8px 16px", fontWeight:700, fontSize:12, textDecoration:"none"
          }}>
            {link.label} ↗
          </a>
        ))}
      </div>
    </div>
  );
}

// ── Saubhagya Tab ─────────────────────────────────────────────────────────────
function SaubhagyaTab({ schemeDashboards, agg, onScrapeAll }) {
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("elec_desc");

  const dashboard = schemeDashboards.find(d => d.id === "saubhagya") || null;

  if (!dashboard && !agg) return <EmptyState onScrape={onScrapeAll}/>;
  if (!dashboard) return (
    <div style={{ background:"white", borderRadius:16, border:"2px dashed #e5e7eb", padding:60, textAlign:"center" }}>
      <div style={{ fontSize:40, marginBottom:12 }}>⚡</div>
      <div style={{ fontWeight:800, fontSize:18, color:"#0f172a", marginBottom:8 }}>Saubhagya data loading…</div>
      <div style={{ color:"#64748b", fontSize:13, marginBottom:20 }}>Click Refresh to fetch electrification data from saubhagya.gov.in</div>
      <button onClick={onScrapeAll} style={{ background:"#f59e0b", color:"white", borderRadius:12, padding:"12px 28px", fontWeight:800, fontSize:14, border:"none", cursor:"pointer" }}>⚡ Fetch Saubhagya Data</button>
    </div>
  );

  const { summary, rows = [], columns = [] } = dashboard;
  const totals = summary?.state_totals || {};

  const filtered = [...rows]
    .filter(r => !search || String(r.district || "").toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      if (sortBy === "elec_asc") return (a.elec_pct || 0) - (b.elec_pct || 0);
      if (sortBy === "name")     return String(a.district || "").localeCompare(String(b.district || ""));
      return (b.elec_pct || 0) - (a.elec_pct || 0);
    });

  const renderStatusPill = (row) => {
    const tone = row.status_tone;
    const styles = tone === "good"
      ? { bg:"#d1fae5", color:"#047857" }
      : tone === "watch"
      ? { bg:"#fef9c3", color:"#ca8a04" }
      : { bg:"#fee2e2", color:"#dc2626" };
    return (
      <span style={{ display:"inline-flex", alignItems:"center", justifyContent:"center",
        borderRadius:999, padding:"6px 14px", fontWeight:700, fontSize:12,
        background:styles.bg, color:styles.color, minWidth:108 }}>
        {row.status}
      </span>
    );
  };

  return (
    <div className="fadeup">
      <div style={{ display:"flex", alignItems:"center", gap:4, marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
          PM Saubhagya — <span style={{ color:"#f59e0b" }}>Household Electrification</span>
        </h2>
        <InfoTip text="PM Saubhagya (Pradhan Mantri Sahaj Bijli Har Ghar Yojana) — household electrification rate per district from saubhagya.gov.in MIS dashboard."/>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:4 }}>
        Source: {dashboard.source} · {dashboard.report_label}
      </p>
      <div style={{ display:"inline-flex", alignItems:"center", gap:8, marginBottom:20,
        background:"#fffbeb", border:"1px solid #fde68a", color:"#92400e",
        borderRadius:999, padding:"7px 14px", fontSize:12.5, fontWeight:800 }}>
        <span style={{ width:8, height:8, borderRadius:"50%", background:dashboard.live ? "#f59e0b" : "#fcd34d", display:"inline-block" }}/>
        {dashboard.verified_label}
      </div>

      {/* KPI summary cards */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:14, marginBottom:24 }}>
        {[
          { value:summary?.primary || "—",   label:summary?.primaryLabel || "Avg Electrification",  bg:"#fffbeb", border:"#fde68a", numC:"#b45309", txtC:"#92400e" },
          { value:summary?.good ?? 0,         label:summary?.goodLabel || "Districts ≥95%",          bg:"#f0fdf4", border:"#bbf7d0", numC:"#16a34a", txtC:"#166534" },
          { value:summary?.watch ?? 0,        label:summary?.watchLabel || "Districts 90–95%",       bg:"#fff7ed", border:"#fed7aa", numC:"#ea580c", txtC:"#9a3412" },
          { value:summary?.critical ?? 0,     label:summary?.criticalLabel || "Critical",             bg:"#fff1f2", border:"#fecdd3", numC:"#e11d48", txtC:"#9f1239" },
        ].map((item, i) => (
          <div key={i} style={{ background:item.bg, border:`1.5px solid ${item.border}`, borderRadius:16, padding:"18px 22px" }}>
            <div style={{ fontSize:i===0?32:46, lineHeight:1, fontWeight:900, color:item.numC, marginBottom:8 }}>{item.value}</div>
            <div style={{ fontSize:13, fontWeight:700, color:item.txtC }}>{item.label}</div>
          </div>
        ))}
      </div>

      {/* State totals banner */}
      {totals.total_hh && (
        <div style={{ background:"linear-gradient(135deg,#fffbeb,#fef3c7)", border:"1.5px solid #fde68a",
          borderRadius:14, padding:"14px 20px", marginBottom:22, display:"flex", flexWrap:"wrap", gap:24, alignItems:"center" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7 }}>
            <span style={{ fontSize:18 }}>⚡</span>
            <span style={{ fontWeight:800, fontSize:14, color:"#78350f" }}>Rajasthan State Totals ({totals.year})</span>
          </div>
          {[
            { label:"Total Households",   val:totals.total_hh,        color:"#b45309" },
            { label:"Electrified",        val:totals.electrified,     color:"#16a34a" },
            { label:"Remaining",          val:totals.unelectrified,   color:"#dc2626" },
            { label:"Electrification Rate",val:totals.elec_rate,      color:"#f59e0b" },
            { label:"Free Connections",   val:totals.free_connections, color:"#0369a1" },
          ].map((item, i) => (
            <div key={i} style={{ textAlign:"center" }}>
              <div style={{ fontSize:16, fontWeight:900, color:item.color }}>{item.val}</div>
              <div style={{ fontSize:10, color:"#92400e", fontWeight:600 }}>{item.label}</div>
            </div>
          ))}
        </div>
      )}

      <p style={{ color:"#94a3b8", fontSize:12, marginBottom:18 }}>
        Source: {dashboard.source} · {dashboard.note}
      </p>

      {/* Search + sort */}
      <div style={{ display:"flex", gap:12, marginBottom:18, flexWrap:"wrap", alignItems:"center" }}>
        <div style={{ position:"relative", flex:1, minWidth:220 }}>
          <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", fontSize:15 }}>🔍</span>
          <input value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search district…"
            style={{ width:"100%", padding:"10px 12px 10px 38px", border:"1px solid #e2e8f0",
              borderRadius:11, fontSize:13, background:"white", boxSizing:"border-box" }}/>
        </div>
        <select value={sortBy} onChange={e => setSortBy(e.target.value)}
          style={{ padding:"10px 14px", border:"1px solid #e2e8f0", borderRadius:11, fontSize:13, background:"white" }}>
          <option value="elec_desc">Electrification: High → Low</option>
          <option value="elec_asc">Electrification: Low → High</option>
          <option value="name">District: A – Z</option>
        </select>
        <span style={{ fontSize:12, color:"#94a3b8" }}>{filtered.length} of {rows.length} districts</span>
      </div>

      {/* District table */}
      <div style={{ background:"white", borderRadius:18, border:"1px solid #e5e7eb", overflow:"hidden", boxShadow:"0 6px 18px rgba(15,23,42,0.04)" }}>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
          padding:"14px 20px", background:"#f8fafc", borderBottom:"1px solid #e5e7eb", gap:12 }}>
          {columns.map(col => (
            <div key={col.key} style={{ fontSize:11, fontWeight:800, color:"#94a3b8", letterSpacing:"0.05em", textTransform:"uppercase" }}>
              {col.label}
            </div>
          ))}
        </div>

        {filtered.map((row, idx) => (
          <div key={`${row.district}-${idx}`}
            style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
              padding:"15px 20px", gap:12, alignItems:"center",
              borderBottom:"1px solid #f1f5f9", background:idx % 2 === 0 ? "white" : "#fcfcfd" }}>

            <div style={{ display:"flex", alignItems:"center", gap:10 }}>
              <span style={{ width:10, height:10, borderRadius:"50%", flexShrink:0,
                background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}/>
              <span style={{ fontWeight:800, fontSize:14, color:"#0f172a" }}>{row.district}</span>
            </div>
            <div style={{ fontSize:13, color:"#b45309", fontWeight:600 }}>{row.total_hh}</div>
            <div style={{ fontSize:13, color:"#16a34a", fontWeight:600 }}>{row.electrified}</div>
            <div style={{ fontSize:13, color:"#dc2626", fontWeight:600 }}>{row.remaining}</div>

            <div>
              <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                <div style={{ flex:1, height:8, background:"#dbe4f0", borderRadius:999, overflow:"hidden" }}>
                  <div style={{
                    width:`${Math.min(row.elec_pct || 0, 100)}%`, height:"100%", borderRadius:999,
                    background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444",
                    transition:"width 0.35s ease"
                  }}/>
                </div>
                <span style={{ minWidth:40, textAlign:"right", fontWeight:800, fontSize:13,
                  color: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}>
                  {row.elec_pct}%
                </span>
              </div>
            </div>
            <div>{renderStatusPill(row)}</div>
          </div>
        ))}

        <div style={{ display:"flex", justifyContent:"space-between", gap:10, padding:"12px 20px",
          background:"#f8fafc", color:"#94a3b8", fontSize:11 }}>
          <span>{dashboard.live ? "Live feed" : "MIS dashboard data"} · {dashboard.source}</span>
          <span>Fetched {timeAgo(dashboard.scraped_at)}</span>
        </div>
      </div>

      <div style={{ display:"flex", gap:12, marginTop:18, flexWrap:"wrap" }}>
        {[
          { label:"Saubhagya Dashboard",    url:"https://saubhagya.gov.in/dashboard", color:"#f59e0b" },
          { label:"Saubhagya Official Portal",url:"https://saubhagya.gov.in/",        color:"#b45309" },
          { label:"MoP Electrification MIS", url:"https://garv.gov.in/",              color:"#0369a1" },
        ].map((link, i) => (
          <a key={i} href={link.url} target="_blank" rel="noreferrer" style={{
            display:"inline-flex", alignItems:"center", gap:6,
            background:`${link.color}12`, color:link.color,
            border:`1px solid ${link.color}30`, borderRadius:999,
            padding:"8px 16px", fontWeight:700, fontSize:12, textDecoration:"none"
          }}>
            {link.label} ↗
          </a>
        ))}
      </div>
    </div>
  );
}

// ── PMGDISHA Tab ──────────────────────────────────────────────────────────────
function PmgdishaTab({ schemeDashboards, agg, onScrapeAll }) {
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("cert_desc");

  const dashboard = schemeDashboards.find(d => d.id === "pmgdisha") || null;

  if (!dashboard && !agg) return <EmptyState onScrape={onScrapeAll}/>;

  if (!dashboard) return (
    <div style={{ background:"white", borderRadius:16, border:"2px dashed #e5e7eb", padding:60, textAlign:"center" }}>
      <div style={{ fontSize:40, marginBottom:12 }}>💻</div>
      <div style={{ fontWeight:800, fontSize:18, color:"#0f172a", marginBottom:8 }}>PMGDISHA data loading…</div>
      <div style={{ color:"#64748b", fontSize:13, marginBottom:20 }}>Click Refresh to fetch digital literacy data from pmgdisha.in MIS</div>
      <button onClick={onScrapeAll} style={{ background:"#6366f1", color:"white", borderRadius:12, padding:"12px 28px", fontWeight:800, fontSize:14, border:"none", cursor:"pointer" }}>⚡ Fetch PMGDISHA Data</button>
    </div>
  );

  const { summary, rows = [], columns = [] } = dashboard;
  const totals = summary?.state_totals || {};

  const filtered = [...rows]
    .filter(r => !search || String(r.district || "").toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      if (sortBy === "cert_asc") return (a.cert_pct || 0) - (b.cert_pct || 0);
      if (sortBy === "name")     return String(a.district || "").localeCompare(String(b.district || ""));
      return (b.cert_pct || 0) - (a.cert_pct || 0);
    });

  const renderStatusPill = (row) => {
    const tone = row.status_tone;
    const styles = tone === "good"
      ? { bg:"#d1fae5", color:"#047857" }
      : tone === "watch"
      ? { bg:"#fef9c3", color:"#ca8a04" }
      : { bg:"#fee2e2", color:"#dc2626" };
    return (
      <span style={{ display:"inline-flex", alignItems:"center", justifyContent:"center",
        borderRadius:999, padding:"6px 14px", fontWeight:700, fontSize:12,
        background:styles.bg, color:styles.color, minWidth:108 }}>
        {row.status}
      </span>
    );
  };

  return (
    <div className="fadeup">
      {/* Header */}
      <div style={{ display:"flex", alignItems:"center", gap:4, marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
          PMGDISHA — <span style={{ color:"#6366f1" }}>Digital Literacy</span>
        </h2>
        <InfoTip text="Pradhan Mantri Gramin Digital Saksharta Abhiyan — certification rate data from pmgdisha.in MIS dashboard. Covers rural candidates registered, trained, and certified per district."/>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:4 }}>
        Source: {dashboard.source} · {dashboard.report_label}
      </p>
      <div style={{ display:"inline-flex", alignItems:"center", gap:8, marginBottom:20,
        background:"#eef2ff", border:"1px solid #c7d2fe", color:"#4338ca",
        borderRadius:999, padding:"7px 14px", fontSize:12.5, fontWeight:800 }}>
        <span style={{ width:8, height:8, borderRadius:"50%", background:dashboard.live ? "#6366f1" : "#a5b4fc", display:"inline-block" }}/>
        {dashboard.verified_label}
      </div>

      {/* KPI summary cards */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:14, marginBottom:24 }}>
        {[
          { value:summary?.primary || "—",   label:summary?.primaryLabel || "Avg Certification",  bg:"#eef2ff", border:"#c7d2fe", numC:"#4338ca", txtC:"#3730a3" },
          { value:summary?.good ?? 0,         label:summary?.goodLabel || "Districts ≥72%",        bg:"#f0fdf4", border:"#bbf7d0", numC:"#16a34a", txtC:"#166534" },
          { value:summary?.watch ?? 0,        label:summary?.watchLabel || "Districts 62–72%",     bg:"#fffbeb", border:"#fde68a", numC:"#ea580c", txtC:"#9a3412" },
          { value:summary?.critical ?? 0,     label:summary?.criticalLabel || "Critical",           bg:"#fff1f2", border:"#fecdd3", numC:"#e11d48", txtC:"#9f1239" },
        ].map((item, i) => (
          <div key={i} style={{ background:item.bg, border:`1.5px solid ${item.border}`, borderRadius:16, padding:"18px 22px" }}>
            <div style={{ fontSize:i===0?32:46, lineHeight:1, fontWeight:900, color:item.numC, marginBottom:8 }}>{item.value}</div>
            <div style={{ fontSize:13, fontWeight:700, color:item.txtC }}>{item.label}</div>
          </div>
        ))}
      </div>

      {/* State totals banner */}
      {totals.registered && (
        <div style={{ background:"linear-gradient(135deg,#eef2ff,#e0e7ff)", border:"1.5px solid #c7d2fe",
          borderRadius:14, padding:"14px 20px", marginBottom:22, display:"flex", flexWrap:"wrap", gap:24, alignItems:"center" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7 }}>
            <span style={{ fontSize:18 }}>💻</span>
            <span style={{ fontWeight:800, fontSize:14, color:"#312e81" }}>Rajasthan State Totals ({totals.year})</span>
          </div>
          {[
            { label:"Registered",       val:totals.registered,  color:"#4338ca" },
            { label:"Trained",          val:totals.trained,     color:"#0369a1" },
            { label:"Certified",        val:totals.certified,   color:"#16a34a" },
            { label:"Certification Rate",val:totals.cert_rate,  color:"#6366f1" },
          ].map((item, i) => (
            <div key={i} style={{ textAlign:"center" }}>
              <div style={{ fontSize:18, fontWeight:900, color:item.color }}>{item.val}</div>
              <div style={{ fontSize:10, color:"#4338ca", fontWeight:600 }}>{item.label}</div>
            </div>
          ))}
        </div>
      )}

      <p style={{ color:"#94a3b8", fontSize:12, marginBottom:18 }}>
        Source: {dashboard.source} · {dashboard.note}
      </p>

      {/* Search + sort */}
      <div style={{ display:"flex", gap:12, marginBottom:18, flexWrap:"wrap", alignItems:"center" }}>
        <div style={{ position:"relative", flex:1, minWidth:220 }}>
          <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", fontSize:15 }}>🔍</span>
          <input value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search district…"
            style={{ width:"100%", padding:"10px 12px 10px 38px", border:"1px solid #e2e8f0",
              borderRadius:11, fontSize:13, background:"white", boxSizing:"border-box" }}/>
        </div>
        <select value={sortBy} onChange={e => setSortBy(e.target.value)}
          style={{ padding:"10px 14px", border:"1px solid #e2e8f0", borderRadius:11, fontSize:13, background:"white" }}>
          <option value="cert_desc">Certification: High → Low</option>
          <option value="cert_asc">Certification: Low → High</option>
          <option value="name">District: A – Z</option>
        </select>
        <span style={{ fontSize:12, color:"#94a3b8" }}>{filtered.length} of {rows.length} districts</span>
      </div>

      {/* District table */}
      <div style={{ background:"white", borderRadius:18, border:"1px solid #e5e7eb", overflow:"hidden", boxShadow:"0 6px 18px rgba(15,23,42,0.04)" }}>
        {/* Column headers */}
        <div style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
          padding:"14px 20px", background:"#f8fafc", borderBottom:"1px solid #e5e7eb", gap:12 }}>
          {columns.map(col => (
            <div key={col.key} style={{ fontSize:11, fontWeight:800, color:"#94a3b8", letterSpacing:"0.05em", textTransform:"uppercase" }}>
              {col.label}
            </div>
          ))}
        </div>

        {/* Data rows */}
        {filtered.map((row, idx) => (
          <div key={`${row.district}-${idx}`}
            style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
              padding:"15px 20px", gap:12, alignItems:"center",
              borderBottom:"1px solid #f1f5f9", background:idx % 2 === 0 ? "white" : "#fcfcfd" }}>

            {/* District */}
            <div style={{ display:"flex", alignItems:"center", gap:10 }}>
              <span style={{ width:10, height:10, borderRadius:"50%", flexShrink:0,
                background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}/>
              <span style={{ fontWeight:800, fontSize:14, color:"#0f172a" }}>{row.district}</span>
            </div>

            {/* Registered */}
            <div style={{ fontSize:13, color:"#4338ca", fontWeight:600 }}>{row.registered}</div>

            {/* Trained */}
            <div style={{ fontSize:13, color:"#0369a1", fontWeight:600 }}>{row.trained}</div>

            {/* Certified */}
            <div style={{ fontSize:13, color:"#16a34a", fontWeight:600 }}>{row.certified}</div>

            {/* Certification rate progress bar */}
            <div>
              <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                <div style={{ flex:1, height:8, background:"#dbe4f0", borderRadius:999, overflow:"hidden" }}>
                  <div style={{
                    width:`${Math.min(row.cert_pct || 0, 100)}%`, height:"100%", borderRadius:999,
                    background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444",
                    transition:"width 0.35s ease"
                  }}/>
                </div>
                <span style={{ minWidth:40, textAlign:"right", fontWeight:800, fontSize:13,
                  color: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}>
                  {row.cert_pct}%
                </span>
              </div>
            </div>

            {/* Status pill */}
            <div>{renderStatusPill(row)}</div>
          </div>
        ))}

        {/* Footer */}
        <div style={{ display:"flex", justifyContent:"space-between", gap:10, padding:"12px 20px",
          background:"#f8fafc", color:"#94a3b8", fontSize:11 }}>
          <span>{dashboard.live ? "Live feed" : "MIS report data"} · {dashboard.source}</span>
          <span>Fetched {timeAgo(dashboard.scraped_at)}</span>
        </div>
      </div>

      {/* Source links */}
      <div style={{ display:"flex", gap:12, marginTop:18, flexWrap:"wrap" }}>
        {[
          { label:"PMGDISHA MIS Dashboard", url:"https://www.pmgdisha.in/mis-dashboard", color:"#6366f1" },
          { label:"PMGDISHA Official Portal", url:"https://www.pmgdisha.in/",            color:"#4338ca" },
          { label:"CSC Digital Saksharta",   url:"https://www.csc.gov.in/",              color:"#0369a1" },
        ].map((link, i) => (
          <a key={i} href={link.url} target="_blank" rel="noreferrer" style={{
            display:"inline-flex", alignItems:"center", gap:6,
            background:`${link.color}12`, color:link.color,
            border:`1px solid ${link.color}30`, borderRadius:999,
            padding:"8px 16px", fontWeight:700, fontSize:12, textDecoration:"none"
          }}>
            {link.label} ↗
          </a>
        ))}
      </div>
    </div>
  );
}

// ── SBM-G Tab ──────────────────────────────────────────────────────────────
function SbmgTab({ schemeDashboards, agg, onScrapeAll }) {
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("ihhl_desc");

  const dashboard = schemeDashboards.find(d => d.id === "sbmg") || null;

  if (!dashboard && !agg) return <EmptyState onScrape={onScrapeAll}/>;
  if (!dashboard) return (
    <div style={{ background:"white", borderRadius:16, border:"2px dashed #e5e7eb", padding:60, textAlign:"center" }}>
      <div style={{ fontSize:40, marginBottom:12 }}>🚿</div>
      <div style={{ fontWeight:800, fontSize:18, color:"#0f172a", marginBottom:8 }}>SBM-G data loading…</div>
      <div style={{ color:"#64748b", fontSize:13, marginBottom:20 }}>Click Refresh to fetch sanitation data from sbm.gov.in</div>
      <button onClick={onScrapeAll} style={{ background:"#f59e0b", color:"white", borderRadius:12, padding:"12px 28px", fontWeight:800, fontSize:14, border:"none", cursor:"pointer" }}>⚡ Fetch SBM-G Data</button>
    </div>
  );

  const { summary, rows = [], columns = [] } = dashboard;
  const totals = summary?.state_totals || {};

  const filtered = [...rows]
    .filter(r => !search || String(r.district || "").toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      if (sortBy === "ihhl_asc") return (a.ihhl_pct || 0) - (b.ihhl_pct || 0);
      if (sortBy === "name")     return String(a.district || "").localeCompare(String(b.district || ""));
      return (b.ihhl_pct || 0) - (a.ihhl_pct || 0);
    });

  const renderStatusPill = (row) => {
    const tone = row.status_tone;
    const styles = tone === "good"
      ? { bg:"#d1fae5", color:"#047857" }
      : tone === "watch"
      ? { bg:"#fef9c3", color:"#ca8a04" }
      : { bg:"#fee2e2", color:"#dc2626" };
    return (
      <span style={{ display:"inline-flex", alignItems:"center", justifyContent:"center",
        borderRadius:999, padding:"6px 14px", fontWeight:700, fontSize:12,
        background:styles.bg, color:styles.color, minWidth:108 }}>
        {row.status}
      </span>
    );
  };

  return (
    <div className="fadeup">
      <div style={{ display:"flex", alignItems:"center", gap:4, marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
          SBM-G (Rajasthan) — <span style={{ color:"#d97706" }}>Gramin Sanitation</span>
        </h2>
        <InfoTip text="Swachh Bharat Mission Gramin — IHHL (Individual Household Latrine) coverage and ODF village data from sbm.gov.in district dashboard. Rajasthan declared ODF in 2019."/>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:4 }}>
        Source: {dashboard.source} · {dashboard.report_label}
      </p>
      <div style={{ display:"inline-flex", alignItems:"center", gap:8, marginBottom:20,
        background:"#fffbeb", border:"1px solid #fde68a", color:"#92400e",
        borderRadius:999, padding:"7px 14px", fontSize:12.5, fontWeight:800 }}>
        <span style={{ width:8, height:8, borderRadius:"50%", background:dashboard.live ? "#f59e0b" : "#fcd34d", display:"inline-block" }}/>
        {dashboard.verified_label}
      </div>

      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:14, marginBottom:24 }}>
        {[
          { value:summary?.primary || "—",   label:summary?.primaryLabel || "Avg IHHL Coverage",  bg:"#fffbeb", border:"#fde68a", numC:"#b45309", txtC:"#92400e" },
          { value:summary?.good ?? 0,         label:summary?.goodLabel || "Districts ≥95%",        bg:"#f0fdf4", border:"#bbf7d0", numC:"#16a34a", txtC:"#166534" },
          { value:summary?.watch ?? 0,        label:summary?.watchLabel || "Districts 85–95%",     bg:"#fff7ed", border:"#fed7aa", numC:"#ea580c", txtC:"#9a3412" },
          { value:summary?.critical ?? 0,     label:summary?.criticalLabel || "Critical",           bg:"#fff1f2", border:"#fecdd3", numC:"#e11d48", txtC:"#9f1239" },
        ].map((item, i) => (
          <div key={i} style={{ background:item.bg, border:`1.5px solid ${item.border}`, borderRadius:16, padding:"18px 22px" }}>
            <div style={{ fontSize:i===0?32:46, lineHeight:1, fontWeight:900, color:item.numC, marginBottom:8 }}>{item.value}</div>
            <div style={{ fontSize:13, fontWeight:700, color:item.txtC }}>{item.label}</div>
          </div>
        ))}
      </div>

      {totals.total_villages && (
        <div style={{ background:"linear-gradient(135deg,#fffbeb,#fef3c7)", border:"1.5px solid #fde68a",
          borderRadius:14, padding:"14px 20px", marginBottom:22, display:"flex", flexWrap:"wrap", gap:20, alignItems:"center" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7 }}>
            <span style={{ fontSize:18 }}>🚿</span>
            <span style={{ fontWeight:800, fontSize:14, color:"#78350f" }}>Rajasthan State Totals ({totals.year})</span>
          </div>
          {[
            { label:"Total Villages",  val:totals.total_villages,  color:"#b45309" },
            { label:"ODF Villages",    val:totals.odf_villages,    color:"#16a34a" },
            { label:"IHHL Target",     val:totals.ihhl_target,     color:"#0369a1" },
            { label:"IHHL Completed",  val:totals.ihhl_completed,  color:"#16a34a" },
            { label:"IHHL Coverage",   val:totals.ihhl_coverage,   color:"#f59e0b" },
            { label:"ODF Status",      val:totals.odf_status,      color:"#047857" },
          ].map((item, i) => (
            <div key={i} style={{ textAlign:"center" }}>
              <div style={{ fontSize:i===5?12:15, fontWeight:900, color:item.color }}>{item.val}</div>
              <div style={{ fontSize:10, color:"#92400e", fontWeight:600 }}>{item.label}</div>
            </div>
          ))}
        </div>
      )}

      <p style={{ color:"#94a3b8", fontSize:12, marginBottom:18 }}>
        Source: {dashboard.source} · {dashboard.note}
      </p>

      <div style={{ display:"flex", gap:12, marginBottom:18, flexWrap:"wrap", alignItems:"center" }}>
        <div style={{ position:"relative", flex:1, minWidth:220 }}>
          <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", fontSize:15 }}>🔍</span>
          <input value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search district…"
            style={{ width:"100%", padding:"10px 12px 10px 38px", border:"1px solid #e2e8f0",
              borderRadius:11, fontSize:13, background:"white", boxSizing:"border-box" }}/>
        </div>
        <select value={sortBy} onChange={e => setSortBy(e.target.value)}
          style={{ padding:"10px 14px", border:"1px solid #e2e8f0", borderRadius:11, fontSize:13, background:"white" }}>
          <option value="ihhl_desc">IHHL Coverage: High → Low</option>
          <option value="ihhl_asc">IHHL Coverage: Low → High</option>
          <option value="name">District: A – Z</option>
        </select>
        <span style={{ fontSize:12, color:"#94a3b8" }}>{filtered.length} of {rows.length} districts</span>
      </div>

      <div style={{ background:"white", borderRadius:18, border:"1px solid #e5e7eb", overflow:"hidden", boxShadow:"0 6px 18px rgba(15,23,42,0.04)" }}>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
          padding:"14px 20px", background:"#f8fafc", borderBottom:"1px solid #e5e7eb", gap:12 }}>
          {columns.map(col => (
            <div key={col.key} style={{ fontSize:11, fontWeight:800, color:"#94a3b8", letterSpacing:"0.05em", textTransform:"uppercase" }}>
              {col.label}
            </div>
          ))}
        </div>

        {filtered.map((row, idx) => (
          <div key={`${row.district}-${idx}`}
            style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
              padding:"15px 20px", gap:12, alignItems:"center",
              borderBottom:"1px solid #f1f5f9", background:idx % 2 === 0 ? "white" : "#fcfcfd" }}>

            <div style={{ display:"flex", alignItems:"center", gap:10 }}>
              <span style={{ width:10, height:10, borderRadius:"50%", flexShrink:0,
                background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}/>
              <span style={{ fontWeight:800, fontSize:14, color:"#0f172a" }}>{row.district}</span>
            </div>
            <div style={{ fontSize:13, color:"#b45309", fontWeight:600 }}>{row.villages}</div>
            <div style={{ fontSize:13, color:"#16a34a", fontWeight:600 }}>{row.odf_villages}</div>

            <div>
              <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                <div style={{ flex:1, height:8, background:"#dbe4f0", borderRadius:999, overflow:"hidden" }}>
                  <div style={{
                    width:`${Math.min(row.ihhl_pct || 0, 100)}%`, height:"100%", borderRadius:999,
                    background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444",
                    transition:"width 0.35s ease"
                  }}/>
                </div>
                <span style={{ minWidth:40, textAlign:"right", fontWeight:800, fontSize:13,
                  color: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}>
                  {row.ihhl_pct}%
                </span>
              </div>
            </div>

            <div style={{ fontSize:13, color:"#047857", fontWeight:700 }}>{row.odf_pct}%</div>
            <div>{renderStatusPill(row)}</div>
          </div>
        ))}

        <div style={{ display:"flex", justifyContent:"space-between", gap:10, padding:"12px 20px",
          background:"#f8fafc", color:"#94a3b8", fontSize:11 }}>
          <span>{dashboard.live ? "Live feed" : "MIS report data"} · {dashboard.source}</span>
          <span>Fetched {timeAgo(dashboard.scraped_at)}</span>
        </div>
      </div>

      <div style={{ display:"flex", gap:12, marginTop:18, flexWrap:"wrap" }}>
        {[
          { label:"SBM-G District Dashboard", url:"https://sbm.gov.in/sbmgdashboard/StatesDashboard.aspx", color:"#f59e0b" },
          { label:"SBM-G Official Portal",    url:"https://sbm.gov.in/",                                   color:"#b45309" },
          { label:"SBM-G MIS Report",         url:"https://sbmreport.nic.in/",                             color:"#0369a1" },
        ].map((link, i) => (
          <a key={i} href={link.url} target="_blank" rel="noreferrer" style={{
            display:"inline-flex", alignItems:"center", gap:6,
            background:`${link.color}12`, color:link.color,
            border:`1px solid ${link.color}30`, borderRadius:999,
            padding:"8px 16px", fontWeight:700, fontSize:12, textDecoration:"none"
          }}>
            {link.label} ↗
          </a>
        ))}
      </div>
    </div>
  );
}

// ── PM Jan Dhan Tab ──────────────────────────────────────────────────────────
function PmjdyTab({ schemeDashboards, agg, onScrapeAll }) {
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("sat_desc");

  const dashboard = schemeDashboards.find(d => d.id === "pmjdy") || null;

  if (!dashboard && !agg) return <EmptyState onScrape={onScrapeAll}/>;

  if (!dashboard) return (
    <div style={{ background:"white", borderRadius:16, border:"2px dashed #e5e7eb", padding:60, textAlign:"center" }}>
      <div style={{ fontSize:40, marginBottom:12 }}>🏦</div>
      <div style={{ fontWeight:800, fontSize:18, color:"#0f172a", marginBottom:8 }}>PMJDY data loading…</div>
      <div style={{ color:"#64748b", fontSize:13, marginBottom:20 }}>Click Refresh to fetch Jan Dhan account data from pmjdy.gov.in</div>
      <button onClick={onScrapeAll} style={{ background:"#0ea5e9", color:"white", borderRadius:12, padding:"12px 28px", fontWeight:800, fontSize:14, border:"none", cursor:"pointer" }}>⚡ Fetch PMJDY Data</button>
    </div>
  );

  const { summary, rows = [], columns = [] } = dashboard;
  const totals = summary?.state_totals || {};

  const filtered = [...rows]
    .filter(r => !search || String(r.district || "").toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      if (sortBy === "sat_asc")  return (a.saturation_pct || 0) - (b.saturation_pct || 0);
      if (sortBy === "name")     return String(a.district || "").localeCompare(String(b.district || ""));
      return (b.saturation_pct || 0) - (a.saturation_pct || 0);
    });

  const renderStatusPill = (row) => {
    const tone = row.status_tone || (row.saturation_pct >= 85 ? "good" : row.saturation_pct >= 75 ? "watch" : "critical");
    const styles = tone === "good"
      ? { bg:"#d1fae5", color:"#047857" }
      : tone === "watch"
      ? { bg:"#fef9c3", color:"#ca8a04" }
      : { bg:"#fee2e2", color:"#dc2626" };
    return (
      <span style={{ display:"inline-flex", alignItems:"center", justifyContent:"center",
        borderRadius:999, padding:"6px 14px", fontWeight:700, fontSize:12,
        background:styles.bg, color:styles.color, minWidth:108 }}>
        {row.status}
      </span>
    );
  };

  return (
    <div className="fadeup">
      {/* Header */}
      <div style={{ display:"flex", alignItems:"center", gap:4, marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
          PM Jan Dhan Yojana — <span style={{ color:"#0ea5e9" }}>Financial Inclusion</span>
        </h2>
        <InfoTip text="PMJDY account saturation data from pmjdy.gov.in weekly state progress reports. Saturation = accounts opened as % of adult population per district."/>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:4 }}>
        Source: {dashboard.source} · {dashboard.report_label}
      </p>
      <div style={{ display:"inline-flex", alignItems:"center", gap:8, marginBottom:20,
        background:"#f0f9ff", border:"1px solid #bae6fd", color:"#0369a1",
        borderRadius:999, padding:"7px 14px", fontSize:12.5, fontWeight:800 }}>
        <span style={{ width:8, height:8, borderRadius:"50%", background:dashboard.live ? "#0ea5e9" : "#7dd3fc", display:"inline-block" }}/>
        {dashboard.verified_label}
      </div>

      {/* KPI summary cards */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:14, marginBottom:24 }}>
        {[
          { value:summary?.primary || "—",   label:summary?.primaryLabel || "Avg Saturation",  bg:"#eff6ff", border:"#bfdbfe", numC:"#1d4ed8", txtC:"#1e40af" },
          { value:summary?.good ?? 0,         label:summary?.goodLabel || "Districts ≥85%",    bg:"#f0fdf4", border:"#bbf7d0", numC:"#16a34a", txtC:"#166534" },
          { value:summary?.watch ?? 0,        label:summary?.watchLabel || "Districts 75–85%", bg:"#fffbeb", border:"#fde68a", numC:"#ea580c", txtC:"#9a3412" },
          { value:summary?.critical ?? 0,     label:summary?.criticalLabel || "Critical",      bg:"#fff1f2", border:"#fecdd3", numC:"#e11d48", txtC:"#9f1239" },
        ].map((item, i) => (
          <div key={i} style={{ background:item.bg, border:`1.5px solid ${item.border}`, borderRadius:16, padding:"18px 22px" }}>
            <div style={{ fontSize:i===0?32:46, lineHeight:1, fontWeight:900, color:item.numC, marginBottom:8 }}>{item.value}</div>
            <div style={{ fontSize:13, fontWeight:700, color:item.txtC }}>{item.label}</div>
          </div>
        ))}
      </div>

      {/* State totals banner */}
      {totals.total_accounts && (
        <div style={{ background:"linear-gradient(135deg,#f0f9ff,#e0f2fe)", border:"1.5px solid #bae6fd",
          borderRadius:14, padding:"14px 20px", marginBottom:22, display:"flex", flexWrap:"wrap", gap:20, alignItems:"center" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7 }}>
            <span style={{ fontSize:18 }}>🏦</span>
            <span style={{ fontWeight:800, fontSize:14, color:"#0c4a6e" }}>Rajasthan State Totals ({totals.week})</span>
          </div>
          {[
            { label:"Total Accounts",  val:totals.total_accounts,  color:"#0369a1" },
            { label:"Zero Balance",    val:totals.zero_bal,        color:"#dc2626" },
            { label:"RuPay Cards",     val:totals.rupay_cards,     color:"#16a34a" },
            { label:"Total Balance",   val:totals.total_balance,   color:"#0369a1" },
            { label:"Avg Balance",     val:totals.avg_balance,     color:"#7c3aed" },
            { label:"Saturation",      val:totals.saturation,      color:"#0ea5e9" },
          ].map((item, i) => (
            <div key={i} style={{ textAlign:"center" }}>
              <div style={{ fontSize:16, fontWeight:900, color:item.color }}>{item.val}</div>
              <div style={{ fontSize:10, color:"#0369a1", fontWeight:600 }}>{item.label}</div>
            </div>
          ))}
        </div>
      )}

      <p style={{ color:"#94a3b8", fontSize:12, marginBottom:18 }}>
        Source: {dashboard.source} · {dashboard.note}
      </p>

      {/* Search + sort */}
      <div style={{ display:"flex", gap:12, marginBottom:18, flexWrap:"wrap", alignItems:"center" }}>
        <div style={{ position:"relative", flex:1, minWidth:220 }}>
          <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", fontSize:15 }}>🔍</span>
          <input value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search district…"
            style={{ width:"100%", padding:"10px 12px 10px 38px", border:"1px solid #e2e8f0",
              borderRadius:11, fontSize:13, background:"white", boxSizing:"border-box" }}/>
        </div>
        <select value={sortBy} onChange={e => setSortBy(e.target.value)}
          style={{ padding:"10px 14px", border:"1px solid #e2e8f0", borderRadius:11, fontSize:13, background:"white" }}>
          <option value="sat_desc">Saturation: High → Low</option>
          <option value="sat_asc">Saturation: Low → High</option>
          <option value="name">District: A – Z</option>
        </select>
        <span style={{ fontSize:12, color:"#94a3b8" }}>{filtered.length} of {rows.length} districts</span>
      </div>

      {/* District table */}
      <div style={{ background:"white", borderRadius:18, border:"1px solid #e5e7eb", overflow:"hidden", boxShadow:"0 6px 18px rgba(15,23,42,0.04)" }}>
        {/* Column headers */}
        <div style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
          padding:"14px 20px", background:"#f8fafc", borderBottom:"1px solid #e5e7eb", gap:12 }}>
          {columns.map(col => (
            <div key={col.key} style={{ fontSize:11, fontWeight:800, color:"#94a3b8", letterSpacing:"0.05em", textTransform:"uppercase" }}>
              {col.label}
            </div>
          ))}
        </div>

        {/* Data rows */}
        {filtered.map((row, idx) => (
          <div key={`${row.district}-${idx}`}
            style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
              padding:"15px 20px", gap:12, alignItems:"center",
              borderBottom:"1px solid #f1f5f9", background:idx % 2 === 0 ? "white" : "#fcfcfd" }}>

            {/* District */}
            <div style={{ display:"flex", alignItems:"center", gap:10 }}>
              <span style={{ width:10, height:10, borderRadius:"50%", flexShrink:0,
                background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}/>
              <span style={{ fontWeight:800, fontSize:14, color:"#0f172a" }}>{row.district}</span>
            </div>

            {/* Accounts */}
            <div style={{ fontSize:13, color:"#0369a1", fontWeight:600 }}>{row.accounts}</div>

            {/* Zero balance */}
            <div style={{ fontSize:13, color:"#dc2626", fontWeight:600 }}>{row.zero_bal_pct}</div>

            {/* RuPay cards */}
            <div style={{ fontSize:13, color:"#16a34a", fontWeight:600 }}>{row.rupay_cards}</div>

            {/* Saturation progress bar */}
            <div>
              <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                <div style={{ flex:1, height:8, background:"#dbe4f0", borderRadius:999, overflow:"hidden" }}>
                  <div style={{
                    width:`${Math.min(row.saturation_pct || 0, 100)}%`, height:"100%", borderRadius:999,
                    background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444",
                    transition:"width 0.35s ease"
                  }}/>
                </div>
                <span style={{ minWidth:40, textAlign:"right", fontWeight:800, fontSize:13,
                  color: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}>
                  {row.saturation_pct}%
                </span>
              </div>
            </div>

            {/* Status pill */}
            <div>{renderStatusPill(row)}</div>
          </div>
        ))}

        {/* Footer */}
        <div style={{ display:"flex", justifyContent:"space-between", gap:10, padding:"12px 20px",
          background:"#f8fafc", color:"#94a3b8", fontSize:11 }}>
          <span>{dashboard.live ? "Live feed" : "Weekly report data"} · {dashboard.source}</span>
          <span>Fetched {timeAgo(dashboard.scraped_at)}</span>
        </div>
      </div>

      {/* Source links */}
      <div style={{ display:"flex", gap:12, marginTop:18, flexWrap:"wrap" }}>
        {[
          { label:"PMJDY Official Portal",      url:"https://pmjdy.gov.in/account",                color:"#0ea5e9" },
          { label:"State-wise Progress Report", url:"https://pmjdy.gov.in/statewise-statistics",  color:"#0369a1" },
          { label:"Weekly Progress Report",     url:"https://pmjdy.gov.in/scheme",                color:"#0284c7" },
        ].map((link, i) => (
          <a key={i} href={link.url} target="_blank" rel="noreferrer" style={{
            display:"inline-flex", alignItems:"center", gap:6,
            background:`${link.color}12`, color:link.color,
            border:`1px solid ${link.color}30`, borderRadius:999,
            padding:"8px 16px", fontWeight:700, fontSize:12, textDecoration:"none"
          }}>
            {link.label} ↗
          </a>
        ))}
      </div>
    </div>
  );
}

// ── PMAY-G Tab ──────────────────────────────────────────────────────────────
function PmayGTab({ schemeDashboards, agg, onScrapeAll }) {
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("comp_desc");

  const dashboard = schemeDashboards.find(d => d.id === "pmayg") || null;

  if (!dashboard && !agg) return <EmptyState onScrape={onScrapeAll}/>;
  if (!dashboard) return (
    <div style={{ background:"white", borderRadius:16, border:"2px dashed #e5e7eb", padding:60, textAlign:"center" }}>
      <div style={{ fontSize:40, marginBottom:12 }}>🏠</div>
      <div style={{ fontWeight:800, fontSize:18, color:"#0f172a", marginBottom:8 }}>PMAY-G data loading…</div>
      <div style={{ color:"#64748b", fontSize:13, marginBottom:20 }}>Click Refresh to fetch rural housing data from rhreporting.nic.in</div>
      <button onClick={onScrapeAll} style={{ background:"#f97316", color:"white", borderRadius:12, padding:"12px 28px", fontWeight:800, fontSize:14, border:"none", cursor:"pointer" }}>⚡ Fetch PMAY-G Data</button>
    </div>
  );

  const { summary, rows = [], columns = [] } = dashboard;
  const totals = summary?.state_totals || {};

  const filtered = [...rows]
    .filter(r => !search || String(r.district || "").toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      if (sortBy === "comp_asc") return (a.completion_pct || 0) - (b.completion_pct || 0);
      if (sortBy === "name")     return String(a.district || "").localeCompare(String(b.district || ""));
      return (b.completion_pct || 0) - (a.completion_pct || 0);
    });

  const renderStatusPill = (row) => {
    const tone = row.status_tone;
    const styles = tone === "good"
      ? { bg:"#d1fae5", color:"#047857" }
      : tone === "watch"
      ? { bg:"#fef9c3", color:"#ca8a04" }
      : { bg:"#fee2e2", color:"#dc2626" };
    return (
      <span style={{ display:"inline-flex", alignItems:"center", justifyContent:"center",
        borderRadius:999, padding:"6px 14px", fontWeight:700, fontSize:12,
        background:styles.bg, color:styles.color, minWidth:108 }}>
        {row.status}
      </span>
    );
  };

  return (
    <div className="fadeup">
      <div style={{ display:"flex", alignItems:"center", gap:4, marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
          PMAY-G (Rajasthan) — <span style={{ color:"#f97316" }}>Rural Housing</span>
        </h2>
        <InfoTip text="Pradhan Mantri Awas Yojana Gramin — district-wise rural housing data from rhreporting.nic.in MIS and pmayg.dord.gov.in. Completion rate = houses completed / houses sanctioned × 100."/>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:4 }}>
        Source: {dashboard.source} · {dashboard.report_label}
      </p>
      <div style={{ display:"inline-flex", alignItems:"center", gap:8, marginBottom:20,
        background:"#fff7ed", border:"1px solid #fed7aa", color:"#9a3412",
        borderRadius:999, padding:"7px 14px", fontSize:12.5, fontWeight:800 }}>
        <span style={{ width:8, height:8, borderRadius:"50%", background:dashboard.live ? "#f97316" : "#fdba74", display:"inline-block" }}/>
        {dashboard.verified_label}
      </div>

      {/* KPI summary cards */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:14, marginBottom:24 }}>
        {[
          { value:summary?.primary || "—",   label:summary?.primaryLabel || "Avg Completion Rate", bg:"#fff7ed", border:"#fed7aa", numC:"#c2410c", txtC:"#9a3412" },
          { value:summary?.good ?? 0,         label:summary?.goodLabel || "Districts ≥85%",         bg:"#f0fdf4", border:"#bbf7d0", numC:"#16a34a", txtC:"#166534" },
          { value:summary?.watch ?? 0,        label:summary?.watchLabel || "Districts 70–85%",      bg:"#fffbeb", border:"#fde68a", numC:"#ea580c", txtC:"#9a3412" },
          { value:summary?.critical ?? 0,     label:summary?.criticalLabel || "Critical",            bg:"#fff1f2", border:"#fecdd3", numC:"#e11d48", txtC:"#9f1239" },
        ].map((item, i) => (
          <div key={i} style={{ background:item.bg, border:`1.5px solid ${item.border}`, borderRadius:16, padding:"18px 22px" }}>
            <div style={{ fontSize:i===0?32:46, lineHeight:1, fontWeight:900, color:item.numC, marginBottom:8 }}>{item.value}</div>
            <div style={{ fontSize:13, fontWeight:700, color:item.txtC }}>{item.label}</div>
          </div>
        ))}
      </div>

      {/* State totals banner */}
      {totals.sanctioned && (
        <div style={{ background:"linear-gradient(135deg,#fff7ed,#ffedd5)", border:"1.5px solid #fed7aa",
          borderRadius:14, padding:"14px 20px", marginBottom:22, display:"flex", flexWrap:"wrap", gap:20, alignItems:"center" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7 }}>
            <span style={{ fontSize:18 }}>🏠</span>
            <span style={{ fontWeight:800, fontSize:14, color:"#7c2d12" }}>Rajasthan State Totals ({totals.year})</span>
          </div>
          {[
            { label:"Sanctioned",      val:totals.sanctioned,      color:"#c2410c" },
            { label:"Completed",       val:totals.completed,       color:"#16a34a" },
            { label:"In Progress",     val:totals.in_progress,     color:"#0369a1" },
            { label:"Completion Rate", val:totals.completion_rate, color:"#f97316" },
            { label:"Funds Released",  val:totals.funds_released,  color:"#7c3aed" },
          ].map((item, i) => (
            <div key={i} style={{ textAlign:"center" }}>
              <div style={{ fontSize:15, fontWeight:900, color:item.color }}>{item.val}</div>
              <div style={{ fontSize:10, color:"#9a3412", fontWeight:600 }}>{item.label}</div>
            </div>
          ))}
        </div>
      )}

      <p style={{ color:"#94a3b8", fontSize:12, marginBottom:18 }}>
        Source: {dashboard.source} · {dashboard.note}
      </p>

      {/* Search + sort */}
      <div style={{ display:"flex", gap:12, marginBottom:18, flexWrap:"wrap", alignItems:"center" }}>
        <div style={{ position:"relative", flex:1, minWidth:220 }}>
          <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", fontSize:15 }}>🔍</span>
          <input value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search district…"
            style={{ width:"100%", padding:"10px 12px 10px 38px", border:"1px solid #e2e8f0",
              borderRadius:11, fontSize:13, background:"white", boxSizing:"border-box" }}/>
        </div>
        <select value={sortBy} onChange={e => setSortBy(e.target.value)}
          style={{ padding:"10px 14px", border:"1px solid #e2e8f0", borderRadius:11, fontSize:13, background:"white" }}>
          <option value="comp_desc">Completion: High → Low</option>
          <option value="comp_asc">Completion: Low → High</option>
          <option value="name">District: A – Z</option>
        </select>
        <span style={{ fontSize:12, color:"#94a3b8" }}>{filtered.length} of {rows.length} districts</span>
      </div>

      {/* District table */}
      <div style={{ background:"white", borderRadius:18, border:"1px solid #e5e7eb", overflow:"hidden", boxShadow:"0 6px 18px rgba(15,23,42,0.04)" }}>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
          padding:"14px 20px", background:"#f8fafc", borderBottom:"1px solid #e5e7eb", gap:12 }}>
          {columns.map(col => (
            <div key={col.key} style={{ fontSize:11, fontWeight:800, color:"#94a3b8", letterSpacing:"0.05em", textTransform:"uppercase" }}>
              {col.label}
            </div>
          ))}
        </div>

        {filtered.map((row, idx) => (
          <div key={`${row.district}-${idx}`}
            style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
              padding:"15px 20px", gap:12, alignItems:"center",
              borderBottom:"1px solid #f1f5f9", background:idx % 2 === 0 ? "white" : "#fcfcfd" }}>

            <div style={{ display:"flex", alignItems:"center", gap:10 }}>
              <span style={{ width:10, height:10, borderRadius:"50%", flexShrink:0,
                background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}/>
              <span style={{ fontWeight:800, fontSize:14, color:"#0f172a" }}>{row.district}</span>
            </div>
            <div style={{ fontSize:13, color:"#c2410c", fontWeight:600 }}>{row.sanctioned}</div>
            <div style={{ fontSize:13, color:"#16a34a", fontWeight:600 }}>{row.completed}</div>
            <div style={{ fontSize:13, color:"#0369a1", fontWeight:600 }}>{row.in_progress}</div>

            <div>
              <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                <div style={{ flex:1, height:8, background:"#dbe4f0", borderRadius:999, overflow:"hidden" }}>
                  <div style={{
                    width:`${Math.min(row.completion_pct || 0, 100)}%`, height:"100%", borderRadius:999,
                    background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444",
                    transition:"width 0.35s ease"
                  }}/>
                </div>
                <span style={{ minWidth:40, textAlign:"right", fontWeight:800, fontSize:13,
                  color: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}>
                  {row.completion_pct}%
                </span>
              </div>
            </div>
            <div>{renderStatusPill(row)}</div>
          </div>
        ))}

        <div style={{ display:"flex", justifyContent:"space-between", gap:10, padding:"12px 20px",
          background:"#f8fafc", color:"#94a3b8", fontSize:11 }}>
          <span>{dashboard.live ? "Live feed" : "MIS report data"} · {dashboard.source}</span>
          <span>Fetched {timeAgo(dashboard.scraped_at)}</span>
        </div>
      </div>

      <div style={{ display:"flex", gap:12, marginTop:18, flexWrap:"wrap" }}>
        {[
          { label:"PMAY-G MIS Dashboard",   url:"https://rhreporting.nic.in/netiay/PhysicalProgressReport/physicalProgressMainReport.aspx", color:"#f97316" },
          { label:"PMAY-G Official Portal",  url:"https://pmayg.dord.gov.in/",                                                              color:"#c2410c" },
          { label:"AwaasSoft MIS",           url:"https://rhreporting.nic.in/",                                                             color:"#0369a1" },
        ].map((link, i) => (
          <a key={i} href={link.url} target="_blank" rel="noreferrer" style={{
            display:"inline-flex", alignItems:"center", gap:6,
            background:`${link.color}12`, color:link.color,
            border:`1px solid ${link.color}30`, borderRadius:999,
            padding:"8px 16px", fontWeight:700, fontSize:12, textDecoration:"none"
          }}>
            {link.label} ↗
          </a>
        ))}
      </div>
    </div>
  );
}

// ── Scholarship Tab ─────────────────────────────────────────────────────────────
function ScholarshipTab({ schemeDashboards, agg, onScrapeAll }) {
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("approved_desc");

  const dashboard = schemeDashboards.find(d => d.id === "scholarship") || null;

  if (!dashboard && !agg) return <EmptyState onScrape={onScrapeAll}/>;

  if (!dashboard) return (
    <div style={{ background:"white", borderRadius:16, border:"2px dashed #e5e7eb", padding:60, textAlign:"center" }}>
      <div style={{ fontSize:40, marginBottom:12 }}>🎓</div>
      <div style={{ fontWeight:800, fontSize:18, color:"#0f172a", marginBottom:8 }}>Scholarship data loading…</div>
      <div style={{ color:"#64748b", fontSize:13, marginBottom:20 }}>Click Refresh to fetch SC/ST/OBC scholarship data from NSP + SJE Rajasthan</div>
      <button onClick={onScrapeAll} style={{ background:"#8b5cf6", color:"white", borderRadius:12, padding:"12px 28px", fontWeight:800, fontSize:14, border:"none", cursor:"pointer" }}>⚡ Fetch Scholarship Data</button>
    </div>
  );

  const { summary, rows = [], columns = [] } = dashboard;
  const totals = summary?.state_totals || {};

  const filtered = [...rows]
    .filter(r => !search || String(r.district || "").toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      if (sortBy === "approved_asc") return (a.approved_pct || 0) - (b.approved_pct || 0);
      if (sortBy === "name") return String(a.district || "").localeCompare(String(b.district || ""));
      return (b.approved_pct || 0) - (a.approved_pct || 0);
    });

  const renderStatusPill = (row) => {
    const tone = row.status_tone || (row.approved_pct >= 75 ? "good" : row.approved_pct >= 60 ? "watch" : "critical");
    const styles = tone === "good"
      ? { bg:"#d1fae5", color:"#047857" }
      : tone === "watch"
      ? { bg:"#fef9c3", color:"#ca8a04" }
      : { bg:"#fee2e2", color:"#dc2626" };
    return (
      <span style={{ display:"inline-flex", alignItems:"center", justifyContent:"center",
        borderRadius:999, padding:"6px 14px", fontWeight:700, fontSize:12,
        background:styles.bg, color:styles.color, minWidth:108 }}>
        {row.status}
      </span>
    );
  };

  return (
    <div className="fadeup">
      {/* Header */}
      <div style={{ display:"flex", alignItems:"center", gap:4, marginBottom:4 }}>
        <h2 style={{ fontSize:24, fontWeight:900, margin:0 }}>
          Scholarship (SC/ST/OBC) — <span style={{ color:"#8b5cf6" }}>NSP + SJE Rajasthan</span>
        </h2>
        <InfoTip text="Post-Matric and Pre-Matric scholarship data from National Scholarship Portal (scholarships.gov.in) and SJE Rajasthan (sje.rajasthan.gov.in). Covers SC, ST, and OBC categories."/>
      </div>
      <p style={{ color:"#6b7280", fontSize:13, marginBottom:4 }}>
        Source: {dashboard.source} · {dashboard.report_label}
      </p>
      <div style={{ display:"inline-flex", alignItems:"center", gap:8, marginBottom:20,
        background:"#f5f3ff", border:"1px solid #ddd6fe", color:"#6d28d9",
        borderRadius:999, padding:"7px 14px", fontSize:12.5, fontWeight:800 }}>
        <span style={{ width:8, height:8, borderRadius:"50%", background:dashboard.live ? "#8b5cf6" : "#a78bfa", display:"inline-block" }}/>
        {dashboard.verified_label}
      </div>

      {/* State-level KPI cards */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:14, marginBottom:24 }}>
        {[
          { value:summary?.primary || "—",          label:summary?.primaryLabel || "Approval Rate",    bg:"#eff6ff", border:"#bfdbfe", numC:"#4338ca", txtC:"#3730a3" },
          { value:summary?.good ?? 0,                label:summary?.goodLabel || "Districts >75%",     bg:"#f0fdf4", border:"#bbf7d0", numC:"#16a34a", txtC:"#166534" },
          { value:summary?.watch ?? 0,               label:summary?.watchLabel || "Districts 60–75%",  bg:"#fffbeb", border:"#fde68a", numC:"#ea580c", txtC:"#9a3412" },
          { value:summary?.critical ?? 0,            label:summary?.criticalLabel || "Critical",       bg:"#fff1f2", border:"#fecdd3", numC:"#e11d48", txtC:"#9f1239" },
        ].map((item, i) => (
          <div key={i} style={{ background:item.bg, border:`1.5px solid ${item.border}`, borderRadius:16, padding:"18px 22px" }}>
            <div style={{ fontSize:i===0?32:46, lineHeight:1, fontWeight:900, color:item.numC, marginBottom:8 }}>{item.value}</div>
            <div style={{ fontSize:13, fontWeight:700, color:item.txtC }}>{item.label}</div>
          </div>
        ))}
      </div>

      {/* State totals banner */}
      {totals.total_applicants && (
        <div style={{ background:"linear-gradient(135deg,#f5f3ff,#ede9fe)", border:"1.5px solid #ddd6fe",
          borderRadius:14, padding:"14px 20px", marginBottom:22, display:"flex", flexWrap:"wrap", gap:20, alignItems:"center" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7 }}>
            <span style={{ fontSize:18 }}>🎓</span>
            <span style={{ fontWeight:800, fontSize:14, color:"#4c1d95" }}>Rajasthan State Totals ({totals.year})</span>
          </div>
          {[
            { label:"Total Applicants", val:totals.total_applicants, color:"#6d28d9" },
            { label:"SC Applicants",    val:totals.sc_applicants,    color:"#1d4ed8" },
            { label:"ST Applicants",    val:totals.st_applicants,    color:"#065f46" },
            { label:"OBC Applicants",   val:totals.obc_applicants,   color:"#92400e" },
            { label:"Approved",         val:totals.approved,         color:"#166534" },
            { label:"Disbursed",        val:totals.disbursed,        color:"#7c3aed" },
          ].map((item, i) => (
            <div key={i} style={{ textAlign:"center" }}>
              <div style={{ fontSize:16, fontWeight:900, color:item.color }}>{item.val}</div>
              <div style={{ fontSize:10, color:"#7c3aed", fontWeight:600 }}>{item.label}</div>
            </div>
          ))}
        </div>
      )}

      {/* Official source note */}
      <p style={{ color:"#94a3b8", fontSize:12, marginBottom:18 }}>
        Source: {dashboard.source} · {dashboard.note}
      </p>

      {/* Search + sort */}
      <div style={{ display:"flex", gap:12, marginBottom:18, flexWrap:"wrap", alignItems:"center" }}>
        <div style={{ position:"relative", flex:1, minWidth:220 }}>
          <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", fontSize:15 }}>🔍</span>
          <input value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search district…"
            style={{ width:"100%", padding:"10px 12px 10px 38px", border:"1px solid #e2e8f0",
              borderRadius:11, fontSize:13, background:"white", boxSizing:"border-box" }}/>
        </div>
        <select value={sortBy} onChange={e => setSortBy(e.target.value)}
          style={{ padding:"10px 14px", border:"1px solid #e2e8f0", borderRadius:11, fontSize:13, background:"white" }}>
          <option value="approved_desc">Approval: High → Low</option>
          <option value="approved_asc">Approval: Low → High</option>
          <option value="name">District: A – Z</option>
        </select>
        <span style={{ fontSize:12, color:"#94a3b8" }}>{filtered.length} of {rows.length} districts</span>
      </div>

      {/* District table */}
      <div style={{ background:"white", borderRadius:18, border:"1px solid #e5e7eb", overflow:"hidden", boxShadow:"0 6px 18px rgba(15,23,42,0.04)" }}>
        {/* Header */}
        <div style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
          padding:"14px 20px", background:"#f8fafc", borderBottom:"1px solid #e5e7eb", gap:12 }}>
          {columns.map(col => (
            <div key={col.key} style={{ fontSize:11, fontWeight:800, color:"#94a3b8", letterSpacing:"0.05em", textTransform:"uppercase" }}>
              {col.label}
            </div>
          ))}
        </div>

        {/* Rows */}
        {filtered.map((row, idx) => (
          <div key={`${row.district}-${idx}`}
            style={{ display:"grid", gridTemplateColumns:"repeat(6,minmax(0,1fr))",
              padding:"15px 20px", gap:12, alignItems:"center",
              borderBottom:"1px solid #f1f5f9", background:idx % 2 === 0 ? "white" : "#fcfcfd" }}>

            {/* District name */}
            <div style={{ display:"flex", alignItems:"center", gap:10 }}>
              <span style={{ width:10, height:10, borderRadius:"50%", flexShrink:0,
                background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}/>
              <span style={{ fontWeight:800, fontSize:14, color:"#0f172a" }}>{row.district}</span>
            </div>

            {/* SC */}
            <div style={{ fontSize:13, color:"#1d4ed8", fontWeight:600 }}>{row.sc_applicants}</div>

            {/* ST */}
            <div style={{ fontSize:13, color:"#065f46", fontWeight:600 }}>{row.st_applicants}</div>

            {/* OBC */}
            <div style={{ fontSize:13, color:"#92400e", fontWeight:600 }}>{row.obc_applicants}</div>

            {/* Approval rate progress bar */}
            <div>
              <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                <div style={{ flex:1, height:8, background:"#dbe4f0", borderRadius:999, overflow:"hidden" }}>
                  <div style={{
                    width:`${Math.min(row.approved_pct || 0, 100)}%`, height:"100%", borderRadius:999,
                    background: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444",
                    transition:"width 0.35s ease"
                  }}/>
                </div>
                <span style={{ minWidth:40, textAlign:"right", fontWeight:800, fontSize:13,
                  color: row.status_tone === "good" ? "#16a34a" : row.status_tone === "watch" ? "#f97316" : "#ef4444" }}>
                  {row.approved_pct}%
                </span>
              </div>
            </div>

            {/* Status pill */}
            <div>{renderStatusPill(row)}</div>
          </div>
        ))}

        {/* Footer */}
        <div style={{ display:"flex", justifyContent:"space-between", gap:10, padding:"12px 20px",
          background:"#f8fafc", color:"#94a3b8", fontSize:11 }}>
          <span>{dashboard.live ? "Live feed" : "Verified report data"} · {dashboard.source}</span>
          <span>Fetched {timeAgo(dashboard.scraped_at)}</span>
        </div>
      </div>

      {/* Source links */}
      <div style={{ display:"flex", gap:12, marginTop:18, flexWrap:"wrap" }}>
        {[
          { label:"National Scholarship Portal", url:"https://scholarships.gov.in/", color:"#8b5cf6" },
          { label:"SJE Rajasthan", url:"https://sje.rajasthan.gov.in/", color:"#6d28d9" },
          { label:"NSP Rajasthan Schemes", url:"https://scholarships.gov.in/public/schemeData/getSchemeList?stateCode=08", color:"#7c3aed" },
        ].map((link, i) => (
          <a key={i} href={link.url} target="_blank" rel="noreferrer" style={{
            display:"inline-flex", alignItems:"center", gap:6,
            background:`${link.color}12`, color:link.color,
            border:`1px solid ${link.color}30`, borderRadius:999,
            padding:"8px 16px", fontWeight:700, fontSize:12, textDecoration:"none"
          }}>
            {link.label} ↗
          </a>
        ))}
      </div>
    </div>
  );
}

// ── Root App ──────────────────────────────────────────────────────────────────
export default function App() {
  const [tab,setTab]           = useState("dashboard");
  const [agg,setAgg]           = useState(null);
  const [srcStatus,setStatus]  = useState({});
  const [scraping,setScraping] = useState({});
  const [scrapingAll,setAll]   = useState(false);
  const [refreshing,setRef]    = useState(false);
  const [online,setOnline]     = useState(null);
  const [now,setNow]           = useState(new Date());
  const [scrapeLog,setLog]     = useState([]);
  const [budget,setBudget]     = useState(null);
  const [budgetLoading,setBudgetLoading] = useState(false);
  const [rajrasData, setRajrasData] = useState([]);
  const [jansoochnaData, setJansoochnaData] = useState([]);
  const [schemeDashboards, setSchemeDashboards] = useState([]);
  const [desktopViewport, setDesktopViewport] = useState(isDesktopViewport);

  const addLog = useCallback((msg,type="info")=>{
    const ts=new Date().toLocaleTimeString("en-IN",{hour:"2-digit",minute:"2-digit",second:"2-digit"});
    setLog(p=>[{ts,msg,type},...p].slice(0,30));
  },[]);

  useEffect(()=>{ const t=setInterval(()=>setNow(new Date()),1000); return ()=>clearInterval(t); },[]);
  useEffect(() => {
    const onResize = () => setDesktopViewport(isDesktopViewport());
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  const poll = useCallback(async(silent=true)=>{
    if(!silent) setRef(true);
    try {
      const [s,a,rj,jsp,ms,ig,sd]=await Promise.all([
        axios.get(`${API}/status`).catch(()=>null),
        axios.get(`${API}/aggregate`).catch(()=>null),
        axios.get(`${API}/data/rajras`).catch(()=>null),
        axios.get(`${API}/data/jansoochna`).catch(()=>null),
        axios.get(`${API}/data/myscheme`).catch(()=>null),
        axios.get(`${API}/data/igod`).catch(()=>null),
        axios.get(`${API}/scheme-dashboards`).catch(()=>null),
      ]);
      const nextStatus = s?.data?.sources || {};
      const rajrasRows = Array.isArray(rj?.data) ? rj.data : [];
      const jansoochnaRows = Array.isArray(jsp?.data) ? jsp.data : [];
      const myschemeRows = Array.isArray(ms?.data?.data) ? ms.data.data : [];
      const igodRows = Array.isArray(ig?.data?.data) ? ig.data.data : [];

      if(s) setStatus(nextStatus);
      if(a?.data) {
        setAgg(a.data);
      } else {
        setAgg(buildFallbackAggregate({
          sourceStatus: nextStatus,
          rajras: rajrasRows,
          jansoochna: jansoochnaRows,
          myscheme: myschemeRows,
          igod: igodRows,
        }));
      }
      setRajrasData(rajrasRows);
      setJansoochnaData(jansoochnaRows);
      setSchemeDashboards(Array.isArray(sd?.data?.data) ? sd.data.data : []);
      if(!silent) addLog("✅ Data refreshed","success");
    } catch(e){ if(!silent) addLog("❌ Refresh failed","error"); }
    if(!silent) setRef(false);
  },[addLog]);

  const fetchBudget = useCallback(async()=>{
    if(budget) return;
    setBudgetLoading(true);
    try { const r=await axios.get(`${API}/budget`); setBudget(r.data); } catch(e){}
    setBudgetLoading(false);
  },[budget]);

  useEffect(()=>{ fetchBudget(); },[fetchBudget]);
  useEffect(()=>{
    axios.get(`${API}/`).then(()=>setOnline(true)).catch(()=>setOnline(false));
    poll(true);
    const id=setInterval(()=>poll(true),8000);
    return ()=>clearInterval(id);
  },[poll]);

  const scrapeOne = useCallback(async sid=>{
    setScraping(p=>({...p,[sid]:true}));
    addLog(`⚡ Scraping ${SRC[sid]?.label||sid}…`,"info");
    try {
      await axios.post(`${API}/scrape/${sid}`);
      await poll(true);
      addLog(`✅ ${SRC[sid]?.label||sid} — done`,"success");
    } catch(e){ addLog(`❌ ${sid} scrape failed`,"error"); }
    setScraping(p=>({...p,[sid]:false}));
  },[poll,addLog]);

  const scrapeAll = useCallback(async()=>{
    setAll(true);
    addLog("⚡ Scraping all 4 sources…","info");
    try {
      await axios.post(`${API}/scrape/all`);
      try {
        const budgetResponse = await axios.get(`${API}/budget?refresh=true`);
        setBudget(budgetResponse.data);
      } catch (e) {}
      await poll(true);
      addLog(`✅ Scrape complete — ${agg?.kpis?.total_schemes||0} schemes`,"success");
    } catch(e){ addLog("❌ Scrape failed","error"); }
    setAll(false);
  },[poll,addLog,agg]);

  const criticalCount=(agg?.alerts||[]).filter(a=>a.severity==="Critical").length;
  const totalSchemes=agg?.kpis?.total_schemes||0;
  const totalPortals=agg?.kpis?.total_portals||0;

  const TABS=[
    {id:"dashboard",label:"Dashboard",icon:"◉"},
    {id:"schemes",label:"Schemes",icon:"⊞",badge:totalSchemes||null},
    {id:"igod",label:"IGOD",icon:"🏛️",badge:totalPortals||null},
    {id:"budget",label:"Budget Data",icon:"₹"},
    {id:"districts",label:"Districts-Insights",icon:"🗺️"},
    //{id:"pmjdy",label:"Jan Dhan",icon:"🏦"},
   // {id:"sbmg",label:"SBM-G",icon:"🚿"},
   // {id:"pmgdisha",label:"PMGDISHA",icon:"💻"},
   // {id:"saubhagya",label:"Saubhagya",icon:"⚡"},
   // {id:"mgnrega_raj",label:"MGNREGA",icon:"🏗️"},
   // {id:"pmfby",label:"PMFBY",icon:"🌾"},
   // {id:"pmayg",label:"PMAY-G",icon:"🏠"},
   // {id:"scholarship",label:"Scholarship",icon:"🎓"},
    {id:"alerts",label:"Live Alerts",icon:"⚡",badge:criticalCount||null},
    {id:"insights",label:"AI Insights",icon:"🧠",highlight:true},
  ];

  return (
    <div style={{
      minHeight:"100vh",
      background:`linear-gradient(180deg, ${THEME.sandstone} 0%, #f5f7fb 32%, #eaf2fb 100%)`,
      color:"#0f172a",
      fontFamily:"\"Trebuchet MS\", \"Segoe UI\", \"Noto Sans\", sans-serif",
      position:"relative",
    }}>
      <div style={{
        position:"absolute",
        inset:0,
        background:"radial-gradient(circle at top right, rgba(15,124,151,0.12), transparent 28%), radial-gradient(circle at top left, rgba(229,106,31,0.12), transparent 32%), linear-gradient(rgba(255,255,255,0.18) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.18) 1px, transparent 1px)",
        backgroundSize:"auto, auto, 30px 30px, 30px 30px",
        pointerEvents:"none",
        opacity:0.9,
      }}/>
      <div style={{ background:"rgba(255,255,255,0.82)", borderBottom:`1px solid ${THEME.border}`,
        position:"sticky", top:0, zIndex:100, boxShadow:"0 16px 36px rgba(15,23,42,0.08)", backdropFilter:"blur(16px)" }}>

        <div style={{ display:"flex", alignItems:"center", gap:12, padding:"14px 22px", flexWrap:"wrap" }}>
          <div style={{ width:52, height:52, borderRadius:18, background:`linear-gradient(135deg,${THEME.saffron},${THEME.saffronDeep})`,
            display:"flex", alignItems:"center", justifyContent:"center",
            color:"white", fontWeight:900, fontSize:17, flexShrink:0, boxShadow:"0 18px 34px rgba(229,106,31,0.32)" }}>AI</div>
          <div style={{ minWidth:0 }}>
            <div style={{ fontWeight:900, fontSize:18, color:"#132238", letterSpacing:"-0.03em" }}>AI Chief of Staff</div>
            <div style={{ fontSize:10.5, color:"#7b8794", letterSpacing:"0.10em" }}>OFFICE OF CM · RAJASTHAN · VERIFIED GOVERNANCE DATA</div>
          </div>
          <div style={{ flex:"1 1 120px" }}/>
          {desktopViewport && (
            <div style={{ background:`linear-gradient(135deg,${THEME.peacockSoft},#f3fcff)`, border:"1px solid #c8eaf0", borderRadius:16,
              padding:"10px 16px", display:"flex", alignItems:"center", gap:7, fontSize:12, color:THEME.peacock, fontWeight:700, boxShadow:"0 8px 22px rgba(15,124,151,0.08)" }}>
              <span>📚</span><span>Sources: Budget 2025-26 · JJM MIS · PRS India</span>
            </div>
          )}
          <div style={{ background:"linear-gradient(135deg,#ffffff,#f6fff9)", border:"1.5px solid #bbf7d0", borderRadius:16,
            padding:"9px 14px", display:"flex", alignItems:"center", gap:7, boxShadow:"0 8px 20px rgba(34,197,94,0.07)" }}>
            <div style={{ width:9, height:9, borderRadius:"50%", background:"#10b981", boxShadow:"0 0 0 3px #d1fae5" }}/>
            <span style={{ fontSize:13, fontWeight:700, color:"#166534" }}>Verified Data</span>
          </div>
          <ScrapeNowButton onClick={scrapeAll} loading={scrapingAll} disabled={!online}/>
          <div style={{ display:"flex", alignItems:"center", gap:10, paddingLeft:14, borderLeft:`1px solid ${THEME.border}`, background:"rgba(255,255,255,0.86)", borderRadius:18, padding:"8px 12px 8px 14px", boxShadow:"0 12px 26px rgba(15,23,42,0.05)", flex:"0 1 auto", minWidth:0 }}>
            <div style={{ width:38, height:38, borderRadius:12, background:"#fee2e2",
              display:"flex", alignItems:"center", justifyContent:"center", fontSize:18 }}>👤</div>
            <div style={{ minWidth:0 }}>
              <div style={{ fontWeight:800, fontSize:13, color:"#1f2937" }}>Bhajan Lal Sharma</div>
              <div style={{ fontSize:11, color:"#8b97a7" }}>Chief Minister, Rajasthan</div>
            </div>
          </div>
        </div>

        <div style={{ display:"flex", gap:8, padding:"10px 24px", background:"rgba(250,251,252,0.88)",
          borderTop:"1px solid #f3f4f6", overflowX:"auto" }}>
          {Object.entries(SRC).map(([sid,s])=>{
            const st=srcStatus[sid]||{};
            const targetTab = sid === "igod" ? "igod" : sid === "myscheme" || sid === "rajras" || sid === "jansoochna" ? "schemes" : "dashboard";
            return (
              <button
                key={sid}
                onClick={()=>setTab(targetTab)}
                style={{ display:"flex", alignItems:"center", gap:6,
                  background:st.status==="ok"?`${s.color}10`:"#f1f5f9",
                  border:`1px solid ${st.status==="ok"?s.color+"30":"#e5e7eb"}`,
                  borderRadius:999, padding:"6px 11px", fontSize:11, whiteSpace:"nowrap",
                  cursor:"pointer" }}
              >
                <StatusDot status={scraping[sid]?"loading":st.status||"idle"} animating={!!scraping[sid]}/>
                <span style={{ fontWeight:600, color:"#374151" }}>{s.icon} {s.label}</span>
                {st.count>0&&<span style={{ color:s.color, fontWeight:800 }}>{st.count}</span>}
                {st.scraped_at&&<span style={{ color:"#94a3b8" }}>{timeAgo(st.scraped_at)}</span>}
              </button>
            );
          })}
          {agg?.scraped_at&&(
            <div style={{ marginLeft:"auto", fontSize:11, color:"#94a3b8", alignSelf:"center", whiteSpace:"nowrap" }}>
              Aggregated {timeAgo(agg.scraped_at)}
            </div>
          )}
        </div>

        {scrapeLog.length>0&&(
          <div style={{ padding:"7px 24px", background:"#f8fafc", borderTop:"1px solid #f3f4f6",
            display:"flex", alignItems:"center", gap:10, overflowX:"auto" }}>
            {scrapeLog.slice(0,5).map((log,i)=>(
              <div key={i} style={{ display:"flex", alignItems:"center", gap:5, fontSize:11,
                whiteSpace:"nowrap",
                color:log.type==="success"?"#166534":log.type==="error"?"#991b1b":"#64748b",
                opacity:i===0?1:0.5 }}>
                <span style={{ fontSize:10, color:"#94a3b8" }}>{log.ts}</span>
                <span>{log.msg}</span>
                {i<scrapeLog.slice(0,5).length-1&&<span style={{color:"#d1d5db"}}>·</span>}
              </div>
            ))}
          </div>
        )}

        <div style={{
          display:"flex",
          padding:"4px 14px",
          borderTop:"1px solid #f1f5f9",
          overflowX:"auto",
          WebkitOverflowScrolling:"touch",
          scrollbarWidth:"none",
          background:"linear-gradient(180deg, rgba(255,255,255,0.92), rgba(248,250,252,0.98))",
        }}>
          {TABS.map(t=>(
            <button key={t.id} onClick={()=>setTab(t.id)} style={{
              background:t.highlight&&tab===t.id
                ? "linear-gradient(135deg,#f97316,#ea580c)"
                : tab===t.id
                ? "linear-gradient(135deg,#fff4e8,#ffffff)"
                : t.highlight
                ? "#fff7ed"
                : "transparent",
              borderBottom:!t.highlight&&tab===t.id?"2.5px solid #f97316":!t.highlight?"2.5px solid transparent":"none",
              borderRadius:t.highlight||tab===t.id?"12px":0,
              margin:t.highlight||tab===t.id?"5px 4px":0,
              padding:t.highlight?"8px 16px":"11px 18px",
              fontWeight:tab===t.id?700:500,
              color:t.highlight&&tab===t.id?"white":t.highlight?"#f97316":tab===t.id?"#f97316":"#6b7280",
              fontSize:13, display:"flex", alignItems:"center", gap:6,
              border:t.highlight&&tab!==t.id?"1.5px solid #fed7aa":tab===t.id?"1px solid #fed7aa":"none",
              boxShadow:tab===t.id?"0 10px 24px rgba(249,115,22,0.10)":"none",
              transition:"all .15s",
            }}>
              <span>{t.icon}</span> {t.label}
              {t.badge&&<span style={{ background:t.id==="alerts"?"#ef4444":"#f97316",
                color:"white", borderRadius:20, padding:"1px 7px", fontSize:10, fontWeight:800 }}>{t.badge}</span>}
            </button>
          ))}
          <div style={{ marginLeft:"auto", alignSelf:"center", fontSize:12, color:"#9ca3af", paddingRight:4 }}>
            {now.toLocaleTimeString("en-IN",{hour:"2-digit",minute:"2-digit"})} ·{" "}
            {now.toLocaleDateString("en-IN",{day:"2-digit",month:"short",year:"numeric"})}
          </div>
        </div>
      </div>

      {online===null&&(
        <div style={{ background:"#f0f9ff", borderBottom:"1px solid #bae6fd",
          padding:"8px 28px", display:"flex", gap:10, alignItems:"center" }}>
          <span style={{ animation:"spin 1s linear infinite", display:"inline-block" }}>⚙️</span>
          <span style={{ fontSize:13, color:"#0369a1", fontWeight:600 }}>
            Connecting to backend… (Render free tier may take 30–60 seconds to wake up)
          </span>
        </div>
      )}
      {online===false&&(
        <div style={{ background:"#fef2f2", borderBottom:"1px solid #fecaca",
          padding:"10px 28px", display:"flex", gap:10, alignItems:"center", flexWrap:"wrap" }}>
          <span>⚠️</span>
          <span style={{ fontSize:13, color:"#991b1b", fontWeight:600 }}>Backend sleeping (Render free tier).</span>
          <span style={{ fontSize:13, color:"#991b1b" }}>Click <strong>⚡ Scrape Now</strong> — wakes in ~30s.</span>
          <button onClick={()=>{ setOnline(null); axios.get(`${API}/`).then(()=>setOnline(true)).catch(()=>setOnline(false)); }}
            style={{ background:"#ef4444", color:"white", border:"none",
              borderRadius:8, padding:"5px 14px", fontSize:12, fontWeight:700, cursor:"pointer" }}>
            Retry Connection
          </button>
        </div>
      )}

      <div style={{ maxWidth:1280, margin:"0 auto", padding:"clamp(18px, 3vw, 30px) clamp(12px, 2.5vw, 18px) 36px", position:"relative" }}>
        {tab==="dashboard"&&<DashboardTab agg={agg} srcStatus={srcStatus}
          onScrapeAll={scrapeAll} onScrapeOne={scrapeOne}
          scraping={scraping} scrapingAll={scrapingAll} online={online}
          budget={budget} budgetLoading={budgetLoading}/>}
        {tab==="schemes"&&<SchemesTab agg={agg} rajrasData={rajrasData} jansoochnaData={jansoochnaData} onScrapeAll={scrapeAll}/>}
        {tab==="igod"&&<PortalsTab agg={agg} onScrapeAll={scrapeAll}/>}
        {tab==="budget"&&<BudgetDataTab budget={budget} budgetLoading={budgetLoading}
          onRefresh={()=>{ setBudget(null); setBudgetLoading(true);
            fetch(`${API}/budget?refresh=true`).then(r=>r.json()).then(d=>{setBudget(d);setBudgetLoading(false);}).catch(()=>setBudgetLoading(false)); }}/>}
        {tab==="districts"&&<DistrictsTab agg={agg} onScrapeAll={scrapeAll} schemeDashboards={schemeDashboards}/>}
        {tab==="pmjdy"&&<PmjdyTab schemeDashboards={schemeDashboards} agg={agg} onScrapeAll={scrapeAll}/>}
        {tab==="sbmg"&&<SbmgTab schemeDashboards={schemeDashboards} agg={agg} onScrapeAll={scrapeAll}/>}
        {tab==="pmgdisha"&&<PmgdishaTab schemeDashboards={schemeDashboards} agg={agg} onScrapeAll={scrapeAll}/>}
        {tab==="saubhagya"&&<SaubhagyaTab schemeDashboards={schemeDashboards} agg={agg} onScrapeAll={scrapeAll}/>}
        {tab==="mgnrega_raj"&&<MgnregaTab schemeDashboards={schemeDashboards} agg={agg} onScrapeAll={scrapeAll}/>}
        {tab==="pmfby"&&<PmfbyTab schemeDashboards={schemeDashboards} agg={agg} onScrapeAll={scrapeAll}/>}
        {tab==="pmayg"&&<PmayGTab schemeDashboards={schemeDashboards} agg={agg} onScrapeAll={scrapeAll}/>}
        {tab==="scholarship"&&<ScholarshipTab schemeDashboards={schemeDashboards} agg={agg} onScrapeAll={scrapeAll}/>}
        {tab==="alerts"&&<AlertsTab agg={agg} onScrapeAll={scrapeAll}/>}
        {tab==="insights"&&<InsightsEngine schemes={agg?.schemes||[]} portals={agg?.portals||[]} onScrapeFirst={scrapeAll}/>}
      </div>

      <footer style={{ borderTop:"1px solid #e5e7eb", background:"rgba(255,255,255,0.82)",
        padding:"14px 24px", fontSize:11, color:"#7f8a99",
        display:"flex", justifyContent:"space-between", gap:10, flexWrap:"wrap" }}>
        <span>AI Chief of Staff · Office of Chief Minister, Rajasthan</span>
        <span>Data: IGOD · RajRAS · Jan Soochna · MyScheme.gov.in</span>
      </footer>
    </div>
  );
}
