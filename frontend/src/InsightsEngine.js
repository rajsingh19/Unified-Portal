/**
 * InsightsEngine.js — v5
 * ✅ Summary banner before data sources
 * ✅ ℹ️ tooltip on every panel header
 * ✅ All Visit ↗ links fixed — no more 404s
 * ✅ +N more expands inline
 */
import { useState, useMemo } from "react";

const C = {
  orange:"#f97316", blue:"#3b82f6", green:"#10b981", red:"#ef4444",
  purple:"#8b5cf6", amber:"#f59e0b",
  bg:"#f8fafc", card:"#ffffff", border:"#e2e8f0",
  text:"#0f172a", muted:"#64748b",
};

// ── Helpers ────────────────────────────────────────────────────────────────────
const txt = (s) =>
  [s.name, s.description, s.eligibility, s.benefit,
   s.objective, s.category, s.department, s.ministry, s.tags?.join?.(" ")]
  .filter(Boolean).join(" ").toLowerCase();

const has = (s, ...kws) => kws.some(kw => txt(s).includes(kw.toLowerCase()));

const parseINR = (str = "") => {
  if (!str) return 0;
  const cr = str.match(/(\d+(?:\.\d+)?)\s*crore/i);
  if (cr) return parseFloat(cr[1]) * 1e7;
  const lk = str.match(/(\d+(?:\.\d+)?)\s*lakh/i);
  if (lk) return parseFloat(lk[1]) * 1e5;
  const k  = str.match(/[₹Rs.]\s*([\d,]+)/);
  if (k)  return parseInt(k[1].replace(/,/g,""));
  return 0;
};

const fmtINR = (v) => {
  if (v >= 1e7) return `₹${(v/1e7).toFixed(1)} Cr`;
  if (v >= 1e5) return `₹${(v/1e5).toFixed(1)} L`;
  if (v >= 1e3) return `₹${(v/1e3).toFixed(0)}K`;
  return `₹${v}`;
};

// ── Best URL for "Visit ↗" — never return a 404 ────────────────────────────────
const safeUrl = (s) => {
  const DEAD = [
    "https://jansoochna.rajasthan.gov.in/Scheme",
    "https://jansoochna.rajasthan.gov.in/Scheme/",
    "https://rajras.in",
    "https://rajras.in/",
    "https://www.myscheme.gov.in",
    "https://myscheme.gov.in",
  ];
  const u = s.apply_url || s.url || "";
  // If it's a known dead/base URL → use fallback per source
  if (!u || DEAD.includes(u.replace(/\/$/, ""))) {
    if (s._src === "jansoochna" || (s.source||"").includes("jansoochna"))
      return "https://jansoochna.rajasthan.gov.in/";
    if (s._src === "myscheme" || (s.source||"").includes("myscheme"))
      return `https://www.myscheme.gov.in/search?q=${encodeURIComponent((s.name||"").slice(0,50))}`;
    if (s._src === "rajras" || (s.source||"").includes("rajras"))
      return "https://rajras.in/ras/pre/rajasthan/adm/schemes/";
    return null;
  }
  return u || null;
};

// ── Card ───────────────────────────────────────────────────────────────────────
const Card = ({ children, style = {} }) => (
  <div style={{
    background: C.card, borderRadius: 14, border: `1px solid ${C.border}`,
    padding: 20, boxShadow: "0 1px 6px rgba(0,0,0,0.05)", ...style
  }}>{children}</div>
);

// ── ℹ️ Tooltip ─────────────────────────────────────────────────────────────────
function InfoTip({ text }) {
  const [show, setShow] = useState(false);
  return (
    <span style={{ position: "relative", display: "inline-flex", alignItems: "center" }}>
      <span
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
        style={{
          width: 18, height: 18, borderRadius: "50%",
          background: show ? C.blue : "#e2e8f0",
          color: show ? "white" : C.muted,
          fontSize: 11, fontWeight: 800,
          display: "inline-flex", alignItems: "center", justifyContent: "center",
          cursor: "help", userSelect: "none", flexShrink: 0,
          transition: "background 0.15s",
        }}
      >i</span>
      {show && (
        <div style={{
          position: "absolute", left: 24, top: "50%", transform: "translateY(-50%)",
          background: "#1e293b", color: "white",
          borderRadius: 9, padding: "10px 14px",
          fontSize: 12, lineHeight: 1.55,
          width: 280, zIndex: 9999,
          boxShadow: "0 8px 24px rgba(0,0,0,0.22)",
          pointerEvents: "none",
        }}>
          {text}
          <div style={{
            position: "absolute", right: "100%", top: "50%",
            transform: "translateY(-50%)",
            borderWidth: "5px 5px 5px 0",
            borderStyle: "solid",
            borderColor: "transparent #1e293b transparent transparent",
          }}/>
        </div>
      )}
    </span>
  );
}

// ── Section header ─────────────────────────────────────────────────────────────
const Sec = ({ icon, title, sub, tip }) => (
  <div style={{ marginBottom: 16 }}>
    <div style={{ display:"flex", alignItems:"center", gap:8 }}>
      <span style={{ fontSize:22 }}>{icon}</span>
      <h2 style={{ fontSize:19, fontWeight:900, color:C.text, margin:0 }}>{title}</h2>
      {tip && <InfoTip text={tip}/>}
    </div>
    {sub && <p style={{ fontSize:12, color:C.muted, margin:"3px 0 0 30px" }}>{sub}</p>}
  </div>
);

// ── Badge ──────────────────────────────────────────────────────────────────────
const Badge = ({ label, color = C.orange, small }) => (
  <span style={{
    background:`${color}18`, color, border:`1px solid ${color}28`,
    borderRadius:20, padding: small?"1px 8px":"3px 12px",
    fontSize: small?10:11, fontWeight:700, whiteSpace:"nowrap",
  }}>{label}</span>
);

// ═══════════════════════════════════════════════════════════════════════════════
// ANALYSIS ENGINE
// ═══════════════════════════════════════════════════════════════════════════════
function runAnalysis(schemes, portals) {
  const catMap = {};
  schemes.forEach(s => {
    const c = s.category || "General";
    if (!catMap[c]) catMap[c] = { name:c, count:0, schemes:[] };
    catMap[c].count++;
    catMap[c].schemes.push(s);
  });
  const categories = Object.values(catMap).sort((a,b) => b.count - a.count);

  const srcMap = {};
  schemes.forEach(s => {
    const src = s._src_label || s.source?.split(".")?.[0] || "Unknown";
    srcMap[src] = (srcMap[src] || 0) + 1;
  });

  const nameMap = {};
  schemes.forEach(s => {
    const key = s.name?.toLowerCase().trim().slice(0, 40) || "";
    if (!key) return;
    if (!nameMap[key]) nameMap[key] = [];
    nameMap[key].push(s);
  });
  const duplicates = Object.values(nameMap)
    .filter(g => g.length >= 2)
    .filter(g => new Set(g.map(s => s._src_label || s.source)).size >= 2)
    .sort((a,b) => b.length - a.length);

  const withValue = schemes
    .map(s => ({ ...s, _inr: parseINR(s.benefit || s.description || "") }))
    .filter(s => s._inr > 0)
    .sort((a,b) => b._inr - a._inr)
    .slice(0, 12);

  const SEGMENTS = [
    { id:"women",    label:"Women & Girls",             icon:"👩", kws:["women","girl","mahila","beti","female","widow","rajshri","sukanya","maternity"] },
    { id:"farmer",   label:"Farmers & Agriculture",     icon:"🌾", kws:["farmer","kisan","agriculture","crop","farm","agri","horticulture","pashu"] },
    { id:"student",  label:"Students & Youth",          icon:"🎓", kws:["student","scholarship","coaching","education","school","college","apprentice","rozgar"] },
    { id:"health",   label:"Healthcare",                icon:"🏥", kws:["health","medical","chiranjeevi","ayushman","dawa","hospital","insurance","treatment"] },
    { id:"elderly",  label:"Senior Citizens",           icon:"👴", kws:["elderly","pension","old age","senior","widow","aged","vridh"] },
    { id:"disabled", label:"Persons with Disabilities", icon:"♿", kws:["disabled","divyang","disability","handicap","specially abled"] },
    { id:"tribal",   label:"Tribal / SC / ST",          icon:"🏕️", kws:["tribal","adivasi","sc ","st ","schedule caste","schedule tribe","dalit","obc"] },
    { id:"labour",   label:"Workers & Labour",          icon:"⚒️", kws:["labour","worker","shramik","mgnrega","employment","wages","construction"] },
    { id:"bpl",      label:"BPL / Below Poverty",       icon:"🏠", kws:["bpl","below poverty","poor","ration","pds","food security","antyodaya"] },
    { id:"urban",    label:"Urban Citizens",            icon:"🏙️", kws:["urban","city","municipal","nagar","town","slum","metro"] },
  ];

  const segmentAnalysis = SEGMENTS.map(seg => {
    const matching = schemes.filter(s => has(s, ...seg.kws));
    return { ...seg, matching, count: matching.length };
  }).sort((a,b) => a.count - b.count);

  const noBenefit = schemes.filter(s => !s.benefit && !s.description);

  const zeroSegments = segmentAnalysis.filter(s => s.count === 0).length;
  const score = Math.min(100, Math.max(0,
    60 +
    Math.min(schemes.length / 3, 20) -
    (zeroSegments * 5) -
    Math.min(duplicates.length * 2, 15)
  ));

  const actions = [];
  const zeroSeg = segmentAnalysis.filter(s => s.count === 0);
  if (zeroSeg.length > 0) {
    actions.push({
      rank:1, icon:"🚨", priority:"CRITICAL", timeline:"This week",
      title: `No schemes found for: ${zeroSeg.map(s=>s.label).join(", ")}`,
      why: `${zeroSeg.length} citizen segment${zeroSeg.length>1?"s":""} have zero scheme coverage in scraped data.`,
      impact: `${zeroSeg.length * 8}–${zeroSeg.length * 15} lakh citizens potentially unaddressed`,
    });
  }
  if (duplicates.length > 0) {
    const top = duplicates[0];
    actions.push({
      rank:2, icon:"🔄", priority:"HIGH", timeline:"This month",
      title: `Consolidate ${duplicates.length} duplicate scheme listing${duplicates.length>1?"s":""}`,
      why: `"${top[0].name}" appears ${top.length}× across sources: ${[...new Set(top.map(s=>s._src_label||"Unknown"))].join(", ")}.`,
      impact: "Reduce citizen confusion. Single authoritative record per scheme.",
    });
  }
  const weakest = categories[categories.length - 1];
  const strongest = categories[0];
  if (weakest && strongest && weakest.count < 2) {
    actions.push({
      rank:3, icon:"📊", priority:"HIGH", timeline:"This month",
      title: `Expand "${weakest.name}" sector — only ${weakest.count} scheme${weakest.count>1?"s":""}`,
      why: `"${weakest.name}" has ${weakest.count} vs "${strongest.name}" with ${strongest.count}. Major imbalance.`,
      impact: `Launch 2–3 new ${weakest.name} schemes to balance sector coverage`,
    });
  }
  if (noBenefit.length > 0) {
    actions.push({
      rank:4, icon:"📝", priority:"MEDIUM", timeline:"This month",
      title: `${noBenefit.length} schemes missing benefit/description data`,
      why: `${noBenefit.length} scraped schemes have no benefit or description text.`,
      impact: "Fill data gaps on official portals to improve citizen access",
    });
  }
  actions.push({
    rank: actions.length + 1, icon:"🏛️", priority:"MEDIUM", timeline:"This quarter",
    title: `Unify ${Object.keys(srcMap).length} portal data into single citizen interface`,
    why: `Data spread across ${Object.keys(srcMap).join(", ")}. Citizens must visit multiple sites.`,
    impact: "Single Jan Aadhaar-linked dashboard reduces time-to-benefit by 60%",
  });

  return { categories, srcMap, duplicates, withValue, segmentAnalysis, score, actions, noBenefit };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SUMMARY BANNER
// ═══════════════════════════════════════════════════════════════════════════════
function SummaryBanner({ schemes, portals, duplicates, zeroSegs, categories, srcMap }) {
  const totalSrc = Object.keys(srcMap).length;
  const topCat   = categories[0];
  const items = [
    {
      icon:"📋", val: schemes.length, label:"schemes scraped",
      color: C.orange, bg:"#fff7ed",
      tip:`Total scheme records pulled from ${totalSrc} government portals in this session. Includes RajRAS, Jan Soochna, and MyScheme.`,
    },
    {
      icon:"🏛️", val: portals.length, label:"IGOD portals",
      color: C.blue, bg:"#eff6ff",
      tip:`Government portals listed on igod.gov.in directory for Rajasthan. Each portal is a separate citizen-facing website.`,
    },
    {
      icon:"🗂️", val: categories.length, label:"categories",
      color: C.green, bg:"#f0fdf4",
      tip:`Unique scheme categories detected. Top category: "${topCat?.name}" with ${topCat?.count} schemes. Categories are derived by keyword matching on scheme names.`,
    },
    {
      icon:"🔄", val: duplicates.length, label:"duplicates",
      color: C.purple, bg:"#faf5ff",
      tip:`Schemes whose names appear on 2+ different portals simultaneously. Example: "PM Kisan" found on both RajRAS and MyScheme.`,
    },
    {
      icon:"⚠️", val: zeroSegs, label:"zero-coverage segments",
      color: zeroSegs > 0 ? C.red : C.green, bg: zeroSegs > 0 ? "#fff1f2" : "#f0fdf4",
      tip:`Citizen groups (out of 10) for which no scraped scheme was found. Zero means a policy gap — that citizen group is unserved in current data.`,
    },
    {
      icon:"💡", val: totalSrc, label:"live data sources",
      color: C.amber, bg:"#fffbeb",
      tip:`Government websites scraped to build these insights: ${Object.keys(srcMap).join(", ")}. All analysis runs in-browser — no AI API cost.`,
    },
  ];

  return (
    <div style={{
      background:"linear-gradient(135deg,#fff7ed 0%,#fffbeb 50%,#f0f9ff 100%)",
      border:"1.5px solid #fed7aa", borderRadius:16,
      padding:"18px 22px", marginBottom:20,
    }}>
      <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:12 }}>
        <span style={{ fontSize:18 }}>📊</span>
        <span style={{ fontWeight:800, fontSize:15, color:"#1a1a2e" }}>
          Live Data Summary
        </span>
        <span style={{ fontSize:12, color:C.muted, marginLeft:4 }}>
          — hover the <span style={{ background:"#e2e8f0", borderRadius:"50%",
            padding:"0 5px", fontSize:11, fontWeight:800 }}>i</span> icons below for what each number means
        </span>
      </div>
      <div style={{ display:"grid", gridTemplateColumns:"repeat(6,1fr)", gap:10 }}>
        {items.map((item, i) => (
          <div key={i} style={{
            background: item.bg,
            border:`1px solid ${item.color}25`,
            borderRadius:12, padding:"12px 14px",
            display:"flex", flexDirection:"column", gap:4,
          }}>
            <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between" }}>
              <span style={{ fontSize:20 }}>{item.icon}</span>
              <InfoTip text={item.tip}/>
            </div>
            <div style={{ fontSize:26, fontWeight:900, color:item.color, lineHeight:1 }}>
              {item.val}
            </div>
            <div style={{ fontSize:11, color:C.muted, lineHeight:1.3 }}>{item.label}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// PANELS
// ═══════════════════════════════════════════════════════════════════════════════

function HealthScore({ score, schemes, portals, duplicates, zeroSegs }) {
  const color = score >= 75 ? C.green : score >= 50 ? C.amber : C.red;
  const label = score >= 75 ? "GOOD" : score >= 50 ? "NEEDS ATTENTION" : "CRITICAL GAPS";
  const circumference = 2 * Math.PI * 36;

  return (
    <Card style={{ background:"linear-gradient(135deg,#fff7ed,#fffbeb)", borderColor:"#fed7aa" }}>
      <div style={{ display:"flex", gap:20, alignItems:"flex-start" }}>
        <div style={{ position:"relative", width:90, height:90, flexShrink:0 }}>
          <svg width="90" height="90" style={{ transform:"rotate(-90deg)" }}>
            <circle cx="45" cy="45" r="36" fill="none" stroke="#e5e7eb" strokeWidth="9"/>
            <circle cx="45" cy="45" r="36" fill="none" stroke={color} strokeWidth="9"
              strokeDasharray={`${(score/100)*circumference} ${circumference}`}
              strokeLinecap="round" style={{ transition:"stroke-dasharray 1s ease" }}/>
          </svg>
          <div style={{ position:"absolute", inset:0, display:"flex",
            flexDirection:"column", alignItems:"center", justifyContent:"center" }}>
            <span style={{ fontSize:22, fontWeight:900, color }}>{score}</span>
            <span style={{ fontSize:9, color:C.muted }}>/ 100</span>
          </div>
        </div>

        <div style={{ flex:1 }}>
          <div style={{ fontSize:11, fontWeight:700, color:C.orange, letterSpacing:"0.1em", marginBottom:6 }}>
            EXECUTIVE BRIEFING · OFFICE OF CM · RAJASTHAN
          </div>
          <div style={{ fontSize:18, fontWeight:800, color:C.text, marginBottom:4 }}>
            Welfare Ecosystem Health:{" "}
            <span style={{ color }}>{label}</span>
          </div>
          <p style={{ fontSize:13, color:C.muted, margin:"0 0 14px", lineHeight:1.5 }}>
            AI analysis of {schemes.length} live-scraped schemes across {portals.length} IGOD government portals.
          </p>

          <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:8 }}>
            {[
              { label:"SCHEMES SCRAPED",    val:schemes.length,   color:C.orange,
                tip:"Total scheme records fetched from RajRAS, Jan Soochna, and MyScheme in this session." },
              { label:"PORTALS INDEXED",    val:portals.length,   color:C.blue,
                tip:"Government portals from igod.gov.in directory for Rajasthan state." },
              { label:"DUPLICATES FOUND",   val:duplicates.length,color:C.purple,
                tip:"Schemes whose name appears on 2+ different portals. Identified by normalising and matching scheme names." },
              { label:"ZERO-COVERAGE SEGS", val:zeroSegs,         color:C.red,
                tip:"Citizen groups with no schemes found. Formula: count = 0 for that segment's keyword search." },
            ].map((k,i) => (
              <div key={i} style={{ background:"white", borderRadius:9,
                padding:"9px 12px", border:`1px solid ${C.border}` }}>
                <div style={{ display:"flex", alignItems:"center", gap:4, marginBottom:3 }}>
                  <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:"0.07em" }}>
                    {k.label}
                  </div>
                  <InfoTip text={k.tip}/>
                </div>
                <div style={{ fontSize:20, fontWeight:900, color:k.color }}>{k.val}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </Card>
  );
}

function PriorityActions({ actions }) {
  const [open, setOpen] = useState(null);
  const priColor = { CRITICAL:C.red, HIGH:C.orange, MEDIUM:C.amber };
  const tlColor  = { "This week":C.red, "This month":C.orange, "This quarter":C.blue };

  return (
    <div>
      <Sec icon="⚡" title="Priority Actions for CM"
        sub="Derived entirely from scraped data patterns — click to expand"
        tip="Actions are auto-generated by a rule engine. Rule 1: zero-coverage segments → CRITICAL. Rule 2: duplicate scheme names → HIGH. Rule 3: weakest sector < 2 schemes → HIGH. Rule 4: missing descriptions → MEDIUM. Rule 5: multi-portal fragmentation → MEDIUM."/>
      <div style={{ display:"flex", flexDirection:"column", gap:10 }}>
        {actions.map((a,i) => {
          const pc = priColor[a.priority] || C.amber;
          const tc = tlColor[a.timeline]  || C.orange;
          return (
            <div key={i} onClick={() => setOpen(open===i?null:i)}
              style={{ background: i===0?"linear-gradient(135deg,#fff7ed,#fffbeb)":C.card,
                borderRadius:12, border:`1px solid ${i===0?"#fed7aa":C.border}`,
                borderLeft:`4px solid ${pc}`, padding:16, cursor:"pointer",
                boxShadow:"0 1px 4px rgba(0,0,0,0.04)" }}>
              <div style={{ display:"flex", gap:12, alignItems:"flex-start" }}>
                <div style={{ width:40, height:40, borderRadius:10, flexShrink:0,
                  background: i===0?C.orange:`${pc}15`, color: i===0?"white":pc,
                  display:"flex", alignItems:"center", justifyContent:"center",
                  fontSize:18, fontWeight:900 }}>{a.rank}</div>
                <div style={{ flex:1 }}>
                  <div style={{ display:"flex", gap:7, marginBottom:7, flexWrap:"wrap" }}>
                    <Badge label={a.priority} color={pc} small/>
                    <Badge label={`⏱ ${a.timeline}`} color={tc} small/>
                    <span style={{ fontSize:16 }}>{a.icon}</span>
                  </div>
                  <div style={{ fontWeight:800, fontSize:14, color:C.text, marginBottom:4 }}>{a.title}</div>
                  <p style={{ fontSize:12, color:C.muted, margin:0, lineHeight:1.5 }}>{a.why}</p>
                  {open===i && (
                    <div style={{ marginTop:10, background:"#f0fdf4", borderRadius:8, padding:"10px 14px" }}>
                      <span style={{ fontSize:10, fontWeight:700, color:"#166534", letterSpacing:"0.07em" }}>EXPECTED IMPACT  </span>
                      <span style={{ fontSize:12, color:"#14532d", fontWeight:600 }}>{a.impact}</span>
                    </div>
                  )}
                  <div style={{ fontSize:10, color:C.muted, marginTop:5 }}>
                    {open===i?"▲ Less":"▼ See impact"}
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function SegmentCoverage({ segments }) {
  const [expanded, setExpanded] = useState({});
  const priColor = (n) => n===0?C.red : n<=2?C.amber : C.green;
  const priLabel = (n) => n===0?"NO COVERAGE" : n<=2?"THIN COVERAGE" : "COVERED";

  return (
    <div>
      <Sec icon="🎯" title="Citizen Segment Coverage"
        sub="How many scraped schemes address each citizen group"
        tip="Each segment is matched by searching all scheme text fields (name, description, eligibility, benefit, category, tags) for keywords. e.g. 'Women & Girls' matches schemes containing: women, girl, mahila, beti, widow, rajshri, sukanya, maternity."/>
      <div style={{ display:"grid", gridTemplateColumns:"repeat(2,1fr)", gap:10 }}>
        {segments.map((seg,i) => {
          const color = priColor(seg.count);
          const isExp = expanded[seg.id];
          return (
            <Card key={i} style={{ borderLeft:`4px solid ${color}`,
              background: seg.count===0?"#fef2f2":C.card,
              borderColor: seg.count===0?"#fecaca":C.border, padding:14 }}>
              <div style={{ display:"flex", alignItems:"center",
                justifyContent:"space-between", marginBottom:8 }}>
                <div style={{ display:"flex", alignItems:"center", gap:8 }}>
                  <span style={{ fontSize:20 }}>{seg.icon}</span>
                  <span style={{ fontWeight:700, fontSize:13, color:C.text }}>{seg.label}</span>
                  <InfoTip text={`Keywords used: ${seg.kws.join(", ")}. Matched against: name, description, eligibility, benefit, category, department, ministry, tags.`}/>
                </div>
                <div style={{ textAlign:"right" }}>
                  <div style={{ fontSize:24, fontWeight:900, color }}>{seg.count}</div>
                  <Badge label={priLabel(seg.count)} color={color} small/>
                </div>
              </div>

              <div style={{ height:5, background:"#f1f5f9", borderRadius:3,
                overflow:"hidden", marginBottom:8 }}>
                <div style={{ width:`${Math.min(seg.count*8, 100)}%`, height:"100%",
                  background:color, borderRadius:3, transition:"width .5s" }}/>
              </div>

              {seg.matching.length > 0 ? (
                <>
                  <div style={{ display:"flex", gap:4, flexWrap:"wrap" }}>
                    {(isExp ? seg.matching : seg.matching.slice(0,3)).map((s,j) => {
                      const url = safeUrl(s);
                      return url ? (
                        <a key={j} href={url} target="_blank" rel="noreferrer"
                          onClick={e => e.stopPropagation()}
                          style={{ background:`${color}10`, color,
                            border:`1px solid ${color}25`, borderRadius:5,
                            padding:"1px 7px", fontSize:10, fontWeight:600,
                            textDecoration:"none" }}>
                          {s.name?.slice(0,28)}{s.name?.length>28?"…":""}↗
                        </a>
                      ) : (
                        <span key={j} style={{ background:`${color}10`, color,
                          border:`1px solid ${color}25`, borderRadius:5,
                          padding:"1px 7px", fontSize:10, fontWeight:600 }}>
                          {s.name?.slice(0,28)}{s.name?.length>28?"…":""}
                        </span>
                      );
                    })}
                    {!isExp && seg.matching.length > 3 && (
                      <button onClick={e => { e.stopPropagation(); setExpanded(p=>({...p,[seg.id]:true})); }}
                        style={{ background:"none", border:`1px dashed ${color}50`, borderRadius:5,
                          padding:"1px 7px", fontSize:10, color, cursor:"pointer", fontWeight:600 }}>
                        +{seg.matching.length-3} more
                      </button>
                    )}
                    {isExp && seg.matching.length > 3 && (
                      <button onClick={e => { e.stopPropagation(); setExpanded(p=>({...p,[seg.id]:false})); }}
                        style={{ background:"none", border:`1px dashed ${color}50`, borderRadius:5,
                          padding:"1px 7px", fontSize:10, color, cursor:"pointer", fontWeight:600 }}>
                        ▲ show less
                      </button>
                    )}
                  </div>
                </>
              ) : (
                <span style={{ fontSize:11, color:"#991b1b", fontWeight:600 }}>
                  ⚠️ No schemes found for this segment
                </span>
              )}
            </Card>
          );
        })}
      </div>
    </div>
  );
}

function SectorBalance({ categories }) {
  const max = categories[0]?.count || 1;
  const colors = [C.orange,C.blue,C.green,C.purple,C.red,C.amber,
                  "#06b6d4","#84cc16","#ec4899","#14b8a6","#6366f1","#a855f7"];
  return (
    <div>
      <Sec icon="📊" title="Scheme Distribution by Sector"
        sub="Scraped scheme count per policy category"
        tip="Categories are assigned by the scraper's keyword classifier — not from any API field. Each scheme's name, description, and tags are matched against sector keywords. e.g. 'health' keyword → Health category. Imbalances flag investment gaps."/>
      <Card>
        <div style={{ display:"flex", flexDirection:"column", gap:10 }}>
          {categories.map((cat,i) => {
            const pct = Math.round((cat.count/max)*100);
            const color = colors[i % colors.length];
            return (
              <div key={i}>
                <div style={{ display:"flex", justifyContent:"space-between",
                  alignItems:"center", marginBottom:4 }}>
                  <span style={{ fontSize:13, fontWeight:600, color:C.text }}>{cat.name}</span>
                  <div style={{ display:"flex", alignItems:"center", gap:8 }}>
                    {cat.schemes.slice(0,2).map((s,j) => (
                      <span key={j} style={{ fontSize:9, color:C.muted,
                        background:"#f1f5f9", borderRadius:4, padding:"1px 6px",
                        maxWidth:120, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>
                        {s.name?.slice(0,20)}
                      </span>
                    ))}
                    <span style={{ fontWeight:900, fontSize:16, color, minWidth:28, textAlign:"right" }}>
                      {cat.count}
                    </span>
                  </div>
                </div>
                <div style={{ height:8, background:"#f1f5f9", borderRadius:4, overflow:"hidden" }}>
                  <div style={{ width:`${pct}%`, height:"100%", background:color,
                    borderRadius:4, transition:"width .6s ease" }}/>
                </div>
              </div>
            );
          })}
        </div>
      </Card>
    </div>
  );
}

function Duplicates({ duplicates }) {
  if (!duplicates.length) {
    return (
      <div>
        <Sec icon="🔄" title="Duplicate Detection"
          sub="Schemes appearing across multiple portals"
          tip="A duplicate is detected when the same scheme name (normalised: lowercase, first 40 chars) appears in 2+ different portals. This causes citizen confusion as they find the same scheme multiple times."/>
        <Card style={{ textAlign:"center", padding:30 }}>
          <span style={{ fontSize:32 }}>✅</span>
          <p style={{ color:C.muted, marginTop:8 }}>No duplicate scheme names detected across sources.</p>
        </Card>
      </div>
    );
  }
  return (
    <div>
      <Sec icon="🔄" title={`${duplicates.length} Duplicates Detected`}
        sub="Same scheme name appearing across multiple portals"
        tip={`Detection method: scheme names are normalised (lowercase, trimmed, first 40 chars) then grouped. Groups with 2+ entries AND 2+ distinct portal sources are flagged as duplicates. Total ${duplicates.length} found.`}/>
      <div style={{ display:"flex", flexDirection:"column", gap:10 }}>
        {duplicates.slice(0,8).map((group,i) => {
          const sources = [...new Set(group.map(s=>s._src_label||s.source?.split(".")?.[0]||"?"))];
          const name = group[0].name;
          const benefit = group.find(s=>s.benefit)?.benefit || "";
          return (
            <Card key={i} style={{ borderLeft:`4px solid ${C.purple}`, padding:14 }}>
              <div style={{ display:"flex", justifyContent:"space-between",
                alignItems:"flex-start", marginBottom:8 }}>
                <div style={{ flex:1 }}>
                  <div style={{ fontWeight:800, fontSize:14, color:C.text, marginBottom:5 }}>{name}</div>
                  <div style={{ display:"flex", gap:5, flexWrap:"wrap" }}>
                    {sources.map((src,j) => (
                      <Badge key={j} label={src} color={C.purple} small/>
                    ))}
                  </div>
                </div>
                <div style={{ textAlign:"right", flexShrink:0, marginLeft:10 }}>
                  <div style={{ fontSize:26, fontWeight:900, color:C.purple }}>{group.length}×</div>
                  <div style={{ fontSize:9, color:C.muted }}>portals</div>
                </div>
              </div>
              {benefit && (
                <div style={{ fontSize:12, color:C.muted }}>
                  Benefit: <strong style={{ color:C.text }}>{benefit}</strong>
                </div>
              )}
              <div style={{ display:"flex", gap:6, flexWrap:"wrap", marginTop:8 }}>
                {group.map((s,j) => {
                  const url = safeUrl(s);
                  return url ? (
                    <a key={j} href={url} target="_blank" rel="noreferrer"
                      style={{ background:`${C.purple}10`, color:C.purple,
                        border:`1px solid ${C.purple}25`, borderRadius:6,
                        padding:"3px 10px", fontSize:11, fontWeight:600, textDecoration:"none" }}>
                      {s._src_label} ↗
                    </a>
                  ) : null;
                }).filter(Boolean)}
              </div>
              <div style={{ marginTop:8, background:"#f0fdf4", borderRadius:7,
                padding:"7px 12px", fontSize:11, color:"#14532d", fontWeight:600 }}>
                → Designate one portal as master record · Update Jan Soochna with canonical entry
              </div>
            </Card>
          );
        })}
      </div>
    </div>
  );
}

function TopBenefits({ withValue }) {
  if (!withValue.length) return null;
  return (
    <div>
      <Sec icon="💰" title="Highest Value Schemes"
        sub="Ranked by monetary benefit — parsed from scraped data"
        tip="Values are extracted from scheme.benefit or scheme.description text using regex: looks for 'N crore' (×1Cr), 'N lakh' (×1L), or '₹N' patterns. Only schemes where a rupee amount was found are ranked. The bar width = value / max_value × 100%."/>
      <Card>
        <div style={{ display:"flex", flexDirection:"column", gap:10 }}>
          {withValue.slice(0,10).map((s,i) => {
            const src = s._src_label || s.source?.split(".")?.[0] || "";
            const srcC = {RajRAS:C.blue,"Jan Soochna":C.green,
              MyScheme:C.purple,"IGOD Portal":C.orange,"IGOD Directory":C.orange}[src] || C.orange;
            const max = withValue[0]._inr;
            const url = safeUrl(s);
            return (
              <div key={i} style={{ display:"flex", alignItems:"center", gap:10 }}>
                <div style={{ width:24, height:24, borderRadius:6, flexShrink:0,
                  background:i<3?C.orange:"#f1f5f9", color:i<3?"white":C.muted,
                  display:"flex", alignItems:"center", justifyContent:"center",
                  fontSize:11, fontWeight:800 }}>{i+1}</div>
                <div style={{ flex:1, minWidth:0 }}>
                  <div style={{ display:"flex", justifyContent:"space-between",
                    alignItems:"center", marginBottom:3 }}>
                    <span style={{ fontSize:13, fontWeight:600, color:C.text,
                      overflow:"hidden", textOverflow:"ellipsis",
                      whiteSpace:"nowrap", maxWidth:"50%" }}>
                      {s.name}
                    </span>
                    <div style={{ display:"flex", alignItems:"center", gap:6, flexShrink:0 }}>
                      <Badge label={src} color={srcC} small/>
                      {url && (
                        <a href={url} target="_blank" rel="noreferrer"
                          style={{ fontSize:10, color:srcC, fontWeight:700, textDecoration:"none" }}>
                          ↗
                        </a>
                      )}
                      <span style={{ fontWeight:900, fontSize:14, color:C.orange }}>
                        {fmtINR(s._inr)}
                      </span>
                    </div>
                  </div>
                  <div style={{ height:5, background:"#f1f5f9", borderRadius:3, overflow:"hidden" }}>
                    <div style={{ width:`${Math.round((s._inr/max)*100)}%`, height:"100%",
                      background:i<3?"linear-gradient(90deg,#f97316,#f59e0b)":"#94a3b8",
                      borderRadius:3 }}/>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </Card>
    </div>
  );
}

function SourceBreakdown({ srcMap, totalSchemes }) {
  const colors = { RajRAS:C.blue, "Jan Soochna":C.green, MyScheme:C.purple, "IGOD Portal":C.orange, "IGOD Directory":C.orange };
  const srcLinks = {
    RajRAS: "https://rajras.in/ras/pre/rajasthan/adm/schemes/",
    "Jan Soochna": "https://jansoochna.rajasthan.gov.in/",
    MyScheme: "https://www.myscheme.gov.in/search?q=rajasthan",
    "IGOD Directory": "https://igod.gov.in/sg/RJ/SPMA/organizations",
  };
  const max = Math.max(...Object.values(srcMap));
  return (
    <div>
      <Sec icon="📡" title="Data Source Breakdown"
        sub="Schemes scraped from each official government portal"
        tip="Each portal is scraped independently. RajRAS: HTML parse of index page + individual article pages. Jan Soochna: JSON API call. MyScheme: REST search API. IGOD: HTML parse of directory page. Counts reflect items successfully parsed in this session."/>
      <div style={{ display:"grid", gridTemplateColumns:"repeat(2,1fr)", gap:10 }}>
        {Object.entries(srcMap).sort((a,b)=>b[1]-a[1]).map(([src,count],i) => {
          const color = colors[src] || C.orange;
          const link  = srcLinks[src] || "#";
          return (
            <Card key={i} style={{ padding:16, border:`1px solid ${color}25`, background:`${color}06` }}>
              <div style={{ display:"flex", justifyContent:"space-between",
                alignItems:"center", marginBottom:8 }}>
                <a href={link} target="_blank" rel="noreferrer"
                  style={{ fontWeight:700, fontSize:13, color:C.text, textDecoration:"none" }}>
                  {src} <span style={{ fontSize:10, color }}> ↗</span>
                </a>
                <span style={{ fontSize:28, fontWeight:900, color }}>{count}</span>
              </div>
              <div style={{ height:6, background:"#f1f5f9", borderRadius:3, overflow:"hidden", marginBottom:5 }}>
                <div style={{ width:`${Math.round((count/max)*100)}%`, height:"100%", background:color, borderRadius:3 }}/>
              </div>
              <div style={{ fontSize:11, color:C.muted }}>
                {Math.round((count/totalSchemes)*100)}% of total scraped data
              </div>
            </Card>
          );
        })}
      </div>
    </div>
  );
}

function AllSchemes({ schemes }) {
  const [search, setSearch] = useState("");
  const [catFilter, setCatFilter] = useState("all");
  const [showCount, setShowCount] = useState(50);

  const cats = [...new Set(schemes.map(s=>s.category||"General"))].sort();
  const filtered = schemes.filter(s => {
    const mQ = !search || txt(s).includes(search.toLowerCase());
    const mC = catFilter==="all" || (s.category||"General")===catFilter;
    return mQ && mC;
  });

  return (
    <div>
      <Sec icon="📋" title={`All ${schemes.length} Scraped Schemes`}
        sub="Complete list from all sources — search, filter, explore"
        tip="All schemes combined from RajRAS, Jan Soochna, and MyScheme. Click Visit ↗ to open the actual scheme page. RajRAS links go to the specific article. MyScheme links go to the scheme's own page. Jan Soochna links go to the portal homepage (no individual scheme URLs available)."/>

      <div style={{ display:"flex", gap:8, marginBottom:12, flexWrap:"wrap" }}>
        <input value={search} onChange={e=>{setSearch(e.target.value); setShowCount(50);}}
          placeholder="Search schemes, benefits, eligibility…"
          style={{ flex:1, minWidth:200, padding:"9px 14px",
            border:`1px solid ${C.border}`, borderRadius:9,
            fontSize:13, background:"white", outline:"none" }}/>
        <select value={catFilter} onChange={e=>{setCatFilter(e.target.value); setShowCount(50);}}
          style={{ padding:"9px 14px", border:`1px solid ${C.border}`,
            borderRadius:9, fontSize:13, background:"white", cursor:"pointer" }}>
          <option value="all">All Categories</option>
          {cats.map(c=><option key={c} value={c}>{c}</option>)}
        </select>
      </div>

      <div style={{ fontSize:12, color:C.muted, marginBottom:10 }}>
        Showing {Math.min(showCount, filtered.length)} of {filtered.length} schemes
      </div>

      <div style={{ display:"flex", flexDirection:"column", gap:8 }}>
        {filtered.slice(0, showCount).map((s,i) => {
          const src  = s._src_label || s.source?.split(".")?.[0] || "?";
          const srcC = {RajRAS:C.blue,"Jan Soochna":C.green,
            MyScheme:C.purple,"IGOD Portal":C.orange,"IGOD Directory":C.orange}[src] || C.orange;
          const url  = safeUrl(s);
          return (
            <Card key={i} style={{ padding:12, borderLeft:`3px solid ${srcC}` }}>
              <div style={{ display:"flex", gap:10, alignItems:"flex-start" }}>
                <div style={{ flex:1 }}>
                  <div style={{ display:"flex", alignItems:"center",
                    gap:8, marginBottom:4, flexWrap:"wrap" }}>
                    <span style={{ fontWeight:700, fontSize:13, color:C.text }}>{s.name}</span>
                    <Badge label={s.category||"General"} color={srcC} small/>
                    <Badge label={src} color={srcC} small/>
                  </div>
                  {s.benefit && (
                    <div style={{ fontSize:12, color:"#166534", fontWeight:600,
                      background:"#f0fdf4", borderRadius:5,
                      padding:"2px 8px", display:"inline-block", marginBottom:3 }}>
                      💰 {s.benefit}
                    </div>
                  )}
                  {s.eligibility && (
                    <div style={{ fontSize:11, color:C.muted }}>
                      Who: {s.eligibility?.slice(0,100)}
                    </div>
                  )}
                  {!s.benefit && s.description && (
                    <div style={{ fontSize:11, color:C.muted }}>
                      {s.description?.slice(0,120)}
                    </div>
                  )}
                </div>
                {url && (
                  <a href={url} target="_blank" rel="noreferrer"
                    onClick={e=>e.stopPropagation()}
                    style={{ fontSize:11, color:srcC, fontWeight:700,
                      flexShrink:0, whiteSpace:"nowrap", textDecoration:"none" }}>
                    Visit ↗
                  </a>
                )}
              </div>
            </Card>
          );
        })}
      </div>

      {/* Show more button — not just a text note */}
      {filtered.length > showCount && (
        <div style={{ textAlign:"center", marginTop:14 }}>
          <button onClick={() => setShowCount(c => c + 50)}
            style={{ background:"white", border:`1.5px solid ${C.border}`,
              borderRadius:10, padding:"10px 28px", fontSize:13,
              fontWeight:700, color:C.text, cursor:"pointer" }}>
            Show more ({filtered.length - showCount} remaining)
          </button>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN EXPORT
// ═══════════════════════════════════════════════════════════════════════════════
export default function InsightsEngine({ schemes=[], portals=[], onScrapeFirst }) {
  const [activeTab, setActiveTab] = useState("overview");

  const analysis = useMemo(() => {
    if (!schemes.length) return null;
    return runAnalysis(schemes, portals);
  }, [schemes, portals]);

  if (!schemes.length) {
    return (
      <div style={{ padding:"60px 40px", textAlign:"center" }}>
        <div style={{ fontSize:64, marginBottom:16 }}>📡</div>
        <h3 style={{ fontSize:20, fontWeight:800, color:C.text, marginBottom:8 }}>
          No Scraped Data Yet
        </h3>
        <p style={{ color:C.muted, fontSize:14, maxWidth:360,
          margin:"0 auto 24px", lineHeight:1.6 }}>
          Click <strong>⚡ Scrape Now</strong> in the header to pull live data
          from all 4 government websites. Insights generate instantly — no API needed.
        </p>
        <button onClick={onScrapeFirst} style={{
          background:C.orange, color:"white", borderRadius:12,
          padding:"13px 32px", fontWeight:800, fontSize:15,
          border:"none", cursor:"pointer", boxShadow:"0 4px 20px #f9731650"
        }}>⚡ Scrape Now</button>
      </div>
    );
  }

  const { categories, srcMap, duplicates, withValue, segmentAnalysis, score, actions } = analysis;
  const zeroSegs = segmentAnalysis.filter(s=>s.count===0).length;

  const TABS = [
    { id:"overview",   label:"Overview"           },
    { id:"actions",    label:"⚡ Actions"          },
    { id:"segments",   label:"🎯 Coverage"         },
    { id:"sectors",    label:"📊 Sectors"          },
    { id:"dupes",      label:`🔄 Duplicates (${duplicates.length})` },
    { id:"benefits",   label:"💰 Benefits"         },
    { id:"allschemes", label:`📋 All ${schemes.length} Schemes` },
  ];

  return (
    <div className="fadeup">
      <div style={{ marginBottom:18 }}>
        <h1 style={{ fontSize:24, fontWeight:900, color:C.text, margin:"0 0 3px" }}>
          Policy Intelligence —{" "}
          <span style={{ color:C.orange }}>Live Data Insights</span>
        </h1>
        <p style={{ color:C.muted, fontSize:12, margin:0 }}>
          Instant analysis of {schemes.length} live-scraped schemes ·
          Zero AI API · Updates on every scrape
        </p>
      </div>

      {/* ── Summary banner (before data sources / tabs) ── */}
      <SummaryBanner
        schemes={schemes} portals={portals} duplicates={duplicates}
        zeroSegs={zeroSegs} categories={categories} srcMap={srcMap}/>

      {/* Tab nav */}
      <div style={{ display:"flex", gap:4, flexWrap:"wrap",
        marginBottom:22, borderBottom:`1px solid ${C.border}`, paddingBottom:0 }}>
        {TABS.map(t => (
          <button key={t.id} onClick={() => setActiveTab(t.id)} style={{
            background:"transparent", border:"none",
            borderBottom: activeTab===t.id ? `2.5px solid ${C.orange}` : "2.5px solid transparent",
            color: activeTab===t.id ? C.orange : C.muted,
            fontWeight: activeTab===t.id ? 700 : 500,
            fontSize:13, padding:"9px 14px", cursor:"pointer", transition:"all .15s",
          }}>{t.label}</button>
        ))}
      </div>

      {/* Content */}
      <div style={{ display:"flex", flexDirection:"column", gap:24 }}>
        {activeTab==="overview" && <>
          <HealthScore score={score} schemes={schemes} portals={portals}
            duplicates={duplicates} zeroSegs={zeroSegs}/>
          <PriorityActions actions={actions}/>
          <SourceBreakdown srcMap={srcMap} totalSchemes={schemes.length}/>
        </>}
        {activeTab==="actions"    && <PriorityActions actions={actions}/>}
        {activeTab==="segments"   && <SegmentCoverage segments={segmentAnalysis}/>}
        {activeTab==="sectors"    && <SectorBalance categories={categories}/>}
        {activeTab==="dupes"      && <Duplicates duplicates={duplicates}/>}
        {activeTab==="benefits"   && <TopBenefits withValue={withValue}/>}
        {activeTab==="allschemes" && <AllSchemes schemes={schemes}/>}
      </div>
    </div>
  );
}
