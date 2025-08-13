// scrape.mjs
// ===============================================================
// Scraper Yallo (Mobile) – onglet "Tout"
// Sortie: data/latest.csv (+ JSON debug)
// Node 20+, Playwright Chromium
// ===============================================================

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { chromium, devices } from "playwright";

// -------------------- Config via ENV (sécurisée & bornée) --------------------
const ENV = (k, d) => (process.env[k] ?? d);
const OP               = (ENV("OP", "yallo") + "").toLowerCase();
const HEADLESS         = ENV("HEADLESS", "1") === "1";
const BLOCK_RESOURCES  = ENV("BLOCK_RESOURCES", "1") === "1";
const MAX_PAGES        = clampInt(ENV("MAX_PAGES", "60"), 1, 300);
const NAV_TIMEOUT_MS   = clampInt(ENV("NAV_TIMEOUT_MS", "25000"), 8000, 60000);
const MAX_RUNTIME_MIN  = clampInt(ENV("MAX_RUNTIME_MIN", "10"), 3, 30);

const AIRTABLE_TOKEN   = ENV("AIRTABLE_TOKEN", "");
const AIRTABLE_BASE    = ENV("AIRTABLE_BASE", "");
const AIRTABLE_TABLE   = ENV("AIRTABLE_TABLE", "");

const OUT_DIR  = "data";
const OUT_CSV  = path.join(OUT_DIR, "latest.csv");
const OUT_JSON = path.join(OUT_DIR, "latest.json");

// -------------------- Helpers génériques --------------------
function clampInt(v, min, max){
  let n = Number.parseInt(v, 10); if (Number.isNaN(n)) n = min;
  return Math.max(min, Math.min(max, n));
}
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
function ensureDir(p){ if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive:true }); }
function textClean(s){ return String(s ?? "").replace(/\s+/g, " ").trim(); }
function slugify(str){
  return String(str ?? "")
    .normalize("NFKD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9]+/g, "-").replace(/^-+|-+$/g, "")
    .toLowerCase();
}
function shortHash(s, len=6){
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, len);
}
function nowIso(){ return new Date().toISOString(); }

// --------- parsing num/price/units robustes (CHF, Go, Gbit/s → Mbps) ---------
function parsePriceCHF(s){
  if (!s) return null;
  const t = s.replace(",", ".").replace(/\s/g, "");
  const m = t.match(/(?:CHF)?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:CHF)?/i);
  return m ? Number.parseFloat(m[1]) : null;
}
function parsePercent(s){
  if (!s) return null;
  const m = s.replace(",", ".").match(/([0-9]+(?:\.[0-9]+)?)\s*%/);
  return m ? Number.parseFloat(m[1]) : null;
}
function parseNumber(s){
  if (!s) return null;
  const v = s.replace(",", ".").match(/-?\d+(?:\.\d+)?/);
  return v ? Number.parseFloat(v[0]) : null;
}
function parseMbpsFromSpeedText(s){
  const t = (s || "").toLowerCase().replace(",", ".").replace(/\s+/g, " ");
  const g = t.match(/([0-9]+(?:\.[0-9]+)?)\s*gbit\/s/);
  if (g) return Math.round(Number.parseFloat(g[1]) * 1000);
  const m = t.match(/([0-9]+(?:\.[0-9]+)?)\s*mbit\/s/);
  if (m) return Math.round(Number.parseFloat(m[1]));
  return null;
}
function parseDataGo(s){
  if (!s) return 0;
  const t = s.toLowerCase();
  if (t.includes("illimit")) return Infinity;
  const m = t.replace(",", ".").match(/([0-9]+(?:\.[0-9]+)?)\s*go/);
  return m ? Number.parseFloat(m[1]) : (t.includes("—") ? 0 : 0);
}
function parseMinutes(s){
  if (!s) return 0;
  const t = s.toLowerCase();
  if (t.includes("illimit")) return Infinity;
  const m = t.match(/([0-9]+)\s*min/);
  return m ? Number.parseInt(m[1], 10) : (t.includes("—") ? 0 : 0);
}

// -------------------- CO₂ (ta méthode, portée en JS) --------------------
function numForCO2(v){
  if (!v) return 0;
  const t = String(v).toLowerCase();
  if (t.includes("illimit")) return Infinity;
  const m = String(v).replace(",", ".").match(/\d+\.?\d*/);
  return m ? Number.parseFloat(m[0]) : 0;
}
function calculateCO2(row){
  const type_offer = row["Type offre"] || "";
  const speed = row["Rapidité réseau Mbps"];
  const data_itin = row["Données en itinérance (Go)"];
  const min_roam = row["Minutes roaming ( Heure )"];
  const tv = row["TV"] || "Non";
  const name = (row["Nom de l'offre"] || "").toLowerCase();

  if (type_offer.includes("1 ")) {
    let base = (speed && speed <= 300) ? 12.5 : 14.5;
    if ((row["Appels en Suisse ( Heure )"] || "").toLowerCase().includes("illimit")) base += 0.5;
    if ((row["SMS & MMS (Suisse)"] || "").toLowerCase().includes("illimit")) base += 0.5;

    const roaming_go = numForCO2(data_itin);
    const is_unlimited_roam_go = roaming_go === Infinity;
    const is_unlimited_roam_min = numForCO2(min_roam) === Infinity;

    const roaming_cont = is_unlimited_roam_go ? 18.3 : roaming_go * 0.35;
    base += roaming_cont;
    if (is_unlimited_roam_min) base -= 12.2;
    if (name.includes("noir") || name.includes("black")) base -= 0.5;
    return round1(base);
  }

  if (type_offer.includes("2 ") || type_offer.includes("3 ")) {
    let base = 35.0;
    if (name.includes("fiber")) base = 30.0;
    else if (name.includes("5g")) base = 45.0;
    if (tv !== "Non" && tv.toLowerCase().includes("chaine")) base += 50.0;
    return round1(base);
  }
  return null;
}
function round1(n){ return Math.round(Number(n) * 10) / 10; }

// -------------------- CSV utils --------------------
const CSV_HEADERS = [
  "Référence de l'offre",
  "Opérateur",
  "Nom de l'offre",
  "Prix CHF/mois",
  "Prix initial CHF",
  "Rabais (%)",
  "TV",
  "Rapidité réseau Mbps",
  "SMS & MMS (Suisse)",
  "Appels en Suisse ( Heure )",
  "Données en itinérance (Go)",
  "Minutes roaming ( Heure )",
  "Pays voisins inclus",
  "Type offre",
  "Expiration",
  "~Émission de CO2 (kg/an)",
  "Durée d'engagement"
];
function csvEscape(v){
  const s = String(v ?? "");
  if (s.includes(",") || s.includes('"') || s.includes("\n")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}
function toCSV(rows){
  const lines = [];
  lines.push(CSV_HEADERS.join(","));
  for (const r of rows) {
    const line = CSV_HEADERS.map(h => csvEscape(r[h])).join(",");
    lines.push(line);
  }
  return lines.join("\n");
}

// -------------------- Génération de référence stable --------------------
function generateReference({ operator, name, price, ts }){
  // YALLO-<slugName>-<prix_sans_point>-<hash6>
  const slug = slugify(name).slice(0, 40) || "offre";
  const p = String(Math.round(Number(price || 0) * 100)); // 12.90 -> 1290
  const h = shortHash(`${operator}|${name}|${price}|${String(ts).slice(0,10)}`, 6);
  return `YALLO-${slug}-${p}-${h}`.toUpperCase();
}

// -------------------- Scrape Yallo Mobile (onglet "Tout") --------------------
async function scrapeYalloMobile(context){
  const page = await context.newPage();
  page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
  page.setDefaultTimeout(NAV_TIMEOUT_MS);

  const url = "https://www.yallo.ch/fr/mobile-products";
  console.log(`[${nowIso()}] Yallo → ${url}`);
  await safeGoto(page, url);

  await acceptCookiesIfAny(page);
  await selectTabTout(page);
  await waitForCards(page);

  const cards = await page.evaluate(() => {
    const candidates = Array.from(document.querySelectorAll(`
      [class*="card"], [class*="Card"], [class*="product"], [data-testid*="card"], article, section
    `)).filter(el => {
      const txt = (el.innerText || "").toLowerCase();
      return txt.includes("données") && txt.includes("appels") && /chf/i.test(txt);
    });

    const byPrice = Array.from(document.querySelectorAll("div")).filter(el => {
      const t = (el.innerText || "").toLowerCase();
      return /chf/.test(t) && t.includes("appels");
    });

    const uniq = new Set();
    [...candidates, ...byPrice].forEach(el => uniq.add(el));

    return Array.from(uniq).map(el => ({
      text: (el.innerText || "").replace(/\s+/g, " ").trim()
    }));
  });

  console.log(`[${nowIso()}] Yallo → cartes candidates: ${cards.length}`);

  const rows = [];
  for (let idx = 0; idx < Math.min(cards.length, MAX_PAGES); idx++) {
    hardStop();

    const c = cards[idx];
    const parsed = parseYalloCard(c.text);
    if (!parsed || !parsed.name || !parsed.price) continue;

    const row = {
      "Référence de l'offre": generateReference({
        operator: "Yallo",
        name: parsed.name,
        price: parsed.price,
        ts: Date.now()
      }),
      "Opérateur": "Yallo",
      "Nom de l'offre": parsed.name,
      "Prix CHF/mois": parsed.price ?? "",
      "Prix initial CHF": parsed.initialPrice ?? "",
      "Rabais (%)": parsed.rabaisPct ?? "",
      "TV": "Non",
      "Rapidité réseau Mbps": parsed.speedMbps ?? "",
      "SMS & MMS (Suisse)": parsed.smsMms ?? "",
      "Appels en Suisse ( Heure )": parsed.appels ?? "",
      "Données en itinérance (Go)": parsed.roamDataGo ?? "",
      "Minutes roaming ( Heure )": parsed.roamMinutes ?? "",
      "Pays voisins inclus": parsed.roamCountries ?? "",
      "Type offre": "1 SIM ( Mobile )",
      "Expiration": parsed.isPermaDiscount ? "Remise permanente" : "",
      "~Émission de CO2 (kg/an)": "",
      "Durée d'engagement": "Sans engagement"
    };
    row["~Émission de CO2 (kg/an)"] = calculateCO2({
      ...row,
      "Rapidité réseau Mbps": row["Rapidité réseau Mbps"] || 0
    });

    rows.push(row);
  }

  await page.close();
  return rows;
}

// -------------------- Parsers spécifiques Yallo --------------------
function parseYalloCard(text){
  const t = textClean(text);

  // Nom (ligne clé avant le prix)
  let name = null;
  {
    const parts = t.split(/chf/i)[0].split(/\n| {2,}/).map(x => x.trim()).filter(Boolean);
    name = parts.reverse().find(x => !/\d+%/.test(x) && !/(suisse|itinérance|données|appels)/i.test(x)) || parts[0];
  }

  // Prix + ancien prix + % rabais
  const price = parsePriceCHF(t);
  let initialPrice = null;
  const old1 = t.match(/au lieu de\s*([0-9]+(?:[\.,][0-9]+)?)\s*[-.–]?/i);
  if (old1) initialPrice = parseFloat(old1[1].replace(",", "."));
  if (!initialPrice){
    const old2 = t.match(/au lieu de\s*CHF?\s*([0-9]+(?:[\.,][0-9]+)?)/i);
    if (old2) initialPrice = parseFloat(old2[1].replace(",", "."));
  }
  let rabaisPct = parsePercent(t);
  if (!rabaisPct && initialPrice && price) rabaisPct = Math.round( (1 - price / initialPrice) * 100 );

  // Suisse (données + vitesse)
  let speedMbps = null;
  let swissDataGo = null;
  {
    const m = t.match(/Suisse(.+?)(Itinérance|CHF|Appels|Rabais|À saisir|En savoir plus|$)/i);
    const bloc = m ? m[1] : t;
    swissDataGo = parseDataGo(bloc);
    speedMbps = parseMbpsFromSpeedText(bloc);
    if (!Number.isFinite(swissDataGo)) swissDataGo = bloc.toLowerCase().includes("illimit") ? Infinity : swissDataGo;
  }

  // Appels (Suisse)
  let appels = "";
  {
    const m = t.match(/Appels(.+?)(Itinérance|CHF|Rabais|À saisir|En savoir plus|$)/i);
    const bloc = m ? m[1] : "";
    if (/illimit/i.test(bloc)) appels = "Illimité";
    else {
      const mins = parseMinutes(bloc);
      appels = mins === Infinity ? "Illimité" : (mins > 0 ? `${mins} min.` : "");
    }
  }

  // SMS & MMS (heuristique)
  let smsMms = /start|début/i.test(name || "") ? "0,15CHF/SMS & 0,50CHF/MMS" : "Illimité";

  // Itinérance
  let roamDataGo = 0, roamMinutes = 0, roamCountries = "";
  {
    const m = t.match(/Itinérance(.+?)(CHF|À saisir|En savoir plus|$)/i);
    const bloc = m ? m[1] : "";
    roamDataGo = parseDataGo(bloc);
    roamMinutes = parseMinutes(bloc);
    const countriesMatch = bloc.match(/(pays voisins|europe.*?tur|eu élargie.*?pays|8 pays les plus populaires|fr, de, it, at, li)/i);
    roamCountries = countriesMatch ? titleCase(countriesMatch[1]) : ( /voisin/i.test(bloc) ? "Pays voisins" : "" );
  }

  const isPermaDiscount = /rabais pour toujours|rabais permanent/i.test(t);

  return {
    name, price, initialPrice, rabaisPct,
    swissDataGo, speedMbps, appels, smsMms,
    roamDataGo, roamMinutes, roamCountries,
    isPermaDiscount
  };
}
function titleCase(s){ return String(s||"").toLowerCase().replace(/\b\w/g, m => m.toUpperCase()); }

// -------------------- Navigation Helpers --------------------
async function safeGoto(page, url){
  await page.goto(url, { waitUntil: "domcontentloaded" });
  await Promise.race([ page.waitForLoadState("networkidle"), sleep(1500) ]);
}
async function acceptCookiesIfAny(page){
  const selectors = [
    'button:has-text("Accepter")',
    'button:has-text("Tout accepter")',
    'button:has-text("OK")',
    'button[aria-label*="accepter" i]',
    '[data-testid*="accept" i]'
  ];
  for (const sel of selectors){
    const btn = page.locator(sel);
    if (await btn.first().isVisible().catch(()=>false)) {
      try { await btn.first().click({ timeout: 1500 }); break; } catch {}
    }
  }
}
async function selectTabTout(page){
  const tab = page.getByRole("tab", { name: /tout/i });
  if (await tab.first().isVisible().catch(()=>false)) {
    try { await tab.first().click(); await sleep(500); return; } catch {}
  }
  const txtSel = ['button:has-text("Tout")','a:has-text("Tout")','[role="tab"]:has-text("Tout")'];
  for (const s of txtSel){
    const el = page.locator(s);
    if (await el.first().isVisible().catch(()=>false)) {
      try { await el.first().click({ timeout: 1500 }); await sleep(500); return; } catch {}
    }
  }
}
async function waitForCards(page){
  await Promise.race([
    page.waitForSelector('text=/Appels/i', { timeout: 8000 }),
    sleep(2000)
  ]);
}

// -------------------- Playwright launch --------------------
function makeLaunchOptions(){
  const iphone = devices["iPhone 13"];
  return {
    headless: HEADLESS,
    args: ["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage"],
    viewport: { width: 1280, height: 1800 },
    userAgent: iphone.userAgent.replace(/iPhone.*?Safari\/[0-9.]+/i, "Chrome/123 Safari/537.36"),
    locale: "fr-FR"
  };
}
function shouldBlock(resourceType, url){
  if (!BLOCK_RESOURCES) return false;
  const t = resourceType;
  if (["image","media","font"].includes(t)) return true;
  const u = String(url);
  if (/\.(png|jpe?g|webp|gif|svg|mp4|webm)(\?|$)/i.test(u)) return true;
  if (/googletagmanager|google-analytics|doubleclick|facebook|hotjar|segment|adservice/i.test(u)) return true;
  return false;
}

// -------------------- Airtable (optionnel) --------------------
async function upsertAirtable(rows){
  if (!AIRTABLE_TOKEN || !AIRTABLE_BASE || !AIRTABLE_TABLE) return;
  const api = `https://api.airtable.com/v0/${AIRTABLE_BASE}/${encodeURIComponent(AIRTABLE_TABLE)}`;
  const headers = { "Authorization": `Bearer ${AIRTABLE_TOKEN}`, "Content-Type": "application/json" };
  const BATCH = 10;
  for (let i = 0; i < rows.length; i += BATCH){
    const slice = rows.slice(i, i + BATCH).map(r => ({ fields: r }));
    try{
      const res = await fetch(api, { method: "POST", headers, body: JSON.stringify({ records: slice }) });
      if (!res.ok) console.error(`[Airtable] POST ${res.status}: ${await res.text()}`);
    }catch(e){ console.error("[Airtable] Exception:", e.message); }
  }
}

// -------------------- Hard stop global --------------------
const STARTED_AT = Date.now();
function hardStop(){
  if ((Date.now() - STARTED_AT) > MAX_RUNTIME_MIN * 60_000) {
    throw new Error("MAX_RUNTIME_REACHED");
  }
}

// -------------------- MAIN --------------------
(async () => {
  ensureDir(OUT_DIR);

  if (OP !== "yallo"){
    console.log(`[INFO] OP=${OP} non supporté dans ce run. On ne scrape que Yallo (mobile).`);
  }

  const launchOptions = makeLaunchOptions();
  const browser = await chromium.launch(launchOptions);
  const context = await browser.newContext({
    locale: "fr-FR",
    geolocation: { latitude: 46.2044, longitude: 6.1432 },
    permissions: ["geolocation"]
  });

  if (BLOCK_RESOURCES){
    await context.route("**/*", route => {
      const req = route.request();
      if (shouldBlock(req.resourceType(), req.url())) route.abort();
      else route.continue();
    });
  }

  const rows = [];
  try{
    hardStop();
    if (OP === "yallo"){
      const y = await scrapeYalloMobile(context);
      rows.push(...y);
    }
  } catch(e){
    if (String(e.message).includes("MAX_RUNTIME_REACHED")) {
      console.error("[HARD-STOP] Temps max atteint -> sortie.");
    } else {
      console.error("[ERROR]", e);
    }
  } finally {
    await context.close().catch(()=>{});
    await browser.close().catch(()=>{});
  }

  // Toujours écrire un CSV valide (même vide) pour l’artefact
  const csv = toCSV(rows);
  fs.writeFileSync(OUT_CSV, csv, "utf8");
  fs.writeFileSync(OUT_JSON, JSON.stringify({ at: nowIso(), rows }, null, 2), "utf8");
  console.log(`[OK] Écrit: ${OUT_CSV} (${rows.length} lignes)`);

  if (rows.length && AIRTABLE_TOKEN) {
    await upsertAirtable(rows);
  }
})().catch(err => {
  console.error("[FATAL]", err);
  process.exit(1);
});
