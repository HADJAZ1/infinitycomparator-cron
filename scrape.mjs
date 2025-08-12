// scrape.mjs — HelixCompare daily scraper (Node 20 + Playwright)
// ---------------------------------------------------------------
// - Crawl courtois des sitemaps opérateurs (FR) via Playwright
// - Extraction -> normalisation -> CSV
// - Estimation CO2 intégrée (~Émission de CO2 (kg/an))
// - Push optionnel vers Airtable (secrets requises)
//
// Secrets (GitHub Actions Settings → Secrets and variables → Actions):
//   AIRTABLE_TOKEN
//   AIRTABLE_BASE
//   AIRTABLE_TABLE
//
// Sortie locale : data/latest.csv

import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";
import { gunzipSync } from "node:zlib";

// -------------------- Opérateurs & filtres --------------------
const OPERATORS = [
  {
    name: "Yallo",
    sitemap: "https://www.yallo.ch/sitemap.xml",
    urlFilter: (u) =>
      /\/fr\//.test(u) &&
      ( /\/mobile/.test(u) || /home-?5g/.test(u) || /home-?cable/.test(u) || /home/.test(u) ),
    typeFromUrl: (u, hasTV) =>
      /home|internet/.test(u) ? (hasTV ? "3 Home Cable Box + TV" : "2 Home Cable Box") : "1 SIM ( Mobile )",
  },
  {
    name: "Sunrise",
    sitemap: "https://www.sunrise.ch/sitemap.xml",
    urlFilter: (u) =>
      /\/fr\//.test(u) && ( /\/mobile/.test(u) || /\/internet/.test(u) || /\/home/.test(u) ),
    typeFromUrl: (u, hasTV) =>
      /home|internet/.test(u) ? (hasTV ? "3 Home Cable Box + TV" : "2 Home Cable Box") : "1 SIM ( Mobile )",
  },
  {
    name: "Salt",
    sitemap: "https://www.salt.ch/sitemap.xml",
    urlFilter: (u) =>
      /\/fr\//.test(u) && ( /\/mobile/.test(u) || /\/internet/.test(u) || /fiber/.test(u) ),
    typeFromUrl: (u, hasTV) =>
      /internet|fiber/.test(u) ? (hasTV ? "3 Home Cable Box + TV" : "2 Home Cable Box") : "1 SIM ( Mobile )",
  },
  {
    name: "Swisscom",
    sitemap: "https://www.swisscom.ch/sitemap.xml",
    urlFilter: (u) =>
      /\/fr\//.test(u) && ( /\/mobile/.test(u) || /\/internet/.test(u) || /\/tv/.test(u) ),
    typeFromUrl: (u, hasTV) =>
      /internet|tv/.test(u) ? (hasTV ? "3 Home Cable Box + TV" : "2 Home Cable Box") : "1 SIM ( Mobile )",
  },
];

// -------------------- CSV --------------------
const CSV_HEADERS = [
  "Référence de l'offre","Opérateur","Nom de l'offre","Prix CHF/mois","Prix initial CHF",
  "Rabais (%)","TV","Rapidité réseau Mbps","SMS & MMS (Suisse)","Appels en Suisse ( Heure )",
  "Données en itinérance (Go)","Minutes roaming ( Heure )","Pays voisins inclus",
  "Type offre","Expiration","~Émission de CO2 (kg/an)","Durée d'engagement"
];

const csvEsc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;

async function writeCSV(rows, outPath) {
  await fs.mkdir(path.dirname(outPath), { recursive: true });
  const lines = [CSV_HEADERS.map(csvEsc).join(",")];
  for (const r of rows) lines.push(CSV_HEADERS.map(h => csvEsc(r[h])).join(","));
  await fs.writeFile(outPath, lines.join("\n"), "utf8");
}

// -------------------- Utils --------------------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function toMbps(numStr, unitGuess) {
  if (!numStr) return "";
  const n = parseFloat(String(numStr).replace(",", "."));
  if (!Number.isFinite(n)) return "";
  return /g/i.test(unitGuess) ? String(Math.round(n * 1000)) : String(Math.round(n));
}

function slugFromUrl(u) {
  try {
    const last = new URL(u).pathname.split("/").filter(Boolean).pop() || "";
    return last.toUpperCase().replace(/[^A-Z0-9]/g, "_").slice(0, 40);
  } catch {
    return u.replace(/[^A-Za-z0-9]/g, "_").slice(0, 40);
  }
}

// -------------------- Estimation CO2 (intégrée) --------------------
const CO2 = {
  MOBILE_LE_300: 12.5,
  MOBILE_GT_300: 14.5,
  ROAM_KG_PER_GB: 0.35,
  ROAM_UNLIMITED_BONUS_GB: 18.3,
  ROAM_UNLIMITED_MIN_OFFSET: -12.2,
  PREMIUM_OFFSET: -0.5,
  HOME_FIBER: 30.0,
  HOME_CABLE: 35.0,
  HOME_5G: 45.0,
  TV_EXTRA: 50.0,
};

const clean = (s) => String(s ?? "").replace(/\u00A0/g, " ").trim();
const hasInf = (s) => clean(s).toLowerCase().includes("illimité");
const toNum = (s) => {
  s = clean(s);
  if (!s || s === "-" || s.toLowerCase() === "null") return 0;
  if (hasInf(s)) return Number.POSITIVE_INFINITY;
  const m = s.match(/-?\d+(?:[.,]\d+)?/);
  return m ? parseFloat(m[0].replace(",", ".")) : 0;
};
const isTvOn = (tv) => {
  const v = clean(tv).toLowerCase();
  if (!v || v === "non") return false;
  return v.includes("chaine") || v.includes("chaîne") || v.includes("tv");
};

function estimateCO2(row) {
  const typeOffer = clean(row["Type offre"]);
  const speed = toNum(row["Rapidité réseau Mbps"]);
  const dataItin = clean(row["Données en itinérance (Go)"]);
  const minRoam = clean(row["Minutes roaming ( Heure )"]);
  const tv = row["TV"];
  const name = clean(row["Nom de l'offre"]).toLowerCase();

  // Mobile (1)
  if (typeOffer.startsWith("1 ")) {
    let base = speed <= 300 ? CO2.MOBILE_LE_300 : CO2.MOBILE_GT_300;

    if (clean(row["Appels en Suisse ( Heure )"]).toLowerCase().includes("illimité")) base += 0.5;
    if (clean(row["SMS & MMS (Suisse)"]).toLowerCase().includes("illimité")) base += 0.5;

    const roamGo = toNum(dataItin);
    base += Number.isFinite(roamGo) ? roamGo * CO2.ROAM_KG_PER_GB : CO2.ROAM_UNLIMITED_BONUS_GB;

    if (!Number.isFinite(toNum(minRoam))) base += CO2.ROAM_UNLIMITED_MIN_OFFSET;

    if (name.includes("noir") || name.includes("black")) base += CO2.PREMIUM_OFFSET;

    return Math.max(0, Math.round(base * 10) / 10);
  }

  // Home (2/3)
  if (typeOffer.startsWith("2 ") || typeOffer.startsWith("3 ")) {
    let base =
      name.includes("fiber") ? CO2.HOME_FIBER :
      name.includes("5g")    ? CO2.HOME_5G    :
                                CO2.HOME_CABLE;
    if (isTvOn(tv)) base += CO2.TV_EXTRA;
    return Math.round(base * 10) / 10;
  }

  return 0;
}

// -------------------- Normalisation d’une offre --------------------
function normRow({ ref, operator, title, price, discount, hasTV, speedVal, speedUnit, smsCH, appelsCH, roamData, roamMin, countries, type }) {
  const row = {
    "Référence de l'offre": ref,
    "Opérateur": operator,
    "Nom de l'offre": title || "",
    "Prix CHF/mois": price || "",
    "Prix initial CHF": "",
    "Rabais (%)": discount ? `${discount}%` : "",
    "TV": hasTV ? "280 Chaines" : "Non",
    "Rapidité réseau Mbps": toMbps(speedVal, speedUnit),
    "SMS & MMS (Suisse)": smsCH || "",
    "Appels en Suisse ( Heure )": appelsCH || "",
    "Données en itinérance (Go)": /illimit/i.test(roamData || "") ? "Illimité" : (roamData || ""),
    "Minutes roaming ( Heure )": /illimit/i.test(roamMin || "") ? "Illimité" : (roamMin || ""),
    "Pays voisins inclus": countries || "Aucun",
    "Type offre": type,
    "Expiration": "",
    "~Émission de CO2 (kg/an)": "",       // rempli juste après
    "Durée d'engagement": "Sans engagement",
  };
  row["~Émission de CO2 (kg/an)"] = estimateCO2(row);
  return row;
}

// -------------------- Sitemap (xml ou gz) --------------------
async function fetchTextSmart(url) {
  const res = await fetch(url, { headers: { "User-Agent": "HelixCompareBot/1.0 (+noncommercial)" } });
  const ct = res.headers.get("content-type") || "";
  if (url.endsWith(".gz") || /gzip/.test(ct)) {
    const buf = Buffer.from(await res.arrayBuffer());
    return gunzipSync(buf).toString("utf8");
    }
  return await res.text();
}

async function fetchSitemapUrls(sitemapUrl, filter, depth = 0) {
  try {
    const xml = await fetchTextSmart(sitemapUrl);
    const isIndex = /<(?:sitemapindex)\b/i.test(xml);
    if (isIndex) {
      const sitemaps = [...xml.matchAll(/<loc>(.*?)<\/loc>/gi)].map(m => m[1]).slice(0, 15);
      const all = [];
      for (const sm of sitemaps) {
        const urls = await fetchSitemapUrls(sm, filter, depth + 1);
        all.push(...urls);
        await sleep(150);
      }
      return all;
    } else {
      const urls = [...xml.matchAll(/<loc>(.*?)<\/loc>/gi)].map(m => m[1]);
      return urls.filter(filter);
    }
  } catch (e) {
    console.warn("⚠️  Sitemap error:", sitemapUrl, e.message);
    return [];
  }
}

// -------------------- Extraction page --------------------
async function extractFromPage(page, url) {
  await page.goto(url, { waitUntil: "networkidle", timeout: 45000 });

  // Ouvre quelques éléments interactifs (accordéons) pour capter plus de texte
  try {
    const clickables = await page.$$("button, summary, [role='button']");
    for (const b of clickables.slice(0, 5)) { await b.click().catch(() => {}); }
    await page.waitForTimeout(250);
  } catch {}

  return await page.evaluate(() => {
    const txt = (el) => (el?.innerText || el?.textContent || "").replace(/\s+/g, " ").trim();
    const all = txt(document.body);

    const title =
      txt(document.querySelector("h1")) ||
      txt(document.querySelector("h2[data-test], h2")) ||
      (document.title || "").replace(/\|.*$/, "").trim();

    const price =
      (all.match(/CHF\s*([0-9]+(?:[.,][0-9]{1,2})?)/i)?.[1]) ||
      (all.match(/([0-9]+(?:[.,][0-9]{1,2})?)\s*(?:CHF)?\s*(?:par\s*mois|\/mois)/i)?.[1]) ||
      "";

    const discount = (all.match(/(\d{1,2})\s*%/i)?.[1]) || "";

    const hasTV = /\bTV\b|280\s*Cha(?:î|i)nes|replay|box\s+tv/i.test(all);

    const speed = all.match(/(\d+(?:[.,]\d+)?)\s*(Gbit\/?s|Gbps|Gbit|Mbit\/?s|Mbps|Mbit)/i);
    const speedVal = speed?.[1] || "";
    const speedUnit = speed?.[2] || "";

    const appelsCH = /Appels?.{0,25}Illimit/i.test(all)
      ? "Illimité"
      : (all.match(/Appels?.{0,15}(\d+\s*h|\d+\s*min)/i)?.[1] || "");

    const smsCH = /SMS.{0,10}Illimit/i.test(all)
      ? "Illimité"
      : (all.match(/SMS.*?(0[,\.]15\s*CHF\/SMS.*?0[,\.]50\s*CHF\/MMS|0\.15\/SMS,\s*0\.50\/MMS)/i)?.[1] || "");

    const roamData = /itin[ée]rance|roaming/i.test(all)
      ? (/illimit/i.test(all) ? "Illimité" : (all.match(/(\d+(?:[.,]\d+)?)\s*(?:Go|GB)\s*(?:en\s+itin[ée]rance|roaming)?/i)?.[1] || "0"))
      : "";

    const roamMin = /itin[ée]rance|roaming/i.test(all)
      ? (/illimit/i.test(all) ? "Illimité" : (all.match(/(\d+)\s*(?:min|minutes)\s*(?:international|roaming)?/i)?.[1] || ""))
      : "";

    const countries =
      (all.match(/Europe.*?USA.*?Canada.*?Turquie/i)?.[0]) ||
      (all.match(/\bFR,\s*DE,\s*IT,\s*AT,\s*LI\b/i)?.[0]) ||
      (all.match(/\bPays voisins(?:\s+\+\s*Balkans)?\b/i)?.[0]) ||
      (all.match(/\bTop\s*10\s*destinations.*?\b/i)?.[0]) ||
      "";

    return { title, price, discount, hasTV, speedVal, speedUnit, smsCH, appelsCH, roamData, roamMin, countries };
  });
}

// -------------------- Run principal --------------------
async function run() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "HelixCompareBot/1.0 (+noncommercial)",
    locale: "fr-CH",
  });
  const page = await ctx.newPage();

  const allRows = [];

  for (const op of OPERATORS) {
    console.log(`🔎 ${op.name} — lecture sitemap`);
    const urls = await fetchSitemapUrls(op.sitemap, op.urlFilter);
    console.log(` → ${urls.length} pages candidates`);
    const sample = urls.slice(0, 80); // limite courtoise

    for (const u of sample) {
      try {
        const d = await extractFromPage(page, u);
        const ref = slugFromUrl(u);
        const row = normRow({
          ref,
          operator: op.name,
          title: d.title,
          price: (d.price || "").toString().replace(",", "."),
          discount: d.discount,
          hasTV: d.hasTV,
          speedVal: d.speedVal,
          speedUnit: d.speedUnit,
          smsCH: d.smsCH,
          appelsCH: d.appelsCH,
          roamData: d.roamData,
          roamMin: d.roamMin,
          countries: d.countries,
          type: op.typeFromUrl(u, d.hasTV),
        });
        allRows.push(row);
        await sleep(350);
      } catch (e) {
        console.warn(`⚠️ ${op.name}: ${u} → ${e.message}`);
      }
    }
  }

  await browser.close();

  // dédoublonnage simple
  const key = (r) => `${r["Référence de l'offre"]}|${r["Opérateur"]}|${r["Nom de l'offre"]}`;
  const seen = new Set();
  const rows = [];
  for (const r of allRows) {
    const k = key(r);
    if (!seen.has(k)) { seen.add(k); rows.push(r); }
  }

  // tri par opérateur puis prix
  rows.sort((a, b) => {
    const op = a["Opérateur"].localeCompare(b["Opérateur"]);
    if (op !== 0) return op;
    const pa = parseFloat(a["Prix CHF/mois"]) || 99999;
    const pb = parseFloat(b["Prix CHF/mois"]) || 99999;
    return pa - pb;
  });

  const outPath = path.join(path.dirname(fileURLToPath(import.meta.url)), "data", "latest.csv");
  await writeCSV(rows, outPath);
  console.log(`✅ ${rows.length} offres → ${outPath}`);

  // -------------------- Push Airtable (optionnel) --------------------
  const { AIRTABLE_TOKEN, AIRTABLE_BASE, AIRTABLE_TABLE } = process.env;

  if (AIRTABLE_TOKEN && AIRTABLE_BASE && AIRTABLE_TABLE) {
    console.log("☁️  Push vers Airtable…");

    const toNumAPI = (v) => {
      const n = parseFloat(String(v ?? "").replace(",", "."));
      return Number.isFinite(n) ? n : null;
    };
    const toPercent0_1 = (v) => {
      if (v == null || v === "") return null;
      const n = parseFloat(String(v).replace("%", "").replace(",", "."));
      return Number.isFinite(n) ? n / 100 : null;
    };
    const toSingle = (v) => (v == null || v === "" ? null : String(v));
    const toMulti = (v) => {
      const s = String(v ?? "").trim();
      return s ? [{ name: s }] : [];
    };

    const records = rows.map((r) => ({
      fields: {
        "Référence de l'offre": toSingle(r["Référence de l'offre"]), // primaire
        "Opérateur": toSingle(r["Opérateur"]),
        "Type offre": toSingle(r["Type offre"]),
        "Nom de l'offre": toSingle(r["Nom de l'offre"]),

        "Prix CHF/mois": toNumAPI(r["Prix CHF/mois"]),
        "Prix initial CHF": toNumAPI(r["Prix initial CHF"]),

        "Rabais (%)": toPercent0_1(r["Rabais (%)"]),

        "TV": toMulti(r["TV"]),

        "Rapidité réseau Mbps": toNumAPI(r["Rapidité réseau Mbps"]),

        "SMS & MMS (Suisse)": toSingle(r["SMS & MMS (Suisse)"]),
        "Appels en Suisse ( Heure )": toSingle(r["Appels en Suisse ( Heure )"]),

        "Données en itinérance (Go)": toSingle(r["Données en itinérance (Go)"]),
        "Minutes roaming ( Heure )": toSingle(r["Minutes roaming ( Heure )"]),

        "Pays voisins inclus": toSingle(r["Pays voisins inclus"]),

        "Expiration": toSingle(r["Expiration"]) || "Remise permanente",

        "~Émission de CO2 (kg/an)": toSingle(r["~Émission de CO2 (kg/an)"]),

        "Durée d'engagement": toSingle(r["Durée d'engagement"]) || "Sans engagement",
      },
    }));

    const chunk = (arr, n = 10) => (arr.length ? [arr.slice(0, n), ...chunk(arr.slice(n), n)] : []);
    for (const part of chunk(records, 10)) {
      const res = await fetch(
        `https://api.airtable.com/v0/${AIRTABLE_BASE}/${encodeURIComponent(AIRTABLE_TABLE)}`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${AIRTABLE_TOKEN}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ records: part, typecast: true }),
        }
      );
      if (!res.ok) {
        const txt = await res.text();
        console.error("Airtable error:", txt);
        throw new Error("Airtable push failed");
      }
      await sleep(300);
    }
    console.log("✅ Airtable mis à jour (table: Offres).");
  } else {
    console.log("ℹ️  Secrets Airtable absents → skip push (CSV uniquement).");
  }
}

// -------------------- Exécution --------------------
run().catch((e) => {
  console.error("❌ Run failed:", e);
  process.exit(1);
});

