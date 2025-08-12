// scrape.mjs — Saint-Graal (sans LLM), robuste & gratuit
// Node >= 20 requis. Sortie: data/latest.csv
//
// ENV utiles :
//   HEADLESS=1            # 0 pour voir le navigateur
//   FAST_MODE=1           # timeouts plus courts lors des tests
//   MAX_PAGES=60          # limite d'URLs traitées
//   OPERATORS=yallo,sunrise  # filtres par domaine
//   BLOCK_RESOURCES=1     # bloque images/medias/fonts (accélère)
//   AIRTABLE_TOKEN=...    # (optionnel) clé Airtable
//   AIRTABLE_BASE=...     # (optionnel) id base
//   AIRTABLE_TABLE=Offres # (optionnel) table (nom ou id)
//
// Usage local :
//   npm i playwright
//   npx playwright install chromium
//   node scrape.mjs
//
// GitHub Actions :
//   - Ajoute secrets Airtable si besoin
//   - Lance le workflow (cron/jour)
//   - Récupère l’artifact latest.csv

import { chromium } from 'playwright';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// ---------- Paths ----------
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.join(__dirname, 'data');
await fs.mkdir(DATA_DIR, { recursive: true });

// ---------- Config ----------
const HEADLESS        = process.env.HEADLESS !== '0';
const FAST_MODE       = process.env.FAST_MODE === '1';
const MAX_PAGES       = parseInt(process.env.MAX_PAGES || '70', 10);
const NAV_TIMEOUT     = FAST_MODE ? 12000 : 22000;
const SEL_TIMEOUT     = FAST_MODE ? 2500  : 5000;
const PAGE_BUDGET_MS  = FAST_MODE ? 18000 : 32000;
const RETRIES         = 2;
const CONCURRENCY     = FAST_MODE ? 3 : 4;
const BLOCK_RESOURCES = process.env.BLOCK_RESOURCES !== '0';

const OPERATORS_FILTER = (process.env.OPERATORS || 'yallo,sunrise')
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);

// ---------- CSV headers ----------
const HEADERS = [
  "Référence de l'offre","Opérateur","Nom de l'offre","Prix CHF/mois","Prix initial CHF","Rabais (%)","TV",
  "Rapidité réseau Mbps","SMS & MMS (Suisse)","Appels en Suisse ( Heure )",
  "Données en itinérance (Go)","Minutes roaming ( Heure )","Pays voisins inclus",
  "Type offre","Expiration","~Émission de CO2 (kg/an)","Durée d'engagement"
];

// ---------- Utils ----------
const log = (...a) => console.log('•', ...a);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const clean = (s) => String(s ?? '').replace(/\u00A0/g,' ').replace(/\s+/g,' ').trim();
const numberish = (s) => {
  const m = String(s ?? '').match(/-?\d+(?:[.,]\d+)?/);
  return m ? parseFloat(m[0].replace(',','.')) : null;
};
const toCSV = (rows) => [
  HEADERS.join(','),
  ...rows.map(r => HEADERS.map(k => {
    let v = r[k];
    if (v === undefined || v === null) v = '';
    const s = String(v).replace(/"/g,'""');
    return /[",\n]/.test(s) ? `"${s}"` : s;
  }).join(','))
].join('\n');

// ---------- Référence stable (hash) ----------
function slugUpper(s) {
  return String(s ?? '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .replace(/[^a-zA-Z0-9]+/g,'_').replace(/^_|_$/g,'')
    .toUpperCase();
}
function fnv32a(str){
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++){
    h ^= str.charCodeAt(i);
    h = (h >>> 0) * 0x01000193;
  }
  return (h >>> 0).toString(16).toUpperCase().padStart(8,'0');
}
function typeCode(typeOffre) {
  const t = String(typeOffre||'');
  if (t.startsWith('1')) return 'T1';
  if (t.startsWith('2')) return 'T2';
  if (t.startsWith('3')) return 'T3';
  return 'TX';
}
function makeRef({ operator, title, url, typeOffre }) {
  const op = slugUpper(operator).slice(0,8);
  const tc = typeCode(typeOffre);
  const sl = slugUpper(title).slice(0,24);
  let pathOnly = '';
  try { pathOnly = new URL(url).pathname; } catch {}
  const h8 = fnv32a(`${pathOnly}|${sl}`).slice(0,8);
  return `${op}_${tc}_${sl}_${h8}`;
}
const seenRefs = new Set();
function ensureUniqueRef(ref){
  let r = ref, n = 2;
  while (seenRefs.has(r)) r = `${ref}_${n++}`;
  seenRefs.add(r);
  return r;
}

// ---------- CO₂ (ta logique intégrée) ----------
function numCO2(v){
  if (v == null) return 0;
  const s = String(v);
  if (s.toLowerCase().includes('illimité')) return Infinity;
  const m = s.replace(',', '.').match(/-?\d+(\.\d+)?/);
  return m ? parseFloat(m[0]) : 0;
}
function calculateCO2(row) {
  const type_offer = row['Type offre'] || '';
  const speed = (() => { const n = numberish(row['Rapidité réseau Mbps']); return (n==null||!isFinite(n)) ? 0 : n; })();
  const data_itin = row['Données en itinérance (Go)'] || '';
  const min_roam = row['Minutes roaming ( Heure )'] || '';
  const tv = row['TV'] || '';
  const name = (row["Nom de l'offre"] || '').toLowerCase();

  if (type_offer.includes('1 ')) {
    let base = speed <= 300 ? 12.5 : 14.5;
    if ((row['Appels en Suisse ( Heure )'] || '').toLowerCase().includes('illimité')) base += 0.5;
    if ((row['SMS & MMS (Suisse)'] || '').toLowerCase().includes('illimité')) base += 0.5;

    const roaming_go = numCO2(data_itin);
    const is_unlimited_roam_go = roaming_go === Infinity;
    const is_unlimited_roam_min = String(min_roam).toLowerCase().includes('illimit');

    const roaming_cont = is_unlimited_roam_go ? 18.3 : (roaming_go * 0.35);
    base += roaming_cont;
    if (is_unlimited_roam_min) base -= 12.2;
    if (name.includes('noir') || name.includes('black')) base -= 0.5;

    return Math.round(base * 10) / 10;
  } else if (type_offer.includes('2 ') || type_offer.includes('3 ')) {
    let base;
    if (name.includes('fiber')) base = 30.0;
    else if (name.includes('5g')) base = 45.0;
    else base = 35.0;

    if (tv && tv.toLowerCase().includes('chaine')) base += 50.0;
    return Math.round(base * 10) / 10;
  }
  return null;
}

// ---------- Canonicalisation ----------
function canonicalize(row, pageText='') {
  const txt = (v) => String(v||'').toLowerCase();
  const normInf = (v) =>
    /illimit|unlimited|ohne limit|flat|illimitato|∞/i.test(String(v||'')) ? 'Illimité' :
    (String(v||'').trim()==='-' ? '' : String(v||''));

  row["Données en itinérance (Go)"] = normInf(row["Données en itinérance (Go)"]);
  row["Minutes roaming ( Heure )"]  = normInf(row["Minutes roaming ( Heure )"]);

  // Vitesse -> Mbps
  let sp = String(row["Rapidité réseau Mbps"]||'');
  const g = sp.match(/(\d+(?:[.,]\d+)?)\s*g(?:bit|bps)?/i);
  const m = sp.match(/(\d+(?:[.,]\d+)?)\s*m(?:bit|bps)?/i);
  if (g) sp = String(Math.round(parseFloat(g[1].replace(',','.')) * 1000));
  else if (m) sp = String(Math.round(parseFloat(m[1].replace(',','.'))));
  row["Rapidité réseau Mbps"] = sp.replace(/[^\d]/g,'') || '';

  // TV
  const tvt = txt(row["TV"]);
  row["TV"] = /280\s*cha/i.test(tvt) ? "280 Chaines" : (/tv|replay|box/.test(tvt) ? "280 Chaines" : "Non");

  // Type offre (déduit du texte de page si vide)
  const page = txt(pageText);
  const type = txt(row["Type offre"]);
  const isHome = /home|internet|cable|fiber|fibre|box|tv|5g home/i.test(page) || /home|cable|fiber|tv/.test(type);
  const hasTV  = row["TV"] !== "Non";
  row["Type offre"] = isHome ? (hasTV ? "3 Home Cable Box + TV" : "2 Home Cable Box") : "1 SIM ( Mobile )";

  // Rabais entier
  let rb = String(row["Rabais (%)"]||'').replace('%','').replace(',','.');
  const rbn = numberish(rb);
  row["Rabais (%)"] = (rbn==null || !isFinite(rbn)) ? '' : String(Math.round(rbn));

  // Prix → nombres
  const pn = numberish(row["Prix CHF/mois"]);     row["Prix CHF/mois"]     = (pn??'');
  const po = numberish(row["Prix initial CHF"]);  row["Prix initial CHF"]  = (po??'');

  // Durée / Expiration si trouvables
  if (!row["Durée d'engagement"] && /sans engagement/i.test(page)) row["Durée d'engagement"] = 'Sans engagement';
  if (!row["Expiration"] && /remise permanente|rabais permanent/i.test(page)) row["Expiration"] = 'Remise permanente';

  return row;
}

// ---------- Sitemaps ----------
async function gatherCandidateURLs() {
  const sitemaps = [];
  if (OPERATORS_FILTER.includes('yallo')) {
    sitemaps.push('https://www.yallo.ch/sitemap.xml', 'https://www.yallo.ch/fr/sitemap.xml');
  }
  if (OPERATORS_FILTER.includes('sunrise')) {
    sitemaps.push('https://www.sunrise.ch/sitemap.xml');
  }

  const urls = new Set();
  for (const sm of sitemaps) {
    try {
      const res = await fetch(sm, { redirect:'follow' });
      const xml = await res.text();
      for (const loc of xml.matchAll(/<loc>(.*?)<\/loc>/g)) urls.add(String(loc[1]));
    } catch (e) { log('Sitemap KO:', sm, e.message); }
  }

  const want = [...urls].filter(u => {
    let host = '';
    try { host = new URL(u).hostname; } catch { return false; }
    const op = host.includes('yallo') ? 'yallo' : host.includes('sunrise') ? 'sunrise' : 'other';
    if (!OPERATORS_FILTER.includes(op)) return false;

    // garde pages offres
    if (!/(mobile|roaming|home|internet|5g|cable|fiber|max|europe|swiss|travel)/i.test(u)) return false;

    // exclusions
    if (/\.(xml|pdf|jpg|jpeg|png|gif|webp|svg)$/i.test(u)) return false;
    if (/login|myyallo|privacy|terms|sitemap|media|assets|image|support|help/i.test(u)) return false;
    return true;
  });

  const list = want.slice(0, MAX_PAGES);
  if (!list.length) list.push('https://www.yallo.ch/fr/mobile');
  log(`URLs candidates: ${list.length}/${want.length} (MAX_PAGES=${MAX_PAGES})`);
  return list;
}

// ---------- Cookies ----------
async function acceptCookies(page) {
  const labels = ['Accepter','J’accepte','Tout accepter','OK','Accept all','Agree','Einverstanden','Alles akzeptieren'];
  for (const t of labels) {
    try {
      const b = page.getByRole('button', { name: new RegExp(t, 'i') });
      if (await b.first().isVisible({ timeout: 800 })) {
        await b.first().click({ timeout: 800 }).catch(()=>{});
        return;
      }
    } catch {}
  }
  // fallback sélecteurs connus
  const sel = ['#onetrust-accept-btn-handler','button[aria-label*="accept"]','button[aria-label*="accepter"]'];
  for (const s of sel) {
    try {
      const l = page.locator(s).first();
      if (await l.isVisible({ timeout: 800 })) { await l.click({ timeout: 800 }).catch(()=>{}); return; }
    } catch {}
  }
}

// ---------- Extraction regex + mini-sélecteurs ----------
function extractByRegex(text) {
  const t = clean(text);

  const mNow = t.match(/CHF\s*([0-9]+(?:[.,][0-9]+)?)/i) || t.match(/([0-9]+(?:[.,][0-9]+)?)\s*CHF/i);
  const mOld = t.match(/au lieu de\s*(?:CHF\s*)?([0-9]+(?:[.,][0-9]+)?)/i);
  const priceNow = mNow ? parseFloat(mNow[1].replace(',','.')) : '';
  const priceOld = mOld ? parseFloat(mOld[1].replace(',','.')) : '';
  const discount = (() => {
    const m = t.match(/(\d{1,3})\s*%/);
    if (m) return m[1];
    if (priceNow && priceOld) return String(Math.round((1 - priceNow/priceOld)*100));
    return '';
  })();

  // speed
  let speedMbps = '';
  const mSp = t.match(/(\d{1,5})\s*Mbit\/s/i) || t.match(/(\d{1,3})\s*Gbit\/s/i) || t.match(/(\d{1,3})\s*Gbps/i);
  if (mSp) speedMbps = /Gbit|Gbps/i.test(mSp[0]) ? String(parseInt(mSp[1],10)*1000) : String(parseInt(mSp[1],10));
  if (/10000\s*Mbit\/s/i.test(t) || /10\s*Gbit\/s/i.test(t)) speedMbps = '10000';

  // TV
  const tv = /280\s*Cha[iî]nes/i.test(t) ? '280 Chaines' : 'Non';

  // appels/sms CH (détection simple mais robuste)
  const appels = /appel/i.test(t) && /illimit/i.test(t) ? 'Illimité' : (/1\s*h/i.test(t) ? '1 h' : '');
  const sms    = /sms/i.test(t) && /illimit/i.test(t) ? 'Illimité' :
                 (/0[,\.]?15.*sms/i.test(t) || /0[,\.]?50.*mms/i.test(t)) ? '0.15/SMS, 0.50/MMS' : '';

  // roaming data / minutes
  const roamData = /roam|itin/i.test(t) && /illimit/i.test(t) ? 'Illimité' :
                   (t.match(/(\d+(?:[.,]\d+)?)\s*Go/i)?.[1]?.replace(',','.') ?? '');
  const roamMin  = /roam|itin/i.test(t) && /illimit/i.test(t) && /(min|h)/i.test(t) ? 'Illimité' :
                   (t.match(/(\d+)\s*h\s*(?:roam|itin)/i)?.[1] || t.match(/(\d+)\s*min\s*(?:roam|itin)/i)?.[1] || '');

  // pays voisins
  const neighbors = /FR,\s*DE,\s*IT,\s*AT,\s*LI/i.test(t) ? 'FR, DE, IT, AT, LI' :
                    /Europe,\s*USA,\s*Canada,\s*Turquie/i.test(t) ? 'Europe, USA, Canada, Turquie' :
                    /Pays voisins \+ Balkans/i.test(t) ? 'Pays voisins + Balkans' :
                    /8 pays les plus populaires/i.test(t) ? '8 pays les plus populaires' :
                    /Top 10 destinations monde/i.test(t) ? 'Top 10 destinations monde' :
                    /Europe, US, Canada/i.test(t) ? 'Europe, US, Canada' :
                    /Pays voisins/i.test(t) ? 'Pays voisins' : '';

  const expiration = /remise permanente|rabais permanent/i.test(t) ? 'Remise permanente' : '';
  const engagement = /sans engagement/i.test(t) ? 'Sans engagement' : '';

  const typeOffre = /home|internet|cable|fiber|fibre|box/i.test(t)
    ? (tv==='280 Chaines' ? '3 Home Cable Box + TV' : '2 Home Cable Box')
    : '1 SIM ( Mobile )';

  return { priceNow, priceOld, discount, speedMbps, tv, appels, sms, roamData, roamMin, neighbors, expiration, engagement, typeOffre };
}

async function extractOfferFromPage(page, url) {
  const started = Date.now();
  await page.goto(url, { waitUntil:'domcontentloaded', timeout: NAV_TIMEOUT }).catch(()=>{});
  await acceptCookies(page);

  let title = '';
  try { title = clean(await page.locator('h1').first().innerText({ timeout: SEL_TIMEOUT }).catch(()=> '')); } catch {}
  if (!title) {
    try {
      const og = await page.locator('meta[property="og:title"]').getAttribute('content', { timeout: 800 }).catch(()=> '');
      title = clean(og||'');
    } catch {}
  }
  const bodyText = clean(await page.innerText('body').catch(()=> ''));
  const op = new URL(url).hostname.includes('yallo') ? 'Yallo' :
             new URL(url).hostname.includes('sunrise') ? 'Sunrise' : 'Opérateur';

  // Extraction regex “grosse maille”
  const ex = extractByRegex(bodyText);

  // Fallback sélecteurs prix si besoin
  if (!ex.priceNow) {
    const selCandidates = [
      '[data-test*="price"]','[data-testid*="price"]','.price','.offer-price','.product-price','[class*="Price"]'
    ];
    for (const s of selCandidates) {
      try {
        const raw = clean(await page.locator(s).first().innerText({ timeout: 800 }).catch(()=> ''));
        const m = raw.match(/([0-9]+(?:[.,][0-9]+)?)/);
        if (m) { ex.priceNow = parseFloat(m[1].replace(',','.')); break; }
      } catch {}
    }
  }
  if (!ex.priceOld) {
    const selOld = ['.old-price','.price-old','[class*="strike"]','[class*="strikethrough"]','s[class*="price"]'];
    for (const s of selOld) {
      try {
        const raw = clean(await page.locator(s).first().innerText({ timeout: 800 }).catch(()=> ''));
        const m = raw.match(/([0-9]+(?:[.,][0-9]+)?)/);
        if (m) { ex.priceOld = parseFloat(m[1].replace(',','.')); break; }
      } catch {}
    }
  }
  if (!ex.discount && ex.priceNow && ex.priceOld) {
    ex.discount = String(Math.round((1 - ex.priceNow/ex.priceOld)*100));
  }

  // Construit la ligne
  const row = {
    "Référence de l'offre": "", // rempli après
    "Opérateur": op,
    "Nom de l'offre": title || 'Offre',
    "Prix CHF/mois": ex.priceNow || '',
    "Prix initial CHF": ex.priceOld || '',
    "Rabais (%)": ex.discount || '',
    "TV": ex.tv || 'Non',
    "Rapidité réseau Mbps": ex.speedMbps || '',
    "SMS & MMS (Suisse)": ex.sms || '',
    "Appels en Suisse ( Heure )": ex.appels || '',
    "Données en itinérance (Go)": ex.roamData || '',
    "Minutes roaming ( Heure )": ex.roamMin || '',
    "Pays voisins inclus": ex.neighbors || '',
    "Type offre": ex.typeOffre || '',
    "Expiration": ex.expiration || '',
    "~Émission de CO2 (kg/an)": "",
    "Durée d'engagement": ex.engagement || ''
  };

  canonicalize(row, bodyText);
  const co2 = calculateCO2(row);
  if (co2 !== null) row["~Émission de CO2 (kg/an)"] = co2;

  row["Référence de l'offre"] = ensureUniqueRef(
    makeRef({ operator: row["Opérateur"], title: row["Nom de l'offre"], url, typeOffre: row["Type offre"] })
  );

  // Respect d’un budget de temps par page
  const elapsed = Date.now() - started;
  if (elapsed < PAGE_BUDGET_MS) await sleep(10);

  return row;
}

// ---------- Pool de workers ----------
async function runWithPool(context, urls) {
  const results = [];
  let index = 0;
  let ok = 0, ko = 0;

  async function worker(id) {
    const page = await context.newPage();
    page.setDefaultTimeout(SEL_TIMEOUT);
    page.setDefaultNavigationTimeout(NAV_TIMEOUT);

    while (true) {
      const my = index++;
      if (my >= urls.length) break;
      const url = urls[my];

      let lastErr = null;
      for (let r=0; r<=RETRIES; r++) {
        try {
          const row = await extractOfferFromPage(page, url);
          if (row && row["Nom de l'offre"]) {
            results.push(row);
            ok++;
            break;
          }
        } catch (e) {
          lastErr = e;
          await sleep(300);
        }
      }
      if (lastErr) { ko++; log(`Skip (${id}):`, url, lastErr.message); }
    }
    await page.close().catch(()=>{});
  }

  const workers = [];
  const count = Math.min(CONCURRENCY, urls.length);
  for (let i=0; i<count; i++) workers.push(worker(i+1));
  await Promise.all(workers);

  log(`OK=${ok} | KO=${ko}`);
  return results;
}

// ---------- Airtable (optionnel) ----------
async function pushAirtable(rows) {
  const { AIRTABLE_TOKEN, AIRTABLE_BASE, AIRTABLE_TABLE } = process.env;
  if (!AIRTABLE_TOKEN || !AIRTABLE_BASE || !AIRTABLE_TABLE) {
    log('Airtable non configuré → skip');
    return;
  }
  const chunk = 10;
  for (let i=0; i<rows.length; i+=chunk) {
    const part = rows.slice(i, i+chunk).map(r => ({
      fields: {
        "Opérateur": r["Opérateur"],
        "Référence de l'offre": r["Référence de l'offre"],
        "Type offre": r["Type offre"],
        "Nom de l'offre": r["Nom de l'offre"],
        "Prix CHF/mois": r["Prix CHF/mois"] || null,
        "Prix initial CHF": r["Prix initial CHF"] || null,
        "Rabais (%)": r["Rabais (%)"] || null,
        "TV": r["TV"] ? [r["TV"]] : [],
        "Rapidité réseau Mbps": r["Rapidité réseau Mbps"] || null,
        "SMS & MMS (Suisse)": r["SMS & MMS (Suisse)"] || null,
        "Appels en Suisse ( Heure )": r["Appels en Suisse ( Heure )"] || null,
        "Données en itinérance (Go)": r["Données en itinérance (Go)"] || null,
        "Minutes roaming ( Heure )": r["Minutes roaming ( Heure )"] || null,
        "Pays voisins inclus": r["Pays voisins inclus"] || null,
        "Expiration": r["Expiration"] || null,
        "~Émission de CO2 (kg/an)": r["~Émission de CO2 (kg/an)"] || null,
        "Durée d'engagement": r["Durée d'engagement"] || null
      }
    }));

    const res = await fetch(
      `https://api.airtable.com/v0/${AIRTABLE_BASE}/${encodeURIComponent(AIRTABLE_TABLE)}`,
      {
        method:'PATCH',
        headers:{ Authorization:`Bearer ${AIRTABLE_TOKEN}`, 'Content-Type':'application/json' },
        body: JSON.stringify({
          performUpsert: { fieldsToMergeOn: ["Référence de l'offre"] },
          records: part
        })
      }
    );
    if (!res.ok) {
      const txt = await res.text().catch(()=> '');
      throw new Error(`Airtable ${res.status}: ${txt}`);
    }
  }
  log('✅ Airtable mis à jour.');
}

// ---------- Main ----------
async function main() {
  log('Collecte des URLs…');
  const urls = await gatherCandidateURLs();

  const browser = await chromium.launch({ headless: HEADLESS });
  const context = await browser.newContext({
    viewport: { width: 1366, height: 900 },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36'
  });

  if (BLOCK_RESOURCES) {
    await context.route('**/*', (route) => {
      const type = route.request().resourceType();
      const url  = route.request().url();
      const block = ['image','media','font'].includes(type) ||
                    /google-analytics|gtm|doubleclick|hotjar|optimizely/i.test(url);
      if (block) return route.abort();
      route.continue();
    });
  }

  const rows = await runWithPool(context, urls);
  await browser.close();

  // Dédup par Référence
  const map = new Map();
  for (const r of rows) map.set(r["Référence de l'offre"], r);
  const finalRows = [...map.values()];

  const outCSV = toCSV(finalRows);
  const outPath = path.join(DATA_DIR, 'latest.csv');
  await fs.writeFile(outPath, outCSV, 'utf8');
  log(`✅ ${finalRows.length} offres écrites dans ${outPath}`);

  // Airtable (optionnel)
  try { await pushAirtable(finalRows); } catch (e) { log('Airtable:', e.message); }
}

main().catch(e => { console.error('❌ Fatal:', e); process.exit(1); });