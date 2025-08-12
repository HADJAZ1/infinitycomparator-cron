// scrape.mjs — version renforcée (timeouts, retries, fallback, anti-blocage)
// Node 20 + Playwright (Chromium). Tourne dans GitHub Actions ou en local.
// ENV utiles (déjà supportés par le workflow) :
//  MAX_PAGES=30 FAST_MODE=1 HEADLESS=1 DEBUG_SNAPSHOTS=0
//  AIRTABLE_TOKEN, AIRTABLE_BASE, AIRTABLE_TABLE (facultatifs)

// ---------- Imports ----------
import { chromium } from 'playwright';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---------- Config ----------
const HEADLESS = process.env.HEADLESS !== '0';
const FAST_MODE = process.env.FAST_MODE === '1';
const MAX_PAGES = parseInt(process.env.MAX_PAGES || '70', 10);
const DEBUG_SNAPSHOTS = process.env.DEBUG_SNAPSHOTS === '1';

// timeouts (ms)
const NAV_TIMEOUT = FAST_MODE ? 12000 : 20000;
const SEL_TIMEOUT = FAST_MODE ? 2500 : 5000;
const PAGE_BUDGET = FAST_MODE ? 18000 : 28000; // temps max par URL
const RETRIES = 2;
const CONCURRENCY = FAST_MODE ? 3 : 4;

const DATA_DIR = path.join(__dirname, 'data');
await fs.mkdir(DATA_DIR, { recursive: true });

// ---------- Utils ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
const log = (...a) => console.log('•', ...a);

function sanitize(s) {
  return String(s ?? '').replace(/\s+/g, ' ').replace(/\u00A0/g, ' ').trim();
}
function firstNonEmpty(...arr) {
  for (const v of arr) if (v && sanitize(v)) return sanitize(v);
  return '';
}
function num(v) {
  if (!v) return 0;
  const s = String(v).toLowerCase();
  if (s.includes('illimité')) return Infinity;
  const m = String(v).replace(',', '.').match(/-?\d+(\.\d+)?/);
  return m ? parseFloat(m[0]) : 0;
}
function pct(v) {
  if (!v) return null;
  const m = String(v).match(/(\d{1,3})(?=%)/);
  return m ? parseInt(m[1], 10) : null;
}

// ---------- CO2 (portage fidèle de ta fonction) ----------
function calculateCO2(row) {
  const type_offer = row['Type offre'] || '';
  const speed = num(row['Rapidité réseau Mbps']);
  const data_itin = row['Données en itinérance (Go)'] || '';
  const min_roam = row['Minutes roaming ( Heure )'] || '';
  const tv = row['TV'] || '';
  const name = (row["Nom de l'offre"] || '').toLowerCase();

  if (type_offer.includes('1 ')) {
    let base = speed <= 300 ? 12.5 : 14.5;
    if ((row['Appels en Suisse ( Heure )'] || '').toLowerCase().includes('illimité')) base += 0.5;
    if ((row['SMS & MMS (Suisse)'] || '').toLowerCase().includes('illimité')) base += 0.5;

    const roaming_go = num(data_itin);
    const is_unlimited_roam_go = roaming_go === Infinity;
    const is_unlimited_roam_min = num(min_roam) === Infinity;

    let roaming_cont = is_unlimited_roam_go ? 18.3 : (roaming_go * 0.35);
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

// ---------- CSV helpers ----------
const HEADERS = [
  "Référence de l'offre","Opérateur","Nom de l'offre","Prix CHF/mois","Prix initial CHF","Rabais (%)","TV",
  "Rapidité réseau Mbps","SMS & MMS (Suisse)","Appels en Suisse ( Heure )",
  "Données en itinérance (Go)","Minutes roaming ( Heure )","Pays voisins inclus",
  "Type offre","Expiration","~Émission de CO2 (kg/an)","Durée d'engagement"
];
function toCSVRow(o) {
  return HEADERS.map(h => {
    let v = o[h];
    if (v === undefined || v === null) v = '';
    const s = String(v).replace(/"/g, '""');
    return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
  }).join(',');
}

// ---------- Crawl list (sitemaps + filtres rapides) ----------
async function gatherCandidateURLs() {
  const maps = [
    'https://www.yallo.ch/sitemap.xml',
    'https://www.yallo.ch/fr/sitemap.xml',
    'https://www.sunrise.ch/sitemap.xml'
  ];
  const urls = new Set();
  for (const m of maps) {
    try {
      const res = await fetch(m, { redirect: 'follow' });
      const xml = await res.text();
      for (const loc of xml.matchAll(/<loc>(.*?)<\/loc>/g)) {
        const u = String(loc[1]);
        urls.add(u);
      }
    } catch (e) {
      log('sitemap fail:', m, e.message);
    }
  }
  // Filtre très strict : pages offres uniquement (évite le blog, faq…)
  const want = [...urls].filter(u =>
    /yallo\.ch/.test(u) &&
    /(mobile|roaming|europe|swiss|home|5g|cable|fiber)/i.test(u) &&
    !/login|myyallo|pdf|media|image|assets|\.xml|sitemap|privacy|terms/i.test(u)
  );

  const list = want.slice(0, MAX_PAGES);
  log(`Candidats retenus: ${list.length}/${want.length} (MAX_PAGES=${MAX_PAGES})`);
  if (list.length === 0) {
    // Fallback: quelques URLs connues (évite le run à vide)
    list.push(
      'https://www.yallo.ch/fr/mobile',
      'https://www.yallo.ch/fr/roaming',
      'https://www.yallo.ch/fr/home-internet'
    );
  }
  return list;
}

// ---------- Extraction ----------
async function acceptCookies(page) {
  const labels = [
    'Accepter','J’accepte','J accepte','OK','Tout accepter','Accept all','Agree'
  ];
  for (const text of labels) {
    const b = page.getByRole('button', { name: new RegExp(text, 'i') });
    try { if (await b.first().isVisible({ timeout: 800 })) { await b.first().click({ timeout: 800 }).catch(()=>{}); return; } } catch {}
  }
}

async function extractTextByLabels(page, labelList) {
  // Cherche "ligne" label : valeur, sinon scanne tout le texte de la page
  try {
    for (const lbl of labelList) {
      const row = page.locator(`xpath=//*[contains(text(),"${lbl}")]//ancestor::*[self::tr or self::li or self::div][1]`);
      const t = sanitize(await row.first().innerText({ timeout: SEL_TIMEOUT }).catch(()=>'')); 
      if (t && t.length < 200) return t;
    }
  } catch {}
  // Fallback: scan global
  const all = sanitize(await page.content().catch(()=>'')); 
  for (const lbl of labelList) {
    const re = new RegExp(lbl + '[^<>{}\\n\\r]{0,120}', 'i');
    const m = all.match(re);
    if (m) return sanitize(m[0]);
  }
  return '';
}

async function extractOfferFromPage(page, url) {
  // 1/ Navigation
  const abortAt = Date.now() + PAGE_BUDGET;
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT }).catch(()=>{});
  await acceptCookies(page);

  // 2/ Titre (nom de l’offre)
  let title = firstNonEmpty(
    await page.locator('h1').first().innerText().catch(()=> ''),
    await page.locator('[data-test=headline],[data-testid=headline],.headline,.product-title').first().innerText().catch(()=> '')
  );

  // 3/ Prix (actuel + barré)
  const bodyText = sanitize(await page.innerText('body').catch(()=> ''));
  const priceNow = (() => {
    // CHF 12.90  |  12.90 CHF  |  CHF 25.–
    const m = bodyText.match(/CHF\s*([0-9]+(?:[.,][0-9]+)?)/i) || bodyText.match(/([0-9]+(?:[.,][0-9]+)?)\s*CHF/i);
    return m ? parseFloat(m[1].replace(',', '.')) : null;
  })();
  const priceOld = (() => {
    // "au lieu de 30.–", "au lieu de CHF 85.–"
    const m = bodyText.match(/au lieu de\s*(?:CHF\s*)?([0-9]+(?:[.,][0-9]+)?)/i);
    return m ? parseFloat(m[1].replace(',', '.')) : null;
  })();
  let rabais = pct(bodyText) ?? (priceNow && priceOld ? Math.round((1 - priceNow/priceOld) * 100) : null);

  // 4/ Caractéristiques
  const appelsCH = await extractTextByLabels(page, ['Appels', 'Appels (Suisse)', 'Appels Suisse']);
  const smsCH = await extractTextByLabels(page, ['SMS', 'SMS & MMS']);
  const donnees = await extractTextByLabels(page, ['Données', 'Data']);
  const itin = await extractTextByLabels(page, ['Itinérance', 'Roaming']);
  const pays = await extractTextByLabels(page, ['pays voisins', 'Europe', 'FR, DE, IT, AT, LI']);
  const tvText = await extractTextByLabels(page, ['TV','Chaînes','Chaines']);

  // 5/ Vitesse (ex. "jusqu\'à 300 Mbit/s" ou "2 Gbit/s" ou "10000 Mbit/s")
  let speedMbps = 0;
  const mSpeed = bodyText.match(/(\d{1,5})\s*(?:M|m)bit\/s/) || bodyText.match(/(\d{1,3})\s*(?:G|g)bit\/s/);
  if (mSpeed) {
    const v = parseInt(mSpeed[1], 10);
    speedMbps = /G/i.test(mSpeed[0]) ? v * 1000 : v;
  }
  if (/10000\s*(?:M|m)bit\/s/.test(bodyText) || /10\s*(?:G|g)bit\/s/.test(bodyText)) speedMbps = 10000; // valeur demandée

  // 6/ Type d’offre + TV bool
  const tv = /280\s*Cha[iî]nes/i.test(tvText) ? '280 Chaines' : (/TV/i.test(tvText) ? 'TV' : 'Non');
  const typeOffre = /home|cable|fibre|fiber|tv|box/i.test(bodyText) ? ( /tv/i.test(tv) ? '3 Home Cable Box + TV' : '2 Home Cable Box') : '1 SIM ( Mobile )';

  // 7/ Champs roaming
  const datasRoam = (() => {
    if (/Illimit[ée]s?/i.test(itin) || /Illimit[ée]s?/i.test(donnees)) return 'Illimité';
    const m = itin.match(/(\d+(?:[.,]\d+)?)\s*Go/i) || donnees.match(/(\d+(?:[.,]\d+)?)\s*Go/i);
    return m ? m[1].replace(',', '.') : '0';
  })();
  const minRoam = (() => {
    if (/Illimit[ée]s?/i.test(itin)) return 'Illimité';
    const m = itin.match(/(\d+)\s*h/i) || bodyText.match(/(\d+)\s*h\s*(?:roaming|itin[ée]rance)/i);
    return m ? m[1] : '0';
  })();

  // 8/ Normalisation complète de la ligne
  const row = {
    "Référence de l'offre": `YALLO_${sanitize(title).toUpperCase().replace(/[^A-Z0-9]+/g,'_').replace(/^_|_$/g,'') || 'OFFRE'}`,
    "Opérateur": "Yallo",
    "Nom de l'offre": title || 'Offre',
    "Prix CHF/mois": priceNow ?? '',
    "Prix initial CHF": priceOld ?? '',
    "Rabais (%)": rabais ?? '',
    "TV": tv,
    "Rapidité réseau Mbps": speedMbps || '',
    "SMS & MMS (Suisse)": smsCH || '',
    "Appels en Suisse ( Heure )": appelsCH || '',
    "Données en itinérance (Go)": datasRoam,
    "Minutes roaming ( Heure )": minRoam,
    "Pays voisins inclus": pays || '',
    "Type offre": typeOffre,
    "Expiration": /toujours|permanent/i.test(bodyText) ? 'Remise permanente' : '',
    "~Émission de CO2 (kg/an)": '', // rempli juste après via calculateCO2
    "Durée d'engagement": /sans engagement/i.test(bodyText) ? 'Sans engagement' : ''
  };

  const co2 = calculateCO2(row);
  if (co2 !== null) row["~Émission de CO2 (kg/an)"] = co2;

  return row;
}

// ---------- Concurrency helper ----------
async function pMap(list, mapper, limit = 4) {
  const ret = [];
  let i = 0;
  const workers = new Array(limit).fill(0).map(async () => {
    while (i < list.length) {
      const idx = i++;
      ret[idx] = await mapper(list[idx], idx).catch(e => ({ __error: e }));
    }
  });
  await Promise.all(workers);
  return ret;
}

// ---------- Main ----------
async function main() {
  const started = Date.now();
  log('Lecture des sitemaps…');
  const urls = await gatherCandidateURLs();

  // Browser
  const browser = await chromium.launch({ headless: HEADLESS, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  const context = await browser.newContext({
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36',
    viewport: { width: 1366, height: 900 },
    javaScriptEnabled: true
  });

  // Bloque ressources lourdes / analytics
  await context.route('**/*', (route) => {
    const type = route.request().resourceType();
    const url = route.request().url();
    const block = ['image','media','font','stylesheet'].includes(type) ||
                  /google-analytics|googletagmanager|doubleclick|hotjar|optimizely/i.test(url);
    if (block) return route.abort();
    route.continue();
  });

  const page = await context.newPage();
  page.setDefaultTimeout(SEL_TIMEOUT);
  page.setDefaultNavigationTimeout(NAV_TIMEOUT);

  const results = [];
  let done = 0, ok = 0, fail = 0;

  await pMap(urls, async (u, idx) => {
    if (Date.now() > started + (FAST_MODE ? 12*60*1000 : 20*60*1000)) return; // garde-fou global
    let lastErr = null;
    for (let r = 0; r <= RETRIES; r++) {
      try {
        const cap = Promise.race([
          extractOfferFromPage(page, u),
          (async ()=>{ await sleep(PAGE_BUDGET); throw new Error('page budget exceeded'); })(),
        ]);
        const row = await cap;
        if (row && !row.__error) {
          results.push(row);
          ok++;
          break;
        }
      } catch (e) {
        lastErr = e;
        await sleep(300 + r*400);
      }
    }
    if (lastErr) {
      fail++;
      if (DEBUG_SNAPSHOTS) {
        const slug = `fail_${idx}`;
        await fs.writeFile(path.join(DATA_DIR, `${slug}.html`), await page.content().catch(()=>'')).
          catch(()=>{});
        await page.screenshot({ path: path.join(DATA_DIR, `${slug}.png`), fullPage: true }).catch(()=>{});
      }
      log('Skip:', u, '-', lastErr.message);
    }
    done++;
    if (done % 5 === 0) log(`Progress: ${done}/${urls.length} (OK ${ok} / KO ${fail})`);
  }, CONCURRENCY);

  await browser.close();

  // Dédup (par Référence de l'offre)
  const map = new Map();
  for (const r of results) {
    map.set(r["Référence de l'offre"], r);
  }
  const deduped = [...map.values()];

  // Écriture CSV
  const out = [HEADERS.join(','), ...deduped.map(toCSVRow)].join('\n');
  const outPath = path.join(DATA_DIR, 'latest.csv');
  await fs.writeFile(outPath, out, 'utf8');
  log(`✅ ${deduped.length} offres → ${outPath}`);

  // Push Airtable (facultatif)
  await pushAirtable(deduped).catch(e => {
    log('ℹ️ Airtable: ' + e.message);
  });
}

// ---------- Airtable push (facultatif) ----------
async function pushAirtable(rows) {
  const token = process.env.AIRTABLE_TOKEN;
  const base = process.env.AIRTABLE_BASE;
  const table = process.env.AIRTABLE_TABLE;

  if (!token || !base || !table) {
    throw new Error('Secrets Airtable absents → skip push (CSV uniquement)');
  }

  // upsert par "Référence de l'offre"
  const chunk = 10;
  for (let i = 0; i < rows.length; i += chunk) {
    const part = rows.slice(i, i + chunk).map(r => ({
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

    const res = await fetch(`https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}`, {
      method: 'PATCH',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        performUpsert: { fieldsToMergeOn: ["Référence de l'offre"] },
        records: part
      })
    });
    if (!res.ok) {
      const txt = await res.text().catch(()=> '');
      throw new Error(`Airtable error ${res.status}: ${txt}`);
    }
  }
  log(`✅ Airtable mis à jour (table: ${process.env.AIRTABLE_TABLE}).`);
}

// ---------- Run ----------
main().catch(e => {
  console.error('❌ Fatal:', e);
  process.exitCode = 1;
});

