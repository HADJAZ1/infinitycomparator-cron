// scrape.mjs — tout-en-un (GitHub Actions lance Node 20)
import fs from "fs/promises";
import { chromium } from "playwright";

const OPERATORS = [
  {
    name: "Yallo",
    sitemap: "https://www.yallo.ch/sitemap.xml",
    urlFilter: (u) =>
      /\/fr\//.test(u) && (u.includes("/mobile-products") || u.includes("/home-5g") || u.includes("/home-cable")),
    typeFromUrl: (u, hasTV) => (u.includes("/home") ? (hasTV ? "3 Home Cable Box + TV" : "2 Home Cable Box") : "1 SIM ( Mobile )")
  },
  {
    name: "Sunrise",
    sitemap: "https://www.sunrise.ch/sitemap.xml",
    urlFilter: (u) =>
      /\/fr\//.test(u) && (u.includes("/mobile") || u.includes("/internet") || u.includes("/home")),
    typeFromUrl: (u, hasTV) => (/home|internet/.test(u) ? (hasTV ? "3 Home Cable Box + TV" : "2 Home Cable Box") : "1 SIM ( Mobile )")
  },
  {
    name: "Salt",
    sitemap: "https://www.salt.ch/sitemap.xml",
    urlFilter: (u) =>
      /\/fr\//.test(u) && (u.includes("/mobile") || u.includes("/internet") || u.includes("/fiber")),
    typeFromUrl: (u, hasTV) => (/internet|fiber/.test(u) ? (hasTV ? "3 Home Cable Box + TV" : "2 Home Cable Box") : "1 SIM ( Mobile )")
  },
  {
    name: "Swisscom",
    sitemap: "https://www.swisscom.ch/sitemap.xml",
    urlFilter: (u) =>
      /\/fr\//.test(u) && (u.includes("/mobile") || u.includes("/internet") || u.includes("/tv")),
    typeFromUrl: (u, hasTV) => (/internet|tv/.test(u) ? (hasTV ? "3 Home Cable Box + TV" : "2 Home Cable Box") : "1 SIM ( Mobile )")
  }
];

const CSV_HEADERS = [
  "Référence de l'offre","Opérateur","Nom de l'offre","Prix CHF/mois","Prix initial CHF",
  "Rabais (%)","TV","Rapidité réseau Mbps","SMS & MMS (Suisse)","Appels en Suisse ( Heure )",
  "Données en itinérance (Go)","Minutes roaming ( Heure )","Pays voisins inclus",
  "Type offre","Expiration","~Émission de CO2 (kg/an)","Durée d'engagement"
];

const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function writeCSV(rows, outPath) {
  await fs.mkdir("data", { recursive: true });
  const lines = [CSV_HEADERS.map(esc).join(",")];
  for (const r of rows) lines.push(CSV_HEADERS.map(h => esc(r[h])).join(","));
  await fs.writeFile(outPath, lines.join("\n"), "utf8");
}

function toMbps(numStr, unitGuess) {
  if (!numStr) return "";
  const n = parseFloat(String(numStr).replace(",", "."));
  if (!isFinite(n)) return "";
  return /g/i.test(unitGuess) ? String(Math.round(n * 1000)) : String(Math.round(n));
}

function normRow({ ref, operator, title, price, discount, hasTV, speedVal, speedUnit, smsCH, appelsCH, roamData, roamMin, countries, type }) {
  return {
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
    "~Émission de CO2 (kg/an)": "",
    "Durée d'engagement": "Sans engagement"
  };
}

async function fetchSitemapUrls(sitemap, filter) {
  const res = await fetch(sitemap, { headers: { "User-Agent": "HelixCompareBot/1.0 (+noncommercial)" } });
  const xml = await res.text();
  const urls = [...xml.matchAll(/<loc>(.*?)<\/loc>/g)].map(m => m[1]);
  return urls.filter(filter);
}

async function extractFromPage(page, url) {
  await page.goto(url, { waitUntil: "networkidle", timeout: 40000 });
  return await page.evaluate(() => {
    const getText = (sel) => document.querySelector(sel)?.textContent?.trim() || "";
    const title = getText("h1, h2[data-test='title'], [data-test='product-title']") || document.title.replace(/\|.*$/, "").trim();
    const body = document.body.innerText.replace(/\s+/g, " ").trim();

    const price = (body.match(/CHF\s*([0-9]+(?:[.,][0-9]{1,2})?)/i)?.[1] ||
                   body.match(/([0-9]+(?:[.,][0-9]{1,2})?)\s*[.–-]\s*par\s*mois/i)?.[1] || "")
                  .toString().replace(",", ".");
    const discount = (body.match(/(\d{1,2})\s*%/i)?.[1] || "").toString();
    const hasTV = /\bTV\b|280\s*Cha(?:î|i)nes|replay/i.test(body);

    const speedMatch = body.match(/(\d+(?:[.,]\d+)?)\s*(Gbit\/?s|Gbps|Gbit|Mbit\/?s|Mbps|Mbit)/i);
    const speedVal = speedMatch?.[1] || "";
    const speedUnit = speedMatch?.[2] || "";

    const appelsCH = /Appels?\s+Illimit/i.test(body) ? "Illimité" : (body.match(/Appels?.*?(\d+\s*h|\d+\s*min)/i)?.[1] || "");
    const smsCH = /SMS\s+Illimit/i.test(body) ? "Illimité" : (body.match(/SMS.*?([0-9].*?)(?:\s|$)/i)?.[1] || "");

    const roamData = /itin[ée]rance|roaming/i.test(body)
      ? (/illimit/i.test(body) ? "Illimité" : (body.match(/(\d+(?:[.,]\d+)?)\s*(?:Go|GB)\s*(?:en\s+itin[ée]rance|roaming)?/i)?.[1] || "0"))
      : "";
    const roamMin = body.match(/(\d+)\s*(?:min|minutes)\s*(?:international|roaming)?/i)?.[1] || "";

    const countries = (body.match(/Europe.*?USA.*?Canada.*?Turquie/i)?.[0] ||
                       body.match(/FR,\s*DE,\s*IT,\s*AT,\s*LI/i)?.[0] || "") || "Aucun";

    return { title, price, discount, hasTV, speedVal, speedUnit, smsCH, appelsCH, roamData, roamMin, countries };
  });
}

async function run() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ userAgent: "HelixCompareBot/1.0 (+noncommercial)", locale: "fr-CH" });
  const page = await ctx.newPage();

  const allRows = [];
  for (const op of OPERATORS) {
    console.log("🔎", op.name, "— sitemap");
    const urls = await fetchSitemapUrls(op.sitemap, op.urlFilter);
    console.log(" →", urls.length, "pages");
    for (const u of urls) {
      try {
        const d = await extractFromPage(page, u);
        const ref = u.split("/").filter(Boolean).pop()?.toUpperCase().replace(/[^A-Z0-9]/g, "_") || "";
        const row = normRow({
          ref,
          operator: op.name,
          title: d.title,
          price: d.price,
          discount: d.discount,
          hasTV: d.hasTV,
          speedVal: d.speedVal,
          speedUnit: d.speedUnit,
          smsCH: d.smsCH,
          appelsCH: d.appelsCH,
          roamData: d.roamData,
          roamMin: d.roamMin,
          countries: d.countries,
          type: op.typeFromUrl(u, d.hasTV)
        });
        allRows.push(row);
        await sleep(400); // cadence douce
      } catch (e) {
        console.warn("⚠️", op.name, ":", u, e.message);
      }
    }
  }

  await browser.close();

  await writeCSV(allRows, "data/latest.csv");
  console.log(`✅ ${allRows.length} offres -> data/latest.csv`);

  // Optionnel : push Airtable si secrets présents
  const { AIRTABLE_TOKEN, AIRTABLE_BASE, AIRTABLE_TABLE } = process.env;
  if (AIRTABLE_TOKEN && AIRTABLE_BASE && AIRTABLE_TABLE) {
    console.log("☁️  Push vers Airtable…");
    const chunk = (arr, n=10) => arr.length ? [arr.slice(0, n), ...chunk(arr.slice(n), n)] : [];
    const records = allRows.map(r => ({
      fields: {
        reference: r["Référence de l'offre"],
        operateur: r["Opérateur"],
        nom: r["Nom de l'offre"],
        prix_chf_mois: r["Prix CHF/mois"],
        prix_initial_chf: r["Prix initial CHF"],
        rabais_pct: r["Rabais (%)"],
        tv: r["TV"],
        vitesse_mbps: r["Rapidité réseau Mbps"],
        sms_ch: r["SMS & MMS (Suisse)"],
        appels_ch: r["Appels en Suisse ( Heure )"],
        data_roaming_go: r["Données en itinérance (Go)"],
        minutes_roaming: r["Minutes roaming ( Heure )"],
        pays_inclus: r["Pays voisins inclus"],
        type_offre: r["Type offre"],
        expiration: r["Expiration"]
      }
    }));

    for (const part of chunk(records, 10)) {
      const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE}/${encodeURIComponent(AIRTABLE_TABLE)}`, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${AIRTABLE_TOKEN}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ records: part, typecast: true })
      });
      if (!res.ok) {
        console.error("Airtable error:", await res.text());
        throw new Error("Airtable push failed");
      }
      await sleep(300);
    }
    console.log("✅ Airtable mis à jour.");
  }
}

await run().catch(e => { console.error(e); process.exit(1); });
