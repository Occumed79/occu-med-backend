const express = require('express');
const cors = require('cors');
const https = require('https');
const cheerio = require('cheerio');

require('dotenv').config({ path: './Env' });

const app = express();
const PORT = process.env.PORT || 3001;
const SAM_KEY = process.env.SAM_API_KEY || '';

app.use(cors({ origin: '*' }));
app.use(express.json());

// ── HTTPS helpers ─────────────────────────────────────────────────────────────
function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: 20000 }, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(body) }); }
        catch (e) { reject(new Error(`JSON parse failed (${res.statusCode}): ${body.slice(0,200)}`)); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function httpsGetH(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = https.request({
      hostname: u.hostname, path: u.pathname + u.search, method: 'GET',
      headers: { 'Accept': 'application/json', ...headers },
      timeout: 20000
    }, (res) => {
      let raw = ''; res.on('data', d => raw += d);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(raw) }); }
        catch(e) { reject(new Error(`JSON parse failed (${res.statusCode}): ${raw.slice(0,200)}`)); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

function httpsPost(url, bodyStr) {
  return new Promise((resolve, reject) => {
    const buf = Buffer.from(bodyStr);
    const u = new URL(url);
    const req = https.request({
      hostname: u.hostname, path: u.pathname + u.search, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': buf.length },
      timeout: 20000
    }, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(raw) }); }
        catch (e) { reject(new Error(`JSON parse failed (${res.statusCode}): ${raw.slice(0,200)}`)); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(buf); req.end();
  });
}

// ── Date helpers ──────────────────────────────────────────────────────────────
function dateRange(daysBack) {
  const today = new Date();
  const start = new Date(today);
  start.setDate(start.getDate() - daysBack);
  const fmt = d => d.toISOString().split('T')[0];
  return { start: fmt(start), end: fmt(today) };
}

function notExpired(r) {
  const ed = r['End Date'] ? new Date(r['End Date']) : null;
  return !ed || ed >= new Date();
}

function notTooOld(r, daysBack) {
  const sd = r['Start Date'] ? new Date(r['Start Date']) : null;
  if (!sd) return true;
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - daysBack);
  return sd >= cutoff;
}

// ── USASpending generic fetcher ───────────────────────────────────────────────
const OCC_KEYWORDS_BATCHES = [
  ['occupational medicine', 'occupational health', 'pre-employment', 'drug testing', 'drug test'],
  ['medical surveillance', 'fit for duty', 'fitness for duty', 'employee health', 'health screening'],
  ['audiometric', 'medical review officer', 'MRO services', 'OSHA compliance', 'workers compensation'],
  ['industrial hygiene', 'physical examination', 'contractor medical', 'deployment medical', 'force health'],
];

async function usaSpendingSearch({ awardTypeCodes, source, noticeType, baseType, daysBack = 90, label = '', extraKeywords = [] }) {
  const { start, end } = dateRange(daysBack);
  const seen = new Set();
  const results = [];

  // Append extra custom keywords as an additional batch if provided
  const allBatches = [...OCC_KEYWORDS_BATCHES];
  if (extraKeywords.length > 0) {
    // Split into batches of 5
    for (let i = 0; i < extraKeywords.length; i += 5) {
      allBatches.push(extraKeywords.slice(i, i + 5));
    }
  }

  for (const batch of allBatches) {
    const body = JSON.stringify({
      filters: {
        time_period: [{ start_date: start, end_date: end }],
        keywords: batch,
        award_type_codes: awardTypeCodes,
      },
      fields: [
        'Award ID', 'Recipient Name', 'Award Amount', 'Awarding Agency',
        'Awarding Sub Agency', 'NAICS Code', 'NAICS Description',
        'Description', 'Start Date', 'End Date',
        'Place of Performance State Code', 'Place of Performance City Name'
      ],
      sort: 'Start Date', order: 'desc', limit: 50, page: 1, subawards: false
    });

    try {
      const { status, data } = await httpsPost(
        'https://api.usaspending.gov/api/v2/search/spending_by_award/', body
      );
      if (status !== 200) {
        console.log(`  ${label} batch error: HTTP ${status}`, JSON.stringify(data).slice(0,150));
        continue;
      }
      for (const r of data.results || []) {
        const awardId = r['Award ID'] || String(Math.random());
        const id = `${source}-${awardId}`;
        if (seen.has(id)) continue;
        if (!notExpired(r)) continue;
        if (!notTooOld(r, daysBack)) continue;
        seen.add(id);
        const recip = r['Recipient Name'] || '';
        const amt = r['Award Amount'];
        results.push({
          id, source,
          title: (r['Description'] || `${noticeType} — ${r['NAICS Description'] || ''}`).substring(0, 120),
          agency: r['Awarding Agency'] || 'Unknown Agency',
          subAgency: r['Awarding Sub Agency'] || '', office: '',
          solNum: awardId, noticeId: awardId,
          noticeType,
          naicsCode: r['NAICS Code'] || '', naicsDesc: r['NAICS Description'] || '',
          setAside: '', setAsideCode: '',
          postedDate: r['Start Date'] || null,
          deadline: r['End Date'] || null,
          archiveDate: null, active: false,
          state: r['Place of Performance State Code'] || '',
          city: r['Place of Performance City Name'] || '',
          desc: `${recip ? 'Recipient: ' + recip + '. ' : ''}Value: ${amt ? '$' + Number(amt).toLocaleString() : 'N/A'}. Period: ${r['Start Date'] || '?'} – ${r['End Date'] || '?'}.`,
          uiLink: `https://www.usaspending.gov/award/${encodeURIComponent(awardId)}`,
          contact: '', awardAmount: amt || 0, recipient: recip,
          classCode: '', baseType,
        });
      }
    } catch (e) { console.error(`  ${label} batch error:`, e.message); }
  }

  console.log(`${label} total: ${results.length}`);
  return results;
}

// ── Source 1: SAM.gov (federal solicitations) ─────────────────────────────────
async function fetchSAM() {
  if (!SAM_KEY) throw new Error('SAM API key not set');
  const seen = new Set();
  const results = [];
  for (const naics of ['621111', '812990', '621999']) {
    const today = new Date();
    const from = new Date(); from.setDate(from.getDate() - 90);
    const fmt = d => `${(d.getMonth()+1).toString().padStart(2,'0')}/${d.getDate().toString().padStart(2,'0')}/${d.getFullYear()}`;
    const url = `https://api.sam.gov/opportunities/v2/search?api_key=${SAM_KEY}&naicsCode=${naics}&limit=100&offset=0&active=Yes&postedFrom=${fmt(from)}&postedTo=${fmt(today)}`;
    console.log(`\nFetching SAM NAICS ${naics}...`);
    try {
      const { status, data } = await httpsGet(url);
      if (status === 429 || data.code === '900804') { console.log(`  SAM rate limited — resets midnight UTC`); continue; }
      if (status === 400) { console.log(`  SAM 400 Bad Request — full response:`, JSON.stringify(data)); continue; }
      if (data.error) { console.log(`  SAM error:`, data.message); continue; }
      const opps = data.opportunitiesData || [];
      console.log(`  SAM NAICS ${naics}: ${opps.length} results`);
      for (const o of opps) {
        const rawId = o.noticeId || o.solicitationNumber || String(Math.random());
        const id = 'SAM-' + rawId;
        if (seen.has(id)) continue;
        seen.add(id);
        results.push({
          id, source: 'SAM',
          title: o.title || 'Untitled',
          agency: o.fullParentPathName || o.department || 'Unknown Agency',
          subAgency: o.subtierName || '', office: o.officeName || '',
          solNum: o.solicitationNumber || '', noticeId: rawId,
          noticeType: o.type || o.baseType || '',
          naicsCode: naics, naicsDesc: '',
          setAside: o.typeOfSetAsideDescription || '',
          setAsideCode: o.typeOfSetAside || '',
          postedDate: o.postedDate || null,
          deadline: o.responseDeadLine || o.archiveDate || null,
          archiveDate: o.archiveDate || null,
          active: o.active === 'Yes',
          state: o.placeOfPerformance?.state?.name || '',
          city: o.placeOfPerformance?.city?.name || '',
          desc: o.description || '',
          uiLink: o.uiLink || `https://sam.gov/opp/${rawId}/view`,
          contact: o.pointOfContact?.[0]?.email || '',
          awardAmount: 0, recipient: '', classCode: o.classificationCode || '', baseType: o.baseType || '',
        });
      }
    } catch (e) { console.error(`  SAM NAICS ${naics} error:`, e.message); }
  }
  console.log(`SAM total: ${results.length}`);
  return results;
}

// ── Source 2: USASpending — Contract Awards (definitive contracts) ─────────────
async function fetchUSASpending(daysBack = 90, extraKeywords = []) {
  console.log('\nFetching USASpending contract awards...');
  return usaSpendingSearch({
    awardTypeCodes: ['A', 'B', 'C', 'D'],
    source: 'USA', noticeType: 'Contract Award', baseType: 'Award',
    daysBack, label: 'USASpending', extraKeywords
  });
}

// ── Source 3: IDV / IDIQ contracts ────────────────────────────────────────────
// Indefinite Delivery Vehicles — umbrella contracts where occ med task orders get issued.
// This is where most ongoing occ med federal work actually lives.
async function fetchIDV(daysBack = 180, extraKeywords = []) {
  console.log('\nFetching IDV/IDIQ contracts...');
  return usaSpendingSearch({
    awardTypeCodes: ['IDV_A', 'IDV_B', 'IDV_B_A', 'IDV_B_B', 'IDV_B_C', 'IDV_C', 'IDV_D', 'IDV_E'],
    source: 'IDV', noticeType: 'IDIQ / Indefinite Delivery Vehicle', baseType: 'IDV',
    daysBack, label: 'IDV', extraKeywords
  });
}

// ── Source 4: Federal Subcontracts ────────────────────────────────────────────
// Prime contractors subbing out occ med work — Concentra, Leidos, SAIC, etc.
// Shows who the real delivery chain is and where Occu-Med could fit as a sub.
async function fetchSubawards(daysBack = 90, extraKeywords = []) {
  console.log('\nFetching federal subawards...');
  const { start, end } = dateRange(daysBack);
  const seen = new Set();
  const results = [];

  const subBatches = [...OCC_KEYWORDS_BATCHES.slice(0, 2)];
  if (extraKeywords && extraKeywords.length > 0) subBatches.push(extraKeywords.slice(0, 5));
  for (const batch of subBatches) {
    const body = JSON.stringify({
      filters: {
        time_period: [{ start_date: start, end_date: end }],
        keywords: batch,
        award_type_codes: ['A', 'B', 'C', 'D'],
      },
      fields: [
        'Sub-Award ID', 'Sub-Awardee Name', 'Sub-Award Amount',
        'Awarding Agency', 'Sub-Award Description',
        'Place of Performance State Code'
      ],
      sort: 'Award ID', order: 'desc', limit: 50, page: 1
    });

    try {
      const { status, data } = await httpsPost(
        'https://api.usaspending.gov/api/v2/search/spending_by_award/subawards/', body
      );
      if (status !== 200) { console.log(`  Subawards HTTP ${status}:`, JSON.stringify(data).slice(0,150)); continue; }
      for (const r of data.results || []) {
        const id = 'SUB-' + (r['Sub-Award ID'] || Math.random());
        if (seen.has(id)) continue;
        seen.add(id);
        const sub = r['Sub-Awardee Name'] || '';
        const amt = r['Sub-Award Amount'];
        results.push({
          id, source: 'SUB',
          title: (r['Sub-Award Description'] || 'Federal Subcontract').substring(0, 120),
          agency: r['Awarding Agency'] || 'Unknown Agency',
          subAgency: '', office: '',
          solNum: r['Sub-Award ID'] || '', noticeId: r['Sub-Award ID'] || '',
          noticeType: 'Federal Subcontract',
          naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '',
          postedDate: r['Sub-Award Date'] || null,
          deadline: null, archiveDate: null, active: false,
          state: r['Place of Performance State Code'] || '', city: '',
          desc: `Subcontract to ${sub || 'N/A'}. Value: ${amt ? '$' + Number(amt).toLocaleString() : 'N/A'}.`,
          uiLink: 'https://www.usaspending.gov/search',
          contact: '', awardAmount: amt || 0, recipient: sub,
          classCode: '', baseType: 'Subaward',
        });
      }
    } catch (e) { console.error('Subawards error:', e.message); }
  }
  console.log(`Subawards total: ${results.length}`);
  return results;
}

// ── Source 5: Federal Grants & Assistance ─────────────────────────────────────
async function fetchGrants(daysBack = 90, extraKeywords = []) {
  console.log('\nFetching federal grants/assistance...');
  return usaSpendingSearch({
    awardTypeCodes: ['02', '03', '04', '05'],
    source: 'GRANTS', noticeType: 'Federal Grant / Assistance', baseType: 'Grant',
    daysBack, label: 'Grants', extraKeywords
  });
}

// ── Source 6: SBIR (best-effort — API often rate-limited) ─────────────────────
async function fetchSBIR() {
  const results = [];
  const seen = new Set();
  for (const kw of ['occupational health', 'occupational medicine', 'drug testing', 'medical surveillance', 'force health']) {
    try {
      const { status, data } = await httpsGet(
        `https://api.www.sbir.gov/public/api/awards?keyword=${encodeURIComponent(kw)}&rows=25`
      );
      if (status !== 200) { console.log(`  SBIR "${kw}": HTTP ${status} — skipping`); continue; }
      for (const a of (Array.isArray(data) ? data : [])) {
        const id = 'SBIR-' + (a.contract || String(Math.random()));
        if (seen.has(id)) continue;
        seen.add(id);
        results.push({
          id, source: 'SBIR',
          title: a.award_title || 'SBIR Award',
          agency: a.agency || 'Unknown', subAgency: a.branch || '', office: '',
          solNum: a.solicitation_number || '', noticeId: a.contract || '',
          noticeType: 'SBIR Award',
          naicsCode: '621111', naicsDesc: 'Occupational Medicine',
          setAside: 'Small Business', setAsideCode: 'SBA',
          postedDate: a.proposal_award_date || null,
          deadline: a.contract_end_date || null,
          archiveDate: null, active: false,
          state: a.state || '', city: a.city || '',
          desc: a.abstract || '',
          uiLink: a.award_link || 'https://www.sbir.gov/awards',
          contact: a.poc_email || '', awardAmount: parseFloat(a.award_amount) || 0,
          recipient: a.firm || '', classCode: '', baseType: 'SBIR Award',
        });
      }
    } catch (e) { console.error(`  SBIR error:`, e.message); }
  }
  console.log(`SBIR total: ${results.length}`);
  return results;
}


// ── Tango by MakeGov ─────────────────────────────────────────────────────────
// Unified federal procurement API — normalizes SAM, FPDS, USASpending + forecasts
// Free tier: 100 req/day. Get key at tango.makegov.com
// Set env var TANGO_API_KEY in your Render environment variables

const TANGO_KEY = process.env.TANGO_API_KEY || '';
const OCC_TERMS = [
  'occupational health', 'occupational medicine', 'drug testing',
  'pre-employment physical', 'medical surveillance', 'fit for duty',
  'fitness for duty', 'employee health screening', 'audiometric',
  'industrial hygiene', 'deployment medical', 'force health protection'
];

async function fetchTango() {
  if (!TANGO_KEY) { console.log('Tango: no TANGO_API_KEY set'); return []; }
  const seen = new Set();
  const results = [];
  const headers = { 'X-API-KEY': TANGO_KEY };

  for (const term of OCC_TERMS.slice(0, 4)) {
    // Opportunities
    try {
      const url = `https://tango.makegov.com/api/opportunities/?search=${encodeURIComponent(term)}&limit=20&ordering=-response_deadline`;
      const { status, data } = await httpsGetH(url, headers);
      const opps = data.results || [];
      console.log(`  Tango opps "${term}": HTTP ${status}, ${opps.length} results`);
      for (const o of opps) {
        const id = 'TANGO-' + (o.id || Math.random());
        if (seen.has(id)) continue; seen.add(id);
        results.push({
          id, source: 'TANGO',
          title: o.title || o.subject || 'Tango Opportunity',
          agency: o.agency_name || o.department || 'Unknown Agency',
          subAgency: '', office: '',
          solNum: o.solicitation_number || o.sam_id || '',
          noticeId: String(o.id || ''),
          noticeType: o.notice_type || 'Solicitation',
          naicsCode: o.naics_code || '621111', naicsDesc: o.naics_description || '',
          setAside: o.set_aside || '', setAsideCode: '',
          postedDate: o.posted_date || null,
          deadline: o.response_deadline || o.close_date || null,
          archiveDate: null, active: true,
          state: o.place_of_performance?.state?.code || o.place_of_performance?.state || '',
          city: o.place_of_performance?.city?.name || '',
          desc: o.description || '',
          uiLink: o.sam_url || `https://sam.gov/opp/${o.sam_id}/view`,
          contact: '', awardAmount: 0, recipient: '',
          classCode: '', baseType: 'Solicitation',
        });
      }
    } catch(e) { console.error(`  Tango opps error "${term}":`, e.message); }

    // Procurement Forecasts
    try {
      const url2 = `https://tango.makegov.com/api/forecasts/?search=${encodeURIComponent(term)}&limit=20`;
      const { status: s2, data: d2 } = await httpsGetH(url2, headers);
      const forecasts = d2.results || [];
      console.log(`  Tango forecasts "${term}": HTTP ${s2}, ${forecasts.length} results`);
      for (const f of forecasts) {
        const id = 'TANGO-FC-' + (f.id || Math.random());
        if (seen.has(id)) continue; seen.add(id);
        results.push({
          id, source: 'TANGO',
          title: '[FORECAST] ' + (f.title || f.description || 'Procurement Forecast'),
          agency: f.agency_name || f.department || 'Unknown Agency',
          subAgency: '', office: '',
          solNum: f.requirement_id || '', noticeId: String(f.id || ''),
          noticeType: 'Procurement Forecast',
          naicsCode: f.naics_code || '621111', naicsDesc: '',
          setAside: f.set_aside || '', setAsideCode: '',
          postedDate: f.fiscal_year ? `${f.fiscal_year}-01-01` : null,
          deadline: f.anticipated_award_date || f.estimated_solicitation_date || null,
          archiveDate: null, active: true,
          state: '', city: '',
          desc: f.description || f.scope || '',
          uiLink: f.url || 'https://tango.makegov.com',
          contact: '', awardAmount: f.estimated_value || 0, recipient: '',
          classCode: '', baseType: 'Forecast',
        });
      }
    } catch(e) { console.error(`  Tango forecasts error "${term}":`, e.message); }
  }

  console.log(`Tango total: ${results.length}`);
  return results;
}

// ── Federal Register API ──────────────────────────────────────────────────────
// Free, no key. New OSHA rules, DoD mandates, HHS requirements.
// An OSHA rule on audiometric testing = every employer needs a contractor.
// This is the earliest possible BD signal — months before a procurement posts.

async function fetchFederalRegister() {
  const terms = ['occupational health', 'drug testing', 'medical surveillance', 'fit for duty', 'force health'];
  const agencies = ['occupational-safety-and-health-administration', 'defense-department', 'health-and-human-services-department'];
  const results = [];
  const seen = new Set();
  const today = new Date();
  const from = new Date(); from.setDate(from.getDate() - 90);
  const fmt = d => d.toISOString().split('T')[0];

  for (const term of terms) {
    const url = `https://www.federalregister.gov/api/v1/documents.json?per_page=20&order=newest&conditions%5Bterm%5D=${encodeURIComponent(term)}&conditions%5Bpublication_date%5D%5Bgte%5D=${fmt(from)}&conditions%5Btype%5D%5B%5D=RULE&conditions%5Btype%5D%5B%5D=PRORULE&conditions%5Btype%5D%5B%5D=NOTICE`;
    console.log(`\nFetching Federal Register: "${term}"...`);
    try {
      const { status, data } = await httpsGet(url);
      if (status !== 200) { console.log(`  FedReg "${term}": HTTP ${status}`); continue; }
      const docs = data.results || [];
      console.log(`  FedReg "${term}": ${docs.length} results`);
      for (const d of docs) {
        const id = 'FEDREG-' + (d.document_number || Math.random());
        if (seen.has(id)) continue; seen.add(id);
        results.push({
          id, source: 'FEDREG',
          title: '[REG ALERT] ' + (d.title || 'Federal Register Notice'),
          agency: d.agencies?.map(a => a.name).join(', ') || 'Federal Agency',
          subAgency: '', office: '',
          solNum: d.document_number || '', noticeId: d.document_number || '',
          noticeType: d.type === 'PRORULE' ? 'Proposed Rule' : d.type === 'RULE' ? 'Final Rule' : 'Federal Notice',
          naicsCode: '621111', naicsDesc: 'Occupational Medicine',
          setAside: '', setAsideCode: '',
          postedDate: d.publication_date || null,
          deadline: d.effective_on || d.comment_date || null,
          archiveDate: null, active: true,
          state: '', city: '',
          desc: d.abstract || d.excerpts || '',
          uiLink: d.html_url || d.pdf_url || 'https://www.federalregister.gov',
          contact: '', awardAmount: 0, recipient: '',
          classCode: '', baseType: 'Regulatory Notice',
        });
      }
    } catch(e) { console.error(`  FedReg error for "${term}":`, e.message); }
  }

  console.log(`Federal Register total: ${results.length}`);
  return results;
}

// ── State Scrapers ────────────────────────────────────────────────────────────
// Server-side fetch of public procurement pages — no login, no ToS violation.
// All these pages are publicly accessible and intended for vendor use.


// Generic scraper helper
async function scrapePage(url, label) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const mod = u.protocol === 'https:' ? https : require('http');
    const req = mod.request({
      hostname: u.hostname, path: u.pathname + u.search, method: 'GET',
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; OccuMed-RFP-Bot/1.0)', 'Accept': 'text/html,application/xhtml+xml' },
      timeout: 20000
    }, (res) => {
      let raw = '';
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        resolve(scrapePage(res.headers.location, label));
        return;
      }
      res.on('data', d => raw += d);
      res.on('end', () => { console.log(`  ${label}: HTTP ${res.statusCode}, ${raw.length} bytes`); resolve(raw); });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

// Texas ESBD — public search, no login needed
async function fetchTexasESBD(keywords) {
  const results = [];
  const seen = new Set();
  const terms = keywords.length > 0 ? keywords : ['occupational health', 'drug testing', 'medical', 'health services'];

  for (const term of terms.slice(0, 3)) {
    try {
      const url = `https://www.txsmartbuy.gov/esbd?keyword=${encodeURIComponent(term)}&status=Open`;
      const html = await scrapePage(url, `TX ESBD "${term}"`);
      const $ = cheerio.load(html);

      // Parse solicitation rows from ESBD table
      $('table tr').each((i, row) => {
        if (i === 0) return; // skip header
        const cells = $(row).find('td');
        if (cells.length < 3) return;
        const title = $(cells[0]).text().trim();
        const agency = $(cells[1]).text().trim();
        const deadline = $(cells[2]).text().trim();
        const link = $(cells[0]).find('a').attr('href') || '';
        if (!title || title.length < 5) return;

        const id = 'TX-' + Buffer.from(title).toString('base64').slice(0, 20);
        if (seen.has(id)) return; seen.add(id);

        results.push({
          id, source: 'STATE-TX',
          title, agency: agency || 'Texas State Agency',
          subAgency: '', office: '',
          solNum: '', noticeId: id,
          noticeType: 'State Solicitation',
          naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '',
          postedDate: null,
          deadline: deadline || null,
          archiveDate: null, active: true,
          state: 'TX', city: '',
          desc: '',
          uiLink: link.startsWith('http') ? link : `https://www.txsmartbuy.gov${link}`,
          contact: '', awardAmount: 0, recipient: '',
          classCode: '', baseType: 'State Bid',
        });
      });
      console.log(`  TX ESBD "${term}": ${results.length} parsed so far`);
    } catch(e) { console.error(`  TX ESBD error for "${term}":`, e.message); }
  }
  return results;
}

// Virginia eVA — public procurement portal
async function fetchVirginiaEVA(keywords) {
  const results = [];
  const seen = new Set();
  const terms = keywords.length > 0 ? keywords : ['occupational health', 'drug testing', 'medical services'];

  for (const term of terms.slice(0, 3)) {
    try {
      const url = `https://eva.virginia.gov/pages/eva-search-main-page.html?searchType=opps&q=${encodeURIComponent(term)}&statusCodes=Open`;
      const html = await scrapePage(url, `VA eVA "${term}"`);
      const $ = cheerio.load(html);

      $('.searchResultRow, .opportunity-row, tr.result-row').each((i, row) => {
        const title = $(row).find('.title, .opp-title, td:first-child').first().text().trim();
        const agency = $(row).find('.agency, .org-name, td:nth-child(2)').first().text().trim();
        const deadline = $(row).find('.deadline, .close-date, td:nth-child(3)').first().text().trim();
        const link = $(row).find('a').first().attr('href') || '';
        if (!title || title.length < 5) return;

        const id = 'VA-' + Buffer.from(title + agency).toString('base64').slice(0, 20);
        if (seen.has(id)) return; seen.add(id);

        results.push({
          id, source: 'STATE-VA',
          title, agency: agency || 'Virginia State Agency',
          subAgency: '', office: '', solNum: '', noticeId: id,
          noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '',
          postedDate: null, deadline: deadline || null,
          archiveDate: null, active: true,
          state: 'VA', city: '', desc: '',
          uiLink: link.startsWith('http') ? link : `https://eva.virginia.gov${link}`,
          contact: '', awardAmount: 0, recipient: '',
          classCode: '', baseType: 'State Bid',
        });
      });
    } catch(e) { console.error(`  VA eVA error for "${term}":`, e.message); }
  }
  console.log(`Virginia eVA total: ${results.length}`);
  return results;
}

// Louisiana LaPAC — completely public, clean HTML table
async function fetchLouisiana(keywords) {
  const results = [];
  const seen = new Set();
  const terms = keywords.length > 0 ? keywords : ['health', 'medical', 'drug'];

  for (const term of terms.slice(0, 3)) {
    try {
      const url = `https://wwwcfprd.doa.louisiana.gov/osp/lapac/bidList.cfm?search=${encodeURIComponent(term)}&status=Open`;
      const html = await scrapePage(url, `LA LaPAC "${term}"`);
      const $ = cheerio.load(html);

      $('table tr').each((i, row) => {
        if (i === 0) return;
        const cells = $(row).find('td');
        if (cells.length < 4) return;
        const title = $(cells[1]).text().trim();
        const agency = $(cells[2]).text().trim();
        const deadline = $(cells[3]).text().trim();
        const link = $(cells[1]).find('a').attr('href') || '';
        if (!title || title.length < 5) return;

        const id = 'LA-' + Buffer.from(title).toString('base64').slice(0, 20);
        if (seen.has(id)) return; seen.add(id);

        results.push({
          id, source: 'STATE-LA',
          title, agency: agency || 'Louisiana State Agency',
          subAgency: '', office: '', solNum: '', noticeId: id,
          noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '',
          postedDate: null, deadline: deadline || null,
          archiveDate: null, active: true,
          state: 'LA', city: '', desc: '',
          uiLink: link.startsWith('http') ? link : `https://wwwcfprd.doa.louisiana.gov${link}`,
          contact: '', awardAmount: 0, recipient: '',
          classCode: '', baseType: 'State Bid',
        });
      });
    } catch(e) { console.error(`  LA LaPAC error for "${term}":`, e.message); }
  }
  console.log(`Louisiana LaPAC total: ${results.length}`);
  return results;
}

// Colorado OSC — public solicitations page
async function fetchColorado(keywords) {
  const results = [];
  const seen = new Set();
  try {
    const url = 'https://osc.colorado.gov/spco/solicitations';
    const html = await scrapePage(url, 'CO OSC');
    const $ = cheerio.load(html);
    const terms = keywords.length > 0 ? keywords.map(k => k.toLowerCase()) : ['health', 'medical', 'drug'];

    $('table tr, .views-row, .solicitation-item').each((i, row) => {
      const title = $(row).find('td:first-child, .title, h3').first().text().trim();
      const agency = $(row).find('td:nth-child(2), .agency').first().text().trim();
      const deadline = $(row).find('td:nth-child(3), .deadline').first().text().trim();
      const link = $(row).find('a').first().attr('href') || '';
      if (!title || title.length < 5) return;

      // Apply relevance filter since we can't keyword-search the page
      const titleLower = title.toLowerCase();
      if (!terms.some(t => titleLower.includes(t))) return;

      const id = 'CO-' + Buffer.from(title).toString('base64').slice(0, 20);
      if (seen.has(id)) return; seen.add(id);

      results.push({
        id, source: 'STATE-CO',
        title, agency: agency || 'Colorado State Agency',
        subAgency: '', office: '', solNum: '', noticeId: id,
        noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
        setAside: '', setAsideCode: '',
        postedDate: null, deadline: deadline || null,
        archiveDate: null, active: true,
        state: 'CO', city: '', desc: '',
        uiLink: link.startsWith('http') ? link : `https://osc.colorado.gov${link}`,
        contact: '', awardAmount: 0, recipient: '',
        classCode: '', baseType: 'State Bid',
      });
    });
  } catch(e) { console.error('  Colorado OSC error:', e.message); }
  console.log(`Colorado OSC total: ${results.length}`);
  return results;
}

// Georgia DOAS — public procurement registry
async function fetchGeorgia(keywords) {
  const results = [];
  const seen = new Set();
  const terms = keywords.length > 0 ? keywords : ['health', 'medical', 'drug testing'];

  for (const term of terms.slice(0, 3)) {
    try {
      const url = `https://ssl.doas.state.ga.us/PRSapp/PR_Search_Results.jsp?searchText=${encodeURIComponent(term)}&status=Open&agencyID=0`;
      const html = await scrapePage(url, `GA DOAS "${term}"`);
      const $ = cheerio.load(html);

      $('table.results tr, tr.dataRow').each((i, row) => {
        const title = $(row).find('td:nth-child(2), .title').first().text().trim();
        const agency = $(row).find('td:nth-child(3), .agency').first().text().trim();
        const deadline = $(row).find('td:nth-child(5), .deadline').first().text().trim();
        const link = $(row).find('a').first().attr('href') || '';
        if (!title || title.length < 5) return;

        const id = 'GA-' + Buffer.from(title + agency).toString('base64').slice(0, 20);
        if (seen.has(id)) return; seen.add(id);

        results.push({
          id, source: 'STATE-GA',
          title, agency: agency || 'Georgia State Agency',
          subAgency: '', office: '', solNum: '', noticeId: id,
          noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '',
          postedDate: null, deadline: deadline || null,
          archiveDate: null, active: true,
          state: 'GA', city: '', desc: '',
          uiLink: link.startsWith('http') ? link : `https://ssl.doas.state.ga.us${link}`,
          contact: '', awardAmount: 0, recipient: '',
          classCode: '', baseType: 'State Bid',
        });
      });
    } catch(e) { console.error(`  GA DOAS error:`, e.message); }
  }
  console.log(`Georgia DOAS total: ${results.length}`);
  return results;
}

// Mississippi — clean public procurement search
async function fetchMississippi(keywords) {
  const results = [];
  const seen = new Set();
  const terms = keywords.length > 0 ? keywords : ['health', 'medical', 'drug'];

  for (const term of terms.slice(0, 3)) {
    try {
      const url = `https://www.ms.gov/dfa/contract_bid_search/Bid?searchText=${encodeURIComponent(term)}&status=Open&autoloadGrid=true`;
      const html = await scrapePage(url, `MS "${term}"`);
      const $ = cheerio.load(html);

      $('table tr, .grid-row').each((i, row) => {
        if (i === 0) return;
        const cells = $(row).find('td');
        if (cells.length < 3) return;
        const title = $(cells[1]).text().trim();
        const agency = $(cells[2]).text().trim();
        const deadline = $(cells[3]).text().trim();
        const link = $(cells[0]).find('a').attr('href') || '';
        if (!title || title.length < 5) return;

        const id = 'MS-' + Buffer.from(title).toString('base64').slice(0, 20);
        if (seen.has(id)) return; seen.add(id);

        results.push({
          id, source: 'STATE-MS',
          title, agency: agency || 'Mississippi State Agency',
          subAgency: '', office: '', solNum: '', noticeId: id,
          noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '',
          postedDate: null, deadline: deadline || null,
          archiveDate: null, active: true,
          state: 'MS', city: '', desc: '',
          uiLink: link.startsWith('http') ? link : `https://www.ms.gov${link}`,
          contact: '', awardAmount: 0, recipient: '',
          classCode: '', baseType: 'State Bid',
        });
      });
    } catch(e) { console.error(`  MS error:`, e.message); }
  }
  console.log(`Mississippi total: ${results.length}`);
  return results;
}

// Master state scraper — runs all states and combines
async function fetchStateBids(extraKeywords = []) {
  console.log('\nFetching state portals...');
  const [tx, va, la, co, ga, ms] = await Promise.allSettled([
    fetchTexasESBD(extraKeywords),
    fetchVirginiaEVA(extraKeywords),
    fetchLouisiana(extraKeywords),
    fetchColorado(extraKeywords),
    fetchGeorgia(extraKeywords),
    fetchMississippi(extraKeywords),
  ]);
  const get = r => r.status === 'fulfilled' ? r.value : [];
  const all = [...get(tx), ...get(va), ...get(la), ...get(co), ...get(ga), ...get(ms)];
  console.log(`State portals combined: ${all.length}`);
  return all;
}

// ── Routes ────────────────────────────────────────────────────────────────────
app.get('/api/status', (req, res) => res.json({
  status: 'ok', version: '5.0.0', samKeyLoaded: !!SAM_KEY, tangoKeyLoaded: !!TANGO_KEY,
  sources: ['SAM.gov', 'USASpending (Contracts)', 'USASpending (IDV/IDIQ)', 'USASpending (Subawards)', 'USASpending (Grants)', 'SBIR.gov'],
  endpoints: ['/api/sam', '/api/usaspending', '/api/idv', '/api/subawards', '/api/grants', '/api/sbir', '/api/opportunities']
}));

// Parse custom keywords from ?keywords=term1,term2,...
function parseKeywords(req) {
  const raw = req.query.keywords || '';
  return raw ? raw.split(',').map(k => k.trim()).filter(Boolean) : [];
}

app.get('/api/tango',      async (req, res) => { try { res.json({ success:true, data: await fetchTango() }); }                                               catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/fedreg',     async (req, res) => { try { res.json({ success:true, data: await fetchFederalRegister() }); }                                        catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/states',     async (req, res) => { try { res.json({ success:true, data: await fetchStateBids(parseKeywords(req)) }); }                            catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/sam',         async (req, res) => { try { res.json({ success:true, data: await fetchSAM() }); }                                                                            catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/usaspending', async (req, res) => { try { res.json({ success:true, data: await fetchUSASpending(parseInt(req.query.days)||90, parseKeywords(req)) }); }                   catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/idv',         async (req, res) => { try { res.json({ success:true, data: await fetchIDV(parseInt(req.query.days)||180, parseKeywords(req)) }); }                          catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/subawards',   async (req, res) => { try { res.json({ success:true, data: await fetchSubawards(parseInt(req.query.days)||90, parseKeywords(req)) }); }                     catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/grants',      async (req, res) => { try { res.json({ success:true, data: await fetchGrants(parseInt(req.query.days)||90, parseKeywords(req)) }); }                        catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/sbir',        async (req, res) => { try { res.json({ success:true, data: await fetchSBIR() }); }                                                                           catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });

app.get('/api/opportunities', async (req, res) => {
  const days = parseInt(req.query.days) || 90;
  const kw = parseKeywords(req);
  const [samR, usaR, idvR, subR, grantR, sbirR, tangoR, fedregR, statesR] = await Promise.allSettled([
    fetchSAM(), fetchUSASpending(days, kw), fetchIDV(days, kw), fetchSubawards(days, kw),
    fetchGrants(days, kw), fetchSBIR(), fetchTango(), fetchFederalRegister(), fetchStateBids(kw)
  ]);
  const get = r => r.status === 'fulfilled' ? r.value : [];
  const err = (lbl, r) => r.status === 'rejected' ? [`${lbl}: ${r.reason?.message}`] : [];
  const all = [...get(samR),...get(usaR),...get(idvR),...get(subR),...get(grantR),...get(sbirR),...get(tangoR),...get(fedregR),...get(statesR)];
  res.json({
    success: true, total: all.length,
    samCount: get(samR).length, usaCount: get(usaR).length,
    idvCount: get(idvR).length, subCount: get(subR).length,
    grantsCount: get(grantR).length, sbirCount: get(sbirR).length,
    tangoCount: get(tangoR).length, fedregCount: get(fedregR).length,
    statesCount: get(statesR).length,
    errors: [...err('SAM',samR),...err('USASpending',usaR),...err('IDV',idvR),...err('Subawards',subR),...err('Grants',grantR),...err('SBIR',sbirR),...err('Tango',tangoR),...err('FedReg',fedregR),...err('States',statesR)],
    data: all
  });
});

// ── Keep-alive ping (prevents Render free tier from sleeping) ────────────────
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || '';
if (RENDER_URL) {
  setInterval(() => {
    https.get(`${RENDER_URL}/api/status`, (res) => {
      console.log(`Keep-alive ping: ${res.statusCode}`);
    }).on('error', (e) => {
      console.log(`Keep-alive ping failed: ${e.message}`);
    });
  }, 10 * 60 * 1000); // every 10 minutes
}

app.listen(PORT, () => {
  console.log(`\nOccu-Med Backend v4.0 running on port ${PORT}`);
  console.log(`SAM API key: ${SAM_KEY ? SAM_KEY.slice(0,12)+'...' : 'NOT SET'}`);
  console.log(`Sources: SAM | Contracts | IDV | Subawards | Grants | SBIR | Tango | FedReg | TX | VA | LA | CO | GA | MS\n`);
});
