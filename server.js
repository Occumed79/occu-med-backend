const express = require('express');
const cors = require('cors');
const https = require('https');
const cheerio = require('cheerio');
const puppeteer = require('puppeteer-core');

require('dotenv').config({ path: './Env' });

const app = express();
const PORT = process.env.PORT || 3001;
const SAM_KEY = process.env.SAM_API_KEY || '';

// ── Upstash Redis Cache ───────────────────────────────────────────────────────
// Free tier: 500K commands/month. Cache results for 6 hours — eliminates
// rate limit hits, cold start wait, and Browserless credit burn on every refresh.
const UPSTASH_URL   = process.env.UPSTASH_REDIS_REST_URL || '';
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const CACHE_TTL     = 6 * 60 * 60; // 6 hours in seconds

async function cacheGet(key) {
  if (!UPSTASH_URL) return null;
  try {
    const res = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
    });
    const json = await res.json();
    if (!json.result) return null;
    return JSON.parse(json.result);
  } catch(e) { console.log(`  Cache GET error: ${e.message}`); return null; }
}

async function cacheSet(key, value, ttl = CACHE_TTL) {
  if (!UPSTASH_URL) return;
  try {
    await fetch(`${UPSTASH_URL}/set/${encodeURIComponent(key)}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ value: JSON.stringify(value), ex: ttl })
    });
  } catch(e) { console.log(`  Cache SET error: ${e.message}`); }
}

// Cached wrapper — checks cache first, fetches and stores if miss
async function withCache(key, fetchFn, ttl = CACHE_TTL) {
  const cached = await cacheGet(key);
  if (cached !== null) {
    console.log(`  [CACHE HIT] ${key} (${cached.length} items)`);
    return cached;
  }
  console.log(`  [CACHE MISS] ${key} — fetching live`);
  const data = await fetchFn();
  await cacheSet(key, data, ttl);
  return data;
}

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
    } catch (e) { console.error(`  ${label} batch error: ${e.message || e.code || JSON.stringify(e)}`); }
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
      },
      fields: [
        'Sub-Award ID', 'Sub-Award Type', 'Sub-Awardee Name', 'Sub-Award Date',
        'Sub-Award Amount', 'Awarding Agency', 'Sub-Award Description'
      ],
      sort: 'Sub-Award Amount', order: 'desc', limit: 50, page: 1
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

const TANGO_KEY       = process.env.TANGO_API_KEY       || '';
const BROWSERLESS_KEY = process.env.BROWSERLESS_API_KEY || '';
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
    // Federal Register API — must use literal brackets, not %5B%5D encoded
    const frBase = 'https://www.federalregister.gov/api/v1/documents.json';
    const frQuery = [
      'per_page=20', 'order=newest',
      'fields[]=title', 'fields[]=document_number', 'fields[]=publication_date',
      'fields[]=type', 'fields[]=abstract', 'fields[]=html_url',
      'fields[]=agencies', 'fields[]=effective_on', 'fields[]=comment_date',
      `conditions[term]=${encodeURIComponent(term)}`,
      `conditions[publication_date][gte]=${fmt(from)}`,
      'conditions[type][]=RULE', 'conditions[type][]=PRORULE', 'conditions[type][]=NOTICE'
    ].join('&');
    const url = `${frBase}?${frQuery}`;
    console.log(`\nFetching Federal Register: "${term}"...`);
    try {
      // Use https.request directly to preserve literal brackets in query string
      const { status, data } = await new Promise((resolve, reject) => {
        const req = https.request({
          hostname: 'www.federalregister.gov',
          path: '/api/v1/documents.json?' + frQuery,
          method: 'GET',
          headers: { 'Accept': 'application/json' },
          timeout: 20000
        }, (res) => {
          let raw = ''; res.on('data', d => raw += d);
          res.on('end', () => { try { resolve({ status: res.statusCode, data: JSON.parse(raw) }); } catch(e) { reject(e); } });
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
        req.end();
      });
      if (status !== 200) {
        // 400 often means the date range is empty or term has special chars
        // Try without date filter as fallback
        console.log(`  FedReg "${term}": HTTP ${status} — retrying without date filter`);
        try {
          const frQueryFallback = [
            'per_page=20', 'order=newest',
            'fields[]=title', 'fields[]=document_number', 'fields[]=publication_date',
            'fields[]=type', 'fields[]=abstract', 'fields[]=html_url',
            'fields[]=agencies', 'fields[]=effective_on', 'fields[]=comment_date',
            `conditions[term]=${encodeURIComponent(term)}`,
            'conditions[type][]=RULE', 'conditions[type][]=PRORULE', 'conditions[type][]=NOTICE'
          ].join('&');
          const { status: s2, data: d2 } = await new Promise((res2, rej2) => {
            const r2 = https.request({
              hostname: 'www.federalregister.gov', path: '/api/v1/documents.json?' + frQueryFallback,
              method: 'GET', headers: { 'Accept': 'application/json' }, timeout: 20000
            }, (resp) => {
              let raw2 = ''; resp.on('data', d => raw2 += d);
              resp.on('end', () => { try { res2({ status: resp.statusCode, data: JSON.parse(raw2) }); } catch(e) { rej2(e); } });
            });
            r2.on('error', rej2); r2.on('timeout', () => { r2.destroy(); rej2(new Error('timeout')); }); r2.end();
          });
          if (s2 === 200) {
            const docs2 = d2.results || [];
            console.log(`  FedReg "${term}" fallback: ${docs2.length} results`);
            for (const d of docs2) {
              const id = 'FEDREG-' + (d.document_number || Math.random());
              if (seen.has(id)) continue; seen.add(id);
              results.push({ id, source:'FEDREG', title:'[REG ALERT] '+(d.title||''), agency:(Array.isArray(d.agencies)?d.agencies.map(a=>a.name||a.raw_name||'').filter(Boolean).join(', '):'') || 'Federal Agency', subAgency:'', office:'', solNum:d.document_number||'', noticeId:d.document_number||'', noticeType:d.type==='PRORULE'?'Proposed Rule':d.type==='RULE'?'Final Rule':'Federal Notice', naicsCode:'621111', naicsDesc:'Occupational Medicine', setAside:'', setAsideCode:'', postedDate:d.publication_date||null, deadline:d.effective_on||d.comment_date||null, archiveDate:null, active:true, state:'', city:'', desc:d.abstract||d.excerpts||'', uiLink:d.html_url||'https://www.federalregister.gov', contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'Regulatory Notice' });
            }
          }
        } catch(e2) { console.error(`  FedReg fallback error: ${e2.message}`); }
        continue;
      }
      const docs = data.results || [];
      console.log(`  FedReg "${term}": ${docs.length} results`);
      for (const d of docs) {
        const id = 'FEDREG-' + (d.document_number || Math.random());
        if (seen.has(id)) continue; seen.add(id);
        results.push({
          id, source: 'FEDREG',
          title: '[REG ALERT] ' + (d.title || 'Federal Register Notice'),
          agency: (Array.isArray(d.agencies) ? d.agencies.map(a => a.name||a.raw_name||'').filter(Boolean).join(', ') : (d.agencies||'')) || 'Federal Agency',
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

// ── State Scrapers (Puppeteer + Browserless.io) ──────────────────────────────
// Uses headless Chrome via Browserless.io to scrape JS-rendered state portals.
// Free tier: 6 hrs/month — more than enough for daily scraping.
// Falls back to cheerio for old CFM/ASP sites that are static HTML.

async function scrapeWithPuppeteer(url, extractFn, label) {
  if (!BROWSERLESS_KEY) {
    console.log(`  ${label}: No BROWSERLESS_KEY set, skipping`);
    return [];
  }
  let browser;
  try {
    browser = await puppeteer.connect({
      browserWSEndpoint: `wss://chrome.browserless.io?token=${BROWSERLESS_KEY}`,
    });
    const page = await browser.newPage();
    await page.setDefaultNavigationTimeout(30000);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
    const results = await page.evaluate(extractFn);
    await browser.close();
    console.log(`  ${label}: ${results.length} results`);
    return results;
  } catch(e) {
    if (browser) try { await browser.close(); } catch(_) {}
    console.error(`  ${label} error: ${e.message}`);
    return [];
  }
}

// ── Texas ESBD ────────────────────────────────────────────────────────────────
async function fetchTexasESBD(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0, 2) : ['occupational health', 'drug testing'];
  const results = [];
  const seen = new Set();

  for (const term of terms) {
    const url = `https://www.txsmartbuy.gov/esbd?keyword=${encodeURIComponent(term)}&status=Open`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      // TX ESBD (txsmartbuy.gov) is a React app — data renders into various containers
      // Try multiple selector strategies
      const trySelectors = [
        '.results-list .result-item',
        '.bid-list li',
        '[data-testid="opportunity-row"]',
        'table tbody tr',
        '.MuiTableRow-root',
        '.opportunity-card',
        'tbody tr',
        'tr'
      ];
      let found = [];
      for (const sel of trySelectors) {
        const els = document.querySelectorAll(sel);
        if (els.length > 0) { found = Array.from(els); break; }
      }
      found.forEach((row, i) => {
        if (i === 0) return;
        const cells = row.querySelectorAll('td, .cell, [class*="cell"]');
        const anchor = row.querySelector('a');
        const title = (
          row.querySelector('[class*="title"], [class*="Title"], h2, h3, h4')?.textContent ||
          anchor?.textContent ||
          cells[0]?.textContent || ''
        ).trim();
        const agency = (cells[1]?.textContent || row.querySelector('[class*="agency"], [class*="Agency"]')?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || row.querySelector('[class*="date"], [class*="Date"]')?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5 && !title.includes('No results') && !title.includes('Sign in') && !title.includes('Loading'))
          items.push({ title, agency, deadline, link });
      });
      // Capture page text for debugging
      items._snippet = (document.body?.innerText || '').slice(0, 300);
      return items;
    }, `TX ESBD "${term}"`);
    if (rows._snippet) console.log(`  TX ESBD snippet: ${(rows._snippet||'').slice(0,150).replace(/\n/g,' ')}`);

    for (const r of rows) {
      const id = 'TX-' + Buffer.from(r.title).toString('base64').slice(0, 20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({
        id, source: 'STATE-TX', title: r.title,
        agency: r.agency || 'Texas State Agency',
        subAgency: '', office: '', solNum: '', noticeId: id,
        noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
        setAside: '', setAsideCode: '', postedDate: null,
        deadline: r.deadline || null, archiveDate: null, active: true,
        state: 'TX', city: '', desc: '',
        uiLink: r.link || `https://www.txsmartbuy.gov/esbd?keyword=${encodeURIComponent(term)}`,
        contact: '', awardAmount: 0, recipient: '', classCode: '', baseType: 'State Bid',
      });
    }
  }
  console.log(`Texas ESBD total: ${results.length}`);
  return results;
}

// ── Virginia eVA ──────────────────────────────────────────────────────────────
async function fetchVirginiaEVA(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0, 2) : ['occupational health', 'medical'];
  const results = [];
  const seen = new Set();

  for (const term of terms) {
    const url = `https://eva.virginia.gov/bso/external/publicBids.sdo?keyword=${encodeURIComponent(term)}&status=OPEN`;
    const rows = await scrapeWithPuppeteer(url, (searchTerm) => {
      // Wait for search box and trigger search
      const items = [];
      const rows = document.querySelectorAll('.opportunity-row, tr.data-row, tbody tr, .search-result');
      rows.forEach(row => {
        const title = row.querySelector('.title, td:first-child, h3, a')?.textContent?.trim();
        const agency = row.querySelector('.agency, td:nth-child(2)')?.textContent?.trim();
        const deadline = row.querySelector('.deadline, td:nth-child(3)')?.textContent?.trim();
        const link = row.querySelector('a')?.href;
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `VA eVA "${term}"`);

    for (const r of rows) {
      const id = 'VA-' + Buffer.from(r.title + (r.agency||'')).toString('base64').slice(0, 20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({
        id, source: 'STATE-VA', title: r.title,
        agency: r.agency || 'Virginia State Agency',
        subAgency: '', office: '', solNum: '', noticeId: id,
        noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
        setAside: '', setAsideCode: '', postedDate: null,
        deadline: r.deadline || null, archiveDate: null, active: true,
        state: 'VA', city: '', desc: '',
        uiLink: r.link || 'https://eva.virginia.gov',
        contact: '', awardAmount: 0, recipient: '', classCode: '', baseType: 'State Bid',
      });
    }
  }
  console.log(`Virginia eVA total: ${results.length}`);
  return results;
}

// ── Colorado OSC ──────────────────────────────────────────────────────────────
async function fetchColorado(keywords) {
  const url = 'https://osc.colorado.gov/spco/solicitations';
  const terms = keywords.length > 0 ? keywords.map(k => k.toLowerCase()) : ['health', 'medical', 'drug'];

  const rows = await scrapeWithPuppeteer(url, () => {
    const items = [];
    // Try multiple selector strategies for Colorado OSC (Drupal-based)
    const containers = document.querySelectorAll(
      '.view-content .views-row, table tbody tr, .views-row, tr.odd, tr.even, ' +
      '.views-table tbody tr, article, .solicitation-item'
    );
    containers.forEach((row, i) => {
      const anchor = row.querySelector('a');
      const title = (
        anchor?.textContent ||
        row.querySelector('td:first-child, .views-field-title, h3, h4')?.textContent ||
        row.querySelector('td')?.textContent || ''
      ).trim();
      const agency = (row.querySelector('td:nth-child(2), .views-field-field-agency')?.textContent || '').trim();
      const deadline = (row.querySelector('td:last-child, .views-field-field-close-date')?.textContent || '').trim();
      const link = anchor?.href || '';
      if (title && title.length > 5 && !title.match(/^\s*$/)) items.push({ title, agency, deadline, link });
    });
    items._snippet = (document.body?.innerText || '').slice(0, 400);
    return items;
  }, 'CO OSC');
  if (rows._snippet) console.log('  CO OSC snippet:', (rows._snippet||'').slice(0,200).replace(/\n/g,' '));

  const results = [];
  const seen = new Set();
  for (const r of rows) {
    const titleLower = r.title.toLowerCase();
    if (!terms.some(t => titleLower.includes(t))) continue;
    const id = 'CO-' + Buffer.from(r.title).toString('base64').slice(0, 20);
    if (seen.has(id)) continue; seen.add(id);
    results.push({
      id, source: 'STATE-CO', title: r.title,
      agency: r.agency || 'Colorado State Agency',
      subAgency: '', office: '', solNum: '', noticeId: id,
      noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
      setAside: '', setAsideCode: '', postedDate: null,
      deadline: r.deadline || null, archiveDate: null, active: true,
      state: 'CO', city: '', desc: '',
      uiLink: r.link || 'https://osc.colorado.gov/spco/solicitations',
      contact: '', awardAmount: 0, recipient: '', classCode: '', baseType: 'State Bid',
    });
  }
  console.log(`Colorado OSC total: ${results.length}`);
  return results;
}

// ── Georgia DOAS ──────────────────────────────────────────────────────────────
async function fetchGeorgia(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0, 2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();

  for (const term of terms) {
    const url = `https://ssl.doas.state.ga.us/PRSapp/PR_index.jsp`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, tr.dataRow, tr.altRow').forEach((row, i) => {
        if (i === 0) return;
        const cells = row.querySelectorAll('td');
        if (cells.length < 3) return;
        const anchor = row.querySelector('a');
        const title = cells[1]?.textContent?.trim() || anchor?.textContent?.trim();
        const agency = cells[2]?.textContent?.trim() || '';
        const deadline = cells[4]?.textContent?.trim() || cells[cells.length-1]?.textContent?.trim() || '';
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `GA DOAS "${term}"`);

    for (const r of rows) {
      const id = 'GA-' + Buffer.from(r.title + (r.agency||'')).toString('base64').slice(0, 20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({
        id, source: 'STATE-GA', title: r.title,
        agency: r.agency || 'Georgia State Agency',
        subAgency: '', office: '', solNum: '', noticeId: id,
        noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
        setAside: '', setAsideCode: '', postedDate: null,
        deadline: r.deadline || null, archiveDate: null, active: true,
        state: 'GA', city: '', desc: '',
        uiLink: r.link || 'https://ssl.doas.state.ga.us/PRSapp/PR_Search_Results.jsp',
        contact: '', awardAmount: 0, recipient: '', classCode: '', baseType: 'State Bid',
      });
    }
  }
  console.log(`Georgia DOAS total: ${results.length}`);
  return results;
}


// ── Generic static page fetcher (for CFM/ASP sites) ─────────────────────────

// ── SELF-HEALING SCRAPER ──────────────────────────────────────────────────────
// When any cheerio scraper returns 0 results, this agent:
//   1. Takes a snippet of the raw HTML
//   2. Sends it to Claude with context about what we're trying to extract
//   3. Gets back a CSS selector strategy
//   4. Retries extraction with the new selectors
//   5. Logs the fix so we can make it permanent
const GEMINI_KEY = process.env.GEMINI_API_KEY || '';

async function selfHealScraper(html, label, stateCode) {
  if (!GEMINI_KEY) {
    console.log(`  [SELF-HEAL] No GEMINI_API_KEY set, skipping`);
    return null;
  }
  if (!html || html.length < 100) return null;

  // Send a focused slice of HTML — enough to understand structure, not too many tokens
  const htmlSlice = html.slice(0, 8000);

  console.log(`  [SELF-HEAL] ${label}: 0 results — asking Claude to analyze structure...`);

  try {
    const prompt = `You are analyzing HTML from a U.S. government procurement portal (${label}) to extract bid/solicitation listings.

The scraper returned 0 results. Analyze this HTML and return a JSON object with the best selectors to extract procurement opportunities.

HTML:
\`\`\`html
${htmlSlice}
\`\`\`

Return ONLY valid JSON with this exact structure (no markdown, no explanation):
{
  "containerSelector": "CSS selector for each row/item containing one opportunity",
  "titleSelector": "CSS selector relative to container for the title/name",
  "agencySelector": "CSS selector relative to container for the agency name",
  "deadlineSelector": "CSS selector relative to container for the deadline/date",
  "linkSelector": "CSS selector relative to container for the link (a tag)",
  "notes": "brief explanation of what you found",
  "hasData": true/false
}

If there are no procurement listings visible (login wall, error page, empty results), set hasData to false.`;

    const resp = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key=${GEMINI_KEY}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: { maxOutputTokens: 400, temperature: 0 }
        })
      }
    );

    const data = await resp.json();
    const raw = data.candidates?.[0]?.content?.parts?.[0]?.text || '';
    const clean = raw.replace(/```json|```/g, '').trim();
    const selectors = JSON.parse(clean);

    if (!selectors.hasData) {
      console.log(`  [SELF-HEAL] ${label}: Claude says no data visible (${selectors.notes})`);
      return null;
    }

    console.log(`  [SELF-HEAL] ${label}: Claude suggests: ${selectors.containerSelector} (${selectors.notes})`);
    return selectors;
  } catch(e) {
    console.error(`  [SELF-HEAL] ${label}: Claude analysis failed: ${e.message}`);
    return null;
  }
}

// Apply healed selectors to re-extract from HTML
function applyHealedSelectors(html, selectors, label, stateCode, baseUrl) {
  const results = [];
  const seen = new Set();
  try {
    const $ = cheerio.load(html);
    $(selectors.containerSelector).each((i, el) => {
      if (i === 0 && $(el).find('th').length > 0) return; // skip header rows
      const title = ($(el).find(selectors.titleSelector).first().text() || '').trim();
      const agency = ($(el).find(selectors.agencySelector).first().text() || '').trim();
      const deadline = ($(el).find(selectors.deadlineSelector).first().text() || '').trim();
      const anchor = $(el).find(selectors.linkSelector).first();
      const link = anchor.attr('href') || '';
      if (!title || title.length < 5) return;
      const id = stateCode + '-HEALED-' + Buffer.from(title).toString('base64').slice(0, 16);
      if (seen.has(id)) return; seen.add(id);
      results.push({
        id, source: 'STATE-' + stateCode, title,
        agency: agency || label + ' Agency',
        subAgency: '', office: '', solNum: '', noticeId: id,
        noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
        setAside: '', setAsideCode: '', postedDate: null,
        deadline: deadline || null, archiveDate: null, active: true,
        state: stateCode, city: '', desc: '',
        uiLink: link.startsWith('http') ? link : (baseUrl + link),
        contact: '', awardAmount: 0, recipient: '', classCode: '', baseType: 'State Bid'
      });
    });
    console.log(`  [SELF-HEAL] ${label}: Re-extracted ${results.length} results with healed selectors`);
  } catch(e) {
    console.error(`  [SELF-HEAL] Apply error: ${e.message}`);
  }
  return results;
}

// Wrapper: scrape, and if 0 results, try self-healing
async function scrapePageHealing(url, label, stateCode, parseHtml) {
  const cacheKey = 'scrape:' + stateCode + ':' + Buffer.from(url).toString('base64').slice(0, 20);

  // Check cache first
  const cached = await cacheGet(cacheKey);
  if (cached !== null) {
    console.log(`  [CACHE HIT] ${label}`);
    return cached;
  }

  let html;
  try {
    html = await scrapePage(url, label);
  } catch(e) {
    console.error(`  ${label} fetch error: ${e.message}`);
    return [];
  }

  // Try normal parse first
  let results = [];
  try { results = parseHtml(html); } catch(e) {}

  // If 0 results, ask Claude to heal
  if (results.length === 0 && html.length > 500) {
    const selectors = await selfHealScraper(html, label, stateCode);
    if (selectors) {
      const baseUrl = new URL(url).origin;
      results = applyHealedSelectors(html, selectors, label, stateCode, baseUrl);
    }
  }

  // Cache results (even empty, for 1 hour to avoid hammering failed sites)
  const ttl = results.length > 0 ? CACHE_TTL : 3600;
  await cacheSet(cacheKey, results, ttl);
  return results;
}

function scrapePage(url, label) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = https.request({
      hostname: u.hostname, path: u.pathname + u.search, method: 'GET',
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; OccuMed/1.0)', 'Accept': 'text/html' },
      timeout: 20000
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        resolve(scrapePage(res.headers.location, label)); return;
      }
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => { console.log(`  ${label}: HTTP ${res.statusCode}, ${raw.length} bytes`); resolve(raw); });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

// ── Louisiana LaPAC (static CFM — cheerio works fine) ────────────────────────
async function fetchLouisiana(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0, 2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();

  for (const term of terms) {
    try {
      const url = `https://lagovpsprd.doa.louisiana.gov/osp/lapac/srchopen.cfm?deptno=all&catno=all&dateStart=&dateEnd=&compareDate=O&keywords=${encodeURIComponent(term)}&keywordsCheck=all`;
      const html = await scrapePage(url, `LA LaPAC "${term}"`);
      if (!html || html.length < 200) { console.error('  LA: empty response'); continue; }
      const $ = cheerio.load(html);

      // LaPAC: skip the search form table, only parse results table
      // Results have bid numbers (numeric) as first column
      $('table tr').each((i, row) => {
        if (i === 0) return;
        const cells = $(row).find('td');
        if (cells.length < 2) return;
        const anchor = $(row).find('a').first();
        const firstCell = $(cells[0]).text().trim();
        const title = anchor.text().trim() || firstCell;
        const agency = $(cells[1]).text().trim();
        const deadline = $(cells[2]).text().trim();
        const link = anchor.attr('href') || '';
        // Skip form labels — real rows have bid numbers or solicitation IDs in first cell
        if (!title || title.length < 5) return;
        if (['Category:', 'Begin Date:', 'End Date:', 'Compare Date:', 'Keywords:', 
             'Bid Number', 'Department', 'Category', 'Submit', 'Reset', 'Search'].includes(title)) return;
        // Real bid numbers are numeric or alphanumeric codes like "654321-26-0091"
        const looksLikeBid = /^[\d]/.test(title) || anchor.text().trim().length > 0;
        if (!looksLikeBid) return;

        const id = 'LA-' + Buffer.from(title).toString('base64').slice(0, 20);
        if (seen.has(id)) return; seen.add(id);
        results.push({
          id, source: 'STATE-LA', title,
          agency: agency || 'Louisiana State Agency',
          subAgency: '', office: '', solNum: '', noticeId: id,
          noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '', postedDate: null,
          deadline: deadline || null, archiveDate: null, active: true,
          state: 'LA', city: '', desc: '',
          uiLink: link.startsWith('http') ? link : `https://wwwcfprd.doa.louisiana.gov${link}`,
          contact: '', awardAmount: 0, recipient: '', classCode: '', baseType: 'State Bid',
        });
      });
    } catch(e) { console.error(`  LA LaPAC error: ${e.message}`); }
  }
  console.log(`Louisiana LaPAC total: ${results.length}`);
  return results;
}

// ── Mississippi (static — cheerio) ───────────────────────────────────────────
async function fetchMississippi(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0, 2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();

  for (const term of terms) {
    try {
      const url = `https://www.ms.gov/dfa/contract_bid_search/Bid/BidSearch?keyword=${encodeURIComponent(term)}&status=open`;
      const html = await scrapePage(url, `MS "${term}"`);
      if (!html || html.length < 200) { console.error('  MS: empty response'); continue; }
      const $ = cheerio.load(html);

      $('table tr').each((i, row) => {
        if (i === 0) return;
        const cells = $(row).find('td');
        if (cells.length < 2) return;
        const anchor = $(row).find('a').first();
        const title = anchor.text().trim() || $(cells[0]).text().trim();
        const agency = $(cells[1]).text().trim();
        const deadline = $(cells[cells.length - 1]).text().trim();
        const link = anchor.attr('href') || '';
        if (!title || title.length < 5) return;

        const id = 'MS-' + Buffer.from(title).toString('base64').slice(0, 20);
        if (seen.has(id)) return; seen.add(id);
        results.push({
          id, source: 'STATE-MS', title,
          agency: agency || 'Mississippi State Agency',
          subAgency: '', office: '', solNum: '', noticeId: id,
          noticeType: 'State Solicitation', naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '', postedDate: null,
          deadline: deadline || null, archiveDate: null, active: true,
          state: 'MS', city: '', desc: '',
          uiLink: link.startsWith('http') ? link : `https://www.ms.gov${link}`,
          contact: '', awardAmount: 0, recipient: '', classCode: '', baseType: 'State Bid',
        });
      });
    } catch(e) { console.error(`  MS error: ${e.message}`); }
  }
  console.log(`Mississippi total: ${results.length}`);
  return results;
}

// ── Master state scraper ──────────────────────────────────────────────────────
async function fetchStateBids(extraKeywords = []) {
  console.log('\nFetching state portals...');
  // Run Puppeteer scrapers sequentially to avoid RAM exhaustion on free tier
  const tx = await fetchTexasESBD(extraKeywords);
  const va = await fetchVirginiaEVA(extraKeywords);
  const co = await fetchColorado(extraKeywords);
  const ga = await fetchGeorgia(extraKeywords);
  // Static scrapers can run in parallel — no browser overhead
  const [la, ms] = await Promise.allSettled([
    fetchLouisiana(extraKeywords),
    fetchMississippi(extraKeywords),
  ]);
  const get = r => r.status === 'fulfilled' ? r.value : [];
  const nc = await fetchNorthCarolina(extraKeywords);
  const ma = await fetchMassachusetts(extraKeywords);
  const al = await fetchAlabama(extraKeywords);
  const md = await fetchMaryland(extraKeywords);
  const fl = await fetchFlorida(extraKeywords);
  const or_ = await fetchOregon(extraKeywords);
  const wa = await fetchWashington(extraKeywords);
  const fc = await fetchFedConnect(extraKeywords);
  const [sc] = await Promise.allSettled([fetchSouthCarolina(extraKeywords)]);
  const get2 = r => r.status === 'fulfilled' ? r.value : [];
  // Additional states — run sequentially to preserve Browserless credits
  const il = await fetchIllinois(extraKeywords);
  const pa = await fetchPennsylvania(extraKeywords);
  const nj = await fetchNewJersey(extraKeywords);
  const mi = await fetchMichigan(extraKeywords);
  const az = await fetchArizona(extraKeywords);
  const mn = await fetchMinnesota(extraKeywords);
  const ky = await fetchKentucky(extraKeywords);
  const [ct] = await Promise.allSettled([fetchConnecticut(extraKeywords)]);
  // Remaining 28 states — run in parallel batches of 4 to save Browserless time
  const batch1 = await Promise.allSettled([fetchOhio(extraKeywords), fetchTennessee(extraKeywords), fetchIndiana(extraKeywords), fetchWisconsin(extraKeywords)]);
  const batch2 = await Promise.allSettled([fetchIowa(extraKeywords), fetchMissouri(extraKeywords), fetchNevada(extraKeywords), fetchCalifornia(extraKeywords)]);
  const batch3 = await Promise.allSettled([fetchNewYork(extraKeywords), fetchOklahoma(extraKeywords), fetchArkansas(extraKeywords), fetchKansas(extraKeywords)]);
  const batch4 = await Promise.allSettled([fetchUtah(extraKeywords), fetchWestVirginia(extraKeywords), fetchNorthDakota(extraKeywords), fetchSouthDakota(extraKeywords)]);
  const batch5 = await Promise.allSettled([fetchNebraska(extraKeywords), fetchMontana(extraKeywords), fetchNewMexico(extraKeywords), fetchIdaho(extraKeywords)]);
  const batch6 = await Promise.allSettled([fetchWyoming(extraKeywords), fetchAlaska(extraKeywords), fetchHawaii(extraKeywords), fetchRhodeIsland(extraKeywords)]);
  const batch7 = await Promise.allSettled([fetchVermont(extraKeywords), fetchMaine(extraKeywords), fetchNewHampshire(extraKeywords), fetchDelaware(extraKeywords)]);
  const g2 = r => r.status === 'fulfilled' ? r.value : [];
  const batchResults = [...batch1,...batch2,...batch3,...batch4,...batch5,...batch6,...batch7].map(g2).flat();
  const all = [...tx,...va,...co,...ga,...get(la),...get(ms),...nc,...ma,...al,...md,...fl,...or_,...wa,...fc,...get2(sc),...il,...pa,...nj,...mi,...az,...mn,...ky,...get2(ct),...batchResults];
  console.log(`State portals combined: ${all.length} (50 states covered)`);
  return all;
}


// ── FPDS Atom Feed ────────────────────────────────────────────────────────────
// Free, no API key, public XML feed of federal contract awards
// NAICS 621111/812990/621999 filtered directly
// Note: FPDS ezsearch being decommissioned FY2026 but active now
async function fetchFPDS() {
  const results = [];
  const seen = new Set();
  const naicsCodes = ['621111', '812990', '621999'];
  
  for (const naics of naicsCodes) {
    try {
      const today = new Date();
      const from = new Date(); from.setDate(from.getDate() - 90);
      const fmt = d => d.toISOString().split('T')[0].replace(/-/g, '');
      const url = `https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&q=NAICS_CODE:"${naics}"+LAST_MOD_DATE:[${fmt(from)},${fmt(today)}]`;
      
      console.log(`  FPDS NAICS ${naics}...`);
      const xml = await new Promise((resolve, reject) => {
        const req = https.request({
          hostname: 'www.fpds.gov',
          path: `/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&q=NAICS_CODE:"${naics}"+LAST_MOD_DATE:[${fmt(from)},${fmt(today)}]`,
          method: 'GET',
          headers: { 'Accept': 'application/xml, text/xml', 'User-Agent': 'OccuMed/1.0' },
          timeout: 20000
        }, (res) => {
          let raw = ''; res.on('data', d => raw += d);
          res.on('end', () => resolve({ status: res.statusCode, body: raw }));
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
        req.end();
      });

      if (xml.status !== 200) { console.log(`  FPDS NAICS ${naics}: HTTP ${xml.status}`); continue; }
      
      // Parse XML with regex (no xml2js needed)
      const entries = xml.body.match(/<entry>([\s\S]*?)<\/entry>/g) || [];
      console.log(`  FPDS NAICS ${naics}: ${entries.length} entries`);

      for (const entry of entries) {
        const get = (tag) => { const m = entry.match(new RegExp(`<${tag}[^>]*>([\s\S]*?)</${tag}>`)); return m ? m[1].replace(/<!\[CDATA\[|\]\]>/g,'').trim() : ''; };
        const title = get('title') || get('name');
        const id = 'FPDS-' + (get('id') || Math.random());
        if (seen.has(id) || !title) continue; seen.add(id);
        
        const agency = get('agencyName') || get('contractingAgencyName') || 'Federal Agency';
        const awardAmt = parseFloat(get('obligatedAmount') || get('totalObligatedAmount') || '0');
        const link = (entry.match(/href="([^"]*)"/) || [])[1] || 'https://www.fpds.gov';
        const dateStr = get('signedDate') || get('lastModifiedDate') || '';
        
        results.push({
          id, source: 'FPDS',
          title: title.substring(0, 200),
          agency, subAgency: get('subAgencyName') || '', office: '',
          solNum: get('PIID') || get('contractNumber') || '',
          noticeId: id, noticeType: 'Contract Award (FPDS)',
          naicsCode: naics, naicsDesc: '',
          setAside: get('typeOfSetAside') || '', setAsideCode: '',
          postedDate: dateStr || null, deadline: null,
          archiveDate: null, active: true,
          state: get('placeOfPerformanceState') || '',
          city: get('placeOfPerformanceCity') || '',
          desc: get('description') || get('productOrServiceCode') || '',
          uiLink: link,
          contact: '', awardAmount: awardAmt, recipient: get('vendorName') || '',
          classCode: '', baseType: 'FPDS Award',
        });
      }
    } catch(e) { console.error(`  FPDS NAICS error: ${e.message}`); }
  }
  console.log(`FPDS total: ${results.length}`);
  return results;
}

// ── North Carolina IPS ────────────────────────────────────────────────────────
// Interactive Purchasing System — old ASP, static HTML, no JS required
async function fetchNorthCarolina(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  
  for (const term of terms) {
    try {
      const url = `https://ips.state.nc.us/ips/pubmain.asp?AppType=2&keyword=${encodeURIComponent(term)}&StatusCode=A`;
      const html = await scrapePage(url, `NC IPS "${term}"`);
      const $ = cheerio.load(html);
      
      $('table tr').each((i, row) => {
        if (i < 2) return; // skip headers
        const cells = $(row).find('td');
        if (cells.length < 3) return;
        const anchor = $(row).find('a').first();
        const title = anchor.text().trim() || $(cells[1]).text().trim();
        const agency = $(cells[0]).text().trim();
        const deadline = $(cells[cells.length-1]).text().trim();
        const link = anchor.attr('href') || '';
        if (!title || title.length < 5) return;
        if (['Agency','Title','Close Date','Open Date'].includes(title)) return;
        
        const id = 'NC-' + Buffer.from(title+agency).toString('base64').slice(0,20);
        if (seen.has(id)) return; seen.add(id);
        results.push({
          id, source: 'STATE-NC', title, agency: agency || 'NC State Agency',
          subAgency:'', office:'', solNum:'', noticeId:id,
          noticeType:'State Solicitation', naicsCode:'621111', naicsDesc:'',
          setAside:'', setAsideCode:'', postedDate:null,
          deadline: deadline||null, archiveDate:null, active:true,
          state:'NC', city:'', desc:'',
          uiLink: link.startsWith('http') ? link : `https://ips.state.nc.us${link}`,
          contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid',
        });
      });
    } catch(e) { console.error(`  NC IPS error: ${e.message}`); }
  }
  console.log(`NC IPS total: ${results.length}`);
  return results;
}

// ── Massachusetts COMMBUYS ────────────────────────────────────────────────────
// Old Oracle system — public search, static HTML
async function fetchMassachusetts(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  
  for (const term of terms) {
    try {
      const url = `https://www.commbuys.com/bso/external/publicBids.sdo?bidType=ALL&keyword=${encodeURIComponent(term)}&openDate=&closeDate=&docType=BD&category=&status=OPEN&vendorSearch=&submit=Search`;
      const html = await scrapePage(url, `MA COMMBUYS "${term}"`);
      const $ = cheerio.load(html);
      
      $('table.bids tr, table tr').each((i, row) => {
        if (i < 1) return;
        const cells = $(row).find('td');
        if (cells.length < 3) return;
        const anchor = $(row).find('a').first();
        const title = anchor.text().trim() || $(cells[1]).text().trim();
        const agency = $(cells[2]).text().trim() || $(cells[0]).text().trim();
        const deadline = $(cells[cells.length-1]).text().trim();
        const link = anchor.attr('href') || '';
        if (!title || title.length < 5) return;
        
        const id = 'MA-' + Buffer.from(title).toString('base64').slice(0,20);
        if (seen.has(id)) return; seen.add(id);
        results.push({
          id, source:'STATE-MA', title, agency: agency||'MA State Agency',
          subAgency:'', office:'', solNum:'', noticeId:id,
          noticeType:'State Solicitation', naicsCode:'621111', naicsDesc:'',
          setAside:'', setAsideCode:'', postedDate:null,
          deadline: deadline||null, archiveDate:null, active:true,
          state:'MA', city:'', desc:'',
          uiLink: link.startsWith('http') ? link : `https://www.commbuys.com${link}`,
          contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid',
        });
      });
    } catch(e) { console.error(`  MA COMMBUYS error: ${e.message}`); }
  }
  console.log(`MA COMMBUYS total: ${results.length}`);
  return results;
}


// ── FedConnect (Puppeteer) ────────────────────────────────────────────────────
async function fetchFedConnect(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['occupational health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://www.fedconnect.net/FedConnect/publicpages/publicsearch/Public_Opportunities.aspx`;
    const rows = await scrapeWithPuppeteer(url, (searchTerm) => {
      const items = [];
      document.querySelectorAll('table tr, .grid tr, [id*="Grid"] tr').forEach((row, i) => {
        if (i === 0) return;
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const anchor = row.querySelector('a');
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `FedConnect "${term}"`);
    for (const r of rows) {
      const titleLower = (r.title||'').toLowerCase();
      if (!['health','medical','drug','occupational','fitness','force'].some(t => titleLower.includes(t))) continue;
      const id = 'FC-' + Buffer.from(r.title+(r.agency||'')).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'FEDCONN', title:r.title, agency:r.agency||'Federal Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'Federal Opportunity',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'', city:'', desc:'', uiLink:r.link||'https://www.fedconnect.net',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'Federal Opportunity' });
    }
  }
  console.log(`FedConnect total: ${results.length}`);
  return results;
}

// ── Alabama (Puppeteer) ───────────────────────────────────────────────────────
async function fetchAlabama(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://www.alabamabuys.gov/page.aspx/en/rfp/request_browse_public?keyword=${encodeURIComponent(term)}`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, .rfp-row, .bid-row').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `Alabama "${term}"`);
    for (const r of rows) {
      const id = 'AL-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-AL', title:r.title, agency:r.agency||'Alabama State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'AL', city:'', desc:'', uiLink:r.link||'https://www.alabamabuys.gov',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Alabama total: ${results.length}`);
  return results;
}

// ── Maryland eMMA (Puppeteer) ─────────────────────────────────────────────────
async function fetchMaryland(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://emma.maryland.gov/page.aspx/en/rfp/request_browse_public?keyword=${encodeURIComponent(term)}`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, .rfp-row').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `Maryland "${term}"`);
    for (const r of rows) {
      const id = 'MD-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-MD', title:r.title, agency:r.agency||'Maryland State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'MD', city:'', desc:'', uiLink:r.link||'https://emma.maryland.gov',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Maryland eMMA total: ${results.length}`);
  return results;
}

// ── Florida VBS (Puppeteer) ───────────────────────────────────────────────────
async function fetchFlorida(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://vendor.myfloridamarketplace.com/search/bids?keyword=${encodeURIComponent(term)}&status=OPEN`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, .bid-list-item, .search-result-row').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `Florida VBS "${term}"`);
    for (const r of rows) {
      const id = 'FL-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-FL', title:r.title, agency:r.agency||'Florida State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'FL', city:'', desc:'', uiLink:r.link||'https://vendor.myfloridamarketplace.com',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Florida VBS total: ${results.length}`);
  return results;
}

// ── Oregon OregonBuys (Puppeteer) ─────────────────────────────────────────────
async function fetchOregon(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://oregonbuys.gov/bso/external/bidBoards/searchPublicBids.sdo?keyword=${encodeURIComponent(term)}&status=OPEN`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, .bid-row').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `Oregon "${term}"`);
    for (const r of rows) {
      const id = 'OR-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-OR', title:r.title, agency:r.agency||'Oregon State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'OR', city:'', desc:'', uiLink:r.link||'https://oregonbuys.gov',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Oregon total: ${results.length}`);
  return results;
}

// ── Washington WEBS (Puppeteer) ───────────────────────────────────────────────
async function fetchWashington(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://fortress.wa.gov/ga/webs/default.aspx`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `Washington "${term}"`);
    for (const r of rows) {
      const titleLower = (r.title||'').toLowerCase();
      if (!['health','medical','drug','occupational'].some(t => titleLower.includes(t))) continue;
      const id = 'WA-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-WA', title:r.title, agency:r.agency||'Washington State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'WA', city:'', desc:'', uiLink:r.link||'https://fortress.wa.gov/ga/webs',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Washington WEBS total: ${results.length}`);
  return results;
}

// ── South Carolina (static ASP) ───────────────────────────────────────────────
async function fetchSouthCarolina(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    try {
      const url = `https://webprod.cio.sc.gov/SCSolicitationWeb/solicitationSearchPage.do?searchText=${encodeURIComponent(term)}&status=A`;
      const html = await scrapePage(url, `SC "${term}"`);
      const $ = cheerio.load(html);
      $('table tr').each((i, row) => {
        if (i === 0) return;
        const cells = $(row).find('td');
        if (cells.length < 2) return;
        const anchor = $(row).find('a').first();
        const title = anchor.text().trim() || $(cells[0]).text().trim();
        const agency = $(cells[1]).text().trim();
        const deadline = $(cells[cells.length-1]).text().trim();
        const link = anchor.attr('href') || '';
        if (!title || title.length < 5) return;
        const id = 'SC-' + Buffer.from(title).toString('base64').slice(0,20);
        if (seen.has(id)) return; seen.add(id);
        results.push({ id, source:'STATE-SC', title, agency:agency||'SC State Agency',
          subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
          naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
          postedDate:null, deadline:deadline||null, archiveDate:null, active:true,
          state:'SC', city:'', desc:'',
          uiLink: link.startsWith('http') ? link : `https://webprod.cio.sc.gov${link}`,
          contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
      });
    } catch(e) { console.error(`  SC error: ${e.message}`); }
  }
  console.log(`South Carolina total: ${results.length}`);
  return results;
}


// ── Illinois BidBuy (Puppeteer) ───────────────────────────────────────────────
async function fetchIllinois(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://www.bidbuy.illinois.gov/bso/external/publicBids.sdo?keyword=${encodeURIComponent(term)}&status=OPEN`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, .bid-row, tr[class*="row"]').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `IL BidBuy "${term}"`);
    for (const r of rows) {
      const id = 'IL-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-IL', title:r.title, agency:r.agency||'Illinois State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'IL', city:'', desc:'', uiLink:r.link||'https://www.bidbuy.illinois.gov',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Illinois BidBuy total: ${results.length}`);
  return results;
}

// ── Pennsylvania eMarketplace (Puppeteer) ─────────────────────────────────────
async function fetchPennsylvania(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://www.emarketplace.state.pa.us/Search.aspx?q=${encodeURIComponent(term)}`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, .search-result, tr[class*="Row"]').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `PA eMarketplace "${term}"`);
    for (const r of rows) {
      const id = 'PA-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-PA', title:r.title, agency:r.agency||'Pennsylvania State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'PA', city:'', desc:'', uiLink:r.link||'https://www.emarketplace.state.pa.us',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Pennsylvania total: ${results.length}`);
  return results;
}

// ── New Jersey NJSTART (Puppeteer) ────────────────────────────────────────────
async function fetchNewJersey(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://www.njstart.gov/bso/external/publicBids.sdo?keyword=${encodeURIComponent(term)}&status=OPEN`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, .bid-row').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `NJ NJSTART "${term}"`);
    for (const r of rows) {
      const id = 'NJ-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-NJ', title:r.title, agency:r.agency||'New Jersey State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'NJ', city:'', desc:'', uiLink:r.link||'https://www.njstart.gov',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`New Jersey total: ${results.length}`);
  return results;
}

// ── Michigan SIGMA VSS (Puppeteer) ────────────────────────────────────────────
async function fetchMichigan(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://sigma.michigan.gov/webapp/PRDVSS2X1/AltSelfService`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, .result-row').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `MI SIGMA "${term}"`);
    for (const r of rows) {
      const titleLower = (r.title||'').toLowerCase();
      if (!terms.map(t=>t.toLowerCase()).some(t => titleLower.includes(t))) continue;
      const id = 'MI-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-MI', title:r.title, agency:r.agency||'Michigan State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'MI', city:'', desc:'', uiLink:r.link||'https://sigma.michigan.gov',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Michigan total: ${results.length}`);
  return results;
}

// ── Arizona APP (Puppeteer) ───────────────────────────────────────────────────
async function fetchArizona(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://app.az.gov/page.aspx/en/rfp/request_browse_public?keyword=${encodeURIComponent(term)}`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, .rfp-row').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `AZ APP "${term}"`);
    for (const r of rows) {
      const id = 'AZ-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-AZ', title:r.title, agency:r.agency||'Arizona State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'AZ', city:'', desc:'', uiLink:r.link||'https://app.az.gov',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Arizona total: ${results.length}`);
  return results;
}

// ── Minnesota SWIFT (Puppeteer) ───────────────────────────────────────────────
async function fetchMinnesota(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://supplier.swift.state.mn.us/psp/supp/?cmd=login&languageCd=ENG`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr, .ps_grid-row').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `MN SWIFT "${term}"`);
    for (const r of rows) {
      const titleLower = (r.title||'').toLowerCase();
      if (!terms.map(t=>t.toLowerCase()).some(t => titleLower.includes(t))) continue;
      const id = 'MN-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-MN', title:r.title, agency:r.agency||'Minnesota State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'MN', city:'', desc:'', uiLink:r.link||'https://supplier.swift.state.mn.us',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Minnesota total: ${results.length}`);
  return results;
}

// ── Connecticut CTsource (static — cheerio) ───────────────────────────────────
async function fetchConnecticut(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    try {
      const url = `https://ctsource.ct.gov/ctsource/solicitation/list?keyword=${encodeURIComponent(term)}&status=open`;
      const html = await scrapePage(url, `CT CTsource "${term}"`);
      const $ = cheerio.load(html);
      $('table tr, .solicitation-row').each((i, row) => {
        if (i === 0) return;
        const cells = $(row).find('td');
        if (cells.length < 2) return;
        const anchor = $(row).find('a').first();
        const title = anchor.text().trim() || $(cells[0]).text().trim();
        const agency = $(cells[1]).text().trim();
        const deadline = $(cells[cells.length-1]).text().trim();
        const link = anchor.attr('href') || '';
        if (!title || title.length < 5) return;
        const id = 'CT-' + Buffer.from(title).toString('base64').slice(0,20);
        if (seen.has(id)) return; seen.add(id);
        results.push({ id, source:'STATE-CT', title, agency:agency||'CT State Agency',
          subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
          naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
          postedDate:null, deadline:deadline||null, archiveDate:null, active:true,
          state:'CT', city:'', desc:'',
          uiLink: link.startsWith('http') ? link : `https://ctsource.ct.gov${link}`,
          contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
      });
    } catch(e) { console.error(`  CT error: ${e.message}`); }
  }
  console.log(`Connecticut total: ${results.length}`);
  return results;
}

// ── Kentucky VSS (Puppeteer) ──────────────────────────────────────────────────
async function fetchKentucky(keywords) {
  const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
  const results = [];
  const seen = new Set();
  for (const term of terms) {
    const url = `https://eProcurement.ky.gov/PRDVSS1X1/AltSelfService`;
    const rows = await scrapeWithPuppeteer(url, () => {
      const items = [];
      document.querySelectorAll('table tr').forEach((row, i) => {
        if (i === 0) return;
        const anchor = row.querySelector('a');
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
        const agency = (cells[1]?.textContent || '').trim();
        const deadline = (cells[cells.length-1]?.textContent || '').trim();
        const link = anchor?.href || '';
        if (title && title.length > 5) items.push({ title, agency, deadline, link });
      });
      return items;
    }, `KY VSS "${term}"`);
    for (const r of rows) {
      const titleLower = (r.title||'').toLowerCase();
      if (!terms.map(t=>t.toLowerCase()).some(t => titleLower.includes(t))) continue;
      const id = 'KY-' + Buffer.from(r.title).toString('base64').slice(0,20);
      if (seen.has(id)) continue; seen.add(id);
      results.push({ id, source:'STATE-KY', title:r.title, agency:r.agency||'Kentucky State Agency',
        subAgency:'', office:'', solNum:'', noticeId:id, noticeType:'State Solicitation',
        naicsCode:'621111', naicsDesc:'', setAside:'', setAsideCode:'',
        postedDate:null, deadline:r.deadline||null, archiveDate:null, active:true,
        state:'KY', city:'', desc:'', uiLink:r.link||'https://eProcurement.ky.gov',
        contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid' });
    }
  }
  console.log(`Kentucky total: ${results.length}`);
  return results;
}


// ── Remaining 28 States — Generic Puppeteer Factory ──────────────────────────
// Each state gets a scraper using their official public procurement URL.
// No login required — all are public bid search pages.

function makeStateScraper(stateCode, stateName, baseUrl, urlTemplate) {
  return async function(keywords) {
    const terms = keywords.length > 0 ? keywords.slice(0,2) : ['health', 'medical'];
    const results = [];
    const seen = new Set();
    const needsFilter = !urlTemplate.includes('{TERM}'); // if URL can't search, filter client-side
    
    const urlsToFetch = needsFilter ? [baseUrl] : terms.map(t => urlTemplate.replace('{TERM}', encodeURIComponent(t)));
    
    for (const url of urlsToFetch) {
      const term = needsFilter ? '' : terms[urlsToFetch.indexOf(url)];
      const rows = await scrapeWithPuppeteer(url, () => {
        const items = [];
        const selectors = ['table tbody tr', 'table tr', '.bid-row', '.result-row', '.solicitation-row', 'tr[class*="row"]'];
        let found = [];
        for (const sel of selectors) {
          const els = document.querySelectorAll(sel);
          if (els.length > 1) { found = Array.from(els); break; }
        }
        if (!found.length) found = Array.from(document.querySelectorAll('tr'));
        found.forEach((row, i) => {
          if (i === 0) return;
          const anchor = row.querySelector('a');
          const cells = row.querySelectorAll('td');
          if (cells.length < 2) return;
          const title = (anchor?.textContent || cells[0]?.textContent || '').trim();
          const agency = (cells[Math.min(1, cells.length-1)]?.textContent || '').trim();
          const deadline = (cells[cells.length-1]?.textContent || '').trim();
          const link = anchor?.href || '';
          if (title && title.length > 5 && !title.match(/^(Title|Description|Agency|Department|Close|Open|Due|Status|Type|Number|#)$/i))
            items.push({ title, agency, deadline, link });
        });
        return items;
      }, `${stateCode} "${term||'all'}"`);
      
      for (const r of rows) {
        if (needsFilter) {
          const tl = (r.title||'').toLowerCase();
          if (!['health','medical','drug','occupational','fitness','force','physical','screening'].some(t => tl.includes(t))) continue;
        }
        const id = stateCode + '-' + Buffer.from(r.title+(r.agency||'')).toString('base64').slice(0,18);
        if (seen.has(id)) continue; seen.add(id);
        results.push({
          id, source:'STATE-'+stateCode, title:r.title,
          agency: r.agency || stateName+' State Agency',
          subAgency:'', office:'', solNum:'', noticeId:id,
          noticeType:'State Solicitation', naicsCode:'621111', naicsDesc:'',
          setAside:'', setAsideCode:'', postedDate:null,
          deadline:r.deadline||null, archiveDate:null, active:true,
          state:stateCode, city:'', desc:'',
          uiLink: r.link?.startsWith('http') ? r.link : r.link ? baseUrl+r.link : baseUrl,
          contact:'', awardAmount:0, recipient:'', classCode:'', baseType:'State Bid'
        });
      }
    }
    console.log(`${stateName} total: ${results.length}`);
    return results;
  };
}

// Create scrapers for all remaining states
const fetchOhio        = makeStateScraper('OH','Ohio',       'https://procure.ohio.gov/proc/viewBids.procure?status=Open', 'https://procure.ohio.gov/proc/viewBids.procure?keyword={TERM}&status=Open');
const fetchTennessee   = makeStateScraper('TN','Tennessee',  'https://www.tn.gov/generalservices/procurement/central-procurement-office--cpo-/supplier-information/request-for-proposals--rfp--opportunities1.html', '');
const fetchIndiana     = makeStateScraper('IN','Indiana',    'https://www.in.gov/idoa/procurement/current-business-opportunities/', '');
const fetchWisconsin   = makeStateScraper('WI','Wisconsin',  'https://vendornet.wi.gov/vendornet/default.asp', '');
const fetchIowa        = makeStateScraper('IA','Iowa',       'https://bidopportunities.iowa.gov/', 'https://bidopportunities.iowa.gov/?keyword={TERM}');
const fetchMissouri    = makeStateScraper('MO','Missouri',   'https://oa.mo.gov/purchasing/bid-opportunities', 'https://oa.mo.gov/purchasing/bid-opportunities?keyword={TERM}');
const fetchNevada      = makeStateScraper('NV','Nevada',     'https://purchasing.nv.gov/Solicitations', 'https://purchasing.nv.gov/Solicitations?keyword={TERM}');
const fetchCalifornia  = makeStateScraper('CA','California', 'https://caleprocure.ca.gov/pages/Events-BS3/event-search.aspx', 'https://caleprocure.ca.gov/pages/Events-BS3/event-search.aspx?keywords={TERM}');
const fetchNewYork     = makeStateScraper('NY','New York',   'https://ogs.ny.gov/procurement/bid-opportunities', '');
const fetchOklahoma    = makeStateScraper('OK','Oklahoma',   'https://www.ok.gov/dcs/solicitationBid/searchSolicitationsBids.php', 'https://www.ok.gov/dcs/solicitationBid/searchSolicitationsBids.php?keyword={TERM}&status=Open');
const fetchArkansas    = makeStateScraper('AR','Arkansas',   'https://arbuy.arkansas.gov/page.aspx/en/rfp/request_browse_public', 'https://arbuy.arkansas.gov/page.aspx/en/rfp/request_browse_public?keyword={TERM}');
const fetchKansas      = makeStateScraper('KS','Kansas',     'https://supplier.sok.ks.gov/psp/sokfsprdsup/SUPPLIER/ERP/h/?tab=PAPP_GUEST', '');
const fetchUtah        = makeStateScraper('UT','Utah',       'https://purchasing.utah.gov/vendor/open-solicitations/', '');
const fetchWestVirginia= makeStateScraper('WV','West Virginia','https://wvOasis.gov/PRDVSS1X1/AltSelfService', '');
const fetchNorthDakota = makeStateScraper('ND','North Dakota','https://www.nd.gov/omb/public/public-notices', '');
const fetchSouthDakota = makeStateScraper('SD','South Dakota','https://bids.sd.gov/', 'https://bids.sd.gov/?keyword={TERM}');
const fetchNebraska    = makeStateScraper('NE','Nebraska',   'https://das.nebraska.gov/materiel/purchasing/bidsopen.html', '');
const fetchMontana     = makeStateScraper('MT','Montana',    'https://vendor.mt.gov/SupplierPortal/public/index.html', '');
const fetchNewMexico   = makeStateScraper('NM','New Mexico', 'https://www.generalservices.state.nm.us/state-purchasing/active-procurements/', '');
const fetchIdaho       = makeStateScraper('ID','Idaho',      'https://purchasing.idaho.gov/current-solicitations/', '');
const fetchWyoming     = makeStateScraper('WY','Wyoming',    'https://ai.wy.gov/GeneralServices/Procurement/bids.aspx', '');
const fetchAlaska      = makeStateScraper('AK','Alaska',     'https://aws.state.ak.us/OnlinePublicNotices/Notices/Search.aspx', '');
const fetchHawaii      = makeStateScraper('HI','Hawaii',     'https://hiepro.ehawaii.gov/sea/app/home;jsessionid=', '');
const fetchRhodeIsland = makeStateScraper('RI','Rhode Island','https://www.ridop.ri.gov/procurement-opportunities', '');
const fetchVermont     = makeStateScraper('VT','Vermont',    'https://www.bgs.vermont.gov/purchasing-contracting/bid-opportunities', '');
const fetchMaine       = makeStateScraper('ME','Maine',      'https://www.maine.gov/dafs/bbm/procurementservices/vendors/rfps', '');
const fetchNewHampshire= makeStateScraper('NH','New Hampshire','https://das.nh.gov/purchasing/bidding-openings.aspx', '');
const fetchDelaware    = makeStateScraper('DE','Delaware',   'https://mmp.delaware.gov/Sourcing/PublicAccess', '');



// ── SBA SUBNet ────────────────────────────────────────────────────────────────
// Free, no login, static HTML. Large prime contractors posting subcontracting
// opportunities — CACI, Leidos, etc. looking for health services subs.
// Directly relevant: primes with DoD health contracts need Occu-Med services.
async function fetchSBASubNet(keywords) {
  const results = [];
  const seen = new Set();
  const terms = keywords.length > 0 ? keywords.slice(0,3) : ['health', 'medical', 'occupational'];
  for (const term of terms) {
    try {
      const url = `https://subnet.sba.gov/client/dsp_Landing.cfm?action=search&keyword=${encodeURIComponent(term)}&state=0`;
      const html = await scrapePage(url, `SBA SUBNet "${term}"`);
      const $ = cheerio.load(html);
      $('table tr').each((i, row) => {
        if (i === 0) return;
        const cells = $(row).find('td');
        if (cells.length < 3) return;
        const title = $(cells[0]).text().trim();
        const company = $(cells[1]).text().trim();
        const deadline = $(cells[2]).text().trim();
        const anchor = $(row).find('a').first();
        const link = anchor.attr('href') || '';
        if (!title || title.length < 5) return;
        const id = 'SUBNET-' + Buffer.from(title + company).toString('base64').slice(0,20);
        if (seen.has(id)) return; seen.add(id);
        results.push({
          id, source: 'SUBNET',
          title: '[SUBCONTRACT] ' + title,
          agency: company || 'Prime Contractor',
          subAgency: '', office: '', solNum: '', noticeId: id,
          noticeType: 'Subcontracting Opportunity',
          naicsCode: '621111', naicsDesc: '',
          setAside: 'Small Business', setAsideCode: 'SBA',
          postedDate: null, deadline: deadline || null,
          archiveDate: null, active: true,
          state: '', city: '', desc: '',
          uiLink: link.startsWith('http') ? link : `https://subnet.sba.gov${link}`,
          contact: '', awardAmount: 0, recipient: '', classCode: '', baseType: 'Subcontract',
        });
      });
    } catch(e) { console.error(`  SBA SUBNet error: ${e.message}`); }
  }
  console.log(`SBA SUBNet total: ${results.length}`);
  return results;
}

// ── Bonfire County/City Portals (Puppeteer) ───────────────────────────────────
// Top government agencies using Bonfire — each has public opportunities tab
// Focused on large counties/cities relevant to Occu-Med's DoD contractor base
const BONFIRE_PORTALS = [
  // Texas
  { slug: 'harriscountytx',  label: 'Harris County TX',   state: 'TX' },
  { slug: 'cityofhoustonrfp',label: 'Houston TX',          state: 'TX' },
  { slug: 'dallas',          label: 'Dallas TX',           state: 'TX' },
  { slug: 'dentoncountytx',  label: 'Denton County TX',   state: 'TX' },
  { slug: 'sanantonio',      label: 'San Antonio TX',      state: 'TX' },
  // California
  { slug: 'lacounty',        label: 'LA County CA',        state: 'CA' },
  { slug: 'sdcounty',        label: 'San Diego County CA', state: 'CA' },
  { slug: 'cityoflosangeles', label: 'Los Angeles CA',     state: 'CA' },
  // Virginia / DC Metro
  { slug: 'fairfaxcounty',   label: 'Fairfax County VA',  state: 'VA' },
  { slug: 'arlingtonva',     label: 'Arlington VA',        state: 'VA' },
  { slug: 'dcgov',           label: 'Washington DC',       state: 'DC' },
  // Arizona
  { slug: 'maricopacounty',  label: 'Maricopa County AZ', state: 'AZ' },
  { slug: 'phoenixaz',       label: 'Phoenix AZ',          state: 'AZ' },
  // Colorado
  { slug: 'denvergov',       label: 'Denver CO',           state: 'CO' },
  // Florida
  { slug: 'miamidade',       label: 'Miami-Dade FL',       state: 'FL' },
];

async function fetchBonfire(keywords) {
  const results = [];
  const seen = new Set();
  const terms = keywords.length > 0 ? keywords.map(k => k.toLowerCase()) : ['health', 'medical', 'drug', 'occupational'];

  for (const portal of BONFIRE_PORTALS) {
    const url = `https://${portal.slug}.bonfirehub.com/portal/?tab=openOpportunities`;
    try {
      const rows = await scrapeWithPuppeteer(url, () => {
        const items = [];
        // Bonfire renders opportunities as cards or table rows
        const selectors = [
          '.opportunity-list-item',
          '.ng-scope[ng-repeat]',
          'table tr',
          '.bid-item',
          '[class*="opportunity"]',
        ];
        let found = [];
        for (const sel of selectors) {
          const els = document.querySelectorAll(sel);
          if (els.length > 0) { found = Array.from(els); break; }
        }
        found.forEach(el => {
          const title = el.querySelector('.title, h3, h4, td:first-child, [class*="title"]')?.textContent?.trim() || '';
          const agency = el.querySelector('.department, .agency, td:nth-child(2), [class*="department"]')?.textContent?.trim() || '';
          const deadline = el.querySelector('.date, .deadline, td:last-child, [class*="date"]')?.textContent?.trim() || '';
          const link = el.querySelector('a')?.href || '';
          if (title && title.length > 5) items.push({ title, agency, deadline, link });
        });
        // Also grab page text snippet for debugging
        items._snippet = document.body?.innerText?.slice(0, 200) || '';
        return items;
      }, `Bonfire ${portal.label}`);

      for (const r of rows) {
        const titleLower = (r.title || '').toLowerCase();
        // Only include health-relevant if we have keywords; otherwise include all
        if (keywords.length > 0 && !terms.some(t => titleLower.includes(t))) continue;
        const id = `BONFIRE-${portal.state}-` + Buffer.from(r.title + portal.slug).toString('base64').slice(0,18);
        if (seen.has(id)) continue; seen.add(id);
        results.push({
          id, source: `BONFIRE-${portal.state}`,
          title: r.title,
          agency: r.agency || portal.label,
          subAgency: '', office: '', solNum: '', noticeId: id,
          noticeType: 'Local Government Bid',
          naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '',
          postedDate: null, deadline: r.deadline || null,
          archiveDate: null, active: true,
          state: portal.state, city: portal.label.replace(/ (TX|CA|VA|DC|AZ|CO|FL)$/, ''),
          desc: '',
          uiLink: r.link || url,
          contact: '', awardAmount: 0, recipient: '', classCode: '', baseType: 'Local Bid',
        });
      }
    } catch(e) { console.error(`  Bonfire ${portal.label} error: ${e.message}`); }
  }
  console.log(`Bonfire portals total: ${results.length}`);
  return results;
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
app.get('/api/subnet',   async (req, res) => { try { res.json({ success:true, data: await fetchSBASubNet(parseKeywords(req)) }); } catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/bonfire',  async (req, res) => { try { res.json({ success:true, data: await fetchBonfire(parseKeywords(req)) }); }          catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/cache-status', async (req, res) => {
  try {
    const key = `opportunities:90:`;
    const cached = await cacheGet(key);
    res.json({
      cacheAvailable: !!UPSTASH_URL,
      hasData: cached !== null,
      itemCount: cached ? cached.total : 0,
      cachedAt: cached ? cached.cachedAt : null,
      upstashConfigured: !!(UPSTASH_URL && UPSTASH_TOKEN),
      aiConfigured: !!GEMINI_KEY, aiProvider: 'Google Gemini (free)',
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/fpds',      async (req, res) => { try { res.json({ success:true, data: await fetchFPDS() }); }                                              catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/sbir',        async (req, res) => { try { res.json({ success:true, data: await fetchSBIR() }); }                                                                           catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });

app.get('/api/opportunities', async (req, res) => {
  const days = parseInt(req.query.days) || 90;
  const kw = parseKeywords(req);
  const forceRefresh = req.query.refresh === 'true';
  const cacheKey = `opportunities:${days}:${kw.join(',')}`;

  // Serve from cache if available and not a forced refresh
  if (!forceRefresh) {
    const cached = await cacheGet(cacheKey);
    if (cached) {
      console.log(`[CACHE] Serving ${cached.total} opportunities from cache (${cached.cachedAt})`);
      return res.json({ ...cached, fromCache: true });
    }
  }

  const [samR, usaR, idvR, subR, grantR, sbirR, tangoR, fedregR, statesR, fpdsR] = await Promise.allSettled([
    fetchSAM(), fetchUSASpending(days, kw), fetchIDV(days, kw), fetchSubawards(days, kw),
    fetchGrants(days, kw), fetchSBIR(), fetchTango(), fetchFederalRegister(), fetchStateBids(kw), fetchFPDS()
  ]);
  const get = r => r.status === 'fulfilled' ? r.value : [];
  const err = (lbl, r) => r.status === 'rejected' ? [`${lbl}: ${r.reason?.message}`] : [];
  const all = [...get(samR),...get(usaR),...get(idvR),...get(subR),...get(grantR),...get(sbirR),...get(tangoR),...get(fedregR),...get(statesR),...get(fpdsR)];
  const payload = {
    success: true, total: all.length,
    samCount: get(samR).length, usaCount: get(usaR).length,
    idvCount: get(idvR).length, subCount: get(subR).length,
    grantsCount: get(grantR).length, sbirCount: get(sbirR).length,
    tangoCount: get(tangoR).length, fedregCount: get(fedregR).length,
    statesCount: get(statesR).length,
    errors: [...err('SAM',samR),...err('USASpending',usaR),...err('IDV',idvR),...err('Subawards',subR),...err('Grants',grantR),...err('SBIR',sbirR),...err('Tango',tangoR),...err('FedReg',fedregR),...err('States',statesR)],
    data: all, cachedAt: new Date().toISOString()
  };
  // Store in cache — only if we got meaningful results
  if (all.length > 10) await cacheSet(cacheKey, payload);
  res.json(payload);
});


// ── Incumbent Tracker ─────────────────────────────────────────────────────────
// Finds who currently holds a contract matching this NAICS/agency combination
app.get('/api/incumbent', async (req, res) => {
  try {
    const naics = req.query.naics || '621111';
    const agency = (req.query.agency || '').substring(0, 60);
    const state  = req.query.state || '';

    const body = JSON.stringify({
      filters: {
        naics_codes: [naics],
        ...(state ? { place_of_performance_locations: [{ country: 'USA', state: state }] } : {}),
        award_type_codes: ['A','B','C','D'],
      },
      fields: ['Award ID','Recipient Name','Award Amount','Period of Performance End Date','Solicitation ID','Awarding Agency'],
      sort: 'Award Amount', order: 'desc', limit: 20, page: 1, subawards: false
    });

    const { status, data } = await httpsPost(
      'https://api.usaspending.gov/api/v2/search/spending_by_award/', body
    );

    if (status !== 200) return res.json({ success: false, data: [] });

    const results = (data.results || []).map(r => ({
      recipient: r['Recipient Name'] || 'Unknown',
      amount: r['Award Amount'] || 0,
      endDate: r['Period of Performance End Date'] || null,
      solNum: r['Solicitation ID'] || '',
      agency: r['Awarding Agency'] || '',
    })).filter(r => r.recipient && r.recipient !== 'Unknown');

    res.json({ success: true, data: results });
  } catch(e) {
    res.status(500).json({ success: false, error: e.message, data: [] });
  }
});

// ── Teaming Radar ─────────────────────────────────────────────────────────────
// Finds contractors with existing occ health contracts in a state
// These are potential teaming partners (or competitors to know about)
app.get('/api/teaming', async (req, res) => {
  try {
    const naics  = req.query.naics || '621111';
    const state  = req.query.state || '';

    const filters = {
      naics_codes: [naics],
      award_type_codes: ['A','B','C','D'],
      time_period: [{ start_date: '2020-01-01', end_date: new Date().toISOString().split('T')[0] }],
    };
    if (state) filters.place_of_performance_locations = [{ country: 'USA', state }];

    const body = JSON.stringify({
      filters,
      fields: ['Recipient Name','Award Amount','Period of Performance End Date','Awarding Agency'],
      sort: 'Award Amount', order: 'desc', limit: 50, page: 1, subawards: false
    });

    const { status, data } = await httpsPost(
      'https://api.usaspending.gov/api/v2/search/spending_by_award/', body
    );

    if (status !== 200) return res.json({ success: false, data: [] });

    // Aggregate by recipient
    const byRecipient = {};
    for (const r of (data.results || [])) {
      const name = r['Recipient Name'];
      if (!name) continue;
      if (!byRecipient[name]) byRecipient[name] = { name, totalAmount: 0, contractCount: 0, latestEnd: null };
      byRecipient[name].totalAmount += Number(r['Award Amount'] || 0);
      byRecipient[name].contractCount++;
      const end = r['Period of Performance End Date'];
      if (end && (!byRecipient[name].latestEnd || end > byRecipient[name].latestEnd)) {
        byRecipient[name].latestEnd = end;
      }
    }

    const results = Object.values(byRecipient)
      .sort((a, b) => b.totalAmount - a.totalAmount)
      .slice(0, 25);

    res.json({ success: true, data: results });
  } catch(e) {
    res.status(500).json({ success: false, error: e.message, data: [] });
  }
});

// ── Background Auto-Refresh ──────────────────────────────────────────────────
// Pre-fetches all sources every 6 hours so cache is always warm.
// Portal loads instantly instead of triggering a 2-3 min live fetch.
async function backgroundRefresh() {
  console.log('\n[BG REFRESH] Starting scheduled refresh...');
  try {
    const baseUrl = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
    const resp = await fetch(`${baseUrl}/api/opportunities?days=90&refresh=true`);
    const data = await resp.json();
    console.log(`[BG REFRESH] Complete — ${data.total || 0} opportunities cached`);
  } catch(e) {
    console.error(`[BG REFRESH] Failed: ${e.message}`);
  }
}

// Run immediately on startup, then every 6 hours
setTimeout(backgroundRefresh, 30000); // 30s after boot (let server stabilize)
setInterval(backgroundRefresh, 6 * 60 * 60 * 1000);

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
  console.log(`Sources: 8 Federal APIs + ALL 50 States\n`);
});
