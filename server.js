const express = require('express');
const cors = require('cors');
const https = require('https');
const cheerio = require('cheerio');
const safeFetch = globalThis.fetch ? globalThis.fetch.bind(globalThis) : require('node-fetch');

require('dotenv').config({ path: './Env' });

const app = express();
const PORT = process.env.PORT || 3001;
const SAM_KEY = process.env.SAM_API_KEY || '';

// ── Upstash Redis Cache ───────────────────────────────────────────────────────
// Free tier: 500K commands/month. Cache results for 6 hours — eliminates
// rate limit hits and cold start wait.
const UPSTASH_URL   = process.env.UPSTASH_REDIS_REST_URL || '';
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const CACHE_TTL     = 6 * 60 * 60; // 6 hours in seconds

async function cacheGet(key) {
  if (!UPSTASH_URL) return null;
  try {
    const res = await safeFetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
    });
    if (!res.ok) return null;
    const text = await res.text();
    if (!text || text.startsWith('<')) return null;
    const json = JSON.parse(text);
    if (!json.result) return null;
    return JSON.parse(json.result);
  } catch(e) { console.log(`  Cache GET error: ${e.message}`); return null; }
}

async function cacheSet(key, value, ttl = CACHE_TTL) {
  if (!UPSTASH_URL) return;
  try {
    // Upstash REST API: value is raw body, TTL is query param EX (not nested object)
    const serialized = JSON.stringify(value);
    // Upstash free tier: 1MB max per key. Skip if over 900KB.
    if (serialized.length > 900000) {
      console.log(`  Cache SKIP: ${key} too large (${Math.round(serialized.length/1024)}KB)`);
      return;
    }
    await safeFetch(`${UPSTASH_URL}/set/${encodeURIComponent(key)}?ex=${ttl}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, 'Content-Type': 'application/json' },
      body: serialized
    });
  } catch(e) { console.log(`  Cache SET error: ${e.message}`); }
}

// Cached wrapper — checks cache first, fetches and stores if miss
async function withCache(key, fetchFn, ttl = CACHE_TTL) {
  const cached = await cacheGet(key);
  if (cached !== null) {
    console.log(`  [CACHE HIT] ${key} (${Array.isArray(cached) ? cached.length : '?'} items)`);
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
// Keywords derived directly from occu-med.com services:
// Pre-Deployment/Post-Deployment Health Assessments, Pre-Placement Medical Evaluations,
// Fit-for-Duty/Return-to-Work, Periodic Medical Surveillance, Global Immunization
// Clients: DoD/State contractors, public agencies, public safety, private employers
const OCC_KEYWORDS_BATCHES = [
  // ── Deployment health (core Occu-Med market — DoD/DoS contractors) ──────────
  ['pre-deployment medical', 'pre-deployment health assessment', 'deployment readiness examination', 'post-deployment health', 'redeployment physical'],
  ['contractor medical clearance', 'overseas deployment medical', 'theater medical requirements', 'CONUS to OCONUS', 'deployment fitness'],

  // ── Pre-placement / hiring exams ─────────────────────────────────────────────
  ['pre-placement medical evaluation', 'pre-employment medical examination', 'post-offer medical', 'employment medical screening', 'new hire physical'],
  ['essential job functions evaluation', 'job fitness evaluation', 'applicant medical evaluation', 'pre-hire health screening'],

  // ── Fit for duty / return to work ────────────────────────────────────────────
  ['fit for duty evaluation', 'fitness for duty examination', 'return to work evaluation', 'work capacity evaluation', 'medical fitness determination'],

  // ── OSHA-mandated medical surveillance programs ───────────────────────────────
  // OSHA 1910.95 — Hearing conservation (most common occ med contract)
  ['hearing conservation program', 'audiometric testing', 'audiometric examination', 'noise-induced hearing loss', 'OSHA 1910.95'],
  // OSHA 1910.134 — Respirator medical evaluations
  ['respirator medical evaluation', 'respiratory protection program', 'OSHA 1910.134', 'fit test medical clearance', 'respirator clearance'],
  // OSHA 1910.1025 / 1926.62 — Lead surveillance
  ['lead medical surveillance', 'blood lead monitoring', 'lead exposure medical', 'OSHA lead standard', 'lead biological monitoring'],
  // OSHA 1910.1001 / 1926.1101 — Asbestos surveillance
  ['asbestos medical surveillance', 'asbestos physical examination', 'OSHA asbestos standard', 'asbestos exposure monitoring'],
  // OSHA 1910.1096 / DOE — Radiation / ionizing radiation workers
  ['radiation medical surveillance', 'ionizing radiation physical', 'radiation worker examination', 'dosimetry medical program'],
  // OSHA 1910.1200 — Hazardous chemicals / HAZMAT
  ['hazmat medical surveillance', 'hazardous materials medical', 'chemical exposure medical', 'OSHA HAZWOPER medical', '1910.120 physical'],
  // OSHA 1910.95 / silica / hexavalent chromium
  ['silica medical surveillance', 'hexavalent chromium medical', 'beryllium medical surveillance', 'cadmium medical surveillance'],
  // OSHA ergonomics / general industry
  ['OSHA medical surveillance program', 'periodic medical evaluation', 'occupational medical surveillance', 'workplace medical monitoring'],

  // ── DOT / FMCSA regulated exams ──────────────────────────────────────────────
  ['DOT physical examination', 'FMCSA medical examination', 'commercial driver medical', 'CDL physical', 'DOT medical certificate'],
  ['DOT drug testing', 'DOT alcohol testing', 'DOT-regulated drug program', 'DOT substance abuse program', 'FMCSA drug clearinghouse'],

  // ── Occupational medicine / general ──────────────────────────────────────────
  ['occupational medicine services', 'occupational health services', 'employee health program', 'workforce health management'],
  ['pulmonary function testing', 'spirometry testing', 'chest X-ray program', 'musculoskeletal evaluation', 'vision screening program'],

  // ── Immunization / travel health ─────────────────────────────────────────────
  ['occupational immunization', 'employee immunization program', 'travel health services', 'overseas immunization', 'vaccination program'],
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
      sort: 'action_date', order: 'desc', limit: 50, page: 1, subawards: false
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
        // Skip inactive/closed opportunities
        if (o.active !== 'Yes') continue;
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
        'Sub-Award ID', 'Sub-Award Type', 'Sub-Awardee Name', 'Sub-Award Date',
        'Sub-Award Amount', 'Awarding Agency', 'Sub-Award Description'
      ],
      sort: 'Action Date', order: 'desc', limit: 50, page: 1
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
// Tango search terms — specific to Occu-Med's service lines
const OCC_TERMS = [
  'pre-deployment medical',
  'deployment health assessment',
  'fit for duty',
  'occupational medical surveillance',
  'hearing conservation',
  'respirator medical evaluation',
  'DOT physical examination',
  'pre-placement medical',
  'OSHA medical surveillance',
  'lead medical surveillance',
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
        // Skip closed opportunities
        const deadline = o.response_deadline || o.close_date || null;
        if (deadline && new Date(deadline) < new Date()) continue;

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
          deadline: deadline,
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
  const terms = ['occupational medical surveillance', 'hearing conservation', 'respirator medical evaluation', 'fit for duty', 'deployment health assessment'];
  const results = [];
  const seen = new Set();

  // Date formatted as MM/DD/YYYY — safer for Federal Register API (avoids 400 on ISO format)
  const d30 = new Date(); d30.setDate(d30.getDate() - 30);
  const dateStr = d30.toISOString().split('T')[0]; // YYYY-MM-DD required by Federal Register API

  for (const term of terms) {
    // Strip literal quotes from term — encoding issues cause 400s
    const safeTerm = term.replace(/"/g, '');
    console.log(`\nFetching Federal Register: "${safeTerm}"...`);
    try {
      // Use safeFetch — federalregister.gov handles standard percent-encoding fine
      const url = `https://www.federalregister.gov/api/v1/documents.json` +
        `?per_page=20&order=newest` +
        `&conditions[term]=${encodeURIComponent(safeTerm)}` +
        `&conditions[publication_date][gte]=${dateStr}`;

      const res = await safeFetch(url, { headers: { 'Accept': 'application/json' } });
      if (!res.ok) {
        console.log(`  FedReg "${safeTerm}": HTTP ${res.status} — skipping`);
        continue;
      }
      const data = await res.json();
      const status = res.status;
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


// ── Socrata Open Data API (SODA) ─────────────────────────────────────────────
// Uses the Socrata Discovery API to search ALL Socrata portals at once.
// One call hits: NYC, Chicago, Texas, LA, SF, Seattle, Philly, Baltimore,
// Miami-Dade, Cook County, Nashville, Maryland, NY State, CA, TX, IL, etc.
// Register free at dev.socrata.com for SOCRATA_APP_TOKEN (removes rate limits).
const SOCRATA_TOKEN = process.env.SOCRATA_APP_TOKEN || '';

async function fetchSocrata() {
  const results = [];
  const seen = new Set();

  // Socrata Discovery API searches across ALL Socrata portals simultaneously
  // Filter to US government domains only (skip international noise)
  const usDomains = [
    'data.cityofnewyork.us', 'data.cityofchicago.org', 'data.texas.gov',
    'data.lacity.org', 'data.sfgov.org', 'data.austintexas.gov',
    'data.seattle.gov', 'data.phila.gov', 'data.baltimorecity.gov',
    'data.miamidade.gov', 'data.cookcountyil.gov', 'data.montgomerycountymd.gov',
    'data.nashville.gov', 'data.brla.gov', 'data.kcmo.org',
    'opendata.maryland.gov', 'data.ny.gov', 'data.ca.gov',
    'data.illinois.gov', 'data.michigan.gov', 'data.pa.gov',
    'data.colorado.gov', 'data.oregon.gov', 'data.utah.gov',
    'data.iowa.gov', 'data.ct.gov', 'data.ok.gov', 'data.mo.gov',
    'data.hawaii.gov', 'data.smcgov.org', 'data.cincinnati-oh.gov',
    'opendata.lasvegasnevada.gov'
  ].join(',');

  // Occu-Med procurement search terms
  const searchTerms = [
    'medical surveillance occupational',
    'health screening solicitation',
    'DOT physical examination',
    'fit for duty medical',
    'occupational health services bid',
    'pre-employment medical',
  ];

  for (const term of searchTerms) {
    try {
      // Discovery API: searches dataset names, descriptions, and column names
      const url = `https://api.us.socrata.com/api/catalog/v1` +
        `?q=${encodeURIComponent(term)}` +
        `&domains=${encodeURIComponent(usDomains)}` +
        `&categories=Government%2CPublic+Safety%2CHealth` +
        `&limit=20&offset=0`;

      const headers = { 'Accept': 'application/json' };
      if (SOCRATA_TOKEN) headers['X-App-Token'] = SOCRATA_TOKEN;

      const res = await safeFetch(url, { headers });
      if (!res.ok) { console.log(`  Socrata Discovery "${term}": HTTP ${res.status}`); continue; }
      const json = await res.json();
      const datasets = json.results || [];
      console.log(`  Socrata Discovery "${term}": ${datasets.length} datasets`);

      for (const ds of datasets) {
        const meta = ds.resource || {};
        const title = meta.name || '';
        if (!title || title.length < 5) continue;

        // Must look like a procurement/solicitation dataset
        const desc = (meta.description || '').toLowerCase();
        const lTitle = title.toLowerCase();
        const isProcurement = ['solicitation','bid','rfp','rfq','contract','procurement','award','purchase']
          .some(kw => lTitle.includes(kw) || desc.includes(kw));
        if (!isProcurement) continue;

        const domain = ds.metadata?.domain || '';
        const id4 = meta.id || '';
        const rawId = `SOCRATA-${id4}`;
        if (seen.has(rawId)) continue; seen.add(rawId);

        const link = meta.permalink || `https://${domain}/d/${id4}`;
        const dataLink = `https://${domain}/resource/${id4}.json`;

        results.push({
          id: rawId, source: 'SOCRATA',
          title,
          agency: ds.classification?.domain_category || domain || 'Municipal Government',
          subAgency: '', office: '', solNum: id4,
          noticeId: rawId, noticeType: 'Municipal Solicitation',
          naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '',
          postedDate: meta.createdAt ? new Date(meta.createdAt * 1000).toISOString().split('T')[0] : null,
          deadline: null,
          archiveDate: null, active: true,
          state: '', city: '',
          desc: (meta.description || '').substring(0, 300),
          uiLink: link,
          dataEndpoint: dataLink,
          contact: '', awardAmount: 0, recipient: '', classCode: '', baseType: 'Municipal Bid',
        });
      }
    } catch(e) { console.error(`  Socrata Discovery error: ${e.message}`); }
  }

  console.log(`Socrata total: ${results.length}`);
  return results;
}

// ── CKAN Open Data API ────────────────────────────────────────────────────────
// CKAN Action API — JSON, no auth required. Full portal list from Gemini research.
const CKAN_PORTALS = [
  // US Federal
  { base: 'https://catalog.data.gov',    label: 'Data.gov',       query: 'occupational health procurement solicitation', fq: '',                     state: '' },
  // US State / City
  { base: 'https://data.virginia.gov',   label: 'Virginia',       query: 'eVA procurement medical services',            fq: 'tags:procurement',      state: 'VA' },
  { base: 'https://data.boston.gov',     label: 'Boston',         query: 'health medical services contract bid',        fq: '',                     state: 'MA' },
  { base: 'https://data.sanjoseca.gov',  label: 'San Jose CA',    query: 'medical occupational health bid',             fq: '',                     state: 'CA' },
  { base: 'https://www.denvergov.org',   label: 'Denver CO',      query: 'medical health solicitation',                 fq: '',                     state: 'CO' },
];

async function fetchCKAN() {
  const results = [];
  const seen = new Set();

  for (const portal of CKAN_PORTALS) {
    const base = portal.base.endsWith('/opendata')
      ? portal.base.replace('/opendata', '')
      : portal.base;
    const url = `${base}/api/3/action/package_search` +
      `?q=${encodeURIComponent(portal.query)}` +
      (portal.fq ? `&fq=${encodeURIComponent(portal.fq)}` : '') +
      `&rows=20&sort=metadata_modified+desc`;

    try {
      const res = await safeFetch(url, { headers: { 'Accept': 'application/json' } });
      if (!res.ok) { console.log(`  CKAN ${portal.label}: HTTP ${res.status}`); continue; }
      const json = await res.json();
      const packages = json.result?.results || [];
      console.log(`  CKAN ${portal.label}: ${packages.length} packages`);

      for (const pkg of packages) {
        const title = pkg.title || '';
        const notes = (pkg.notes || '').toLowerCase();
        const tags = (pkg.tags || []).map(t => t.name || '').join(' ').toLowerCase();
        const relevantText = (title + ' ' + notes + ' ' + tags).toLowerCase();
        const isRelevant = ['solicitation','rfp','rfq','bid','contract','procurement','medical','health','occupational']
          .some(kw => relevantText.includes(kw));
        if (!isRelevant) continue;

        const id = `CKAN-${portal.label.replace(/\s/g,'')}-${pkg.id || Buffer.from(title).toString('base64').slice(0,10)}`;
        if (seen.has(id)) continue; seen.add(id);

        const resources = pkg.resources || [];
        const link = resources.find(r => ['JSON','CSV','HTML'].includes(r.format?.toUpperCase()))?.url
          || `${portal.base}/dataset/${pkg.name}`;

        results.push({
          id, source: 'CKAN',
          title, agency: pkg.organization?.title || portal.label,
          subAgency: '', office: '', solNum: pkg.name || '',
          noticeId: id, noticeType: 'Open Data Procurement',
          naicsCode: '621111', naicsDesc: '',
          setAside: '', setAsideCode: '',
          postedDate: pkg.metadata_created ? pkg.metadata_created.split('T')[0] : null,
          deadline: pkg.extras?.find(e => e.key === 'deadline_date')?.value || null,
          archiveDate: null, active: true,
          state: portal.state, city: '',
          desc: (pkg.notes || '').substring(0, 300),
          uiLink: link,
          contact: pkg.maintainer_email || pkg.author_email || '',
          awardAmount: 0, recipient: '', classCode: '', baseType: 'Open Data',
        });
      }
    } catch(e) { console.error(`  CKAN ${portal.label} error: ${e.message}`); }
  }

  console.log(`CKAN total: ${results.length}`);
  return results;
}


// Puppeteer/Browserless scraping removed — too unreliable on free tier.
// Federal APIs (SAM, USASpending, IDV, Grants, Tango, FedReg) provide
// all high-quality opportunities without any external browser dependency.
async function fetchStateBids() {
  return [];
}

async function fetchSBASubNet() {
  return [];
}

async function fetchBonfire() {
  return [];
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
    const combined = await cacheGet('opp:federal:');
    const sourceKeys = ['opp:sam','opp:usa','opp:idv','opp:grants','opp:tango','opp:fedreg','opp:fpds','opp:subawards','opp:states'];
    const sourceCounts = {};
    for (const k of sourceKeys) {
      const d = await cacheGet(k);
      sourceCounts[k] = d ? d.length : 0;
    }
    res.json({
      upstashConfigured: !!(UPSTASH_URL && UPSTASH_TOKEN),
      aiConfigured: !!GEMINI_KEY,
      refreshRunning,
      inMemoryCachedAt,
      combinedCacheHit: !!combined,
      totalCached: combined ? combined.total : (inMemoryCache ? inMemoryCache.total : 0),
      cachedAt: combined ? combined.cachedAt : (inMemoryCachedAt ? inMemoryCachedAt.toISOString() : null),
      sourceCounts,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/socrata',    async (req, res) => { try { res.json({ success:true, data: await fetchSocrata() }); }    catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/ckan',       async (req, res) => { try { res.json({ success:true, data: await fetchCKAN() }); }      catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/sbir',        async (req, res) => { try { res.json({ success:true, data: await fetchSBIR() }); }                                                                           catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });

// ── In-memory fallback when Redis is unavailable ──────────────────────────────
let inMemoryCache = null;
let inMemoryCachedAt = null;

app.get('/api/opportunities', async (req, res) => {
  const kw = parseKeywords(req);
  const cacheKey = `opp:federal:${kw.join(',')}`;

  // FIX 1+2: /api/opportunities NEVER runs live scraping inline.
  // It ONLY reads from cache and returns immediately.
  // All live fetching happens in backgroundRefresh() which runs as a background job.

  // Try Redis cache first
  const cached = await cacheGet(cacheKey);
  if (cached) {
    console.log(`[CACHE] Serving ${cached.total} opps from Redis`);
    return res.json({ ...cached, fromCache: true });
  }

  // Fallback: in-memory cache (survives Redis outage)
  if (inMemoryCache) {
    console.log(`[CACHE] Serving ${inMemoryCache.total} opps from memory`);
    return res.json({ ...inMemoryCache, fromCache: true, cacheSource: 'memory' });
  }

  // Nothing cached yet — tell the client to wait for background refresh
  return res.json({
    success: true, total: 0, data: [], fromCache: false,
    message: 'Data is loading — background refresh running. Try again in 2 minutes.',
    cachedAt: null
  });
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
// FIX 1+2+4: Calls fetch functions DIRECTLY — no HTTP roundtrip, no timeout risk.
// Stores results in separate per-source cache keys to stay under 1MB Upstash limit.
// The /api/opportunities endpoint reads from these keys and assembles the response.
let refreshRunning = false;

async function backgroundRefresh() {
  if (refreshRunning) { console.log('[BG REFRESH] Already running, skipping'); return; }
  refreshRunning = true;
  console.log('\n[BG REFRESH] Starting (calling fetch functions directly)...');
  const kw = [];
  const days = 90;

  try {
    // FIX 4: Fetch and cache each source SEPARATELY to stay under 1MB per key
    const sources = [
      { key: 'opp:sam',        fn: () => fetchSAM() },
      { key: 'opp:usa',        fn: () => fetchUSASpending(days, kw) },
      { key: 'opp:idv',        fn: () => fetchIDV(days, kw) },
      { key: 'opp:grants',     fn: () => fetchGrants(days, kw) },
      { key: 'opp:tango',      fn: () => fetchTango() },
      { key: 'opp:fedreg',     fn: () => fetchFederalRegister() },
      { key: 'opp:subawards',  fn: () => fetchSubawards(days, kw) },
      { key: 'opp:socrata',    fn: () => fetchSocrata() },
      { key: 'opp:ckan',       fn: () => fetchCKAN() },
    ];

    // Run fast API sources in parallel
    const results = await Promise.allSettled(sources.map(src => src.fn()));
    let totalCount = 0;
    for (let i = 0; i < sources.length; i++) {
      const data = results[i].status === 'fulfilled' ? results[i].value : [];
      await cacheSet(sources[i].key, data);
      console.log(`  [BG] ${sources[i].key}: ${data.length} items cached`);
      totalCount += data.length;
    }

    // States run separately (slow, Puppeteer) — cache independently
    console.log('  [BG] Running state scrapers...');
    const states = await fetchStateBids(kw);
    await cacheSet('opp:states', states);
    console.log(`  [BG] opp:states: ${states.length} items cached`);
    totalCount += states.length;

    // Build combined index (just IDs + metadata, not full data) for the status endpoint
    const allData = [];
    const allKeys = [...sources.map(s => s.key), 'opp:states', 'opp:socrata', 'opp:ckan'];
    for (const key of allKeys) {
      const d = await cacheGet(key);
      if (d) allData.push(...d);
    }

    const summary = {
      success: true, total: allData.length,
      cachedAt: new Date().toISOString(),
      data: allData  // full combined payload for the response
    };

    // Cache the combined payload — skip if over 1MB, clients will assemble from parts
    await cacheSet(`opp:federal:`, summary);
    inMemoryCache = summary;
    inMemoryCachedAt = new Date();

    console.log(`[BG REFRESH] Complete — ${allData.length} total opportunities cached`);
  } catch(e) {
    console.error(`[BG REFRESH] Error: ${e.message}`);
  } finally {
    refreshRunning = false;
  }
}

// Run after 60s on startup (let server stabilize), then every 6 hours
setTimeout(backgroundRefresh, 60000);
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
  console.log(`\nOccu-Med Backend v5.1 running on port ${PORT}`);
  console.log(`SAM API key: ${SAM_KEY ? SAM_KEY.slice(0,12)+'...' : 'NOT SET'}`);
  console.log(`Sources: 8 Federal APIs + ALL 50 States\n`);
});
