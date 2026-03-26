const express = require('express');
const cors = require('cors');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
require('dotenv').config({ path: './Env' });

const app = express();
const PORT = process.env.PORT || 3001;
const SAM_KEY = process.env.SAM_API_KEY || '';
const TANGO_KEY = process.env.TANGO_API_KEY || '';

app.use(cors({ origin: '*' }));
app.use(express.json());

// ── RETRY HTTP HELPERS ─────────────────────────────────────────────────────────
function httpsGet(url, retries = 3, timeout = 25000) {
  return new Promise((resolve, reject) => {
    const attemptFetch = (attemptsLeft) => {
      const protocol = url.startsWith('https') ? https : http;
      const req = protocol.get(url, { timeout }, (res) => {
        let body = '';
        res.on('data', d => body += d);
        res.on('end', () => {
          try { resolve({ status: res.statusCode, data: JSON.parse(body) }); }
          catch (e) { reject(new Error(`JSON parse failed`)); }
        });
      });
      req.on('error', (err) => {
        if (attemptsLeft > 1) {
          console.log(`  Retry ${4-attemptsLeft+1}/3...`);
          setTimeout(() => attemptFetch(attemptsLeft - 1), 1000);
        } else { reject(err); }
      });
      req.on('timeout', () => {
        req.destroy();
        if (attemptsLeft > 1) setTimeout(() => attemptFetch(attemptsLeft - 1), 1000);
        else reject(new Error('Timeout'));
      });
    };
    attemptFetch(retries);
  });
}

function httpsGetH(url, headers = {}, retries = 3, timeout = 25000) {
  return new Promise((resolve, reject) => {
    const attemptFetch = (attemptsLeft) => {
      const u = new URL(url);
      const protocol = u.protocol === 'https:' ? https : http;
      const req = protocol.request({
        hostname: u.hostname, path: u.pathname + u.search, method: 'GET',
        headers: { 'Accept': 'application/json', 'User-Agent': 'OccuMed-Backend/1.0', ...headers },
        timeout
      }, (res) => {
        let raw = '';
        res.on('data', d => raw += d);
        res.on('end', () => {
          try { resolve({ status: res.statusCode, data: JSON.parse(raw) }); }
          catch (e) { reject(new Error(`JSON parse failed`)); }
        });
      });
      req.on('error', (err) => {
        if (attemptsLeft > 1) setTimeout(() => attemptFetch(attemptsLeft - 1), 1000);
        else reject(err);
      });
      req.on('timeout', () => {
        req.destroy();
        if (attemptsLeft > 1) setTimeout(() => attemptFetch(attemptsLeft - 1), 1000);
        else reject(new Error('timeout'));
      });
      req.end();
    };
    attemptFetch(retries);
  });
}

function httpsPost(url, bodyStr, retries = 3, timeout = 25000) {
  return new Promise((resolve, reject) => {
    const attemptFetch = (attemptsLeft) => {
      const buf = Buffer.from(bodyStr);
      const u = new URL(url);
      const protocol = u.protocol === 'https:' ? https : http;
      const req = protocol.request({
        hostname: u.hostname, path: u.pathname + u.search, method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': buf.length, 'User-Agent': 'OccuMed-Backend/1.0' },
        timeout
      }, (res) => {
        let raw = '';
        res.on('data', d => raw += d);
        res.on('end', () => {
          try { resolve({ status: res.statusCode, data: JSON.parse(raw) }); }
          catch (e) { reject(new Error(`JSON parse failed`)); }
        });
      });
      req.on('error', (err) => {
        if (attemptsLeft > 1) setTimeout(() => attemptFetch(attemptsLeft - 1), 1000);
        else reject(err);
      });
      req.on('timeout', () => {
        req.destroy();
        if (attemptsLeft > 1) setTimeout(() => attemptFetch(attemptsLeft - 1), 1000);
        else reject(new Error('Timeout'));
      });
      req.write(buf);
      req.end();
    };
    attemptFetch(retries);
  });
}

function dateRange(daysBack) {
  const today = new Date();
  const start = new Date(today);
  start.setDate(start.getDate() - daysBack);
  const fmt = d => d.toISOString().split('T')[0];
  return { start: fmt(start), end: fmt(today) };
}

const OCC_KEYWORDS = ['occupational medicine', 'occupational health', 'pre-employment', 'drug testing', 'medical surveillance', 'fit for duty'];

// ── SOURCE 1: SAM.GOV (with OpenRFPs fallback) ────────────────────────────────
async function fetchSAM() {
  if (!SAM_KEY) return [];
  const results = [];
  console.log('[SAM] Fetching...');
  for (const naics of ['621111']) {
    try {
      const today = new Date();
      const from = new Date(); from.setDate(from.getDate() - 90);
      const fmt = d => `${(d.getMonth()+1).toString().padStart(2,'0')}/${d.getDate().toString().padStart(2,'0')}/${d.getFullYear()}`;
      const url = `https://api.sam.gov/opportunities/v2/search?api_key=${SAM_KEY}&naicsCode=${naics}&limit=100&offset=0&active=Yes&postedFrom=${fmt(from)}&postedTo=${fmt(today)}`;
      const { status, data } = await httpsGet(url, 2, 25000);
      if (status !== 200) continue;
      for (const o of (data.opportunitiesData || [])) {
        results.push({
          id: 'SAM-' + (o.noticeId || Math.random()),
          source: 'SAM',
          title: o.title || 'Untitled',
          agency: o.fullParentPathName || 'Unknown',
          postedDate: o.postedDate || null,
          deadline: o.responseDeadLine || null,
        });
      }
    } catch (e) { console.error(`[SAM] Error: ${e.message}`); }
  }
  console.log(`[SAM] Total: ${results.length}`);
  return results;
}

// ── SOURCE 1B: OPENRFPS.ORG (SAM.GOV FALLBACK) ─────────────────────────────────
async function fetchOpenRFPsSAM() {
  console.log('[OpenRFPs-SAM] Fetching as SAM.gov fallback...');
  const results = [];
  try {
    const searchUrl = `https://www.openrfps.org/api/opportunities?keywords=occupational+health&limit=50`;
    const { status, data } = await httpsGet(searchUrl, 2, 25000);
    if (status === 200) {
      for (const o of (data.opportunities || data || [])) {
        results.push({
          id: 'OPENRFPS-SAM-' + (o.id || Math.random()),
          source: 'SAM',
          title: o.title || o.opportunity_name || 'RFP',
          agency: o.agency || 'Federal',
          postedDate: o.published_at || o.posted_date || null,
          deadline: o.deadline || o.due_date || null,
        });
      }
    }
  } catch (e) { console.error(`[OpenRFPs-SAM] Error: ${e.message}`); }
  console.log(`[OpenRFPs-SAM] Total: ${results.length}`);
  return results;
}

// ── SOURCE 2: USASPENDING ─────────────────────────────────────────────────────
async function fetchUSASpending() {
  console.log('[USASpending] Fetching contracts...');
  const { start, end } = dateRange(90);
  const results = [];
  try {
    const body = JSON.stringify({
      filters: { time_period: [{ start_date: start, end_date: end }], keywords: OCC_KEYWORDS, award_type_codes: ['A', 'B', 'C', 'D'] },
      fields: ['Award ID', 'Recipient Name', 'Award Amount', 'Awarding Agency', 'Description'],
      sort: 'Start Date', order: 'desc', limit: 50, page: 1
    });
    const { status, data } = await httpsPost('https://api.usaspending.gov/api/v2/search/spending_by_award/', body, 2, 25000);
    if (status === 200) {
      for (const r of (data.results || [])) {
        results.push({
          id: 'USA-' + (r['Award ID'] || Math.random()),
          source: 'USA',
          title: r['Description'] || 'Award',
          recipient: r['Recipient Name'] || '',
          awardAmount: r['Award Amount'] || 0,
        });
      }
    }
  } catch (e) { console.error(`[USASpending] Error: ${e.message}`); }
  console.log(`[USASpending] Total: ${results.length}`);
  return results;
}

// ── SOURCE 3: IDV ───────────────────────────────────────────────────────��────────
async function fetchIDV() {
  console.log('[IDV] Fetching IDIQ contracts...');
  const { start, end } = dateRange(180);
  const results = [];
  try {
    const body = JSON.stringify({
      filters: { time_period: [{ start_date: start, end_date: end }], keywords: OCC_KEYWORDS, award_type_codes: ['IDV_A', 'IDV_B'] },
      fields: ['Award ID', 'Recipient Name'],
      limit: 50, page: 1
    });
    const { status, data } = await httpsPost('https://api.usaspending.gov/api/v2/search/spending_by_award/', body, 2, 25000);
    if (status === 200) {
      for (const r of (data.results || [])) {
        results.push({ id: 'IDV-' + (r['Award ID'] || Math.random()), source: 'IDV' });
      }
    }
  } catch (e) { console.error(`[IDV] Error: ${e.message}`); }
  console.log(`[IDV] Total: ${results.length}`);
  return results;
}

// ── SOURCE 4: SUBAWARDS (with OpenRFPs fallback) ───────────────────────────────
async function fetchSubawards() {
  console.log('[Subawards] Fetching...');
  const { start, end } = dateRange(90);
  const results = [];
  try {
    const body = JSON.stringify({
      filters: { time_period: [{ start_date: start, end_date: end }], keywords: OCC_KEYWORDS },
      fields: ['Sub-Award ID', 'Sub-Awardee Name', 'Sub-Award Amount'],
      sort: 'Sub-Award Date', order: 'desc', limit: 50, page: 1
    });
    const { status, data } = await httpsPost('https://api.usaspending.gov/api/v2/search/spending_by_award/subawards/', body, 2, 25000);
    if (status === 200) {
      for (const r of (data.results || [])) {
        results.push({ id: 'SUB-' + (r['Sub-Award ID'] || Math.random()), source: 'SUB', recipient: r['Sub-Awardee Name'] || '' });
      }
    }
  } catch (e) { console.error(`[Subawards] Error: ${e.message}`); }
  console.log(`[Subawards] Total: ${results.length}`);
  return results;
}

// ── SOURCE 4B: OPENRFPS.ORG (SUBAWARDS FALLBACK) ────────────────────────────────
async function fetchOpenRFPsSubawards() {
  console.log('[OpenRFPs-Subawards] Fetching as fallback...');
  const results = [];
  try {
    const url = `https://www.openrfps.org/api/opportunities?keywords=subcontract+occupational&limit=30`;
    const { status, data } = await httpsGet(url, 2, 25000);
    if (status === 200) {
      for (const o of (data.opportunities || data || [])) {
        results.push({
          id: 'OPENRFPS-SUB-' + (o.id || Math.random()),
          source: 'SUB',
          title: o.title || 'Subcontract',
          recipient: o.awardee || '',
        });
      }
    }
  } catch (e) { console.error(`[OpenRFPs-Subawards] Error: ${e.message}`); }
  console.log(`[OpenRFPs-Subawards] Total: ${results.length}`);
  return results;
}

// ── SOURCE 5: GRANTS ──────────────────────────────────────────────────────────
async function fetchGrants() {
  console.log('[Grants] Fetching...');
  const { start, end } = dateRange(90);
  const results = [];
  try {
    const body = JSON.stringify({
      filters: { time_period: [{ start_date: start, end_date: end }], keywords: OCC_KEYWORDS, award_type_codes: ['02', '03', '04'] },
      limit: 50, page: 1
    });
    const { status, data } = await httpsPost('https://api.usaspending.gov/api/v2/search/spending_by_award/', body, 2, 25000);
    if (status === 200) results.push(...(data.results || []).map(r => ({ id: 'GRANT-' + Math.random(), source: 'GRANTS' })));
  } catch (e) { console.error(`[Grants] Error: ${e.message}`); }
  console.log(`[Grants] Total: ${results.length}`);
  return results;
}

// ── SOURCE 6: SBIR (with OpenRFPs fallback) ──────────────────────────────────
async function fetchSBIR() {
  console.log('[SBIR] Fetching...');
  const results = [];
  for (const kw of ['occupational', 'health']) {
    try {
      const { status, data } = await httpsGet(`https://api.www.sbir.gov/public/api/awards?keyword=${encodeURIComponent(kw)}&rows=20`, 2, 20000);
      if (status === 200) results.push(...(Array.isArray(data) ? data : []).map(a => ({ id: 'SBIR-' + (a.contract || Math.random()), source: 'SBIR' })));
    } catch (e) { console.error(`[SBIR] Error: ${e.message}`); }
  }
  console.log(`[SBIR] Total: ${results.length}`);
  return results;
}

// ── SOURCE 6B: OPENRFPS.ORG (SBIR FALLBACK) ──────────────────────────────────
async function fetchOpenRFPsSBIR() {
  console.log('[OpenRFPs-SBIR] Fetching as SBIR fallback...');
  const results = [];
  try {
    const url = `https://www.openrfps.org/api/opportunities?keywords=small+business+occupational+health&limit=30`;
    const { status, data } = await httpsGet(url, 2, 25000);
    if (status === 200) {
      for (const o of (data.opportunities || data || [])) {
        results.push({
          id: 'OPENRFPS-SBIR-' + (o.id || Math.random()),
          source: 'SBIR',
          title: o.title || 'SBIR',
          agency: o.agency || 'SBA',
        });
      }
    }
  } catch (e) { console.error(`[OpenRFPs-SBIR] Error: ${e.message}`); }
  console.log(`[OpenRFPs-SBIR] Total: ${results.length}`);
  return results;
}

// ── SOURCE 7: TANGO (with OpenRFPs fallback) ──────────────────────────────────
async function fetchTango() {
  if (!TANGO_KEY) return [];
  console.log('[Tango] Fetching...');
  const results = [];
  try {
    const { status, data } = await httpsGetH(`https://tango.makegov.com/api/opportunities/?search=occupational&limit=15`, { 'X-API-KEY': TANGO_KEY }, 2, 20000);
    if (status === 200) results.push(...(data.results || []).map(o => ({ id: 'TANGO-' + (o.id || Math.random()), source: 'TANGO' })));
  } catch (e) { console.error(`[Tango] Error: ${e.message}`); }
  console.log(`[Tango] Total: ${results.length}`);
  return results;
}

// ── SOURCE 7B: OPENRFPS.ORG (TANGO FALLBACK) ──────────────────────────────────
async function fetchOpenRFPsTango() {
  console.log('[OpenRFPs-Tango] Fetching as Tango fallback...');
  const results = [];
  try {
    const url = `https://www.openrfps.org/api/opportunities?keywords=occupational+health+services&limit=30`;
    const { status, data } = await httpsGet(url, 2, 25000);
    if (status === 200) {
      for (const o of (data.opportunities || data || [])) {
        results.push({
          id: 'OPENRFPS-TANGO-' + (o.id || Math.random()),
          source: 'TANGO',
          title: o.title || 'Opportunity',
          agency: o.agency || 'Unknown',
        });
      }
    }
  } catch (e) { console.error(`[OpenRFPs-Tango] Error: ${e.message}`); }
  console.log(`[OpenRFPs-Tango] Total: ${results.length}`);
  return results;
}

// ── SOURCE 8: FEDERAL REGISTER ────────────────────────────────────────────────
async function fetchFederalRegister() {
  console.log('[FedReg] Fetching...');
  const results = [];
  const from = new Date(); from.setDate(from.getDate() - 90);
  const fmt = d => d.toISOString().split('T')[0];
  try {
    const url = `https://www.federalregister.gov/api/v1/documents.json?per_page=20&conditions%5Bterm%5D=occupational&conditions%5Bpublication_date%5D%5Bgte%5D=${fmt(from)}`;
    const { status, data } = await httpsGet(url, 2, 20000);
    if (status === 200) results.push(...(data.results || []).map(d => ({ id: 'FEDREG-' + (d.document_number || Math.random()), source: 'FEDREG' })));
  } catch (e) { console.error(`[FedReg] Error: ${e.message}`); }
  console.log(`[FedReg] Total: ${results.length}`);
  return results;
}

// ── SOURCE 9: STATE SCRAPERS (with OpenRFPs fallback) ──────────────────────────
async function scrapePage(url) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const protocol = u.protocol === 'https:' ? https : http;
    const req = protocol.request({ hostname: u.hostname, path: u.pathname + u.search, method: 'GET', headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 20000 }, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => resolve(raw));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

async function fetchStateBids() {
  console.log('[States] Fetching...');
  let results = [];
  try {
    const html = await scrapePage('https://www.txsmartbuy.gov/esbd?keyword=health&status=Open');
    const $ = cheerio.load(html);
    $('table tbody tr').each((i, row) => {
      const cells = $(row).find('td');
      if (cells.length > 0) {
        results.push({ id: 'TX-' + Math.random(), source: 'STATE', state: 'TX' });
      }
    });
  } catch (e) { console.error(`[States] Error: ${e.message}`); }
  console.log(`[States] Total: ${results.length}`);
  return results;
}

// ── SOURCE 9B: OPENRFPS.ORG (STATE BIDS FALLBACK) ────────────────────────────
async function fetchOpenRFPsStates() {
  console.log('[OpenRFPs-States] Fetching as state bids fallback...');
  const results = [];
  try {
    const url = `https://www.openrfps.org/api/opportunities?keywords=occupational+health+state&limit=50`;
    const { status, data } = await httpsGet(url, 2, 25000);
    if (status === 200) {
      for (const o of (data.opportunities || data || [])) {
        results.push({
          id: 'OPENRFPS-STATE-' + (o.id || Math.random()),
          source: 'STATE',
          title: o.title || 'State Bid',
          state: o.state || 'N/A',
        });
      }
    }
  } catch (e) { console.error(`[OpenRFPs-States] Error: ${e.message}`); }
  console.log(`[OpenRFPs-States] Total: ${results.length}`);
  return results;
}

// ── ROUTES ────────────────────────────────────────────────────────────────────
app.get('/api/status', (req, res) => res.json({
  status: 'ok', version: '7.0.0',
  samKeyLoaded: !!SAM_KEY,
  tangoKeyLoaded: !!TANGO_KEY,
  sources: '9 primary + 6 OpenRFPs fallbacks'
}));

app.get('/api/sam', async (req, res) => {
  try {
    const primary = await fetchSAM();
    const fallback = primary.length === 0 ? await fetchOpenRFPsSAM() : [];
    res.json({ success: true, data: [...primary, ...fallback] });
  } catch (e) { res.status(500).json({ success: false, error: e.message, data: [] }); }
});

app.get('/api/usaspending', async (req, res) => {
  try { res.json({ success: true, data: await fetchUSASpending() }); }
  catch (e) { res.status(500).json({ success: false, error: e.message, data: [] }); }
});

app.get('/api/idv', async (req, res) => {
  try { res.json({ success: true, data: await fetchIDV() }); }
  catch (e) { res.status(500).json({ success: false, error: e.message, data: [] }); }
});

app.get('/api/subawards', async (req, res) => {
  try {
    const primary = await fetchSubawards();
    const fallback = primary.length === 0 ? await fetchOpenRFPsSubawards() : [];
    res.json({ success: true, data: [...primary, ...fallback] });
  } catch (e) { res.status(500).json({ success: false, error: e.message, data: [] }); }
});

app.get('/api/grants', async (req, res) => {
  try { res.json({ success: true, data: await fetchGrants() }); }
  catch (e) { res.status(500).json({ success: false, error: e.message, data: [] }); }
});

app.get('/api/sbir', async (req, res) => {
  try {
    const primary = await fetchSBIR();
    const fallback = primary.length === 0 ? await fetchOpenRFPsSBIR() : [];
    res.json({ success: true, data: [...primary, ...fallback] });
  } catch (e) { res.status(500).json({ success: false, error: e.message, data: [] }); }
});

app.get('/api/tango', async (req, res) => {
  try {
    const primary = await fetchTango();
    const fallback = primary.length === 0 ? await fetchOpenRFPsTango() : [];
    res.json({ success: true, data: [...primary, ...fallback] });
  } catch (e) { res.status(500).json({ success: false, error: e.message, data: [] }); }
});

app.get('/api/fedreg', async (req, res) => {
  try { res.json({ success: true, data: await fetchFederalRegister() }); }
  catch (e) { res.status(500).json({ success: false, error: e.message, data: [] }); }
});

app.get('/api/states', async (req, res) => {
  try {
    const primary = await fetchStateBids();
    const fallback = primary.length === 0 ? await fetchOpenRFPsStates() : [];
    res.json({ success: true, data: [...primary, ...fallback] });
  } catch (e) { res.status(500).json({ success: false, error: e.message, data: [] }); }
});

app.get('/api/openrfps', async (req, res) => {
  try {
    const [sam, sub, sbir, tango, states] = await Promise.allSettled([
      fetchOpenRFPsSAM(), fetchOpenRFPsSubawards(), fetchOpenRFPsSBIR(), fetchOpenRFPsTango(), fetchOpenRFPsStates()
    ]);
    const get = r => r.status === 'fulfilled' ? r.value : [];
    res.json({ success: true, data: [...get(sam), ...get(sub), ...get(sbir), ...get(tango), ...get(states)] });
  } catch (e) { res.status(500).json({ success: false, error: e.message, data: [] }); }
});

app.get('/api/opportunities', async (req, res) => {
  const [sam, usa, idv, sub, grants, sbir, tango, fedreg, states] = await Promise.allSettled([
    (async () => {
      const p = await fetchSAM();
      return p.length > 0 ? p : await fetchOpenRFPsSAM();
    })(),
    fetchUSASpending(),
    fetchIDV(),
    (async () => {
      const p = await fetchSubawards();
      return p.length > 0 ? p : await fetchOpenRFPsSubawards();
    })(),
    fetchGrants(),
    (async () => {
      const p = await fetchSBIR();
      return p.length > 0 ? p : await fetchOpenRFPsSBIR();
    })(),
    (async () => {
      const p = await fetchTango();
      return p.length > 0 ? p : await fetchOpenRFPsTango();
    })(),
    fetchFederalRegister(),
    (async () => {
      const p = await fetchStateBids();
      return p.length > 0 ? p : await fetchOpenRFPsStates();
    })()
  ]);
  
  const get = r => r.status === 'fulfilled' ? r.value : [];
  const all = [...get(sam), ...get(usa), ...get(idv), ...get(sub), ...get(grants), ...get(sbir), ...get(tango), ...get(fedreg), ...get(states)];
  
  res.json({
    success: true, total: all.length,
    samCount: get(sam).length, usaCount: get(usa).length,
    idvCount: get(idv).length, subCount: get(sub).length,
    grantsCount: get(grants).length, sbirCount: get(sbir).length,
    tangoCount: get(tango).length, fedregCount: get(fedreg).length,
    statesCount: get(states).length,
    data: all
  });
});

app.listen(PORT, () => {
  console.log(`\n✅ Occu-Med Backend v7.0.0 running on port ${PORT}`);
  console.log(`Hybrid Primary + OpenRFPs Fallback Mode`);
  console.log(`SAM Key: ${SAM_KEY ? '✓ SET' : '✗ NOT SET'}`);
  console.log(`Tango Key: ${TANGO_KEY ? '✓ SET' : '✗ NOT SET'}\n`);
});
