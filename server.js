const express = require('express');
const cors = require('cors');
const https = require('https');

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

async function usaSpendingSearch({ awardTypeCodes, source, noticeType, baseType, daysBack = 90, label = '' }) {
  const { start, end } = dateRange(daysBack);
  const seen = new Set();
  const results = [];

  for (const batch of OCC_KEYWORDS_BATCHES) {
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
async function fetchUSASpending(daysBack = 90) {
  console.log('\nFetching USASpending contract awards...');
  return usaSpendingSearch({
    awardTypeCodes: ['A', 'B', 'C', 'D'],
    source: 'USA', noticeType: 'Contract Award', baseType: 'Award',
    daysBack, label: 'USASpending'
  });
}

// ── Source 3: IDV / IDIQ contracts ────────────────────────────────────────────
// Indefinite Delivery Vehicles — umbrella contracts where occ med task orders get issued.
// This is where most ongoing occ med federal work actually lives.
async function fetchIDV(daysBack = 180) {
  console.log('\nFetching IDV/IDIQ contracts...');
  return usaSpendingSearch({
    awardTypeCodes: ['IDV_A', 'IDV_B', 'IDV_B_A', 'IDV_B_B', 'IDV_B_C', 'IDV_C', 'IDV_D', 'IDV_E'],
    source: 'IDV', noticeType: 'IDIQ / Indefinite Delivery Vehicle', baseType: 'IDV',
    daysBack, label: 'IDV'
  });
}

// ── Source 4: Federal Subcontracts ────────────────────────────────────────────
// Prime contractors subbing out occ med work — Concentra, Leidos, SAIC, etc.
// Shows who the real delivery chain is and where Occu-Med could fit as a sub.
async function fetchSubawards(daysBack = 90) {
  console.log('\nFetching federal subawards...');
  const { start, end } = dateRange(daysBack);
  const seen = new Set();
  const results = [];

  for (const batch of OCC_KEYWORDS_BATCHES.slice(0, 2)) {
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
async function fetchGrants(daysBack = 90) {
  console.log('\nFetching federal grants/assistance...');
  return usaSpendingSearch({
    awardTypeCodes: ['02', '03', '04', '05'],
    source: 'GRANTS', noticeType: 'Federal Grant / Assistance', baseType: 'Grant',
    daysBack, label: 'Grants'
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

// ── Routes ────────────────────────────────────────────────────────────────────
app.get('/api/status', (req, res) => res.json({
  status: 'ok', version: '4.0.0', samKeyLoaded: !!SAM_KEY,
  sources: ['SAM.gov', 'USASpending (Contracts)', 'USASpending (IDV/IDIQ)', 'USASpending (Subawards)', 'USASpending (Grants)', 'SBIR.gov'],
  endpoints: ['/api/sam', '/api/usaspending', '/api/idv', '/api/subawards', '/api/grants', '/api/sbir', '/api/opportunities']
}));

app.get('/api/sam',         async (req, res) => { try { res.json({ success:true, data: await fetchSAM() }); }         catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/usaspending', async (req, res) => { try { res.json({ success:true, data: await fetchUSASpending(parseInt(req.query.days)||90) }); } catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/idv',         async (req, res) => { try { res.json({ success:true, data: await fetchIDV(parseInt(req.query.days)||180) }); }       catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/subawards',   async (req, res) => { try { res.json({ success:true, data: await fetchSubawards(parseInt(req.query.days)||90) }); }   catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/grants',      async (req, res) => { try { res.json({ success:true, data: await fetchGrants(parseInt(req.query.days)||90) }); }      catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });
app.get('/api/sbir',        async (req, res) => { try { res.json({ success:true, data: await fetchSBIR() }); }                                    catch(e) { res.status(500).json({ success:false, error:e.message, data:[] }); } });

app.get('/api/opportunities', async (req, res) => {
  const days = parseInt(req.query.days) || 90;
  const [samR, usaR, idvR, subR, grantR, sbirR] = await Promise.allSettled([
    fetchSAM(), fetchUSASpending(days), fetchIDV(days), fetchSubawards(days), fetchGrants(days), fetchSBIR()
  ]);
  const get = r => r.status === 'fulfilled' ? r.value : [];
  const err = (lbl, r) => r.status === 'rejected' ? [`${lbl}: ${r.reason?.message}`] : [];
  const all = [...get(samR),...get(usaR),...get(idvR),...get(subR),...get(grantR),...get(sbirR)];
  res.json({
    success: true, total: all.length,
    samCount: get(samR).length, usaCount: get(usaR).length,
    idvCount: get(idvR).length, subCount: get(subR).length,
    grantsCount: get(grantR).length, sbirCount: get(sbirR).length,
    errors: [...err('SAM',samR),...err('USASpending',usaR),...err('IDV',idvR),...err('Subawards',subR),...err('Grants',grantR),...err('SBIR',sbirR)],
    data: all
  });
});

app.listen(PORT, () => {
  console.log(`\nOccu-Med Backend v4.0 running on port ${PORT}`);
  console.log(`SAM API key: ${SAM_KEY ? SAM_KEY.slice(0,12)+'...' : 'NOT SET'}`);
  console.log(`Sources: SAM | Contracts | IDV/IDIQ | Subawards | Grants | SBIR\n`);
});
