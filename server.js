const express = require('express');
const cors = require('cors');
const https = require('https');
const cheerio = require('cheerio');
const safeFetch = globalThis.fetch ? globalThis.fetch.bind(globalThis) : require('node-fetch');

require('dotenv').config({ path: './Env' });

const app = express();
const PORT = process.env.PORT || 3001;
const SAM_KEY = process.env.SAM_API_KEY || '';

app.use(cors({ origin: '*' }));
app.use(express.json());

app.get('/api/health', (_req, res) => {
  res.status(200).json({ ok: true, service: 'occu-med-backend', awake: true });
});

app.head('/api/health', (_req, res) => {
  res.status(200).end();
});

// Render keep-awake endpoint added. Existing application code should remain below this line.
