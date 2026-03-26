const axios = require('axios');

const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // milliseconds
const TIMEOUT = 25000; // milliseconds

const endpoints = {
    sam: 'https://api.sam.gov',
    usaspending: 'https://api.usaspending.gov',
    idv: 'https://api.usaspending.gov/api/v2/idv',
    subawards: 'https://api.usaspending.gov/api/v2/subawards',
    grants: 'https://api.usaspending.gov/api/v2/grants',
    sbir: 'https://api.usaspending.gov/api/v2/sbir',
    tango: 'https://api.tango.me',
    federalRegister: 'https://www.federalregister.gov/api/v1/documents',
    states: 'https://api.example.com/states',
    opportunities: '/api/opportunities'
};

async function fetchWithRetry(url, options, retries = MAX_RETRIES) {
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            const response = await axios.get(url, {...options, timeout: TIMEOUT});
            return response.data;
        } catch (error) {
            if (error.response && error.response.status === 429) {
                console.warn(`Rate limit exceeded. Attempt ${attempt + 1} of ${retries}.`);
                await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
            } else {
                console.error(`Error fetching ${url}:`, error);
                break;
            }
        }
    }
    throw new Error(`Failed to fetch ${url} after ${MAX_RETRIES} attempts.`);
}

async function fetchData() {
    try {
        const samData = await fetchWithRetry(endpoints.sam);
        // Handle SAM data

        const usaspendingData = await fetchWithRetry(endpoints.usaspending);
        // Handle USASpending data

        // Add additional fetching logic for IDV, Subawards, Grants, SBIR, Tango, Federal Register, and States endpoints
        // Include flexible selectors and sorting for Subawards with Sub-Award Date field

        console.log('Fetch completed successfully.');
    } catch (error) {
        console.error('An error occurred while fetching data:', error);
    }
}

fetchData();