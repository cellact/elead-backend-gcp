'use strict';

const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, '..', 'data');
const DOMAINS_FILE = path.join(DATA_DIR, 'domains.json');
const LEADS_FILE = path.join(DATA_DIR, 'leads.json');

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function readJson(file, fallback) {
  ensureDataDir();
  if (!fs.existsSync(file)) {
    fs.writeFileSync(file, JSON.stringify(fallback, null, 2));
    return fallback;
  }
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function writeJson(file, value) {
  ensureDataDir();
  const tmp = `${file}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(value, null, 2));
  fs.renameSync(tmp, file);
}

function loadDomains() {
  return readJson(DOMAINS_FILE, {});
}

function loadLeads() {
  return readJson(LEADS_FILE, []);
}

function saveDomains(domains) {
  writeJson(DOMAINS_FILE, domains);
}

function saveLeads(leads) {
  writeJson(LEADS_FILE, leads);
}

function getDomain(domain) {
  const domains = loadDomains();
  return domains[domain] || null;
}

function linkDomain({ domain, spAddress, semaphoreInteractor, secondLevelInteractor }) {
  const domains = loadDomains();
  const existing = domains[domain] || {};
  domains[domain] = {
    ...existing,
    domain,
    spAddress: spAddress.toLowerCase(),
    linkedAt: existing.linkedAt || new Date().toISOString(),
  };
  if (semaphoreInteractor) {
    domains[domain].semaphoreInteractor = semaphoreInteractor;
  }
  if (secondLevelInteractor) {
    domains[domain].secondLevelInteractor = secondLevelInteractor;
  }
  saveDomains(domains);
  return domains[domain];
}

function findLead({ label, domain }) {
  const leads = loadLeads();
  const needle = String(label || '').toLowerCase();
  if (domain) {
    return leads.find((row) => row.label === needle && row.domain === domain) || null;
  }
  return leads.find((row) => row.label === needle) || null;
}

function addLead(lead) {
  const leads = loadLeads();
  leads.push(lead);
  saveLeads(leads);
  return lead;
}

function listDomains() {
  return Object.values(loadDomains());
}

function leadsForDomain(domain) {
  return loadLeads().filter((lead) => lead.domain === domain);
}

function leadsForSp(spAddress) {
  const sp = String(spAddress || '').toLowerCase();
  return loadLeads().filter((lead) => lead.spAddress === sp);
}

function updateLeadStatus(domain, label, status) {
  const leads = loadLeads();
  const lead = leads.find((row) => row.domain === domain && row.label === label);
  if (!lead) return null;
  lead.status = status;
  lead.updatedAt = new Date().toISOString();
  saveLeads(leads);
  return lead;
}

module.exports = {
  getDomain,
  listDomains,
  linkDomain,
  addLead,
  findLead,
  leadsForDomain,
  leadsForSp,
  updateLeadStatus,
};
