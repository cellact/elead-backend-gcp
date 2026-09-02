'use strict';

require('dotenv').config({ override: true });

const crypto = require('crypto');
const { ethers } = require('ethers');
const functions = require('@google-cloud/functions-framework');
const store = require('./lib/store');
const { CHAIN_ID, getSdk, withLinkedSemaphore, statusKey } = require('./lib/sdk');
const { verifyProof } = require('@semaphore-protocol/proof');

const DOMAIN_RE = /^[a-z0-9-]+$/;
const ZERO = '0x0000000000000000000000000000000000000000';

function setCors(res) {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');
  res.set('Access-Control-Max-Age', '3600');
}

function pathname(req) {
  const raw = req.path || (req.url || '/').split('?')[0] || '/';
  return raw.replace(/\/+$/, '') || '/';
}

function normalizeDomain(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\.global$/, '');
}

function publicBaseUrl() {
  return (process.env.PUBLIC_BASE_URL || 'http://127.0.0.1:5173').replace(/\/$/, '');
}

function buildClaimUrl(userSecret, label) {
  const params = new URLSearchParams({
    secret: String(userSecret),
    label: String(label),
  });
  if (process.env.CLAIM_DEV === 'true') {
    params.set('dev', 'true');
  }
  if (process.env.CLAIM_DEV === 'false') {
    params.set('dev', 'false');
  }
  const claimPage = `${publicBaseUrl()}/claim?${params.toString()}`;
  const provider = process.env.CLAIM_PROVIDER || 'Elead';
  return `arnacon://install?url=${encodeURIComponent(claimPage)}&provider=${encodeURIComponent(provider)}`;
}

function allocateLabel() {
  return `l${crypto.randomBytes(4).toString('hex')}`;
}

function jsonError(res, status, message) {
  return res.status(status).json({ error: message });
}

async function handleConfig(_req, res) {
  const sdk = await getSdk();
  const artifactNames = ['SecondLevelInteractor', 'ArnaconResolver'];
  const artifacts = {};
  for (const name of artifactNames) {
    const artifact = sdk.factoryLoader.getArtifact(name);
    if (!artifact || !artifact.bytecode) {
      throw new Error(`missing bytecode for ${name}`);
    }
    artifacts[name] = { bytecode: artifact.bytecode };
  }
  return res.json({
    backendAddress: sdk.getSignerAddress(),
    chainId: CHAIN_ID,
    contracts: sdk.getAllContractAddresses(),
    artifacts,
  });
}

async function handleLinkDomain(req, res) {
  const domain = normalizeDomain(req.body && req.body.domain);
  const spAddress = String((req.body && req.body.spAddress) || '').trim();
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  if (!ethers.utils.isAddress(spAddress) || spAddress.toLowerCase() === ZERO) {
    return jsonError(res, 400, 'Invalid spAddress');
  }
  const linked = store.linkDomain({
    domain,
    spAddress,
    secondLevelInteractor: req.body.secondLevelInteractor,
    semaphoreInteractor: req.body.semaphoreInteractor,
  });
  return res.json(linked);
}

async function handleGenerateLeadQR(req, res) {
  const domain = normalizeDomain(req.body && req.body.domain);
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  const linked = store.getDomain(domain);
  if (!linked) {
    return jsonError(res, 400, `Domain ${domain} is not linked. Call /linkDomain after makeController.`);
  }

  const label = allocateLabel();
  const inserted = await withLinkedSemaphore(linked, (sdk) => sdk.insertCommitment(label));
  const userSecret = inserted.userSecret;

  const lead = store.addLead({
    domain,
    label,
    userSecret,
    commitment: inserted.commitment,
    insertTx: inserted.transactionHash,
    spAddress: linked.spAddress,
    status: 'unclaimed',
    createdAt: new Date().toISOString(),
  });

  const url = buildClaimUrl(userSecret, label);

  return res.json({
    url,
    label: lead.label,
    domain: lead.domain,
  });
}

async function handleListDomains(req, res) {
  const sp = String((req.query && req.query.sp) || '').trim().toLowerCase();
  const rows = store.listDomains();
  const domains = sp
    ? rows.filter((row) => row.spAddress === sp)
    : rows;
  return res.json({
    domains: domains.map((row) => ({
      domain: row.domain,
      spAddress: row.spAddress,
      linkedAt: row.linkedAt,
    })),
  });
}

async function handleFetchLeads(req, res) {
  const sp = String((req.query && req.query.sp) || (req.body && req.body.sp) || '').trim();
  const domainRaw = (req.query && req.query.domain) || (req.body && req.body.domain);
  const domain = domainRaw ? normalizeDomain(domainRaw) : '';

  let rows;
  if (domain) {
    if (!DOMAIN_RE.test(domain)) {
      return jsonError(res, 400, 'Invalid domain');
    }
    rows = store.leadsForDomain(domain);
  } else if (ethers.utils.isAddress(sp)) {
    rows = store.leadsForSp(sp);
  } else {
    return jsonError(res, 400, 'domain or sp is required');
  }

  const sdk = await getSdk();
  const leads = [];
  for (const row of rows) {
    let chainStatus = '';
    try {
      chainStatus = await sdk.getRecord(statusKey(row.label), row.domain);
    } catch (err) {
      chainStatus = '';
    }
    leads.push({
      domain: row.domain,
      label: row.label,
      status: chainStatus || row.status,
      storeStatus: row.status,
      createdAt: row.createdAt,
      fullName: `${row.label}.${row.domain}.global`,
    });
  }
  return res.json({ domain: domain || null, sp: sp || null, leads });
}

async function handleEnsureSemaphore(req, res) {
  const domain = normalizeDomain(req.body && req.body.domain);
  const secondLevelInteractor = String(
    (req.body && req.body.secondLevelInteractor) || '',
  ).trim();
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  if (!ethers.utils.isAddress(secondLevelInteractor) || secondLevelInteractor.toLowerCase() === ZERO) {
    return jsonError(res, 400, 'Invalid secondLevelInteractor');
  }
  const linked = store.getDomain(domain);
  if (!linked) {
    return jsonError(res, 400, `Domain ${domain} is not linked`);
  }

  if (
    linked.semaphoreInteractor &&
    linked.secondLevelInteractor &&
    linked.secondLevelInteractor.toLowerCase() === secondLevelInteractor.toLowerCase()
  ) {
    return res.json({
      domain,
      secondLevelInteractor,
      semaphoreInteractor: linked.semaphoreInteractor,
      needsGrant: false,
      linked,
    });
  }

  const sdk = await getSdk();
  const contracts = sdk.deploymentManager.contracts;
  delete contracts.SemaphoreInteractor;
  delete contracts.SemaphoreInteractor_DeployBlock;

  const deployed = await sdk.deploySemaphoreInteractor(secondLevelInteractor);
  const semaphoreInteractor = deployed.semaphoreInteractor;
  const saved = store.linkDomain({
    domain,
    spAddress: linked.spAddress,
    secondLevelInteractor,
    semaphoreInteractor,
  });
  return res.json({
    domain,
    secondLevelInteractor,
    semaphoreInteractor,
    needsGrant: true,
    linked: saved,
  });
}

function resolveOwner(body) {
  const owner = body && body.owner;
  if (owner && ethers.utils.isAddress(owner) && owner.toLowerCase() !== ZERO) {
    return ethers.utils.getAddress(owner);
  }
  const id = String((body && body.web3identity) || '').trim().toLowerCase();
  const suffix = '.arnacon.global';
  let uid = id;
  if (id.endsWith(suffix)) {
    uid = id.slice(0, -suffix.length);
  }
  if (/^0x[a-f0-9]{40}$/.test(uid)) {
    return ethers.utils.getAddress(uid);
  }
  if (/^[a-f0-9]{40}$/.test(uid)) {
    return ethers.utils.getAddress(`0x${uid}`);
  }
  if (ethers.utils.isAddress(id) && id.toLowerCase() !== ZERO) {
    return ethers.utils.getAddress(id);
  }
  return null;
}

async function handleGetGroupMembers(req, res) {
  const domain = normalizeDomain((req.query && req.query.domain) || (req.body && req.body.domain));
  const label = String((req.query && req.query.label) || (req.body && req.body.label) || '')
    .trim()
    .toLowerCase();
  let linked = null;
  if (DOMAIN_RE.test(domain)) {
    linked = store.getDomain(domain);
  } else if (label) {
    const lead = store.findLead({ label });
    if (lead) linked = store.getDomain(lead.domain);
  }
  if (!linked) {
    return jsonError(res, 400, 'domain or label required to select Semaphore group');
  }

  const payload = await withLinkedSemaphore(linked, async (sdk) => {
    const si = sdk.semaphore._getSemaphoreInteractor();
    const [commitments, groupId, scope, merkleTreeRoot, memberCount] = await Promise.all([
      sdk.getGroupMembers(),
      si.groupId(),
      si.REGISTER_SCOPE(),
      si.getMerkleRoot(),
      si.getMemberCount(),
    ]);
    return {
      commitments,
      scope: scope.toString(),
      groupId: groupId.toString(),
      merkleTreeRoot: merkleTreeRoot.toString(),
      memberCount: memberCount.toString(),
      domain: linked.domain,
    };
  });
  return res.json(payload);
}

async function handleActivateWithProof(req, res) {
  const body = req.body || {};
  const proof = body.proof;
  const label = String(body.label || '').trim().toLowerCase();
  const domainHint = body.domain ? normalizeDomain(body.domain) : '';
  if (!proof || !label) {
    return jsonError(res, 400, 'proof and label are required');
  }
  const owner = resolveOwner(body);
  if (!owner) {
    return jsonError(
      res,
      400,
      'owner 0x address required (or web3identity whose uid is an address)',
    );
  }
  const lead = store.findLead({ label, domain: domainHint || undefined });
  if (!lead) {
    return jsonError(res, 404, 'Lead not found');
  }
  const linked = store.getDomain(lead.domain);
  if (!linked) {
    return jsonError(res, 400, 'Domain is not linked');
  }

  let jsProofValid;
  try {
    jsProofValid = await verifyProof(proof);
  } catch (err) {
    return jsonError(res, 400, `Invalid client proof: ${err.message}`);
  }
  if (!jsProofValid) {
    return jsonError(res, 400, 'Semaphore JS verifyProof returned false');
  }

  const parentName = lead.domain;
  const result = await withLinkedSemaphore(linked, async (sdk) => {
    const si = sdk.semaphore._getSemaphoreInteractor();
    const tx = await sdk.semaphore.deploymentManager.executeTransaction(
      si,
      'registerSubnodeWithProof',
      [proof, owner, parentName],
      'Registering subnode with pre-generated ZK proof',
    );
    return { transactionHash: tx.hash };
  });

  store.updateLeadStatus(lead.domain, lead.label, 'claimed');
  const fullName = `${lead.label}.${lead.domain}.global`;
  const clientUrl = (process.env.ELEAD_HTML_URL || 'https://cellact.github.io/Elead-HTML/').replace(
    /\/$/,
    '',
  );
  console.log('[elead] activate notify (stub)', {
    owner,
    label: lead.label,
    name: fullName,
    clientUrl,
    transactionHash: result.transactionHash,
  });

  return res.json({
    label: lead.label,
    owner,
    name: fullName,
    domain: lead.domain,
    transactionHash: result.transactionHash,
    clientUrl,
  });
}

async function handleSetLeadStatus(req, res) {
  const domain = normalizeDomain(req.body && req.body.domain);
  const label = String((req.body && req.body.label) || '').trim().toLowerCase();
  const status = String((req.body && req.body.status) || '').trim();
  if (!DOMAIN_RE.test(domain) || !label || !status) {
    return jsonError(res, 400, 'domain, label, and status are required');
  }
  const linked = store.getDomain(domain);
  if (!linked) {
    return jsonError(res, 400, `Domain ${domain} is not linked`);
  }
  const existing = store.leadsForDomain(domain).find((row) => row.label === label);
  if (!existing) {
    return jsonError(res, 404, 'Lead not found');
  }

  const sdk = await getSdk();
  await sdk.createRecord(statusKey(label), status, domain, linked.spAddress);
  const updated = store.updateLeadStatus(domain, label, status);
  return res.json({
    domain,
    label,
    status,
    fullName: `${label}.${domain}.global`,
    updatedAt: updated && updated.updatedAt,
  });
}

async function dispatch(req, res) {
  const path = pathname(req);
  const action = req.body && req.body.action;

  if (req.method === 'GET' && (path === '/config' || path.endsWith('/config'))) {
    return handleConfig(req, res);
  }
  if (req.method === 'GET' && path.endsWith('/domains')) {
    return handleListDomains(req, res);
  }
  if (req.method === 'POST' && (action === 'linkDomain' || path.endsWith('/linkDomain'))) {
    return handleLinkDomain(req, res);
  }
  if (req.method === 'POST' && (action === 'ensureSemaphore' || path.endsWith('/ensureSemaphore'))) {
    return handleEnsureSemaphore(req, res);
  }
  if (req.method === 'GET' && path.endsWith('/group-members')) {
    return handleGetGroupMembers(req, res);
  }
  if (req.method === 'POST' && (action === 'generateLeadQR' || path.endsWith('/generateLeadQR'))) {
    return handleGenerateLeadQR(req, res);
  }
  if (
    req.method === 'POST' &&
    (action === 'activateWithProof' || path.endsWith('/activateWithProof'))
  ) {
    return handleActivateWithProof(req, res);
  }
  if (
    (req.method === 'GET' && path.endsWith('/fetchLeads')) ||
    (req.method === 'POST' && (action === 'fetchLeads' || path.endsWith('/fetchLeads')))
  ) {
    return handleFetchLeads(req, res);
  }
  if (req.method === 'POST' && (action === 'setLeadStatus' || path.endsWith('/setLeadStatus'))) {
    return handleSetLeadStatus(req, res);
  }
  if (req.method === 'GET' && (path === '/' || path.endsWith('/health'))) {
    return res.json({ service: 'elead', ok: true, chainId: CHAIN_ID });
  }
  return jsonError(res, 404, 'Not found');
}

functions.http('elead', async (req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') {
    return res.status(204).send('');
  }
  try {
    await dispatch(req, res);
  } catch (err) {
    console.error('[elead]', err);
    if (!res.headersSent) {
      res.status(500).json({ error: err.message || 'Internal error' });
    }
  }
});
