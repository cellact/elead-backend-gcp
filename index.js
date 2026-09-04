'use strict';

require('dotenv').config({ override: false });

const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { ethers } = require('ethers');
const functions = require('@google-cloud/functions-framework');
const { Firestore, FieldValue } = require('@google-cloud/firestore');
const ArnaconSDK = require('arnacon-sdk');
const { verifyProof } = require('@semaphore-protocol/proof');

const CHAIN_ID = parseInt(process.env.CHAIN_ID || '11155111', 10);
const DEFAULT_RPC =
  CHAIN_ID === 137
    ? 'https://polygon-bor-rpc.publicnode.com'
    : 'https://ethereum-sepolia-rpc.publicnode.com';
const SEPOLIA_VERIFIER = '0xd66efA909cA5161BFDCE39058f61b3c6186263B7';
const ELEAD_PRODUCT_TYPE = process.env.ELEAD_PRODUCT_TYPE || 'Elead';
const ELEAD_DOMAINS_COLLECTION = process.env.ELEAD_DOMAINS_COLLECTION || 'elead_domains';
const DEFAULT_NOTIFICATION_CENTER =
  'https://europe-west1-arnacon-production-gcp.cloudfunctions.net/notification-center';
const DEFAULT_POLYGON_RPC = 'https://polygon-bor-rpc.publicnode.com';
const DEFAULT_POLYGON_ENS_REGISTRY = '0x6e0A65396233B8A29d76cA2DDd87bc5F19A82c36';
const DEFAULT_POLYGON_NAME_WRAPPER = '0xCAb1585C37118d066Bc3AD79919B4CAE5cd42BC2';
const DEFAULT_POLYGON_TEMP_NAME_WRAPPER = '0xd1f0B5e182Ef58EF7822947458F6E125635a6645';
const DEFAULT_POLYGON_EMAIL_NAME_WRAPPER = '0x42Ec624E65970D338f823EBC7EE859c5BB607462';

const DATA_DIR =
  process.env.K_SERVICE || process.env.FUNCTION_TARGET
    ? path.join(os.tmpdir(), 'elead-data')
    : path.join(__dirname, 'data');
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

const store = {
  getDomain,
  listDomains,
  linkDomain,
  addLead,
  findLead,
  leadsForDomain,
  leadsForSp,
  updateLeadStatus,
};

let firestore;
function getFirestore() {
  if (!firestore) {
    firestore = new Firestore();
  }
  return firestore;
}

function domainDoc(domain) {
  return getFirestore().collection(ELEAD_DOMAINS_COLLECTION).doc(domain);
}

async function loadDomainRecord(domain) {
  try {
    const snap = await domainDoc(domain).get();
    if (!snap.exists) {
      return null;
    }
    return snap.data() || null;
  } catch (err) {
    console.warn('[elead] firestore get domain failed', err.message);
    return null;
  }
}

async function saveDomainRecord(domain, patch) {
  const payload = {
    domain,
    chainId: CHAIN_ID,
    updatedAt: FieldValue.serverTimestamp(),
  };
  for (const [key, value] of Object.entries(patch || {})) {
    if (value === undefined || value === null || value === '') {
      continue;
    }
    payload[key] = value;
  }
  await domainDoc(domain).set(payload, { merge: true });
}

const SIGN_MAX_SKEW_MS = 5 * 60 * 1000;

async function linkedSpAddress(domain) {
  const record = await loadDomainRecord(domain);
  if (record && record.spAddress && ethers.utils.isAddress(record.spAddress)) {
    return String(record.spAddress).toLowerCase();
  }
  const disk = store.getDomain(domain);
  if (disk && disk.spAddress) {
    return String(disk.spAddress).toLowerCase();
  }
  return null;
}

function inboxSignMessage(kind, domain, inboxLabel, timestamp) {
  const header = kind === 'routing' ? 'elead-inbox-routing' : 'elead-inbox';
  return `${header}\n${domain}\n${inboxLabel}\n${timestamp}`;
}

function requireTimestamp(raw) {
  const ts = Number(raw);
  if (!Number.isFinite(ts)) {
    throw new Error('Invalid timestamp');
  }
  if (Math.abs(Date.now() - ts) > SIGN_MAX_SKEW_MS) {
    throw new Error('Signature timestamp expired');
  }
  return ts;
}

async function walletOwns2ld(wallet, domain) {
  const resolved = await resolveDomainOnChain(domain);
  const sdk = await getSdk();
  const grcAddr = sdk.getAllContractAddresses().GlobalRegistrarController;
  if (!grcAddr || !ethers.utils.isAddress(grcAddr)) {
    throw new Error('GlobalRegistrarController missing from SDK addresses');
  }
  const grc = new ethers.Contract(
    grcAddr,
    ['function get2LDControllerFor(address) view returns (address)'],
    sdk.getProvider(),
  );
  const slc = await grc.get2LDControllerFor(wallet);
  if (!slc || slc.toLowerCase() === ZERO) {
    return false;
  }
  return slc.toLowerCase() === resolved.secondLevelController.toLowerCase();
}

async function requireSpSignature({ domain, inboxLabel, timestamp, signature, kind }) {
  const ts = requireTimestamp(timestamp);
  const message = inboxSignMessage(kind, domain, inboxLabel, ts);
  let recovered;
  try {
    recovered = ethers.utils.verifyMessage(message, signature);
  } catch (_err) {
    throw new Error('Invalid signature');
  }
  const recoveredLc = recovered.toLowerCase();
  const linked = await linkedSpAddress(domain);
  if (linked && recoveredLc === linked) {
    return { sp: recoveredLc, timestamp: ts };
  }
  let owns = false;
  try {
    owns = await walletOwns2ld(recoveredLc, domain);
  } catch (err) {
    console.warn('[elead] 2ld ownership check failed', err.message);
  }
  if (!owns) {
    console.warn('[elead] inbox sig rejected', {
      domain,
      kind,
      inboxLabel,
      recovered: recoveredLc,
      linked,
    });
    throw new Error(
      linked
        ? `Signer ${recoveredLc} is not the linked SP ${linked}`
        : `${domain}.global is not linked and ${recoveredLc} does not own the 2LD`,
    );
  }
  try {
    await saveDomainRecord(domain, { spAddress: recoveredLc });
    store.linkDomain({ domain, spAddress: recoveredLc });
  } catch (err) {
    console.warn('[elead] persist SP after 2ld check failed', err.message);
  }
  return { sp: recoveredLc, timestamp: ts };
}

function inboxFullName(domain, label) {
  return `${label}.${domain}.global`;
}

async function upsertInbox(domain, inbox) {
  await saveDomainRecord(domain, {
    inboxMirror: {
      label: inbox.label,
      fullName: inbox.fullName,
      status: inbox.status || null,
      updatedAt: new Date().toISOString(),
    },
  });
  return inbox;
}

function studioDomainFrom(value) {
  const raw = String(value || '')
    .trim()
    .toLowerCase();
  if (!raw) {
    return '';
  }
  const withoutGlobal = raw.replace(/\.global$/, '');
  const parts = withoutGlobal.split('.').filter(Boolean);
  if (parts.length === 0) {
    return '';
  }
  return parts[parts.length - 1];
}

const SI_TEXT_KEY = 'semaphoreInteractor';
const INBOX_LIST_KEY = 'inboxList';
const PUBLIC_RESOLVER_TEXT_ABI = [
  'function text(bytes32 node, string key) view returns (string)',
];
const SLI_SET_TEXT_ABI = [
  'function setText(string name, string key, string value)',
];

async function publicResolverText(sdk, domain, key) {
  const resolverAddr = sdk.getAllContractAddresses().PublicResolver;
  if (!resolverAddr || resolverAddr.toLowerCase() === ZERO) {
    return '';
  }
  const resolver = new ethers.Contract(
    resolverAddr,
    PUBLIC_RESOLVER_TEXT_ABI,
    sdk.getProvider(),
  );
  const raw = await resolver.text(ethers.utils.namehash(`${domain}.global`), key);
  return String(raw || '').trim();
}

async function semaphoreInteractorFromEns(sdk, domain) {
  const value = await publicResolverText(sdk, domain, SI_TEXT_KEY);
  if (!value || !ethers.utils.isAddress(value) || value.toLowerCase() === ZERO) {
    return null;
  }
  return ethers.utils.getAddress(value);
}

function normalizeInboxStatus(value) {
  return String(value || '')
    .trim()
    .toLowerCase() === 'inactive'
    ? 'inactive'
    : 'active';
}

function serializeInboxList(rows) {
  return JSON.stringify(
    rows.map((row) => ({
      label: row.label,
      fullName: row.fullName,
      status: normalizeInboxStatus(row.status),
    })),
  );
}

function parseInboxList(raw) {
  if (!raw) {
    return [];
  }
  let data;
  try {
    data = JSON.parse(raw);
  } catch (_err) {
    return [];
  }
  if (!Array.isArray(data)) {
    return [];
  }
  const out = [];
  const seen = new Set();
  for (const row of data) {
    let label = '';
    let fullName = '';
    let status = 'active';
    if (typeof row === 'string') {
      fullName = row.trim().toLowerCase();
      if (!fullName.endsWith('.global')) {
        continue;
      }
      const withoutGlobal = fullName.replace(/\.global$/, '');
      const parts = withoutGlobal.split('.').filter(Boolean);
      if (parts.length < 1) {
        continue;
      }
      label = parts[0];
    } else if (row && typeof row === 'object') {
      label = String(row.label || '')
        .trim()
        .toLowerCase();
      fullName = String(row.fullName || '')
        .trim()
        .toLowerCase();
      if (!fullName && label) {
        fullName = `${label}.${String(row.domain || '').trim().toLowerCase()}.global`;
      }
      status = normalizeInboxStatus(row.status);
    }
    if (!label || !fullName || seen.has(label)) {
      continue;
    }
    seen.add(label);
    out.push({ label, fullName, status });
  }
  return out;
}

async function readInboxListFromEns(sdk, domain) {
  try {
    return parseInboxList(await publicResolverText(sdk, domain, INBOX_LIST_KEY));
  } catch (err) {
    console.warn('[elead] inboxList text read failed', err.message);
    return [];
  }
}

async function writeInboxListToEns(sdk, sliAddr, domain, rows) {
  const sli = new ethers.Contract(sliAddr, SLI_SET_TEXT_ABI, sdk.signer);
  const tx = await sli.setText(domain, INBOX_LIST_KEY, serializeInboxList(rows));
  await tx.wait();
  return parseInboxList(serializeInboxList(rows));
}

async function appendClaimedInboxToEns(sdk, sliAddr, domain, label, fullName) {
  const current = await readInboxListFromEns(sdk, domain);
  const next = current.filter((row) => row.label !== label);
  next.push({
    label,
    fullName: String(fullName).toLowerCase(),
    status: 'active',
  });
  return writeInboxListToEns(sdk, sliAddr, domain, next);
}

let cached;
let walletChain = Promise.resolve();

function withWalletQueue(fn) {
  const run = walletChain.then(fn, fn);
  walletChain = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}

function normalizePrivateKey(raw) {
  let key = String(raw || '').trim().split('#')[0].trim();
  if (key.startsWith('0x') || key.startsWith('0X')) {
    key = key.slice(2);
  }
  key = key.replace(/[^0-9a-fA-F]/g, '');
  if (key.length !== 64) {
    throw new Error('PRIVATE_KEY must be 32-byte hex. Restart the function after editing .env');
  }
  return `0x${key}`;
}

async function getSdk() {
  if (cached) return cached;
  const privateKey = normalizePrivateKey(process.env.PRIVATE_KEY);
  const rpcUrl = (process.env.URL_RPC || '').trim() || DEFAULT_RPC;
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl, {
    name: CHAIN_ID === 137 ? 'matic' : 'sepolia',
    chainId: CHAIN_ID,
  });
  const signer = new ethers.Wallet(privateKey, provider);
  const sdk = new ArnaconSDK({
    signer,
    chainId: CHAIN_ID,
    rpcUrl,
  });
  cached = sdk;
  return sdk;
}

function snapshotAddresses(sdk) {
  return { ...sdk.getAllContractAddresses() };
}

async function withLinkedSemaphore(linked, fn) {
  if (!linked || !linked.semaphoreInteractor) {
    throw new Error(
      'Domain has no SemaphoreInteractor. Re-run SP onboarding /ensureSemaphore.',
    );
  }
  const sdk = await getSdk();
  const prev = snapshotAddresses(sdk);
  sdk.setContractAddresses({
    ...prev,
    SemaphoreInteractor: linked.semaphoreInteractor,
    ...(linked.secondLevelInteractor
      ? { SecondLevelInteractor: linked.secondLevelInteractor }
      : {}),
  });
  try {
    return await fn(sdk);
  } finally {
    sdk.setContractAddresses(prev);
  }
}

function statusKey(label) {
  return `lead:${label}:status`;
}

const SI_VIEW_ABI = [
  'function interactor() view returns (address)',
  'function REGISTER_SCOPE() view returns (uint256)',
];
const SLC_VIEW_ABI = ['function getSecondLevelInteractor() view returns (address)'];
const SLI_ROLE_ABI = [
  'function hasRole(bytes32 role, address account) view returns (bool)',
];
const SLI_EXEC_ABI = [
  'function executeTransaction(address target, bytes data)',
  'function arnaconResolver() view returns (address)',
];
const RESOLVER_PRODUCT_ABI = [
  'function deployProductTypeRegistry(string name)',
  'function getProductTypeRegistry(string name) view returns (address)',
  'function getNFTContracts(string name) view returns (address[])',
  'function addNFTContract(string name, address nftContract)',
];
const PTR_ABI = [
  'function getProductTypes() view returns (string[])',
  'function createProductType(string productType, string metadata)',
];
const REGISTRY_ABI = ['function owner(bytes32 node) view returns (address)'];
const WRAPPER_ABI = ['function ownerOf(uint256 id) view returns (address)'];

async function findSemaphoreInteractor(sdk, sliAddr, domain) {
  if (!domain) {
    return null;
  }
  let fromEns;
  try {
    fromEns = await semaphoreInteractorFromEns(sdk, domain);
  } catch (err) {
    console.warn('[elead] PublicResolver text semaphoreInteractor failed', err.message);
    return null;
  }
  if (!fromEns) {
    return null;
  }
  const si = new ethers.Contract(fromEns, SI_VIEW_ABI, sdk.getProvider());
  try {
    const bound = await si.interactor();
    if (String(bound).toLowerCase() === sliAddr.toLowerCase()) {
      return fromEns;
    }
  } catch (err) {
    console.warn('[elead] ENS semaphoreInteractor is not an SI', fromEns, err.message);
  }
  return null;
}

async function resolveDomainOnChain(domain) {
  const sdk = await getSdk();
  const provider = sdk.getProvider();
  const addresses = sdk.getAllContractAddresses();
  const node = ethers.utils.namehash(`${domain}.global`);
  const registry = new ethers.Contract(addresses.ENSRegistry, REGISTRY_ABI, provider);
  let owner = await registry.owner(node);
  if (!owner || owner.toLowerCase() === ZERO) {
    throw new Error(`${domain}.global is not registered on this chain`);
  }
  if (
    addresses.NameWrapper &&
    owner.toLowerCase() === addresses.NameWrapper.toLowerCase()
  ) {
    const wrapper = new ethers.Contract(addresses.NameWrapper, WRAPPER_ABI, provider);
    owner = await wrapper.ownerOf(ethers.BigNumber.from(node));
  }
  const slc = new ethers.Contract(owner, SLC_VIEW_ABI, provider);
  let sliAddr;
  try {
    sliAddr = await slc.getSecondLevelInteractor();
  } catch (_err) {
    throw new Error(`${domain}.global owner is not a 2LD controller`);
  }
  if (!sliAddr || sliAddr.toLowerCase() === ZERO) {
    throw new Error(`${domain}.global has no SecondLevelInteractor`);
  }
  const semaphoreInteractor = await findSemaphoreInteractor(sdk, sliAddr, domain);
  const sli = new ethers.Contract(sliAddr, SLI_ROLE_ABI, provider);
  const role = ethers.utils.id('CONTROLLER_ROLE');
  let backendIsController = false;
  let siGranted = false;
  try {
    backendIsController = await sli.hasRole(role, sdk.getSignerAddress());
  } catch (_err) {
    backendIsController = false;
  }
  if (semaphoreInteractor) {
    try {
      siGranted = await sli.hasRole(role, semaphoreInteractor);
    } catch (_err) {
      siGranted = false;
    }
  }
  return {
    domain,
    secondLevelInteractor: sliAddr,
    secondLevelController: owner,
    semaphoreInteractor,
    backendIsController,
    siGranted,
  };
}

const DOMAIN_RE = /^[a-z0-9-]+$/;
const ZERO = '0x0000000000000000000000000000000000000000';

function eleadProductHtmlUrl() {
  const fromEnv = String(process.env.ELEAD_HTML_URL || '').trim();
  if (fromEnv) {
    return fromEnv.replace(/\/v2\.0\.0\/?$/i, '').replace(/\/+$/, '') + '/';
  }
  const claim = String(process.env.CLAIM_PAGE_URL || '').trim();
  if (claim) {
    return `${claim
      .replace(/\/install\.html.*$/i, '')
      .replace(/\/v2\.0\.0\/?$/i, '')
      .replace(/\/+$/, '')}/`;
  }
  return 'https://cellact.github.io/Elead-HTML/';
}

function eleadHtmlVersionBase() {
  return `${eleadProductHtmlUrl()}v2.0.0/`;
}

function eleadPagesRoot() {
  return eleadProductHtmlUrl().replace(/\/+$/, '');
}

function eleadSpHtmlUrl() {
  const fromEnv = String(process.env.ELEAD_SP_HTML_URL || '').trim();
  if (fromEnv) {
    return fromEnv.replace(/\/+$/, '');
  }
  return `${eleadPagesRoot()}/sp-elead`;
}

function buildProductClientUrl({ isSp } = {}) {
  if (isSp) {
    return eleadSpHtmlUrl();
  }
  return `${eleadPagesRoot()}/elead`;
}

function eleadProductInfo() {
  const url = eleadProductHtmlUrl();
  return {
    name: ELEAD_PRODUCT_TYPE,
    type: ELEAD_PRODUCT_TYPE,
    url,
    html: url,
    packageType: 'ENS',
    description: 'A private line to the service provider. No phone. No email.',
  };
}

async function notifyEleadActivated({
  owner,
  label,
  domain,
  serviceContract,
  isSp,
}) {
  const info = eleadProductInfo();
  const clientUrl = buildProductClientUrl({ isSp });
  const allotted = `${label}.${domain}.global`;
  const endpoint =
    String(process.env.URL_NOTIFICATION_CENTER || DEFAULT_NOTIFICATION_CENTER).trim() ||
    DEFAULT_NOTIFICATION_CENTER;
  const body = {
    user_address: owner,
    item: 'BATMAN',
    package_type: 'ELEAD',
    uuid_to_sign: crypto.randomUUID(),
    callee: allotted,
    domain: 'paris1.cellact.nl',
    label: allotted,
    metadata: {
      serviceContract,
      name: info.name,
      description: info.description,
      clientUrl,
      image: 'https://cellact.github.io/Arnacon_HTML/logo.png',
      productIdentifier: 'ELEAD',
      callProtocol: 'webrtc',
      messageProtocol: 'webrtc',
      domain: `${domain}.global`,
    },
  };
  console.log('[elead] activate notify request', {
    endpoint,
    user_address: body.user_address,
    package_type: body.package_type,
    callee: body.callee,
    label: body.label,
    domain: body.domain,
    metadataDomain: body.metadata.domain,
    serviceContract,
    clientUrl: body.metadata.clientUrl,
  });
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  const clipped = text.length > 4000 ? `${text.slice(0, 4000)}…` : text;
  console.log('[elead] activate notify response', {
    status: response.status,
    ok: response.ok,
    statusText: response.statusText,
    body: clipped,
  });
  if (!response.ok) {
    throw new Error(`Notification failed (${response.status}): ${clipped}`);
  }
  try {
    return JSON.parse(text);
  } catch (_err) {
    return { raw: text };
  }
}

async function sliExecute(sdk, sliAddr, target, data) {
  const sli = new ethers.Contract(sliAddr, SLI_EXEC_ABI, sdk.signer);
  const tx = await sli.executeTransaction(target, data);
  await tx.wait();
}

async function getEleadProduct(domain) {
  const info = eleadProductInfo();
  const resolved = await resolveDomainOnChain(domain);
  const sdk = await getSdk();
  const sli = new ethers.Contract(
    resolved.secondLevelInteractor,
    SLI_EXEC_ABI,
    sdk.signer,
  );
  const resolverAddr = await sli.arnaconResolver();
  const resolver = new ethers.Contract(resolverAddr, RESOLVER_PRODUCT_ABI, sdk.signer);
  const ptr = await resolver.getProductTypeRegistry(domain);
  const nfts = await resolver.getNFTContracts(domain);
  let types = [];
  if (ptr && ptr.toLowerCase() !== ZERO) {
    const registry = new ethers.Contract(ptr, PTR_ABI, sdk.signer);
    types = await registry.getProductTypes();
  }
  const ready = types.some(
    (row) => String(row).toLowerCase() === ELEAD_PRODUCT_TYPE.toLowerCase(),
  );
  return {
    domain,
    productType: ELEAD_PRODUCT_TYPE,
    url: info.url,
    productTypeRegistry: ptr && ptr.toLowerCase() !== ZERO ? ptr : null,
    nftContracts: nfts,
    types,
    ready,
    backendIsController: resolved.backendIsController,
  };
}

async function ensureEleadProduct(domain) {
  domain = normalizeDomain(domain);
  if (!DOMAIN_RE.test(domain)) {
    throw new Error('Invalid domain');
  }
  const info = eleadProductInfo();
  const resolved = await resolveDomainOnChain(domain);
  const sdk = await getSdk();
  const sliAddr = resolved.secondLevelInteractor;
  const sli = new ethers.Contract(sliAddr, SLI_EXEC_ABI, sdk.signer);
  const resolverAddr = await sli.arnaconResolver();
  if (!resolverAddr || resolverAddr.toLowerCase() === ZERO) {
    throw new Error(`${domain}.global has no ArnaconResolver`);
  }
  const resolverIface = new ethers.utils.Interface(RESOLVER_PRODUCT_ABI);
  const resolver = new ethers.Contract(resolverAddr, RESOLVER_PRODUCT_ABI, sdk.signer);

  let ptr = await resolver.getProductTypeRegistry(domain);
  if (!ptr || ptr.toLowerCase() === ZERO) {
    await sliExecute(
      sdk,
      sliAddr,
      resolverAddr,
      resolverIface.encodeFunctionData('deployProductTypeRegistry', [domain]),
    );
    ptr = await resolver.getProductTypeRegistry(domain);
  }
  if (!ptr || ptr.toLowerCase() === ZERO) {
    throw new Error('deployProductTypeRegistry did not return a registry');
  }

  let nfts = await resolver.getNFTContracts(domain);
  if (!nfts || nfts.length === 0) {
    const nft = await sdk.deploymentManager.deployIfNeeded('ProductsNFT', resolverAddr);
    await sliExecute(
      sdk,
      sliAddr,
      resolverAddr,
      resolverIface.encodeFunctionData('addNFTContract', [domain, nft.address]),
    );
    nfts = [nft.address];
  }

  const ptrIface = new ethers.utils.Interface(PTR_ABI);
  const registry = new ethers.Contract(ptr, PTR_ABI, sdk.signer);
  const types = await registry.getProductTypes();
  const exists = types.some(
    (row) => String(row).toLowerCase() === ELEAD_PRODUCT_TYPE.toLowerCase(),
  );
  if (exists) {
    return {
      domain,
      productType: ELEAD_PRODUCT_TYPE,
      url: info.url,
      productTypeRegistry: ptr,
      nftContracts: nfts,
      existing: true,
      ready: true,
    };
  }

  const payload = {
    ...info,
    type: ELEAD_PRODUCT_TYPE,
    timestamp: Math.floor(Date.now() / 1000),
    productTypeRegistry: ptr,
  };
  let metadata = JSON.stringify(payload);
  try {
    metadata = await sdk.deploymentManager.ipfs.uploadToIPFS(payload);
  } catch (err) {
    console.warn('[elead] IPFS upload failed, storing JSON metadata', err.message);
  }

  try {
    const tx = await registry.createProductType(ELEAD_PRODUCT_TYPE, metadata);
    await tx.wait();
  } catch (_err) {
    await sliExecute(
      sdk,
      sliAddr,
      ptr,
      ptrIface.encodeFunctionData('createProductType', [ELEAD_PRODUCT_TYPE, metadata]),
    );
  }

  return {
    domain,
    productType: ELEAD_PRODUCT_TYPE,
    url: info.url,
    productTypeRegistry: ptr,
    nftContracts: nfts,
    metadata,
    existing: false,
    ready: true,
  };
}

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

function parseRequestBody(req) {
  let body = req.body;
  if (Buffer.isBuffer(body)) {
    const text = body.toString('utf8').trim();
    body = text ? JSON.parse(text) : {};
  } else if (typeof body === 'string') {
    const text = body.trim();
    body = text ? JSON.parse(text) : {};
  }
  req.body = body && typeof body === 'object' && !Array.isArray(body) ? body : {};
  return req.body;
}

function normalizeDomain(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\.global$/, '');
}

function claimPageBase({ isSp } = {}) {
  const product = isSp ? 'sp-elead' : 'elead';
  const fromEnv = String(
    isSp
      ? process.env.CLAIM_PAGE_URL_SP || process.env.CLAIM_PAGE_URL || ''
      : process.env.CLAIM_PAGE_URL || '',
  )
    .trim()
    .replace(/\/$/, '')
    .replace(/\?.*$/, '');
  if (new RegExp(`/${product}/v2\\.0\\.0/install\\.html$`, 'i').test(fromEnv)) {
    return fromEnv;
  }
  return `${eleadPagesRoot()}/${product}/v2.0.0/install.html`;
}

function buildClaimUrl(userSecret, label, domain, { isSp } = {}) {
  const params = new URLSearchParams({
    secret: String(userSecret),
    label: String(label),
    domain: String(domain),
  });
  if (process.env.CLAIM_DEV === 'true') {
    params.set('dev', 'true');
  }
  if (process.env.CLAIM_DEV === 'false') {
    params.set('dev', 'false');
  }
  const claimPage = `${claimPageBase({ isSp })}?${params.toString()}`;
  const provider = process.env.CLAIM_PROVIDER || 'Elead';
  return `arnacon://install?url=${encodeURIComponent(claimPage)}&provider=${encodeURIComponent(provider)}`;
}

function allocateLabel() {
  return `l${crypto.randomBytes(4).toString('hex')}`;
}

function jsonError(res, status, message) {
  let text = 'Request failed';
  if (typeof message === 'string' && message.trim()) {
    text = message;
  } else if (message instanceof Error && message.message) {
    text = message.message;
  } else if (message != null) {
    try {
      text = JSON.stringify(message);
    } catch (_err) {
      text = String(message);
    }
  }
  return res.status(status).json({ error: text });
}

const INBOX_FEED_SCHEMA = 'elead.inbox.feed.v1';
const LEAD_STATUS_KEYS = ['pending', 'in_progress', 'done', 'expired'];

function swarmBeeUrl() {
  return String(process.env.SWARM_BEE_URL || '').trim();
}

function swarmPostageBatchId() {
  return String(process.env.SWARM_POSTAGE_BATCH_ID || '')
    .trim()
    .replace(/^0x/i, '');
}

function swarmFeedPrivateKeyHex() {
  const raw = String(
    process.env.SWARM_FEED_PRIVATE_KEY || process.env.PRIVATE_KEY || '',
  ).trim();
  const hex = raw.replace(/^0x/i, '');
  if (!/^[0-9a-fA-F]{64}$/.test(hex)) {
    throw new Error('SWARM_FEED_PRIVATE_KEY or PRIVATE_KEY must be a 32-byte hex key');
  }
  return hex;
}

function emptyLeadCounts() {
  return { pending: 0, in_progress: 0, done: 0, expired: 0 };
}

function recomputeLeadCounts(cases) {
  const counts = emptyLeadCounts();
  for (const row of Object.values(cases || {})) {
    const status = String((row && row.status) || '').trim();
    if (Object.prototype.hasOwnProperty.call(counts, status)) {
      counts[status] += 1;
    }
  }
  return counts;
}

function inboxFeedTopicText(domain, inboxLabel) {
  return `elead:${inboxLabel}.${domain}.global`;
}

function inboxLabelFromInput(value, domain) {
  let raw = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\.global$/, '');
  const d = String(domain || '')
    .trim()
    .toLowerCase();
  if (d && (raw === d || raw.endsWith(`.${d}`))) {
    raw = raw.slice(0, raw.length - d.length).replace(/\.$/, '');
  }
  const label = raw.split('.').filter(Boolean)[0] || '';
  return label;
}

function inboxFeedMessage(domain, inboxLabel, timestamp) {
  return `elead-inbox-feed\n${domain}\n${inboxLabel}\n${timestamp}`;
}

function toUtf8Maybe(value) {
  if (value && typeof value.toUtf8 === 'function') {
    return value.toUtf8();
  }
  if (value && value.payload && typeof value.payload.toUtf8 === 'function') {
    return value.payload.toUtf8();
  }
  return '';
}

function emptyInboxFeed(domain, inboxLabel) {
  const now = new Date().toISOString();
  return {
    schema: INBOX_FEED_SCHEMA,
    domain,
    inbox: inboxFullName(domain, inboxLabel),
    updatedAt: now,
    counts: emptyLeadCounts(),
    cases: {},
  };
}

function normalizeInboxFeed(raw, domain, inboxLabel) {
  const base = emptyInboxFeed(domain, inboxLabel);
  if (!raw || typeof raw !== 'object') {
    return base;
  }
  const cases = {};
  const src = raw.cases && typeof raw.cases === 'object' ? raw.cases : {};
  for (const [lead, row] of Object.entries(src)) {
    const label = String(lead || '')
      .trim()
      .toLowerCase();
    if (!label) {
      continue;
    }
    cases[label] = {
      status: String((row && row.status) || '').trim() || 'pending',
      updatedAt: String((row && row.updatedAt) || base.updatedAt),
    };
  }
  const countsIn = raw.counts && typeof raw.counts === 'object' ? raw.counts : {};
  const counts = emptyLeadCounts();
  for (const key of LEAD_STATUS_KEYS) {
    const n = Number(countsIn[key]);
    counts[key] = Number.isFinite(n) && n >= 0 ? Math.floor(n) : 0;
  }
  return {
    schema: INBOX_FEED_SCHEMA,
    domain,
    inbox: inboxFullName(domain, inboxLabel),
    updatedAt: String(raw.updatedAt || base.updatedAt),
    counts,
    cases,
  };
}

let beeJsModule;

async function loadBeeJs() {
  if (!beeJsModule) {
    beeJsModule = await import('@ethersphere/bee-js');
  }
  return beeJsModule;
}

async function requireSwarmWriteConfig() {
  const beeUrl = swarmBeeUrl();
  const batchId = swarmPostageBatchId();
  if (!beeUrl) {
    throw new Error('SWARM_BEE_URL is not set');
  }
  if (!batchId || batchId.includes('REPLACE')) {
    throw new Error('SWARM_POSTAGE_BATCH_ID is not set');
  }
  return { beeUrl, batchId, privateKeyHex: swarmFeedPrivateKeyHex() };
}

async function requireSwarmReadConfig() {
  const beeUrl = swarmBeeUrl();
  if (!beeUrl) {
    throw new Error('SWARM_BEE_URL is not set');
  }
  return { beeUrl, privateKeyHex: swarmFeedPrivateKeyHex() };
}

async function inboxFeedReader(domain, inboxLabel) {
  const { Bee, Topic, PrivateKey } = await loadBeeJs();
  const { beeUrl, privateKeyHex } = await requireSwarmReadConfig();
  const privateKey = new PrivateKey(privateKeyHex);
  const bee = new Bee(beeUrl);
  const topic = Topic.fromString(inboxFeedTopicText(domain, inboxLabel));
  const reader = bee.feed.makeReader(topic, privateKey.publicKey().address());
  return { bee, topic, privateKey, reader };
}

async function downloadInboxFeed(domain, inboxLabel) {
  const { reader } = await inboxFeedReader(domain, inboxLabel);
  try {
    const latest = await reader.downloadPayload();
    const text = toUtf8Maybe(latest);
    const parsed = text ? JSON.parse(text) : null;
    return normalizeInboxFeed(parsed, domain, inboxLabel);
  } catch (_err) {
    return emptyInboxFeed(domain, inboxLabel);
  }
}

async function uploadInboxFeed(domain, inboxLabel, payload) {
  const { Bee, Topic, PrivateKey } = await loadBeeJs();
  const { beeUrl, batchId, privateKeyHex } = await requireSwarmWriteConfig();
  const privateKey = new PrivateKey(privateKeyHex);
  const bee = new Bee(beeUrl);
  const topic = Topic.fromString(inboxFeedTopicText(domain, inboxLabel));
  const writer = bee.feed.makeWriter(topic, privateKey);
  const body = normalizeInboxFeed(payload, domain, inboxLabel);
  body.updatedAt = new Date().toISOString();
  await writer.uploadPayload(batchId, JSON.stringify(body));
  return body;
}

async function ensNameOwner(fullName) {
  const sdk = await getSdk();
  const provider = sdk.getProvider();
  const addresses = sdk.getAllContractAddresses();
  const node = ethers.utils.namehash(fullName);
  const registry = new ethers.Contract(addresses.ENSRegistry, REGISTRY_ABI, provider);
  let owner = await registry.owner(node);
  if (!owner || owner.toLowerCase() === ZERO) {
    throw new Error(`${fullName} has no ENS owner`);
  }
  if (
    addresses.NameWrapper &&
    owner.toLowerCase() === addresses.NameWrapper.toLowerCase()
  ) {
    const wrapper = new ethers.Contract(addresses.NameWrapper, WRAPPER_ABI, provider);
    owner = await wrapper.ownerOf(ethers.BigNumber.from(node));
  }
  if (!owner || owner.toLowerCase() === ZERO) {
    throw new Error(`${fullName} has no wrapped owner`);
  }
  return owner.toLowerCase();
}

async function requireInboxOnList(domain, inboxLabel) {
  const sdk = await getSdk();
  const list = await readInboxListFromEns(sdk, domain);
  const hit = list.find((row) => row.label === inboxLabel);
  if (!hit) {
    throw new Error(`${inboxFullName(domain, inboxLabel)} is not on inboxList`);
  }
  return hit;
}

async function requireInboxFeedSignature({ domain, inboxLabel, timestamp, signature }) {
  const ts = requireTimestamp(timestamp);
  await requireInboxOnList(domain, inboxLabel);
  const message = inboxFeedMessage(domain, inboxLabel, ts);
  let recovered;
  try {
    recovered = ethers.utils.verifyMessage(message, signature);
  } catch (_err) {
    throw new Error('Invalid signature');
  }
  const recoveredLc = recovered.toLowerCase();
  const fullName = inboxFullName(domain, inboxLabel);
  const owner = await ensNameOwner(fullName);
  if (recoveredLc !== owner) {
    throw new Error(`Signer ${recoveredLc} is not ENS owner ${owner} of ${fullName}`);
  }
  return { owner: recoveredLc, timestamp: ts, fullName };
}

function parseInboxFeedQuery(req) {
  const src = Object.assign({}, req.query || {}, req.body || {});
  const domain = normalizeDomain(src.domain);
  const inboxLabel = inboxLabelFromInput(src.inbox || src.inboxName || src.label, domain);
  const lead = String(src.lead || src.leadLabel || '')
    .trim()
    .toLowerCase();
  return { domain, inboxLabel, lead, src };
}

async function handleGetInboxFeedLead(req, res) {
  const { domain, inboxLabel, lead } = parseInboxFeedQuery(req);
  if (!DOMAIN_RE.test(domain) || !DOMAIN_RE.test(inboxLabel) || !lead) {
    return jsonError(res, 400, 'domain, inbox, and lead are required');
  }
  try {
    await requireInboxOnList(domain, inboxLabel);
    const feed = await downloadInboxFeed(domain, inboxLabel);
    const row = feed.cases[lead] || null;
    return res.json({
      domain,
      inbox: inboxLabel,
      fullName: inboxFullName(domain, inboxLabel),
      lead,
      status: row ? row.status : null,
      updatedAt: row ? row.updatedAt : null,
      found: Boolean(row),
    });
  } catch (err) {
    return jsonError(res, 400, err.message || err);
  }
}

async function handlePostInboxFeedLead(req, res) {
  const { domain, inboxLabel, lead, src } = parseInboxFeedQuery(req);
  const status = String(src.status || '')
    .trim()
    .toLowerCase()
    .replace(/-/g, '_');
  if (!DOMAIN_RE.test(domain) || !DOMAIN_RE.test(inboxLabel) || !lead) {
    return jsonError(res, 400, 'domain, inbox, and lead are required');
  }
  if (!LEAD_STATUS_KEYS.includes(status)) {
    return jsonError(res, 400, `status must be one of ${LEAD_STATUS_KEYS.join(', ')}`);
  }
  try {
    await requireInboxFeedSignature({
      domain,
      inboxLabel,
      timestamp: src.timestamp,
      signature: src.signature,
    });
    const feed = await downloadInboxFeed(domain, inboxLabel);
    const now = new Date().toISOString();
    feed.cases[lead] = { status, updatedAt: now };
    feed.counts = recomputeLeadCounts(feed.cases);
    const saved = await uploadInboxFeed(domain, inboxLabel, feed);
    return res.json({
      domain,
      inbox: inboxLabel,
      fullName: saved.inbox,
      lead,
      status,
      updatedAt: saved.cases[lead].updatedAt,
      counts: saved.counts,
    });
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    const code = /signer|signature|owner|timestamp|expired/i.test(msg) ? 401 : 400;
    return jsonError(res, code, msg);
  }
}

async function handleGetInboxFeedSummary(req, res) {
  const { domain, inboxLabel } = parseInboxFeedQuery(req);
  if (!DOMAIN_RE.test(domain) || !DOMAIN_RE.test(inboxLabel)) {
    return jsonError(res, 400, 'domain and inbox are required');
  }
  try {
    await requireInboxOnList(domain, inboxLabel);
    const feed = await downloadInboxFeed(domain, inboxLabel);
    return res.json({
      domain,
      inbox: inboxLabel,
      fullName: feed.inbox,
      updatedAt: feed.updatedAt,
      counts: feed.counts,
    });
  } catch (err) {
    return jsonError(res, 400, err.message || err);
  }
}

async function handlePostInboxFeedSummary(req, res) {
  const { domain, inboxLabel, src } = parseInboxFeedQuery(req);
  if (!DOMAIN_RE.test(domain) || !DOMAIN_RE.test(inboxLabel)) {
    return jsonError(res, 400, 'domain and inbox are required');
  }
  const countsIn = src.counts && typeof src.counts === 'object' ? src.counts : src;
  const counts = emptyLeadCounts();
  for (const key of LEAD_STATUS_KEYS) {
    const n = Number(countsIn[key]);
    if (!Number.isFinite(n) || n < 0) {
      return jsonError(res, 400, `counts.${key} must be a non-negative integer`);
    }
    counts[key] = Math.floor(n);
  }
  try {
    await requireInboxFeedSignature({
      domain,
      inboxLabel,
      timestamp: src.timestamp,
      signature: src.signature,
    });
    const feed = await downloadInboxFeed(domain, inboxLabel);
    feed.counts = counts;
    const saved = await uploadInboxFeed(domain, inboxLabel, feed);
    return res.json({
      domain,
      inbox: inboxLabel,
      fullName: saved.inbox,
      updatedAt: saved.updatedAt,
      counts: saved.counts,
    });
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    const code = /signer|signature|owner|timestamp|expired/i.test(msg) ? 401 : 400;
    return jsonError(res, code, msg);
  }
}

async function handleListInboxFeeds(req, res) {
  const domain = normalizeDomain((req.query && req.query.domain) || (req.body && req.body.domain));
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  try {
    const sdk = await getSdk();
    const list = await readInboxListFromEns(sdk, domain);
    const inboxes = [];
    for (const row of list) {
      try {
        const feed = await downloadInboxFeed(domain, row.label);
        inboxes.push({
          label: row.label,
          fullName: row.fullName,
          updatedAt: feed.updatedAt,
          counts: feed.counts,
          cases: feed.cases,
        });
      } catch (err) {
        inboxes.push({
          label: row.label,
          fullName: row.fullName,
          error: err.message || String(err),
          counts: emptyLeadCounts(),
          cases: {},
        });
      }
    }
    return res.json({ domain, inboxes });
  } catch (err) {
    return jsonError(res, 400, err.message || err);
  }
}

async function handleConfig(_req, res) {
  const sdk = await getSdk();
  const artifactNames = [
    'SecondLevelInteractor',
    'ArnaconResolver',
    'PoseidonT3',
    'SemaphoreInteractor',
  ];
  const artifacts = {};
  for (const name of artifactNames) {
    const artifact = sdk.factoryLoader.getArtifact(name);
    if (!artifact || !artifact.bytecode) {
      throw new Error(`missing bytecode for ${name}`);
    }
    artifacts[name] = {
      bytecode: artifact.bytecode,
      linkReferences: artifact.linkReferences || {},
    };
  }
  const contracts = { ...sdk.getAllContractAddresses() };
  if (CHAIN_ID === 11155111) {
    contracts.SemaphoreVerifier = SEPOLIA_VERIFIER;
  }
  return res.json({
    backendAddress: sdk.getSignerAddress(),
    chainId: CHAIN_ID,
    contracts,
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
  try {
    await saveDomainRecord(domain, {
      spAddress: spAddress.toLowerCase(),
      secondLevelInteractor: req.body.secondLevelInteractor,
      semaphoreInteractor: req.body.semaphoreInteractor,
    });
  } catch (err) {
    console.error('[elead] firestore save domain failed', err.message);
  }
  return res.json(linked);
}

async function handleGenerateLeadQR(req, res) {
  const domain = normalizeDomain(req.body && req.body.domain);
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  const resolved = await resolveDomainOnChain(domain);
  if (!resolved.semaphoreInteractor) {
    return jsonError(
      res,
      400,
      `No SemaphoreInteractor on chain for ${domain}.global. Run /ensureSemaphore and grant CONTROLLER_ROLE.`,
    );
  }

  const label = allocateLabel();
  const inserted = await withWalletQueue(() =>
    withLinkedSemaphore(resolved, (sdk) => sdk.insertCommitment(label)),
  );
  const userSecret = inserted.userSecret;

  const lead = store.addLead({
    domain,
    label,
    kind: 'lead',
    userSecret,
    commitment: inserted.commitment,
    insertTx: inserted.transactionHash,
    spAddress: resolved.secondLevelController,
    status: 'unclaimed',
    createdAt: new Date().toISOString(),
  });

  const url = buildClaimUrl(userSecret, label, domain, { isSp: false });

  return res.json({
    url,
    label: lead.label,
    domain: lead.domain,
  });
}

async function handleGenerateInboxQR(req, res) {
  const body = req.body || {};
  const domain = normalizeDomain(body.domain);
  const inboxName = normalizeDomain(body.inboxName || body.label);
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  if (!DOMAIN_RE.test(inboxName)) {
    return jsonError(res, 400, 'Invalid inbox name');
  }
  try {
    await requireSpSignature({
      domain,
      inboxLabel: inboxName,
      timestamp: body.timestamp,
      signature: body.signature,
      kind: 'inbox',
    });
  } catch (err) {
    console.warn('[elead] generateInboxQR 401', err && err.message ? err.message : err);
    return jsonError(res, 401, err && err.message ? err.message : err);
  }

  const sdk = await getSdk();
  const existingInboxes = await readInboxListFromEns(sdk, domain);
  const existing = existingInboxes.find((row) => row.label === inboxName);
  if (existing) {
    return jsonError(res, 400, `Inbox ${inboxFullName(domain, inboxName)} already exists`);
  }
  if (store.findLead({ label: inboxName, domain })) {
    return jsonError(res, 400, `Label ${inboxName} is already used on ${domain}.global`);
  }

  const resolved = await resolveDomainOnChain(domain);
  if (!resolved.semaphoreInteractor) {
    return jsonError(
      res,
      400,
      `No SemaphoreInteractor on chain for ${domain}.global. Run /ensureSemaphore and grant CONTROLLER_ROLE.`,
    );
  }

  const inserted = await withWalletQueue(() =>
    withLinkedSemaphore(resolved, (sdk) => sdk.insertCommitment(inboxName)),
  );
  const createdAt = new Date().toISOString();
  const fullName = inboxFullName(domain, inboxName);
  store.addLead({
    domain,
    label: inboxName,
    kind: 'inbox',
    userSecret: inserted.userSecret,
    commitment: inserted.commitment,
    insertTx: inserted.transactionHash,
    spAddress: resolved.secondLevelController,
    status: 'unclaimed',
    createdAt,
  });
  try {
    await upsertInbox(domain, {
      label: inboxName,
      fullName,
      status: 'unclaimed',
      createdAt,
    });
  } catch (err) {
    console.error('[elead] firestore save inbox failed', err.message);
  }

  const url = buildClaimUrl(inserted.userSecret, inboxName, domain, { isSp: true });
  return res.json({
    url,
    label: inboxName,
    domain,
    fullName,
  });
}

async function handleGetInbox(req, res) {
  const path = pathname(req);
  const pathTail = path.split('/getInbox/')[1];
  const fromDomain =
    (req.query && (req.query.fromDomain || req.query.domain)) ||
    (req.body && (req.body.fromDomain || req.body.domain)) ||
    (pathTail ? decodeURIComponent(pathTail) : '');
  const domain = studioDomainFrom(fromDomain);
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'fromDomain is required (2LD or {label}.{domain}.global)');
  }
  const sdk = await getSdk();
  const fromEns = await readInboxListFromEns(sdk, domain);
  const active = fromEns.filter((row) => normalizeInboxStatus(row.status) === 'active');
  const picked = active.length > 0 ? active[crypto.randomInt(active.length)] : null;
  if (!picked) {
    return jsonError(res, 404, `No active inboxList entries for ${domain}`);
  }
  return res.json({
    domain,
    inbox: picked.fullName,
    label: picked.label,
  });
}

async function handleListInboxes(req, res) {
  const domain = normalizeDomain((req.query && req.query.domain) || (req.body && req.body.domain));
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  const sdk = await getSdk();
  const ensInboxes = await readInboxListFromEns(sdk, domain);
  return res.json({
    domain,
    receiveMode: null,
    receiveInbox: null,
    inboxes: ensInboxes,
    inboxList: ensInboxes,
  });
}

async function handleSetInboxActive(req, res) {
  const body = req.body || {};
  const domain = normalizeDomain(body.domain);
  const inboxName = normalizeDomain(body.inboxName);
  const status = normalizeInboxStatus(body.status);
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  if (!DOMAIN_RE.test(inboxName)) {
    return jsonError(res, 400, 'Invalid inboxName');
  }
  if (String(body.status || '').trim().toLowerCase() !== status) {
    return jsonError(res, 400, 'status must be active or inactive');
  }
  try {
    await requireSpSignature({
      domain,
      inboxLabel: inboxName,
      timestamp: body.timestamp,
      signature: body.signature,
      kind: 'inbox',
    });
  } catch (err) {
    return jsonError(res, 401, err && err.message ? err.message : err);
  }
  const sdk = await getSdk();
  const ensInboxes = await readInboxListFromEns(sdk, domain);
  const existing = ensInboxes.find((row) => row.label === inboxName);
  if (!existing) {
    return jsonError(res, 404, `${inboxFullName(domain, inboxName)} is not on inboxList`);
  }
  const next = ensInboxes.map((row) =>
    row.label === inboxName ? { ...row, status } : row,
  );
  try {
    const resolved = await resolveDomainOnChain(domain);
    await withWalletQueue(async () => {
      const queued = await getSdk();
      return writeInboxListToEns(
        queued,
        resolved.secondLevelInteractor,
        domain,
        next,
      );
    });
  } catch (err) {
    return jsonError(res, 500, err && err.message ? err.message : err);
  }
  return res.json({
    domain,
    receiveMode: null,
    receiveInbox: null,
    inboxes: next,
    inboxList: next,
  });
}

async function handleSetInboxRouting(req, res) {
  const body = req.body || {};
  const domain = normalizeDomain(body.domain);
  const mode = String(body.mode || '').trim().toLowerCase();
  const inboxName = body.inboxName ? normalizeDomain(body.inboxName) : '';
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  if (mode !== 'single' && mode !== 'group') {
    return jsonError(res, 400, 'mode must be single or group');
  }
  if (mode === 'single' && !DOMAIN_RE.test(inboxName)) {
    return jsonError(res, 400, 'inboxName required for single routing');
  }
  try {
    await requireSpSignature({
      domain,
      inboxLabel: mode === 'group' ? '-' : inboxName,
      timestamp: body.timestamp,
      signature: body.signature,
      kind: 'routing',
    });
  } catch (err) {
    return jsonError(res, 401, err && err.message ? err.message : err);
  }
  const sdk = await getSdk();
  const inboxes = await readInboxListFromEns(sdk, domain);
  if (mode === 'single' && !inboxes.some((row) => row.label === inboxName)) {
    return jsonError(res, 400, `Unknown inbox ${inboxName}`);
  }
  try {
    await saveDomainRecord(domain, {
      receiveMode: mode,
      receiveInbox: mode === 'single' ? inboxName : null,
    });
  } catch (err) {
    return jsonError(res, 503, `Could not persist routing: ${err.message}`);
  }
  return res.json({
    domain,
    receiveMode: mode,
    receiveInbox: mode === 'single' ? inboxName : null,
    inboxes,
  });
}

async function handleResolveDomain(req, res) {
  const domain = normalizeDomain((req.query && req.query.domain) || (req.body && req.body.domain));
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  const resolved = await resolveDomainOnChain(domain);
  return res.json(resolved);
}

async function handleListDomains(req, res) {
  const domainRaw = (req.query && req.query.domain) || '';
  if (!domainRaw) {
    return res.json({ domains: store.listDomains() });
  }
  const domain = normalizeDomain(domainRaw);
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  try {
    const resolved = await resolveDomainOnChain(domain);
    return res.json({
      domains: [
        {
          domain: resolved.domain,
          secondLevelInteractor: resolved.secondLevelInteractor,
          semaphoreInteractor: resolved.semaphoreInteractor,
        },
      ],
    });
  } catch (_err) {
    return res.json({ domains: [] });
  }
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
  rows = rows.filter((row) => row.kind !== 'inbox');

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

async function deploySemaphoreInteractorWithoutGrant(sdk, secondLevelInteractor) {
  const poseidonT3 = await sdk.deploymentManager.deployIfNeeded('PoseidonT3');
  const semaphoreVerifier = await sdk.deploymentManager.deployIfNeeded('SemaphoreVerifier');
  const semaphoreInteractor = await sdk.deploymentManager.deployIfNeeded(
    'SemaphoreInteractor',
    semaphoreVerifier.address,
    secondLevelInteractor,
    { _libraries: { PoseidonT3: poseidonT3.address } },
  );
  return {
    poseidonT3: poseidonT3.address,
    semaphoreVerifier: semaphoreVerifier.address,
    semaphoreInteractor: semaphoreInteractor.address,
    interactor: secondLevelInteractor,
  };
}

async function sliHasControllerRole(sliAddr, account, provider) {
  const sli = new ethers.Contract(
    sliAddr,
    ['function hasRole(bytes32 role, address account) view returns (bool)'],
    provider,
  );
  const role = ethers.utils.keccak256(ethers.utils.toUtf8Bytes('CONTROLLER_ROLE'));
  return sli.hasRole(role, account);
}

async function handleEnsureSemaphore(req, res) {
  const body = req.body || {};
  const domain = normalizeDomain(body.domain);
  let secondLevelInteractor = String(body.secondLevelInteractor || '').trim();
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, `Invalid domain (${JSON.stringify(body.domain || '')})`);
  }
  let resolved;
  try {
    resolved = await resolveDomainOnChain(domain);
  } catch (err) {
    return jsonError(res, 400, err.message || 'Could not resolve domain on chain');
  }
  if (
    !ethers.utils.isAddress(secondLevelInteractor) ||
    secondLevelInteractor.toLowerCase() === ZERO
  ) {
    secondLevelInteractor = resolved.secondLevelInteractor;
  }
  if (
    !ethers.utils.isAddress(secondLevelInteractor) ||
    secondLevelInteractor.toLowerCase() === ZERO
  ) {
    return jsonError(res, 400, 'Invalid secondLevelInteractor');
  }
  if (resolved.semaphoreInteractor) {
    return res.json({
      domain,
      secondLevelInteractor,
      semaphoreInteractor: resolved.semaphoreInteractor,
      needsGrant: !resolved.siGranted,
    });
  }

  const sdk = await getSdk();
  if (CHAIN_ID === 11155111) {
    sdk.setContractAddresses({
      ...sdk.getAllContractAddresses(),
      SemaphoreVerifier: SEPOLIA_VERIFIER,
    });
  }
  const contracts = sdk.deploymentManager.contracts;
  delete contracts.SemaphoreInteractor;
  delete contracts.SemaphoreInteractor_DeployBlock;

  const deployed = await deploySemaphoreInteractorWithoutGrant(sdk, secondLevelInteractor);
  let needsGrant = true;
  try {
    needsGrant = !(await sliHasControllerRole(
      secondLevelInteractor,
      deployed.semaphoreInteractor,
      sdk.getProvider(),
    ));
  } catch (err) {
    console.warn('[elead] hasRole CONTROLLER_ROLE check failed', err.message);
  }
  return res.json({
    domain,
    secondLevelInteractor,
    semaphoreInteractor: deployed.semaphoreInteractor,
    needsGrant,
  });
}

async function handleEnsureProduct(req, res) {
  const domain = normalizeDomain(
    (req.body && req.body.domain) || (req.query && req.query.domain),
  );
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'Invalid domain');
  }
  if (req.method === 'GET') {
    try {
      return res.json(await getEleadProduct(domain));
    } catch (err) {
      return jsonError(res, 400, err.message || 'Could not read Elead product');
    }
  }
  try {
    const result = await withWalletQueue(() => ensureEleadProduct(domain));
    return res.json(result);
  } catch (err) {
    return jsonError(res, 400, err.message || 'Could not create Elead product');
  }
}

function wrapperSet(...addrs) {
  const out = new Set();
  for (const raw of addrs) {
    const v = String(raw || '').trim();
    if (v && ethers.utils.isAddress(v)) {
      out.add(ethers.utils.getAddress(v).toLowerCase());
    }
  }
  return out;
}

async function resolveNameOwnerOnProvider(fullName, provider, registryAddr, wrappers) {
  if (!registryAddr || !ethers.utils.isAddress(registryAddr)) {
    return null;
  }
  const registry = new ethers.Contract(registryAddr, REGISTRY_ABI, provider);
  const node = ethers.utils.namehash(fullName);
  let owner;
  try {
    owner = await registry.owner(node);
  } catch (_err) {
    return null;
  }
  if (!owner || owner.toLowerCase() === ZERO) {
    return null;
  }
  if (wrappers.has(owner.toLowerCase())) {
    const wrapper = new ethers.Contract(owner, WRAPPER_ABI, provider);
    try {
      const wrapped = await wrapper.ownerOf(ethers.BigNumber.from(node));
      if (wrapped && wrapped.toLowerCase() !== ZERO) {
        return ethers.utils.getAddress(wrapped);
      }
    } catch (_err) {
      /* not in this wrapper */
    }
    return null;
  }
  return ethers.utils.getAddress(owner);
}

async function resolveNameOwner(fullName) {
  const sdk = await getSdk();
  const addresses = sdk.getAllContractAddresses();
  const sepolia = await resolveNameOwnerOnProvider(
    fullName,
    sdk.getProvider(),
    addresses.ENSRegistry,
    wrapperSet(addresses.NameWrapper),
  );
  if (sepolia) {
    return sepolia;
  }
  const polygonRpc =
    String(process.env.URL_POLYGON_RPC || process.env.POLYGON_RPC || DEFAULT_POLYGON_RPC).trim() ||
    DEFAULT_POLYGON_RPC;
  const polygonProvider = new ethers.providers.JsonRpcProvider(polygonRpc, {
    chainId: 137,
    name: 'matic',
  });
  return resolveNameOwnerOnProvider(
    fullName,
    polygonProvider,
    String(process.env.POLYGON_ENS_REGISTRY || DEFAULT_POLYGON_ENS_REGISTRY).trim(),
    wrapperSet(
      process.env.POLYGON_NAME_WRAPPER || DEFAULT_POLYGON_NAME_WRAPPER,
      process.env.POLYGON_TEMP_NAME_WRAPPER || DEFAULT_POLYGON_TEMP_NAME_WRAPPER,
      process.env.POLYGON_EMAIL_NAME_WRAPPER || DEFAULT_POLYGON_EMAIL_NAME_WRAPPER,
    ),
  );
}

async function resolveOwner(body) {
  const owner = body && body.owner;
  if (owner && ethers.utils.isAddress(owner) && owner.toLowerCase() !== ZERO) {
    return ethers.utils.getAddress(owner);
  }
  const id = String((body && body.web3identity) || '').trim().toLowerCase();
  if (!id) {
    return null;
  }
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
  const fullName = id.includes('.')
    ? id.endsWith('.global')
      ? id
      : `${id}.global`
    : `${id}${suffix}`;
  return resolveNameOwner(fullName);
}

async function handleGetGroupMembers(req, res) {
  const domainHint = normalizeDomain((req.query && req.query.domain) || (req.body && req.body.domain));
  const label = String((req.query && req.query.label) || (req.body && req.body.label) || '')
    .trim()
    .toLowerCase();
  let domain = domainHint;
  if (!DOMAIN_RE.test(domain) && label) {
    const lead = store.findLead({ label });
    if (lead) domain = lead.domain;
  }
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'domain or label required to select Semaphore group');
  }
  const resolved = await resolveDomainOnChain(domain);
  if (!resolved.semaphoreInteractor) {
    return jsonError(res, 400, `No SemaphoreInteractor on chain for ${domain}.global`);
  }

  const payload = await withLinkedSemaphore(resolved, async (sdk) => {
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
      domain: resolved.domain,
    };
  });
  return res.json(payload);
}

async function handleActivateWithProof(req, res) {
  const body = req.body || {};
  const proof = body.proof;
  const label = String(body.label || '').trim().toLowerCase();
  const domainHint = body.domain
    ? normalizeDomain(body.domain)
    : req.query && req.query.domain
      ? normalizeDomain(req.query.domain)
      : '';
  if (!proof || !label) {
    return jsonError(res, 400, 'proof and label are required');
  }
  const owner = await resolveOwner(body);
  if (!owner) {
    return jsonError(
      res,
      400,
      'Could not resolve owner from web3identity (ENS) or owner 0x address',
    );
  }
  const lead = store.findLead({ label, domain: domainHint || undefined });
  let domain = domainHint || (lead && lead.domain) || '';
  if (!DOMAIN_RE.test(domain)) {
    return jsonError(res, 400, 'domain required (unknown label and no domain in body)');
  }
  const resolved = await resolveDomainOnChain(domain);
  if (!resolved.semaphoreInteractor) {
    return jsonError(res, 400, `No SemaphoreInteractor on chain for ${domain}.global`);
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

  const parentName = domain;
  const result = await withWalletQueue(() =>
    withLinkedSemaphore(resolved, async (sdk) => {
      const si = sdk.semaphore._getSemaphoreInteractor();
      const tx = await sdk.semaphore.deploymentManager.executeTransaction(
        si,
        'registerSubnodeWithProof',
        [proof, owner, parentName],
        'Registering subnode with pre-generated ZK proof',
      );
      return { transactionHash: tx.hash };
    }),
  );

  if (lead) {
    store.updateLeadStatus(lead.domain, lead.label, 'claimed');
  }
  const inboxHit = (await readInboxListFromEns(sdk, domain)).find((row) => row.label === label);
  const kind = (lead && lead.kind) || (inboxHit ? 'inbox' : 'lead');
  if (kind === 'inbox') {
    const fullInboxName = `${label}.${domain}.global`;
    try {
      await withWalletQueue(async () => {
        const sdk = await getSdk();
        return appendClaimedInboxToEns(
          sdk,
          resolved.secondLevelInteractor,
          domain,
          label,
          fullInboxName,
        );
      });
    } catch (err) {
      console.warn('[elead] inboxList ENS update failed', err.message);
    }
    try {
      await upsertInbox(domain, {
        label,
        fullName: fullInboxName,
        status: 'claimed',
      });
    } catch (err) {
      console.warn('[elead] firestore claim inbox failed', err.message);
    }
  }
  const fullName = `${label}.${domain}.global`;
  const isSp = kind === 'inbox';
  const clientUrl = buildProductClientUrl({ isSp });
  let notify;
  try {
    notify = await notifyEleadActivated({
      owner,
      label,
      domain,
      serviceContract: resolved.secondLevelInteractor,
      isSp,
    });
  } catch (err) {
    console.warn('[elead] activate notify failed (non-fatal)', err.message);
    notify = { error: err.message };
  }

  return res.json({
    label,
    owner,
    name: fullName,
    domain,
    transactionHash: result.transactionHash,
    clientUrl,
    notify,
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

  console.log('[elead] dispatch', { path, action });

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
  if (
    (req.method === 'GET' || req.method === 'POST') &&
    (action === 'ensureProduct' || path.endsWith('/ensureProduct'))
  ) {
    return handleEnsureProduct(req, res);
  }
  if (req.method === 'GET' && path.endsWith('/group-members')) {
    return handleGetGroupMembers(req, res);
  }
  if (req.method === 'POST' && (action === 'generateLeadQR' || path.endsWith('/generateLeadQR'))) {
    return handleGenerateLeadQR(req, res);
  }
  if (req.method === 'POST' && (action === 'generateInboxQR' || path.endsWith('/generateInboxQR'))) {
    return handleGenerateInboxQR(req, res);
  }
  if (
    req.method === 'GET' &&
    (path === '/getInbox' || path.endsWith('/getInbox') || /\/getInbox\//.test(path))
  ) {
    return handleGetInbox(req, res);
  }
  if (req.method === 'GET' && path.endsWith('/inboxes')) {
    return handleListInboxes(req, res);
  }
  if (req.method === 'POST' && (action === 'setInboxActive' || path.endsWith('/setInboxActive'))) {
    return handleSetInboxActive(req, res);
  }
  if (req.method === 'POST' && (action === 'setInboxRouting' || path.endsWith('/setInboxRouting'))) {
    return handleSetInboxRouting(req, res);
  }
  if (
    req.method === 'GET' &&
    (path.endsWith('/inboxFeed/lead') || action === 'getInboxFeedLead')
  ) {
    return handleGetInboxFeedLead(req, res);
  }
  if (
    req.method === 'POST' &&
    (path.endsWith('/inboxFeed/lead') || action === 'setInboxFeedLead')
  ) {
    return handlePostInboxFeedLead(req, res);
  }
  if (
    req.method === 'GET' &&
    (path.endsWith('/inboxFeed/summary') || action === 'getInboxFeedSummary')
  ) {
    return handleGetInboxFeedSummary(req, res);
  }
  if (
    req.method === 'POST' &&
    (path.endsWith('/inboxFeed/summary') || action === 'setInboxFeedSummary')
  ) {
    return handlePostInboxFeedSummary(req, res);
  }
  if (
    req.method === 'GET' &&
    (path.endsWith('/inboxFeeds') || action === 'listInboxFeeds')
  ) {
    return handleListInboxFeeds(req, res);
  }
  if (
    req.method === 'POST' &&
    (action === 'activateWithProof' ||
      path.endsWith('/activateWithProof') ||
      (req.body && req.body.proof && req.body.label))
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

functions.http('helloHttp', async (req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') {
    return res.status(204).send('');
  }
  try {
    parseRequestBody(req);
    await dispatch(req, res);
  } catch (err) {
    console.error('[elead]', err);
    if (!res.headersSent) {
      res.status(500).json({ error: err.message || 'Internal error' });
    }
  }
});

module.exports = {
  ensureEleadProduct,
  getEleadProduct,
  eleadProductHtmlUrl,
};
