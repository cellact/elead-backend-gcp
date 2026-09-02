'use strict';

const ArnaconSDK = require('arnacon-sdk');

const CHAIN_ID = parseInt(process.env.CHAIN_ID || '11155111', 10);

let cached;

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
  const rpcUrl = (process.env.URL_RPC || '').trim();
  const sdk = new ArnaconSDK({
    privateKey,
    chainId: CHAIN_ID,
    ...(rpcUrl ? { rpcUrl } : {}),
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

module.exports = {
  CHAIN_ID,
  getSdk,
  withLinkedSemaphore,
  statusKey,
};
