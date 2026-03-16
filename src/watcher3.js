import fs from "node:fs";
import process from "node:process";
import "dotenv/config";
import pino from "pino";
import {
  decodeNameServiceInstruction,
  isNameServiceProgram,
  NS_PROGRAM_ID
} from "./nameServiceDecoder.js";
import {
  extractNameAccountEntries,
  formatTimestampForFilename
} from "./nameAccountExtraction.js";

function envInt(name, fallback) {
  const value = process.env[name];
  if (!value) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseTime(value) {
  if (!value) {
    return null;
  }
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

function startOfLocalDayMs(date = new Date()) {
  const local = new Date(date);
  local.setHours(0, 0, 0, 0);
  return local.getTime();
}

const config = {
  httpUrl: process.env.SOLANA_HTTP_URL,
  targetProgramId: process.env.TARGET_PROGRAM_ID ?? NS_PROGRAM_ID,
  commitment: process.env.COMMITMENT ?? "confirmed",
  signatureBatchLimit: envInt("SIGNATURE_BATCH_LIMIT", 1000),
  txConcurrency: envInt("TX_CONCURRENCY", 10),
  queueMax: envInt("QUEUE_MAX", 50000),
  rpcTimeoutMs: envInt("RPC_TIMEOUT_MS", 12000),
  rpcMaxRetries: envInt("RPC_MAX_RETRIES", 6),
  rpcBackoffMs: envInt("RPC_BACKOFF_MS", 300),
  outputPath: process.env.TX_OUTPUT_PATH ?? "./watcher3.transactions.jsonl",
  statePath: process.env.STATE_PATH ?? "./watcher3.state.json",
  nameAccountsOutputDir: process.env.NAME_ACCOUNTS_OUTPUT_DIR ?? ".",
  nameAccountsFilePrefix: process.env.NAME_ACCOUNTS_FILE_PREFIX ?? "watcher3.name_accounts",
  initialEndTime: process.env.INITIAL_END_TIME ?? "",
  stopAtPreviousHead: (process.env.STOP_AT_PREVIOUS_HEAD ?? "true").toLowerCase() === "true"
};

if (!config.httpUrl) {
  console.error("Missing required env: SOLANA_HTTP_URL");
  process.exit(1);
}

const log = pino({ level: process.env.LOG_LEVEL ?? "info" });
fs.mkdirSync(config.nameAccountsOutputDir, { recursive: true });
const outputStream = fs.createWriteStream(config.outputPath, { flags: "a" });

let requestId = 1;
let shutdownRequested = false;
let activeWorkers = 0;

const queue = [];
const queuedSet = new Set();
const seenSet = new Set();

const metrics = {
  pagesScanned: 0,
  signaturesScanned: 0,
  signaturesQueued: 0,
  txFetched: 0,
  txMatched: 0,
  txErrors: 0,
  txNullResponses: 0,
  queueDropped: 0
};

const state = loadState();
if (!state.cursor) {
  state.cursor = {};
}
if (!state.exports) {
  state.exports = {};
}

const run = createRunContext();
const nameAccountsStream = fs.createWriteStream(run.nameAccountsTmpPath, { flags: "w" });

function loadState() {
  try {
    if (!fs.existsSync(config.statePath)) {
      return {};
    }
    const raw = fs.readFileSync(config.statePath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    log.warn({ err: error.message }, "failed to load state file; starting from scratch");
    return {};
  }
}

function saveState() {
  const payload = {
    updatedAt: new Date().toISOString(),
    cursor: state.cursor,
    lastRun: {
      startedAt: run.startedAtIso,
      mode: run.mode,
      headSignature: run.headSignature,
      stopReason: run.stopReason,
      stopSignature: run.stopSignature,
      stopBlockTime: run.stopBlockTime,
      nameAccountsPath: run.nameAccountsFinalPath,
      nameAccountsLines: run.nameAccountsLines,
      nameAccountsTransactions: run.nameAccountsTransactions
    },
    exports: state.exports,
    metrics
  };
  fs.writeFileSync(config.statePath, JSON.stringify(payload, null, 2));
}

function createRunContext() {
  const now = new Date();
  const configuredInitialEndTimeMs = parseTime(config.initialEndTime);
  const initialEndTimeMs = configuredInitialEndTimeMs ?? startOfLocalDayMs(now);
  const previousHeadSignature = state.cursor.latestHeadSignature ?? null;
  const previousHeadObservedAtMs = parseTime(state.cursor.latestHeadObservedAt);

  return {
    startedAtIso: now.toISOString(),
    headSignature: null,
    previousHeadSignature,
    previousHeadObservedAtMs,
    initialEndTimeMs,
    initialEndTimeIso: new Date(initialEndTimeMs).toISOString(),
    mode: previousHeadSignature ? "incremental" : "initial-backfill",
    stopReason: null,
    stopSignature: previousHeadSignature,
    stopBlockTime: previousHeadObservedAtMs ?? initialEndTimeMs,
    nameAccountsTmpPath: buildBatchPath(now.toISOString(), "pending"),
    nameAccountsFinalPath: null,
    nameAccountsLines: 0,
    nameAccountsTransactions: 0
  };
}

function buildBatchPath(startedAtIso, suffix) {
  const startedAt = formatTimestampForFilename(startedAtIso);
  return `${config.nameAccountsOutputDir}/${config.nameAccountsFilePrefix}.${startedAt}.${suffix}.txt`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rpcCall(method, params) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.rpcTimeoutMs);
  try {
    const response = await fetch(config.httpUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: requestId++, method, params }),
      signal: controller.signal
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const payload = await response.json();
    if (payload.error) {
      throw new Error(`RPC ${payload.error.code}: ${payload.error.message}`);
    }

    return payload.result;
  } finally {
    clearTimeout(timeout);
  }
}

async function rpcWithRetry(method, params) {
  let lastError = null;
  for (let attempt = 0; attempt <= config.rpcMaxRetries; attempt += 1) {
    try {
      return await rpcCall(method, params);
    } catch (error) {
      lastError = error;
      const backoff = Math.min(config.rpcBackoffMs * 2 ** attempt, 5000);
      const jitter = Math.floor(Math.random() * 200);
      await sleep(backoff + jitter);
    }
  }
  throw lastError;
}

function enqueue(signature, source, slot = null) {
  if (!signature || seenSet.has(signature) || queuedSet.has(signature)) {
    return;
  }

  if (queue.length >= config.queueMax) {
    metrics.queueDropped += 1;
    return;
  }

  queue.push({ signature, source, slot });
  queuedSet.add(signature);
  metrics.signaturesQueued += 1;
}

function normalizeAccountKeys(txResult) {
  const keys = txResult?.transaction?.message?.accountKeys ?? [];
  if (!Array.isArray(keys)) {
    return [];
  }
  return keys.map((item) => (typeof item === "string" ? item : item?.pubkey ?? ""));
}

function extractProgramId(ix, accountKeys) {
  if (ix?.programId) {
    return ix.programId;
  }
  if (typeof ix?.programIdIndex === "number") {
    return accountKeys[ix.programIdIndex] ?? "";
  }
  return "";
}

function extractAccountIndexes(ix) {
  if (!Array.isArray(ix?.accounts)) {
    return [];
  }
  return ix.accounts.length > 0 && typeof ix.accounts[0] === "number" ? ix.accounts : [];
}

function extractAccountAddresses(ix, accountKeys) {
  if (!Array.isArray(ix?.accounts)) {
    return [];
  }
  if (ix.accounts.length === 0) {
    return [];
  }
  if (typeof ix.accounts[0] === "string") {
    return ix.accounts;
  }
  return ix.accounts.map((idx) => accountKeys[idx] ?? `unknown-index:${idx}`);
}

function decodeInstruction(ix, accountKeys, location) {
  const programId = extractProgramId(ix, accountKeys);
  const accountIndexes = extractAccountIndexes(ix);
  const accountAddresses = extractAccountAddresses(ix, accountKeys);

  const out = {
    location,
    programId,
    stackHeight: ix?.stackHeight ?? null,
    data: ix?.data ?? null,
    accounts: accountAddresses
  };

  if (isNameServiceProgram(programId)) {
    out.nameService = decodeNameServiceInstruction(ix?.data, accountKeys, accountIndexes);
  }

  return out;
}

function buildEvent(signature, txResult, sourceMeta) {
  const accountKeys = normalizeAccountKeys(txResult);
  const outer = txResult?.transaction?.message?.instructions ?? [];
  const innerGroups = txResult?.meta?.innerInstructions ?? [];

  const allInstructions = [];
  for (let i = 0; i < outer.length; i += 1) {
    allInstructions.push(decodeInstruction(outer[i], accountKeys, { kind: "outer", index: i }));
  }

  for (const group of innerGroups) {
    const parentIndex = group?.index ?? null;
    const inner = Array.isArray(group?.instructions) ? group.instructions : [];
    for (let i = 0; i < inner.length; i += 1) {
      allInstructions.push(
        decodeInstruction(inner[i], accountKeys, { kind: "inner", parentIndex, index: i })
      );
    }
  }

  const matchedInstructions = allInstructions.filter((ix) => ix.programId === config.targetProgramId);

  return {
    type: "transaction",
    signature,
    source: sourceMeta,
    slot: txResult?.slot ?? null,
    blockTime: txResult?.blockTime ?? null,
    success: txResult?.meta?.err == null,
    error: txResult?.meta?.err ?? null,
    feeLamports: txResult?.meta?.fee ?? null,
    computeUnitsConsumed: txResult?.meta?.computeUnitsConsumed ?? null,
    version: txResult?.version ?? null,
    matchedProgramId: config.targetProgramId,
    matchedInstructions,
    logs: txResult?.meta?.logMessages ?? []
  };
}

async function fetchAndEmit(item) {
  const tx = await rpcWithRetry("getTransaction", [
    item.signature,
    {
      encoding: "jsonParsed",
      commitment: config.commitment,
      maxSupportedTransactionVersion: 0
    }
  ]);

  if (!tx) {
    metrics.txNullResponses += 1;
    return;
  }

  metrics.txFetched += 1;
  const event = buildEvent(item.signature, tx, {
    from: item.source,
    observedSlot: item.slot
  });

  if (event.matchedInstructions.length === 0) {
    return;
  }

  metrics.txMatched += 1;
  const txLine = `${JSON.stringify(event)}\n`;
  outputStream.write(txLine);
  process.stdout.write(txLine);

  const nameAccountEntries = extractNameAccountEntries(event);
  if (nameAccountEntries.length > 0) {
    run.nameAccountsTransactions += 1;
    for (const entry of nameAccountEntries) {
      nameAccountsStream.write(
        `${entry.signature}\t${entry.instructionName}\t${entry.nameAccount}\n`
      );
      run.nameAccountsLines += 1;
    }
  }
}

async function workerLoop() {
  while (!shutdownRequested) {
    const item = queue.shift();
    if (!item) {
      await sleep(25);
      continue;
    }

    queuedSet.delete(item.signature);
    activeWorkers += 1;

    try {
      await fetchAndEmit(item);
      seenSet.add(item.signature);
    } catch (error) {
      metrics.txErrors += 1;
      log.error({ signature: item.signature, err: error.message }, "failed to fetch tx");
    } finally {
      activeWorkers -= 1;
    }
  }
}

async function getSignaturePage(beforeSignature = null) {
  const options = {
    limit: Math.max(1, Math.min(config.signatureBatchLimit, 1000)),
    commitment: config.commitment
  };
  if (beforeSignature) {
    options.before = beforeSignature;
  }

  return rpcWithRetry("getSignaturesForAddress", [config.targetProgramId, options]);
}

function isRowAtOrBeforeInitialEnd(row) {
  return typeof row?.blockTime === "number" && row.blockTime * 1000 <= run.initialEndTimeMs;
}

async function scanRange() {
  let beforeSignature = null;

  while (!shutdownRequested) {
    const page = await getSignaturePage(beforeSignature);
    metrics.pagesScanned += 1;

    if (!Array.isArray(page) || page.length === 0) {
      run.stopReason = run.mode === "incremental" ? "previous-head-not-found" : "reached-earliest-signature";
      break;
    }

    if (!run.headSignature && page[0]?.signature) {
      run.headSignature = page[0].signature;
    }

    metrics.signaturesScanned += page.length;

    const toQueue = [];
    let stopAfterThisPage = false;

    for (const row of page) {
      if (
        run.mode === "incremental" &&
        config.stopAtPreviousHead &&
        row.signature === run.previousHeadSignature
      ) {
        run.stopReason = "reached-previous-head";
        stopAfterThisPage = true;
        break;
      }

      if (
        run.mode === "incremental" &&
        typeof run.previousHeadObservedAtMs === "number" &&
        typeof row?.blockTime === "number" &&
        row.blockTime * 1000 <= run.previousHeadObservedAtMs
      ) {
        run.stopReason = "reached-previous-head-time";
        stopAfterThisPage = true;
        break;
      }

      if (run.mode === "initial-backfill" && isRowAtOrBeforeInitialEnd(row)) {
        run.stopReason = "reached-initial-end-time";
        stopAfterThisPage = true;
        break;
      }

      toQueue.push(row);
    }

    // Process oldest -> newest so the emitted batch is time-ordered.
    for (let i = toQueue.length - 1; i >= 0; i -= 1) {
      enqueue(toQueue[i].signature, run.mode, toQueue[i].slot ?? null);
    }

    if (metrics.pagesScanned % 10 === 0) {
      saveState();
      log.info(
        {
          mode: run.mode,
          pagesScanned: metrics.pagesScanned,
          signaturesScanned: metrics.signaturesScanned,
          queueLength: queue.length,
          beforeSignature
        },
        "watcher3 scan progress"
      );
    }

    if (stopAfterThisPage) {
      break;
    }

    beforeSignature = page[page.length - 1]?.signature ?? null;
    if (!beforeSignature) {
      run.stopReason = "missing-pagination-cursor";
      break;
    }

    while (!shutdownRequested && queue.length > config.queueMax * 0.8) {
      await sleep(150);
    }
  }
}

async function waitForQueueToDrain() {
  while (!shutdownRequested) {
    if (queue.length === 0 && queuedSet.size === 0 && activeWorkers === 0) {
      await sleep(100);
      if (queue.length === 0 && queuedSet.size === 0 && activeWorkers === 0) {
        return;
      }
    } else {
      await sleep(100);
    }
  }
}

function printMetrics() {
  log.info(
    {
      ...metrics,
      queueLength: queue.length,
      activeWorkers,
      seenSignatures: seenSet.size,
      mode: run.mode,
      headSignature: run.headSignature,
      previousHeadSignature: run.previousHeadSignature,
      previousHeadObservedAt: run.previousHeadObservedAtMs
        ? new Date(run.previousHeadObservedAtMs).toISOString()
        : null,
      stopReason: run.stopReason,
      initialEndTime: run.initialEndTimeIso
    },
    "watcher3 metrics"
  );
}

function shutdown(signal) {
  if (shutdownRequested) {
    return;
  }
  shutdownRequested = true;
  saveState();
  outputStream.end();
  nameAccountsStream.end();
  log.info({ signal }, "shutting down watcher3");
  setTimeout(() => process.exit(0), 250).unref();
}

async function closeStream(stream) {
  await new Promise((resolve, reject) => {
    stream.on("error", reject);
    stream.end(resolve);
  });
}

function finalizeNameAccountsBatch() {
  const endedAtIso = new Date().toISOString();
  const finalPath = buildBatchPath(run.startedAtIso, formatTimestampForFilename(endedAtIso));
  fs.renameSync(run.nameAccountsTmpPath, finalPath);
  run.nameAccountsFinalPath = finalPath;
  state.exports.nameAccounts = {
    updatedAt: endedAtIso,
    latestFilePath: finalPath,
    startedAt: run.startedAtIso,
    endedAt: endedAtIso,
    lines: run.nameAccountsLines,
    transactions: run.nameAccountsTransactions,
    sourceTransactionsPath: config.outputPath
  };
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
setInterval(printMetrics, 30_000).unref();

log.info(
  {
    httpUrl: config.httpUrl,
    commitment: config.commitment,
    targetProgramId: config.targetProgramId,
    signatureBatchLimit: config.signatureBatchLimit,
    txConcurrency: config.txConcurrency,
    outputPath: config.outputPath,
    statePath: config.statePath,
    nameAccountsOutputDir: config.nameAccountsOutputDir,
    nameAccountsFilePrefix: config.nameAccountsFilePrefix,
    mode: run.mode,
    previousHeadSignature: run.previousHeadSignature,
    initialEndTime: run.initialEndTimeIso
  },
  "starting watcher3"
);

for (let i = 0; i < config.txConcurrency; i += 1) {
  workerLoop();
}

await scanRange();
await waitForQueueToDrain();

if (run.headSignature) {
  state.cursor.latestHeadSignature = run.headSignature;
  state.cursor.latestHeadObservedAt = run.startedAtIso;
  state.cursor.lastCompletedAt = new Date().toISOString();
  state.cursor.initialEndTime = run.initialEndTimeIso;
}

await closeStream(nameAccountsStream);
await closeStream(outputStream);
finalizeNameAccountsBatch();
saveState();
process.exit(0);
