import express from 'express';
import { createRequire } from 'module';
import { spawn } from 'node:child_process';
import { Connection, clusterApiUrl, PublicKey } from '@solana/web3.js';
import pgp from 'pg-promise';
import dotenv from 'dotenv';
import fs from 'fs';
import readline from 'readline';
import Bottleneck from 'bottleneck';
import dayjs from 'dayjs';
import { log } from 'console';

const require = createRequire(import.meta.url);
const {
    getAllDomains,
    getDomainKeysWithReverses,
    getAllRegisteredDomains,
    NameRegistryState,
    getRecordV2Key,
    Record,
    getRecords,
    getRecordV2,
    getDomainKeySync,
    getPrimaryDomain,
    reverseLookup,
    getMultiplePrimaryDomains,
    ROOT_DOMAIN_ACCOUNT,
    getHandleAndRegistryKey,
    getTwitterRegistry,
} = require('@bonfida/spl-name-service');

// Load environment variables from the .env file
dotenv.config();

const pg = pgp();

// PostgreSQL connection using DSN
const db = pg({
    connectionString: process.env.PG_DSN,
    ssl: {
        rejectUnauthorized: false, // Accept self-signed certificates
    },
});

const SOLANA_MAIN_CLIENT = new Connection(process.env.ALCHEMY_RPC);


const rpcLimiter = new Bottleneck({
    minTime: 0, // means Bottleneck will not add any fixed delay between jobs
    maxConcurrent: 20, // means at most 20 jobs can run at the same time.
    reservoir: 50, // means you start with 50 available executions
    reservoirRefreshAmount: 50, // mean the bucket refills to 50 every 1 second
    reservoirRefreshInterval: 1000 // ...every 1 second
});

// const rpcLimiter = new Bottleneck({
//     minTime: 100, // 10 requests per second
//     maxConcurrent: 1, // keep only one Solana RPC request in flight
//     reservoir: 10, // allow up to 10 requests each second
//     reservoirRefreshAmount: 10, // refill to 10 requests
//     reservoirRefreshInterval: 1000 // every 1 second
// });

const SOL_TLD = new PublicKey("58PwtjSDuFHuUkYjH9BYnnQKHfwo9reZhC2zMJv9JPkx"); // .sol TLD
const NAME_PROGRAM_ID = new PublicKey("namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX");
const solanaZeroAddress = "11111111111111111111111111111111";
const WATCHER_TX_OUTPUT_PATH = process.env.TX_OUTPUT_PATH ?? "./data/watcher3.transactions.jsonl";
const WATCHER_STATE_PATH = process.env.STATE_PATH ?? "./data/watcher3.state.json";
const WATCHER_NAME_ACCOUNTS_OUTPUT_DIR = process.env.NAME_ACCOUNTS_OUTPUT_DIR ?? "./data";
const WATCHER_NAME_ACCOUNTS_FILE_PREFIX = process.env.NAME_ACCOUNTS_FILE_PREFIX ?? "watcher3.name_accounts";
const HALF_HOUR_MS = Number.parseInt(process.env.HALF_HOUR_MS ?? "", 10) || 30 * 60 * 1000;

// fetchAllDomains
// dumps all the domains namenode
// Fetch all registered .sol domains
const fetchAllDomains = async () => {
    try {
        console.log("Fetching all registered .sol domains...");
        // const connection = new Connection(clusterApiUrl('mainnet-beta'), 'confirmed');
        const connection = new Connection(process.env.ALCHEMY_RPC, 'confirmed')
        const registeredDomains = await getAllRegisteredDomains(connection);
        // const registeredDomains = await getAllRegisteredDomainsNew(connection)
        console.log("Total domains fetched:", registeredDomains.length);
        console.log(registeredDomains[0])
        const domainsList = registeredDomains.map(domain => domain.pubkey);
        const filePath = "./data/domains.csv"; 
        await fs.promises.writeFile(filePath, domainsList.join("\n"));
        console.log(`Domains successfully saved to ${filePath}`);
    } catch (error) {
        console.error("Error fetching domains:", error);
    }
};

async function readDomainsAndUpsert() {
    try {
        console.log("Connected to PostgreSQL");

        const filePath = './data/domains.csv';
        const fileStream = fs.createReadStream(filePath);
        
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity // Recognize CR LF sequences
        });

        const batchSize = 1000;
        let batch = [];
        let batchCount = 0;

        for await (const line of rl) {
            if (line.trim() !== '') {
                batch.push(line.trim());
                if (batch.length === batchSize) {
                    await processAndUpsertBatchForloop(batch);
                    batchCount++;
                    console.log(`Batch ${batchCount} upserted.`);
                    batch = [];
                }
            }
        }

        // remaining lines
        if (batch.length > 0) {
            await processAndUpsertBatchForloop(batch);
            batchCount++;
            console.log(`Batch ${batchCount} upserted.`);
        }

        console.log(`All batches completed. Batch count: ${batchCount}`);
    } catch (error) {
        console.error('Error:', error);
    } finally {
        console.log("Disconnected from PostgreSQL");
    }
}

async function processAndUpsertBatchForloop(batch) {
    try {
        for (const domain_pubkey of batch) {

            // Prepare the SQL upsert query with the fetched domain details
            const insertQuery = `
                INSERT INTO solana_name_indexer (namenode)
                VALUES ($1)
                ON CONFLICT (namenode) DO NOTHING;
            `;

            // Perform the upsert operation
            await db.none(insertQuery, [domain_pubkey]);

            console.log(`Insert namenode: ${domain_pubkey}`);
        }
        console.log('Batch processed successfully.');
    } catch (error) {
        console.error('Error in processAndUpsertBatch:', error);
    }
}

// fetchDomainsAndUpsert
// fetch all namenode(sns.id pubkey) owner/nft_owner/content
// this pipeline can not get name
async function fetchDomainsAndUpsert() {
    try {
        console.log("Connected to PostgreSQL");

        const batchSize = 1000;
        let allCount = 0;
        let batchCount = 0;
        let lastId = 0;
        let hasMoreRows = true;

        while (hasMoreRows) {
            // Fetch 1000 rows from the database where owner is NULL and sorted by id
            const query = `
                SELECT id, namenode 
                FROM solana_name_indexer 
                WHERE owner IS NULL 
                AND id > $1
                ORDER BY id ASC 
                LIMIT $2`;

            const batch = await db.any(query, [lastId, batchSize]);

            if (batch.length > 0) {
                for (const row of batch) {
                    // Call getDomainInfo and fetch domain details
                    // const { pubkey } = getDomainKeySync("v2ex");
                    const pubkey = new PublicKey(row.namenode);
                    const domainInfo = await retryGetDomainInfo(pubkey);
                    if (domainInfo) {
                        allCount++;
                        // Prepare the SQL upsert query with the fetched domain details
                        const insertQuery = `
                            INSERT INTO solana_name_indexer (namenode, nft_owner, is_tokenized, parent_node, registration_time, registration_hash, registration_height, expire_time, owner, resolver, resolved_address, contenthash, update_time)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                            ON CONFLICT (namenode) DO UPDATE
                            SET nft_owner = EXCLUDED.nft_owner,
                                is_tokenized = EXCLUDED.is_tokenized,
                                parent_node = EXCLUDED.parent_node,
                                registration_time = EXCLUDED.registration_time,
                                registration_hash = EXCLUDED.registration_hash,
                                registration_height = EXCLUDED.registration_height,
                                expire_time = EXCLUDED.expire_time,
                                owner = EXCLUDED.owner,
                                resolver = EXCLUDED.resolver,
                                resolved_address = EXCLUDED.resolved_address,
                                contenthash = EXCLUDED.contenthash,
                                update_time = EXCLUDED.update_time;`;
        
                        // Perform the upsert operation
                        await db.none(insertQuery, [
                            domainInfo.namenode,
                            domainInfo.nft_owner,
                            domainInfo.is_tokenized,
                            domainInfo.parent_node,
                            domainInfo.registration_time,
                            domainInfo.registration_hash,
                            domainInfo.registration_height,
                            domainInfo.expire_time,
                            domainInfo.owner,
                            domainInfo.resolver,
                            domainInfo.resolved_address,
                            domainInfo.contenthash,
                            domainInfo.update_time
                        ]);
                    
                        console.log(`${allCount} upserted.`);
                        console.log(`Upserted domain: ${domainInfo.namenode}`);
                    } else {
                        console.log(`Failed to fetch domain info for ${row.namenode}. Skipping.`);
                    }
                }

                // Update the last processed ID
                lastId = batch[batch.length - 1].id;

                batchCount++;
                console.log(`Batch ${batchCount} upserted.`);
            }

            // Check if we fetched less than batchSize, which means no more rows left
            if (batch.length < batchSize) {
                hasMoreRows = false;
            }
        }

        console.log(`All batches completed. Batch count: ${batchCount}`);
    } catch (error) {
        console.error('Error:', error);
    } finally {
        console.log("Disconnected from PostgreSQL");
    }
}

async function fetchDomainsByOwnersAndUpsert() {
    try {
        console.log("Connected to PostgreSQL");

        const batchSize = 1000;
        let allCount = 0;
        let batchCount = 0;
        let lastId = 0;
        let hasMoreRows = true;
        // Use a Set to filter out duplicate owners
        const processedOwners = new Set();

        while (hasMoreRows) {
            // Fetch 1000 rows from the database where name is NULL and sorted by id
            // owner != '11111111111111111111111111111111'
            const query = `
                SELECT id, owner 
                FROM solana_name_indexer 
                WHERE name IS NULL AND owner != '11111111111111111111111111111111'
                AND id > $1
                ORDER BY id ASC 
                LIMIT $2`;

            const batch = await db.any(query, [lastId, batchSize]);
            if (batch.length > 0) {
                for (const row of batch) {
                    const owner = row.owner;
                    if (processedOwners.has(owner)) {
                        console.log(`processedOwners Ignore Owner(${owner}) has already been fetched`);
                        continue;
                    }
                    // Call getDomainsWithWallet and fetch domain name details
                    const domainDetails = await retryGetDomainsWithWallet(owner);
                    processedOwners.add(owner);
                    if (domainDetails.length > 0) {
                        allCount += domainDetails.length
                        const insertQuery = `
                            INSERT INTO solana_name_indexer (namenode, name, label_name, parent_node, expire_time, owner, resolver, resolved_address, update_time)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (namenode) DO UPDATE
                            SET name = EXCLUDED.name,
                                label_name = EXCLUDED.label_name,
                                parent_node = EXCLUDED.parent_node,
                                expire_time = EXCLUDED.expire_time,
                                owner = EXCLUDED.owner,
                                resolver = EXCLUDED.resolver,
                                resolved_address = EXCLUDED.resolved_address,
                                update_time = EXCLUDED.update_time;`;
                        
                        for (const domainDetail of domainDetails) {
                            await db.none(insertQuery, [
                                domainDetail.namenode,
                                domainDetail.name,
                                domainDetail.label_name,
                                domainDetail.parent_node,
                                domainDetail.expire_time,
                                domainDetail.owner,
                                domainDetail.resolver,
                                domainDetail.resolved_address,
                                domainDetail.update_time
                            ]);
                        }
                        console.log(`${allCount} upserted.`);
                        console.log(`Upserted owner domains: ${row.owner}, ${domainDetails.length} records`);
                    } else {
                        console.log(`Failed to fetch owner domains for ${row.owner}. Skipping.`);
                    }
                }
                // Update the last processed ID
                lastId = batch[batch.length - 1].id;

                batchCount++;
                console.log(`Batch ${batchCount} upserted.`);
            }
            // Check if we fetched less than batchSize, which means no more rows left
            if (batch.length < batchSize) {
                hasMoreRows = false;
            }
        }

    } catch (error) {
        console.error('Error:', error);
    } finally {
        console.log("Disconnected from PostgreSQL");
    }
}


async function fetchPrimaryDomainsAndUpdate() {
    try {
        console.log("Connected to PostgreSQL");

        const batchSize = 100;
        let allCount = 0;
        let batchCount = 0;
        let offset = 0;
        let hasMoreRows = true;

        while (hasMoreRows) {
            // Fetch 100 distinct owners from the database
            const query = `
                SELECT DISTINCT ON (owner) owner 
                FROM solana_name_indexer 
                WHERE owner IS NOT NULL AND owner != '11111111111111111111111111111111'
                LIMIT $1 OFFSET $2`;

            const batch = await db.any(query, [batchSize, offset]);
            let hasPrimaryCount = 0;
            if (batch.length > 0) {
                
                const wallets = batch.map((row) => new PublicKey(row.owner));
                // Call retryGetPrimaryDomains to retrieve primary domain details
                const result = await retryGetPrimaryDomains(wallets);
                hasPrimaryCount = result.length;
                for (const row of result) {
                    // Step 1: Update sns_profile to unset is_primary for existing reverse addresses
                    await db.none(
                        `UPDATE solana_name_indexer 
                         SET is_primary = false, reverse_address = null 
                         WHERE reverse_address = $1`,
                        [row.reverse_address]
                    );
                    console.log(`Reverselookup ${row.reverse_address} => ${row.name}`)
                    // Step 2: Upsert with new primary domain information
                    const insertQuery = `
                        INSERT INTO solana_name_indexer (namenode, name, label_name, is_primary, reverse_address, update_time)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (namenode) DO UPDATE
                        SET name = EXCLUDED.name,
                            label_name = EXCLUDED.label_name,
                            is_primary = EXCLUDED.is_primary,
                            reverse_address = EXCLUDED.reverse_address,
                            update_time = EXCLUDED.update_time;`;

                    await db.none(insertQuery, [
                        row.namenode,
                        row.name,
                        row.label_name,
                        row.is_primary,
                        row.reverse_address,
                        row.update_time,
                    ]);

                    allCount++;
                    // console.log(`Upserted primary domain for wallet: ${row.reverse_address}`);
                }

                // Update last processed ID to keep track of pagination
                offset += batchSize;
                batchCount++;
                console.log(`offset ${offset}`)
                console.log(`Batch(${batchCount}) ${batch.length}rows has ${hasPrimaryCount} upserted.`);
            }

            // Stop the loop if there are no more rows to fetch
            if (batch.length < batchSize) {
                hasMoreRows = false;
            }
        }

        console.log(`All batches completed. Total upserted records: ${allCount}`);
    } catch (error) {
        console.error('Error:', error);
    } finally {
        console.log("Disconnected from PostgreSQL");
    }
}


async function fetchDomainTextsV2AndUpdate() {
    try {
        console.log("Connected to PostgreSQL");

        const batchSize = 100;
        let allCount = 0;
        let batchCount = 0;
        let offset = 0;
        let hasMoreRows = true;

        while (hasMoreRows) {
            // Fetch 100 distinct owners from the database
            const query = `
                SELECT name
                FROM solana_name_indexer 
                WHERE name IS NOT NULL AND owner != '11111111111111111111111111111111'
                LIMIT $1 OFFSET $2`;

            const batch = await db.any(query, [batchSize, offset]);
            let hasTextsCount = 0;
            if (batch.length > 0) {
                for (const row of batch) {
                    const name = row.name;
                    const domainName = name.endsWith('.sol') ? name.slice(0, -4) : name;
                    const domain_texts = await retryGetTextsV2(domainName);
                    if (Object.keys(domain_texts).length > 0) {
                        hasTextsCount += 1;
                        allCount++;
                        const texts_json = JSON.stringify(domain_texts);
                        console.log(`domain ${domainName} GetTextsV2 texts:`, texts_json);
                        await db.none(
                            `UPDATE solana_name_indexer 
                             SET texts_v2 = $1
                             WHERE name = $2`,
                            [texts_json, name]
                        );
                    } else {
                        console.log(`domain ${domainName} has no texts`);
                    }
                }

                // Update last processed ID to keep track of pagination
                offset += batchSize;
                batchCount++;
                console.log(`offset ${offset}`)
                console.log(`Batch(${batchCount}) ${batch.length}rows has ${hasTextsCount} upserted.`);
            }

            // Stop the loop if there are no more rows to fetch
            if (batch.length < batchSize) {
                hasMoreRows = false;
            }
        }
        console.log(`All batches completed. Total upserted records: ${allCount}`);
    } catch (error) {
        console.error('Error:', error);
    } finally {
        console.log("Disconnected from PostgreSQL");
    }
}

async function retryGetDomainInfo(domain_pubkey, retries = 3) {
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            return await getDomainInfo(domain_pubkey);
        } catch (error) {
            console.error(`Attempt ${attempt + 1} failed for ${domain_pubkey}:`, error);
            const message = error?.message || '';
            const isRateLimited = message.includes('429') || message.includes('Too Many Requests');
            const delayMs = isRateLimited ? (attempt + 1) * 5000 : 3000;
            await new Promise(resolve => setTimeout(resolve, delayMs));
        }
    }

    // If all retries fail, return the fallback solanaZeroAddress info
    const formattedNow = dayjs().format('YYYY-MM-DD HH:mm:ss');
    return {
        namenode: domain_pubkey,
        nft_owner: null,
        is_tokenized: false,
        parent_node: SOL_TLD.toBase58(), // Default parent_node
        expire_time: null,
        owner: solanaZeroAddress, // Solana zero address
        resolver: null,
        resolved_address: null,
        contenthash: null,
        update_time: formattedNow,
    };
}

async function getRegistrationTime(nameAccount, connection = SOLANA_MAIN_CLIENT) {
    const pubkey = nameAccount instanceof PublicKey
        ? nameAccount
        : new PublicKey(nameAccount);

    try {
        let before;
        let oldestSignatureInfo = null;

        while (true) {
            const signatures = await rpcLimiter.schedule(() => connection.getSignaturesForAddress(pubkey, {
                before,
                limit: 1000,
            }));

            if (signatures.length === 0) {
                break;
            }

            oldestSignatureInfo = signatures[signatures.length - 1];

            if (signatures.length < 1000) {
                break;
            }

            before = oldestSignatureInfo.signature;
        }

        if (!oldestSignatureInfo) {
            return {
                nameAccount: pubkey.toBase58(),
                signature: null,
                slot: null,
                block_time: null,
                block_datetime: null,
            };
        }

        const tx = await rpcLimiter.schedule(() => connection.getTransaction(oldestSignatureInfo.signature, {
            maxSupportedTransactionVersion: 0,
        }));
        const blockTime = tx?.blockTime ?? oldestSignatureInfo.blockTime ?? null;

        return {
            nameAccount: pubkey.toBase58(),
            signature: oldestSignatureInfo.signature,
            slot: oldestSignatureInfo.slot ?? null,
            block_time: blockTime,
            block_datetime: blockTime ? dayjs(blockTime * 1000).format('YYYY-MM-DD HH:mm:ss') : null,
        };
    } catch (error) {
        console.error(`Error fetching registration time for ${pubkey.toBase58()}:`, error);
        return {
            nameAccount: pubkey.toBase58(),
            signature: null,
            slot: null,
            block_time: null,
            block_datetime: null,
        };
    }
}


const getDomainInfo = async (domain_pubkey) => {
    try {
        const pubkey = new PublicKey(domain_pubkey);
        const { registry, nftOwner } = await rpcLimiter.schedule(() =>
            NameRegistryState.retrieve(SOLANA_MAIN_CLIENT, pubkey)
        );
        let contenthash = registry.data.toString('utf-8').trim();
        contenthash = contenthash.replace(/\x00+$/, '');

        // Further clean up invalid or problematic characters if necessary
        if (contenthash === '' || /^[\x00]+$/.test(contenthash)) {
            contenthash = null;
        } else {
            contenthash = contenthash.replace(/[\0\x00]+/g, ''); // Remove any lingering null bytes
        }
        
        const formattedNow = dayjs().format('YYYY-MM-DD HH:mm:ss');
        const registerInfo = await getRegistrationTime(pubkey);

        return {
            namenode: pubkey.toBase58(),
            nft_owner: nftOwner ? nftOwner.toBase58() : null,
            is_tokenized: !!nftOwner,
            parent_node: registry.parentName.toBase58(),
            expire_time: "2116-09-24 09:30:00",
            registration_time: registerInfo.block_datetime,
            registration_hash: registerInfo.signature,
            registration_height: registerInfo.slot,
            owner: registry.owner.toBase58(),
            resolver: NAME_PROGRAM_ID.toBase58(),
            resolved_address: registry.owner.toBase58(),
            contenthash: contenthash,
            update_time: formattedNow,
        };
    } catch (error) {
        if (error?.type === 'AccountDoesNotExist') {
            console.warn(`AccountDoesNotExist for domain_pubkey ${domain_pubkey}`);
            return null;  // Don't throw — no retry needed
        }

        console.error(`Error fetching domain info for ${domain_pubkey}:`, error);
        throw error;
    }
};


async function retryGetDomainsWithWallet(wallet, retries = 3) {
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            return await getDomainsWithWallet(wallet);
        } catch (error) {
            console.error(`Attempt ${attempt + 1} failed for wallet ${wallet}:`, error);
        }

        // Wait for 3 seconds before retrying
        await new Promise(resolve => setTimeout(resolve, 3000));
    }

    // If all retries fail, return []
    return [];
}

const getDomainsWithWallet = async (wallet) => {
    try {
        const walletPubkey = new PublicKey(wallet);
        const domainsWithReverses = await rpcLimiter.schedule(() =>
            getDomainKeysWithReverses(SOLANA_MAIN_CLIENT, walletPubkey)
        );
        let domains = [];

        domainsWithReverses.forEach((domain) => {
            // console.log(`Domain: ${domain.domain}, Public Key: ${domain.pubKey}`);

            const formattedNow = dayjs().format('YYYY-MM-DD HH:mm:ss');

            domains.push({
                namenode: domain.pubKey.toBase58(),
                name: domain.domain + ".sol",
                label_name: domain.domain,
                parent_node: SOL_TLD.toBase58(), // Ensure SOL_TLD is defined
                expire_time: "2116-09-24 09:30:00", // Placeholder expire time
                owner: walletPubkey.toBase58(),
                resolver: NAME_PROGRAM_ID.toBase58(), // Ensure NAME_PROGRAM_ID is defined
                resolved_address: walletPubkey.toBase58(),
                update_time: formattedNow, // Current time in 'yyyy-MM-dd HH:mm:ss' format
            });
        });

        return domains;
    } catch (error) {
        console.error(`Error fetching domains with wallet ${wallet}:`, error);
        throw error;
    }
};


async function retryGetPrimaryDomains(wallets, retries = 3) {
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            return await getPrimaryDomains(wallets);
        } catch (error) {
            console.error(`Attempt ${attempt + 1} failed for batchGetPrimaryDomains ${wallets}:`, error);
        }

        // Wait for 3 seconds before retrying
        await new Promise(resolve => setTimeout(resolve, 3000));
    }

    // If all retries fail, return []
    return [];
}

function chunkArray(items, chunkSize) {
    const chunks = [];
    for (let i = 0; i < items.length; i += chunkSize) {
        chunks.push(items.slice(i, i + chunkSize));
    }
    return chunks;
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function getNextHalfHourDelayMs() {
    const now = Date.now();
    const nextBoundary = Math.ceil(now / HALF_HOUR_MS) * HALF_HOUR_MS;
    return Math.max(nextBoundary - now, 1000);
}

async function sleepWithStatus(ms) {
    let remaining = ms;
    while (remaining > 0) {
        const step = Math.min(remaining, 60_000);
        console.log(`hold on: sleeping ${Math.ceil(remaining / 1000)}s until next historical cycle`);
        await sleep(step);
        remaining -= step;
    }
}

async function runWatcher3HistoricalFetch() {
    await new Promise((resolve, reject) => {
        const child = spawn(process.execPath, ["src/watcher3.js"], {
            cwd: process.cwd(),
            stdio: "inherit",
            env: {
                ...process.env,
                TX_OUTPUT_PATH: WATCHER_TX_OUTPUT_PATH,
                STATE_PATH: WATCHER_STATE_PATH,
                NAME_ACCOUNTS_OUTPUT_DIR: WATCHER_NAME_ACCOUNTS_OUTPUT_DIR,
                NAME_ACCOUNTS_FILE_PREFIX: WATCHER_NAME_ACCOUNTS_FILE_PREFIX,
            },
        });

        child.on("error", reject);
        child.on("exit", (code) => {
            if (code === 0) {
                resolve();
                return;
            }
            reject(new Error(`watcher3 exited with code ${code}`));
        });
    });
}

async function getLatestWatcherNameAccountsFile() {
    if (!fs.existsSync(WATCHER_STATE_PATH)) {
        return null;
    }

    const raw = await fs.promises.readFile(WATCHER_STATE_PATH, "utf8");
    const state = JSON.parse(raw);
    return state?.exports?.nameAccounts?.latestFilePath ?? null;
}

const getPrimaryDomains = async (wallets) => {
    try {
        const primaryDomains = await rpcLimiter.schedule(() =>
            getMultiplePrimaryDomains(SOLANA_MAIN_CLIENT, wallets)
        );
        const result = [];
        const formattedNow = dayjs().format('YYYY-MM-DD HH:mm:ss');
        primaryDomains.forEach((primary_name, index) => {
            if (primary_name !== undefined && typeof primary_name === 'string') {
                const { pubkey: primary_name_pubkey } = getDomainKeySync(primary_name);
                result.push({
                    reverse_address: wallets[index].toBase58(),
                    is_primary: true,
                    name: primary_name + ".sol",
                    label_name: primary_name,
                    namenode: primary_name_pubkey.toBase58(),
                    update_time: formattedNow,
                });
            }
        });

        // Return the result, which will be empty array if all entries were undefined
        return result;
    } catch (error) {
        console.error('Error fetching primary domains:', error);
        return []; // Return empty array in case of error
    }
};


async function retryGetTextsV2(domainName, retries = 3) {
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            return await getTextsV2(domainName);  // Attempt to fetch texts
        } catch (error) {
            console.error(`Attempt ${attempt + 1} failed for getTextsV2 ${domainName}:`, error);
        }

        // Wait for 3 seconds before retrying
        await new Promise(resolve => setTimeout(resolve, 3000));
    }

    // If all retries fail, return an empty object or handle differently
    return {};  // Return empty object as a fallback
}

const getTextsV2 = async (domainName) => {
    // const record_keys = [
    //     Record.IPNS,
    //     Record.IPFS,
    //     Record.ARWV,
    //     Record.SOL,
    //     Record.BTC,
    //     Record.LTC,
    //     Record.DOGE,
    //     Record.BSC,
    //     Record.Email,
    //     Record.Url,
    //     Record.Discord,
    //     Record.Reddit,
    //     Record.Twitter,
    //     Record.Telegram,
    //     Record.Pic,
    //     Record.SHDW,
    //     Record.POINT,
    //     Record.Injective,
    //     Record.Backpack
    // ];
    const record_keys = [
        // Record.IPNS,
        // Record.IPFS,
        // Record.ARWV,
        Record.SOL,
        // Record.BTC,
        // Record.LTC,
        // Record.DOGE,
        // Record.BSC,
        Record.Email,
        Record.Url,
        Record.Discord,
        // Record.Reddit,
        Record.Twitter,
        Record.Telegram,
        Record.Pic,
        // Record.SHDW,
        // Record.POINT,
        // Record.Injective,
        // Record.Backpack
    ];

    try {
        const texts = {}; // json object
        for (const record of record_keys) {
            try {
                const { deserializedContent } = await rpcLimiter.schedule(() =>
                    getRecordV2(SOLANA_MAIN_CLIENT, domainName, record, { deserialize: true })
                );
                if (deserializedContent) {
                    // const value = retrievedRecord.getContent().toString();
                    texts[record.toLowerCase()] = deserializedContent;
                    // if (isValidUTF8(value)) {
                    //     texts[record.toLowerCase()] = value;
                    // } else {
                    //     console.warn(`Invalid UTF-8 detected in record ${record} for domain ${domainName}. Skipping.`);
                    // }
                }
            } catch (error) {
                // console.error(`Error retrieving record ${record} for domain ${domainName}:`, error);
                continue;
            }
        }
        console.log(`domain ${domainName} getRecordV2 texts:`, texts);
        return texts;
    } catch (error) {
        console.error(`Error fetching texts getRecordV2 error:`, error);
        throw error
    }
};

async function fetchNamenodesFromFileAndUpsert(filePath) {
    const upsertFilePath = filePath.replace('.name_accounts.', '.name_accounts.upsert.');
    const doneFilePath = filePath.replace('.name_accounts.', '.name_accounts.done.');
    const content = await fs.promises.readFile(filePath, 'utf8');
    const nameAccountEntryMap = new Map();
    for (const line of content.split(/\r?\n/).map((item) => item.trim()).filter(Boolean)) {
        const parts = line.split('\t');
        if (parts.length < 3 || !parts[2]) {
            continue;
        }
        const instruction = parts[1]?.trim() ?? null;
        const nameAccount = parts[2].trim();
        const existing = nameAccountEntryMap.get(nameAccount);
        nameAccountEntryMap.set(nameAccount, {
            name_account: nameAccount,
            instructions: new Set([
                ...(existing?.instructions ?? []),
                instruction,
            ].filter(Boolean)),
            instruction,
        });
    }
    const nameAccountEntries = [...nameAccountEntryMap.values()].map((entry) => ({
        ...entry,
        instruction: entry.instructions.has('NameRegistryCreate') ? 'NameRegistryCreate' : entry.instruction,
    }));

    console.log(`Found ${nameAccountEntries.length} unique name_account entries in ${filePath}`);
    const candidateNamenodeMap = new Map();
    for (const entry of nameAccountEntries) {
        const nameAccount = entry.name_account;
        if (nameAccount === solanaZeroAddress) {
            continue;
        }

        const seedDomainInfo = await retryGetDomainInfo(nameAccount);
        console.log('inputSeedDomainInfo:', seedDomainInfo);

        if (!seedDomainInfo || !seedDomainInfo.namenode) {
            continue;
        }

        if (seedDomainInfo.parent_node !== SOL_TLD.toBase58()) {
            continue;
        }

        const existing = candidateNamenodeMap.get(seedDomainInfo.namenode);
        candidateNamenodeMap.set(seedDomainInfo.namenode, {
            seedDomainInfo,
            instructions: new Set([
                ...(existing?.instructions ?? []),
                entry.instruction,
            ].filter(Boolean)),
        });
    }
    console.log(`Resolved ${candidateNamenodeMap.size} unique candidate namenodes`);

    const upsertQuery = `
        INSERT INTO solana_name_indexer (
            namenode,
            name,
            label_name,
            parent_node,
            registration_time,
            registration_hash,
            registration_height,
            expire_time,
            nft_owner,
            is_tokenized,
            is_primary,
            reverse_address,
            owner,
            resolver,
            resolved_address,
            contenthash,
            texts_v2,
            update_time
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10, $11, $12, $13, $14, $15, $16, $17, $18
        )
        ON CONFLICT (namenode) DO UPDATE
        SET label_name = EXCLUDED.label_name,
            parent_node = EXCLUDED.parent_node,
            registration_time = EXCLUDED.registration_time,
            registration_hash = EXCLUDED.registration_hash,
            registration_height = EXCLUDED.registration_height,
            expire_time = EXCLUDED.expire_time,
            nft_owner = EXCLUDED.nft_owner,
            is_tokenized = EXCLUDED.is_tokenized,
            is_primary = EXCLUDED.is_primary,
            reverse_address = EXCLUDED.reverse_address,
            owner = EXCLUDED.owner,
            resolver = EXCLUDED.resolver,
            resolved_address = EXCLUDED.resolved_address,
            contenthash = EXCLUDED.contenthash,
            texts_v2 = EXCLUDED.texts_v2,
            update_time = EXCLUDED.update_time;
    `;

    const candidateNamenodes = [...candidateNamenodeMap.keys()];
    const existingRows = candidateNamenodes.length
        ? await db.any(
            `SELECT namenode, name, label_name
             FROM solana_name_indexer
             WHERE namenode IN ($1:csv)`,
            [candidateNamenodes]
        )
        : [];
    const existingRowMap = new Map(existingRows.map((row) => [row.namenode, row]));

    const allCandidateOwners = new Set();
    const ownersNeedingDomainDetails = new Set();
    for (const [, candidate] of candidateNamenodeMap.entries()) {
        const { seedDomainInfo } = candidate;
        if (seedDomainInfo.owner && seedDomainInfo.owner !== solanaZeroAddress) {
            allCandidateOwners.add(seedDomainInfo.owner);
        }

        const existingRow = existingRowMap.get(seedDomainInfo.namenode) ?? null;
        const missingName = !existingRow?.name;
        const missingLabelName = !existingRow?.label_name;
        if ((missingName || missingLabelName) && seedDomainInfo.owner && seedDomainInfo.owner !== solanaZeroAddress) {
            ownersNeedingDomainDetails.add(seedDomainInfo.owner);
        }
    }

    console.log(`Owners needing domainDetails backfill: ${ownersNeedingDomainDetails.size}`);
    console.log([...ownersNeedingDomainDetails]);

    const ownerDomainDetailsMap = new Map();
    for (const owner of ownersNeedingDomainDetails) {
        const domainDetails = await retryGetDomainsWithWallet(owner);
        ownerDomainDetailsMap.set(owner, domainDetails);
    }

    const primaryOwnerWallets = [...allCandidateOwners].map((owner) => new PublicKey(owner));
    const primaryWalletChunks = chunkArray(primaryOwnerWallets, 100);
    const allPrimaryDomains = [];
    for (const walletChunk of primaryWalletChunks) {
        const chunkResult = await retryGetPrimaryDomains(walletChunk);
        allPrimaryDomains.push(...chunkResult);
    }
    const primaryDomainsByOwner = new Map();
    for (const item of allPrimaryDomains) {
        if (!primaryDomainsByOwner.has(item.reverse_address)) {
            primaryDomainsByOwner.set(item.reverse_address, []);
        }
        primaryDomainsByOwner.get(item.reverse_address).push(item);
    }

    const results = [];
    const mergedRowLines = [];
    for (const [namenode, candidate] of candidateNamenodeMap.entries()) {
        const { seedDomainInfo } = candidate;
        console.log(`Processing candidate namenode ${namenode}`);
        const primaryDomains = primaryDomainsByOwner.get(seedDomainInfo.owner) ?? [];
        const primaryMatch = primaryDomains.find((item) => item.namenode === namenode);
        const existingRow = existingRowMap.get(namenode) ?? null;
        const ownerDomainDetails = ownerDomainDetailsMap.get(seedDomainInfo.owner) ?? [];
        const matchedDomainDetail = ownerDomainDetails.find((item) => item.namenode === namenode) ?? null;
        const resolvedName = existingRow?.name ?? matchedDomainDetail?.name ?? primaryMatch?.name ?? null;
        const resolvedLabelName = existingRow?.label_name ?? matchedDomainDetail?.label_name ?? primaryMatch?.label_name ?? null;
        const domainName = resolvedLabelName || (resolvedName?.endsWith('.sol') ? resolvedName.slice(0, -4) : resolvedName);
        const domainTexts = domainName ? await retryGetTextsV2(domainName) : {};
        const formattedNow = dayjs().format('YYYY-MM-DD HH:mm:ss');
        const mergedRow = {
            namenode: seedDomainInfo.namenode,
            name: resolvedName,
            label_name: resolvedLabelName,
            parent_node: seedDomainInfo.parent_node ?? SOL_TLD.toBase58(),
            registration_time: seedDomainInfo.registration_time ?? null,
            registration_hash: seedDomainInfo.registration_hash ?? null,
            registration_height: seedDomainInfo.registration_height ?? null,
            expire_time: seedDomainInfo.expire_time ?? null,
            nft_owner: seedDomainInfo.nft_owner ?? null,
            is_tokenized: seedDomainInfo.is_tokenized ?? false,
            is_primary: Boolean(primaryMatch),
            reverse_address: primaryMatch?.reverse_address ?? null,
            owner: seedDomainInfo.owner,
            resolver: seedDomainInfo.resolver ?? NAME_PROGRAM_ID.toBase58(),
            resolved_address: seedDomainInfo.resolved_address ?? seedDomainInfo.owner,
            contenthash: seedDomainInfo.contenthash ?? null,
            texts_v2: Object.keys(domainTexts).length ? JSON.stringify(domainTexts) : null,
            update_time: formattedNow,
        };

        await db.none(upsertQuery, [
            mergedRow.namenode,
            mergedRow.name,
            mergedRow.label_name,
            mergedRow.parent_node,
            mergedRow.registration_time,
            mergedRow.registration_hash,
            mergedRow.registration_height,
            mergedRow.expire_time,
            mergedRow.nft_owner,
            mergedRow.is_tokenized,
            mergedRow.is_primary,
            mergedRow.reverse_address,
            mergedRow.owner,
            mergedRow.resolver,
            mergedRow.resolved_address,
            mergedRow.contenthash,
            mergedRow.texts_v2,
            mergedRow.update_time,
        ]);

        results.push({
            name_account: namenode,
            upserted_count: 1,
        });
        mergedRowLines.push(JSON.stringify(mergedRow));
        console.log(`Upserted namenode ${mergedRow.namenode} (${mergedRow.name ?? 'unknown'})`);
    }

    await fs.promises.writeFile(upsertFilePath, `${mergedRowLines.join('\n')}\n`, 'utf8');
    console.log(`Wrote upsert results to ${upsertFilePath}`);

    await fs.promises.rename(filePath, doneFilePath);
    console.log(`Renamed input file to ${doneFilePath}`);

    return results;
}


const run = async () => {
    while (true) {
        console.log("starting historical watcher cycle");
        await runWatcher3HistoricalFetch();

        const latestNameAccountsFile = await getLatestWatcherNameAccountsFile();
        if (latestNameAccountsFile && fs.existsSync(latestNameAccountsFile)) {
            console.log(`processing historical batch ${latestNameAccountsFile}`);
            const results = await fetchNamenodesFromFileAndUpsert(latestNameAccountsFile);
            console.log(results);
        } else {
            console.log("no watcher3 name_accounts batch found after historical fetch");
        }

        await sleepWithStatus(getNextHalfHourDelayMs());
    }
}

// Execute the run function
run().catch(console.error);
