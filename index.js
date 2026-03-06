import express from 'express';
import { createRequire } from 'module';
import { Connection, clusterApiUrl, PublicKey } from '@solana/web3.js';
import pgp from 'pg-promise';
import dotenv from 'dotenv';
import fs from 'fs';
import readline from 'readline';
import Bottleneck from 'bottleneck';
import dayjs from 'dayjs';

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


// const limiter = new Bottleneck({
//     minTime: 0, // means Bottleneck will not add any fixed delay between jobs
//     maxConcurrent: 20, // means at most 20 jobs can run at the same time.
//     reservoir: 50, // means you start with 50 available executions
//     reservoirRefreshAmount: 50, // mean the bucket refills to 50 every 1 second
//     reservoirRefreshInterval: 1000 // ...every 1 second
// });

const rpcLimiter = new Bottleneck({
    minTime: 250, // add spacing between individual RPC requests
    maxConcurrent: 1, // keep only one Solana RPC request in flight
    reservoir: 4, // allow a small burst
    reservoirRefreshAmount: 4, // refill a small burst
    reservoirRefreshInterval: 1000 // every 1 second
});

const SOL_TLD = new PublicKey("58PwtjSDuFHuUkYjH9BYnnQKHfwo9reZhC2zMJv9JPkx"); // .sol TLD
const NAME_PROGRAM_ID = new PublicKey("namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX");
const solanaZeroAddress = "11111111111111111111111111111111";

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


const run = async () => {
    // await fetchAllDomains();
    // await readDomainsAndUpsert();
    await fetchDomainsAndUpsert();
    // const pubkey = new PublicKey("6o79HpB1JekRD327UwLYJ4uoExm5k4LdTSGQiGwxZki6");
    // const pubkey = new PublicKey("11LNLDoppF2PZzTfFieSob4PfRMBxauxbZxccB4US5d");
    // const result = await retryGetDomainInfo(pubkey);
    // console.log(result);
}

// Execute the run function
run().catch(console.error);
