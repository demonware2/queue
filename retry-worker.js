const { open } = require('sqlite');
const sqlite3 = require('sqlite3');
const Redis = require('ioredis');
const Job = require('./models/job');
const QueueService = require('./services/queue');
const config = require('./config');
const logger = require('./services/logger');

let db;
let jobModel;
let redis;
let queueService;
let syncInterval;
let keepRunning = true;

const TWENTY_FOUR_HOURS_IN_SECONDS = 24 * 60 * 60;

async function processDueJobs() {
    try {
        const now = Date.now();
        const jobIds = await redis.zrangebyscore('queue:retries', 0, now);

        if (jobIds.length > 0) {
            logger.info(`RetryWorker: Found ${jobIds.length} job(s) to process.`);
            console.log(`RetryWorker: Found ${jobIds.length} job(s) to process.`);

            const requeuePromises = jobIds.map(async (jobId) => {
                const job = await jobModel.getById(jobId);
                if (job) {
                    await redis.zrem('queue:retries', jobId);
                    await jobModel.updateStatus(jobId, 'pending');
                    await queueService.addJob(job.id, job.type, JSON.parse(job.payload));
                    console.log(`RetryWorker: Re-queued job ${jobId} (Type: ${job.type})`);
                } else {
                    await redis.zrem('queue:retries', jobId);
                    logger.warn(`RetryWorker: Job ${jobId} not found in database, removed from retry queue.`);
                }
            });

            await Promise.all(requeuePromises);
        }
    } catch (error) {
        logger.error(`RetryWorker: Error processing due jobs: ${error.message}`);
    }
}

async function mainLoop() {
    while (keepRunning) {
        try {
            await processDueJobs();

            const nextJob = await redis.zrange('queue:retries', 0, 0, 'WITHSCORES');
            let timeout = TWENTY_FOUR_HOURS_IN_SECONDS;

            if (nextJob.length > 0) {
                const nextJobTimestamp = parseInt(nextJob[1], 10);
                const delay = (nextJobTimestamp - Date.now()) / 1000;
                timeout = Math.max(1, Math.ceil(delay));
                logger.info(`RetryWorker: Next job due in ${timeout} seconds. Waiting for signal...`);
            } else {
                logger.info(`RetryWorker: No jobs in retry queue. Waiting for signal for up to 24 hours...`);
            }

            await redis.brpop('queue:retry_signal', timeout);

        } catch (error) {
            if (keepRunning) {
                logger.error(`RetryWorker: Error in main loop: ${error.message}`);
                await new Promise(resolve => setTimeout(resolve, 5000));
            }
        }
    }
}

async function syncFromDatabase() {
    logger.info('RetryWorker: Syncing retry queue from database...');
    try {
        const jobsToRetry = await jobModel.getDueForRetry();
        if (jobsToRetry.length > 0) {
            logger.info(`RetryWorker: Found ${jobsToRetry.length} job(s) in DB to sync.`);
            let syncedCount = 0;
            for (const job of jobsToRetry) {
                const score = await redis.zscore('queue:retries', job.id);
                if (!score) {
                    const nextAttemptTimestamp = new Date(job.next_attempt_at).getTime();
                    await redis.zadd('queue:retries', nextAttemptTimestamp, job.id);
                    syncedCount++;
                }
            }
            if (syncedCount > 0) {
                console.log(`RetryWorker: Synced ${syncedCount} job(s) from DB to retry queue.`);
                await redis.lpush('queue:retry_signal', '1');
            }
        }
    } catch (error) {
        logger.error(`RetryWorker: Error syncing from database: ${error.message}`);
    }
}

async function main() {
    try {
        db = await open({ filename: config.sqlite.filename, driver: sqlite3.Database });
        jobModel = new Job(db);
        redis = new Redis(config.redis);
        queueService = new QueueService();

        logger.info('RetryWorker: Database and Redis connections established.');

        mainLoop();

        syncFromDatabase();
        syncInterval = setInterval(syncFromDatabase, 3 * 60 * 60 * 1000);

        logger.info('RetryWorker: Started successfully with event-driven scheduling.');

    } catch (error) {
        logger.error(`RetryWorker: Initialization failed: ${error.message}`);
        process.exit(1);
    }
}

async function shutdown() {
    logger.info('RetryWorker: Shutting down...');
    keepRunning = false;
    clearInterval(syncInterval);
    try {
        const signalRedis = new Redis(config.redis);
        await signalRedis.lpush('queue:retry_signal', 'shutdown');
        await signalRedis.quit();
    } catch (e) { /* ignore */ }

    if (redis) await redis.quit();
    if (queueService) await queueService.shutdown();
    if (db) await db.close();
    process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

main();