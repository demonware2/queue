const express = require('express');
const bodyParser = require('body-parser');
const { initDatabase } = require('./db');
const Job = require('./models/job');
const Worker = require('./models/worker');
const QueueService = require('./services/queue');
const WorkerManager = require('./services/worker-manager');
const config = require('./config');

async function startServer() {
    const db = await initDatabase();

    const jobModel = new Job(db);
    const workerModel = new Worker(db);

    const queueService = new QueueService();
    await queueService.init();

    const workerManager = new WorkerManager(db, workerModel);
    await workerManager.init();

    queueService.setHandlers({
        onJobComplete: async (data) => {
            await jobModel.updateStatus(data.jobId, 'completed', data.workerId, data.result);
            await workerModel.updateStatus(data.workerId, 'idle');
            console.log(`Job ${data.jobId} completed by worker ${data.workerId}`);
        },
        onJobFailed: async (data) => {
            await jobModel.updateStatus(data.jobId, 'failed', data.workerId, { error: data.error });
            const job = await jobModel.getById(data.jobId);
            if (job && job.is_retry_enabled && job.attempts < job.retry_count) {
                const nextAttemptTimestamp = new Date(job.next_attempt_at).getTime();
                await queueService.redis.zadd('queue:retries', nextAttemptTimestamp, job.id);
                await queueService.redis.lpush('queue:retry_signal', '1');
            }
            await workerModel.updateStatus(data.workerId, 'idle');
            console.log(`Job ${data.jobId} failed on worker ${data.workerId}. Error: ${data.error}`);
        }
    });

    const workers = await workerModel.getAll();
    if (workers.length === 0) {
        for (const type of Object.values(config.jobTypes)) {
            for (let i = 0; i < config.workerSettings.defaultCount; i++) {
                await workerManager.createWorker(type);
            }
        }
    }

    const app = express();
    app.use(bodyParser.json());

    app.post('/api/jobs', async (req, res) => {
        try {
            const { type, payload } = req.body;

            if (!type || !payload) {
                return res.status(400).json({ error: 'Type and payload are required' });
            }

            console.log(type);
            console.log(payload)

            if (typeof payload !== 'object' || Array.isArray(payload) || !Object.keys(payload).length) {
                return res.status(400).json({ error: 'Payload must be a non-empty object' });
            }

            if (!Object.values(config.jobTypes).includes(type)) {
                return res.status(400).json({ error: `Invalid job type. Must be one of: ${Object.values(config.jobTypes).join(', ')}` });
            }

            const pickDefined = (...values) => values.find((value) => value !== undefined && value !== null);

            const normalizeBoolean = (value) => {
                if (typeof value === 'boolean') return value;
                if (typeof value === 'number') return value !== 0;
                if (typeof value === 'string') {
                    const lowered = value.trim().toLowerCase();
                    if (['true', '1', 'yes', 'y'].includes(lowered)) return true;
                    if (['false', '0', 'no', 'n'].includes(lowered)) return false;
                }
                return undefined;
            };

            const normalizeInteger = (value) => {
                if (value === undefined || value === null) return undefined;
                const parsed = Number(value);
                if (!Number.isFinite(parsed)) return undefined;
                return Math.max(0, Math.floor(parsed));
            };

            const rawRetryEnabled = pickDefined(
                payload.isRetryEnabled,
                payload.retryEnabled,
                payload.retry_options?.isRetryEnabled,
                payload.retry_options?.enabled,
                payload.retryOptions?.isRetryEnabled,
                payload.retryOptions?.retryEnabled,
                payload.retryOptions?.enabled,
                req.body.isRetryEnabled,
                req.body.retryEnabled
            );

            const rawRetryDelay = pickDefined(
                payload.retryDelay,
                payload.retry_delay,
                payload.retryOptions?.retryDelay,
                payload.retryOptions?.delay,
                payload.retry_options?.retryDelay,
                payload.retry_options?.delay,
                req.body.retryDelay,
                req.body.retry_delay
            );

            const rawRetryCount = pickDefined(
                payload.retryCount,
                payload.retry_count,
                payload.retryOptions?.retryCount,
                payload.retryOptions?.count,
                payload.retry_options?.retryCount,
                payload.retry_options?.count,
                req.body.retryCount,
                req.body.retry_count
            );

            const normalizedRetryEnabled = normalizeBoolean(rawRetryEnabled);
            const normalizedRetryDelay = normalizeInteger(rawRetryDelay);
            const normalizedRetryCount = normalizeInteger(rawRetryCount);

            if (rawRetryEnabled !== undefined || rawRetryDelay !== undefined || rawRetryCount !== undefined) {
                console.log('Job retry config (raw):', {
                    rawRetryEnabled,
                    rawRetryDelay,
                    rawRetryCount
                });
            }

            const resolvedRetryEnabled = normalizedRetryEnabled ?? false;
            const resolvedRetryDelay = normalizedRetryDelay ?? 1;
            const resolvedRetryCount = normalizedRetryCount ?? 5;

            const retryOptions = {
                isRetryEnabled: resolvedRetryEnabled,
                retryDelay: resolvedRetryDelay,
                retryCount: resolvedRetryCount
            };

            const jobPayload = { ...payload };
            if (payload.retryOptions && typeof payload.retryOptions === 'object' && !Array.isArray(payload.retryOptions)) {
                jobPayload.retryOptions = { ...payload.retryOptions };
            }
            if (payload.retry_options && typeof payload.retry_options === 'object' && !Array.isArray(payload.retry_options)) {
                jobPayload.retry_options = { ...payload.retry_options };
            }
            delete jobPayload.isRetryEnabled;
            delete jobPayload.retryEnabled;
            delete jobPayload.retryDelay;
            delete jobPayload.retry_delay;
            delete jobPayload.retryCount;
            delete jobPayload.retry_count;

            const pruneRetryContainer = (container) => {
                if (!container || typeof container !== 'object' || Array.isArray(container)) {
                    return;
                }
                delete container.delay;
                delete container.count;
                delete container.isRetryEnabled;
                delete container.retryDelay;
                delete container.retryCount;
                delete container.retryEnabled;
                if (!Object.keys(container).length) {
                    return true;
                }
                return false;
            };

            if (pruneRetryContainer(jobPayload.retryOptions)) {
                delete jobPayload.retryOptions;
            }
            if (pruneRetryContainer(jobPayload.retry_options)) {
                delete jobPayload.retry_options;
            }

            const jobId = await jobModel.create(type, jobPayload, retryOptions);

            await queueService.addJob(jobId, type, jobPayload);

            console.log('Job retry config (resolved):', retryOptions);

            console.log(`Job created with ID: ${jobId}, Type: ${type}, RetryEnabled: ${retryOptions.isRetryEnabled}, RetryDelay: ${retryOptions.retryDelay}h, RetryCount: ${retryOptions.retryCount}`);

            res.status(201).json({ jobId });
        } catch (error) {
            console.error('Error creating job:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.get('/api/jobs/:id', async (req, res) => {
        try {
            const job = await db.get('SELECT * FROM jobs WHERE id = ?', [req.params.id]);

            if (!job) {
                return res.status(404).json({ error: 'Job not found' });
            }

            job.payload = JSON.parse(job.payload);

            if (job.result) {
                job.result = JSON.parse(job.result);
            }

            res.json({ job });
        } catch (error) {
            console.error('Error getting job:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.patch('/api/jobs/:id', async (req, res) => {
        try {
            const { status, workerId, result, error, manageRetry } = req.body;

            const resultPayload = result !== undefined ? result : (error ? { error } : null);
            const options = {};
            if (typeof manageRetry === 'boolean') {
                options.manageRetry = manageRetry;
            }

            await jobModel.updateStatus(req.params.id, status, workerId, resultPayload, options);

            res.json({ success: true });
        } catch (error) {
            console.error('Error updating job:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.post('/api/jobs/:id/claim', async (req, res) => {
        try {
            const { id } = req.params;
            const claimed = await jobModel.claimIfPending(id);
            res.json({ claimed });
        } catch (error) {
            console.error('Error claiming job:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.get('/api/jobs/next/:type', async (req, res) => {
        try {
            const job = await jobModel.getNextPending(req.params.type);

            if (!job) {
                return res.json({ job: null });
            }

            job.payload = JSON.parse(job.payload);

            res.json({ job });
        } catch (error) {
            console.error('Error getting next job:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.post('/api/workers', async (req, res) => {
        try {
            const { type } = req.body;

            if (!type) {
                return res.status(400).json({ error: 'Type is required' });
            }

            if (!Object.values(config.jobTypes).includes(type)) {
                return res.status(400).json({ error: `Invalid worker type. Must be one of: ${Object.values(config.jobTypes).join(', ')}` });
            }

            const workerId = await workerManager.createWorker(type);

            res.status(201).json({ workerId });
        } catch (error) {
            console.error('Error creating worker:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.delete('/api/workers/:id', async (req, res) => {
        try {
            const success = await workerManager.stopWorker(req.params.id);

            if (!success) {
                return res.status(404).json({ error: 'Worker not found' });
            }

            res.json({ success: true });
        } catch (error) {
            console.error('Error stopping worker:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.patch('/api/workers/:id', async (req, res) => {
        try {
            const { status } = req.body;

            await workerModel.updateStatus(req.params.id, status);

            res.json({ success: true });
        } catch (error) {
            console.error('Error updating worker:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.post('/api/workers/scale', async (req, res) => {
        try {
            const { type, count } = req.body;

            if (!type || !count) {
                return res.status(400).json({ error: 'Type and count are required' });
            }

            if (!Object.values(config.jobTypes).includes(type)) {
                return res.status(400).json({ error: `Invalid worker type. Must be one of: ${Object.values(config.jobTypes).join(', ')}` });
            }

            if (count < 1 || count > config.workerSettings.maxCount) {
                return res.status(400).json({ error: `Count must be between 1 and ${config.workerSettings.maxCount}` });
            }

            await workerManager.scaleWorkers(type, count);

            res.json({ success: true });
        } catch (error) {
            console.error('Error scaling workers:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.get('/api/stats', async (req, res) => {
        try {
            const jobStats = await jobModel.getStats();
            const workerStats = await workerModel.getStats();

            res.json({
                jobs: jobStats,
                workers: workerStats
            });
        } catch (error) {
            console.error('Error getting stats:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.get('/api/workers/:id', async (req, res) => {
        try {
            const worker = await db.get('SELECT * FROM workers WHERE id = ?', [req.params.id]);

            if (!worker) {
                return res.status(404).json({ error: 'Worker not found' });
            }

            res.json({ worker });
        } catch (error) {
            console.error('Error getting worker:', error);
            res.status(500).json({ error: error.message });
        }
    });

    app.listen(config.server.port, () => {
        console.log(`Server running on port ${config.server.port}`);
    });

    process.on('SIGINT', async () => {
        console.log('Shutting down...');
        await workerManager.shutdown();
        await queueService.shutdown();
        await db.close();
        process.exit(0);
    });
}

startServer();
