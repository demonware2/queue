const dns = require('dns');
dns.setServers(['8.8.8.8', '1.1.1.1', '8.8.4.4']);

const Redis = require('ioredis');
const config = require('./config');
const axios = require('axios');
const minimist = require('minimist');
const EmailService = require('./services/email-service');
const CronjobService = require('./services/cronjob-service');
const WhatsAppService = require('./services/whatsapp-service');
const DocConverterService = require('./services/doc-converter-service');
const WebhookService = require('./services/webhook-service');
const DelayedInputService = require('./services/delayed-input-service');
const TelegramService = require('./services/telegram-service');
const PushNotificationService = require('./services/push-notification-service');
const BackupService = require('./services/backup-service');
const GitDeployService = require('./services/git-deploy-service');
const WebCrawlService = require('./services/web-crawl-service');
const logger = require('./services/logger');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');

const args = minimist(process.argv.slice(2));
const workerId = args.id;
const workerType = args.type;

if (!workerId || !workerType) {
    console.error('Worker ID and type are required');
    process.exit(1);
}

const redis = new Redis(config.redis);

let emailService = null;
let cronjobService = null;
let healthCheckInterval = null;
let whatsAppService = null;
let docConverterService = null;
let webhookService = null;
let delayedInputService = null;
let telegramService = null;
let pushNotificationService = null;
let backupService = null;
let gitDeployService = null;
let webCrawlService = null;
let keepRunning = true;

function safeStringify(value, opts = {}) {
    const { maxDepth = 5, maxChars = 20000 } = opts;
    const seen = new WeakSet();

    function helper(val, depth) {
        if (depth > maxDepth) return '[MaxDepth]';
        if (val === null) return null;
        const t = typeof val;
        if (t === 'string' || t === 'number' || t === 'boolean') return val;
        if (t === 'bigint') return String(val);
        if (t === 'undefined' || t === 'function' || t === 'symbol') return `[${t}]`;
        if (t === 'object') {
            if (seen.has(val)) return '[Circular]';
            seen.add(val);
            if (Array.isArray(val)) return val.map(v => {
                try { return helper(v, depth + 1); } catch (e) { return `[Error: ${e.message}]`; }
            });
            const out = {};
            for (const k of Object.keys(val)) {
                try {
                    out[k] = helper(val[k], depth + 1);
                } catch (e) {
                    out[k] = `[Error: ${e.message}]`;
                }
            }
            return out;
        }
        return val;
    }

    try {
        let s = JSON.stringify(helper(value, 0));
        if (s.length > maxChars) s = s.slice(0, maxChars) + '...';
        return s;
    } catch (e) {
        try { return String(value); } catch (_) { return '[Unserializable]'; }
    }
}

function setupEmailHealthCheck() {
    if (!emailService) return;
    if (!healthCheckInterval) {
        logger.info(`Worker ${workerId}: Starting periodic email service health checks`);
        healthCheckInterval = setInterval(async () => {
            try {
                logger.debug(`Worker ${workerId}: Checking email service health across modules...`);
                await emailService.checkServiceHealth();
            } catch (error) {
                logger.warn(`Worker ${workerId}: Email health check error: ${error.message}`);
            }
        }, 30 * 60 * 1000);
    }
}

let configSub = null;

if (workerType === config.jobTypes.EMAIL) {
    emailService = new EmailService();
    configSub = redis.duplicate();
    configSub.subscribe('config:email-updated');
    configSub.on('message', async (channel, message) => {
        if (channel === 'config:email-updated' && emailService) {
            try {
                const data = JSON.parse(message);
                const targetModule = data.module || 'Global';
                logger.info(`Worker ${workerId}: Received email config reload event for module '${targetModule}'`);
                await emailService.init(targetModule);
                logger.info(`Worker ${workerId}: Email config for module '${targetModule}' re-initialized successfully`);
            } catch (err) {
                logger.warn(`Worker ${workerId}: Error reloading email config: ${err.message}`);
            }
        }
    });
}

if (workerType === config.jobTypes.CRONJOB) {
    cronjobService = new CronjobService();
}

if (workerType === config.jobTypes.BACKUP) {
    backupService = new BackupService();
}

if (workerType === config.jobTypes.WHATSAPP) {
    whatsAppService = new WhatsAppService(redis);
}

if (workerType === config.jobTypes.DOC_CONVERT) {
    docConverterService = new DocConverterService(redis);
}

if (workerType === config.jobTypes.WEBHOOK) {
    webhookService = new WebhookService(redis);
}

if (workerType === config.jobTypes.DELAYED_INPUT) {
    delayedInputService = new DelayedInputService(redis);
}

if (workerType === config.jobTypes.TELEGRAM) {
    telegramService = new TelegramService(redis);
}

if (workerType === config.jobTypes.PUSH_NOTIFICATION) {
    pushNotificationService = new PushNotificationService(redis);
}

if (workerType === config.jobTypes.GIT_DEPLOY) {
    gitDeployService = new GitDeployService(redis);
}

if (workerType === config.jobTypes.WEB_CRAWL) {
    webCrawlService = new WebCrawlService();
}

const API_ENDPOINTS = {
    [config.jobTypes.SMS]: 'http://localhost/ci4/api/sms',
    [config.jobTypes.NOTIFICATION]: 'http://localhost/ci4/api/notification',
};

async function processJob(job, preclaimed = false) {
    logger.debug(`Worker ${workerId} starting to process job ${job.id} of type ${job.type}`);
    logger.debug(`Job payload: ${safeStringify(job.payload)}`);

    try {
        logger.info(`Worker ${workerId} processing job ${job.id} of type ${job.type}`);

        if (!preclaimed) {
            try {
                await axios.patch(`http://localhost:${config.server.port}/api/jobs/${job.id}`, {
                    status: 'processing',
                    workerId
                });
                logger.debug(`Job ${job.id} marked as processing`);
            } catch (patchError) {
                logger.warn(`Error updating job status: ${patchError.message}`);
                throw patchError;
            }
        } else {
            logger.debug(`Job ${job.id} already claimed as processing`);
        }

        try {
            await axios.patch(`http://localhost:${config.server.port}/api/workers/${workerId}`, {
                status: 'busy'
            });
            logger.debug(`Worker ${workerId} marked as busy`);
        } catch (workerError) {
            logger.warn(`Error updating worker status: ${workerError.message}`);
            throw workerError;
        }

        let result;

        if (job.type === config.jobTypes.GIT_DEPLOY && gitDeployService) {
            logger.info(`Starting Git deployment execution for branch: ${job.payload.branch}`);
            try {
                result = await gitDeployService.runGitDeploy(job.payload);
                logger.info(`Git deployment execution completed with exit code: ${result.exitCode}`);
                if (result.exitCode !== 0) {
                    throw new Error(`Git deployment script exited with non-zero code ${result.exitCode}. Error: ${result.error}`);
                }
            } catch (deployError) {
                logger.warn(`Error executing Git deployment: ${deployError.message}`);
                throw deployError;
            }
        } else if (job.type === config.jobTypes.BACKUP && backupService) {
            logger.info(`Starting backup execution for name: ${job.payload.name}`);
            try {
                result = await backupService.runBackup(job.payload);

                logger.info(`Backup execution completed with exit code: ${result.exitCode}`);
                if (result.exitCode !== 0) {
                    throw new Error(`Backup script exited with non-zero code ${result.exitCode}. Error: ${result.error}`);
                }
            } catch (backupError) {
                logger.warn(`Error executing backup: ${backupError.message}`);
                throw backupError;
            }
        } else if (job.type === config.jobTypes.CRONJOB && cronjobService) {
            logger.info(`Starting cronjob execution for script: ${job.payload.script}`);
            try {
                result = await cronjobService.runScript(job.payload);
                logger.debug(`Cronjob execution completed with result: ${safeStringify(result)}`);

                if (job.payload && job.payload.taskId) {
                    const taskId = job.payload.taskId;
                    logger.debug(`Updating task_scheduler for task ${taskId} to completed`);

                    try {
                        await cronjobService.updateTaskStatus(
                            taskId,
                            'success',
                            result.output + (result.error ? '\n\nErrors:\n' + result.error : '')
                        );
                        logger.debug(`Successfully updated task_scheduler for task ${taskId}`);
                    } catch (taskError) {
                        logger.warn(`Error updating task_scheduler: ${taskError.message}`);
                    }
                }
            } catch (scriptError) {
                logger.warn(`Error running script: ${scriptError.message}`);
                throw scriptError;
            }
        } else if (job.type === config.jobTypes.EMAIL && emailService) {

            if (!job.payload.to || !job.payload.subject || !job.payload.html) {
                throw new Error('Email payload must include "to", "subject", and "html" fields');
            }

            result = await emailService.sendEmail(job.payload);
        } else if (job.type === config.jobTypes.WHATSAPP && whatsAppService) {
            if (!job.payload.message) {
                throw new Error('Message is required for WhatsApp');
            }

            if (job.payload.groupId) {
                logger.info(`Starting WhatsApp group message to group ${job.payload.groupId}`);
                result = await whatsAppService.sendGroupMessage(job.payload);
            } else if (job.payload.number) {
                logger.info(`Starting WhatsApp message to number ${job.payload.number}`);
                result = await whatsAppService.sendMessage(job.payload);
            } else {
                throw new Error('Either number or groupId must be provided for WhatsApp message');
            }
        } else if (job.type === config.jobTypes.DOC_CONVERT && docConverterService) {
            logger.info(`Starting document conversion for file: ${job.payload.fileHash}`);
            result = await docConverterService.process(job);
        } else if (job.type === config.jobTypes.DELAYED_INPUT && delayedInputService) {
            result = await delayedInputService.process(job.payload);
        } else if (job.type === config.jobTypes.WEBHOOK && webhookService) {
            result = await webhookService.send(job.payload);
        } else if (job.type === config.jobTypes.TELEGRAM && telegramService) {
            result = await telegramService.sendMessage(job.payload);
        } else if (job.type === config.jobTypes.PUSH_NOTIFICATION && pushNotificationService) {
            result = await pushNotificationService.sendNotification(job.payload);
        } else if (job.type === config.jobTypes.WEB_CRAWL && webCrawlService) {
            logger.info(`Worker ${workerId} starting web crawl for: ${job.payload.uuid || job.payload.batch_id}`);
            try {
                result = await webCrawlService.runCrawl(job.payload);
                logger.info(`Web crawl execution completed with exit code: ${result.exitCode}`);
                if (result.exitCode !== 0) {
                    throw new Error(`Web crawl script exited with non-zero code ${result.exitCode}. Error: ${result.error}`);
                }
            } catch (crawlError) {
                logger.warn(`Error executing web crawl: ${crawlError.message}`);
                throw crawlError;
            }
        } else if (job.type === config.jobTypes.AI_SANDBOX) {
            logger.info(`Worker ${workerId} forwarding job ${job.id} to AI Sandbox...`);
            const sandboxUrl = process.env.AI_SANDBOX_URL || 'http://localhost:8085/execute';
            const response = await axios.post(sandboxUrl, {
                task_id: String(job.id),
                action: job.payload.action || 'run_prompt',
                api_slug: job.payload.api_slug,
                prompt: job.payload.prompt,
                data: job.payload.data
            }, {
                timeout: 120000
            });
            result = response.data;
        } else {
            const endpoint = API_ENDPOINTS[job.type];
            if (!endpoint) {
                throw new Error(`Unknown job type: ${job.type}`);
            }
            const response = await axios.post(endpoint, job.payload, {
                headers: {
                    'Content-Type': 'application/json',
                    'X-Job-Type': job.type,
                    'X-Worker-ID': workerId
                }
            });
            result = response.data;
        }

        logger.debug(`About to mark job ${job.id} as completed`);
        try {
            await axios.patch(`http://localhost:${config.server.port}/api/jobs/${job.id}`, {
                status: 'completed',
                result: result
            });
            logger.debug(`Job ${job.id} marked as completed`);
        } catch (completeError) {
            logger.warn(`Error marking job as completed: ${completeError.message}`);
            throw completeError;
        }

        logger.debug(`About to mark worker ${workerId} as idle`);
        try {
            await axios.patch(`http://localhost:${config.server.port}/api/workers/${workerId}`, {
                status: 'idle'
            });
            logger.debug(`Worker ${workerId} marked as idle`);
        } catch (idleError) {
            logger.warn(`Error marking worker as idle: ${idleError.message}`);
        }

        try {
            await redis.publish('worker:job-complete', JSON.stringify({
                jobId: job.id,
                workerId,
                result: result
            }));
            logger.debug(`Published job completion event for job ${job.id}`);
        } catch (pubError) {
            logger.warn(`Error publishing job completion: ${pubError.message}`);
        }

        return result;
    } catch (error) {
        logger.error(`Worker ${workerId} failed to process job ${job.id}: ${error.message}`);
        logger.debug(`Full error: ${error.stack}`);

        try {
            await axios.patch(`http://localhost:${config.server.port}/api/workers/${workerId}`, {
                status: 'idle'
            });
            logger.debug(`Worker ${workerId} marked as idle after failure`);
        } catch (idleError) {
            logger.warn(`Error marking worker as idle after failure: ${idleError.message}`);
        }

        try {
            await axios.patch(`http://localhost:${config.server.port}/api/jobs/${job.id}`,
                {
                    status: 'failed',
                    result: { error: error.message },
                    manageRetry: false
                });
            logger.debug(`Job ${job.id} marked as failed`);
        } catch (failError) {
            logger.warn(`Error marking job as failed: ${failError.message}`);
        }

        try {
            await redis.publish('worker:job-failed', JSON.stringify({
                jobId: job.id,
                workerId,
                error: error.message
            }));
            logger.debug(`Published job failure event for job ${job.id}`);
        } catch (pubError) {
            logger.warn(`Error publishing job failure: ${pubError.message}`);
        }
    }
}

async function brpopLoop() {
    const queueKey = `jobs:${workerType}`;
    let lastLoopErrorAt = 0;
    while (keepRunning) {
        try {
            const res = await redis.brpop(queueKey, 5);
            if (res && res.length === 2) {
                const jobData = res[1];
                let job;
                try {
                    job = JSON.parse(jobData);
                } catch (e) {
                    logger.warn(`Failed to parse job from Redis: ${e.message}`);
                    continue;
                }
                try {
                    const claimResp = await axios.post(`http://localhost:${config.server.port}/api/jobs/${job.id}/claim`);
                    if (claimResp.data && claimResp.data.claimed) {
                        await processJob(job, true);
                    } else {
                        logger.debug(`Skipping job ${job.id}: not pending (already claimed/processed)`);
                    }

                    if (workerType === config.jobTypes.DOC_CONVERT) {
                        logger.debug(`Breathing room: waiting 2 seconds before next job`);
                        await new Promise(r => setTimeout(r, 2000));
                    }

                    if (workerType === config.jobTypes.WEB_CRAWL) {
                        logger.debug(`Breathing room: waiting 3 seconds before next crawl job`);
                        await new Promise(r => setTimeout(r, 3000));
                    }
                } catch (claimErr) {
                    logger.warn(`Claim request failed for job ${job.id}: ${claimErr.message}`);
                }
                continue;
            }

            try {
                const response = await axios.get(`http://localhost:${config.server.port}/api/jobs/next/${workerType}`);
                if (response.data && response.data.job) {
                    await processJob(response.data.job);
                }
            } catch (apiErr) {
                logger.debug(`Fallback API check error: ${apiErr.message}`);
            }
        } catch (error) {
            if (!keepRunning) break;
            const now = Date.now();
            if (now - lastLoopErrorAt > 30000) {
                logger.warn(`Worker ${workerId} BRPOP loop error: ${error.message}`);
                lastLoopErrorAt = now;
            }
            await new Promise(r => setTimeout(r, 500));
        }
    }
}

async function main() {
    try {
        if (workerType === config.jobTypes.EMAIL && emailService) {
            const initialized = await emailService.init();
            if (!initialized) {
                throw new Error('Failed to initialize email service');
            }

            setupEmailHealthCheck();

            try {
                const db = await open({
                    filename: process.env.CONFIG_DB_PATH,
                    driver: sqlite3.Database,
                });

                const rows = await db.all("SELECT DISTINCT module FROM email_configuration WHERE module IS NOT NULL");
                const modules = rows
                    .map(r => r.module)
                    .filter(m => m && m !== 'Global');

                if (modules.length) {
                    logger.info(`Worker ${workerId}: Warming up email modules: ${modules.join(', ')}`);
                }

                for (const mod of modules) {
                    try {
                        const ok = await emailService.init(mod);
                        if (!ok) {
                            logger.warn(`Worker ${workerId}: Email module '${mod}' failed to initialize during warmup`);
                        }
                    } catch (e) {
                        logger.warn(`Worker ${workerId}: Error warming up email module '${mod}': ${e.message}`);
                    }
                }

                try {
                    await emailService.init('Global');
                } catch (e) {
                    logger.warn(`Worker ${workerId}: Failed to initialize Global during warmup: ${e.message}`);
                }

                await db.close();
            } catch (warmErr) {
                logger.warn(`Worker ${workerId}: Email warmup skipped due to error: ${warmErr.message}`);
            }
        }

        if (workerType === config.jobTypes.CRONJOB && cronjobService) {
            const initialized = await cronjobService.init();
            if (!initialized) {
                throw new Error('Failed to initialize cronjob service');
            }
            logger.info(`Worker ${workerId}: Cronjob service initialized successfully`);
        }

        if (workerType === config.jobTypes.BACKUP && backupService) {
            const initialized = await backupService.init();
            if (!initialized) {
                throw new Error('Failed to initialize backup service');
            }
            logger.info(`Worker ${workerId}: Backup service initialized successfully`);
        }

        if (workerType === config.jobTypes.GIT_DEPLOY && gitDeployService) {
            const initialized = await gitDeployService.init();
            if (!initialized) {
                throw new Error('Failed to initialize git deploy service');
            }
            logger.info(`Worker ${workerId}: Git deploy service initialized successfully`);
        }

        if (workerType === config.jobTypes.WEB_CRAWL && webCrawlService) {
            const initialized = await webCrawlService.init();
            if (!initialized) {
                throw new Error('Failed to initialize web crawl service');
            }
            logger.info(`Worker ${workerId}: Web crawl service initialized successfully`);
        }

        logger.info(`Worker ${workerId} (${workerType}) started (BRPOP consumption)`);
        brpopLoop();
    } catch (error) {
        console.error(`Worker ${workerId} initialization error:`, error.message);
        process.exit(1);
    }
}

process.on('SIGTERM', async () => {
    logger.info(`Worker ${workerId} shutting down...`);

    if (healthCheckInterval) {
        clearInterval(healthCheckInterval);
        healthCheckInterval = null;
    }

    if (cronjobService) {
        await cronjobService.shutdown();
    }
    keepRunning = false;
    if (configSub) {
        await configSub.quit();
    }
    await redis.quit();
    process.exit(0);
});

process.on('SIGINT', async () => {
    logger.info(`Worker ${workerId} shutting down...`);

    if (healthCheckInterval) {
        clearInterval(healthCheckInterval);
        healthCheckInterval = null;
    }

    if (cronjobService) {
        await cronjobService.shutdown();
    }
    keepRunning = false;
    if (configSub) {
        await configSub.quit();
    }
    await redis.quit();
    process.exit(0);
});

main();