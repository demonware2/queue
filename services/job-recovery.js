const config = require('../config');

const MAX_TIMEOUT_HOURS = 48;

function normalizeRecoveryOptions(raw) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
        return null;
    }

    const timeoutHours = Number(raw.timeoutHours);

    return {
        requeueOnRestart: raw.requeueOnRestart === true,
        timeoutHours: Number.isFinite(timeoutHours) && timeoutHours > 0 ? Math.min(timeoutHours, MAX_TIMEOUT_HOURS) : 0,
    };
}

function recoveryOf(job) {
    try {
        const payload = typeof job.payload === 'string' ? JSON.parse(job.payload) : job.payload;
        return (payload && payload._recovery) || {};
    } catch (e) {
        return {};
    }
}

async function scheduleRetry(jobModel, queueService, jobId) {
    const job = await jobModel.getById(jobId);
    if (job && job.status === 'failed' && job.is_retry_enabled && job.attempts < job.retry_count && job.next_attempt_at) {
        await queueService.redis.zadd('queue:retries', new Date(job.next_attempt_at).getTime(), job.id);
        await queueService.redis.lpush('queue:retry_signal', '1');
    }
}

async function recoverInterruptedJobs(jobModel, queueService) {
    const maxAgeSeconds = config.recovery.requeueMaxAgeHours * 3600;

    for (const job of await jobModel.getProcessing()) {
        const canRequeue = recoveryOf(job).requeueOnRestart === true
            && job.is_retry_enabled
            && job.attempts + 1 < job.retry_count
            && job.idle_seconds <= maxAgeSeconds;

        try {
            if (canRequeue && await jobModel.requeueIfProcessing(job.id, 'requeued after queue server restart')) {
                await queueService.addJob(job.id, job.type, JSON.parse(job.payload));
                console.log(`Recovery: requeued interrupted job ${job.id} (${job.type})`);
            } else if (await jobModel.failIfProcessing(job.id, 'Recovered: queue server restarted while job was running', false)) {
                console.log(`Recovery: marked interrupted job ${job.id} (${job.type}) as failed`);
            }
        } catch (error) {
            console.error(`Recovery: failed to recover job ${job.id}:`, error);
        }
    }
}

async function reapStaleJobs(jobModel, queueService, workerManager) {
    const graceSeconds = config.recovery.orphanGraceMinutes * 60;

    for (const job of await jobModel.getProcessing()) {
        const worker = job.worker_id ? workerManager.workers[job.worker_id] : null;
        const workerGone = !worker || Math.floor(worker.startedAt / 1000) > job.updated_epoch + 1;
        const timeoutHours = recoveryOf(job).timeoutHours || 0;
        const timedOut = timeoutHours > 0 && job.idle_seconds > timeoutHours * 3600 + graceSeconds;

        if (job.idle_seconds <= graceSeconds || (!workerGone && !timedOut)) {
            continue;
        }

        const reason = workerGone
            ? 'Recovered: worker exited while job was running'
            : `Recovered: job exceeded the ${timeoutHours}h time limit`;

        try {
            if (!await jobModel.failIfProcessing(job.id, reason, true)) {
                continue;
            }
            await scheduleRetry(jobModel, queueService, job.id);
            console.log(`Reaper: job ${job.id} (${job.type}) marked failed. ${reason}`);

            if (!workerGone) {
                await workerManager.restartWorker(job.worker_id);
                console.log(`Reaper: restarted worker ${job.worker_id} stuck on job ${job.id}`);
            }
        } catch (error) {
            console.error(`Reaper: failed to recover job ${job.id}:`, error);
        }
    }
}

module.exports = { normalizeRecoveryOptions, scheduleRetry, recoverInterruptedJobs, reapStaleJobs };