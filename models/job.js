const config = require('../config');

class Job {
    constructor(db) {
        this.db = db;
    }

    async create(type, payload, options = {}) {
        const {
            isRetryEnabled = false,
            retryDelay = 1,
            retryCount = 5
        } = options;

        const result = await this.db.run(
            `INSERT INTO jobs (type, payload, status, is_retry_enabled, retry_delay, retry_count, created_at, updated_at) 
             VALUES (?, ?, ?, ?, ?, ?, datetime('now', 'localtime'), datetime('now', 'localtime'))`,
            [type, JSON.stringify(payload), 'pending', isRetryEnabled, retryDelay, retryCount]
        );
        return result.lastID;
    }

    async getDueForRetry() {
        return this.db.all(
            `SELECT * FROM jobs 
             WHERE status = 'failed' 
               AND is_retry_enabled = 1
               AND attempts < retry_count
               AND next_attempt_at <= datetime('now', 'localtime')`
        );
    }

    async getNextPending(type) {
        try {
            const job = await this.db.get(
                `SELECT * FROM jobs 
                 WHERE status = 'pending' AND type = ? 
                 AND (next_attempt_at IS NULL OR next_attempt_at <= datetime('now', 'localtime'))
                 ORDER BY created_at ASC LIMIT 1`,
                [type]
            );

            if (!job) {
                return null;
            }

            const updateResult = await this.db.run(
                `UPDATE jobs SET status = 'processing', updated_at = datetime('now', 'localtime')
                 WHERE id = ? AND status = 'pending'`,
                [job.id]
            );

            if (updateResult.changes === 0) {
                console.log(`Job ${job.id} was claimed by another worker`);
                return null;
            }

            return job;
        } catch (error) {
            console.error('Error getting next pending job:', error);
            throw error;
        }
    }

    async updateStatus(id, status, workerId = null, result = null, options = {}) {
        const serializedResult = result ? JSON.stringify(result) : null;

        if (status === 'failed') {
            const job = await this.getById(id);
            if (!job) return;

            const { manageRetry = true } = options ?? {};

            if (manageRetry && job.is_retry_enabled && job.attempts < job.retry_count) {
                const d = new Date(Date.now() + Math.max(job.retry_delay * 60 * 60 * 1000, config.retry.minDelayMinutes * 60 * 1000));
                const pad = (n) => String(n).padStart(2, '0');
                const nextAttemptAt = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
                await this.db.run(
                    `UPDATE jobs SET status = ?, worker_id = ?, result = ?, attempts = attempts + 1, next_attempt_at = ?, updated_at = datetime('now', 'localtime') WHERE id = ?`,
                    ['failed', workerId, serializedResult, nextAttemptAt, id]
                );
                console.log(`Job ${id} scheduled for retry. Attempt: ${job.attempts + 1} of ${job.retry_count}. Next attempt at: ${nextAttemptAt}`);
            } else if (manageRetry) {
                await this.db.run(
                    `UPDATE jobs SET status = ?, worker_id = ?, result = ?, next_attempt_at = NULL, updated_at = datetime('now', 'localtime') WHERE id = ?`,
                    ['failed', workerId, serializedResult, id]
                );
            } else {
                await this.db.run(
                    `UPDATE jobs SET status = ?, worker_id = ?, result = ?, updated_at = datetime('now', 'localtime') WHERE id = ?`,
                    ['failed', workerId, serializedResult, id]
                );
            }
        } else {
            await this.db.run(
                `UPDATE jobs SET status = ?, worker_id = ?, result = ?, updated_at = datetime('now', 'localtime') WHERE id = ?`,
                [status, workerId, serializedResult, id]
            );
        }
    }

    async getById(id) {
        return await this.db.get(`SELECT * FROM jobs WHERE id = ?`, [id]);
    }

    async updateRetrySchedule(id, attempts, nextAttemptAt) {
        await this.db.run(
            `UPDATE jobs SET attempts = ?, next_attempt_at = ?, status = 'pending', updated_at = datetime('now', 'localtime') WHERE id = ?`,
            [attempts, nextAttemptAt, id]
        );
    }

    async claimIfPending(id, workerId = null) {
        try {
            const result = await this.db.run(
                `UPDATE jobs SET status = 'processing', worker_id = COALESCE(?, worker_id), updated_at = datetime('now', 'localtime') WHERE id = ? AND status = 'pending'`,
                [workerId, id]
            );
            return result.changes > 0;
        } catch (error) {
            console.error('Error claiming job if pending:', error);
            throw error;
        }
    }

    async getProcessing() {
        return this.db.all(
            `SELECT id, type, payload, worker_id, attempts, retry_count, is_retry_enabled,
                    CAST(strftime('%s', updated_at, 'utc') AS INTEGER) AS updated_epoch,
                    CAST(strftime('%s', 'now') AS INTEGER) - CAST(strftime('%s', updated_at, 'utc') AS INTEGER) AS idle_seconds
             FROM jobs WHERE status = 'processing'`
        );
    }

    async requeueIfProcessing(id, note) {
        const result = await this.db.run(
            `UPDATE jobs SET status = 'pending', worker_id = NULL, attempts = attempts + 1, next_attempt_at = NULL,
                    result = ?, updated_at = datetime('now', 'localtime')
             WHERE id = ? AND status = 'processing'`,
            [JSON.stringify({ recovered: note }), id]
        );
        return result.changes > 0;
    }

    async failIfProcessing(id, reason, manageRetry = true) {
        const job = await this.getById(id);
        const result = await this.db.run(
            `UPDATE jobs SET status = 'failed', updated_at = datetime('now', 'localtime') WHERE id = ? AND status = 'processing'`,
            [id]
        );
        if (result.changes === 0) {
            return false;
        }
        await this.updateStatus(id, 'failed', job ? job.worker_id : null, { error: reason }, { manageRetry });
        return true;
    }

    async requeueFailedForRetry(id) {
        const result = await this.db.run(
            `UPDATE jobs SET status = 'pending', worker_id = NULL, result = NULL, updated_at = datetime('now', 'localtime')
             WHERE id = ? AND status = 'failed'`,
            [id]
        );
        return result.changes > 0;
    }

    async getStats() {
        const stats = await this.db.get(`
            SELECT 
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM jobs
        `);

        const byType = await this.db.all('SELECT type, COUNT(*) as count FROM jobs GROUP BY type');

        return {
            pending: { count: stats.pending || 0 },
            processing: { count: stats.processing || 0 },
            completed: { count: stats.completed || 0 },
            failed: { count: stats.failed || 0 },
            byType
        };
    }
}

module.exports = Job;