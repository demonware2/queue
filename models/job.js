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
            `INSERT INTO jobs (type, payload, status, is_retry_enabled, retry_delay, retry_count) 
             VALUES (?, ?, ?, ?, ?, ?)`,
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
               AND next_attempt_at <= CURRENT_TIMESTAMP`
        );
    }

    async getNextPending(type) {
        try {
            const job = await this.db.get(
                `SELECT * FROM jobs 
                 WHERE status = 'pending' AND type = ? 
                 AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
                 ORDER BY created_at ASC LIMIT 1`,
                [type]
            );

            if (!job) {
                return null;
            }

            const updateResult = await this.db.run(
                `UPDATE jobs SET status = 'processing', updated_at = CURRENT_TIMESTAMP
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

    async updateStatus(id, status, workerId = null, result = null) {
        const job = await this.getById(id);
        if (!job) return;

        if (status === 'failed' && job.is_retry_enabled && job.attempts < job.retry_count) {
            const nextAttemptAt = new Date(Date.now() + job.retry_delay * 60 * 60 * 1000).toISOString();
            await this.db.run(
                `UPDATE jobs SET status = ?, worker_id = ?, result = ?, attempts = attempts + 1, next_attempt_at = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
                ['failed', workerId, result ? JSON.stringify(result) : null, nextAttemptAt, id]
            );
            console.log(`Job ${id} scheduled for retry. Attempt: ${job.attempts + 1} of ${job.retry_count}. Next attempt at: ${nextAttemptAt}`);
        } else {
            await this.db.run(
                `UPDATE jobs SET status = ?, worker_id = ?, result = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
                [status, workerId, result ? JSON.stringify(result) : null, id]
            );
        }
    }

    async getById(id) {
        return await this.db.get(`SELECT * FROM jobs WHERE id = ?`, [id]);
    }

    async updateRetrySchedule(id, attempts, nextAttemptAt) {
        await this.db.run(
            `UPDATE jobs SET attempts = ?, next_attempt_at = ?, status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
            [attempts, nextAttemptAt, id]
        );
    }

    async claimIfPending(id) {
        try {
            const result = await this.db.run(
                `UPDATE jobs SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'pending'`,
                [id]
            );
            return result.changes > 0;
        } catch (error) {
            console.error('Error claiming job if pending:', error);
            throw error;
        }
    }

    async getStats() {
        return {
            pending: await this.db.get('SELECT COUNT(*) as count FROM jobs WHERE status = ?', ['pending']),
            processing: await this.db.get('SELECT COUNT(*) as count FROM jobs WHERE status = ?', ['processing']),
            completed: await this.db.get('SELECT COUNT(*) as count FROM jobs WHERE status = ?', ['completed']),
            failed: await this.db.get('SELECT COUNT(*) as count FROM jobs WHERE status = ?', ['failed']),
            byType: await this.db.all('SELECT type, COUNT(*) as count FROM jobs GROUP BY type')
        };
    }
}

module.exports = Job;