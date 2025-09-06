class Job {
    constructor(db) {
        this.db = db;
    }

    async create(type, payload) {
        const result = await this.db.run(
            `INSERT INTO jobs (type, payload, status) VALUES (?, ?, ?)`,
            [type, JSON.stringify(payload), 'pending']
        );
        return result.lastID;
    }

    async getNextPending(type) {
        try {
            let job;
            try {
                job = await this.db.get(
                    `SELECT * FROM jobs 
           WHERE status = 'pending' AND type = ? 
             AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
           ORDER BY created_at ASC LIMIT 1`,
                    [type]
                );
            } catch (e) {
                // Fallback if next_attempt_at column doesn't exist yet
                if (e && /no such column: next_attempt_at/i.test(e.message)) {
                    job = await this.db.get(
                        `SELECT * FROM jobs WHERE status = 'pending' AND type = ? ORDER BY created_at ASC LIMIT 1`,
                        [type]
                    );
                } else {
                    throw e;
                }
            }

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
        await this.db.run(
            `UPDATE jobs SET status = ?, worker_id = ?, result = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
            [status, workerId, result ? JSON.stringify(result) : null, id]
        );
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