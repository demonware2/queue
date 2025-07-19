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
      const job = await this.db.get(
        `SELECT * FROM jobs WHERE status = 'pending' AND type = ? ORDER BY created_at ASC LIMIT 1`,
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
    await this.db.run(
      `UPDATE jobs SET status = ?, worker_id = ?, result = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
      [status, workerId, result ? JSON.stringify(result) : null, id]
    );
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