class Worker {
  constructor(db) {
    this.db = db;
  }

  async create(type) {
    const result = await this.db.run(
      `INSERT INTO workers (type, status, last_active) VALUES (?, ?, CURRENT_TIMESTAMP)`,
      [type, 'idle']
    );
    return result.lastID;
  }

  async getAvailable(type) {
    return await this.db.get(
      `SELECT * FROM workers WHERE status = 'idle' AND type = ? AND is_active = 1 ORDER BY last_active ASC LIMIT 1`,
      [type]
    );
  }

  async updateStatus(id, status) {
    await this.db.run(
      `UPDATE workers SET status = ?, last_active = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
      [status, id]
    );
  }

  async getAll() {
    return await this.db.all('SELECT * FROM workers');
  }

  async getByType(type) {
    return await this.db.all('SELECT * FROM workers WHERE type = ?', [type]);
  }

  async getStats() {
    return {
      total: await this.db.get('SELECT COUNT(*) as count FROM workers'),
      idle: await this.db.get('SELECT COUNT(*) as count FROM workers WHERE status = ?', ['idle']),
      busy: await this.db.get('SELECT COUNT(*) as count FROM workers WHERE status = ?', ['busy']),
      byType: await this.db.all('SELECT type, COUNT(*) as count FROM workers GROUP BY type')
    };
  }
}

module.exports = Worker;
