require('dotenv').config();

const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const config = require('./config');

async function initDatabase() {
    const db = await open({
        filename: config.sqlite.filename,
        driver: sqlite3.Database,
    });

    await db.exec(`
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
    `);

    await db.exec(`
        CREATE TABLE IF NOT EXISTS workers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            status TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            last_active DATETIME,
            created_at DATETIME DEFAULT (datetime('now', 'localtime')),
            updated_at DATETIME DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL,
            worker_id INTEGER,
            result TEXT,
            attempts INTEGER DEFAULT 0,
            next_attempt_at DATETIME,
            is_retry_enabled BOOLEAN DEFAULT 0,
            retry_delay INTEGER DEFAULT 1,
            retry_count INTEGER DEFAULT 5,
            created_at DATETIME DEFAULT (datetime('now', 'localtime')),
            updated_at DATETIME DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (worker_id) REFERENCES workers (id)
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
        CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (type);
        CREATE INDEX IF NOT EXISTS idx_workers_type ON workers (type);
        CREATE INDEX IF NOT EXISTS idx_workers_status ON workers (status);
        CREATE INDEX IF NOT EXISTS idx_jobs_pending_claim ON jobs (status, type, next_attempt_at, created_at);
    `);

    try {
        const columns = await db.all("PRAGMA table_info('jobs')");
        const colNames = columns.map(c => c.name);
        if (!colNames.includes('attempts')) {
            await db.exec(`ALTER TABLE jobs ADD COLUMN attempts INTEGER DEFAULT 0`);
        }
        if (!colNames.includes('next_attempt_at')) {
            await db.exec(`ALTER TABLE jobs ADD COLUMN next_attempt_at DATETIME`);
        }
        if (!colNames.includes('is_retry_enabled')) {
            await db.exec(`ALTER TABLE jobs ADD COLUMN is_retry_enabled BOOLEAN DEFAULT 0`);
        }
        if (!colNames.includes('retry_delay')) {
            await db.exec(`ALTER TABLE jobs ADD COLUMN retry_delay INTEGER DEFAULT 1`);
        }
        if (!colNames.includes('retry_count')) {
            await db.exec(`ALTER TABLE jobs ADD COLUMN retry_count INTEGER DEFAULT 5`);
        }

        await db.exec(`CREATE INDEX IF NOT EXISTS idx_jobs_next_attempt ON jobs (next_attempt_at)`);
        await db.exec(`CREATE INDEX IF NOT EXISTS idx_jobs_retry_sync ON jobs (status, is_retry_enabled, next_attempt_at)`);
    } catch (e) {
        console.error('Migration check failed:', e);
    }

    try {
        const workerColumns = await db.all("PRAGMA table_info('workers')");
        const workerColNames = workerColumns.map(c => c.name);
        if (!workerColNames.includes('is_active')) {
            await db.exec(`ALTER TABLE workers ADD COLUMN is_active INTEGER DEFAULT 1`);
        }
    } catch (e) {
        console.error('Worker migration check failed:', e);
    }

    return db;
}

module.exports = { initDatabase };