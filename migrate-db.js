require('dotenv').config();
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const config = require('./config');

async function runMigration() {
    console.log('Connecting to database...');
    const db = await open({
        filename: config.sqlite.filename,
        driver: sqlite3.Database,
    });

    console.log('Running database migration...');
    try {
        const columns = await db.all("PRAGMA table_info('jobs')");
        const colNames = columns.map(c => c.name);

        if (!colNames.includes('is_retry_enabled')) {
            console.log('Adding column: is_retry_enabled');
            await db.exec(`ALTER TABLE jobs ADD COLUMN is_retry_enabled BOOLEAN DEFAULT 0`);
        }

        if (!colNames.includes('retry_delay')) {
            console.log('Adding column: retry_delay');
            await db.exec(`ALTER TABLE jobs ADD COLUMN retry_delay INTEGER DEFAULT 1`);
        }

        if (!colNames.includes('retry_count')) {
            console.log('Adding column: retry_count');
            await db.exec(`ALTER TABLE jobs ADD COLUMN retry_count INTEGER DEFAULT 5`);
        }

        console.log('Migration complete.');
    } catch (e) {
        console.error('Migration failed:', e);
    } finally {
        await db.close();
    }
}

runMigration();
