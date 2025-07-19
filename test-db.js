const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('/var/www/siroum-extension/queue-system/queue_system.db', (err) => {
  if (err) {
    console.error('Database opening error:', err);
    process.exit(1);
  }
  console.log('Database connected successfully');
  
  db.all("SELECT name FROM sqlite_master WHERE type='table'", (err, tables) => {
    if (err) {
      console.error('Error querying tables:', err);
    } else {
      console.log('Tables:', tables);
    }
    db.close(() => {
      console.log('Database connection closed');
    });
  });
});
