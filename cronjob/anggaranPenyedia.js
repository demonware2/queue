const axios = require('axios');
const Redis = require('ioredis');
const mysql = require('mysql2/promise');
const fs = require('fs');
const util = require('util');
const path = require('path');
const readFile = util.promisify(fs.readFile);
const dotenv = require('dotenv');

dotenv.config();

const config = {
  redis: {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: process.env.REDIS_PORT || 6379
  },
  database: {
    host: process.env.DB_ISB_HOST || 'localhost',
    user: process.env.DB_ISB_USER || 'root',
    password: process.env.DB_ISB_PASS || '',
    database: process.env.DB_ISB_NAME || 'isb'
  }
};

const logToConsole = (message, type = 'info') => {
  const timestamp = new Date().toISOString();
  const colorCodes = {
    error: '\x1b[31m',
    warning: '\x1b[33m',
    info: '\x1b[32m',
    reset: '\x1b[0m'
  };
  
  const logMessage = `[${timestamp}] ${type.toUpperCase()}: ${message}`;
  
  console.log(`${colorCodes[type] || ''}${logMessage}${colorCodes.reset}`);
  
  try {
    const logDir = path.join(process.cwd(), 'logs');
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }
    
    const today = new Date().toISOString().slice(0, 10);
    const logFile = path.join(logDir, `anggaranpenyedia-${today}.log`);
    
    fs.appendFileSync(logFile, logMessage + '\n');
  } catch (logError) {
    console.error(`Failed to write to log file: ${logError.message}`);
  }
};

let redis;
try {
  redis = new Redis(config.redis);
  redis.on('error', (err) => {
    logToConsole(`Redis error: ${err.message}`, 'error');
  });
} catch (error) {
  logToConsole(`Redis connection failed: ${error.message}`, 'error');
  process.exit(1);
}

let dbPool;

async function initializeDatabase() {
  try {
    dbPool = await mysql.createPool({
      host: config.database.host,
      user: config.database.user,
      password: config.database.password,
      database: config.database.database,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0
    });
    
    const [rows] = await dbPool.query('SELECT 1');
    logToConsole('Database connection initialized successfully');
    return true;
  } catch (error) {
    logToConsole(`Database connection failed: ${error.message}`, 'error');
    return false;
  }
}

async function initializeRedis() {
  try {
    const pingResponse = await redis.ping();
    if (pingResponse === 'PONG') {
      logToConsole('Redis connection successful');
      return true;
    } else {
      logToConsole('Redis ping failed', 'error');
      return false;
    }
  } catch (error) {
    logToConsole(`Redis initialization failed: ${error.message}`, 'error');
    return false;
  }
}

let lastRequestTime = 0;

async function executeCurl(url) {
  try {
    const currentTime = Date.now();
    const timeSinceLastRequest = currentTime - lastRequestTime;
    
    if (timeSinceLastRequest < 500) {
      await new Promise(resolve => setTimeout(resolve, 500 - timeSinceLastRequest));
    }
    
    lastRequestTime = Date.now();
    
    const response = await axios.get(url, {
      headers: {
        'Content-Type': 'application/json'
      },
      validateStatus: null,
      maxRedirects: 5,
      timeout: 30000
    });

    if (response.status < 200 || response.status >= 300) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    return response.data;
  } catch (error) {
    logToConsole(`cURL error: ${error.message}`, 'error');
    return false;
  }
}

async function getTahunIsb() {
  try {
    const currentYear = new Date().getFullYear();
    const lastYear = currentYear - 1;
    const nextYear = currentYear + 1;

    const now = new Date();
    const currentDate = `${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
    
    const deadlineDate = '02-05';
    const startNextYearDate = '11-01';

    if (currentDate < deadlineDate) {
      return [lastYear, currentYear];
    } else if (currentDate >= startNextYearDate) {
      return [currentYear, nextYear];
    } else {
      return [currentYear];
    }
  } catch (error) {
    logToConsole(`Error getting ISB years: ${error.message}`, 'error');
    return [new Date().getFullYear()];
  }
}

async function transform(tahun) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();
    
    const [rows] = await connection.query(
      `SELECT * FROM api_paketanggaranpenyedia 
       WHERE tahun = ? 
       ORDER BY created DESC 
       LIMIT 1`,
      [tahun]
    );
    
    if (rows.length === 0) {
      throw new Error("No data found in api_paketanggaranpenyedia");
    }
    
    const row = rows[0];
    let decode;
    
    try {
      if (typeof row.response === 'object') {
        decode = row.response;
      } else {
        decode = JSON.parse(row.response);
      }
    } catch (error) {
      throw new Error(`Failed to decode JSON: ${error.message}`);
    }
    
    await connection.query(
      `DELETE FROM paketanggaranpenyedia WHERE tahun_anggaran_dana = ?`,
      [tahun]
    );
    
    const revisi = decode.map(v => [
      v.kd_rup || null,
      v.kd_rup_lokal || null,
      v.kd_komponen || null,
      v.kd_kegiatan || null,
      v.pagu || null,
      v.mak || null,
      v.sumber_dana || null,
      v.tahun_anggaran || null,
      null,
      new Date()
    ]);
    
    if (revisi.length > 0) {
      await connection.query(
        `INSERT INTO paketanggaranpenyedia 
         (koderup, id_rup_client, kodekomponen, kodekegiatan, 
          pagu, mak, sumberdana, tahun_anggaran_dana, kodeobjekakun, create_date) 
         VALUES ?`,
        [revisi]
      );
    }
    
    await connection.commit();
    logToConsole(`Transform successful for year ${tahun}!`, 'info');
    return true;
    
  } catch (error) {
    await connection.rollback();
    logToConsole(`Transform failed: ${error.message}`, 'error');
    return false;
  } finally {
    connection.release();
  }
}

async function master(tahun) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();
    
    const [rows] = await connection.query(
      `SELECT * FROM api_paketanggaranpenyedia 
       WHERE tahun = ? 
       ORDER BY created DESC 
       LIMIT 1`,
      [tahun]
    );
    
    if (rows.length === 0) {
      throw new Error("No data found in api_paketanggaranpenyedia");
    }
    
    const row = rows[0];
    let decode;
    
    try {
      if (typeof row.response === 'object') {
        decode = row.response;
      } else {
        decode = JSON.parse(row.response);
      }
    } catch (error) {
      throw new Error(`Failed to decode JSON: ${error.message}`);
    }
    
    await connection.query(
      `DELETE FROM paket_sirupmak_tm 
       WHERE jenispaket = 'PENYEDIA' AND tahun_anggaran_dana = ?`,
      [tahun]
    );

    const revisi = decode.map(v => [
      null,
      'PENYEDIA',
      v.kd_rup || null,
      v.pagu || null,
      v.mak || null,
      v.sumber_dana || null,
      v.tahun_anggaran || null
    ]);
    
    if (revisi.length > 0) {
      await connection.query(
        `INSERT INTO paket_sirupmak_tm
         (id_siruptm, jenispaket, koderup, pagu, mak, sumberdana, tahun_anggaran_dana) 
         VALUES ?`,
        [revisi]
      );
    }
    
    await connection.commit();
    logToConsole(`Master successful for year ${tahun}!`, 'info');
    return true;
    
  } catch (error) {
    await connection.rollback();
    logToConsole(`Master failed: ${error.message}`, 'error');
    return false;
  } finally {
    connection.release();
  }
}

async function deleteRedis() {
  try {
    const keys = await redis.keys("*indekspbj*");
    
    for (const key of keys) {
      logToConsole(`Deleting key: ${key}`, 'info');
      await redis.del(key);
    }
    
    logToConsole('Redis keys deleted successfully', 'info');
    return true;
  } catch (error) {
    logToConsole(`Failed to delete Redis keys: ${error.message}`, 'error');
    return false;
  }
}

async function update() {
  try {
    logToConsole('Starting AnggaranPenyedia update process', 'info');
    
    if (!await initializeRedis()) {
      const message = "Redis server is unavailable!";
      logToConsole(message, 'error');
      return;
    }

    const tahuns = await getTahunIsb();
    
    for (const tahun of tahuns) {
      logToConsole(`Starting update for year: ${tahun}`, 'warning');
      
      const luaScriptPath = path.resolve(process.cwd(), 'limiter_api_ISB.lua');
      const luaScript = await readFile(luaScriptPath, 'utf8');
      const scriptSha = await redis.script('load', luaScript);
      
      const maxRetries = 5;
      const retryInterval = 1;
      
      let success = false;
      
      for (let retries = 0; retries < maxRetries; retries++) {
        try {
          const isAllowed = await redis.evalsha(scriptSha, 0);
          
          if (isAllowed) {
            const url = `https://isb.lkpp.go.id/isb-2/api/dd124d53-3009-4121-b44d-9f2516ae14f4/json/9453/RUP-PaketAnggaranPenyedia/tipe/4:12/parameter/${tahun}:L15`;
            
            logToConsole(`Fetching data from: ${url}`, 'info');
            const result = await executeCurl(url);
            
            if (!result) {
              logToConsole("Failed to fetch data from API!", 'error');
              await new Promise(resolve => setTimeout(resolve, retryInterval * 1000));
              continue;
            }
            
            const connection = await dbPool.getConnection();
            
            try {
              await connection.beginTransaction();
              
              const [countResult] = await connection.query(
                'SELECT COUNT(*) as total FROM api_paketanggaranpenyedia'
              );
              const totalRows = countResult[0].total;
              
              if (totalRows >= 14) {
                const [oldestRow] = await connection.query(
                  'SELECT id FROM api_paketanggaranpenyedia ORDER BY created ASC LIMIT 1'
                );
                
                if (oldestRow.length > 0) {
                  await connection.query(
                    'DELETE FROM api_paketanggaranpenyedia WHERE id = ?',
                    [oldestRow[0].id]
                  );
                }
              }
              
              const resultJson = typeof result === 'object' ? JSON.stringify(result) : result;
              await connection.query(
                'INSERT INTO api_paketanggaranpenyedia (request, response, tahun) VALUES (?, ?, ?)',
                [url, resultJson, tahun]
              );
              
              await connection.commit();
              logToConsole(`API data saved successfully for year ${tahun}!`, 'info');
              
              const transformResult = await transform(tahun);
              if (transformResult) {
                logToConsole(`Transform successful for year ${tahun}!`, 'info');
              } else {
                logToConsole(`Transform failed for year ${tahun}`, 'error');
              }
              
              const masterResult = await master(tahun);
              if (masterResult) {
                logToConsole(`Master successful for year ${tahun}!`, 'info');
              } else {
                logToConsole(`Master failed for year ${tahun}`, 'error');
              }
              
              success = true;
              break;
              
            } catch (error) {
              await connection.rollback();
              logToConsole(`Database error: ${error.message}`, 'error');
            } finally {
              connection.release();
            }
          } else {
            logToConsole("Rate limit hit, retrying...", 'warning');
            await new Promise(resolve => setTimeout(resolve, retryInterval * 1000));
          }
        } catch (error) {
          logToConsole(`Error in update process: ${error.message}`, 'error');
          await new Promise(resolve => setTimeout(resolve, retryInterval * 1000));
        }
      }
      
      if (!success) {
        logToConsole(`Failed to update year ${tahun} after ${maxRetries} attempts`, 'error');
      }
    }
    
    await deleteRedis();
    
    logToConsole('Update process completed successfully', 'info');
    return true;
  } catch (error) {
    logToConsole(`Error in update process: ${error.message}`, 'error');
    return false;
  }
}

async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  const taskIdArg = args.find(arg => arg.startsWith('--task-id='));
  const taskId = taskIdArg ? taskIdArg.split('=')[1] : null;
  
  if (command === 'update') {
    try {
      if (taskId) {
        logToConsole(`Starting update with task ID: ${taskId}`, 'info');
      }

      if (!await initializeDatabase()) {
        process.exit(1);
      }

      await update();
      
      process.exit(0);
    } catch (error) {
      logToConsole(`Fatal error: ${error.message}`, 'error');
      process.exit(1);
    }
  } else {
    logToConsole('Usage: node anggaranPenyedia.js update [--task-id=X]', 'info');
    process.exit(1);
  }
}

main();