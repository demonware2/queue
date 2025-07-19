// Fixed version of asetSimanApi.js
const axios = require('axios');
const Redis = require('ioredis');
const mysql = require('mysql2/promise');
const fs = require('fs');
const util = require('util');
const path = require('path');
const readFile = util.promisify(fs.readFile);
const dotenv = require('dotenv');

// Load environment variables
dotenv.config();

// Configuration object
const config = {
  redis: {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: process.env.REDIS_PORT || 6379
  },
  database: {
    host: process.env.DB_SIMAN_HOST || 'localhost',
    user: process.env.DB_SIMAN_USER || 'root',
    password: process.env.DB_SIMAN_PASS || '',
    database: process.env.DB_SIMAN_NAME || 'siman'
  },
  api: {
    tokenUrl: process.env.SIMAN_API_TOKEN_URL,
    clientId: process.env.SIMAN_CLIENT_ID,
    clientSecret: process.env.SIMAN_CLIENT_SECRET,
    grantType: process.env.SIMAN_GRANT_TYPE,
    baKey: process.env.SIMAN_BA_KEY
  }
};

// Set up logger with file logging
const logToConsole = (message, type = 'info') => {
  const timestamp = new Date().toISOString();
  const colorCodes = {
    error: '\x1b[31m', // Red
    warning: '\x1b[33m', // Yellow
    info: '\x1b[32m', // Green
    reset: '\x1b[0m' // Reset
  };
  
  const logMessage = `[${timestamp}] ${type.toUpperCase()}: ${message}`;
  
  // Console output with colors
  console.log(`${colorCodes[type] || ''}${logMessage}${colorCodes.reset}`);
  
  // Also write to file log
  try {
    const logDir = path.join(process.cwd(), 'logs');
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }
    
    const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    const logFile = path.join(logDir, `asetsiman-${today}.log`);
    
    fs.appendFileSync(logFile, logMessage + '\n');
  } catch (logError) {
    console.error(`Failed to write to log file: ${logError.message}`);
  }
};

// Create Redis client
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

// Database connection pool
let dbPool;

// Initialize database connection
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
    
    // Test the connection
    const [rows] = await dbPool.query('SELECT 1');
    logToConsole('Database connection initialized successfully');
    return true;
  } catch (error) {
    logToConsole(`Database connection failed: ${error.message}`, 'error');
    return false;
  }
}

// Initialize Redis
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

// Get Access Token
async function getTokenBMN(url, data, key) {
  try {
    const keyRedis = 'tokenBMNSimanV1';

    if (key === 'get') {
      const cachedData = await redis.get(keyRedis);

      if (!cachedData) {
        const result = await curlBMNToken(url, data);

        if (result === false) {
          return false;
        }

        const token = result.access_token;
        const expiresIn = result.expires_in;

        await redis.set(keyRedis, token);
        await redis.expire(keyRedis, expiresIn);
        
        return token;
      } else {
        return cachedData;
      }
    } else if (key === 'delete') {
      await redis.del(keyRedis);
      return true;
    } else {
      throw new Error('Invalid key');
    }
  } catch (error) {
    logToConsole(`Failed to cache data to Redis: ${error.message}`, 'error');

    if (key === 'get') {
      const result = await curlBMNToken(url, data);

      if (result === false) {
        return false;
      }

      return result.access_token;
    }
    
    return false;
  }
}

// Execute curl request for token
async function curlBMNToken(url, data) {
  try {
    const response = await axios.post(url, new URLSearchParams(data), {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      validateStatus: null, // Don't throw on any status code
      maxRedirects: 5
    });

    if (response.status < 200 || response.status >= 300) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    if (typeof response.data !== 'object') {
      throw new Error('Failed to decode JSON response');
    }

    return response.data;
  } catch (error) {
    logToConsole(`cURL error for token: ${error.message}`, 'error');
    return false;
  }
}

// Get Access Token with retries
async function getAccessToken(url, data, maxRetries, retryInterval, scriptSha) {
  for (let retries = 0; retries < maxRetries; retries++) {
    try {
      // FIX: Need to pass 0 as the number of keys for evalsha
      const isAllowed = await redis.evalsha(scriptSha, 0);

      if (isAllowed) {
        const token = await getTokenBMN(url, data, 'get');

        if (token !== false && token !== '') {
          return token;
        }
      }
      
      // Sleep before retry
      await new Promise(resolve => setTimeout(resolve, retryInterval * 1000));
    } catch (error) {
      logToConsole(`Error getting access token: ${error.message}`, 'error');
      await new Promise(resolve => setTimeout(resolve, retryInterval * 1000));
    }
  }

  logToConsole("Failed to fetch token from API!", 'error');
  return false;
}

// Track the last request time
let lastRequestTime = 0;

// Execute API request
async function executeCurl(url, token) {
  try {
    // Add delay between requests to reduce server load
    const currentTime = Date.now();
    const timeSinceLastRequest = currentTime - lastRequestTime;
    
    if (timeSinceLastRequest < 500) { // Half second between requests
      await new Promise(resolve => setTimeout(resolve, 500 - timeSinceLastRequest));
    }
    
    lastRequestTime = Date.now();
    
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      validateStatus: null, // Don't throw on any status code
      maxRedirects: 5,
      timeout: 30000 // 30 seconds timeout
    });

    if (response.status < 200 || response.status >= 300) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    console.log(response.data);

    return response.data;
  } catch (error) {
    logToConsole(`cURL error: ${error.message}`, 'error');
    return false;
  }
}

// Get last update date for a table
async function getLastUpdate(tableName) {
  try {
    const [rows] = await dbPool.query(
      `SELECT MAX(TGL_TARIK) as last_update FROM ${tableName}`
    );
    
    return rows[0].last_update || false;
  } catch (error) {
    logToConsole(`Error getting last update for ${tableName}: ${error.message}`, 'error');
    return false;
  }
}

// Insert API data to database
async function inputApi(url, data, keyTable, tableMapping) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();

    const jsonData = JSON.stringify(data);

    const dataInput = {
      request: url,
      response: jsonData
    };

    if (tableMapping[keyTable]) {
      const tableName = tableMapping[keyTable];

      // Check total rows
      const [countResult] = await connection.query(
        `SELECT COUNT(*) as total FROM ${tableName}`
      );
      const totalRows = countResult[0].total;

      // Delete oldest row if needed
      if (totalRows >= 4) {
        const [oldestRow] = await connection.query(
          `SELECT id FROM ${tableName} ORDER BY created ASC LIMIT 1`
        );

        if (oldestRow.length > 0) {
          await connection.query(
            `DELETE FROM ${tableName} WHERE id = ?`,
            [oldestRow[0].id]
          );
        }
      }

      // Insert new record
      await connection.query(
        `INSERT INTO ${tableName} SET ?`,
        dataInput
      );
    } else {
      throw new Error(`Invalid keyTable provided: ${keyTable}`);
    }

    await connection.commit();
    logToConsole(`Success saving data for ${keyTable}!`, 'info');
    return true;
  } catch (error) {
    await connection.rollback();
    logToConsole(`Error processing data: ${error.message}`, 'error');
    return false;
  } finally {
    connection.release();
  }
}

// Delete Redis keys
async function deleteRedis() {
  try {
    const keys = await redis.keys("*simanBMN:*");
    
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

// Main update function
async function update() {
  let allOutput = '';
  try {
    logToConsole('Starting AsetSimanApi update process', 'info');
    
    // Initialize Redis if not already done
    if (!await initializeRedis()) {
      const message = "Redis server is unavailable!";
      logToConsole(message, 'error');
      return;
    }

    // Load Lua script
    const luaScriptPath = path.resolve(process.cwd(), 'limiter_api_BMN.lua');
    const luaScript = await readFile(luaScriptPath, 'utf8');
    const scriptSha = await redis.script('load', luaScript);
    logToConsole('Lua script loaded successfully', 'info');
    
    // Configuration
    const maxRetries = 5;
    const retryInterval = 1;

    // Get access token
    const url = config.api.tokenUrl;
    const data = {
      client_id: config.api.clientId,
      client_secret: config.api.clientSecret,
      grant_type: config.api.grantType
    };

    const token = await getAccessToken(url, data, maxRetries, retryInterval, scriptSha);

    if (token === false || token === '') {
      const message = "Failed to fetch token from API!";
      logToConsole(message, 'error');
      return;
    }

    const baKey = config.api.baKey;

    // Table definitions
    const tabelBMN = [
      'getAsetAlatAngkutan',
      'getAsetAlatBerat',
      'getAsetBangunanAir',
      'getAsetBPYBDS',
      'getAsetGedungBangunan',
      'getAsetHibah',
      'getAsetHilang2023',
      'getAsetHilang',
      'getAsetInstalasiJaringan',
      'getAsetJalanJembatan',
      'getAsetKDP',
      'getAsetPersediaan',
      'getAsetPihakKe3',
      'getAsetPMNonTIK',
      'getAsetPMTIK',
      'getAsetRenovasi',
      'getAsetRumahNegara',
      'getAsetRusakBerat',
      'getAsetSenjata',
      'getAsetTakBerwujud',
      'getAsetTanah',
      'getAsetTetapLainnya',
      'getAsetRumahNegaraUp',
      'getAsetTanahUpdate',
      'getAsetHentiGuna'
    ];

    const tableMapping = {
      'getAsetTanahUpdate': 'API_SIMAN_ASET_TANAH_UPDATE',
      'getAsetAlatAngkutan': 'API_SIMAN_ASET_ALAT_ANGKUTAN',
      'getAsetAlatBerat': 'API_SIMAN_ASET_ALAT_BERAT',
      'getAsetBangunanAir': 'API_SIMAN_ASET_BANGUNAN_AIR',
      'getAsetBPYBDS': 'API_SIMAN_ASET_BPYBDS',
      'getAsetGedungBangunan': 'API_SIMAN_ASET_GEDUNG_BANGUNAN',
      'getAsetHibah': 'API_SIMAN_ASET_HIBAH',
      'getAsetHilang2023': 'API_SIMAN_ASET_HILANG_2023',
      'getAsetHilang': 'API_SIMAN_ASET_HILANG',
      'getAsetInstalasiJaringan': 'API_SIMAN_ASET_INSTALASI_JARINGAN',
      'getAsetJalanJembatan': 'API_SIMAN_ASET_JALAN_JEMBATAN',
      'getAsetKDP': 'API_SIMAN_ASET_KDP',
      'getAsetPersediaan': 'API_SIMAN_ASET_PERSEDIAAN',
      'getAsetPihakKe3': 'API_SIMAN_ASET_PIHAK_KE3',
      'getAsetPMNonTIK': 'API_SIMAN_ASET_PM_NON_TIK',
      'getAsetPMTIK': 'API_SIMAN_ASET_PM_TIK',
      'getAsetRenovasi': 'API_SIMAN_ASET_RENOVASI',
      'getAsetRumahNegara': 'API_SIMAN_ASET_RUMAH_NEGARA',
      'getAsetRusakBerat': 'API_SIMAN_ASET_RUSAK_BERAT',
      'getAsetSenjata': 'API_SIMAN_ASET_SENJATA',
      'getAsetTakBerwujud': 'API_SIMAN_ASET_TAK_BERWUJUD',
      'getAsetTanah': 'API_SIMAN_ASET_TANAH',
      'getAsetTetapLainnya': 'API_SIMAN_ASET_TETAP_LAINNYA',
      'getAsetRumahNegaraUp': 'API_SIMAN_ASET_RUMAH_NEGARA_UP',
      'getAsetHentiGuna': 'API_SIMAN_ASET_HENTI_GUNA'
    };

    const tableData = {
      'API_SIMAN_ASET_ALAT_ANGKUTAN': 'DATA_SIMAN_ASET_ALAT_ANGKUTAN',
      'API_SIMAN_ASET_ALAT_BERAT': 'DATA_SIMAN_ASET_ALAT_BERAT',
      'API_SIMAN_ASET_BANGUNAN_AIR': 'DATA_SIMAN_ASET_BANGUNAN_AIR',
      'API_SIMAN_ASET_BPYBDS': 'DATA_SIMAN_ASET_BPYBDS',
      'API_SIMAN_ASET_GEDUNG_BANGUNAN': 'DATA_SIMAN_ASET_GEDUNG_BANGUNAN',
      'API_SIMAN_ASET_HIBAH': 'DATA_SIMAN_ASET_HIBAH',
      'API_SIMAN_ASET_HILANG_2023': 'DATA_SIMAN_ASET_HILANG_2023',
      'API_SIMAN_ASET_HILANG': 'DATA_SIMAN_ASET_HILANG',
      'API_SIMAN_ASET_INSTALASI_JARINGAN': 'DATA_SIMAN_ASET_INSTALASI_JARINGAN',
      'API_SIMAN_ASET_JALAN_JEMBATAN': 'DATA_SIMAN_ASET_JALAN_JEMBATAN',
      'API_SIMAN_ASET_KDP': 'DATA_SIMAN_ASET_KDP',
      'API_SIMAN_ASET_PERSEDIAAN': 'DATA_SIMAN_ASET_PERSEDIAAN',
      'API_SIMAN_ASET_PIHAK_KE3': 'DATA_SIMAN_ASET_PIHAK_KE3',
      'API_SIMAN_ASET_PM_NON_TIK': 'DATA_SIMAN_ASET_PM_NON_TIK',
      'API_SIMAN_ASET_PM_TIK': 'DATA_SIMAN_ASET_PM_TIK',
      'API_SIMAN_ASET_RENOVASI': 'DATA_SIMAN_ASET_RENOVASI',
      'API_SIMAN_ASET_RUMAH_NEGARA': 'DATA_SIMAN_ASET_RUMAH_NEGARA',
      'API_SIMAN_ASET_RUSAK_BERAT': 'DATA_SIMAN_ASET_RUSAK_BERAT',
      'API_SIMAN_ASET_SENJATA': 'DATA_SIMAN_ASET_SENJATA',
      'API_SIMAN_ASET_TAK_BERWUJUD': 'DATA_SIMAN_ASET_TAK_BERWUJUD',
      'API_SIMAN_ASET_TANAH': 'DATA_SIMAN_ASET_TANAH',
      'API_SIMAN_ASET_TETAP_LAINNYA': 'DATA_SIMAN_ASET_TETAP_LAINNYA',
      'API_SIMAN_ASET_RUMAH_NEGARA_UP': 'DATA_SIMAN_ASET_RUMAH_NEGARA_UP',
      'API_SIMAN_ASET_TANAH_UPDATE': 'DATA_SIMAN_ASET_TANAH_UPDATE',
      'API_SIMAN_ASET_HENTI_GUNA': 'DATA_SIMAN_ASET_HENTI_GUNA'
    };

    // Process each table
    for (const value of tabelBMN) {
      let lastUpdate = false;
      
      if (tableMapping[value]) {
        const tableApiName = tableMapping[value];
        if (tableData[tableApiName]) {
          const tableName = tableData[tableApiName];
          logToConsole(`Get last update ${tableName}`, 'info');
          allOutput += `Get last update ${tableName}\n`;
          
          try {
            lastUpdate = await getLastUpdate(tableName);
          } catch (error) {
            logToConsole(`Error getting last update for ${tableName}: ${error.message}`, 'error');
            allOutput += `Error getting last update for ${tableName}: ${error.message}\n`;
            continue;
          }

          if (lastUpdate === false) {
            logToConsole(`Fetching all data for ${value}`, 'warning');
            allOutput += `Fetching all data for ${value}\n`;
          } else {
            logToConsole(`Checking for updates since ${lastUpdate} for ${value}`, 'warning');
            allOutput += `Checking for updates since ${lastUpdate} for ${value}\n`;
          }
        } else {
          logToConsole('Invalid tableName provided', 'error');
          allOutput += `Invalid tableName provided for ${value}\n`;
          continue;
        }
      } else {
        logToConsole('Invalid keyTable provided', 'error');
        allOutput += `Invalid keyTable provided: ${value}\n`;
        continue;
      }

      let firstId = 1;
      const increment = 1000;

      const urlBase = `https://apigateway.kemenkeu.go.id/gateway/SLDKSimanKL/1.0/${value}`;
      let allResults = [];
      let shouldContinuePaging = true;
      let latestResults = null;

      while (shouldContinuePaging) {
        const urlApi = `${urlBase}/${baKey}/${firstId}/${firstId + increment - 1}`;
        let isDataFetched = false;
        latestResults = null;  // Reset for this page

        for (let retries = 0; retries < maxRetries; retries++) {
          try {
            // FIX: Need to pass 0 as the number of keys for evalsha
            const isAllowed = await redis.evalsha(scriptSha, 0);

            if (isAllowed) {
              const results = await executeCurl(urlApi, token);
              
              if (results === false) {
                await new Promise(resolve => setTimeout(resolve, retryInterval * 1000));
                continue;
              }

              if (results.Exception && results.Exception.includes('Token specified is invalid or has expired')) {
                await getTokenBMN(null, null, 'delete');
                const newToken = await getAccessToken(url, data, maxRetries, retryInterval, scriptSha);
                if (newToken === false || newToken === '') {
                  logToConsole("Failed to refresh token from API!", 'error');
                  allOutput += "Failed to refresh token from API!\n";
                  continue;
                }
                token = newToken;
                continue;
              }

              // Store the results for reference outside the retry loop
              latestResults = results;
              
              // No results present at all or explicitly "Tidak Ada Data"
              if (!results.results || results.results === "Tidak Ada Data") {
                logToConsole(`No data for ${value}. Skipping...`, 'warning');
                allOutput += `No data for ${value}. Skipping...\n`;
                isDataFetched = true;  // We've fetched (and found no data)
                shouldContinuePaging = false;  // Stop paging for this asset type
                break;
              }

              // Check for non-array results
              if (!Array.isArray(results.results)) {
                logToConsole(`Unexpected data format for ${value}. Skipping...`, 'warning');
                allOutput += `Unexpected data format for ${value}. Skipping...\n`;
                isDataFetched = true;  // We've fetched (but format wasn't as expected)
                shouldContinuePaging = false;  // Stop paging for this asset type
                break;
              }

              // Empty array - we've reached the end of the data
              if (results.results.length === 0) {
                logToConsole(`No more data for ${value}. Completed paging.`, 'info');
                allOutput += `No more data for ${value}. Completed paging.\n`;
                isDataFetched = true;
                shouldContinuePaging = false;
                break;
              }

              // If we have a lastUpdate date, check if the data is newer
              if (lastUpdate && results.results[0] && results.results[0].TGL_TARIK) {
                const tglTarikNew = results.results[0].TGL_TARIK;

                if (tglTarikNew <= lastUpdate) {
                  logToConsole(`Data ${value} sudah diupdate`, 'info');
                  allOutput += `Data ${value} sudah diupdate\n`;
                  isDataFetched = true;
                  shouldContinuePaging = false;
                  break;
                }
              }

              // If we got here, we have valid data to add
              allResults = allResults.concat(results.results);
              isDataFetched = true;
              
              // Check if we need to continue paging
              if (results.results.length < increment) {
                shouldContinuePaging = false;  // Fewer results than requested means we've reached the end
              }
              
              break;  // Exit retry loop on success
            } else {
              await new Promise(resolve => setTimeout(resolve, retryInterval * 1000));
            }
          } catch (error) {
            logToConsole(`Error processing ${urlApi}: ${error.message}`, 'error');
            allOutput += `Error processing ${urlApi}: ${error.message}\n`;
            await new Promise(resolve => setTimeout(resolve, retryInterval * 1000));
          }
        }

        if (!isDataFetched) {
          logToConsole("Failed to fetch data after maximum retries", 'error');
          allOutput += "Failed to fetch data after maximum retries\n";
          break;  // Break out of the paging loop
        }

        // Only increment the first ID if we're continuing to the next page
        if (shouldContinuePaging) {
          firstId += increment;
        }
      }

      // Process the accumulated results
      if (allResults.length > 0) {
        logToConsole(`Processing ${allResults.length} records for ${value}`, 'info');
        
        try {
          const queryInput = await inputApi(urlBase, allResults, value, tableMapping);
          if (queryInput === false) {
            logToConsole("Failed to Save Data", 'error');
            allOutput += "Failed to Save Data\n";
          } else {
            logToConsole(`Successfully saved ${allResults.length} records for ${value}`, 'info');
            allOutput += `Successfully saved ${allResults.length} records for ${value}\n`;
          }
        } catch (error) {
          logToConsole(`Error saving data for ${value}: ${error.message}`, 'error');
          allOutput += `Error saving data for ${value}: ${error.message}\n`;
        }
        
        // Clear memory
        allResults = [];
      } else {
        // If we didn't get any results but the API responded with success
        if (latestResults !== null && latestResults !== false) {
          logToConsole(`No new data found for ${value}`, 'info');
          allOutput += `No new data found for ${value}\n`;
        }
      }
    }

    // Delete Redis keys
    await deleteRedis();
    
    logToConsole('Update process completed successfully', 'info');
    allOutput += 'Update process completed successfully\n';
    
    return true;
  } catch (error) {
    const errorMessage = `Error in update process: ${error.message}\n${error.stack}`;
    logToConsole(errorMessage, 'error');
    allOutput += errorMessage;
    
    return false;
  }
}

// Initialize and run
async function main() {
  // Process command line arguments
  const args = process.argv.slice(2);
  const command = args[0];
  
  // Check for task-id parameter
  const taskIdArg = args.find(arg => arg.startsWith('--task-id='));
  const taskId = taskIdArg ? taskIdArg.split('=')[1] : null;
  
  if (command === 'update') {
    try {
      // Log that we're starting with taskId if available
      if (taskId) {
        logToConsole(`Starting update with task ID: ${taskId}`, 'info');
      }
      
      // Initialize database
      if (!await initializeDatabase()) {
        process.exit(1);
      }
      
      // Run update process
      await update();
      
      // Exit cleanly
      process.exit(0);
    } catch (error) {
      logToConsole(`Fatal error: ${error.message}`, 'error');
      process.exit(1);
    }
  } else {
    logToConsole('Usage: node asetSimanApi.js update [--task-id=X]', 'info');
    process.exit(1);
  }
}

// Run the application
main();