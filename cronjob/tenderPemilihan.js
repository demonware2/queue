// tenderPemilihan.js
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
    host: process.env.DB_ISB_HOST || 'localhost',
    user: process.env.DB_ISB_USER || 'root',
    password: process.env.DB_ISB_PASS || '',
    database: process.env.DB_ISB_NAME || 'isb'
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
    const logFile = path.join(logDir, `tenderpemilihan-${today}.log`);
    
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

// Get ISB years
async function getTahunIsb() {
  try {
    const currentYear = new Date().getFullYear();
    const lastYear = currentYear - 1;
    const nextYear = currentYear + 1;

    const now = new Date();
    const currentDate = `${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
    
    const deadlineDate = '02-05'; // February 5th
    const startNextYearDate = '11-01'; // November 1st

    if (currentDate < deadlineDate) {
      return [lastYear, currentYear];
    } else if (currentDate >= startNextYearDate) {
      return [currentYear, nextYear];
    } else {
      return [currentYear];
    }
  } catch (error) {
    logToConsole(`Error getting ISB years: ${error.message}`, 'error');
    return [new Date().getFullYear()]; // Return current year as fallback
  }
}

// Track the last request time
let lastRequestTime = 0;

// Execute API request
async function executeCurl(url) {
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
        'Content-Type': 'application/json'
      },
      validateStatus: null, // Don't throw on any status code
      maxRedirects: 5,
      timeout: 30000 // 30 seconds timeout
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

// Helper functions for sestama and idsestama
function getSestama(satkerId, kodeAnggaran) {
  if (satkerId !== '18074' && satkerId !== '432731') {
    return 'Tidak';
  }

  let extractedCode;
  const matches = /\.(\d{4})\./.exec(kodeAnggaran);
  if (matches && matches[1]) {
    extractedCode = matches[1];
  } else {
    extractedCode = kodeAnggaran;
  }

  const biroMap = {
    '6384': 'Biro Umum',
    '3158': 'Biro KS dan Humas',
    '3159': 'Biro Rorenkeu',
    '4110': 'Biro Hukor',
    '4112': 'Biro SDM'
  };

  return biroMap[extractedCode] || 'Tidak';
}

function getIdsestama(satkerId, kodeAnggaran) {
  if (satkerId !== '18074' && satkerId !== '432731') {
    return '';
  }

  let extractedCode;
  const matches = /\.(\d{4})\./.exec(kodeAnggaran);
  if (matches && matches[1]) {
    extractedCode = matches[1];
  } else {
    return '';
  }

  const knownCodes = ['6384', '3158', '3159', '4110', '4112'];

  if (knownCodes.includes(extractedCode)) {
    return extractedCode;
  }

  return '';
}

function getMetodePemilihan(mtdPemilihan) {
  if (mtdPemilihan === 'Tender') {
    return '5';
  } else if (mtdPemilihan === 'Seleksi') {
    return '7';
  } else if (mtdPemilihan === 'e-Lelang Cepat') {
    return '4';
  } else if (mtdPemilihan === 'Tender Cepat') {
    return '4';
  } else if (mtdPemilihan === 'Pengadaan Langsung') {
    return '3';
  } else if (mtdPemilihan === 'Penunjukan Langsung') {
    return '6';
  } else if (mtdPemilihan === 'Dikecualikan') {
    return '1';
  } else {
    return '0';
  }
}

// Delete Redis keys
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

// Tender Pengumuman operations
async function updateTenderPengumuman(tahuns, scriptSha, maxRetries = 5, retryInterval = 1) {
  for (const tahun of tahuns) {
    logToConsole(`Starting update Tender Pengumuman for year: ${tahun}`, 'warning');
    
    let success = false;
    
    for (let retries = 0; retries < maxRetries; retries++) {
      try {
        // Check rate limiter
        const isAllowed = await redis.evalsha(scriptSha, 0);
        
        if (isAllowed) {
          const url = `https://isb.lkpp.go.id/isb-2/api/75ffd5d3-417d-4be6-a2b3-085900321bcf/json/9466/SPSE-TenderPengumuman/tipe/4:4/parameter/${tahun}:191`;
          
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
            
            // Check total rows
            const [countResult] = await connection.query(
              'SELECT COUNT(*) as total FROM api_tenderpengumumandetail'
            );
            const totalRows = countResult[0].total;
            
            // Delete oldest row if needed
            if (totalRows >= 14) {
              const [oldestRow] = await connection.query(
                'SELECT id FROM api_tenderpengumumandetail ORDER BY created ASC LIMIT 1'
              );
              
              if (oldestRow.length > 0) {
                await connection.query(
                  'DELETE FROM api_tenderpengumumandetail WHERE id = ?',
                  [oldestRow[0].id]
                );
              }
            }
            
            // Insert new record
            const resultJson = typeof result === 'object' ? JSON.stringify(result) : result;
            await connection.query(
              'INSERT INTO api_tenderpengumumandetail (request, response, tahun) VALUES (?, ?, ?)',
              [url, resultJson, tahun]
            );
            
            await connection.commit();
            logToConsole(`API data saved successfully for year ${tahun}!`, 'info');
            
            // Process the data
            try {
              await transformTenderPengumuman(tahun);
              logToConsole(`Transform successful for year ${tahun}!`, 'info');
              
              await masterTenderPengumuman(tahun);
              logToConsole(`Master successful for year ${tahun}!`, 'info');
              
              await rupBaruTenderPengumuman(tahun);
              logToConsole(`RUP Baru successful for year ${tahun}!`, 'info');
              
              success = true;
              break;
            } catch (processError) {
              logToConsole(`Error in data processing: ${processError.message}`, 'error');
            }
          } catch (dbError) {
            await connection.rollback();
            logToConsole(`Database error: ${dbError.message}`, 'error');
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
      logToConsole(`Failed to update Tender Pengumuman for year ${tahun} after ${maxRetries} attempts`, 'error');
    }
  }
  
  return true;
}

async function transformTenderPengumuman(tahun) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();
    
    // Get the latest data
    const [rows] = await connection.query(
      `SELECT * FROM api_tenderpengumumandetail 
       WHERE tahun = ? 
       ORDER BY created DESC 
       LIMIT 1`,
      [tahun]
    );
    
    if (rows.length === 0) {
      throw new Error("No data found in api_tenderpengumumandetail");
    }
    
    const row = rows[0];
    let decode;
    
    try {
      decode = typeof row.response === 'object' ? row.response : JSON.parse(row.response);
    } catch (error) {
      throw new Error(`Failed to decode JSON: ${error.message}`);
    }
    
    // Prepare data for batch insertion
    const batchData = decode.map(v => {
      return [
        v.tahun_anggaran || null,
        v.kd_klpd || null,
        v.nama_klpd || null,
        v.jenis_klpd || null,
        v.kd_satker_str || null,
        v.nama_satker || null,
        v.kd_tender || null,
        v.kd_pkt_dce || null,
        v.kd_rup || null,
        v.nama_paket || null,
        v.pagu || null,
        v.hps || null,
        v.sumber_dana || null,
        v.jenis_pengadaan || null,
        v.mtd_pemilihan || null,
        v.mtd_evaluasi || null,
        v.mtd_kualifikasi || null,
        v.kontrak_pembayaran || null,
        null, // kontrak_tahun
        null, // jenis_kontrak
        ((v.status_tender === 'Selesai' || v.status_tender === 'Berlangsung') ? 'Aktif' : 'Ditutup'), // nama_status_tender
        v.versi_tender || null,
        v.ket_diulang || null,
        v.ket_ditutup || null,
        v.tgl_buat_paket || null,
        v.tgl_kolektif_kolegial || null,
        v.tgl_pengumuman_tender || null,
        v.url_lpse || null,
        v.kualifikasi_paket || null,
        v.lokasi_pekerjaan || null,
        v.nip_ppk || null,
        v.nip_pokja || null, // nama_ppk (apparently this was swapped in the original code)
        v.nip_pokja || null,
        v.nama_pokja || null,
        v.status_tender || null
      ];
    });
    
    // Delete existing data
    await connection.query('DELETE FROM tenderpengumumandetail WHERE tahun_anggaran = ?', [tahun]);
    
    // Insert new data if any
    if (batchData.length > 0) {
      await connection.query(`
        INSERT INTO tenderpengumumandetail (
          tahun_anggaran, kd_klpd, nama_klpd, jenis_klpd, kd_satker, nama_satker,
          kd_tender, kd_paket, kd_rup_paket, nama_paket, pagu, hps, ang, jenis_pengadaan,
          mtd_pemilihan, mtd_evaluasi, mtd_kualifikasi, kontrak_pembayaran, kontrak_tahun,
          jenis_kontrak, nama_status_tender, versi_tender, ket_diulang, ket_ditutup,
          tgl_buat_paket, tgl_kolektif_kolegial, tgl_pengumuman_tender, url_lpse,
          kualifikasi_paket, lokasi_pekerjaan, nip_ppk, nama_ppk, nip_pokja, nama_pokja, status_tender
        ) VALUES ?
      `, [batchData]);
    }
    
    await connection.commit();
    return true;
  } catch (error) {
    await connection.rollback();
    logToConsole(`Transform Tender Pengumuman failed: ${error.message}`, 'error');
    throw error;
  } finally {
    connection.release();
  }
}

async function masterTenderPengumuman(tahun) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();
    
    // Get the latest data
    const [rows] = await connection.query(
      `SELECT * FROM api_tenderpengumumandetail 
       WHERE tahun = ? 
       ORDER BY created DESC 
       LIMIT 1`,
      [tahun]
    );
    
    if (rows.length === 0) {
      throw new Error("No data found in api_tenderpengumumandetail");
    }
    
    const row = rows[0];
    let decode;
    
    try {
      decode = typeof row.response === 'object' ? row.response : JSON.parse(row.response);
    } catch (error) {
      throw new Error(`Failed to decode JSON: ${error.message}`);
    }
    
    // Prepare data for batch insertion
    const batchData = decode.map(v => {
      return [
        v.tahun_anggaran || null,
        v.kd_satker_str || null,
        v.nama_satker || null,
        v.kd_tender || null,
        v.kd_pkt_dce || null,
        v.kd_rup || null,
        v.nama_paket || null,
        v.pagu || null,
        v.hps || null,
        v.sumber_dana || null,
        v.jenis_pengadaan || null,
        v.mtd_pemilihan || null,
        v.mtd_evaluasi || null,
        v.mtd_kualifikasi || null,
        v.kontrak_pembayaran || null,
        null, // kontrak_tahun
        null, // jenis_kontrak
        ((v.status_tender === 'Selesai' || v.status_tender === 'Berlangsung') ? 'Aktif' : 'Ditutup'), // nama_status_tender
        v.versi_tender || null,
        v.ket_diulang || null,
        v.ket_ditutup || null,
        v.tgl_buat_paket || null,
        v.tgl_kolektif_kolegial || null,
        v.tgl_pengumuman_tender || null,
        v.kualifikasi_paket || null,
        v.lokasi_pekerjaan || null,
        v.nip_ppk || null,
        v.nip_pokja || null, // nama_ppk (apparently this was swapped in the original code)
        v.nip_pokja || null,
        v.nama_pokja || null,
        v.status_tender || null
      ];
    });
    
    // Delete existing data
    await connection.query('DELETE FROM tender_tm WHERE tahun_anggaran = ?', [tahun]);
    
    // Insert new data if any
    if (batchData.length > 0) {
      await connection.query(`
        INSERT INTO tender_tm (
          tahun_anggaran, kd_satker, nama_satker, kd_tender, kd_paket, kd_rup_paket,
          nama_paket, pagu, hps, ang, jenis_pengadaan, mtd_pemilihan,
          mtd_evaluasi, mtd_kualifikasi, kontrak_pembayaran, kontrak_tahun,
          jenis_kontrak, nama_status_tender, versi_tender, ket_diulang, ket_ditutup,
          tgl_buat_paket, tgl_kolektif_kolegial, tgl_pengumuman_tender,
          kualifikasi_paket, lokasi_pekerjaan, nip_ppk, nama_ppk, nip_pokja, nama_pokja, status_tender
        ) VALUES ?
      `, [batchData]);
    }
    
    await connection.commit();
    return true;
  } catch (error) {
    await connection.rollback();
    logToConsole(`Master Tender Pengumuman failed: ${error.message}`, 'error');
    throw error;
  } finally {
    connection.release();
  }
}

async function rupBaruTenderPengumuman(tahun) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();
    
    // Get RUP data from tender_tm
    const [rupData] = await connection.query(
      `SELECT kd_rup_paket FROM tender_tm WHERE tahun_anggaran = ?`,
      [tahun]
    );
    
    // Get mapping data
    const [mappingData] = await connection.query(
      `SELECT kd_rup_lama, kd_rup_baru FROM paket_siruphistory_tm WHERE tahun_anggaran = ?`,
      [tahun]
    );
    
    // Create mapping dictionary
    const rupMapping = {};
    for (const row of mappingData) {
      rupMapping[row.kd_rup_lama] = row.kd_rup_baru;
    }
    
    // Get status mapping data
    const [statusData] = await connection.query(`
      SELECT A.koderup, B.statusumumkan 
      FROM paket_sirup_tm A
      INNER JOIN (
        SELECT koderup, statusumumkan 
        FROM paket_sirup_tm 
        WHERE jenispaket = 'PENYEDIA' AND tahunanggaran = ? 
        GROUP BY koderup, statusumumkan
      ) B ON A.koderup = B.koderup
      WHERE A.jenispaket = 'PENYEDIA' AND A.tahunanggaran = ?
      GROUP BY A.koderup, B.statusumumkan
    `, [tahun, tahun]);
    
    // Create status mapping dictionary
    const statusMapping = {};
    for (const row of statusData) {
      statusMapping[row.koderup] = row.statusumumkan;
    }
    
    // Process RUP mappings
    const updateBatch = [];
    
    for (const rupRow of rupData) {
      let initialKdRup = rupRow.kd_rup_paket;
      let currentKdRup = rupRow.kd_rup_paket;
      let newKdRup = null;
      let iteration = 0;
      let isTerumumkanFound = false;
      
      // Check if initial RUP is already Terumumkan
      if (statusMapping[currentKdRup] === 'Terumumkan') {
        isTerumumkanFound = true;
      }
      
      // Loop through mappings to find Terumumkan status
      while (!isTerumumkanFound && iteration < 15) {
        if (rupMapping[currentKdRup]) {
          const nextRup = rupMapping[currentKdRup];
          newKdRup = nextRup;
          
          if (statusMapping[nextRup] === 'Terumumkan') {
            isTerumumkanFound = true;
            break;
          } else {
            currentKdRup = nextRup;
          }
        } else {
          break;
        }
        
        iteration++;
      }
      
      // Add to update batch if new RUP found
      if (newKdRup) {
        updateBatch.push([newKdRup, initialKdRup]);
      }
    }
    
    // Update records if there are changes
    if (updateBatch.length > 0) {
      for (const [newRup, oldRup] of updateBatch) {
        await connection.query(
          'UPDATE tender_tm SET kd_rup_baru = ? WHERE kd_rup_paket = ?',
          [newRup, oldRup]
        );
      }
    }
    
    await connection.commit();
    return true;
  } catch (error) {
    await connection.rollback();
    logToConsole(`RUP Baru Tender Pengumuman failed: ${error.message}`, 'error');
    throw error;
  } finally {
    connection.release();
  }
}

// Tender Selesai operations
async function updateTenderSelesai(tahuns, scriptSha, maxRetries = 5, retryInterval = 1) {
  for (const tahun of tahuns) {
    logToConsole(`Starting update Tender Selesai for year: ${tahun}`, 'warning');
    
    let success = false;
    
    for (let retries = 0; retries < maxRetries; retries++) {
      try {
        // Check rate limiter
        const isAllowed = await redis.evalsha(scriptSha, 0);
        
        if (isAllowed) {
          const url = `https://isb.lkpp.go.id/isb-2/api/d309db94-ebbb-4068-9e23-d2e08049398d/json/9491/SPSE-TenderSelesai/tipe/4:4/parameter/${tahun}:191`;
          
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
            
            // Check total rows
            const [countResult] = await connection.query(
              'SELECT COUNT(*) as total FROM api_tenderselesaidetail'
            );
            const totalRows = countResult[0].total;
            
            // Delete oldest row if needed
            if (totalRows >= 14) {
              const [oldestRow] = await connection.query(
                'SELECT id FROM api_tenderselesaidetail ORDER BY created ASC LIMIT 1'
              );
              
              if (oldestRow.length > 0) {
                await connection.query(
                  'DELETE FROM api_tenderselesaidetail WHERE id = ?',
                  [oldestRow[0].id]
                );
              }
            }
            
            // Insert new record
            const resultJson = typeof result === 'object' ? JSON.stringify(result) : result;
            await connection.query(
              'INSERT INTO api_tenderselesaidetail (request, response, tahun) VALUES (?, ?, ?)',
              [url, resultJson, tahun]
            );
            
            await connection.commit();
            logToConsole(`API data saved successfully for year ${tahun}!`, 'info');
            
            // Process the data
            try {
              // Also need to get the tender nilai data
              await updateTenderNilai(tahun, scriptSha, maxRetries, retryInterval);
              logToConsole(`Update Tender Nilai successful for year ${tahun}!`, 'info');
              
              await transformTenderSelesai(tahun);
              logToConsole(`Transform successful for year ${tahun}!`, 'info');
              
              await masterTenderSelesai(tahun);
              logToConsole(`Master successful for year ${tahun}!`, 'info');
              
              success = true;
              break;
            } catch (processError) {
              logToConsole(`Error in data processing: ${processError.message}`, 'error');
            }
          } catch (dbError) {
            await connection.rollback();
            logToConsole(`Database error: ${dbError.message}`, 'error');
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
      logToConsole(`Failed to update Tender Selesai for year ${tahun} after ${maxRetries} attempts`, 'error');
    }
  }
  
  return true;
}

async function updateTenderNilai(tahun, scriptSha, maxRetries = 5, retryInterval = 1) {
  let success = false;
  
  for (let retries = 0; retries < maxRetries; retries++) {
    try {
      // Check rate limiter
      const isAllowed = await redis.evalsha(scriptSha, 0);
      
      if (isAllowed) {
        const url = `https://isb.lkpp.go.id/isb-2/api/8c135532-ee90-4706-8628-dfd02a2461d4/json/9490/SPSE-TenderSelesaiNilai/tipe/4:4/parameter/${tahun}:191`;
        
        logToConsole(`Fetching Tender Nilai data from: ${url}`, 'info');
        const result = await executeCurl(url);
        
        if (!result) {
          logToConsole("Failed to fetch Tender Nilai data from API!", 'error');
          await new Promise(resolve => setTimeout(resolve, retryInterval * 1000));
          continue;
        }
        
        const connection = await dbPool.getConnection();
        
        try {
          await connection.beginTransaction();
          
          // Check total rows
          const [countResult] = await connection.query(
            'SELECT COUNT(*) as total FROM api_tenderselesaidetail_nilai'
          );
          const totalRows = countResult[0].total;
          // Delete oldest row if needed
          if (totalRows >= 14) {
            const [oldestRow] = await connection.query(
              'SELECT id FROM api_tenderselesaidetail_nilai ORDER BY created ASC LIMIT 1'
            );
            
            if (oldestRow.length > 0) {
              await connection.query(
                'DELETE FROM api_tenderselesaidetail_nilai WHERE id = ?',
                [oldestRow[0].id]
              );
            }
          }
          
          // Insert new record
          const resultJson = typeof result === 'object' ? JSON.stringify(result) : result;
          await connection.query(
            'INSERT INTO api_tenderselesaidetail_nilai (request, response, tahun) VALUES (?, ?, ?)',
            [url, resultJson, tahun]
          );
          
          await connection.commit();
          logToConsole(`Tender Nilai API data saved successfully for year ${tahun}!`, 'info');
          success = true;
          break;
        } catch (dbError) {
          await connection.rollback();
          logToConsole(`Database error: ${dbError.message}`, 'error');
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
    logToConsole(`Failed to update Tender Nilai for year ${tahun} after ${maxRetries} attempts`, 'error');
    return false;
  }
  
  return true;
}

async function transformTenderSelesai(tahun) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();
    
    // Get the tender selesai data
    const [rows] = await connection.query(
      `SELECT * FROM api_tenderselesaidetail 
       WHERE tahun = ? 
       ORDER BY created DESC 
       LIMIT 1`,
      [tahun]
    );
    
    if (rows.length === 0) {
      throw new Error("No data found in api_tenderselesaidetail");
    }
    
    // Get the tender nilai data
    const [rows2] = await connection.query(
      `SELECT * FROM api_tenderselesaidetail_nilai 
       WHERE tahun = ? 
       ORDER BY created DESC 
       LIMIT 1`,
      [tahun]
    );
    
    if (rows2.length === 0) {
      throw new Error("No data found in api_tenderselesaidetail_nilai");
    }
    
    const row = rows[0];
    const row2 = rows2[0];
    let decode, decode2;
    
    // Parse JSON data
    try {
      decode = typeof row.response === 'object' ? row.response : JSON.parse(row.response);
      decode2 = typeof row2.response === 'object' ? row2.response : JSON.parse(row2.response);
    } catch (error) {
      throw new Error(`Failed to decode JSON: ${error.message}`);
    }
    
    // Prepare data for batch insertion
    const batchData = decode.map(v => {
      return [
        v.tahun_anggaran || null,
        v.kd_klpd || null,
        v.nama_klpd || null,
        v.jenis_klpd || null,
        v.kd_satker_str || null,
        v.nama_satker || null,
        v.kd_lpse || null,
        v.kd_tender || null,
        v.kd_pkt_dce || null,
        v.kd_rup || null,
        v.pagu || null,
        v.hps || null,
        v.tgl_pengumuman_tender || null,
        v.tgl_penetapan_pemenang || null,
        v.mak || null,
        v.nama_paket || null
      ];
    });
    
    // Delete existing data
    await connection.query('DELETE FROM tenderselesaidetail WHERE tahun_anggaran = ?', [tahun]);
    
    // Insert new data if any
    if (batchData.length > 0) {
      await connection.query(`
        INSERT INTO tenderselesaidetail (
          tahun_anggaran, kd_klpd, nama_klpd, jenis_klpd, kd_satker, nama_satker,
          kd_lpse, kd_tender, kd_paket, kd_rup_paket, pagu, hps,
          tgl_pengumuman_tender, tgl_penetapan_pemenang, kode_mak, nama_paket
        ) VALUES ?
      `, [batchData]);
    }
    
    // Prepare update data from nilai
    const updateBatch = decode2.map(v => {
      return [
        v.nilai_penawaran || null,
        v.nilai_terkoreksi || null,
        v.nilai_negosiasi || null,
        v.nilai_kontrak || null,
        v.kd_penyedia || null,
        v.nama_penyedia || null,
        v.npwp_penyedia || null,
        v.nilai_pdn_kontrak || null,
        v.nilai_umk_kontrak || null,
        v.kd_tender || null
      ];
    });
    
    // Update records with nilai data
    if (updateBatch.length > 0) {
      for (const batch of updateBatch) {
        await connection.query(`
          UPDATE tenderselesaidetail SET
            nilai_penawaran = ?,
            nilai_terkoreksi = ?,
            nilai_negosiasi = ?,
            nilai_kontrak = ?,
            kd_penyedia = ?,
            nama_penyedia = ?,
            npwp_penyedia = ?,
            nilai_pdn_kontrak = ?,
            nilai_umk_kontrak = ?
          WHERE kd_tender = ?
        `, batch);
      }
    }
    
    await connection.commit();
    return true;
  } catch (error) {
    await connection.rollback();
    logToConsole(`Transform Tender Selesai failed: ${error.message}`, 'error');
    throw error;
  } finally {
    connection.release();
  }
}

async function masterTenderSelesai(tahun) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();
    
    // Get the tender selesai data
    const [rows] = await connection.query(
      `SELECT * FROM api_tenderselesaidetail 
       WHERE tahun = ? 
       ORDER BY created DESC 
       LIMIT 1`,
      [tahun]
    );
    
    if (rows.length === 0) {
      throw new Error("No data found in api_tenderselesaidetail");
    }
    
    // Get the tender nilai data
    const [rows2] = await connection.query(
      `SELECT * FROM api_tenderselesaidetail_nilai 
       WHERE tahun = ? 
       ORDER BY created DESC 
       LIMIT 1`,
      [tahun]
    );
    
    if (rows2.length === 0) {
      throw new Error("No data found in api_tenderselesaidetail_nilai");
    }
    
    const row = rows[0];
    const row2 = rows2[0];
    let decode, decode2;
    
    // Parse JSON data
    try {
      decode = typeof row.response === 'object' ? row.response : JSON.parse(row.response);
      decode2 = typeof row2.response === 'object' ? row2.response : JSON.parse(row2.response);
    } catch (error) {
      throw new Error(`Failed to decode JSON: ${error.message}`);
    }
    
    // Prepare basic data updates
    const updateBatch1 = decode.map(v => {
      return [
        v.tgl_penetapan_pemenang || null,
        v.mak || null,
        v.kd_tender || null
      ];
    });
    
    // Update basic details
    if (updateBatch1.length > 0) {
      for (const batch of updateBatch1) {
        await connection.query(`
          UPDATE tender_tm SET
            tgl_penetapan_pemenang = ?,
            kode_mak = ?
          WHERE kd_tender = ?
        `, batch);
      }
    }
    
    // Prepare nilai data updates
    const updateBatch2 = decode2.map(v => {
      return [
        v.nilai_penawaran || null,
        v.nilai_terkoreksi || null,
        v.nilai_negosiasi || null,
        v.nilai_kontrak || null,
        v.kd_penyedia || null,
        v.nama_penyedia || null,
        v.npwp_penyedia || null,
        v.nilai_pdn_kontrak || null,
        v.nilai_umk_kontrak || null,
        v.kd_tender || null
      ];
    });
    
    // Update nilai details
    if (updateBatch2.length > 0) {
      for (const batch of updateBatch2) {
        await connection.query(`
          UPDATE tender_tm SET
            nilai_penawaran = ?,
            nilai_terkoreksi = ?,
            nilai_negosiasi = ?,
            nilai_kontrak = ?,
            kd_penyedia = ?,
            nama_penyedia = ?,
            npwp_penyedia = ?,
            nilai_pdn_kontrak = ?,
            nilai_umk_kontrak = ?
          WHERE kd_tender = ?
        `, batch);
      }
    }
    
    await connection.commit();
    return true;
  } catch (error) {
    await connection.rollback();
    logToConsole(`Master Tender Selesai failed: ${error.message}`, 'error');
    throw error;
  } finally {
    connection.release();
  }
}

// Data fetching helpers
async function fetchPaketSirupData(tahun) {
  try {
    const [rows] = await dbPool.query(`
      SELECT 
        A.koderup,
        MAX(A.statuspdn) AS statuspdn,
        MAX(A.statususahakecil) AS statususahakecil,
        MAX(B.mak) AS mak
      FROM paket_sirup_tm A
      LEFT JOIN (
        SELECT koderup, MAX(mak) AS mak
        FROM paket_sirupmak_tm
        WHERE mak IS NOT NULL
        GROUP BY koderup
      ) B ON A.koderup = B.koderup
      WHERE A.statusumumkan IN (
        'Terumumkan', 'Draf', 'Final Draft', 'Draf Lengkap',
        'Terrevisi Satu ke Satu', 'Terrevisi Satu ke Banyak',
        'Inisiasi Revisi Satu ke Satu', 'Terrevisi Batal',
        'Terinisiasi', 'Konsolidasi Satu Satker',
        'Konsolidasi PPK', 'Terdelete'
      )
      AND A.jenispaket = 'PENYEDIA'
      AND A.tahunanggaran = ?
      GROUP BY A.koderup
    `, [tahun]);
    
    logToConsole(`Fetched ${rows.length} rows from paket_sirup`, 'warning');
    
    // Convert to dictionary with koderup as key
    const result = {};
    for (const row of rows) {
      result[row.koderup] = row;
    }
    
    return result;
  } catch (error) {
    logToConsole(`Error fetching paket sirup data: ${error.message}`, 'error');
    return {};
  }
}

async function fetchSatkerDataSirup() {
  try {
    const [rows] = await dbPool.query(`
      SELECT id_satker_katalog as namasatkerecat, name as namasatker, id_satker_sirup as kodesatker 
      FROM siroum.satker_tm 
      WHERE is_active = 1 AND upt != 'lain'
    `);
    
    logToConsole(`Fetched ${rows.length} rows from satker`, 'warning');
    
    // Convert to dictionary with kodesatker as key
    const result = {};
    for (const row of rows) {
      result[row.kodesatker] = row;
    }
    
    return result;
  } catch (error) {
    logToConsole(`Error fetching satker data: ${error.message}`, 'error');
    return {};
  }
}

async function fetchEkontrakDataTender(tahun) {
  try {
    const [rows] = await dbPool.query(`
      SELECT max(kd_tender) as kd_tender, max(no_kontrak) as no_kontrak, max(nilai_kontrak_sppbj) as nilai_kontrak_sppbj 
      FROM ekontrak_tm
      WHERE tahun_anggaran = ? AND jenis_kontrak = 'TENDER'
      GROUP BY kd_tender
    `, [tahun]);
    
    logToConsole(`Fetched ${rows.length} rows from ekontrak_tm`, 'warning');
    
    // Convert to dictionary with kd_tender as key
    const result = {};
    for (const row of rows) {
      result[row.kd_tender] = row;
    }
    
    return result;
  } catch (error) {
    logToConsole(`Error fetching ekontrak data: ${error.message}`, 'error');
    return {};
  }
}

// Metode Tender
async function metodeTender(tahun) {
  const connection = await dbPool.getConnection();
  try {
    logToConsole(`Starting metodeTender for year: ${tahun}`, 'warning');
    
    await connection.beginTransaction();
    
    // Fetch required data
    const paketSirupData = await fetchPaketSirupData(tahun);
    const satkerData = await fetchSatkerDataSirup();
    const ekontrakDataTender = await fetchEkontrakDataTender(tahun);
    
    // Get main data
    const [mainData] = await connection.query(`
      SELECT
        kd_tender, nama_satker, nama_paket, pagu, hps, nilai_penawaran, nilai_kontrak, 
        mtd_pemilihan, kd_satker, tahun_anggaran, jenis_pengadaan, kode_mak, tgl_pengumuman_tender,
        kd_penyedia, nilai_negosiasi, nama_penyedia, kd_rup_paket, kd_rup_baru
      FROM tender_tm
      WHERE nama_status_tender = 'Aktif' AND tahun_anggaran = ?
    `, [tahun]);
    
    logToConsole(`Fetched ${mainData.length} rows from tender_tm`, 'warning');
    
    // Prepare final data
    const finalData = [];
    
    for (const row of mainData) {
      const kdPaket = row.kd_tender;
      const kdRup = (row.kd_rup_baru && row.kd_rup_baru != 0) ? row.kd_rup_baru : row.kd_rup_paket;
      const satker = satkerData[row.kd_satker] || {};
      const ekontrak = ekontrakDataTender[kdPaket] || {};
      const paketSirup = paketSirupData[kdRup] || {};
      const kodeMak = row.kode_mak || paketSirup.mak || '';
      
      finalData.push([
        kdPaket,                                                             // kd_tender
        row.nama_satker,                                                     // nama_satker
        row.nama_paket,                                                      // nama_paket
        row.pagu,                                                            // pagu
        row.hps,                                                             // hps
        row.nilai_penawaran,                                                 // nilai_penawaran
        row.nilai_kontrak || ekontrak.nilai_kontrak_sppbj || null,           // nilai_kontrak
        row.mtd_pemilihan,                                                   // mtd_pemilihan
        row.kd_satker,                                                       // kd_satker
        row.tahun_anggaran,                                                  // tahun_anggaran
        row.jenis_pengadaan,                                                 // jenis_pengadaan
        kodeMak,                                                             // kode_mak
        paketSirup.statuspdn || 'PDN',                                       // statuspdn
        paketSirup.statususahakecil || 'usahaKecil',                         // statususahakecil
        ekontrak.no_kontrak || null,                                         // no_kontrak
        ekontrak.nilai_kontrak_sppbj || null,                                // nilai_kontrak_sppbj
        row.tgl_pengumuman_tender,                                           // tgl_pengumuman_tender
        row.kd_penyedia,                                                     // kd_penyedia
        row.nilai_negosiasi == 0 ? row.nilai_penawaran : row.nilai_negosiasi, // nilai_hasil_pemilihan
        row.nama_penyedia,                                                   // nama_penyedia
        (satker.kodesatker || '') + '_unit',                                 // kodesatker
        (kodeMak.includes('.53')) ? 'Modal' : 'Barang',                      // barangmodal
        getSestama(row.kd_satker, kodeMak),                                  // sestama
        getIdsestama(row.kd_satker, kodeMak),                                // idsestama
        kdRup,                                                               // koderuppenc
        getMetodePemilihan(row.mtd_pemilihan)                                // metodeid
      ]);
    }
    
    logToConsole(`Final data count: ${finalData.length}`, 'warning');
    
    // Delete existing data
    await connection.query('DELETE FROM isb_data_kompilasi.isb_data_paket_tender WHERE tahun_anggaran = ?', [tahun]);
    
    // Insert new data if any
    if (finalData.length > 0) {
      await connection.query(`
        INSERT INTO isb_data_kompilasi.isb_data_paket_tender (
          kd_tender, nama_satker, nama_paket, pagu, hps, nilai_penawaran, nilai_kontrak, 
          mtd_pemilihan, kd_satker, tahun_anggaran, jenis_pengadaan, kode_mak, 
          statuspdn, statususahakecil, no_kontrak, nilai_kontrak_sppbj, tgl_pengumuman_tender,
          kd_penyedia, nilai_hasil_pemilihan, nama_penyedia, kodesatker, barangmodal,
          sestama, idsestama, koderuppenc, metodeid
        ) VALUES ?
      `, [finalData]);
      
      const affectedRows = finalData.length;
      logToConsole(`Success in metodeTender! Inserted ${affectedRows} rows.`, 'info');
    } else {
      logToConsole(`No data to insert in metodeTender.`, 'warning');
    }
    
    await connection.commit();
    return true;
  } catch (error) {
    await connection.rollback();
    logToConsole(`Metode Tender failed: ${error.message}`, 'error');
    return false;
  } finally {
    connection.release();
  }
}

// Data IPS operations
async function updateDataIPS(tahun) {
  const connection = await dbPool.getConnection();
  try {
    logToConsole(`Starting updateDataIPS for year: ${tahun}`, 'warning');
    
    await connection.beginTransaction();
    
    // Sync satkers
    await syncSatkers(tahun);
    
    // Update tender data
    await updateTenderData(tahun);
    
    // Update kontrak data
    await updateKontrakTender(tahun);
    
    await connection.commit();
    logToConsole(`UpdateDataIPS successful for year ${tahun}!`, 'info');
    return true;
  } catch (error) {
    await connection.rollback();
    logToConsole(`UpdateDataIPS failed: ${error.message}`, 'error');
    return false;
  } finally {
    connection.release();
  }
}

async function syncSatkers(tahun) {
  const connection = await dbPool.getConnection();
  try {
    // Get current satkers
    const [currentSatkers] = await connection.query(`
      SELECT 
        id_satker_katalog as kode_satker_katalog, 
        name as nama_satker, 
        id_satker_sirup as kode_satker_sirup, 
        wilayah_ukpbj, 
        upt
      FROM siroum.satker_tm
      WHERE is_active = 1 AND anggaran != 'lain'
    `);
    
    // Get existing satkers in IPS table
    const [existingSatkers] = await connection.query(`
      SELECT kode_satker_katalog
      FROM isb_data_kompilasi.isb_data_ips
      WHERE tahun_anggaran = ?
    `, [tahun]);
    
    // Find new satkers to add
    const existingSatkerIds = existingSatkers.map(row => row.kode_satker_katalog);
    const currentSatkerIds = currentSatkers.map(row => row.kode_satker_katalog);
    
    const newSatkers = currentSatkers.filter(satker => 
      !existingSatkerIds.includes(satker.kode_satker_katalog)
    );
    
    // Insert new satkers
    if (newSatkers.length > 0) {
      // Add tahun_anggaran to each new satker
      const batchData = newSatkers.map(satker => {
        return [
          satker.kode_satker_katalog,
          satker.nama_satker,
          satker.kode_satker_sirup,
          satker.wilayah_ukpbj,
          satker.upt,
          tahun
        ];
      });
      
      await connection.query(`
        INSERT INTO isb_data_kompilasi.isb_data_ips 
        (kode_satker_katalog, nama_satker, kode_satker_sirup, wilayah_ukpbj, upt, tahun_anggaran)
        VALUES ?
      `, [batchData]);
    }
    
    // Find removed satkers
    const removedSatkerIds = existingSatkerIds.filter(id => 
      !currentSatkerIds.includes(id)
    );
    
    // Delete removed satkers
    if (removedSatkerIds.length > 0) {
      for (const id of removedSatkerIds) {
        await connection.query(`
          DELETE FROM isb_data_kompilasi.isb_data_ips
          WHERE tahun_anggaran = ? AND kode_satker_katalog = ?
        `, [tahun, id]);
      }
    }
    
    return true;
  } catch (error) {
    logToConsole(`syncSatkers failed: ${error.message}`, 'error');
    throw error;
  } finally {
    connection.release();
  }
}

async function updateTenderData(tahun) {
  const connection = await dbPool.getConnection();
  try {
    const [dataTender] = await connection.query(`
      SELECT
        kd_satker as kode_satker_sirup,
        SUM(COALESCE(pagu, 0)) as nilai_etender,
        COUNT(kd_satker) as paket_etender
      FROM isb_data_kompilasi.isb_data_paket_tender
      WHERE tahun_anggaran = ?
      GROUP BY kd_satker
    `, [tahun]);
    
    if (dataTender.length > 0) {
      for (const row of dataTender) {
        await connection.query(`
          UPDATE isb_data_kompilasi.isb_data_ips SET
            nilai_etender_pemilihan = ?,
            paket_etender_pemilihan = ?
          WHERE tahun_anggaran = ? AND kode_satker_sirup = ?
        `, [
          row.nilai_etender,
          row.paket_etender,
          tahun,
          row.kode_satker_sirup
        ]);
      }
    }
    
    return true;
  } catch (error) {
    logToConsole(`updateTenderData failed: ${error.message}`, 'error');
    throw error;
  } finally {
    connection.release();
  }
}

async function updateKontrakTender(tahun) {
  const connection = await dbPool.getConnection();
  try {
    const [dataKontrak] = await connection.query(`
      SELECT
        kd_satker as kode_satker_sirup,
        SUM(CASE WHEN no_kontrak IS NOT NULL AND no_kontrak != '' THEN 1 ELSE 0 END) as kontrak_etender
      FROM isb_data_kompilasi.isb_data_paket_tender
      WHERE tahun_anggaran = ?
      GROUP BY kd_satker
    `, [tahun]);
    
    if (dataKontrak.length > 0) {
      for (const row of dataKontrak) {
        await connection.query(`
          UPDATE isb_data_kompilasi.isb_data_ips SET
            ekontrak_tender = ?
          WHERE tahun_anggaran = ? AND kode_satker_sirup = ?
        `, [
          row.kontrak_etender,
          tahun,
          row.kode_satker_sirup
        ]);
      }
    }
    
    return true;
  } catch (error) {
    logToConsole(`updateKontrakTender failed: ${error.message}`, 'error');
    throw error;
  } finally {
    connection.release();
  }
}

// Main update function
async function runScheduler() {
  try {
    logToConsole('Starting Tender Pemilihan scheduler', 'info');
    
    // Initialize Redis if not already done
    if (!await initializeRedis()) {
      const message = "Redis server is unavailable!";
      logToConsole(message, 'error');
      return false;
    }

    // Get the years to process
    const tahuns = await getTahunIsb();
    
    // Load Lua script
    const luaScriptPath = path.resolve(process.cwd(), 'limiter_api_ISB.lua');
    const luaScript = await readFile(luaScriptPath, 'utf8');
    const scriptSha = await redis.script('load', luaScript);
    
    // Configuration
    const maxRetries = 5;
    const retryInterval = 1;
    
    // Update Tender Pengumuman
    const tenderPengumuman = await updateTenderPengumuman(tahuns, scriptSha, maxRetries, retryInterval);
    if (tenderPengumuman) {
      logToConsole("Success in update Tender Pengumuman!", 'info');
    }
    
    // Update Tender Selesai
    const tenderSelesai = await updateTenderSelesai(tahuns, scriptSha, maxRetries, retryInterval);
    if (tenderSelesai) {
      logToConsole("Success in update Tender Selesai!", 'info');
    }
    
    // Process metode tender and update data IPS for each year
    for (const tahun of tahuns) {
      await metodeTender(tahun);
      await updateDataIPS(tahun);
    }
    
    // Delete Redis keys
    await deleteRedis();
    
    logToConsole('All done!', 'info');
    return true;
  } catch (error) {
    logToConsole(`Error in scheduler: ${error.message}`, 'error');
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
  
  if (command === 'run') {
    try {
      // Log that we're starting with taskId if available
      if (taskId) {
        logToConsole(`Starting Tender Pemilihan with task ID: ${taskId}`, 'info');
      }
      
      // Initialize database
      if (!await initializeDatabase()) {
        process.exit(1);
      }
      
      // Run scheduler
      await runScheduler();
      
      // Exit cleanly
      process.exit(0);
    } catch (error) {
      logToConsole(`Fatal error: ${error.message}`, 'error');
      process.exit(1);
    }
  } else {
    logToConsole('Usage: node tenderPemilihan.js run [--task-id=X]', 'info');
    process.exit(1);
  }
}

// Run the application
main();