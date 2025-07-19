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
    const logFile = path.join(logDir, `ecatepurchasing-${today}.log`);
    
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

async function transform(tahun) {
  const connection = await dbPool.getConnection();
  try {
    logToConsole(`Transforming data for year: ${tahun}`, 'warning');
    
    await connection.beginTransaction();
    
    const [rows] = await connection.query(
      `SELECT * FROM api_ecatepurchasing 
       WHERE tahun = ? 
       ORDER BY created DESC 
       LIMIT 1`,
      [tahun]
    );
    
    if (rows.length === 0) {
      throw new Error("No data found in api_ecatepurchasing");
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
    
    const master = [];
    const accumulatedTotals = {};

    for (const value of decode) {
      master.push([
        value.tahun_anggaran,
        value.kd_klpd,
        value.satker_id,
        value.nama_satker,
        value.alamat_satker,
        value.npwp_satker,
        value.kd_paket,
        value.no_paket,
        value.nama_paket,
        value.kd_rup,
        value.nama_sumber_dana,
        value.kode_anggaran,
        value.kd_komoditas,
        value.kd_produk,
        value.kd_penyedia,
        value.jml_jenis_produk,
        value.kd_penyedia_distributor,
        value.kuantitas,
        value.harga_satuan,
        value.ongkos_kirim,
        value.total_harga,
        value.kd_user_pokja,
        value.no_telp_user_pokja,
        value.email_user_pokja,
        value.kd_user_ppk,
        value.ppk_nip,
        value.jabatan_ppk,
        value.tanggal_buat_paket,
        value.tanggal_edit_paket,
        value.deskripsi,
        value.status_paket,
        value.paket_status_str,
        value.catatan_produk
      ]);
 
      if (accumulatedTotals[value.kd_paket]) {
        accumulatedTotals[value.kd_paket] += parseFloat(value.total_harga);
      } else {
        accumulatedTotals[value.kd_paket] = parseFloat(value.total_harga);
      }
    }
    
    await connection.query('DELETE FROM ecat_paketepurchasing WHERE tahun_anggaran = ?', [tahun]);
    
    if (master.length > 0) {
      await connection.query(
        `INSERT INTO ecat_paketepurchasing (
          tahun_anggaran, kd_klpd, satker_id, nama_satker, alamat_satker, npwp_satker,
          kd_paket, no_paket, nama_paket, kd_rup, nama_sumber_dana, kode_anggaran,
          kd_komoditas, kd_produk, kd_penyedia, jml_jenis_produk, kd_penyedia_distributor,
          kuantitas, harga_satuan, ongkos_kirim, total_harga, kd_user_pokja,
          no_telp_user_pokja, email_user_pokja, kd_user_ppk, ppk_nip, jabatan_ppk,
          tanggal_buat_paket, tanggal_edit_paket, deskripsi, status_paket,
          paket_status_str, catatan_produk
        ) VALUES ?`,
        [master]
      );
    }
    
    const updateQueries = [];
    for (const [kdPaket, total] of Object.entries(accumulatedTotals)) {
      updateQueries.push(`WHEN kd_paket = '${kdPaket}' THEN ${total}`);
    }
    
    if (updateQueries.length > 0) {
      const caseStatement = updateQueries.join(' ');
      const kdPakets = Object.keys(accumulatedTotals).map(kd => `'${kd}'`).join(',');
      
      const updateTotalsSql = `
        UPDATE ecat_paketepurchasing 
        SET total = CASE ${caseStatement} END 
        WHERE kd_paket IN (${kdPakets})
      `;
      
      await connection.query(updateTotalsSql);
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
      `SELECT * FROM api_ecatepurchasing 
       WHERE tahun = ? 
       ORDER BY created DESC 
       LIMIT 1`,
      [tahun]
    );
    
    if (rows.length === 0) {
      throw new Error("No data found in api_ecatepurchasing");
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
    
    const master = [];
    const accumulatedTotals = {};
    
    for (const value of decode) {
      master.push([
        value.tahun_anggaran,
        value.satker_id,
        value.nama_satker,
        value.alamat_satker,
        value.npwp_satker,
        value.kd_paket,
        value.no_paket,
        value.nama_paket,
        value.kd_rup,
        value.nama_sumber_dana,
        value.kode_anggaran,
        value.kd_komoditas,
        value.kd_produk,
        value.jml_jenis_produk,
        value.kd_penyedia_distributor,
        value.kuantitas,
        value.harga_satuan,
        value.ongkos_kirim,
        value.total_harga,
        value.ppk_nip,
        value.tanggal_buat_paket,
        value.tanggal_edit_paket,
        value.deskripsi,
        value.status_paket,
        value.paket_status_str,
        value.catatan_produk,
        value.kd_penyedia
      ]);
      
      if (accumulatedTotals[value.kd_paket]) {
        accumulatedTotals[value.kd_paket] += parseFloat(value.total_harga);
      } else {
        accumulatedTotals[value.kd_paket] = parseFloat(value.total_harga);
      }
    }
    
    await connection.query('DELETE FROM ecat_tm WHERE tahun_anggaran = ?', [tahun]);
    
    if (master.length > 0) {
      await connection.query(
        `INSERT INTO ecat_tm (
          tahun_anggaran, satker_id, nama_satker, alamat_satker, npwp_satker,
          kd_paket, no_paket, nama_paket, kd_rup, nama_sumber_dana, kode_anggaran,
          kd_komoditas, kd_produk, jml_jenis_produk, kd_penyedia_distributor,
          kuantitas, harga_satuan, ongkos_kirim, total_harga, ppk_nip,
          tanggal_buat_paket, tanggal_edit_paket, deskripsi, status_paket,
          paket_status_str, catatan_produk, kd_penyedia
        ) VALUES ?`,
        [master]
      );
    }
    
    const updateQueries = [];
    for (const [kdPaket, total] of Object.entries(accumulatedTotals)) {
      updateQueries.push(`WHEN kd_paket = '${kdPaket}' THEN ${total}`);
    }
    
    if (updateQueries.length > 0) {
      const caseStatement = updateQueries.join(' ');
      const kdPakets = Object.keys(accumulatedTotals).map(kd => `'${kd}'`).join(',');
      
      const updateTotalsSql = `
        UPDATE ecat_tm 
        SET total = CASE ${caseStatement} END 
        WHERE kd_paket IN (${kdPakets})
      `;
      
      await connection.query(updateTotalsSql);
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

async function rupBaru(tahun) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();
    
    const [ecatData] = await connection.query(
      `SELECT kd_rup FROM ecat_tm 
       WHERE tahun_anggaran = ? 
       GROUP BY kd_rup`,
      [tahun]
    );
    
    const [mappingData] = await connection.query(
      `SELECT kd_rup_lama, kd_rup_baru FROM paket_siruphistory_tm 
       WHERE tahun_anggaran = ?`,
      [tahun]
    );

    const rupMapping = {};
    for (const row of mappingData) {
      rupMapping[row.kd_rup_lama] = row.kd_rup_baru;
    }
    
    const [statusData] = await connection.query(
      `SELECT A.koderup, B.statusumumkan 
       FROM paket_sirup_tm A
       INNER JOIN (
         SELECT koderup, statusumumkan 
         FROM paket_sirup_tm 
         WHERE jenispaket = 'PENYEDIA' AND tahunanggaran = ? 
         GROUP BY koderup, statusumumkan
       ) B ON A.koderup = B.koderup
       WHERE A.jenispaket = 'PENYEDIA' AND A.tahunanggaran = ?
       GROUP BY A.koderup, B.statusumumkan`,
      [tahun, tahun]
    );
    
    const statusMapping = {};
    for (const row of statusData) {
      statusMapping[row.koderup] = row.statusumumkan;
    }
    
    const updateBatch = [];
    
    for (const row of ecatData) {
      let initialKdRup = row.kd_rup;
      let currentKdRup = row.kd_rup;
      let newKdRup = null;
      let iteration = 0;
      let isTerumumkanFound = false;
      
      if (statusMapping[currentKdRup] === 'Terumumkan') {
        isTerumumkanFound = true;
      }
      
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
      
      if (newKdRup) {
        updateBatch.push({
          kd_rup: initialKdRup,
          kd_rup_baru: newKdRup
        });
      }
    }
    
    if (updateBatch.length > 0) {
      for (const item of updateBatch) {
        await connection.query(
          'UPDATE ecat_tm SET kd_rup_baru = ? WHERE kd_rup = ?',
          [item.kd_rup_baru, item.kd_rup]
        );
      }
    }
    
    await connection.commit();
    logToConsole(`RUP Baru successful for year ${tahun}!`, 'info');
    return true;
  } catch (error) {
    await connection.rollback();
    logToConsole(`RUP Baru failed: ${error.message}`, 'error');
    return false;
  } finally {
    connection.release();
  }
}

async function metodePurchasing(tahun) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();
    
    logToConsole("Fetching data for metodePurchasing...", 'info');
    
    const [paketSirupRows] = await connection.query(`
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
    
    const paketSirupData = {};
    for (const row of paketSirupRows) {
      paketSirupData[row.koderup] = row;
    }
    logToConsole(`Fetched ${paketSirupRows.length} rows from paket_sirup`, 'warning');
    
    const [satkerRows] = await connection.query(`
      SELECT id_satker_katalog as namasatkerecat, name as namasatker, id_satker_sirup as kodesatker 
      FROM siroum.satker_tm where is_active = 1
    `);

    const satkerData = {};
    for (const row of satkerRows) {
      satkerData[row.namasatkerecat] = row;
    }
    logToConsole(`Fetched ${satkerRows.length} rows from satker`, 'warning');
    
    const [produkRows] = await connection.query(`
      SELECT kd_produk, nama_kategori_terkecil, nama_sub_kategori_1, nama_produk, nama_penyedia
      FROM ecat_produkdetail
      WHERE tahun_paket = ?
    `, [tahun]);
    
    const produkData = {};
    for (const row of produkRows) {
      produkData[row.kd_produk] = row;
    }
    logToConsole(`Fetched ${produkRows.length} rows from ecat_produkdetail`, 'warning');
    
    const [mainRows] = await connection.query(`
      SELECT
        kd_paket, satker_id, total, kd_komoditas, kd_penyedia, nama_paket,
        tahun_anggaran, paket_status_str, status_paket, kd_produk, kode_anggaran,
        tanggal_buat_paket, kd_rup, kd_rup_baru
      FROM ecat_tm
      WHERE paket_status_str IN ('Paket Proses', 'Paket Selesai') AND tahun_anggaran = ?
    `, [tahun]);
    
    logToConsole(`Fetched ${mainRows.length} rows from ecat_tm`, 'warning');
    
    const finalData = [];
    
    for (const row of mainRows) {
      const kdPaket = row.kd_paket;
      const kdRup = (row.kd_rup_baru && row.kd_rup_baru != 0) ? row.kd_rup_baru : row.kd_rup;
      const satker = satkerData[row.satker_id] || {};
      const produk = produkData[row.kd_produk] || {};
      const paketSirup = paketSirupData[kdRup] || {};
      
      const sestama = getSestama(row.satker_id, row.kode_anggaran);
      const idSestama = getIdsestama(row.satker_id, row.kode_anggaran);
      
      finalData.push([
        kdPaket,
        row.satker_id,
        satker.namasatker || '',
        row.total || 0,
        row.kd_komoditas,
        row.kd_penyedia,
        produk.nama_penyedia || '',
        row.nama_paket,
        row.tahun_anggaran,
        row.paket_status_str,
        row.status_paket,
        row.kd_produk,
        produk.nama_kategori_terkecil || '',
        produk.nama_sub_kategori_1 || '',
        row.kode_anggaran || paketSirup.mak || '',
        paketSirup.statuspdn || 'PDN',
        paketSirup.statususahakecil || 'usahaKecil',
        row.tanggal_buat_paket,
        satker.namasatker || '',
        row.kd_rup,
        (satker.kodesatker || '') + '_unit',
        (row.kode_anggaran && row.kode_anggaran.includes('.53')) ? 'Modal' : 'Barang',
        sestama,
        idSestama,
        kdRup,
        '2',
        satker.kodesatker || '',
        produk.nama_produk || ''
      ]);
    }

    await connection.query('DELETE FROM isb_data_kompilasi.isb_data_paket_purchasing WHERE tahun_anggaran = ?', [tahun]);

    if (finalData.length > 0) {
      await connection.query(`
        INSERT INTO isb_data_kompilasi.isb_data_paket_purchasing (
          kd_paket, satker_id, nama_satker, total, kd_komoditas, kd_penyedia, 
          nama_penyedia, nama_paket, tahun_anggaran, paket_status_str, status_paket, 
          kd_produk, nama_kategori_terkecil, nama_sub_kategori_1, kode_anggaran, 
          statuspdn, statususahakecil, tanggal_buat_paket, namasatker, kode_rup_lama, 
          kodesatkerecat, barangmodal, sestama, idsestama, koderuppenc, 
          idmetode, kodesatker, nama_produk
        ) VALUES ?
      `, [finalData]);
    }
    
    const affectedRows = finalData.length;
    logToConsole(`Success in ecatPenyedia! Inserted ${affectedRows} rows.`, 'info');
    
    await connection.commit();
    return true;
  } catch (error) {
    await connection.rollback();
    logToConsole(`Metode Purchasing failed: ${error.message}`, 'error');
    return false;
  } finally {
    connection.release();
  }
}

async function syncSatkers(tahun) {
  const connection = await dbPool.getConnection();
  try {
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
    
    const [existingSatkers] = await connection.query(`
      SELECT kode_satker_katalog
      FROM isb_data_kompilasi.isb_data_ips
      WHERE tahun_anggaran = ?
    `, [tahun]);

    const existingSatkerIds = existingSatkers.map(row => row.kode_satker_katalog);
    const currentSatkerIds = currentSatkers.map(row => row.kode_satker_katalog);
    
    const newSatkers = currentSatkers.filter(satker => 
      !existingSatkerIds.includes(satker.kode_satker_katalog)
    );

    if (newSatkers.length > 0) {
      const newSatkersWithYear = newSatkers.map(satker => {
        return {...satker, tahun_anggaran: tahun};
      });
      
      await connection.query(`
        INSERT INTO isb_data_kompilasi.isb_data_ips 
        (kode_satker_katalog, nama_satker, kode_satker_sirup, wilayah_ukpbj, upt, tahun_anggaran)
        VALUES ?
      `, [newSatkersWithYear.map(row => [
        row.kode_satker_katalog,
        row.nama_satker,
        row.kode_satker_sirup,
        row.wilayah_ukpbj,
        row.upt,
        row.tahun_anggaran
      ])]);
    }
    
    const removedSatkerIds = existingSatkerIds.filter(id => 
      !currentSatkerIds.includes(id)
    );
    
    if (removedSatkerIds.length > 0) {
      await connection.query(`
        DELETE FROM isb_data_kompilasi.isb_data_ips
        WHERE tahun_anggaran = ? AND kode_satker_katalog IN (?)
      `, [tahun, removedSatkerIds]);
    }
    
    return true;
  } catch (error) {
    logToConsole(`syncSatkers failed: ${error.message}`, 'error');
    return false;
  } finally {
    connection.release();
  }
}

async function updatePurchasingData(tahun) {
  const connection = await dbPool.getConnection();
  try {
    const [rows] = await connection.query(`
      SELECT
        satker_id as kode_satker_katalog,
        SUM(COALESCE(total, 0)) as nilai_semua,
        COUNT(satker_id) as paket_semua,
        SUM(CASE WHEN paket_status_str = 'Paket Selesai' THEN COALESCE(total, 0) ELSE 0 END) as nilai_selesai,
        SUM(CASE WHEN paket_status_str = 'Paket Selesai' THEN 1 ELSE 0 END) as paket_selesai
      FROM isb_data_kompilasi.isb_data_paket_purchasing
      WHERE tahun_anggaran = ?
      GROUP BY satker_id
    `, [tahun]);
    
    const updates = [];
    for (const row of rows) {
      updates.push([
        row.nilai_semua || 0,
        row.paket_semua || 0,
        row.nilai_selesai || 0,
        row.paket_selesai || 0,
        tahun,
        row.kode_satker_katalog
      ]);
    }
    
    if (updates.length > 0) {
      for (const data of updates) {
        await connection.query(`
          UPDATE isb_data_kompilasi.isb_data_ips
          SET 
            nilai_purchasing_pemilihan_semua = ?,
            paket_purchasing_pemilihan_semua = ?,
            nilai_purchasing_pemilihan_selesai = ?,
            paket_purchasing_pemilihan_selesai = ?
          WHERE tahun_anggaran = ? AND kode_satker_katalog = ?
        `, data);
      }
    }
    
    return true;
  } catch (error) {
    logToConsole(`updatePurchasingData failed: ${error.message}`, 'error');
    return false;
  } finally {
    connection.release();
  }
}

async function updateDataIPS(tahun) {
  const connection = await dbPool.getConnection();
  try {
    await connection.beginTransaction();
    
    const syncSuccess = await syncSatkers(tahun);
    if (!syncSuccess) {
      throw new Error("Failed to sync satkers");
    }
    
    const updateSuccess = await updatePurchasingData(tahun);
    if (!updateSuccess) {
      throw new Error("Failed to update purchasing data");
    }
    
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

async function update() {
  try {
    logToConsole('Starting EcatPaketEpurchasing update process', 'info');
    
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
            const url = `https://isb.lkpp.go.id/isb-2/api/9130d34f-06df-437b-90a3-9d31639d37f1/json/9460/Ecat-PaketEPurchasing/tipe/4:12/parameter/${tahun}:L15`;
            
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
                'SELECT COUNT(*) as total FROM api_ecatepurchasing'
              );
              const totalRows = countResult[0].total;
              
              if (totalRows >= 14) {
                const [oldestRow] = await connection.query(
                  'SELECT id FROM api_ecatepurchasing ORDER BY created ASC LIMIT 1'
                );
                
                if (oldestRow.length > 0) {
                  await connection.query(
                    'DELETE FROM api_ecatepurchasing WHERE id = ?',
                    [oldestRow[0].id]
                  );
                }
              }
              
              const resultJson = typeof result === 'object' ? JSON.stringify(result) : result;
              await connection.query(
                'INSERT INTO api_ecatepurchasing (request, response, tahun) VALUES (?, ?, ?)',
                [url, resultJson, tahun]
              );
              
              await connection.commit();
              logToConsole(`API data saved successfully for year ${tahun}!`, 'info');
              
              try {
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
                
                const rupBaruResult = await rupBaru(tahun);
                if (rupBaruResult) {
                  logToConsole(`RUP Baru successful for year ${tahun}!`, 'info');
                } else {
                  logToConsole(`RUP Baru failed for year ${tahun}`, 'error');
                }
                
                const metodePurchasingResult = await metodePurchasing(tahun);
                if (metodePurchasingResult) {
                  logToConsole(`Metode Purchasing successful for year ${tahun}!`, 'info');
                } else {
                  logToConsole(`Metode Purchasing failed for year ${tahun}`, 'error');
                }
                
                const updateDataIPSResult = await updateDataIPS(tahun);
                if (updateDataIPSResult) {
                  logToConsole(`Update Data IPS successful for year ${tahun}!`, 'info');
                } else {
                  logToConsole(`Update Data IPS failed for year ${tahun}`, 'error');
                }
                
                const deleteRedisResult = await deleteRedis();
                if (deleteRedisResult) {
                  logToConsole(`Delete Redis successful for year ${tahun}!`, 'info');
                } else {
                  logToConsole(`Delete Redis failed for year ${tahun}`, 'error');
                }
                
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
        logToConsole(`Failed to update year ${tahun} after ${maxRetries} attempts`, 'error');
      }
    }
    
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
    logToConsole('Usage: node ecatPaketEpurchasing.js update [--task-id=X]', 'info');
    process.exit(1);
  }
}

main();