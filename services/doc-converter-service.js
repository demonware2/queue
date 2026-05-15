require('dotenv').config();
const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const path = require('path');
const https = require('https');
const logger = require('./logger');

const httpsAgent = new https.Agent({
    rejectUnauthorized: false
});

class DocConverterService {
    constructor(redis) {
        this.redis = redis;
        this.engine = process.env.CONVERSION_ENGINE || 'libreoffice';
        this.gotenbergUrl = process.env.GOTENBERG_URL || 'http://localhost:3004/forms/libreoffice/convert';
        this.libreofficeUrl = process.env.DOC_CONVERTER_URL || 'http://localhost:3001/convert';

        this.converterUrl = (this.engine === 'gotenberg') ? this.gotenbergUrl : this.libreofficeUrl;

        this.siroumApiUrl = process.env.SIROUM_API_URL || 'http://localhost/api/v1/file-processing/receive';
        this.siroumStatusUrl = (process.env.SIROUM_API_BASE || 'http://localhost/api/v1/file-processing') + '/update-status';
        this.siroumLogUrl = (process.env.SIROUM_API_BASE || 'http://localhost/api/v1/file-processing') + '/add-log';
        this.siroumDownloadUrl = process.env.SIROUM_DOWNLOAD_URL || 'http://localhost/api/v1/file-processing/get-original/';
        this.apiToken = process.env.INTERNAL_API_TOKEN;
        this.convertTimeout = parseInt(process.env.DOC_CONVERT_TIMEOUT) || 300000; // Default 5 minutes
        this.lockKey = 'lock:doc_convert_global';
    }

    async process(job) {
        const { conversionId, fileHash, originalName, userId } = job.payload;
        let lastError = null;
        const maxInternalAttempts = 3;

        logger.info(`[Job ${job.id}] Starting doc conversion process for ${originalName}`);

        // Acquire Global Lock (Wait until available)
        let hasLock = false;
        logger.debug(`[Job ${job.id}] Attempting to acquire global lock: ${this.lockKey}`);
        while (!hasLock) {
            const result = await this.redis.set(this.lockKey, process.pid, 'NX', 'PX', this.convertTimeout + 10000);
            if (result === 'OK') {
                hasLock = true;
                logger.debug(`[Job ${job.id}] Global lock acquired`);
            } else {
                logger.debug(`[Job ${job.id}] Waiting for global lock...`);
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        }

        try {
            for (let attempt = 1; attempt <= maxInternalAttempts; attempt++) {
                try {
                    logger.info(`[Job ${job.id}] Internal attempt ${attempt} for ${originalName}`);
                    
                    const result = await this.executeConversionStep(job, attempt);

                    return result;
                } catch (error) {
                    lastError = error;
                    
                    let errorMsg = error.message;
                    if (error.response && error.response.data) {
                        const data = error.response.data;
                        if (typeof data === 'string') {
                            errorMsg = data;
                        } else if (typeof data === 'object' && data !== null) {
                            errorMsg = data.error || data.message || data.details || 
                                       (data.messages && data.messages.error) || 
                                       (data.messages && typeof data.messages === 'string' ? data.messages : null) ||
                                       JSON.stringify(data);
                        }
                    }
                    
                    logger.warn(`[Job ${job.id}] Attempt ${attempt} failed: ${errorMsg}`);

                    if (attempt < maxInternalAttempts) {
                        await this.updateStatus(conversionId, 'processing', `Attempt ${attempt} failed, retrying immediately... (${errorMsg})`, attempt);
                        await this.addLog(conversionId, 'retry', `Attempt ${attempt} failed, retrying...`, 'warning');

                        await new Promise(resolve => setTimeout(resolve, 2000));
                    }
                }
            }

            let finalErrorMsg = lastError.message;
            if (lastError.response && lastError.response.data) {
                const data = lastError.response.data;
                if (typeof data === 'string') {
                    finalErrorMsg = data;
                } else if (typeof data === 'object' && data !== null) {
                    finalErrorMsg = data.error || data.message || data.details || 'Unknown API error';
                }
            }
            
            await this.updateStatus(conversionId, 'failed', `Failed after ${maxInternalAttempts} immediate attempts: ${finalErrorMsg}`, maxInternalAttempts);
            await this.addLog(conversionId, 'error', `All ${maxInternalAttempts} attempts failed.`, 'error');
            
            throw lastError;

        } finally {
            await this.redis.del(this.lockKey);
            logger.debug(`[Job ${job.id}] Global lock released`);
        }
    }

    async executeConversionStep(job, attempt) {
        const { conversionId, fileHash, originalName, userId, engine = this.engine } = job.payload;
        let tempInputPath = null;
        let tempOutputPath = null;

        try {
            // 1. Update status to processing
            await this.updateStatus(conversionId, 'processing', null, attempt);
            await this.addLog(conversionId, 'process', `Processing attempt ${attempt} using ${engine} engine`, 'info');

            // 2. Download file from siroum
            tempInputPath = path.join(__dirname, '../temp', `input_${fileHash}_${Date.now()}`);
            if (!fs.existsSync(path.dirname(tempInputPath))) {
                fs.mkdirSync(path.dirname(tempInputPath), { recursive: true });
            }

            const downloadResponse = await axios({
                method: 'get',
                url: `${this.siroumDownloadUrl}?fileHash=${fileHash}`,
                responseType: 'stream',
                headers: { 'X-API-Key': this.apiToken },
                httpsAgent: httpsAgent
            }).catch(err => {
                err.message = `Download from Siroum failed: ${err.message}`;
                throw err;
            });

            const writer = fs.createWriteStream(tempInputPath);
            downloadResponse.data.pipe(writer);

            await new Promise((resolve, reject) => {
                writer.on('finish', resolve);
                writer.on('error', reject);
            });

            // 3. Send to doc-converter (or Gotenberg)
            const url = (engine === 'gotenberg') ? this.gotenbergUrl : this.libreofficeUrl;
            const fieldName = (engine === 'gotenberg') ? 'files' : 'file';

            const form = new FormData();
            form.append(fieldName, fs.createReadStream(tempInputPath), {
                filename: originalName
            });

            const convertResponse = await axios.post(url, form, {
                headers: { ...form.getHeaders() },
                responseType: 'arraybuffer',
                timeout: this.convertTimeout,
                httpsAgent: httpsAgent
            }).catch(err => {
                err.message = `${engine} service failed: ${err.message}`;
                throw err;
            });

            logger.info(`[Job ${job.id}] Conversion received from service (${convertResponse.data.byteLength} bytes)`);

            tempOutputPath = path.join(__dirname, '../temp', `output_${fileHash}_${Date.now()}.pdf`);
            fs.writeFileSync(tempOutputPath, convertResponse.data);

            logger.info(`[Job ${job.id}] Saved temp output to ${tempOutputPath}`);

            // 4. Upload back to siroum
            const uploadForm = new FormData();
            uploadForm.append('originalHash', fileHash);
            uploadForm.append('userId', userId || 'system');
            uploadForm.append('conversionId', conversionId);
            uploadForm.append('file', fs.createReadStream(tempOutputPath), {
                filename: originalName.split('.')[0] + '.pdf',
                contentType: 'application/pdf'
            });

            logger.info(`[Job ${job.id}] Uploading back to Siroum: ${this.siroumApiUrl}`);

            const uploadResponse = await axios.post(this.siroumApiUrl, uploadForm, {
                headers: {
                    ...uploadForm.getHeaders(),
                    'X-API-Key': this.apiToken
                },
                httpsAgent: httpsAgent,
                timeout: 120000 // 2 minutes for upload
            }).catch(err => {
                const errorData = err.response ? JSON.stringify(err.response.data) : 'No response data';
                err.message = `Upload to Siroum failed: ${err.message}. Response: ${errorData}`;
                throw err;
            });

            logger.info(`[Job ${job.id}] Upload successful: ${JSON.stringify(uploadResponse.data)}`);

            return uploadResponse.data;

        } finally {
            if (tempInputPath && fs.existsSync(tempInputPath)) fs.unlinkSync(tempInputPath);
            if (tempOutputPath && fs.existsSync(tempOutputPath)) fs.unlinkSync(tempOutputPath);
        }
    }

    async updateStatus(conversionId, status, errorMessage = null, attempts = null) {
        if (!conversionId) return;
        try {
            const params = new URLSearchParams();
            params.append('conversionId', conversionId);
            params.append('status', status);
            if (errorMessage) params.append('errorMessage', errorMessage);
            if (attempts) params.append('attempts', attempts);

            await axios.post(this.siroumStatusUrl, params, {
                headers: { 'X-API-Key': this.apiToken },
                httpsAgent: httpsAgent,
                timeout: 10000
            });
        } catch (err) {
            logger.error(`Failed to update status for ${conversionId}: ${err.message}`);
        }
    }

    async addLog(conversionId, step, message, status = 'info') {
        if (!conversionId) return;
        try {
            const params = new URLSearchParams();
            params.append('conversionId', conversionId);
            params.append('step', step);
            params.append('message', message);
            params.append('status', status);

            await axios.post(this.siroumLogUrl, params, {
                headers: { 'X-API-Key': this.apiToken },
                httpsAgent: httpsAgent,
                timeout: 10000
            });
        } catch (err) {
            logger.error(`Failed to add log for ${conversionId}: ${err.message}`);
        }
    }
}

module.exports = DocConverterService;