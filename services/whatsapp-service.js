const axios = require('axios');
const dotenv = require('dotenv');
const Redis = require('ioredis');
const config = require('../config');

dotenv.config();

class WhatsAppService {
    constructor(redisInstance = null) {
        this.defaultBaseUrl = process.env.WHATSAPP_API_URL || 'http://localhost:7827';

        this.defaultDelayMs = parseInt(process.env.WHATSAPP_DELAY_MS || '25000');
        this.minDelayMs = parseInt(process.env.WHATSAPP_MIN_DELAY_MS || '20000');
        this.maxDelayMs = parseInt(process.env.WHATSAPP_MAX_DELAY_MS || '30000');

        this.initialized = new Map();

        this.limitCount = config.whatsapp?.limitCount || 80;
        this.limitWindowSeconds = (config.whatsapp?.limitWindowHours || 2) * 3600;
        this.redisKey = 'whatsapp:limit:count';

        if (redisInstance) {
            this.redis = redisInstance;
        } else {
            this.redis = new Redis(config.redis);
        }

        this.REDIS_LOCK_KEY = 'whatsapp:global_lock';
        this.REDIS_QUEUE_KEY = 'whatsapp:processing_queue';

        this.wablasToken = process.env.WABLAS_TOKEN || '';
        this.wablasSecret = process.env.WABLAS_SECRET || '';
        this.wablasBaseUrl = process.env.WABLAS_BASE_URL || 'https://bdg.wablas.com/api';
    }

    async _checkRateLimit() {
        while (true) {
            const count = await this.redis.get(this.redisKey);
            const currentCount = count ? parseInt(count) : 0;

            if (currentCount >= this.limitCount) {
                const ttl = await this.redis.ttl(this.redisKey);

                if (ttl === -2) {
                    continue;
                }

                if (ttl === -1) {
                    await this.redis.expire(this.redisKey, this.limitWindowSeconds);
                    console.log(`[WhatsAppService] Limit active but no expiry found. Resetting to ${this.limitWindowSeconds}s.`);
                    await new Promise(resolve => setTimeout(resolve, 5000));
                    continue;
                }

                const waitTimeMs = (ttl * 1000) + 1000;
                const finalWaitMs = Math.min(waitTimeMs, 300000);

                console.log(`[WhatsAppService] Rate limit reached (${currentCount}/${this.limitCount}). Waiting ${Math.round(finalWaitMs / 1000)}s for reset...`);
                await new Promise(resolve => setTimeout(resolve, finalWaitMs));
                continue;
            }

            const newCount = await this.redis.incr(this.redisKey);

            if (newCount === 1) {
                await this.redis.expire(this.redisKey, this.limitWindowSeconds);
            } else {
                const ttl = await this.redis.ttl(this.redisKey);
                if (ttl === -1) {
                    await this.redis.expire(this.redisKey, this.limitWindowSeconds);
                }
            }

            if (newCount <= this.limitCount) {
                console.log(`[WhatsAppService] Rate limit check passed: ${newCount}/${this.limitCount}`);
                return true;
            }

            console.log(`[WhatsAppService] Limit exceeded after increment: ${newCount}/${this.limitCount}. Retrying wait...`);
        }
    }

    async init(baseUrl = null) {
        const targetUrl = baseUrl || this.defaultBaseUrl;

        if (this.initialized.get(targetUrl)) {
            return true;
        }

        try {
            const response = await axios.get(`${targetUrl}/status`, {
                timeout: 10000,
                validateStatus: null
            });

            if (response.status !== 200) {
                console.error(`[${targetUrl}] WhatsApp API returned status: ${response.status}`);
                return false;
            }

            if (response.data && response.data.ready) {
                console.log(`[${targetUrl}] WhatsApp service initialized successfully`);
                this.initialized.set(targetUrl, true);
                return true;
            } else {
                console.error(`[${targetUrl}] WhatsApp API is not ready:`, response.data);
                return false;
            }
        } catch (error) {
            console.error(`[${targetUrl}] Failed to initialize WhatsApp service:`, error.message);
            return false;
        }
    }

    async acquireGlobalLock(maxWaitMs = 300000) {
        const lockId = `${Date.now()}-${Math.random()}`;
        const startTime = Date.now();

        console.log(`[WhatsApp] Attempting to acquire global lock (lockId: ${lockId})...`);

        while (Date.now() - startTime < maxWaitMs) {
            const result = await this.redis.set(
                this.REDIS_LOCK_KEY,
                lockId,
                'PX', 600000,
                'NX'
            );

            if (result === 'OK') {
                console.log(`[WhatsApp] ✅ Global lock acquired (lockId: ${lockId})`);
                return lockId;
            }

            await new Promise(resolve => setTimeout(resolve, 100));
        }

        throw new Error(`Failed to acquire WhatsApp global lock after ${maxWaitMs}ms`);
    }

    async releaseGlobalLock(lockId) {
        try {
            const currentLockId = await this.redis.get(this.REDIS_LOCK_KEY);
            if (currentLockId === lockId) {
                await this.redis.del(this.REDIS_LOCK_KEY);
                console.log(`[WhatsApp] ✅ Global lock released (lockId: ${lockId})`);
            } else {
                console.warn(`[WhatsApp] ⚠️ Lock already released or owned by another process`);
            }
        } catch (error) {
            console.error(`[WhatsApp] Error releasing lock:`, error.message);
        }
    }

    calculateDelay(customDelay = null) {
        if (customDelay) {
            return parseInt(customDelay);
        }

        const min = this.minDelayMs;
        const max = this.maxDelayMs;
        return Math.floor(Math.random() * (max - min + 1)) + min;
    }

    async sendMessage(payload) {
        let lockId = null;

        try {
            lockId = await this.acquireGlobalLock();
            const result = await this._sendMessageInternal(payload);
            return result;
        } finally {
            if (lockId) {
                await this.releaseGlobalLock(lockId);
            }
        }
    }

    async _sendMessageInternal(payload) {
        console.log("Entering _sendMessageInternal");
        const baseUrl = payload.baseUrl || this.defaultBaseUrl;

        try {
            await this._checkRateLimit();

            await this.waitUntilReady(baseUrl, 45000, 1500);

            if (!payload.number || !payload.message) {
                throw new Error('Number and message are required for WhatsApp message');
            }

            console.log(`[${baseUrl}] Sending message to ${payload.number}...`);
            let response = await axios.post(`${baseUrl}/send-message`, {
                number: payload.number,
                message: payload.message
            }, {
                timeout: 30000
            });

            const sentTime = new Date().toLocaleTimeString();
            console.log(`[${baseUrl}] ✅ Message sent to ${payload.number} at ${sentTime}`);

            const delayMs = this.calculateDelay(payload.delay || payload.delayMs);
            console.log(`[${baseUrl}] Waiting ${delayMs}ms before next message...`);
            await new Promise(resolve => setTimeout(resolve, delayMs));

            if (response.data && response.data.success) {
                return {
                    success: true,
                    messageId: Date.now().toString(),
                    response: response.data,
                    baseUrl: baseUrl,
                    sentAt: sentTime,
                    delayUsed: delayMs,
                    method: 'primary'
                };
            } else {
                throw new Error(response.data?.error || 'Failed to send WhatsApp message');
            }
        } catch (error) {
            const status = error.response?.status;
            if (status === 503) {
                console.warn(`[${baseUrl}] Not ready (503). Waiting and retrying send to ${payload.number}...`);
                await this.waitUntilReady(baseUrl, 30000, 1500);
                try {
                    const retryResp = await axios.post(`${baseUrl}/send-message`, {
                        number: payload.number,
                        message: payload.message
                    }, { timeout: 30000 });
                    if (retryResp.data?.success) {
                        const sentTime = new Date().toLocaleTimeString();
                        console.log(`[${baseUrl}] ✅ Message sent on retry to ${payload.number} at ${sentTime}`);
                        return {
                            success: true,
                            messageId: Date.now().toString(),
                            response: retryResp.data,
                            baseUrl,
                            sentAt: sentTime,
                            delayUsed: delayMs,
                            method: 'primary-retry'
                        };
                    }
                } catch (e2) {
                    console.warn(`[${baseUrl}] Retry failed for ${payload.number}: ${e2.message}`);
                }
            }

            console.error(`[${baseUrl}] WhatsApp message to ${payload.number} failed:`, error.message);
            console.log(`[BACKUP] Trying Wablas for ${payload.number}`);
            return await this._sendViaWablas(payload.number, payload.message, false);
        }
    }

    async sendGroupMessage(payload) {
        let lockId = null;

        try {
            lockId = await this.acquireGlobalLock();
            const result = await this._sendGroupMessageInternal(payload);
            return result;
        } finally {
            if (lockId) {
                await this.releaseGlobalLock(lockId);
            }
        }
    }

    async _sendGroupMessageInternal(payload) {
        const baseUrl = payload.baseUrl || this.defaultBaseUrl;

        try {
            await this._checkRateLimit();

            await this.waitUntilReady(baseUrl, 45000, 1500);

            if (!payload.groupId || !payload.message) {
                throw new Error('Group ID and message are required for WhatsApp group message');
            }

            console.log(`[${baseUrl}] Sending group message to ${payload.groupId}...`);
            let response = await axios.post(`${baseUrl}/send-group-message`, {
                groupId: payload.groupId,
                message: payload.message
            }, {
                timeout: 30000
            });

            const sentTime = new Date().toLocaleTimeString();
            console.log(`[${baseUrl}] ✅ Group message sent to ${payload.groupId} at ${sentTime}`);

            const delayMs = this.calculateDelay(payload.delay || payload.delayMs);
            console.log(`[${baseUrl}] Waiting ${delayMs}ms before next message...`);
            await new Promise(resolve => setTimeout(resolve, delayMs));

            if (response.data && response.data.success) {
                return {
                    success: true,
                    messageId: Date.now().toString(),
                    response: response.data,
                    baseUrl: baseUrl,
                    sentAt: sentTime,
                    delayUsed: delayMs,
                    method: 'primary'
                };
            } else {
                throw new Error(response.data?.error || 'Failed to send WhatsApp group message');
            }
        } catch (error) {
            const status = error.response?.status;
            if (status === 503) {
                console.warn(`[${baseUrl}] Not ready (503). Waiting and retrying group send to ${payload.groupId}...`);
                await this.waitUntilReady(baseUrl, 30000, 1500);
                try {
                    const retryResp = await axios.post(`${baseUrl}/send-group-message`, {
                        groupId: payload.groupId,
                        message: payload.message
                    }, { timeout: 30000 });
                    if (retryResp.data?.success) {
                        const sentTime = new Date().toLocaleTimeString();
                        console.log(`[${baseUrl}] ✅ Group message sent on retry to ${payload.groupId} at ${sentTime}`);
                        return {
                            success: true,
                            messageId: Date.now().toString(),
                            response: retryResp.data,
                            baseUrl,
                            sentAt: sentTime,
                            delayUsed: delayMs,
                            method: 'primary-retry'
                        };
                    }
                } catch (e2) {
                    console.warn(`[${baseUrl}] Retry failed for group ${payload.groupId}: ${e2.message}`);
                }
            }

            console.error(`[${baseUrl}] WhatsApp group message to ${payload.groupId} failed:`, error.message);
            console.log(`[BACKUP] Trying Wablas for group ${payload.groupId}`);
            return await this._sendViaWablas(payload.groupId, payload.message, true);
        }
    }

    async waitUntilReady(baseUrl, timeoutMs = 30000, intervalMs = 1000) {
        const start = Date.now();
        while (Date.now() - start < timeoutMs) {
            try {
                console.log(`[${baseUrl}] Checking status...`);
                const response = await axios.get(`${baseUrl}/status`, { timeout: 5000, validateStatus: null });
                if (response.status === 200 && response.data?.ready) {
                    if (!this.initialized.get(baseUrl)) this.initialized.set(baseUrl, true);
                    console.log(`[${baseUrl}] Status is ready.`);
                    return true;
                }
                console.log(`[${baseUrl}] Status not ready, status: ${response.status}`);
            } catch (e) {
                console.error(`[${baseUrl}] Error in waitUntilReady: ${e.message}`);
            }
            await new Promise(r => setTimeout(r, intervalMs));
        }

        const resp = await axios.get(`${baseUrl}/status`, { timeout: 5000, validateStatus: null });
        if (resp.status === 200 && resp.data?.ready) {
            if (!this.initialized.get(baseUrl)) this.initialized.set(baseUrl, true);
            return true;
        }
        throw new Error(`WhatsApp service not ready at ${baseUrl} after ${timeoutMs}ms`);
    }

    async _sendViaWablas(target, message, isGroup = false) {
        const token = process.env.WABLAS_TOKEN || this.wablasToken;
        const secret = process.env.WABLAS_SECRET || this.wablasSecret;
        const baseUrl = process.env.WABLAS_BASE_URL || this.wablasBaseUrl || 'https://bdg.wablas.com/api';

        if (!token || !secret) {
            throw new Error('Wablas credentials not configured');
        }

        try {
            let response;
            const sentTime = new Date().toLocaleTimeString();

            const cleanBaseUrl = baseUrl.replace(/\/v2\/?$/, '').replace(/\/$/, '');
            const v2Url = `${cleanBaseUrl}/v2/send-message`;
            const authHeader = secret ? `${token}.${secret}` : token;

            const payload = {
                data: [
                    {
                        phone: target,
                        message: message,
                        isGroup: isGroup ? 'true' : 'false'
                    }
                ]
            };

            response = await axios.post(v2Url, payload, {
                timeout: 30000,
                headers: {
                    'Authorization': authHeader,
                    'Content-Type': 'application/json'
                }
            });

            if (response.status === 200) {
                console.log(`[WABLAS] ✅ ${isGroup ? 'Group message' : 'Message'} sent to ${target} at ${sentTime}`);
                return {
                    success: true,
                    messageId: Date.now().toString(),
                    response: response.data,
                    baseUrl: 'wablas-backup',
                    sentAt: sentTime,
                    method: 'backup',
                    service: 'wablas'
                };
            } else {
                throw new Error(`Wablas returned status: ${response.status}`);
            }
        } catch (error) {
            console.error(`[WABLAS] Failed to send ${isGroup ? 'group message' : 'message'} to ${target}:`, error.message);
            throw new Error(`Both primary and backup services failed: ${error.message}`);
        }
    }

    async checkServiceHealth(baseUrl = null) {
        const targetUrl = baseUrl || this.defaultBaseUrl;
        try {
            const response = await axios.get(`${targetUrl}/ping`, {
                timeout: 5000
            });

            return response.data && response.data.whatsapp_ready;
        } catch (error) {
            console.error(`[${targetUrl}] WhatsApp health check failed:`, error.message);
            return false;
        }
    }

    async getServiceStatus() {
        const primaryHealth = await this.checkServiceHealth();
        const wablasConfigured = !!(this.wablasToken && this.wablasSecret);

        return {
            primary: {
                healthy: primaryHealth,
                url: this.defaultBaseUrl
            },
            backup: {
                configured: wablasConfigured,
                service: 'wablas'
            }
        };
    }

    async resetGlobalLock() {
        try {
            await this.redis.del(this.REDIS_LOCK_KEY);
            console.log('[WhatsApp] Global lock forcefully reset');
            return true;
        } catch (error) {
            console.error('[WhatsApp] Error resetting global lock:', error.message);
            return false;
        }
    }

    async getQueueStatus() {
        try {
            const lockId = await this.redis.get(this.REDIS_LOCK_KEY);
            const ttl = await this.redis.pttl(this.REDIS_LOCK_KEY);

            return {
                isLocked: !!lockId,
                lockId: lockId,
                lockExpiresInMs: ttl > 0 ? ttl : null,
                redisConnected: this.redis.status === 'ready'
            };
        } catch (error) {
            console.error('[WhatsApp] Error getting queue status:', error.message);
            return {
                isLocked: false,
                lockId: null,
                lockExpiresInMs: null,
                redisConnected: false,
                error: error.message
            };
        }
    }
}

module.exports = WhatsAppService;