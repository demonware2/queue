const axios = require('axios');
const crypto = require('crypto');
const https = require('https');
const logger = require('./logger');

class WebhookService {
    constructor(redis) {
        this.redis = redis;
        this.defaultLimit = 120; // 10 requests
        this.defaultWindow = 60; // per 60 seconds

        this.httpsAgent = new https.Agent({
            rejectUnauthorized: false,
            keepAlive: true,
            keepAliveMsecs: 1000,
            maxSockets: 100
        });
    }

    async _checkRateLimit(url, options = {}) {
        const domain = new URL(url).hostname;
        const key = `webhook:limit:${domain}`;
        const limit = options.limit || this.defaultLimit;
        const window = options.window || this.defaultWindow;

        while (true) {
            try {
                const current = await this.redis.get(key);
                const count = current ? parseInt(current) : 0;

                if (count >= limit) {
                    const ttl = await this.redis.ttl(key);
                    const waitTime = (ttl > 0 ? ttl : 5) * 1000;
                    
                    logger.warn(`[WebhookService] Rate limit hit for ${domain} (${count}/${limit}). Waiting ${waitTime/1000}s...`);
                    await new Promise(resolve => setTimeout(resolve, waitTime));
                    continue;
                }

                const newCount = await this.redis.incr(key);
                if (newCount === 1) {
                    await this.redis.expire(key, window);
                }

                if (newCount <= limit) {
                    logger.debug(`[WebhookService] Rate limit passed for ${domain}: ${newCount}/${limit}`);
                    return true;
                }

                await new Promise(resolve => setTimeout(resolve, 1000));
            } catch (error) {
                logger.error(`[WebhookService] Redis error during rate limit check: ${error.message}`);
                return true;
            }
        }
    }

    async send(payload) {
        const { url, secret, event, data, headers: customHeaders } = payload;

        // 1. Check Rate Limit (Blocks until ready)
        await this._checkRateLimit(url, {
            limit: payload.rate_limit || 20,
            window: 60
        });

        // 2. Prepare Payload
        const jsonPayload = JSON.stringify({
            event,
            payload: data,
            timestamp: Math.floor(Date.now() / 1000),
            uuid: crypto.randomUUID()
        });

        // 3. Construct Headers
        const headers = {
            'Content-Type': 'application/json',
            'X-Siroum-Event': event,
            'User-Agent': 'SiROUM-Enterprise-Webhook/2.0 (NodeJS-Worker)',
        };

        // Add custom headers
        if (Array.isArray(customHeaders)) {
            customHeaders.forEach(h => {
                if (h.key && h.value) headers[h.key] = h.value;
            });
        }

        // Add HMAC Signature
        if (secret) {
            const signature = crypto
                .createHmac('sha256', secret)
                .update(jsonPayload)
                .digest('hex');
            headers['X-Siroum-Signature'] = signature;
        }

        const startTime = Date.now();
        try {
            logger.info(`[WebhookService] Sending ${event} to ${url}`);
            
            const response = await axios.post(url, jsonPayload, {
                headers,
                timeout: 15000,
                httpsAgent: this.httpsAgent,
                validateStatus: null,
                maxContentLength: 10 * 1024 * 1024, // Memory protection: 10MB max response
                maxBodyLength: 10 * 1024 * 1024,    // Memory protection: 10MB max request
                maxRedirects: 5
            });

            const duration = (Date.now() - startTime) / 1000;
            logger.info(`[WebhookService] ${event} delivered to ${url} | Status: ${response.status} | Duration: ${duration}s`);
            
            if (response.status >= 400) {
                const responseSnippet = typeof response.data === 'object' ? JSON.stringify(response.data) : String(response.data);
                logger.warn(`[WebhookService] ${url} responded with error: ${responseSnippet.substring(0, 500)}`);
            }

            return {
                status: response.status >= 200 && response.status < 300 ? 'success' : 'failed',
                response_code: response.status,
                response_body: typeof response.data === 'object' ? JSON.stringify(response.data) : response.data,
                duration,
                error_message: response.status >= 400 ? `HTTP ${response.status}` : null
            };
        } catch (error) {
            const duration = (Date.now() - startTime) / 1000;
            logger.error(`[WebhookService] Failed to send ${event} to ${url}: ${error.message}`);
            
            if (error.response) {
                logger.error(`[WebhookService] Response status: ${error.response.status}`);
                logger.error(`[WebhookService] Response data: ${JSON.stringify(error.response.data).substring(0, 500)}`);
            }

            return {
                status: 'failed',
                response_code: error.response ? error.response.status : 0,
                response_body: error.response ? JSON.stringify(error.response.data) : null,
                duration,
                error_message: error.message
            };
        }
    }
}

module.exports = WebhookService;