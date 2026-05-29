const axios = require('axios');
const logger = require('./logger');

class TelegramService {
    constructor(redisInstance = null) {
        this.minDelayMs = 1500; // Minimum 1.5 seconds delay
        if (redisInstance) {
            this.redis = redisInstance;
        } else {
            const Redis = require('ioredis');
            const config = require('../config');
            this.redis = new Redis(config.redis);
        }
        this.redisKey = 'telegram:last_sent_time';
        this.lockKey = 'telegram:global_lock';
    }

    validatePayload(token, chatId) {
        if (!token || !chatId) {
            throw new Error('Telegram bot token and chatId are required in payload');
        }

        const tokenRegex = /^[0-9]+:[a-zA-Z0-9_-]+$/;
        if (!tokenRegex.test(token)) {
            throw new Error('Invalid Telegram Bot Token format');
        }

        const chatIdRegex = /^(-?[0-9]+|@[a-zA-Z0-9_]{5,})$/;
        if (!chatIdRegex.test(String(chatId))) {
            throw new Error('Invalid Telegram Chat ID format');
        }
    }

    async acquireLock(maxWaitMs = 60000) {
        const lockId = `${Date.now()}-${Math.random()}`;
        const startTime = Date.now();
        while (Date.now() - startTime < maxWaitMs) {
            const result = await this.redis.set(this.lockKey, lockId, 'PX', 15000, 'NX');
            if (result === 'OK') {
                return lockId;
            }
            await new Promise(resolve => setTimeout(resolve, 50));
        }
        throw new Error('Timeout acquiring Telegram rate limit lock');
    }

    async releaseLock(lockId) {
        try {
            const current = await this.redis.get(this.lockKey);
            if (current === lockId) {
                await this.redis.del(this.lockKey);
            }
        } catch (error) {
            logger.error(`[TelegramService] Error releasing lock: ${error.message}`);
        }
    }

    async sendMessage(payload) {
        let lockId = null;
        try {
            lockId = await this.acquireLock();

            const lastSentTimeStr = await this.redis.get(this.redisKey);
            const lastSentTime = lastSentTimeStr ? parseInt(lastSentTimeStr, 10) : 0;

            const now = Date.now();
            const elapsed = now - lastSentTime;
            if (elapsed < this.minDelayMs) {
                const waitTime = this.minDelayMs - elapsed;
                logger.info(`[TelegramService] Rate limit protection: waiting ${waitTime}ms before sending...`);
                await new Promise(resolve => setTimeout(resolve, waitTime));
            }

            const result = await this._sendMessageInternal(payload);

            await this.redis.set(this.redisKey, Date.now());

            return result;
        } finally {
            if (lockId) {
                await this.releaseLock(lockId);
            }
        }
    }

    async _sendMessageInternal(payload) {
        const token = payload.token;
        const chatId = payload.chatId;
        const topicId = payload.topicId;

        this.validatePayload(token, chatId);

        const url = `https://api.telegram.org/bot${token}/sendMessage`;
        const postData = {
            chat_id: chatId,
            text: payload.text,
            parse_mode: 'Markdown'
        };

        if (topicId !== undefined && topicId !== null && topicId !== '') {
            postData.message_thread_id = parseInt(topicId, 10);
        }

        try {
            logger.info(`[TelegramService] Sending message to chat ${chatId} (topic: ${topicId || 'default'})`);
            const response = await axios.post(url, postData, { timeout: 10000 });
            return response.data;
        } catch (error) {
            if (error.response && error.response.status === 429) {
                const retryAfter = error.response.data?.parameters?.retry_after || 5;
                logger.warn(`[TelegramService] Rate limit (429) encountered. Waiting ${retryAfter}s before retrying...`);
                await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
                return this._sendMessageInternal(payload);
            }

            const errorDescription = error.response?.data?.description || '';
            const isMarkdownError = errorDescription.includes('can\'t find end of') || 
                                    errorDescription.includes('bad request') || 
                                    errorDescription.includes('can\'t parse');
            
            if (isMarkdownError) {
                logger.warn(`[TelegramService] Markdown parsing failed. Falling back to plain text. Error: ${errorDescription}`);
                delete postData.parse_mode;
                postData.text = "[Markdown Fallback]\n" + payload.text;
                try {
                    const retryResponse = await axios.post(url, postData, { timeout: 10000 });
                    return retryResponse.data;
                } catch (retryError) {
                    logger.error(`[TelegramService] Plain text fallback retry failed: ${retryError.message}`);
                    throw retryError;
                }
            }

            logger.error(`[TelegramService] API request failed: ${error.message} (${errorDescription})`);
            throw error;
        }
    }
}

module.exports = TelegramService;