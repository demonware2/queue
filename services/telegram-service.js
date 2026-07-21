const axios = require('axios');
const logger = require('./logger');
const crypto = require('crypto');

class TelegramService {
    constructor(redisInstance = null) {
        this.minGlobalDelayMs = 1000; // Minimum 1 second delay between messages across different chats
        this.minChatDelayMs = 3000;   // Minimum 3 seconds delay between messages to the SAME group chat (max 20 msgs/min)
        if (redisInstance) {
            this.redis = redisInstance;
        } else {
            const Redis = require('ioredis');
            const config = require('../config');
            this.redis = new Redis(config.redis);
        }
    }

    getBotHash(token) {
        return crypto.createHash('md5').update(token || '').digest('hex').substring(0, 12);
    }

    getLockKey(token) {
        return `telegram:lock:${this.getBotHash(token)}`;
    }

    getChatLastSentKey(token, chatId) {
        return `telegram:last_sent:${this.getBotHash(token)}:${chatId}`;
    }

    getGlobalLastSentKey(token) {
        return `telegram:last_sent_global:${this.getBotHash(token)}`;
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

    async acquireLock(token, maxWaitMs = 60000) {
        const lockKey = this.getLockKey(token);
        const lockId = `${Date.now()}-${Math.random()}`;
        const startTime = Date.now();
        while (Date.now() - startTime < maxWaitMs) {
            const result = await this.redis.set(lockKey, lockId, 'PX', 20000, 'NX');
            if (result === 'OK') {
                return lockId;
            }
            await new Promise(resolve => setTimeout(resolve, 50));
        }
        throw new Error('Timeout acquiring Telegram rate limit lock');
    }

    async releaseLock(token, lockId) {
        try {
            const lockKey = this.getLockKey(token);
            const current = await this.redis.get(lockKey);
            if (current === lockId) {
                await this.redis.del(lockKey);
            }
        } catch (error) {
            logger.error(`[TelegramService] Error releasing lock: ${error.message}`);
        }
    }

    async sendMessage(payload) {
        if (payload.action === 'delete') {
            return this._deleteMessageInternal(payload);
        }

        const token = payload.token;
        const chatId = payload.chatId;
        this.validatePayload(token, chatId);

        const isGroupOrChannel = String(chatId).startsWith('-') || String(chatId).startsWith('@');
        const requiredDelay = isGroupOrChannel ? this.minChatDelayMs : this.minGlobalDelayMs;

        const maxRetries = 3;
        let attempt = 0;

        while (attempt < maxRetries) {
            attempt++;
            let lockId = null;
            try {
                lockId = await this.acquireLock(token);

                const chatKey = this.getChatLastSentKey(token, chatId);
                const globalKey = this.getGlobalLastSentKey(token);

                const [lastChatSentStr, lastGlobalSentStr] = await Promise.all([
                    this.redis.get(chatKey),
                    this.redis.get(globalKey)
                ]);

                const lastChatSent = lastChatSentStr ? parseInt(lastChatSentStr, 10) : 0;
                const lastGlobalSent = lastGlobalSentStr ? parseInt(lastGlobalSentStr, 10) : 0;

                const now = Date.now();
                const chatElapsed = now - lastChatSent;
                const globalElapsed = now - lastGlobalSent;

                const chatWait = requiredDelay - chatElapsed;
                const globalWait = this.minGlobalDelayMs - globalElapsed;
                const waitTime = Math.max(0, chatWait, globalWait);

                if (waitTime > 0) {
                    logger.info(`[TelegramService] Rate limit protection (chat: ${chatId}): waiting ${waitTime}ms...`);
                    await new Promise(resolve => setTimeout(resolve, waitTime));
                }

                const result = await this._sendMessageInternal(payload);

                const sentTime = Date.now();
                await Promise.all([
                    this.redis.set(chatKey, sentTime, 'PX', 60000),
                    this.redis.set(globalKey, sentTime, 'PX', 60000)
                ]);

                return result;
            } catch (error) {
                if (error.response && error.response.status === 429) {
                    const retryAfter = error.response.data?.parameters?.retry_after || 5;
                    logger.warn(`[TelegramService] Rate limit (429) encountered on attempt ${attempt}/${maxRetries}. Sleeping ${retryAfter}s...`);
                    if (lockId) {
                        await this.releaseLock(token, lockId);
                        lockId = null;
                    }
                    await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
                    if (attempt < maxRetries) {
                        continue;
                    }
                }
                throw error;
            } finally {
                if (lockId) {
                    await this.releaseLock(token, lockId);
                }
            }
        }
    }

    async _sendMessageInternal(payload) {
        const token = payload.token;
        const chatId = payload.chatId;
        const topicId = payload.topicId;

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

    async _deleteMessageInternal(payload) {
        const token = payload.token;
        const chatId = payload.chatId;
        const messageId = payload.messageId;

        if (!token || !chatId || !messageId) {
            throw new Error('[TelegramService] deleteMessage requires token, chatId, and messageId in payload');
        }

        this.validatePayload(token, chatId);

        const url = `https://api.telegram.org/bot${token}/deleteMessage`;

        try {
            logger.info(`[TelegramService] Deleting message ${messageId} from chat ${chatId}`);
            const response = await axios.post(url, {
                chat_id:    chatId,
                message_id: messageId
            }, { timeout: 10000 });
            return response.data;
        } catch (error) {
            const errorDescription = error.response?.data?.description || '';
            logger.error(`[TelegramService] deleteMessage failed: ${error.message} (${errorDescription})`);
            throw error;
        }
    }
}

module.exports = TelegramService;