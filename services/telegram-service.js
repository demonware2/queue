const axios = require('axios');
const logger = require('./logger');

class TelegramService {
    constructor() {
        this.lastSentTime = 0;
        this.minDelayMs = 1500;
    }

    validatePayload(token, chatId) {
        if (!token || !chatId) {
            throw new Error('Telegram bot token and chatId are required in the payload');
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

    async sendMessage(payload) {
        const { token, chatId, text, topicId } = payload;

        this.validatePayload(token, chatId);

        const now = Date.now();
        const elapsed = now - this.lastSentTime;
        if (elapsed < this.minDelayMs) {
            const waitTime = this.minDelayMs - elapsed;
            logger.info(`Rate limit protection: waiting ${waitTime}ms before sending Telegram message...`);
            await new Promise(resolve => setTimeout(resolve, waitTime));
        }

        const url = `https://api.telegram.org/bot${token}/sendMessage`;
        const postData = {
            chat_id: chatId,
            text: text,
            parse_mode: 'Markdown'
        };

        if (topicId !== undefined && topicId !== null && topicId !== '') {
            postData.message_thread_id = parseInt(topicId, 10);
        }

        try {
            logger.info(`Sending Telegram message to chat ${chatId} (topic: ${topicId || 'default'})`);
            const response = await axios.post(url, postData, { timeout: 10000 });

            this.lastSentTime = Date.now();
            return response.data;
        } catch (error) {
            this.lastSentTime = Date.now();

            if (error.response && error.response.status === 429) {
                const retryAfter = error.response.data?.parameters?.retry_after || 5;
                logger.warn(`Telegram API rate limit (429) encountered. Waiting ${retryAfter} seconds before retrying...`);
                await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));

                return this.sendMessage(payload);
            }

            const errorDescription = error.response?.data?.description || '';
            const isMarkdownError = errorDescription.includes('can\'t find end of') || 
                                    errorDescription.includes('bad request') || 
                                    errorDescription.includes('can\'t parse');
            
            if (isMarkdownError) {
                logger.warn(`Markdown parsing failed for Telegram message. Falling back to plain text. Error: ${errorDescription}`);
                delete postData.parse_mode;
                postData.text = "[Markdown Fallback]\n" + text;
                try {
                    const retryResponse = await axios.post(url, postData, { timeout: 10000 });
                    return retryResponse.data;
                } catch (retryError) {
                    logger.error(`Telegram plain text retry failed: ${retryError.message}`);
                    throw retryError;
                }
            }

            logger.error(`Telegram API request failed: ${error.message} (${errorDescription})`);
            throw error;
        }
    }
}

module.exports = TelegramService;