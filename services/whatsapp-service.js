const axios = require('axios');
const dotenv = require('dotenv');

dotenv.config();

class WhatsAppService {
  constructor() {
    this.defaultBaseUrl = process.env.WHATSAPP_API_URL || 'http://localhost:7827';
    this.defaultDelayMs = process.env.WHATSAPP_DELAY_MS || 5000;
    this.initialized = new Map();
    this.messageQueues = new Map();

    this.wablasToken = process.env.WABLAS_TOKEN || '';
    this.wablasSecret = process.env.WABLAS_SECRET || '';
    this.wablasBaseUrl = 'https://bdg.wablas.com/api';
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

      if (response.data && (response.data.ready || response.data.connecting)) {
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

  getQueueForPort(baseUrl) {
    if (!this.messageQueues.has(baseUrl)) {
      this.messageQueues.set(baseUrl, Promise.resolve());
    }
    return this.messageQueues.get(baseUrl);
  }

  async sendMessage(payload) {
    const baseUrl = payload.baseUrl || this.defaultBaseUrl;

    const currentQueue = this.getQueueForPort(baseUrl);
    const newQueue = currentQueue.then(async () => {
      return this._sendMessageInternal(payload);
    });

    this.messageQueues.set(baseUrl, newQueue);
    return newQueue;
  }

  async _sendMessageInternal(payload) {
    const baseUrl = payload.baseUrl || this.defaultBaseUrl;

    try {
      if (!this.initialized.get(baseUrl)) {
        await this.init(baseUrl);
        if (!this.initialized.get(baseUrl)) {
          throw new Error(`WhatsApp service is not initialized at ${baseUrl}`);
        }
      }

      if (!payload.number || !payload.message) {
        throw new Error('Number and message are required for WhatsApp message');
      }

      const delayMs = parseInt(payload.delay || payload.delayMs || this.defaultDelayMs);

      console.log(`[${baseUrl}] Processing message to ${payload.number} (delay: ${delayMs}ms)`);

      await new Promise(resolve => setTimeout(resolve, delayMs));

      const response = await axios.post(`${baseUrl}/send-message`, {
        number: payload.number,
        message: payload.message
      }, {
        timeout: 30000
      });

      const sentTime = new Date().toLocaleTimeString();
      console.log(`[${baseUrl}] ✅ Message sent to ${payload.number} at ${sentTime}`);

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
      console.error(`[${baseUrl}] WhatsApp message to ${payload.number} failed:`, error.message);

      console.log(`[BACKUP] Trying Wablas for ${payload.number}`);
      return await this._sendViaWablas(payload.number, payload.message, false);
    }
  }

  async sendGroupMessage(payload) {
    const baseUrl = payload.baseUrl || this.defaultBaseUrl;

    const currentQueue = this.getQueueForPort(baseUrl);
    const newQueue = currentQueue.then(async () => {
      return this._sendGroupMessageInternal(payload);
    });

    this.messageQueues.set(baseUrl, newQueue);
    return newQueue;
  }

  async _sendGroupMessageInternal(payload) {
    const baseUrl = payload.baseUrl || this.defaultBaseUrl;

    try {
      if (!this.initialized.get(baseUrl)) {
        await this.init(baseUrl);
        if (!this.initialized.get(baseUrl)) {
          throw new Error(`WhatsApp service is not initialized at ${baseUrl}`);
        }
      }

      if (!payload.groupId || !payload.message) {
        throw new Error('Group ID and message are required for WhatsApp group message');
      }

      const delayMs = parseInt(payload.delay || payload.delayMs || this.defaultDelayMs);

      console.log(`[${baseUrl}] Processing group message to ${payload.groupId} (delay: ${delayMs}ms)`);

      await new Promise(resolve => setTimeout(resolve, delayMs));

      const response = await axios.post(`${baseUrl}/send-group-message`, {
        groupId: payload.groupId,
        message: payload.message
      }, {
        timeout: 30000
      });

      const sentTime = new Date().toLocaleTimeString();
      console.log(`[${baseUrl}] ✅ Group message sent to ${payload.groupId} at ${sentTime}`);

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
      console.error(`[${baseUrl}] WhatsApp group message to ${payload.groupId} failed:`, error.message);

      console.log(`[BACKUP] Trying Wablas for group ${payload.groupId}`);
      return await this._sendViaWablas(payload.groupId, payload.message, true);
    }
  }

  async _sendViaWablas(target, message, isGroup = false) {
    if (!this.wablasToken || !this.wablasSecret) {
      throw new Error('Wablas credentials not configured');
    }

    try {
      let response;
      const sentTime = new Date().toLocaleTimeString();

      if (isGroup) {
        const url = `${this.wablasBaseUrl}/send-message?phone=${encodeURIComponent(target)}&message=${encodeURIComponent(message)}&token=${this.wablasToken}`;

        response = await axios.get(url, {
          timeout: 30000,
          headers: {
            'Content-Type': 'application/json'
          }
        });
      } else {
        const payload = {
          data: [
            {
              phone: target,
              message: message,
              isGroup: 'false'
            }
          ]
        };

        response = await axios.post(`${this.wablasBaseUrl}/v2/send-message`, payload, {
          timeout: 30000,
          headers: {
            'Authorization': `${this.wablasToken}.${this.wablasSecret}`,
            'Content-Type': 'application/json'
          }
        });
      }

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

  resetQueue(baseUrl = null) {
    const targetUrl = baseUrl || this.defaultBaseUrl;
    this.messageQueues.set(targetUrl, Promise.resolve());
    console.log(`[${targetUrl}] WhatsApp message queue reset`);
  }

  resetAllQueues() {
    this.messageQueues.clear();
    console.log('All WhatsApp message queues reset');
  }

  getQueueStatus() {
    const status = {};
    for (const [baseUrl, queue] of this.messageQueues) {
      status[baseUrl] = {
        hasQueue: !!queue,
        initialized: this.initialized.get(baseUrl) || false
      };
    }
    return status;
  }
}

module.exports = WhatsAppService;