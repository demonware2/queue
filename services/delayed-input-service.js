const WebhookService = require('./webhook-service');

const RESTORE_LOCK_KEY = 'delayed_input:restore_lock';
const RESTORE_LOCK_TTL_MS = 30000; // 30 seconds
const MAX_RETRIES = 5;
const RETRY_DELAY_MS = 10000; // 10 seconds retry delay

class DelayedInputService {
    constructor(redis) {
        this.redis = redis;
        this.webhookService = new WebhookService(redis);
        this.restoreScheduledTriggers().catch((err) => {
            console.error('[DelayedInputService] Recovery error:', err);
        });
    }

    async process(payload) {
        const { key, delay, webhook_url, secret, event, headers } = payload;
        if (!key) {
            throw new Error('Redis key is required for delayed_input');
        }

        const activeKey = `delayed_active:${key}`;
        const delayMinutes = parseFloat(delay) || 0;
        const delayMs = delayMinutes * 60 * 1000;
        const scheduledTime = Date.now() + delayMs;
        const stored = JSON.stringify({ scheduledTime, payload, retryCount: 0 });

        const ttlSeconds = Math.ceil(delayMs / 1000) + 3600; // Delay + 1 hour safety margin
        const set = await this.redis.set(activeKey, stored, 'EX', ttlSeconds, 'NX');

        if (!set) {
            console.log(`[DelayedInputService] Key "${key}" already scheduled. Skipping to preserve initial delay.`);
            return { status: 'ignored', reason: 'already_scheduled' };
        }

        console.log(`[DelayedInputService] Scheduling trigger for key "${key}" in ${delayMinutes} minutes.`);

        setTimeout(() => {
            this.triggerWebhook(payload, activeKey).catch((err) => {
                console.error(`[DelayedInputService] Error executing trigger for key "${key}":`, err);
            });
        }, delayMs);

        return { status: 'scheduled', scheduledTime };
    }

    async triggerWebhook(payload, activeKey) {
        const { key, webhook_url, secret, event, headers } = payload;
        const valStr = await this.redis.get(activeKey);
        const deleted = await this.redis.del(activeKey);
        if (deleted === 0 || !valStr) {
            console.log(`[DelayedInputService] Trigger for key "${key}" was already claimed by another worker. Skipping.`);
            return;
        }

        let retryCount = 0;
        try {
            const data = JSON.parse(valStr);
            retryCount = data.retryCount || 0;
        } catch (e) {
            retryCount = 0;
        }

        try {
            const exists = await this.redis.exists(key);
            if (!exists) {
                throw new Error(`Redis key "${key}" has no data/does not exist yet.`);
            }

            console.log(`[DelayedInputService] Redis key "${key}" contains data. Triggering webhook to: ${webhook_url}`);
            
            const webhookPayload = {
                url: webhook_url,
                secret,
                event: event || 'delayed_input',
                data: { key },
                headers
            };

            const response = await this.webhookService.send(webhookPayload);
            if (!response || response.status === 'failed') {
                const bodyDetail = (response && response.response_body) ? `: ${response.response_body}` : '';
                const errMsg = ((response && response.error_message) || 'Webhook service returned failed status') + bodyDetail;
                throw new Error(errMsg);
            }

            console.log(`[DelayedInputService] Webhook triggered successfully for key: ${key}`);
        } catch (err) {
            if (retryCount < MAX_RETRIES) {
                const nextRetryCount = retryCount + 1;
                const nextRetryDelay = RETRY_DELAY_MS;
                const scheduledTime = Date.now() + nextRetryDelay;
                
                const retryStored = JSON.stringify({
                    scheduledTime,
                    payload,
                    retryCount: nextRetryCount
                });

                const ttlSeconds = Math.ceil(nextRetryDelay / 1000) + 3600;
                await this.redis.set(activeKey, retryStored, 'EX', ttlSeconds);
                
                console.warn(`[DelayedInputService] Trigger for key "${key}" failed (${err.message}). Rescheduling retry #${nextRetryCount} in ${nextRetryDelay / 1000} seconds.`);

                setTimeout(() => {
                    this.triggerWebhook(payload, activeKey).catch((retryErr) => {
                        console.error(`[DelayedInputService] Error executing retry trigger for key "${key}":`, retryErr);
                    });
                }, nextRetryDelay);
            } else {
                const failedStored = JSON.stringify({
                    scheduledTime: Date.now(),
                    payload,
                    _retryOf: key,
                    failedPermanently: true,
                    error: err.message
                });

                const failedKey = `delayed_failed:${key}`;
                await this.redis.set(failedKey, failedStored, 'EX', 3600);
                console.error(`[DelayedInputService] Webhook trigger failed permanently for key "${key}" after ${MAX_RETRIES} retries. Error: ${err.message}`);
            }
            throw err;
        }
    }

    async restoreScheduledTriggers() {
        const lockId = `${process.pid}-${Date.now()}`;
        const locked = await this.redis.set(RESTORE_LOCK_KEY, lockId, 'PX', RESTORE_LOCK_TTL_MS, 'NX');

        if (!locked) {
            console.log('[DelayedInputService] Another worker is already restoring triggers. Skipping.');
            return;
        }

        try {
            console.log('[DelayedInputService] Restoring pending scheduled triggers...');
            const keys = await this.redis.keys('delayed_active:*');

            if (keys.length === 0) {
                console.log('[DelayedInputService] No pending scheduled triggers found.');
                return;
            }

            console.log(`[DelayedInputService] Found ${keys.length} pending triggers to restore.`);

            for (const activeKey of keys) {
                try {
                    const val = await this.redis.get(activeKey);
                    if (!val) continue;

                    const { scheduledTime, payload } = JSON.parse(val);
                    const delayMs = Math.max(0, scheduledTime - Date.now());

                    console.log(`[DelayedInputService] Restoring trigger for key "${payload.key}" in ${Math.round(delayMs / 1000)} seconds.`);

                    setTimeout(() => {
                        this.triggerWebhook(payload, activeKey).catch((err) => {
                            console.error(`[DelayedInputService] Error executing restored trigger for key "${payload.key}":`, err);
                        });
                    }, delayMs);
                } catch (err) {
                    console.error(`[DelayedInputService] Failed to restore trigger for key "${activeKey}":`, err);
                }
            }
        } finally {
            const current = await this.redis.get(RESTORE_LOCK_KEY);
            if (current === lockId) {
                await this.redis.del(RESTORE_LOCK_KEY);
            }
        }
    }
}

module.exports = DelayedInputService;