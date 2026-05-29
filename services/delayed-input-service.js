const WebhookService = require('./webhook-service');

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
        const existing = await this.redis.get(activeKey);

        if (existing) {
            console.log(`[DelayedInputService] Key "${key}" already scheduled. Skipping to preserve initial delay.`);
            return { status: 'ignored', reason: 'already_scheduled' };
        }

        const delayMinutes = parseFloat(delay) || 0;
        const delayMs = delayMinutes * 60 * 1000;
        const scheduledTime = Date.now() + delayMs;

        console.log(`[DelayedInputService] Scheduling trigger for key "${key}" in ${delayMinutes} minutes.`);

        await this.redis.set(activeKey, JSON.stringify({ scheduledTime, payload }));

        setTimeout(() => {
            this.triggerWebhook(payload, activeKey).catch((err) => {
                console.error(`[DelayedInputService] Error executing trigger for key "${key}":`, err);
            });
        }, delayMs);

        return { status: 'scheduled', scheduledTime };
    }

    async triggerWebhook(payload, activeKey) {
        const { key, webhook_url, secret, event, headers } = payload;
        
        try {
            const exists = await this.redis.exists(key);
            if (!exists) {
                console.log(`[DelayedInputService] Redis key "${key}" has no data/does not exist. Skipping webhook trigger.`);
                return;
            }

            console.log(`[DelayedInputService] Redis key "${key}" contains data. Triggering webhook to: ${webhook_url}`);
            
            const webhookPayload = {
                url: webhook_url,
                secret,
                event: event || 'delayed_input',
                data: { key },
                headers
            };

            await this.webhookService.send(webhookPayload);
            console.log(`[DelayedInputService] Webhook triggered successfully for key: ${key}`);
        } finally {
            await this.redis.del(activeKey);
        }
    }

    async restoreScheduledTriggers() {
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
                const delayMs = scheduledTime - Date.now();

                console.log(`[DelayedInputService] Restoring trigger for key "${payload.key}" in ${Math.max(0, delayMs) / 1000} seconds.`);

                setTimeout(() => {
                    this.triggerWebhook(payload, activeKey).catch((err) => {
                        console.error(`[DelayedInputService] Error executing restored trigger for key "${payload.key}":`, err);
                    });
                }, Math.max(0, delayMs));
            } catch (err) {
                console.error(`[DelayedInputService] Failed to restore trigger for key "${activeKey}":`, err);
            }
        }
    }
}

module.exports = DelayedInputService;