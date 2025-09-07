const Redis = require('ioredis');
const config = require('../config');

class QueueService {
  constructor() {
    this.redis = new Redis(config.redis);
    this.subscriber = new Redis(config.redis);
    this.publisher = new Redis(config.redis);
    this.handlers = {};
  }

  async init() {
    await this.subscriber.subscribe('worker:job-complete', 'worker:job-failed');
    
    this.subscriber.on('message', (channel, message) => {
      let data;
      try {
        data = JSON.parse(message);
      } catch (e) {
        console.warn(`QueueService: failed to parse message on ${channel}: ${e.message}`);
        return;
      }

      if (channel === 'worker:job-complete' && this.handlers.onJobComplete) {
        try {
          const p = this.handlers.onJobComplete(data);
          if (p && typeof p.then === 'function') p.catch(err => console.error('onJobComplete handler error:', err));
        } catch (err) {
          console.error('onJobComplete handler threw:', err);
        }
      }

      if (channel === 'worker:job-failed' && this.handlers.onJobFailed) {
        try {
          const p = this.handlers.onJobFailed(data);
          if (p && typeof p.then === 'function') p.catch(err => console.error('onJobFailed handler error:', err));
        } catch (err) {
          console.error('onJobFailed handler threw:', err);
        }
      }
    });
  }

  setHandlers(handlers) {
    this.handlers = handlers;
  }

  async addJob(jobId, type, payload) {
    await this.redis.lpush(`jobs:${type}`, JSON.stringify({
      id: jobId,
      type,
      payload
    }));
    
    await this.publisher.publish('job:new', JSON.stringify({ type }));
    
    return jobId;
  }

  async getNextJob(type) {
    const jobData = await this.redis.rpop(`jobs:${type}`);
    if (!jobData) return null;
    try {
      return JSON.parse(jobData);
    } catch (e) {
      console.warn(`QueueService: failed to parse job data for type ${type}: ${e.message}`);
      return null;
    }
  }

  async jobComplete(jobId, workerId, result) {
    await this.publisher.publish('worker:job-complete', JSON.stringify({
      jobId,
      workerId,
      result
    }));
  }

  async jobFailed(jobId, workerId, error) {
    await this.publisher.publish('worker:job-failed', JSON.stringify({
      jobId,
      workerId,
      error
    }));
  }

  async shutdown() {
    await this.subscriber.quit();
    await this.publisher.quit();
    await this.redis.quit();
  }
}

module.exports = QueueService;
