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
      const data = JSON.parse(message);
      
      if (channel === 'worker:job-complete' && this.handlers.onJobComplete) {
        this.handlers.onJobComplete(data);
      }
      
      if (channel === 'worker:job-failed' && this.handlers.onJobFailed) {
        this.handlers.onJobFailed(data);
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
    return jobData ? JSON.parse(jobData) : null;
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
