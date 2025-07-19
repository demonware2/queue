const Redis = require('ioredis');
const config = require('./config');
const axios = require('axios');
const minimist = require('minimist');
const EmailService = require('./services/email-service');
const CronjobService = require('./services/cronjob-service');
const WhatsAppService = require('./services/whatsapp-service');

const args = minimist(process.argv.slice(2));
const workerId = args.id;
const workerType = args.type;

if (!workerId || !workerType) {
  console.error('Worker ID and type are required');
  process.exit(1);
}

const redis = new Redis(config.redis);
const subscriber = new Redis(config.redis);

let emailService = null;
let cronjobService = null;
let healthCheckInterval = null;
let whatsAppService = null;

function setupEmailHealthCheck() {
  if (emailService && emailService.useBackup && !healthCheckInterval) {
    console.log(`Worker ${workerId}: Starting periodic email service health checks`);
    
    healthCheckInterval = setInterval(async () => {
      try {
        console.log(`Worker ${workerId}: Checking main email service health...`);
        const healthy = await emailService.checkServiceHealth();
        
        if (!emailService.useBackup) {
          console.log(`Worker ${workerId}: Main email service recovered, stopping health checks`);
          clearInterval(healthCheckInterval);
          healthCheckInterval = null;
        }
      } catch (error) {
        console.error(`Worker ${workerId}: Email health check error:`, error.message);
      }
    }, 5 * 60 * 1000);
  }
}

if (workerType === config.jobTypes.EMAIL) {
  emailService = new EmailService();
}

if (workerType === config.jobTypes.CRONJOB) {
  cronjobService = new CronjobService();
}

if (workerType === config.jobTypes.WHATSAPP) {
  whatsAppService = new WhatsAppService();
}

const API_ENDPOINTS = {
  [config.jobTypes.SMS]: 'http://localhost/ci4/api/sms',
  [config.jobTypes.NOTIFICATION]: 'http://localhost/ci4/api/notification',
};

async function processJob(job) {
  console.log(`[DEBUG] Worker ${workerId} starting to process job ${job.id} of type ${job.type}`);
  console.log(`[DEBUG] Job payload: ${JSON.stringify(job.payload)}`);
  
  try {
    console.log(`Worker ${workerId} processing job ${job.id} of type ${job.type}`);

    try {
      await axios.patch(`http://localhost:${config.server.port}/api/jobs/${job.id}`, {
        status: 'processing',
        workerId
      });
      console.log(`[DEBUG] Job ${job.id} marked as processing`);
    } catch (patchError) {
      console.error(`[DEBUG] Error updating job status: ${patchError.message}`);
      throw patchError;
    }

    try {
      await axios.patch(`http://localhost:${config.server.port}/api/workers/${workerId}`, {
        status: 'busy'
      });
      console.log(`[DEBUG] Worker ${workerId} marked as busy`);
    } catch (workerError) {
      console.error(`[DEBUG] Error updating worker status: ${workerError.message}`);
      throw workerError;
    }

    let result;

    if (job.type === config.jobTypes.CRONJOB && cronjobService) {
      console.log(`[DEBUG] Starting cronjob execution for script: ${job.payload.script}`);
      try {
        result = await cronjobService.runScript(job.payload);
        console.log(`[DEBUG] Cronjob execution completed with result: ${JSON.stringify(result)}`);
        
        if (job.payload && job.payload.taskId) {
          const taskId = job.payload.taskId;
          console.log(`[DEBUG] Updating task_scheduler for task ${taskId} to completed`);
          
          try {
            await cronjobService.updateTaskStatus(
              taskId, 
              'success', 
              result.output + (result.error ? '\n\nErrors:\n' + result.error : '')
            );
            console.log(`[DEBUG] Successfully updated task_scheduler for task ${taskId}`);
          } catch (taskError) {
            console.error(`[DEBUG] Error updating task_scheduler: ${taskError.message}`);
          }
        }
      } catch (scriptError) {
        console.error(`[DEBUG] Error running script: ${scriptError.message}`);
        throw scriptError;
      }
    } else if (job.type === config.jobTypes.EMAIL && emailService) {
      
      if (!job.payload.to || !job.payload.subject || !job.payload.html) {
        throw new Error('Email payload must include "to", "subject", and "html" fields');
      }
      
      result = await emailService.sendEmail(job.payload);
    } else if (job.type === config.jobTypes.WHATSAPP && whatsAppService) {
      if (!job.payload.message) {
        throw new Error('Message is required for WhatsApp');
      }
      
      if (job.payload.groupId) {
        console.log(`[DEBUG] Starting WhatsApp group message to group ${job.payload.groupId}`);
        result = await whatsAppService.sendGroupMessage(job.payload);
      } else if (job.payload.number) {
        console.log(`[DEBUG] Starting WhatsApp message to number ${job.payload.number}`);
        result = await whatsAppService.sendMessage(job.payload);
      } else {
        throw new Error('Either number or groupId must be provided for WhatsApp message');
      }
    } else {
      const endpoint = API_ENDPOINTS[job.type];
      if (!endpoint) {
        throw new Error(`Unknown job type: ${job.type}`);
      }
      const response = await axios.post(endpoint, job.payload, {
        headers: {
          'Content-Type': 'application/json',
          'X-Job-Type': job.type,
          'X-Worker-ID': workerId
        }
      });
      result = response.data;
    }
    
    console.log(`[DEBUG] About to mark job ${job.id} as completed`);
    try {
      await axios.patch(`http://localhost:${config.server.port}/api/jobs/${job.id}`, {
        status: 'completed',
        result: result
      });
      console.log(`[DEBUG] Job ${job.id} marked as completed`);
    } catch (completeError) {
      console.error(`[DEBUG] Error marking job as completed: ${completeError.message}`);
      throw completeError;
    }

    console.log(`[DEBUG] About to mark worker ${workerId} as idle`);
    try {
      await axios.patch(`http://localhost:${config.server.port}/api/workers/${workerId}`, {
        status: 'idle'
      });
      console.log(`[DEBUG] Worker ${workerId} marked as idle`);
    } catch (idleError) {
      console.error(`[DEBUG] Error marking worker as idle: ${idleError.message}`);
    }

    try {
      await redis.publish('worker:job-complete', JSON.stringify({
        jobId: job.id,
        workerId,
        result: result
      }));
      console.log(`[DEBUG] Published job completion event for job ${job.id}`);
    } catch (pubError) {
      console.error(`[DEBUG] Error publishing job completion: ${pubError.message}`);
    }
    
    return result;
  } catch (error) {
    console.error(`Worker ${workerId} failed to process job ${job.id}:`, error.message);
    console.error(`[DEBUG] Full error: ${error.stack}`);
    
    try {
      await axios.patch(`http://localhost:${config.server.port}/api/jobs/${job.id}`, {
        status: 'failed',
        result: { error: error.message }
      });
      console.log(`[DEBUG] Job ${job.id} marked as failed`);
    } catch (failedError) {
      console.error(`[DEBUG] Error marking job as failed: ${failedError.message}`);
    }
    
    try {
      await axios.patch(`http://localhost:${config.server.port}/api/workers/${workerId}`, {
        status: 'idle'
      });
      console.log(`[DEBUG] Worker ${workerId} marked as idle after failure`);
    } catch (idleError) {
      console.error(`[DEBUG] Error marking worker as idle after failure: ${idleError.message}`);
    }
    
    try {
      await redis.publish('worker:job-failed', JSON.stringify({
        jobId: job.id,
        workerId,
        error: error.message
      }));
      console.log(`[DEBUG] Published job failure event for job ${job.id}`);
    } catch (pubError) {
      console.error(`[DEBUG] Error publishing job failure: ${pubError.message}`);
    }
    
    throw error;
  }
}

async function pollForJobs() {
  try {
    const workerResponse = await axios.get(`http://localhost:${config.server.port}/api/workers/${workerId}`);
    if (workerResponse.data && workerResponse.data.worker && workerResponse.data.worker.status !== 'busy') {
      await axios.patch(`http://localhost:${config.server.port}/api/workers/${workerId}`, {
        status: 'idle'
      });
    } else {
      setTimeout(pollForJobs, 1000);
      return;
    }

    const response = await axios.get(`http://localhost:${config.server.port}/api/jobs/next/${workerType}`);
    
    if (response.data && response.data.job) {
      await axios.patch(`http://localhost:${config.server.port}/api/workers/${workerId}`, {
        status: 'busy'
      });
      
      await processJob(response.data.job);
    }
  } catch (error) {
    console.error(`Worker ${workerId} error:`, error.message);
  }

  setTimeout(pollForJobs, 1000);
}

async function main() {
  try {
    if (workerType === config.jobTypes.EMAIL && emailService) {
      const initialized = await emailService.init();
      if (!initialized) {
        throw new Error('Failed to initialize email service');
      }
      
      if (emailService.useBackup) {
        setupEmailHealthCheck();
      }
    }

    if (workerType === config.jobTypes.CRONJOB && cronjobService) {
      const initialized = await cronjobService.init();
      if (!initialized) {
        throw new Error('Failed to initialize cronjob service');
      }
      console.log(`Worker ${workerId}: Cronjob service initialized successfully`);
    }
    
    await subscriber.subscribe('job:new');
    
    subscriber.on('message', (channel, message) => {
      const data = JSON.parse(message);

      if (channel === 'job:new' && data.type === workerType) {
        pollForJobs();
      }
    });
    
    console.log(`Worker ${workerId} (${workerType}) started`);
    
    pollForJobs();
  } catch (error) {
    console.error(`Worker ${workerId} initialization error:`, error.message);
    process.exit(1);
  }
}

process.on('SIGTERM', async () => {
  console.log(`Worker ${workerId} shutting down...`);
  
  if (healthCheckInterval) {
    clearInterval(healthCheckInterval);
    healthCheckInterval = null;
  }

  if (cronjobService) {
    await cronjobService.shutdown();
  }
  
  await subscriber.quit();
  await redis.quit();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log(`Worker ${workerId} shutting down...`);
  
  if (healthCheckInterval) {
    clearInterval(healthCheckInterval);
    healthCheckInterval = null;
  }

  if (cronjobService) {
    await cronjobService.shutdown();
  }
  
  await subscriber.quit();
  await redis.quit();
  process.exit(0);
});

main();
