require('dotenv').config();
const { spawn, exec } = require('child_process');

class ZoomDriveService {
  constructor(redis = null) {
    this.redis = redis;
    this.processes = {};
    this.lastReplenishAt = 0;
    this.isReplenishing = false;
  }

  async init() {
    console.log('Initializing ZoomDriveService');
    return true;
  }

  async runSync(payload) {
    const { recording_id, file_id } = payload || {};
    const sparkPath = process.env.SIROUM_SPARK_PATH || process.env.BACKUP_SPARK_PATH || '/var/www/siroum/spark';
    const commandPrefix = (process.env.BACKUP_COMMAND_PREFIX || process.env.GIT_DEPLOY_COMMAND_PREFIX || 'php').replace(/^["']|["']$/g, '').trim();

    let fullCommand = `${commandPrefix} ${sparkPath} zoom:sync-drive`;
    if (recording_id) {
      fullCommand += ` --recording_id ${recording_id}`;
    }
    if (file_id) {
      fullCommand += ` --file_id ${file_id}`;
    }

    console.log('[ZoomDriveService] Running:', fullCommand);

    let stdoutChunks = [];
    let stderrChunks = [];

    const childProcess = spawn(fullCommand, [], {
      stdio: 'pipe',
      shell: true
    });

    const jobId = 'zoom_drive_' + (recording_id || file_id || Date.now());
    this.processes[jobId] = childProcess;

    childProcess.stdout.on('data', (chunk) => {
      stdoutChunks.push(chunk.toString());
    });

    childProcess.stderr.on('data', (chunk) => {
      stderrChunks.push(chunk.toString());
    });

    return new Promise((resolve, reject) => {
      childProcess.on('close', (code) => {
        delete this.processes[jobId];

        this.checkAndReplenish().catch(() => {});

        if (code === 0) {
          resolve({
            exitCode: code,
            output: stdoutChunks.join(''),
            error: stderrChunks.join('')
          });
        } else {
          reject(new Error('Zoom Drive sync exited with code ' + code + '. Output: ' + stdoutChunks.join('') + ' Error: ' + stderrChunks.join('')));
        }
      });

      childProcess.on('error', (error) => {
        delete this.processes[jobId];
        this.checkAndReplenish().catch(() => {});
        reject(error);
      });
    });
  }

  async checkAndReplenish() {
    if (!this.redis) return;
    const now = Date.now();
    if (now - this.lastReplenishAt < 3000 || this.isReplenishing) return;

    try {
      const queueKey = 'jobs:zoom_sync_drive';
      const queueLen = await this.redis.llen(queueKey);
      if (queueLen > 0) return;

      this.isReplenishing = true;
      this.lastReplenishAt = now;

      const sparkPath = process.env.SIROUM_SPARK_PATH || process.env.BACKUP_SPARK_PATH || '/var/www/siroum/spark';
      const commandPrefix = (process.env.BACKUP_COMMAND_PREFIX || process.env.GIT_DEPLOY_COMMAND_PREFIX || 'php').replace(/^["']|["']$/g, '').trim();
      const cmd = `${commandPrefix} ${sparkPath} zoom:scan-and-queue-drive --replenish_only=1`;

      exec(cmd, (error, stdout, stderr) => {
        this.isReplenishing = false;
        if (error) {
          console.warn(`[ZoomDriveService] Replenish check error: ${error.message}`);
        } else if (stdout && stdout.includes('Successfully enqueued')) {
          console.log(`[ZoomDriveService] Replenished next batch: ${stdout.trim()}`);
        }
      });
    } catch (e) {
      this.isReplenishing = false;
      console.warn(`[ZoomDriveService] Failed replenish check: ${e.message}`);
    }
  }
}

module.exports = ZoomDriveService;