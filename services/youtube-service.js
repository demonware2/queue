require('dotenv').config();
const { spawn } = require('child_process');

class YouTubeService {
  constructor(redisInstance = null) {
    this.processes = {};
    this.redis = redisInstance;
    this.maxDailyUploads = parseInt(process.env.YOUTUBE_DAILY_UPLOAD_LIMIT) || 100;
    this.redisKey = 'youtube:uploads:count_today';
  }

  async init() {
    console.log('Initializing YouTubeService');
    return true;
  }

  getSecondsUntilPacificMidnight() {
    try {
      const now = new Date();
      const ptString = now.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' });
      const ptDate = new Date(ptString);
      const midnightPt = new Date(ptDate);
      midnightPt.setDate(midnightPt.getDate() + 1);
      midnightPt.setHours(0, 0, 0, 0);

      return Math.max(60, Math.floor((midnightPt.getTime() - ptDate.getTime()) / 1000));
    } catch (e) {
      return 3600 * 12;
    }
  }

  async checkQuota() {
    try {
      const count = await this.redis.get(this.redisKey);
      const current = count ? parseInt(count) : 0;
      if (current >= this.maxDailyUploads) {
        const ttl = await this.redis.ttl(this.redisKey);
        const waitTime = ttl > 0 ? ttl : this.getSecondsUntilPacificMidnight();
        throw new Error('YouTube daily upload quota reached (' + current + '/' + this.maxDailyUploads + '). Quota resets in ' + Math.round(waitTime / 60) + ' minutes.');
      }
      return true;
    } catch (err) {
      if (err.message.includes('quota reached')) {
        throw err;
      }
      console.warn('[YouTubeService] Quota check warning:', err.message);
      return true;
    }
  }

  async runUpload(payload) {
    const { recording_id, privacy = 'unlisted' } = payload || {};
    if (!recording_id) {
      throw new Error('recording_id is required for YouTube upload.');
    }

    await this.checkQuota();

    const sparkPath = process.env.SIROUM_SPARK_PATH || '/var/www/html/biroumum/spark';

    let commandScript = 'php';
    let commandArgs = [
      sparkPath,
      'zoom:youtube-upload',
      '--recording_id', String(recording_id),
      '--privacy', String(privacy)
    ];

    console.log('[YouTubeService] Running:', commandScript, commandArgs.join(' '));

    let stdoutChunks = [];
    let stderrChunks = [];

    const childProcess = spawn(commandScript, commandArgs, {
      stdio: 'pipe',
      shell: false
    });

    const jobId = 'youtube_' + recording_id + '_' + Date.now();
    this.processes[jobId] = childProcess;

    childProcess.stdout.on('data', (chunk) => {
      stdoutChunks.push(chunk.toString());
    });

    childProcess.stderr.on('data', (chunk) => {
      stderrChunks.push(chunk.toString());
    });

    return new Promise((resolve, reject) => {
      childProcess.on('close', async (code) => {
        delete this.processes[jobId];
        const output = stdoutChunks.join('');
        const error = stderrChunks.join('');

        if (code === 0 && (output.includes('SUCCESS') || output.includes('berhasil'))) {
          if (this.redis) {
            try {
              const newCount = await this.redis.incr(this.redisKey);
              if (newCount === 1) {
                await this.redis.expire(this.redisKey, this.getSecondsUntilPacificMidnight());
              }
            } catch (rErr) {
              console.warn('[YouTubeService] Failed to increment Redis quota:', rErr.message);
            }
          }

          resolve({
            exitCode: code,
            output,
            error
          });
        } else {
          reject(new Error('YouTube upload failed with code ' + code + '. Output: ' + output + ' Error: ' + error));
        }
      });

      childProcess.on('error', (error) => {
        delete this.processes[jobId];
        reject(error);
      });
    });
  }
}

module.exports = YouTubeService;