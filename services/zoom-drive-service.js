require('dotenv').config();
const { spawn } = require('child_process');

class ZoomDriveService {
  constructor() {
    this.processes = {};
  }

  async init() {
    console.log('Initializing ZoomDriveService');
    return true;
  }

  async runSync(payload) {
    const { recording_id, file_id } = payload || {};
    const sparkPath = process.env.SIROUM_SPARK_PATH || process.env.BACKUP_SPARK_PATH || '/var/www/html/biroumum/spark';
    const commandPrefix = (process.env.BACKUP_COMMAND_PREFIX || process.env.GIT_DEPLOY_COMMAND_PREFIX || 'php83').replace(/^["']|["']$/g, '').trim();

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
        reject(error);
      });
    });
  }
}

module.exports = ZoomDriveService;