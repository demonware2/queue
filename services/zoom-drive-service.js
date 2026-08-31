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
    const sparkPath = process.env.ZOOM_SPARK_PATH || '/var/www/siroum/spark';

    let commandScript = 'php';
    let commandArgs = [sparkPath, 'zoom:sync-drive'];

    if (recording_id) {
      commandArgs.push('--recording_id', String(recording_id));
    }
    if (file_id) {
      commandArgs.push('--file_id', String(file_id));
    }

    console.log('[ZoomDriveService] Running:', commandScript, commandArgs.join(' '));

    let stdoutChunks = [];
    let stderrChunks = [];

    const childProcess = spawn(commandScript, commandArgs, {
      stdio: 'pipe',
      shell: false
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