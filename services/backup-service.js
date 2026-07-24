require('dotenv').config();

const { spawn } = require('child_process');

class BackupService {
  constructor() {
    this.processes = {};
  }

  async init() {
    console.log('Initializing BackupService');
    return true;
  }

  async runBackup(payload) {
    const { name, backup_type, storage = 'local' } = payload;
    const sparkPath = process.env.BACKUP_SPARK_PATH || '/var/www/siroum/spark';
    const timeoutPath = process.env.BACKUP_TIMEOUT_PATH || '';

    let commandScript;
    let commandArgs = [];

    if (timeoutPath) {
      commandScript = timeoutPath;
      commandArgs.push('1900', sparkPath);
    } else {
      commandScript = sparkPath;
    }

    commandArgs.push(
      'database_backup_siroum:run_single',
      '--name',
      name,
      '--type',
      backup_type,
      '--storage',
      storage
    );

    console.log(`Running backup command: ${commandScript} ${commandArgs.join(' ')}`);

    let stdoutChunks = [];
    let stderrChunks = [];

    const childProcess = spawn(commandScript, commandArgs, {
      stdio: 'pipe',
      shell: true
    });

    const jobId = `backup_${name}_${Date.now()}`;
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
        resolve({
          exitCode: code,
          output: stdoutChunks.join(''),
          error: stderrChunks.join('')
        });
      });

      childProcess.on('error', (error) => {
        delete this.processes[jobId];
        reject(error);
      });
    });
  }
}

module.exports = BackupService;