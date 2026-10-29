require('dotenv').config();

const { spawn } = require('child_process');
const config = require('../config');

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
    const commandPrefix = process.env.BACKUP_COMMAND_PREFIX || '';

    let commandScript;
    let commandArgs = [];

    if (commandPrefix.trim()) {
      const prefixParts = commandPrefix.trim().split(/\s+/);
      commandScript = prefixParts[0];
      commandArgs = prefixParts.slice(1);
      commandArgs.push(sparkPath);
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
      shell: true,
      detached: true
    });

    const jobId = `backup_${name}_${Date.now()}`;
    this.processes[jobId] = childProcess;

    const timeoutHours = config.jobTimeoutsHours.backup;
    let timedOut = false;
    const timer = timeoutHours > 0 ? setTimeout(() => {
      timedOut = true;
      console.error(`Backup ${name} (${backup_type}) exceeded ${timeoutHours}h, stopping it`);
      this.killProcessGroup(childProcess);
    }, timeoutHours * 3600 * 1000) : null;

    childProcess.stdout.on('data', (chunk) => {
      stdoutChunks.push(chunk.toString());
    });

    childProcess.stderr.on('data', (chunk) => {
      stderrChunks.push(chunk.toString());
    });

    return new Promise((resolve, reject) => {
      childProcess.on('close', (code) => {
        clearTimeout(timer);
        delete this.processes[jobId];
        resolve({
          exitCode: timedOut ? 124 : code,
          output: stdoutChunks.join(''),
          error: timedOut ? `Backup timed out after ${timeoutHours}h and was stopped` : stderrChunks.join('')
        });
      });

      childProcess.on('error', (error) => {
        clearTimeout(timer);
        delete this.processes[jobId];
        reject(error);
      });
    });
  }

  killProcessGroup(childProcess) {
    try {
      process.kill(-childProcess.pid, 'SIGTERM');
      setTimeout(() => {
        try { process.kill(-childProcess.pid, 'SIGKILL'); } catch (e) { /* sudah berhenti */ }
      }, 30_000).unref();
    } catch (e) {
      console.error(`Failed to stop backup process group ${childProcess.pid}: ${e.message}`);
    }
  }

  killAll() {
    Object.values(this.processes).forEach((childProcess) => this.killProcessGroup(childProcess));
  }
}

module.exports = BackupService;