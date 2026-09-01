require('dotenv').config();
const { spawn } = require('child_process');

class ZoomScanService {
  constructor() {
    this.processes = {};
  }

  async init() {
    console.log('Initializing ZoomScanService');
    return true;
  }

  async runScanCloud(payload) {
    const { from_date, to_date, account_id, user_scope, include_local } = payload || {};
    const sparkPath = process.env.SIROUM_SPARK_PATH || process.env.BACKUP_SPARK_PATH || '/var/www/siroum/spark';
    const commandPrefix = (process.env.BACKUP_COMMAND_PREFIX || process.env.GIT_DEPLOY_COMMAND_PREFIX || 'php').replace(/^["']|["']$/g, '').trim();

    let fullCommand = `${commandPrefix} ${sparkPath} zoom:scan-and-queue-drive`;
    if (from_date) fullCommand += ` --from ${from_date}`;
    if (to_date) fullCommand += ` --to ${to_date}`;
    if (account_id) fullCommand += ` --account_id ${account_id}`;
    if (user_scope) fullCommand += ` --user_scope ${user_scope}`;
    if (include_local !== undefined) fullCommand += ` --include_local ${include_local ? '1' : '0'}`;

    console.log('[ZoomScanService] Running Cloud Scan:', fullCommand);

    return this._executeCommand(fullCommand, 'zoom_scan_cloud_' + Date.now());
  }

  async runRescanDrive(payload) {
    const { from_date, to_date, account_id } = payload || {};
    const sparkPath = process.env.SIROUM_SPARK_PATH || process.env.BACKUP_SPARK_PATH || '/var/www/siroum/spark';
    const commandPrefix = (process.env.BACKUP_COMMAND_PREFIX || process.env.GIT_DEPLOY_COMMAND_PREFIX || 'php').replace(/^["']|["']$/g, '').trim();

    let fullCommand = `${commandPrefix} ${sparkPath} zoom:rescan-drive`;
    if (from_date) fullCommand += ` --from ${from_date}`;
    if (to_date) fullCommand += ` --to ${to_date}`;
    if (account_id) fullCommand += ` --account_id ${account_id}`;

    console.log('[ZoomScanService] Running Rescan Drive:', fullCommand);

    return this._executeCommand(fullCommand, 'zoom_rescan_drive_' + Date.now());
  }

  async runScanTrash(payload) {
    const { from_date, to_date, account_id, user_scope } = payload || {};
    const sparkPath = process.env.SIROUM_SPARK_PATH || process.env.BACKUP_SPARK_PATH || '/var/www/siroum/spark';
    const commandPrefix = (process.env.BACKUP_COMMAND_PREFIX || process.env.GIT_DEPLOY_COMMAND_PREFIX || 'php').replace(/^["']|["']$/g, '').trim();

    let fullCommand = `${commandPrefix} ${sparkPath} zoom:scan-trash`;
    if (from_date) fullCommand += ` --from ${from_date}`;
    if (to_date) fullCommand += ` --to ${to_date}`;
    if (account_id) fullCommand += ` --account_id ${account_id}`;
    if (user_scope) fullCommand += ` --user_scope ${user_scope}`;

    console.log('[ZoomScanService] Running Trash Scan:', fullCommand);

    return this._executeCommand(fullCommand, 'zoom_scan_trash_' + Date.now());
  }

  _executeCommand(fullCommand, jobId) {
    let stdoutChunks = [];
    let stderrChunks = [];

    const childProcess = spawn(fullCommand, [], {
      stdio: 'pipe',
      shell: true
    });

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
          reject(new Error('Process exited with code ' + code + '. Output: ' + stdoutChunks.join('') + ' Error: ' + stderrChunks.join('')));
        }
      });

      childProcess.on('error', (error) => {
        delete this.processes[jobId];
        reject(error);
      });
    });
  }
}

module.exports = ZoomScanService;