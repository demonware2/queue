require('dotenv').config();

const { spawn } = require('child_process');

class GeneralService {
  constructor(redis = null) {
    this.redis = redis;
    this.processes = {};
  }

  async init() {
    console.log('[GeneralService] Initialized GeneralService');
    return true;
  }

  /**
   * Executes a general whitelisted command (e.g. spark command).
   */
  async runCommand(payload) {
    const sparkPath = process.env.SIROUM_SPARK_PATH || '/var/www/siroum/spark';
    const phpBinary = process.env.PHP_BINARY_PATH || 'php';

    const execution = payload.execution || {};
    const command = execution.command || payload.command;
    const args = execution.args || payload.args || [];

    if (!command) {
      throw new Error('Command is required for general queue execution.');
    }

    const finalArgs = [sparkPath];
    if (Array.isArray(args) && args.length > 0) {
      if (args[0] === command) {
        finalArgs.push(...args);
      } else {
        finalArgs.push(command, ...args);
      }
    } else {
      finalArgs.push(command);
    }

    const maxTimeout = (execution.max_timeout || payload.max_timeout || 300) * 1000;
    const jobId = `general_${Date.now()}`;

    console.log(`[GeneralService] Running spark command: ${phpBinary} ${finalArgs.join(' ')}`);

    let stdoutChunks = [];
    let stderrChunks = [];

    return new Promise((resolve, reject) => {
      let isTimedOut = false;
      const childProcess = spawn(phpBinary, finalArgs, {
        stdio: 'pipe',
        shell: false,
        env: {
          ...process.env,
          NODE_ENV: 'production'
        }
      });

      this.processes[jobId] = childProcess;

      const timer = setTimeout(() => {
        isTimedOut = true;
        try {
          childProcess.kill('SIGTERM');
        } catch (e) {
          // ignore
        }
      }, maxTimeout);

      childProcess.stdout.on('data', (chunk) => {
        stdoutChunks.push(chunk.toString());
      });

      childProcess.stderr.on('data', (chunk) => {
        stderrChunks.push(chunk.toString());
      });

      childProcess.on('close', (code) => {
        clearTimeout(timer);
        delete this.processes[jobId];

        const output = stdoutChunks.join('');
        const error = stderrChunks.join('');

        if (isTimedOut) {
          return reject(new Error(`Command timed out after ${maxTimeout / 1000}s. Output:\n${output}\nErrors:\n${error}`));
        }

        console.log(`[GeneralService] Command finished with exit code: ${code}`);

        if (code === 0) {
          resolve({
            exitCode: code,
            output,
            error
          });
        } else {
          const err = new Error(`Command failed with exit code ${code}. Error: ${error}\nOutput: ${output}`);
          err.exitCode = code;
          err.output = output;
          err.error = error;
          reject(err);
        }
      });

      childProcess.on('error', (err) => {
        clearTimeout(timer);
        delete this.processes[jobId];
        reject(err);
      });
    });
  }
}

module.exports = GeneralService;