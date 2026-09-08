require('dotenv').config();

const { spawn } = require('child_process');
const logger = require('./logger');

class LiteLlmService {
  constructor() {
    this.processes = {};
  }

  async init() {
    logger.info('[LiteLlmService] Initialized LiteLlmService');
    return true;
  }

  resolveCommand(payload) {
    const action = payload && payload.action ? payload.action : null;

    switch (action) {
      case 'dpp_deep_analysis': {
        const jobUuid = payload.job_uuid;
        if (!jobUuid) {
          throw new Error('payload.job_uuid is required for dpp_deep_analysis job.');
        }
        return { command: 'dpp:ai-deep-analysis-step', args: [jobUuid], identifier: jobUuid };
      }
      default:
        throw new Error(`Unknown lite_llm action: ${action === null ? '(missing)' : action}`);
    }
  }

  async runJob(payload) {
    const { command, args: commandExtraArgs, identifier } = this.resolveCommand(payload);

    const sparkPath = process.env.LITELLM_SPARK_PATH || process.env.SIROUM_SPARK_PATH || '/var/www/siroum/spark';
    const commandPrefix = process.env.LITELLM_COMMAND_PREFIX || process.env.GIT_DEPLOY_COMMAND_PREFIX || '';

    let commandScript;
    let commandArgs = [];

    if (commandPrefix.trim()) {
      const prefixParts = commandPrefix.trim().split(/\s+/);
      commandScript = prefixParts[0];
      commandArgs = prefixParts.slice(1);
      commandArgs.push(sparkPath);
    } else {
      commandScript = 'php';
      commandArgs = [sparkPath];
    }

    commandArgs.push(command, ...commandExtraArgs);

    logger.info(`[LiteLlmService] Running: ${commandScript} ${commandArgs.join(' ')}`);

    const stdoutChunks = [];
    const stderrChunks = [];

    const childProcess = spawn(commandScript, commandArgs, {
      stdio: 'pipe',
      shell: true
    });

    const processKey = `lite_llm_${identifier}_${Date.now()}`;
    this.processes[processKey] = childProcess;

    childProcess.stdout.on('data', (chunk) => {
      const line = chunk.toString().trim();
      if (line) {
        logger.debug(`[LiteLlmService] stdout: ${line}`);
      }
      stdoutChunks.push(chunk.toString());
    });

    childProcess.stderr.on('data', (chunk) => {
      const line = chunk.toString().trim();
      if (line) {
        logger.warn(`[LiteLlmService] stderr: ${line}`);
      }
      stderrChunks.push(chunk.toString());
    });

    return new Promise((resolve, reject) => {
      childProcess.on('close', (code) => {
        delete this.processes[processKey];
        const output = stdoutChunks.join('');
        const error = stderrChunks.join('');

        logger.info(`[LiteLlmService] Finished "${identifier}" with exit code: ${code}`);

        resolve({
          exitCode: code,
          output,
          error
        });
      });

      childProcess.on('error', (error) => {
        delete this.processes[processKey];
        reject(error);
      });
    });
  }

  async shutdown() {
    for (const id of Object.keys(this.processes)) {
      if (this.processes[id]) {
        logger.info(`[LiteLlmService] Killing process: ${id}`);
        this.processes[id].kill();
      }
    }
  }
}

module.exports = LiteLlmService;