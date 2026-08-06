require('dotenv').config();

const { spawn } = require('child_process');
const logger = require('./logger');

class WebCrawlService {
  constructor() {
    this.processes = {};
  }

  async init() {
    logger.info('[WebCrawlService] Initialized WebCrawlService');
    return true;
  }

  async runCrawl(payload) {
    const { uuid, batch_id } = payload;
    const identifier = uuid || batch_id;

    if (!identifier) {
      throw new Error('Either uuid or batch_id is required for web crawl job.');
    }

    const sparkPath = process.env.CRAWL_SPARK_PATH || '/var/www/html/biroumum/spark';
    const commandPrefix = process.env.CRAWL_COMMAND_PREFIX || '';

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

    commandArgs.push('scraper:run', identifier);

    logger.info(`[WebCrawlService] Running crawl command: ${commandScript} ${commandArgs.join(' ')}`);

    let stdoutChunks = [];
    let stderrChunks = [];

    const childProcess = spawn(commandScript, commandArgs, {
      stdio: 'pipe',
      shell: true
    });

    const jobId = `web_crawl_${identifier}_${Date.now()}`;
    this.processes[jobId] = childProcess;

    childProcess.stdout.on('data', (chunk) => {
      const line = chunk.toString().trim();
      if (line) {
        logger.debug(`[WebCrawlService] stdout: ${line}`);
      }
      stdoutChunks.push(chunk.toString());
    });

    childProcess.stderr.on('data', (chunk) => {
      const line = chunk.toString().trim();
      if (line) {
        logger.warn(`[WebCrawlService] stderr: ${line}`);
      }
      stderrChunks.push(chunk.toString());
    });

    return new Promise((resolve, reject) => {
      childProcess.on('close', (code) => {
        delete this.processes[jobId];
        const output = stdoutChunks.join('');
        const error = stderrChunks.join('');

        logger.info(`[WebCrawlService] Crawl execution finished for "${identifier}" with exit code: ${code}`);

        resolve({
          exitCode: code,
          output,
          error
        });
      });

      childProcess.on('error', (error) => {
        delete this.processes[jobId];
        reject(error);
      });
    });
  }

  async shutdown() {
    for (const id of Object.keys(this.processes)) {
      if (this.processes[id]) {
        logger.info(`[WebCrawlService] Killing process: ${id}`);
        this.processes[id].kill();
      }
    }
  }
}

module.exports = WebCrawlService;