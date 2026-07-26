require('dotenv').config();

const { spawn } = require('child_process');

class GitDeployService {
  constructor(redis) {
    this.redis = redis;
    this.processes = {};
  }

  async init() {
    console.log('[GitDeployService] Initialized GitDeployService');
    return true;
  }

  async runGitDeploy(payload) {
    const { branch } = payload;
    const sparkPath = process.env.SIROUM_SPARK_PATH || '/var/www/siroum/spark';

    if (!branch) {
      throw new Error('Branch name is required for git deployment job.');
    }

    console.log(`[GitDeployService] Triggering deployment for branch "${branch}" via spark CLI...`);

    let stdoutChunks = [];
    let stderrChunks = [];

    const childProcess = spawn('php', [sparkPath, 'git-deploy:worker'], {
      stdio: 'pipe',
      shell: true,
      env: {
        ...process.env,
        GIT_TERMINAL_PROMPT: '0',
        GIT_CONFIG_PARAMETERS: "'safe.directory=*'"
      }
    });

    const jobId = `git_deploy_${branch}_${Date.now()}`;
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
        const output = stdoutChunks.join('');
        const error = stderrChunks.join('');

        console.log(`[GitDeployService] Deployment execution finished with exit code: ${code}`);

        if (code === 0) {
          resolve({
            exitCode: code,
            output,
            error
          });
        } else {
          reject(new Error(`Git deployment failed with exit code ${code}. Error: ${error || output}`));
        }
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
        this.processes[id].kill();
      }
    }
  }
}

module.exports = GitDeployService;