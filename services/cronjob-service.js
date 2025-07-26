require('dotenv').config();

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const util = require('util');
const exec = util.promisify(require('child_process').exec);
const sqlite3 = require('sqlite3').verbose();

class CronjobService {
  constructor() {
    this.processes = {};
    this.taskProcesses = {};
    this.MAX_CPU_USAGE = 80;
    this.MAX_MEM_USAGE = 85;
    this.RESOURCE_CHECK_RETRIES = 5;
    this.RESOURCE_CHECK_INTERVAL = 30000;
  }

  async init() {
    try {
      console.log('Initializing CronjobService');
      this.ensureCronjobDirectory();
      this.ensureLogsDirectory();
      return true;
    } catch (error) {
      console.error('Failed to initialize CronjobService:', error.message);
      return false;
    }
  }

  ensureCronjobDirectory() {
    const cronjobDir = path.join(process.cwd(), 'cronjob');
    if (!fs.existsSync(cronjobDir)) {
      fs.mkdirSync(cronjobDir, { recursive: true });
      console.log('Created cronjob directory');
    }
  }

  ensureLogsDirectory() {
    const logsDir = path.join(process.cwd(), 'logs');
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
      console.log('Created logs directory');
    }
  }

  async checkSystemAvailability() {
    try {
      const { stdout: cpuInfo } = await exec("cat /proc/loadavg");
      const loadAvg = parseFloat(cpuInfo.split(' ')[0]);

      const { stdout: memInfo } = await exec("free -m");
      const lines = memInfo.trim().split('\n');
      const memLine = lines[1].trim().split(/\s+/);
      const totalMem = parseInt(memLine[1]);
      const usedMem = parseInt(memLine[2]);
      const memoryUsagePercent = (usedMem / totalMem) * 100;

      const { stdout: cpuCount } = await exec("nproc");
      const numCores = parseInt(cpuCount.trim());

      const cpuUsagePercent = (loadAvg / numCores) * 100;

      console.log(`System resources - CPU usage: ${cpuUsagePercent.toFixed(2)}%, Memory usage: ${memoryUsagePercent.toFixed(2)}%`);

      return {
        available: cpuUsagePercent < this.MAX_CPU_USAGE && memoryUsagePercent < this.MAX_MEM_USAGE,
        stats: {
          cpuUsage: cpuUsagePercent.toFixed(2),
          memoryUsage: memoryUsagePercent.toFixed(2),
          maxCpuThreshold: this.MAX_CPU_USAGE,
          maxMemThreshold: this.MAX_MEM_USAGE
        }
      };
    } catch (error) {
      console.error('Error checking system resources:', error.message);
      return { available: true, stats: { error: error.message } };
    }
  }

  async waitForResources(taskId = null) {
    let retryCount = 0;

    while (retryCount < this.RESOURCE_CHECK_RETRIES) {
      const { available, stats } = await this.checkSystemAvailability();

      if (available) {
        console.log('System resources are available, proceeding with task');
        return true;
      }

      retryCount++;
      const waitMessage = `System resources are limited (CPU: ${stats.cpuUsage}%, Memory: ${stats.memoryUsage}%). ` +
        `Waiting (attempt ${retryCount}/${this.RESOURCE_CHECK_RETRIES})...`;

      console.log(waitMessage);

      if (taskId) {
        await this.updateTaskStatus(taskId, 'waiting', waitMessage);
      }

      await new Promise(resolve => setTimeout(resolve, this.RESOURCE_CHECK_INTERVAL));
    }

    console.log(`Max retries (${this.RESOURCE_CHECK_RETRIES}) reached waiting for system resources`);
    return false;
  }

  async runScript(jobPayload) {
    if (!jobPayload || !jobPayload.script) {
      throw new Error('Invalid job payload: script name is required');
    }

    const scriptName = jobPayload.script;
    const scriptArgs = jobPayload.args || [];
    const taskId = jobPayload.taskId || null;

    const scriptPath = this.getScriptPath(scriptName);

    if (!fs.existsSync(scriptPath)) {
      throw new Error(`Script not found: ${scriptPath}`);
    }

    if (taskId) {
      await this.updateTaskStatus(taskId, 'pending', 'Checking system resources');
    }

    const resourcesAvailable = await this.waitForResources(taskId);

    if (!resourcesAvailable) {
      const errorMsg = 'Insufficient system resources to run the task after multiple attempts';
      console.error(errorMsg);

      if (taskId) {
        await this.updateTaskStatus(taskId, 'failed', errorMsg);
      }

      throw new Error(errorMsg);
    }

    const isNodeScript = scriptPath.endsWith('.js');

    console.log(`Running ${isNodeScript ? 'Node.js' : 'system'} script: ${scriptPath}`);

    try {
      const jobId = `${scriptName}_${Date.now()}`;
      let result;

      if (taskId) {
        await this.updateTaskStatus(taskId, 'running', 'Task started');
      }

      if (isNodeScript) {
        result = await this.runNodeScript(jobId, scriptPath, [...scriptArgs, taskId ? `--task-id=${taskId}` : ''], taskId);
      } else {
        result = await this.runSystemCommand(jobId, scriptPath, scriptArgs, taskId);
      }

      if (taskId) {
        await this.updateTaskStatus(
          taskId,
          result.exitCode === 0 ? 'success' : 'failed',
          result.output + (result.error ? '\n\nErrors:\n' + result.error : '')
        );
      }

      return {
        jobId,
        scriptName,
        exitCode: result.exitCode,
        output: result.output,
        error: result.error
      };
    } catch (error) {
      console.error(`Error running script ${scriptPath}:`, error.message);

      if (taskId) {
        await this.updateTaskStatus(taskId, 'failed', `Error: ${error.message}`);
      }

      throw error;
    }
  }

  getScriptPath(scriptName) {
    if (scriptName.includes('/') || scriptName.includes('\\')) {
      return path.resolve(scriptName);
    }

    return path.join(process.cwd(), 'cronjob', scriptName);
  }

  async runNodeScript(jobId, scriptPath, args = [], taskId = null) {
    return new Promise((resolve, reject) => {
      const output = [];
      const errorOutput = [];

      console.log(`Starting Node.js script: node ${scriptPath} ${args.join(' ')}`);

      const childProcess = spawn('node', [scriptPath, ...args], {
        stdio: 'pipe',
        detached: false,
        env: {
          ...process.env,
          JOB_ID: jobId,
          TASK_ID: taskId
        }
      });

      this.processes[jobId] = childProcess;

      if (taskId) {
        this.taskProcesses[taskId] = childProcess;

        console.log(`Process started with PID ${childProcess.pid} for task ${taskId}`);
        this.updateTaskStatus(taskId, 'running', `Process started with PID ${childProcess.pid}`, childProcess.pid);
      }

      childProcess.stdout.on('data', (data) => {
        const text = data.toString().trim();
        console.log(`[${jobId}]: ${text}`);
        output.push(text);
      });

      childProcess.stderr.on('data', (data) => {
        const text = data.toString().trim();
        console.error(`[${jobId}] ERROR: ${text}`);
        errorOutput.push(text);
      });

      childProcess.on('close', (code) => {
        console.log(`Script ${jobId} exited with code ${code}`);
        delete this.processes[jobId];

        if (taskId && this.taskProcesses[taskId]) {
          delete this.taskProcesses[taskId];
        }

        resolve({
          exitCode: code,
          output: output.join('\n'),
          error: errorOutput.join('\n')
        });
      });

      childProcess.on('error', (err) => {
        console.error(`Failed to start script ${jobId}:`, err.message);
        delete this.processes[jobId];

        if (taskId && this.taskProcesses[taskId]) {
          delete this.taskProcesses[taskId];
        }

        reject(err);
      });
    });
  }

  async runSystemCommand(jobId, scriptPath, args = [], taskId = null) {
    try {
      console.log(`Running system command: ${scriptPath} ${args.join(' ')}`);

      let stdoutChunks = [];
      let stderrChunks = [];

      const childProcess = spawn(scriptPath, args, {
        stdio: 'pipe',
        shell: true
      });

      this.processes[jobId] = childProcess;

      if (taskId) {
        this.taskProcesses[taskId] = childProcess;
        console.log(`Process started with PID ${childProcess.pid} for task ${taskId}`);
        this.updateTaskStatus(taskId, 'running', `Process started with PID ${childProcess.pid}`, childProcess.pid);
      }

      childProcess.stdout.on('data', (chunk) => {
        stdoutChunks.push(chunk.toString());
      });

      childProcess.stderr.on('data', (chunk) => {
        stderrChunks.push(chunk.toString());
      });

      return new Promise((resolve, reject) => {
        childProcess.on('close', (code) => {
          delete this.processes[jobId];

          if (taskId && this.taskProcesses[taskId]) {
            delete this.taskProcesses[taskId];
          }

          resolve({
            exitCode: code,
            output: stdoutChunks.join(''),
            error: stderrChunks.join('')
          });
        });

        childProcess.on('error', (error) => {
          delete this.processes[jobId];

          if (taskId && this.taskProcesses[taskId]) {
            delete this.taskProcesses[taskId];
          }

          reject(error);
        });
      });
    } catch (error) {
      console.error(`System command ${jobId} failed:`, error.message);

      return {
        exitCode: error.code || 1,
        output: error.stdout || '',
        error: error.stderr || error.message
      };
    }
  }

  stopScript(jobId) {
    if (this.processes[jobId]) {
      console.log(`Stopping script with job ID: ${jobId}`);
      this.processes[jobId].kill();
      delete this.processes[jobId];
      return true;
    }
    return false;
  }

  async updateTaskStatus(taskId, status, output = '', pid = null) {
    if (!taskId) return;

    const dbPath = path.resolve(process.env.TASKSCHEDULER_DB_PATH);

    return new Promise((resolve, reject) => {
      const db = new sqlite3.Database(dbPath, (err) => {
        if (err) {
          console.error('Error connecting to SQLite database:', err.message);
          return reject(err);
        }

        if (pid === null && this.taskProcesses[taskId]) {
          pid = this.taskProcesses[taskId].pid;
          console.log(`Using stored PID ${pid} for task ${taskId}`);
        }

        const isRunning = status === 'running' ? 1 : 0;
        const startRunning = status === 'running' ? new Date().toISOString() : null;

        if (status === 'success' || status === 'failed') {
          console.log(`Updating task ${taskId} status to ${status} (completion)`);
          db.run(
            `UPDATE task_scheduler 
             SET is_running = 0, start_running = NULL, pid = NULL 
             WHERE id = ?`,
            [taskId],
            function (err) {
              if (err) {
                console.error('Error updating task_scheduler on completion:', err.message);
                db.close();
                return reject(err);
              }

              const endTime = new Date().toISOString();

              db.get(
                `SELECT id FROM task_scheduler_log 
                 WHERE task_id = ? AND status = 'running'
                 ORDER BY id DESC LIMIT 1`,
                [taskId],
                (err, row) => {
                  if (err) {
                    console.error('Error checking for log record:', err.message);
                    db.close();
                    return reject(err);
                  }

                  if (row) {
                    db.run(
                      `UPDATE task_scheduler_log 
                       SET end_time = ?, status = ?, output = ?, update_date = ?
                       WHERE id = ?`,
                      [endTime, status, output, endTime, row.id],
                      function (err) {
                        db.close();
                        if (err) {
                          console.error('Error updating log:', err.message);
                          return reject(err);
                        }
                        resolve(true);
                      }
                    );
                  } else {
                    const startTime = new Date(Date.now() - 1000).toISOString();

                    db.run(
                      `INSERT INTO task_scheduler_log
                       (task_id, start_time, end_time, status, output, create_date, update_date)
                       VALUES (?, ?, ?, ?, ?, ?, ?)`,
                      [taskId, startTime, endTime, status, output, startTime, endTime],
                      function (err) {
                        db.close();
                        if (err) {
                          console.error('Error inserting log:', err.message);
                          return reject(err);
                        }
                        resolve(true);
                      }
                    );
                  }
                }
              );
            }
          );
        } else {
          console.log(`Updating task ${taskId} status to ${status} with PID ${pid || 'null'}`);
          db.run(
            `UPDATE task_scheduler 
             SET is_running = ?, start_running = ?, pid = ? 
             WHERE id = ?`,
            [isRunning, startRunning, pid, taskId],
            function (err) {
              if (err) {
                console.error('Error updating task_scheduler for running status:', err.message);
                db.close();
                return reject(err);
              }

              const startTime = new Date().toISOString();

              db.get(
                `SELECT id FROM task_scheduler_log 
                 WHERE task_id = ? AND status = 'running'
                 ORDER BY id DESC LIMIT 1`,
                [taskId],
                (err, row) => {
                  if (err) {
                    console.error('Error checking for log record:', err.message);
                    db.close();
                    return reject(err);
                  }

                  if (row) {
                    db.run(
                      `UPDATE task_scheduler_log 
                       SET output = ?, update_date = ?
                       WHERE id = ?`,
                      [output, startTime, row.id],
                      function (err) {
                        db.close();
                        if (err) {
                          console.error('Error updating log:', err.message);
                          return reject(err);
                        }
                        resolve(true);
                      }
                    );
                  } else {
                    db.run(
                      `INSERT INTO task_scheduler_log
                       (task_id, start_time, status, output, create_date, update_date)
                       VALUES (?, ?, ?, ?, ?, ?)`,
                      [taskId, startTime, 'running', output, startTime, startTime],
                      function (err) {
                        db.close();
                        if (err) {
                          console.error('Error inserting log:', err.message);
                          return reject(err);
                        }
                        resolve(true);
                      }
                    );
                  }
                }
              );
            }
          );
        }
      });
    });
  }

  async shutdown() {
    console.log('Shutting down CronjobService');

    const activeJobs = Object.keys(this.processes);
    if (activeJobs.length > 0) {
      console.log(`Stopping ${activeJobs.length} active cronjob processes...`);

      for (const jobId of activeJobs) {
        this.stopScript(jobId);
      }
    }
  }
}

module.exports = CronjobService;