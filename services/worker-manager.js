const { spawn } = require('child_process');
const path = require('path');
const config = require('../config');

class WorkerManager {
  constructor(db, workerModel) {
    this.db = db;
    this.workerModel = workerModel;
    this.workers = {};
    this.restartState = {};
    this.restartTimers = {};
  }

  async init() {
    const dbWorkers = await this.workerModel.getAll();

    for (const worker of dbWorkers) {
      await this.startWorker(worker.id, worker.type);
    }

    for (const type of Object.values(config.jobTypes)) {
      const typeWorkers = dbWorkers.filter(w => w.type === type);
      if (typeWorkers.length === 0) {
        console.log(`No worker found for type "${type}", creating default worker...`);
        await this.createWorker(type);
      }
    }
  }

  async startWorker(id, type) {
    if (this.restartTimers[id]) {
      clearTimeout(this.restartTimers[id]);
      delete this.restartTimers[id];
    }

    const workerScript = type === config.jobTypes.RETRY ? '../retry-worker.js' : '../worker.js';
    const workerArgs = type === config.jobTypes.RETRY ? [] : ['--id', id, '--type', type];

    const workerProcess = spawn('node', [
      path.join(__dirname, workerScript),
      ...workerArgs
    ], {
      stdio: 'pipe',
      detached: false
    });

    this.workers[id] = {
      process: workerProcess,
      type,
      id,
      startedAt: Date.now(),
    };

    workerProcess.stdout.on('data', (data) => {
      console.log(`Worker ${id} (${type}): ${data.toString().trim()}`);
    });

    workerProcess.stderr.on('data', (data) => {
      console.error(`Worker ${id} (${type}) ERROR: ${data.toString().trim()}`);
    });

    workerProcess.on('exit', async (code) => {
      const record = this.workers[id];
      const runtimeMs = record && record.startedAt ? (Date.now() - record.startedAt) : 0;
      console.log(`Worker ${id} (${type}) exited with code ${code}`);
      delete this.workers[id];

      if (code !== 0) {
        this.scheduleRestart(id, type, runtimeMs);
      } else {
        delete this.restartState[id];
      }
    });

    return id;
  }

  async createWorker(type) {
    const id = await this.workerModel.create(type);
    await this.startWorker(id, type);
    return id;
  }

  async stopWorker(id) {
    if (this.workers[id]) {
      this.workers[id].process.kill();
      delete this.workers[id];
      if (this.restartTimers[id]) {
        clearTimeout(this.restartTimers[id]);
        delete this.restartTimers[id];
      }
      return true;
    }
    return false;
  }

  async scaleWorkers(type, count) {
    const currentWorkers = await this.workerModel.getByType(type);
    
    if (currentWorkers.length < count) {
      for (let i = currentWorkers.length; i < count; i++) {
        await this.createWorker(type);
      }
    } else if (currentWorkers.length > count) {
      const workersToRemove = currentWorkers.slice(0, currentWorkers.length - count);
      for (const worker of workersToRemove) {
        await this.stopWorker(worker.id);
      }
    }
  }

  async shutdown() {
    const workerIds = Object.keys(this.workers);
    for (const id of workerIds) {
      await this.stopWorker(id);
    }
  }

  scheduleRestart(id, type, lastRuntimeMs = 0) {
    const resetThresholdMs = 60_000;
    let state = this.restartState[id] || { failures: 0, delayMs: 1000 };

    if (lastRuntimeMs > resetThresholdMs) {
      state = { failures: 0, delayMs: 1000 };
    }

    state.failures += 1;
    state.delayMs = Math.min(state.failures === 1 ? 1000 : state.delayMs * 2, 30_000);
    this.restartState[id] = state;

    const delay = state.delayMs;
    console.log(`Restarting worker ${id} (${type}) in ${delay}ms (failures=${state.failures})...`);

    if (this.restartTimers[id]) {
      clearTimeout(this.restartTimers[id]);
    }

    this.restartTimers[id] = setTimeout(async () => {
      try {
        await this.startWorker(id, type);
      } catch (e) {
        console.error(`Failed to restart worker ${id} (${type}): ${e.message}`);
        this.scheduleRestart(id, type, 0);
      }
    }, delay);
  }
}

module.exports = WorkerManager;