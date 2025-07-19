const { spawn } = require('child_process');
const path = require('path');

class WorkerManager {
  constructor(db, workerModel) {
    this.db = db;
    this.workerModel = workerModel;
    this.workers = {};
  }

  async init() {
    const dbWorkers = await this.workerModel.getAll();
    
    for (const worker of dbWorkers) {
      await this.startWorker(worker.id, worker.type);
    }
  }

  async startWorker(id, type) {
    const workerProcess = spawn('node', [
      path.join(__dirname, '../worker.js'),
      '--id', id,
      '--type', type
    ], {
      stdio: 'pipe',
      detached: false
    });

    this.workers[id] = {
      process: workerProcess,
      type,
      id
    };

    workerProcess.stdout.on('data', (data) => {
      console.log(`Worker ${id} (${type}): ${data.toString().trim()}`);
    });

    workerProcess.stderr.on('data', (data) => {
      console.error(`Worker ${id} (${type}) ERROR: ${data.toString().trim()}`);
    });

    workerProcess.on('exit', async (code) => {
      console.log(`Worker ${id} (${type}) exited with code ${code}`);
      delete this.workers[id];
      
      if (code !== 0) {
        console.log(`Restarting worker ${id} (${type})...`);
        await this.startWorker(id, type);
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
}

module.exports = WorkerManager;
