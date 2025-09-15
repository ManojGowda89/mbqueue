const { MongoClient } = require('mongodb');
const os = require('os');

class QueueManager {
  constructor(connectionString, dbName, options = {}) {
    this.connectionString = connectionString;
    this.dbName = dbName;
    this.client = null;
    this.db = null;
    
    // Batch configuration
    this.batchSize = options.batchSize || 1000;
    this.batchTimeout = options.batchTimeout || 5000;
    this.maxPendingJobs = options.maxPendingJobs || 100000; // Memory protection
    
    // Queue memory storage
    this.pendingBatches = {};
    this.batchTimers = {};
    this.batchStats = {};
    
    // Processing settings
    this.processors = {};
    this.isProcessing = {};
    this.jobOptions = {};
    
    // Monitoring
    this.stats = {
      totalAdded: 0,
      totalProcessed: 0,
      lastFlushTime: null,
      peakMemoryUsage: 0,
      lastDbWriteTime: 0
    };
    
    // Start monitoring memory usage
    this.memoryMonitorInterval = setInterval(() => this._monitorMemory(), 5000);
  }

  // Connect to MongoDB
  async connect() {
    if (!this.client) {
      this.client = new MongoClient(this.connectionString, {
        maxPoolSize: 20, // Increase connection pool for high concurrency
        socketTimeoutMS: 30000,
        connectTimeoutMS: 30000,
        writeConcern: { w: 1 } // Use write concern for better performance
      });
      await this.client.connect();
      this.db = this.client.db(this.dbName);
      console.log('✅ Connected to MongoDB');

      const collections = ['jobs', 'completed_jobs', 'failed_jobs'];
      for (const col of collections) {
        await this.db.collection(col).createIndex({ jobType: 1, priority: -1, createdAt: 1 });
        await this.db.collection(col).createIndex({ status: 1 });
      }
    }
    return this.db;
  }

  async close() {
    // Stop all processors
    Object.keys(this.processors).forEach(jobType => {
      this.stopProcessing(jobType);
    });
    
    // Flush all pending batches
    for (const jobType in this.pendingBatches) {
      if (this.pendingBatches[jobType].length > 0) {
        await this._flushBatch(jobType, true); // Force flush
      }
    }
    
    // Stop memory monitoring
    if (this.memoryMonitorInterval) {
      clearInterval(this.memoryMonitorInterval);
    }
    
    // Close MongoDB connection
    if (this.client) {
      await this.client.close();
      this.client = null;
      this.db = null;
      console.log('Disconnected from MongoDB');
    }
  }

  // Monitor system memory usage and initiate emergency flushes if needed
  _monitorMemory() {
    const memUsage = process.memoryUsage();
    const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
    this.stats.peakMemoryUsage = Math.max(this.stats.peakMemoryUsage, heapUsedMB);
    
    const freeMemMB = Math.round(os.freemem() / 1024 / 1024);
    const totalMemMB = Math.round(os.totalmem() / 1024 / 1024);
    const memUsagePercent = 100 - (freeMemMB / totalMemMB * 100);
    
    // If memory usage is above 80%, flush all pending batches
    if (memUsagePercent > 80) {
      console.warn(`⚠️ High memory usage (${memUsagePercent.toFixed(1)}%), flushing all batches`);
      for (const jobType in this.pendingBatches) {
        if (this.pendingBatches[jobType].length > 0) {
          this._flushBatch(jobType, true).catch(err => {
            console.error(`Failed to flush batch for ${jobType} during memory pressure:`, err);
          });
        }
      }
    }
    
    // Log memory stats occasionally
    if (Math.random() < 0.1) { // ~10% chance to log
      console.log(`Memory usage: ${heapUsedMB}MB (heap) | System: ${memUsagePercent.toFixed(1)}% | Peak: ${this.stats.peakMemoryUsage}MB`);
    }
  }

  // Add job to queue with improved batching
  async addJob(jobType, data, options = {}) {
    // Initialize batch structures if needed
    if (!this.pendingBatches[jobType]) {
      this.pendingBatches[jobType] = [];
      this.batchStats[jobType] = { added: 0, flushed: 0, lastFlushTime: Date.now() };
      // Set up timeout for this job type
      this._resetBatchTimer(jobType);
    }
    
    const priority = options.priority || 0;
    const jobId = this._generateId();
    
    // Create job object
    const job = {
      _id: jobId,
      jobType,
      data,
      priority,
      status: 'pending',
      createdAt: new Date(),
      attempts: 0,
      maxAttempts: options.maxAttempts || 3
    };
    
    // Add to pending batch
    this.pendingBatches[jobType].push(job);
    this.batchStats[jobType].added++;
    this.stats.totalAdded++;
    
    // Check memory pressure
    const totalPendingJobs = Object.values(this.pendingBatches)
      .reduce((sum, jobs) => sum + jobs.length, 0);
    
    // Flush if batch is full or overall memory pressure
    if (this.pendingBatches[jobType].length >= this.batchSize || 
        totalPendingJobs >= this.maxPendingJobs) {
      // Use setTimeout to not block the current call
      setTimeout(() => this._flushBatch(jobType), 0);
    }
    
    return jobId;
  }

  // Reset batch timer for a job type
  _resetBatchTimer(jobType) {
    if (this.batchTimers[jobType]) {
      clearTimeout(this.batchTimers[jobType]);
    }
    
    this.batchTimers[jobType] = setTimeout(() => {
      this._flushBatch(jobType).catch(err => {
        console.error(`Error in timed batch flush for ${jobType}:`, err);
        // Reschedule the timer even on error
        this._resetBatchTimer(jobType);
      });
    }, this.batchTimeout);
  }

  // Start processing jobs (continuous)
  startProcessing(jobType, processor, options = {}) {
    this.jobOptions[jobType] = {
      batchSize: options.batchSize || 100,
      pollInterval: options.pollInterval || 1000,
      maxConcurrent: options.maxConcurrent || 1,
      backoffFactor: options.backoffFactor || 1.5,
      maxBackoffInterval: options.maxBackoffInterval || 30000
    };
    
    this.processors[jobType] = processor;
    this.isProcessing[jobType] = true;
    
    // Start processing loops (up to maxConcurrent)
    for (let i = 0; i < this.jobOptions[jobType].maxConcurrent; i++) {
      this._processLoop(jobType, i);
    }
    
    console.log(`🚀 Started processing "${jobType}" jobs (batch size: ${this.jobOptions[jobType].batchSize}, concurrency: ${this.jobOptions[jobType].maxConcurrent})`);
  }

  // Stop processing jobs
  stopProcessing(jobType) {
    this.isProcessing[jobType] = false;
    console.log(`⏹️ Stopped processing "${jobType}" jobs`);
  }

  // Get job counts by status
  async getJobCounts(jobType) {
    await this.connect();
    
    const pendingInMemory = this.pendingBatches[jobType]?.length || 0;
    
    const pipeline = [{ $match: { jobType } }, { $group: { _id: '$status', count: { $sum: 1 } } }];
    const results = await this.db.collection('jobs').aggregate(pipeline).toArray();
    const failedResults = await this.db.collection('failed_jobs').aggregate(pipeline).toArray();
    const completedResults = await this.db.collection('completed_jobs').aggregate(pipeline).toArray();

    const counts = { 
      pending: pendingInMemory, // Include in-memory jobs in the count
      processing: 0, 
      completed: 0, 
      failed: 0 
    };
    
    results.forEach(item => {
      if (item._id === 'pending') {
        counts.pending += item.count; // Add DB pending to in-memory pending
      } else {
        counts[item._id] = item.count;
      }
    });
    
    failedResults.forEach(item => counts.failed += item.count);
    completedResults.forEach(item => counts.completed += item.count);

    return counts;
  }

  // Get queue statistics
  getStats() {
    const now = Date.now();
    const stats = {
      ...this.stats,
      pendingJobsByType: {},
      memoryUsageMB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      uptime: process.uptime(),
      batchStats: this.batchStats
    };
    
    // Add pending counts for each job type
    for (const jobType in this.pendingBatches) {
      stats.pendingJobsByType[jobType] = this.pendingBatches[jobType].length;
    }
    
    return stats;
  }

  // Main processing loop with adaptive polling and backoff
  async _processLoop(jobType, workerId) {
    if (!this.isProcessing[jobType]) return;
    
    let currentBackoff = this.jobOptions[jobType].pollInterval;
    let emptyBatchCount = 0;
    
    try {
      await this.connect();
      
      // Flush any pending batch for this job type
      if (this.pendingBatches[jobType] && this.pendingBatches[jobType].length > 0) {
        await this._flushBatch(jobType);
      }

      // Get batch of jobs to process
      const batchSize = this.jobOptions[jobType].batchSize;
      const jobs = await this.db.collection('jobs')
        .find({ jobType, status: 'pending' })
        .sort({ priority: -1, createdAt: 1 })
        .limit(batchSize)
        .toArray();

      if (jobs.length > 0) {
        console.log(`📤 Worker ${workerId}: Processing batch of ${jobs.length} "${jobType}" jobs`);
        emptyBatchCount = 0; // Reset empty batch counter
        currentBackoff = this.jobOptions[jobType].pollInterval; // Reset backoff
        
        // Process each job in the batch
        for (const job of jobs) {
          if (!this.isProcessing[jobType]) break; // Stop if processing was disabled
          
          try {
            await this.db.collection('jobs').updateOne(
              { _id: job._id, status: 'pending' }, // Make sure job is still pending
              { $set: { status: 'processing', startedAt: new Date() } }
            );

            const result = await this.processors[jobType](job.data, job);
            await this._complete(jobType, job._id, result);
            this.stats.totalProcessed++;
          } catch (err) {
            console.error(`Error processing ${jobType} job ${job._id}:`, err);
            await this._error(jobType, job._id, err);
          }
        }
        
        // Continue processing immediately if we got a full batch
        if (jobs.length >= batchSize && this.isProcessing[jobType]) {
          setImmediate(() => this._processLoop(jobType, workerId));
          return;
        }
      } else {
        // No jobs found, apply backoff strategy
        emptyBatchCount++;
        
        if (emptyBatchCount > 3) {
          // Increase backoff time exponentially, up to max
          currentBackoff = Math.min(
            currentBackoff * this.jobOptions[jobType].backoffFactor,
            this.jobOptions[jobType].maxBackoffInterval
          );
        }
      }
      
      // Schedule next iteration with current backoff
      if (this.isProcessing[jobType]) {
        setTimeout(() => this._processLoop(jobType, workerId), currentBackoff);
      }
    } catch (err) {
      console.error(`Error in process loop for "${jobType}" worker ${workerId}:`, err);
      // On error, wait a bit and try again with increased backoff
      const errorBackoff = Math.min(currentBackoff * 2, this.jobOptions[jobType].maxBackoffInterval);
      
      if (this.isProcessing[jobType]) {
        setTimeout(() => this._processLoop(jobType, workerId), errorBackoff);
      }
    }
  }

  // Mark job complete
  async _complete(jobType, jobId, result) {
    const job = await this.db.collection('jobs').findOne({ _id: jobId });
    if (!job) throw new Error(`Job ${jobId} not found`);

    await this.db.collection('completed_jobs').insertOne({
      ...job,
      status: 'completed',
      completedAt: new Date(),
      result
    });

    await this.db.collection('jobs').deleteOne({ _id: jobId });
  }

  // Mark job failed
  async _error(jobType, jobId, error) {
    const job = await this.db.collection('jobs').findOne({ _id: jobId });
    if (!job) throw new Error(`Job ${jobId} not found`);

    const attempts = job.attempts + 1;

    if (attempts >= job.maxAttempts) {
      await this.db.collection('failed_jobs').insertOne({
        ...job,
        status: 'failed',
        failedAt: new Date(),
        error: error.message,
        stack: error.stack
      });
      await this.db.collection('jobs').deleteOne({ _id: jobId });
    } else {
      // Calculate exponential backoff for retries
      const backoffDelay = Math.pow(2, attempts - 1) * 1000; // 1s, 2s, 4s, 8s...
      const retryAt = new Date(Date.now() + backoffDelay);
      
      await this.db.collection('jobs').updateOne(
        { _id: jobId },
        { 
          $set: { 
            status: 'pending', 
            attempts, 
            lastError: error.message, 
            lastErrorAt: new Date(),
            retryAt
          } 
        }
      );
    }
  }

  // Flush batch to DB with improved error handling and retries
  async _flushBatch(jobType, force = false) {
    if (!this.pendingBatches[jobType] || this.pendingBatches[jobType].length === 0) {
      this._resetBatchTimer(jobType);
      return;
    }

    // Clear the batch timer
    if (this.batchTimers[jobType]) {
      clearTimeout(this.batchTimers[jobType]);
      this.batchTimers[jobType] = null;
    }

    // Take jobs from pending queue
    const batchToInsert = [...this.pendingBatches[jobType]];
    this.pendingBatches[jobType] = [];
    
    // Update stats
    this.batchStats[jobType].flushed += batchToInsert.length;
    this.batchStats[jobType].lastFlushTime = Date.now();
    this.stats.lastFlushTime = Date.now();

    // Create a new batch timer for subsequent jobs
    this._resetBatchTimer(jobType);

    try {
      await this.connect();
      
      const startTime = Date.now();
      
      if (batchToInsert.length > 0) {
        // Split into manageable chunks to avoid MongoDB document size limits
        const chunkSize = 1000; // MongoDB has 16MB limit per batch
        for (let i = 0; i < batchToInsert.length; i += chunkSize) {
          const chunk = batchToInsert.slice(i, i + chunkSize);
          await this.db.collection('jobs').insertMany(chunk, { ordered: false });
        }
        
        const insertTime = Date.now() - startTime;
        this.stats.lastDbWriteTime = insertTime;
        
        console.log(`📥 Inserted batch of ${batchToInsert.length} "${jobType}" jobs in ${insertTime}ms (${Math.round(batchToInsert.length / insertTime * 1000)} jobs/sec)`);
      }
    } catch (err) {
      console.error(`Error flushing batch for ${jobType}:`, err);
      
      // Put the failed batch back in the queue
      this.pendingBatches[jobType] = [...batchToInsert, ...this.pendingBatches[jobType]];
      
      // Try again with exponential backoff
      const retryDelay = Math.min(5000 * Math.pow(1.5, Math.min(this.batchStats[jobType].errors || 0, 5)), 60000);
      this.batchStats[jobType].errors = (this.batchStats[jobType].errors || 0) + 1;
      this.batchStats[jobType].lastError = err.message;
      this.batchStats[jobType].lastErrorTime = Date.now();
      
      console.log(`Will retry flushing ${jobType} batch in ${Math.round(retryDelay/1000)}s`);
      
      // Override the timer with a retry
      if (this.batchTimers[jobType]) {
        clearTimeout(this.batchTimers[jobType]);
      }
      this.batchTimers[jobType] = setTimeout(() => this._flushBatch(jobType), retryDelay);
    }
  }

  _generateId() {
    return Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
  }
}

module.exports = QueueManager;