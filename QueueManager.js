const ConnectionManager = require('./ConnectionManager');
const BatchManager = require('./BatchManager');
const JobProcessor = require('./JobProcessor');
const Monitoring = require('./Monitoring');
const { generateId, chunkArray } = require('./utils');

/**
 * Main queue manager class
 */
class QueueManager {
  /**
   * Create a queue manager
   * @param {string} connectionString - MongoDB connection string
   * @param {string} dbName - Database name
   * @param {Object} options - Configuration options
   */
  constructor(connectionString, dbName, options = {}) {
    // Initialize components
    this.connectionManager = new ConnectionManager(connectionString, dbName, options);
    this.batchManager = new BatchManager(options);
    this.jobProcessor = null; // Will initialize after connection
    
    this.monitoring = new Monitoring((memoryPercent) => {
      this._handleMemoryPressure(memoryPercent);
    });
    
    // Start monitoring
    this.monitoring.startMonitoring(options.monitorInterval || 5000);
  }

  /**
   * Connect to MongoDB
   * @returns {Promise<Object>} MongoDB database instance
   */
  async connect() {
    const db = await this.connectionManager.connect();
    
    // Initialize job processor after connection
    if (!this.jobProcessor) {
      this.jobProcessor = new JobProcessor(db);
    }
    
    return db;
  }

  /**
   * Close connections and clean up
   * @returns {Promise<void>}
   */
  async close() {
    // Stop all processors
    if (this.jobProcessor) {
      Object.keys(this.jobProcessor.processors || {}).forEach(jobType => {
        this.stopProcessing(jobType);
      });
    }
    
    // Flush all pending batches
    await this._flushAllBatches(true);
    
    // Stop monitoring
    this.monitoring.stopMonitoring();
    
    // Close MongoDB connection
    await this.connectionManager.close();
  }

  /**
   * Add a job to the queue
   * @param {string} jobType - Type of job
   * @param {Object} data - Job data
   * @param {Object} options - Job options
   * @returns {Promise<string>} Job ID
   */
  async addJob(jobType, data, options = {}) {
    const priority = options.priority || 0;
    const jobId = generateId();
    
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
    
    // Add to batch manager
    const shouldFlush = this.batchManager.addJob(jobType, job);
    this.monitoring.updateStats({ totalAdded: this.monitoring.stats.totalAdded + 1 });
    
    // Set up batch timer if not already set
    this.batchManager.resetBatchTimer(jobType, (jType) => this._flushBatch(jType));
    
    // Flush if batch is full or memory pressure
    if (shouldFlush) {
      // Use setTimeout to not block the current call
      setTimeout(() => this._flushBatch(jobType), 0);
    }
    
    return jobId;
  }

  /**
   * Start processing jobs
   * @param {string} jobType - Type of job
   * @param {Function} processor - Job processor function
   * @param {Object} options - Processing options
   */
  startProcessing(jobType, processor, options = {}) {
    if (!this.jobProcessor) {
      throw new Error('Must connect to database before starting processing');
    }
    
    this.jobProcessor.startProcessing(jobType, processor, options);
  }

  /**
   * Stop processing jobs
   * @param {string} jobType - Type of job
   */
  stopProcessing(jobType) {
    if (this.jobProcessor) {
      this.jobProcessor.stopProcessing(jobType);
    }
  }

  /**
   * Get job counts by status
   * @param {string} jobType - Type of job
   * @returns {Promise<Object>} Job counts
   */
  async getJobCounts(jobType) {
    await this.connect();
    const db = this.connectionManager.db;
    
    const pendingInMemory = this.batchManager.pendingBatches[jobType]?.length || 0;
    
    const pipeline = [{ $match: { jobType } }, { $group: { _id: '$status', count: { $sum: 1 } } }];
    const results = await db.collection('jobs').aggregate(pipeline).toArray();
    const failedResults = await db.collection('failed_jobs').aggregate(pipeline).toArray();
    const completedResults = await db.collection('completed_jobs').aggregate(pipeline).toArray();

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

  /**
   * Get queue statistics
   * @returns {Object} Queue statistics
   */
  getStats() {
    const monitoringStats = this.monitoring.getStats();
    const batchStats = this.batchManager.getStats();
    
    const stats = {
      ...monitoringStats,
      pendingJobsByType: {},
      batchStats
    };
    
    // Add pending counts for each job type
    for (const jobType in this.batchManager.pendingBatches) {
      stats.pendingJobsByType[jobType] = this.batchManager.pendingBatches[jobType].length;
    }
    
    // Add processor stats if available
    if (this.jobProcessor) {
      stats.processorStats = this.jobProcessor.stats;
    }
    
    return stats;
  }

  /**
   * Handle memory pressure by flushing batches
   * @param {number} memoryPercent - Current memory usage percentage
   * @private
   */
  _handleMemoryPressure(memoryPercent) {
    console.warn(`Flushing all batches due to memory pressure (${memoryPercent.toFixed(1)}%)`);
    this._flushAllBatches(true).catch(err => {
      console.error('Error flushing batches during memory pressure:', err);
    });
  }

  /**
   * Flush all pending batches
   * @param {boolean} force - Force flush even if batches are small
   * @private
   */
  async _flushAllBatches(force = false) {
    const jobTypes = Object.keys(this.batchManager.pendingBatches);
    const promises = jobTypes.map(jobType => {
      if (this.batchManager.pendingBatches[jobType].length > 0 || force) {
        return this._flushBatch(jobType, force);
      }
      return Promise.resolve();
    });
    
    await Promise.all(promises);
  }

  /**
   * Flush a batch of jobs to the database
   * @param {string} jobType - Type of job
   * @param {boolean} force - Force flush even if batch is small
   * @private
   */
  async _flushBatch(jobType, force = false) {
    // Get jobs from batch manager
    const batchToInsert = this.batchManager.getPendingJobs(jobType);
    
    if (batchToInsert.length === 0) {
      this.batchManager.resetBatchTimer(jobType, (jType) => this._flushBatch(jType));
      return;
    }
    
    // Update monitoring stats
    this.monitoring.updateStats({ lastFlushTime: Date.now() });
    
    // Create a new batch timer for subsequent jobs
    this.batchManager.resetBatchTimer(jobType, (jType) => this._flushBatch(jType));

    try {
      await this.connect();
      const db = this.connectionManager.db;
      
      const startTime = Date.now();
      
      if (batchToInsert.length > 0) {
        // Split into manageable chunks to avoid MongoDB document size limits
        const chunks = chunkArray(batchToInsert, 1000);
        for (const chunk of chunks) {
          await db.collection('jobs').insertMany(chunk, { ordered: false });
        }
        
        const insertTime = Date.now() - startTime;
        this.monitoring.updateStats({ lastDbWriteTime: insertTime });
        
        console.log(`📥 Inserted batch of ${batchToInsert.length} "${jobType}" jobs in ${insertTime}ms (${Math.round(batchToInsert.length / insertTime * 1000)} jobs/sec)`);
      }
    } catch (err) {
      console.error(`Error flushing batch for ${jobType}:`, err);
      
      // Handle flush error in batch manager
      this.batchManager.handleFlushError(
        jobType, 
        batchToInsert, 
        err, 
        (jType) => this._flushBatch(jType)
      );
    }
  }
}

module.exports = QueueManager;