/**
 * Manages job batching for efficient database operations
 */
class BatchManager {
  /**
   * Create a batch manager
   * @param {Object} options - Batch configuration options
   */
  constructor(options = {}) {
    this.batchSize = options.batchSize || 1000;
    this.batchTimeout = options.batchTimeout || 5000;
    this.maxPendingJobs = options.maxPendingJobs || 100000;
    
    this.pendingBatches = {};
    this.batchTimers = {};
    this.batchStats = {};
  }

  /**
   * Add a job to a batch
   * @param {string} jobType - Type of job
   * @param {Object} job - Job data
   * @returns {boolean} True if batch is ready to flush
   */
  addJob(jobType, job) {
    // Initialize batch structures if needed
    if (!this.pendingBatches[jobType]) {
      this.pendingBatches[jobType] = [];
      this.batchStats[jobType] = { 
        added: 0, 
        flushed: 0, 
        lastFlushTime: Date.now(),
        errors: 0
      };
    }
    
    // Add to pending batch
    this.pendingBatches[jobType].push(job);
    this.batchStats[jobType].added++;
    
    // Check if batch should be flushed
    const totalPendingJobs = Object.values(this.pendingBatches)
      .reduce((sum, jobs) => sum + jobs.length, 0);
    
    return this.pendingBatches[jobType].length >= this.batchSize || 
           totalPendingJobs >= this.maxPendingJobs;
  }

  /**
   * Reset batch timer for a job type
   * @param {string} jobType - Type of job
   * @param {Function} flushCallback - Function to call when timer expires
   */
  resetBatchTimer(jobType, flushCallback) {
    if (this.batchTimers[jobType]) {
      clearTimeout(this.batchTimers[jobType]);
    }
    
    this.batchTimers[jobType] = setTimeout(() => {
      flushCallback(jobType).catch(err => {
        // console.error(`Error in timed batch flush for ${jobType}:`, err);
        this.resetBatchTimer(jobType, flushCallback);
      });
    }, this.batchTimeout);
  }

  /**
   * Get pending jobs for a job type
   * @param {string} jobType - Type of job
   * @returns {Array} Array of pending jobs
   */
  getPendingJobs(jobType) {
    if (!this.pendingBatches[jobType]) {
      return [];
    }
    
    const jobs = [...this.pendingBatches[jobType]];
    this.pendingBatches[jobType] = [];
    
    // Update stats
    this.batchStats[jobType].flushed += jobs.length;
    this.batchStats[jobType].lastFlushTime = Date.now();
    
    return jobs;
  }

  /**
   * Handle batch flush error by returning jobs to pending queue
   * @param {string} jobType - Type of job
   * @param {Array} jobs - Jobs that failed to flush
   * @param {Error} error - Error that occurred
   * @param {Function} retryCallback - Function to call for retry
   */
  handleFlushError(jobType, jobs, error, retryCallback) {
    // Put the failed batch back in the queue
    this.pendingBatches[jobType] = [...jobs, ...(this.pendingBatches[jobType] || [])];
    
    // Track error stats
    this.batchStats[jobType].errors = (this.batchStats[jobType].errors || 0) + 1;
    this.batchStats[jobType].lastError = error.message;
    this.batchStats[jobType].lastErrorTime = Date.now();
    
    // Calculate retry delay with exponential backoff
    const retryDelay = Math.min(
      5000 * Math.pow(1.5, Math.min(this.batchStats[jobType].errors, 5)), 
      60000
    );
    
    // console.log(`Will retry flushing ${jobType} batch in ${Math.round(retryDelay/1000)}s`);
    
    // Override the timer with a retry
    if (this.batchTimers[jobType]) {
      clearTimeout(this.batchTimers[jobType]);
    }
    
    this.batchTimers[jobType] = setTimeout(() => retryCallback(jobType), retryDelay);
  }

  /**
   * Get batch statistics
   * @returns {Object} Batch statistics
   */
  getStats() {
    return this.batchStats;
  }
}

module.exports = BatchManager;