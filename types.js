/**
 * @typedef {Object} QueueOptions
 * @property {number} [batchSize=1000] - Maximum batch size for DB operations
 * @property {number} [batchTimeout=5000] - Maximum time in ms before flushing a batch
 * @property {number} [maxPendingJobs=100000] - Maximum jobs to keep in memory
 * @property {number} [monitorInterval=5000] - Memory monitoring interval in ms
 * @property {number} [maxPoolSize=20] - MongoDB connection pool size
 */

/**
 * @typedef {Object} JobOptions
 * @property {number} [priority=0] - Job priority (higher values processed first)
 * @property {number} [maxAttempts=3] - Maximum retry attempts
 */

/**
 * @typedef {Object} ProcessorOptions
 * @property {number} [batchSize=100] - Number of jobs to process in one batch
 * @property {number} [pollInterval=1000] - Initial polling interval in ms
 * @property {number} [maxConcurrent=1] - Maximum concurrent processors
 * @property {number} [backoffFactor=1.5] - Backoff multiplier for empty batches
 * @property {number} [maxBackoffInterval=30000] - Maximum backoff interval in ms
 */

// This file is for documentation purposes only
module.exports = {};