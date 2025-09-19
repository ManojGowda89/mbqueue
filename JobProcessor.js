/**
 * Handles job processing
 */
class JobProcessor {
  /**
   * Create a job processor
   * @param {Object} db - MongoDB database connection
   */
  constructor(db) {
    this.db = db;
    this.processors = {};
    this.isProcessing = {};
    this.jobOptions = {};
    this.stats = {
      totalProcessed: 0
    };
  }

  /**
   * Start processing jobs
   * @param {string} jobType - Type of job to process
   * @param {Function} processor - Function to process jobs
   * @param {Object} options - Processing options
   */
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

  /**
   * Stop processing jobs
   * @param {string} jobType - Type of job to stop processing
   */
  stopProcessing(jobType) {
    this.isProcessing[jobType] = false;
    console.log(`⏹️ Stopped processing "${jobType}" jobs`);
  }

  /**
   * Main processing loop
   * @param {string} jobType - Type of job
   * @param {number} workerId - Worker ID
   * @private
   */
  async _processLoop(jobType, workerId) {
    if (!this.isProcessing[jobType]) return;
    
    let currentBackoff = this.jobOptions[jobType].pollInterval;
    let emptyBatchCount = 0;
    
    try {
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

  /**
   * Mark job as complete
   * @param {string} jobType - Type of job
   * @param {string} jobId - Job ID
   * @param {*} result - Job result
   * @private
   */
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

  /**
   * Handle job error
   * @param {string} jobType - Type of job
   * @param {string} jobId - Job ID
   * @param {Error} error - Error object
   * @private
   */
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
}

module.exports = JobProcessor;

// /**
//  * Handles job processing with improved concurrency control
//  */
// class JobProcessor {
//   /**
//    * Create a job processor
//    * @param {Object} db - MongoDB database connection
//    */
//   constructor(db) {
//     this.db = db;
//     this.processors = {};
//     this.isProcessing = {};
//     this.jobOptions = {};
//     this.stats = {
//       totalProcessed: 0,
//       duplicateAttempts: 0,
//       jobNotFoundErrors: 0
//     };
//   }

//   /**
//    * Start processing jobs
//    * @param {string} jobType - Type of job to process
//    * @param {Function} processor - Function to process jobs
//    * @param {Object} options - Processing options
//    */
//   startProcessing(jobType, processor, options = {}) {
//     this.jobOptions[jobType] = {
//       batchSize: options.batchSize || 100,
//       pollInterval: options.pollInterval || 1000,
//       maxConcurrent: options.maxConcurrent || 1,
//       backoffFactor: options.backoffFactor || 1.5,
//       maxBackoffInterval: options.maxBackoffInterval || 30000,
//       allowDuplicates: options.allowDuplicates || false,
//       jobLockTimeout: options.jobLockTimeout || 60000, // 60 seconds default lock timeout
//       workerId: `worker-${Math.random().toString(36).substring(2, 10)}`
//     };
    
//     this.processors[jobType] = processor;
//     this.isProcessing[jobType] = true;
    
//     // Start processing loops (up to maxConcurrent)
//     for (let i = 0; i < this.jobOptions[jobType].maxConcurrent; i++) {
//       this._processLoop(jobType, i);
//     }
    
//     console.log(`🚀 Started processing "${jobType}" jobs (batch size: ${this.jobOptions[jobType].batchSize}, concurrency: ${this.jobOptions[jobType].maxConcurrent})`);
//   }

//   /**
//    * Stop processing jobs
//    * @param {string} jobType - Type of job to stop processing
//    */
//   stopProcessing(jobType) {
//     this.isProcessing[jobType] = false;
//     console.log(`⏹️ Stopped processing "${jobType}" jobs`);
//   }

//   /**
//    * Main processing loop with improved job locking
//    * @param {string} jobType - Type of job
//    * @param {number} workerId - Worker ID
//    * @private
//    */
//   async _processLoop(jobType, workerId) {
//     if (!this.isProcessing[jobType]) return;
    
//     let currentBackoff = this.jobOptions[jobType].pollInterval;
//     let emptyBatchCount = 0;
//     const workerIdentifier = `${this.jobOptions[jobType].workerId}-${workerId}`;
    
//     try {
//       // Find and update atomically to claim jobs
//       // This uses findAndModify to atomically claim jobs with a distributed lock pattern
//       const lockExpiryTime = new Date(Date.now() + this.jobOptions[jobType].jobLockTimeout);
      
//       const jobs = [];
//       const batchSize = this.jobOptions[jobType].batchSize;
      
//       // Claim jobs one at a time with findOneAndUpdate to avoid multiple workers claiming the same job
//       for (let i = 0; i < batchSize; i++) {
//         const result = await this.db.collection('jobs').findOneAndUpdate(
//           { 
//             jobType, 
//             status: 'pending',
//             $or: [
//               { lockedBy: { $exists: false } },
//               { lockExpires: { $lt: new Date() } }
//             ]
//           },
//           { 
//             $set: { 
//               status: 'processing', 
//               startedAt: new Date(),
//               lockedBy: workerIdentifier,
//               lockExpires: lockExpiryTime
//             } 
//           },
//           { 
//             sort: { priority: -1, createdAt: 1 },
//             returnDocument: 'after'
//           }
//         );
        
//         if (result.value) {
//           jobs.push(result.value);
//         } else {
//           break; // No more jobs available
//         }
//       }

//       if (jobs.length > 0) {
//         console.log(`📤 Worker ${workerId}: Processing batch of ${jobs.length} "${jobType}" jobs`);
//         emptyBatchCount = 0; // Reset empty batch counter
//         currentBackoff = this.jobOptions[jobType].pollInterval; // Reset backoff
        
//         // Process each job in the batch
//         for (const job of jobs) {
//           if (!this.isProcessing[jobType]) break; // Stop if processing was disabled
          
//           try {
//             const result = await this.processors[jobType](job.data, job);
//             await this._complete(jobType, job._id, result, workerIdentifier);
//             this.stats.totalProcessed++;
//           } catch (err) {
//             console.error(`Error processing ${jobType} job ${job._id}:`, err);
//             await this._error(jobType, job._id, err, workerIdentifier);
//           }
//         }
        
//         // Continue processing immediately if we got a full batch
//         if (jobs.length >= batchSize && this.isProcessing[jobType]) {
//           setImmediate(() => this._processLoop(jobType, workerId));
//           return;
//         }
//       } else {
//         // No jobs found, apply backoff strategy
//         emptyBatchCount++;
        
//         if (emptyBatchCount > 3) {
//           // Increase backoff time exponentially, up to max
//           currentBackoff = Math.min(
//             currentBackoff * this.jobOptions[jobType].backoffFactor,
//             this.jobOptions[jobType].maxBackoffInterval
//           );
//         }
//       }
      
//       // Schedule next iteration with current backoff
//       if (this.isProcessing[jobType]) {
//         setTimeout(() => this._processLoop(jobType, workerId), currentBackoff);
//       }
//     } catch (err) {
//       console.error(`Error in process loop for "${jobType}" worker ${workerId}:`, err);
//       // On error, wait a bit and try again with increased backoff
//       const errorBackoff = Math.min(currentBackoff * 2, this.jobOptions[jobType].maxBackoffInterval);
      
//       if (this.isProcessing[jobType]) {
//         setTimeout(() => this._processLoop(jobType, workerId), errorBackoff);
//       }
//     }
//   }

//   /**
//    * Mark job as complete with improved error handling
//    * @param {string} jobType - Type of job
//    * @param {string} jobId - Job ID
//    * @param {*} result - Job result
//    * @param {string} workerId - Worker ID that processed the job
//    * @private
//    */
//   async _complete(jobType, jobId, result, workerId) {
//     try {
//       // First check if we still own the lock on this job
//       const job = await this.db.collection('jobs').findOne({ 
//         _id: jobId,
//         lockedBy: workerId 
//       });
      
//       if (!job) {
//         // Job might have been processed by another worker already
//         // Check if it's in completed_jobs
//         const completedJob = await this.db.collection('completed_jobs').findOne({ _id: jobId });
//         if (completedJob) {
//           // Job was already completed by another worker, just log and move on
//           this.stats.duplicateAttempts++;
//           return;
//         }
        
//         // If we can't find it in completed_jobs either, it might be truly missing or moved to failed_jobs
//         throw new Error(`Job ${jobId} not found or lock expired`);
//       }

//       // Use a transaction if MongoDB supports it (version 4.0+)
//       const session = this.db.client.startSession ? await this.db.client.startSession() : null;
      
//       try {
//         if (session) {
//           await session.withTransaction(async () => {
//             await this.db.collection('completed_jobs').insertOne({
//               ...job,
//               status: 'completed',
//               completedAt: new Date(),
//               result
//             }, { session });
            
//             await this.db.collection('jobs').deleteOne({ _id: jobId, lockedBy: workerId }, { session });
//           });
//         } else {
//           // If transactions not supported, do sequential operations with checks
//           try {
//             await this.db.collection('completed_jobs').insertOne({
//               ...job,
//               status: 'completed',
//               completedAt: new Date(),
//               result
//             });
//           } catch (err) {
//             // If duplicate key error and allowDuplicates is true, we can ignore it
//             if (err.code === 11000 && this.jobOptions[jobType].allowDuplicates) {
//               this.stats.duplicateAttempts++;
//               // Continue to deletion
//             } else {
//               throw err;
//             }
//           }
          
//           // Only delete if we still have the lock
//           await this.db.collection('jobs').deleteOne({ _id: jobId, lockedBy: workerId });
//         }
//       } finally {
//         if (session) await session.endSession();
//       }
//     } catch (err) {
//       // Special handling for duplicate key errors if allowDuplicates is enabled
//       if (err.code === 11000 && this.jobOptions[jobType].allowDuplicates) {
//         this.stats.duplicateAttempts++;
//         // Try to clean up the job if it's still in the jobs collection
//         await this.db.collection('jobs').deleteOne({ _id: jobId });
//         return;
//       }
      
//       // For job not found errors, increment counter but don't re-throw
//       if (err.message.includes('not found')) {
//         this.stats.jobNotFoundErrors++;
//         return;
//       }
      
//       throw err;
//     }
//   }

//   /**
//    * Handle job error with improved locking
//    * @param {string} jobType - Type of job
//    * @param {string} jobId - Job ID
//    * @param {Error} error - Error object
//    * @param {string} workerId - Worker ID that processed the job
//    * @private
//    */
//   async _error(jobType, jobId, error, workerId) {
//     try {
//       // First check if we still own the lock on this job
//       const job = await this.db.collection('jobs').findOne({ 
//         _id: jobId,
//         lockedBy: workerId 
//       });
      
//       if (!job) {
//         // Job might have been processed by another worker already
//         // Check if it's in completed_jobs or failed_jobs
//         const completedJob = await this.db.collection('completed_jobs').findOne({ _id: jobId });
//         if (completedJob) {
//           // Job was already completed by another worker, just log and move on
//           this.stats.duplicateAttempts++;
//           return;
//         }
        
//         const failedJob = await this.db.collection('failed_jobs').findOne({ _id: jobId });
//         if (failedJob) {
//           // Job was already failed by another worker, just log and move on
//           this.stats.duplicateAttempts++;
//           return;
//         }
        
//         // If we can't find it anywhere, it might be truly missing
//         this.stats.jobNotFoundErrors++;
//         return;
//       }

//       const attempts = job.attempts + 1;

//       if (attempts >= job.maxAttempts) {
//         // Use a transaction if MongoDB supports it
//         const session = this.db.client.startSession ? await this.db.client.startSession() : null;
        
//         try {
//           if (session) {
//             await session.withTransaction(async () => {
//               await this.db.collection('failed_jobs').insertOne({
//                 ...job,
//                 status: 'failed',
//                 failedAt: new Date(),
//                 error: error.message,
//                 stack: error.stack
//               }, { session });
              
//               await this.db.collection('jobs').deleteOne({ _id: jobId, lockedBy: workerId }, { session });
//             });
//           } else {
//             // If transactions not supported, do sequential operations with checks
//             await this.db.collection('failed_jobs').insertOne({
//               ...job,
//               status: 'failed',
//               failedAt: new Date(),
//               error: error.message,
//               stack: error.stack
//             });
            
//             // Only delete if we still have the lock
//             await this.db.collection('jobs').deleteOne({ _id: jobId, lockedBy: workerId });
//           }
//         } finally {
//           if (session) await session.endSession();
//         }
//       } else {
//         // Calculate exponential backoff for retries
//         const backoffDelay = Math.pow(2, attempts - 1) * 1000; // 1s, 2s, 4s, 8s...
//         const retryAt = new Date(Date.now() + backoffDelay);
        
//         // Release lock and update retry information
//         await this.db.collection('jobs').updateOne(
//           { _id: jobId, lockedBy: workerId },
//           { 
//             $set: { 
//               status: 'pending', 
//               attempts, 
//               lastError: error.message, 
//               lastErrorAt: new Date(),
//               retryAt
//             },
//             $unset: { lockedBy: "", lockExpires: "" }
//           }
//         );
//       }
//     } catch (err) {
//       // For job not found errors, increment counter but don't re-throw
//       if (err.message.includes('not found')) {
//         this.stats.jobNotFoundErrors++;
//         return;
//       }
      
//       throw err;
//     }
//   }
// }

// module.exports = JobProcessor;