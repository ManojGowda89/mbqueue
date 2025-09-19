/**
 * MBQueue - MongoDB-backed job queue system
 * @module mbqueue
 */

const QueueManager = require('./QueueManager');

// Export CommonJS style
module.exports = QueueManager;

// Also support ES modules
if (typeof exports === 'object' && typeof module !== 'undefined') {
  module.exports.default = QueueManager;
  // Add named exports if needed
  module.exports.QueueManager = QueueManager;
}