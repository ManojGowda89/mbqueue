const os = require('os');

/**
 * Handles memory monitoring and system stats
 */
class Monitoring {
  /**
   * Create a monitoring instance
   * @param {Function} onMemoryPressure - Callback for memory pressure events
   */
  constructor(onMemoryPressure) {
    this.onMemoryPressure = onMemoryPressure;
    this.stats = {
      totalAdded: 0,
      totalProcessed: 0,
      lastFlushTime: null,
      peakMemoryUsage: 0,
      lastDbWriteTime: 0
    };
    
    this.memoryMonitorInterval = null;
  }

  /**
   * Start monitoring
   * @param {number} interval - Check interval in ms
   */
  startMonitoring(interval = 5000) {
    this.memoryMonitorInterval = setInterval(() => this._monitorMemory(), interval);
  }

  /**
   * Stop monitoring
   */
  stopMonitoring() {
    if (this.memoryMonitorInterval) {
      clearInterval(this.memoryMonitorInterval);
      this.memoryMonitorInterval = null;
    }
  }

  /**
   * Check memory usage and trigger callbacks if needed
   * @private
   */
  _monitorMemory() {
    const memUsage = process.memoryUsage();
    const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
    this.stats.peakMemoryUsage = Math.max(this.stats.peakMemoryUsage, heapUsedMB);
    
    const freeMemMB = Math.round(os.freemem() / 1024 / 1024);
    const totalMemMB = Math.round(os.totalmem() / 1024 / 1024);
    const memUsagePercent = 100 - (freeMemMB / totalMemMB * 100);
    
    // If memory usage is above 80%, trigger memory pressure callback
    if (memUsagePercent > 80) {
      console.warn(`⚠️ High memory usage (${memUsagePercent.toFixed(1)}%)`);
      if (this.onMemoryPressure) {
        this.onMemoryPressure(memUsagePercent);
      }
    }
    
    // Log memory stats occasionally
    if (Math.random() < 0.1) { // ~10% chance to log
      console.log(`Memory usage: ${heapUsedMB}MB (heap) | System: ${memUsagePercent.toFixed(1)}% | Peak: ${this.stats.peakMemoryUsage}MB`);
    }
  }

  /**
   * Update stats
   * @param {Object} updates - Stats to update
   */
  updateStats(updates) {
    Object.assign(this.stats, updates);
  }

  /**
   * Get current stats
   * @returns {Object} Current stats
   */
  getStats() {
    return {
      ...this.stats,
      memoryUsageMB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      uptime: process.uptime()
    };
  }
}

module.exports = Monitoring;