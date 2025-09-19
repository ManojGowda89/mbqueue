const { MongoClient } = require('mongodb');

/**
 * Manages MongoDB connections
 */
class ConnectionManager {
  /**
   * Create a connection manager
   * @param {string} connectionString - MongoDB connection string
   * @param {string} dbName - Database name
   * @param {Object} options - Connection options
   */
  constructor(connectionString, dbName, options = {}) {
    this.connectionString = connectionString;
    this.dbName = dbName;
    this.client = null;
    this.db = null;
    this.options = {
      maxPoolSize: options.maxPoolSize || 20,
      socketTimeoutMS: options.socketTimeoutMS || 30000,
      connectTimeoutMS: options.connectTimeoutMS || 30000,
      writeConcern: options.writeConcern || { w: 1 }
    };
  }

  /**
   * Connect to MongoDB
   * @returns {Promise<Object>} MongoDB database instance
   */
  async connect() {
    if (!this.client) {
      this.client = new MongoClient(this.connectionString, this.options);
      await this.client.connect();
      this.db = this.client.db(this.dbName);
      console.log('✅ Connected to MongoDB');

      // Create indexes for better performance
      await this._createIndexes();
    }
    return this.db;
  }

  /**
   * Close the MongoDB connection
   * @returns {Promise<void>}
   */
  async close() {
    if (this.client) {
      await this.client.close();
      this.client = null;
      this.db = null;
      console.log('Disconnected from MongoDB');
    }
  }

  /**
   * Create necessary indexes
   * @private
   */
  async _createIndexes() {
    const collections = ['jobs', 'completed_jobs', 'failed_jobs'];
    for (const col of collections) {
      await this.db.collection(col).createIndex({ jobType: 1, priority: -1, createdAt: 1 });
      await this.db.collection(col).createIndex({ status: 1 });
    }
  }
}

module.exports = ConnectionManager;