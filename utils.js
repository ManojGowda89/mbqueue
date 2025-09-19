/**
 * Generate a unique ID for jobs
 * @returns {string} Unique ID
 */
function generateId() {
  return Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
}

/**
 * Split an array into chunks
 * @param {Array} array - Array to split
 * @param {number} chunkSize - Size of each chunk
 * @returns {Array} Array of chunks
 */
function chunkArray(array, chunkSize = 1000) {
  const chunks = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}

module.exports = {
  generateId,
  chunkArray
};