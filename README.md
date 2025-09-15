---

````markdown
# 📦 mbqueue

A **high-performance, MongoDB-backed job queue** for Node.js, designed for **high-volume job insertion, parallel processing, memory-efficient batching, and distributed deployments**.  

`mbqueue` is lightweight, robust, and scalable, ideal for **emails, notifications, background tasks, and more**.

---

## 🚀 Features

- **High-volume job insertion**: Rapidly add thousands of jobs per second.
- **Batch processing with adaptive flushing**: Supports batching jobs in memory and flushing them to MongoDB efficiently.
- **Concurrent workers**: Process jobs in parallel using multiple workers per job type.
- **Memory management**: Monitors system memory and triggers emergency flushes if needed.
- **Robust error handling**: Retries failed jobs with exponential backoff and tracks job attempts.
- **Job prioritization**: Supports priority-based processing.
- **Distributed architecture**: Add jobs from multiple servers and process jobs across multiple servers without duplication.
- **Scalable**: Easily scale horizontally by adding more worker servers.
- **MongoDB indexes**: Optimized for fast job retrieval and processing.
- **Real-time statistics**: Get queue stats, job counts, memory usage, and processing performance.

---

## 📦 Installation

```bash
npm install mbqueue
````

* GitHub: [https://github.com/ManojGowda89/mbqueue](https://github.com/ManojGowda89/mbqueue)
* NPM: [https://www.npmjs.com/package/mbqueue](https://www.npmjs.com/package/mbqueue)

---

## 🛠️ Usage

### 1. Initialize QueueManager

```js
const QueueManager = require('mbqueue');

const queue = new QueueManager('mongodb://localhost:27017', 'job_queue', {
  batchSize: 5000,
  batchTimeout: 10000,
  maxPendingJobs: 500000
});

await queue.connect();
```

---

### 2. Add Jobs

```js
// Email job
await queue.addJob('email', { name: 'John Doe', email: 'john@example.com' });

// Notification job with priority
await queue.addJob('notification', { userId: 1, message: 'Alert!' }, { priority: 5 });
```

---

### 3. Start Processing Jobs

```js
// Email processor
queue.startProcessing('email', async (data) => {
  await new Promise(res => setTimeout(res, 50));
  return { sent: true, timestamp: new Date() };
}, {
  batchSize: 1,
  maxConcurrent: 10
});

// Notification processor
queue.startProcessing('notification', async (data) => {
  return { processed: true };
}, {
  batchSize: 100,
  maxConcurrent: 5
});
```

---

### 4. Get Queue Stats

```js
const stats = queue.getStats();
console.log(stats);

const counts = await queue.getJobCounts('notification');
console.log(counts);
```

---

## 🏗️ Distributed Setup

`mbqueue` supports **multi-server producers and workers**:

* **Central MongoDB** as the single source of truth.
* **Multiple producers** can add jobs concurrently.
* **Multiple workers** across servers process jobs without duplication.

### Architecture Diagram

```
         ┌─────────────┐        ┌─────────────┐
         │ Producer 1  │        │ Producer 2  │
         └──────┬──────┘        └──────┬──────┘
                │                     │
                ▼                     ▼
          ┌─────────────────────────────┐
          │         MongoDB             │
          │     (Central Queue DB)      │
          └─────────────┬──────────────┘
                        │
       ┌────────────────┼────────────────┐
       ▼                ▼                ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Worker 1    │  │ Worker 2    │  │ Worker 3    │
│ (Server A)  │  │ (Server B)  │  │ (Server C)  │
└─────────────┘  └─────────────┘  └─────────────┘
```

---

## 🔹 Job Lifecycle

1. **Job creation** → Added to **memory batch**.
2. **Batch flush** → Flushed to **MongoDB** when batch size or timeout is reached.
3. **Processing** → Workers claim pending jobs atomically and update status to `processing`.
4. **Completion** → Successfully processed jobs moved to `completed_jobs`.
5. **Failure/Retry** → Failed jobs retried with **exponential backoff** until `maxAttempts`; otherwise moved to `failed_jobs`.

---

## ⚡ High-Volume & Memory Management

* Supports **hundreds of thousands of jobs in memory**.
* **Memory monitoring** ensures stability:

  * Automatic batch flush when memory usage exceeds 80%.
  * Peak memory usage tracking.
* **Chunked batch insertion** avoids MongoDB document size limits.
* Adaptive polling reduces DB load when the queue is empty.
* Supports **rapid job insertion**, e.g., thousands of jobs per second.

---

## 💡 Advantages Over Other Queue Systems

| Feature                | mbqueue | Bull  | RabbitMQ | Kafka | PG-Boss  |
| ---------------------- | ------- | ----- | -------- | ----- | -------- |
| DB backend             | MongoDB | Redis | AMQP     | Kafka | Postgres |
| High-volume insertion  | ✅       | ✅     | ✅        | ✅     | ✅        |
| Distributed processing | ✅       | ✅     | ✅        | ✅     | ✅        |
| Memory management      | ✅       | ❌     | ❌        | ❌     | ❌        |
| Job retry with backoff | ✅       | ✅     | ✅        | ✅     | ✅        |
| Priority jobs          | ✅       | ✅     | ✅        | ✅     | ✅        |
| Horizontal scalability | ✅       | ✅     | ✅        | ✅     | ✅        |
| Easy setup             | ✅       | ✅     | ❌        | ❌     | ✅        |

---

## ⏱️ Scaling with Multiple Workers

* Set `maxConcurrent` to run multiple processing loops per job type.
* Deploy workers across servers to **scale horizontally**.
* Jobs are **claimed atomically**, ensuring no duplicates.

---

## 🖼️ Queue Monitoring

```js
const stats = queue.getStats();
console.log(stats);
/*
{
  totalAdded: 1000,
  totalProcessed: 950,
  lastFlushTime: 1690000000000,
  peakMemoryUsage: 120,
  pendingJobsByType: { email: 50, notification: 0 },
  memoryUsageMB: 110,
  uptime: 3600
}
*/
```

---

## 🔧 Cleanup & Shutdown

```js
await queue.close();
```

* Flushes all pending batches.
* Stops all workers.
* Safely disconnects MongoDB.

---

## 📌 Key Takeaways

* Fully **high-volume capable** with **parallel workers**.
* **Memory-safe batching** and **adaptive flushing**.
* Supports **distributed multi-server deployments**.
* Robust **retry and failure handling**.
* Simple MongoDB-backed alternative to Redis/Bull/RabbitMQ/Kafka.

---

## 📂 Links

* GitHub: [https://github.com/ManojGowda89/mbqueue](https://github.com/ManojGowda89/mbqueue)
* NPM: [https://www.npmjs.com/package/mbqueue](https://www.npmjs.com/package/mbqueue)

---


