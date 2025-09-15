---

# 📬 Job Queue with `mbqueue`

This project demonstrates how to use **[`mbqueue`](https://www.npmjs.com/package/mbqueue)** — a MongoDB-backed job queue manager.
It allows you to enqueue jobs, process them asynchronously with workers, and track their execution in MongoDB.

---

## 📦 What is `QueueManager`?

`QueueManager` (from `mbqueue`) is a **lightweight job queue system** that uses MongoDB collections as the backend.

It provides:

* ✅ Durable job storage in MongoDB (per-queue collections)
* ✅ Background workers with `startProcessing()`
* ✅ Job statistics with `getJobCounts()`
* ✅ Support for concurrency, batch processing, retries
* ✅ Scaling across multiple processes or servers

---

## ⚙️ How It Works

### 1. Initialize Queue

```js
const QueueManager = require("mbqueue");
const queue = new QueueManager("mongodb://localhost:27017", "job_queue");
await queue.connect();
```

### 2. Add Jobs

```js
const jobId = await queue.addJob("email", { to: "alice@example.com" });
```

* First argument → queue name (`email`, `notification`, etc.)
* Second argument → job payload
* Third argument (optional) → options (e.g., priority)

### 3. Start Processing

```js
queue.startProcessing(
  "email",
  async (data, job) => {
    console.log("📧 Sending email:", data);
    await new Promise(res => setTimeout(res, 500)); // simulate email
    return { sent: true, timestamp: new Date() };
  },
  { batchSize: 1 } // process one at a time
);

queue.startProcessing(
  "notification",
  async (data, job) => {
    console.log("🔔 Sending notification:", data);
    await new Promise(res => setTimeout(res, 200));
    return { sent: true };
  },
  { batchSize: 100, maxConcurrent: 5 } // process in bulk
);
```

* `queue.startProcessing(type, workerFn, options)`

  * `type`: queue name
  * `workerFn`: function to process each job
  * `options`:

    * `batchSize`: how many jobs to pull per batch
    * `maxConcurrent`: max parallel jobs per batch

### 4. Get Job Counts

```js
const counts = await queue.getJobCounts("email");
console.log(counts);
// { pending: 12, processing: 3, completed: 50, failed: 5 }
```

---

## 🖼️ Architecture Diagram

```mermaid
flowchart TD
    Client[Client / API Request] -->|POST /job/:type| ExpressServer[Express Server]
    ExpressServer -->|queue.addJob()| QueueManager[QueueManager (mbqueue)]
    QueueManager -->|Insert job| MongoDB[(MongoDB Collection: queue_<type>)]

    subgraph WorkerLoop[Worker Loop (startProcessing)]
        MongoDB -->|Fetch pending jobs| QueueManager
        QueueManager -->|Pass job data| WorkerFn[Worker Function]
        WorkerFn -->|Success/Fail| QueueManager
        QueueManager -->|Update job status| MongoDB
    end

    WorkerFn --> Logs[Console Logs / Result]
```

---

## 📊 Job Lifecycle in MongoDB

| Stage          | Example Document (simplified)                                   |
| -------------- | --------------------------------------------------------------- |
| **Pending**    | `{ _id, status: "pending", data: {...}, createdAt }`            |
| **Processing** | `{ _id, status: "processing", data: {...}, startedAt }`         |
| **Completed**  | `{ _id, status: "completed", result: {...}, finishedAt }`       |
| **Failed**     | `{ _id, status: "failed", error: "Error message", finishedAt }` |

---

## ⚡ Scaling with Multiple Workers

* Start **multiple workers** on the same machine or across servers.
* All workers connect to MongoDB and share the load.
* Jobs are atomically locked → no duplicate processing.

```bash
node worker.js   # Worker 1
node worker.js   # Worker 2
```

✅ Works locally or globally (multiple servers).

---

## 🛠 API Routes in `server.js`

### ➕ Add Job

```http
POST /job/:type
Content-Type: application/json

{
  "to": "alice@example.com",
  "message": "Hello!"
}
```

Response:

```json
{ "success": true, "jobId": "650fc1..." }
```

### 📊 Job Counts

```http
GET /job/:type/counts
```

Response:

```json
{
  "type": "email",
  "counts": { "pending": 2, "processing": 1, "completed": 10, "failed": 0 }
}
```

---

## ✅ Advantages of `mbqueue`

* Durable (jobs survive restarts).
* Scalable (run many workers).
* Flexible (batch size + concurrency options).
* Simple (MongoDB-based, no Redis needed).

---

