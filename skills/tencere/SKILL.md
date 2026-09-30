---
name: tencere
description: >-
  Comprehensive guide and runbook for Tencere, the high-performance embedded and distributed KV, cache,
  timeseries, and coordination database. Use when designing schemas and collections (KV, Map, Set, Sorted,
  Stream, Queue, Vector, TimeSeries), configuring storage and persistence (Daktilo WAL, Cemalloc, Muttafa),
  setting up multi-node Raft clusters with Raptiye replication, managing distributed coordination primitives
  (locks, fencing tokens, semaphores, once, idempotency, rate limiters, cache stampede protection),
  operating point-in-time history and time-travel rollbacks, or running the standalone server/REPL.
---

# Tencere Embedded & Distributed Database: Definitive Runbook & Developer Guide

Tencere is a modern, high-performance KV, cache, timeseries, and coordination database designed for both **embedded** (in-process) and **distributed multi-node server** environments.

---

## 🏗️ 1. Architecture & Layering

Tencere composes dedicated, specialized primitives rather than reinventing the infrastructure wheel:

```text
cemalloc  ──► Memory allocation, slab views & zero-leak recycling
Muttafa   ──► Packed in-memory structures & local storage engine
daktilo   ──► Durable ordered write-ahead operation log (WAL)
Raptiye   ──► Distributed multi-node consensus (Raft), leader election & transport
Tencere   ──► Database semantics, collections, coordination primitives & developer API
```

### Durability Modes
When opening Tencere (`Tencere.open(dir, { durability })`):
* `"strict"`: Flushes and `fsync`s the Daktilo WAL on every mutation. Maximum safety, bounded by disk fsync IOPS.
* `"batch"` (recommended default for persistent workloads): Coalesces disk flushes within a small micro-batch window (e.g. 5ms), achieving 40K–100K+ durable ops/sec with minimal loss window.
* `"async"`: Asynchronous background log flushes.
* In-Memory (`dataDir: null`): Bypasses WAL disk I/O, unlocking 500K–2.8M+ in-memory operations/sec with zero allocations on hot paths.

---

## 🌐 2. Multi-Node Clustering & Raptiye Raft Replication

Tencere integrates **Raptiye** directly into `ClusterManager` (`src/cluster/cluster.js`) to provide distributed fault tolerance, state machine replication, and cluster membership.

### Key Concepts
1. **Leader Election & Heartbeats:** Düğümler arasında Raft protokolü ile otomatik lider seçimi yapılır. Sadece seçilen lider düğüm yazma (mutation) işlemlerini kabul eder.
2. **Leader Fencing Protection:** Lider olmayan (follower/candidate) bir düğümde mutasyon tetiklenirse, sistem `[Cluster Fencing] Node X is not leader` hatası fırlatarak split-brain ve tutarsız yazmaları engeller.
3. **Write Replication & Ack Levels:** Liderde işlenen her mutasyon ikili formatta paketlenip Raft günlüğüne gönderilir. Seçenekler:
   * `ack: "quorum"` (varsayılan): Düğümlerin çoğunluğu (n/2 + 1) commit edene kadar bekler.
   * `ack: "all"`: Tüm düğümlerin commit etmesini bekler.
   * `ack: "local"`: Yerel uygulandıktan hemen sonra döner.
4. **Partisyonlama (Partition Routing):** `PartitionManager` (`src/core/partitions.js`) 128 partisyon (`DEFAULT_PARTITION_COUNT`) üzerinde FNV-1a hash algoritmasıyla anahtarları küme üyelerine dağıtır ve otomatik rebalance uygular.
5. **Transport:** Ağ üzerinden gerçek makineler için `TCPTransport`, birim ve entegrasyon testleri için `MemoryTransport` / `MemoryNetwork`.
6. **Log Kalıcılığı:** Düğümler disk modunda Raft durumunu `<dataDir>/raft` klasöründe `FileLog` ile saklar; in-memory modda `MemoryLog` kullanılır.

### Cluster Configuration Options (`ClusterConfig`)

```typescript
export interface ClusterConfig {
  nodeId: string | number;                         // Current node unique ID (e.g., 1)
  peers?: (string | number)[];                    // Peer node IDs (e.g., [2, 3])
  port?: number;                                  // TCP listen port for cluster RPC (e.g., 7337)
  host?: string;                                  // TCP bind address (default '127.0.0.1')
  peerAddresses?: Record<string | number, string>;// e.g., { 2: "10.0.0.2:7337", 3: "10.0.0.3:7337" }
  election?: {
    minTimeout?: number;                          // Min election timeout in ms (default 30-150)
    maxTimeout?: number;                          // Max election timeout in ms (default 80-300)
  };
  heartbeatInterval?: number;                     // Heartbeat interval in ms (default 15-50)
  network?: any;                                  // MemoryNetwork instance for mock testing
}
```

### Usage: Production Multi-Node TCP Setup

```javascript
import { Tencere } from "tencere";

// Node 1 (10.0.0.1:7337)
const db = await Tencere.open("./data-node1", {
  cluster: {
    nodeId: 1,
    peers: [2, 3],
    port: 7337,
    host: "10.0.0.1",
    peerAddresses: {
      2: "10.0.0.2:7337",
      3: "10.0.0.3:7337"
    },
    election: { minTimeout: 150, maxTimeout: 300 },
    heartbeatInterval: 50
  }
});

// Lider seçilene kadar bekle
const leaderId = await db.cluster.waitForLeader(5000);
console.log("Cluster Leader ID:", leaderId);

// Durum kontrolü
const status = db.cluster.status();
console.log(status);
// { enabled: true, nodeId: 1, term: 1, isLeader: true, role: 'LEADER', peers: [2, 3] }

// Yazma operasyonu çoğunluk (quorum) onaylı replike edilir
await db.set("cluster:config:version", "v2.1.0", { ack: "quorum" });

await db.close();
```

### Usage: In-Memory Multi-Node Testing

```javascript
import { Tencere } from "tencere";
import { MemoryNetwork } from "raptiye";

const net = new MemoryNetwork();

const node1 = await Tencere.open({
  cluster: { nodeId: 1, peers: [2], network: net, election: { minTimeout: 30, maxTimeout: 60 }, heartbeatInterval: 15 }
});
const node2 = await Tencere.open({
  cluster: { nodeId: 2, peers: [1], network: net, election: { minTimeout: 30, maxTimeout: 60 }, heartbeatInterval: 15 }
});

const leaderId = (await node1.cluster.waitForLeader(1500)) || (await node2.cluster.waitForLeader(1500));
const leader = leaderId === 1 ? node1 : node2;
const follower = leaderId === 1 ? node2 : node1;

await leader.set("replicated:state", "active");

// Follower state is synchronized
console.log(await follower.get("replicated:state")); // "active"

await node1.close();
await node2.close();
```

### Usage: Cluster-Aware Remote Client (`TencereClient.cluster`)
```javascript
import { TencereClient } from "tencere";

// Seed adresleriyle kümeye bağlan (otomatik topoloji keşfi ve lider yönlendirmesi)
const cluster = await TencereClient.cluster(["10.0.0.1:7337", "10.0.0.2:7337"], {
  readPreference: "leader", // 'leader' (güçlü tutarlılık) | 'follower' (okuma yükü dağıtımı) | 'nearest'
  maxRetries: 3,
  retryDelayMs: 100
});

// Yazmalar doğrudan ve otomatik olarak mevcut lidere yönlendirilir:
await cluster.set("user:101", { name: "Alice", active: true });
await cluster.increment("views", 1);

// Okumalar readPreference ayarına göre yönlendirilir:
const user = await cluster.get("user:101");

// Lider failover olduğunda clusterClient otomatik olarak yeni lideri keşfeder ve yeniden bağlanır.
await cluster.close();
```

### Usage: Smart Follower Redirection & Server-Side Write Forwarding
- **`ERR_NOT_LEADER` Error**: Takipçi (follower) düğümüne doğrudan yazma yapıldığında `code: "ERR_NOT_LEADER"`, `leaderId` ve `leaderAddress` bilgileri içeren yapısal hata döner.
- **`autoRedirect: true`**: Standalone `TencereClient.connect("10.0.0.2:7337", { autoRedirect: true })` takipçiye bağlansa bile ilk yazmada otomatik olarak liderin adresine geçer.
- **`--forward-writes` (Server-side Proxy)**: `tencere serve --forward-writes` ile başlatılan takipçi düğümler, gelen yazma isteklerini arka planda şeffaf bir şekilde küme liderine vekaleten iletir ve sonucu döner.

### Usage: Instant Test Clusters & Chaos Testing (`createTestCluster`)
Geliştiricilerin sıfır konfigürasyonla küme entegrasyon testleri, split-brain, network partition ve düğüm çökmesi senaryolarını test etmesini sağlar:

```javascript
import { createTestCluster } from "tencere/testing";

// 1 satırda 3 düğümlü in-memory veya TCP test kümesi ayağa kaldır
const cluster = await createTestCluster({ nodes: 3 });

// Lider ve takipçi tespiti
const leader = cluster.leader; // veya await cluster.waitForLeader()
const followers = cluster.followers;

// Ağ kaosu ve split-brain simülasyonu
cluster.isolate(leader); // Lideri izole et -> Kalan çoğunluk yeni lider seçer
const newLeader = await cluster.waitForLeader();

// İki gruba ayır (bipartition): [1] vs [2, 3]
cluster.partition([1], [2, 3]);

// Ağı iyileştir (heal)
cluster.heal();

// Düğüm durdurma ve yeniden başlatma simülasyonu
await cluster.stopNode(2);
await cluster.startNode(2);
// veya tek adımda: await cluster.restartNode(2);

// Gerçek TCP soketleri ile test kümesi
const tcpCluster = await createTestCluster({ nodes: 3, tcp: true, basePort: 9100 });
const client = await tcpCluster.client();
await client.set("key", "val");

await cluster.destroy();
await tcpCluster.destroy();
```

### Usage: Cluster Observability, Health API & Telemetry Metrics
Küme mutabakat sağlığını, quorum bütünlüğünü ve Raft telemetri metriklerini sorgulama:

```javascript
// Kapsamlı küme sağlığı kontrolü (HEALTHY, DEGRADED, QUORUM_LOST)
const health = await db.cluster.health({ pingPeers: true });
console.log(health.status);    // "HEALTHY" | "DEGRADED" | "QUORUM_LOST"
console.log(health.readiness); // true (yazma/okuma kabul ediyor) | false (quorum kaybı)
console.log(health.quorum);    // { required: 2, reachable: 3, total: 3, hasQuorum: true }
console.log(health.nodes);     // [{ id: 1, role: "leader", status: "ONLINE", latencyMs: 0.2 }, ...]

// Raft replikasyon telemetri metrikleri
const metrics = db.cluster.metrics();
console.log(metrics.commitIndex);    // Raft mutabakat log indeksi
console.log(metrics.lastApplied);    // State machine'e uygulanan son indeks
console.log(metrics.replicationLag); // Replikasyon gecikmesi (entry farkı)
console.log(metrics.bytes);          // { wireBytes, payloadBytes, ... }

// İstemci üzerinden tüm küme düğümlerinin sağlığını ve gecikmelerini ölçme:
const clientHealth = await cluster.health();
```

### Usage: Single-Command Local Cluster Development (`tencere cluster dev`)
Yerel geliştirme ortamında tek komutla çok düğümlü küme ayağa kaldırmak için:

```bash
# 3 düğümlü yerel test kümesi (varsayılan: 7337, 7338, 7339 portları, in-memory)
tencere cluster dev --nodes 3 --memory

# Disk kalıcılığı ile yerel küme:
tencere cluster dev --nodes 3 --data-dir ./.tencere-cluster
```

---

## ⚡ 3. Engine Modes: Async, Sync, Server & Client

### 1. Asynchronous Embedded Engine (Primary)
```javascript
import { Tencere } from "tencere";

const db = await Tencere.open("./data", { durability: "batch" });
await db.set("key", "val", { ttl: "1h" });
const val = await db.get("key");
await db.close();
```

### 2. Synchronous Embedded Engine (`TencereSync`)
Direct in-process synchronous API for latency-critical tight loops without Promise allocations:
```javascript
import { TencereSync } from "tencere/sync";

const db = new TencereSync("./data");
db.set("counter", 100);
const c = db.get("counter"); // 100
db.counter("views").inc();
db.close();
```

### 3. Standalone TCP Server & Remote Client
```bash
# Start server (with optional cluster write forwarding)
tencere serve ./data --port 7337 --forward-writes
```
```javascript
import { TencereClient } from "tencere";

const client = await TencereClient.connect("127.0.0.1:7337", { autoRedirect: true });

// Basic operations
await client.set("key", "remote-val");
console.log(await client.get("key"));

// Pipelining
const [r1, r2] = await client.pipeline()
  .set("p1", 10)
  .increment("p1", 5)
  .exec();

// Change Streams
await client.watch("users:*", (event) => {
  console.log("Remote change:", event.key, event.value);
});

await client.close();
```

---

## 🔒 4. Distributed Coordination Primitives

Tencere includes first-class coordination primitives designed to eliminate separate dependencies like Redis/Redlock or ZooKeeper for application orchestration:

### 1. Distributed Locks with Monotonic Fencing Tokens (`db.lock`, `db.tryLock`)
```javascript
await db.lock("order:1001", async ({ token, renew }) => {
  // token: strictly monotonically increasing bigint
  // Protects downstream storage (e.g., PostgreSQL or S3) from stale delayed workers
  console.log("Acquired lock with fencing token:", token);
  await processPayment(1001, token);
}, { ttl: 5000, waitTimeout: 3000 });
```

### 2. Single-Execution Guarantee (`db.once`)
Ensures a job executes exactly once across the cluster:
```javascript
const executed = await db.once("job:daily-cleanup:2026-09-28", async () => {
  await runDailyCleanup();
}, { ttl: "24h" });
```

### 3. Request Deduplication & Replay (`db.idempotent`)
Caches and replays response payloads to protect non-idempotent endpoints:
```javascript
const response = await db.idempotent(idempotencyKey, async () => {
  return await chargeCreditCard(orderId, amount);
}, { ttl: "10m" });
```

### 4. Distributed Semaphores (`db.semaphore`)
Lease-based concurrency limiter:
```javascript
await db.semaphore("ai:embedding:concurrent-limit", { limit: 5, ttl: 10000 }, async () => {
  await generateEmbeddings(batch);
});
```

### 5. Atomic Sliding-Window Rate Limiting (`db.rateLimit`)
High-performance sliding-window rate limiter without race conditions:
```javascript
const { allowed, remaining, resetMs } = await db.rateLimit(`ip:${clientIp}`, {
  limit: 100,
  windowMs: 60000
});
if (!allowed) throw new Error("Rate limit exceeded. Try again in " + resetMs + "ms");
```

### 6. Cache Stampede Protection & Stale-While-Revalidate (`db.cache`)
Guarantees single origin fetch during cache misses (avoids thundering herd):
```javascript
const userData = await db.cache("user:profile:42", { ttl: "5m", stale: "15m" }, async () => {
  return await dbQueryUserProfile(42);
});
```

### 7. Task Scheduling with Lease Fencing (`db.schedule`)
```javascript
const task = db.schedule("metrics:aggregate", { interval: "1m" }, async () => {
  await aggregateNodeMetrics();
});
// task.stop();
```

---

## 📦 5. First-Class Collections Guide

### 1. TimeSeries (`db.timeseries`)
High-throughput time-series engine backed by SkipList and Daktilo WAL:
```javascript
const ts = db.timeseries("telemetry:cpu", {
  retention: "30d",
  tags: { indexed: ["host", "region"] }
});

await ts.add(54.2, { tags: { host: "api-1", region: "eu-west" } });
await ts.addMany([[Date.now(), 61.0], [Date.now() + 1000, 58.5]]);

// Range scan with aggregation & epoch/UTC bucketing
const hourlyAverages = await ts
  .where({ host: "api-1" })
  .between("24h ago", "now")
  .bucket("1h")
  .avg();

// Realtime CDC Watch
for await (const sample of ts.watch()) {
  console.log("Incoming point:", sample.timestamp, sample.value);
}
```

### 2. Sorted Collection (`db.sorted`)
Leaderboards and priority indices with rank lookups:
```javascript
const lb = db.sorted("leaderboard");
await lb.set("player1", 1500);
await lb.set("player2", 2300);
const top5 = await lb.top(5);
const rank = await lb.rank("player1");
const slice = await lb.between(1000, 2000).limit(10).values();
```

### 3. Queue (`db.queue`)
Reliable message queue with visibility leases and dead-letter handling:
```javascript
const q = db.queue("email-tasks", { visibilityTimeout: 30000, maxRetries: 3 });
await q.push({ to: "user@example.com", subject: "Welcome" });

// Worker consumption
const msg = await q.pull();
if (msg) {
  try {
    await sendEmail(msg.payload);
    await msg.ack();
  } catch (err) {
    await msg.nack();
  }
}
```

### 4. Vector Collection (`db.vector`)
Zero-dependency in-memory vector search with Top-K bounded min-heap:
```javascript
const docs = db.vector("docs", { metric: "cosine", dimensions: 128 });
await docs.set("doc:1", embedding128, { title: "Documentation" });
const matches = await docs.search(queryEmbedding, { limit: 5 });
```

---

## ⏳ 6. Time Travel, History & Rollback

Tencere uses forward-only canonical mutation streams (`OP_RESTORE`) preserving an immutable audit log without truncating history.

```javascript
// Point-in-time reads
const oldTitle = await db.get("config:title", { at: "1h ago" });
const version3 = await db.get("config:title", { version: 3n });

// Read-only historical snapshot view across all collections
const past = db.at("30m ago");
console.log(await past.get("config:title"));
console.log(await past.sorted("leaderboard").top(10));

// Atomic forward rollback of a single key
await db.rollback("config:title", { to: "1h ago" });

// Conflict-checked multi-key scope rollback plan
const plan = await db.scope("tenant:corp").rollbackPlan({ to: "2h ago" });
console.log(await plan.summary());
await plan.apply({ onConflict: "abort" });
```

---

## 🛠️ 7. CLI, Diagnostics & Invariants

### Command Line Interface
```bash
# REPL
tencere repl
tencere repl 127.0.0.1:7337
tencere repl --local ./data

# Server
tencere serve ./data --port 7337

# State inspection & backup
tencere inspect ./data
tencere stats ./data
tencere backup ./data ./backup-archive
tencere restore ./backup-archive ./restored-data

# Cluster Observability, Dashboard & Probes (Kubernetes / Docker)
tencere cluster nodes 127.0.0.1:7337            # Unicode tablo dashboard: Node, Role, Term, Health, Address, Latency
tencere cluster health 127.0.0.1:7337           # K8s readinessProbe uyumlu: 0 = READY/HEALTHY, 1 = QUORUM_LOST
tencere cluster health 127.0.0.1:7337 --json    # Monitoring ve alert sistemleri için JSON çıktısı
tencere cluster metrics 127.0.0.1:7337          # Raft telemetri metrikleri

# Agent Skill Installation
tencere skill install                           # Installs skill to workspace (.agents/skills/tencere) and global
tencere skill install --global                  # Installs to ~/.gemini/config/skills/tencere
tencere skill install --workspace               # Installs to .agents/skills/tencere
```

### Programmatic Invariants & Diagnostics
```javascript
// Compute cryptographic SHA-256 state hash for verification
const hash = await db.debug.stateHash();

// Verify partition hash across cluster nodes
const pHash = await db.debug.partitionHash(0, 128);

// Verify core data structure invariants
const report = await db.debug.verify();
console.log(report.valid); // true
```
