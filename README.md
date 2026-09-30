# Tencere

> Modern, high-performance KV / cache / coordination database designed for both embedded and standalone/server use.

Tencere is built from the ground up around **first-class Developer Experience (DX)**. Instead of wrestling with cryptic command protocols and fragmented tooling, Tencere provides a cohesive, fluent, and ergonomic API with modern storage and coordination primitives designed to feel completely natural in contemporary JavaScript and TypeScript applications.

Tencere composes the infrastructure layer rather than reinventing it:

```text
cemalloc  ──► memory / allocation
Muttafa   ──► local storage / packed data structures
daktilo   ──► durable ordered operation log / WAL
Raptiye   ──► replication / transport / cluster
Tencere   ──► database semantics + developer API
```

---

## Key Features

- **Embedded First & Server Capable**: Use in-process (`await Tencere.open("./data")`) or standalone server (`tencere serve ./data`) sharing the exact same engine and binary wire protocol.
- **Async-First & Sync Local Engine**: Asynchronous distributed-safe API (`await db.get()`), plus pure synchronous local bindings (`new TencereSync("./data")`).
- **Byte-Oriented Values**: First-class support for raw `Uint8Array` bytes, strings, numbers, booleans, and JSON without unnecessary serialization overhead.
- **Adaptive Expiration & Lifecycle**: Single-timer min-heap expiry index, sliding TTL (`sliding: true`), and atomic consume-on-read (`consume: true`).
- **Atomic Concurrency & CAS**: Optimistic concurrency with monotonic versions (`{ withVersion: true, ifVersion: N }`), document patching (`$set`, `$inc`, `$unset`, `$push`), and optimistic updates with automatic backoff retry (`db.update()`).
- **First-Class Collections**:
  - `KV` (`db.kv("sessions")`)
  - `Map` (`db.map("users")`)
  - `Set` (`db.setCollection("tags")`)
  - `Sorted` (`db.sorted("scores")`) with decoupled value storage and fluent range queries (`between()`, `above()`, `below()`)
  - `Stream` (`db.stream("events")`) with async iterator consumption (`for await (const e of stream.consume())`)
  - `Queue` (`db.queue("jobs")`) with visibility leases, delayed scheduling, retries, and worker loops
  - `Vector` (`db.vector("docs")`) with cosine, euclidean, and dot product similarity search
- **Distributed Coordination**:
  - Distributed lease locks with monotonic fencing tokens and stale-owner release protection (`db.lock()`, `db.tryLock()`)
  - Single-execution guarantee (`db.once()`)
  - Request deduplication & result replay (`db.idempotent()`)
  - Lease-based concurrency limiters (`db.semaphore()`)
  - Atomic sliding-window rate limiting (`db.rateLimit()`)
  - Cache stampede prevention and stale-while-revalidate (`db.cache()`)
  - Periodic & timestamp task scheduling with lease fencing (`db.schedule()`)
  - Reactive change streams without polling (`db.watch()`)
  - Event-driven condition predicates (`db.waitFor()`)
- **Distributed Clustering & Raft Consensus**:
  - Embedded multi-node Raft consensus powered by Raptiye
  - Leader election, terms, and heartbeat monitoring
  - Strict leader fencing protection against split-brain writes
  - Flexible write ack policies (`quorum`, `all`, `local`)
  - 128 FNV-1a hash partitions with automated cluster rebalancing
  - Both production `TCPTransport` and testing `MemoryNetwork` support
- **History, Time Travel & Rollback** (Opt-in Canonical Architecture):
  - Forward-only canonical mutation stream (`OP_RESTORE`) preserving full auditability without log truncation
  - Time travel with read-only historical database views: `const past = db.at("1h ago")`
  - Single-key historical reads: `await db.get("key", { at: "1h ago" })`, `{ atSequence: 100n }`, `{ version: 42n }`
  - Reversible rollbacks across keys, collections (`db.map("users").rollback()`), and scopes
  - Conflict-checked rollback plans with streaming diffs (`plan.changes()`, `plan.apply({ onConflict: "abort" })`)
  - Retention pinning snapshots (`db.snapshot()`) and restore plans (`db.restorePlan(snapshot)`)
  - Coordination audit logs for locks, semaphores, and leases without historical reactivation
- **AI-Workload Friendly**:
  - Semantic similarity cache (`db.semantic.set/get`)
  - Composite agent memory ranking (`db.memory.add/recall`)

---

## Installation

```bash
npm install tencere
```

---

## Quickstart

### 1. Embedded Usage (Async)

```js
import { Tencere } from "tencere";

const db = await Tencere.open("./data", { durability: "batch" });

// Basic KV
await db.set("greeting", "Hello from Tencere!");
console.log(await db.get("greeting"));

// Sorted Collection & Leaderboard
const scores = db.sorted("leaderboard");
await scores.set("ahmet", 1000);
await scores.set("elena", 1250);
console.log(await scores.top(5));

// Distributed Lock with Fencing Token
await db.lock("order:42", async ({ token }) => {
  console.log("Acquired lock with fencing token:", token);
});

// Cache with stampede protection & SWR
const user = await db.cache("user:42", { ttl: "5m", stale: "10m" }, async () => {
  return fetchUserFromAPI(42);
});

await db.close();
```

### 2. Time Travel, History & Rollback

```js
import { Tencere } from "tencere";

const db = await Tencere.open("./data", {
  history: {
    enabled: true,
    retention: "7d",
    maxVersions: 20,
    include: ["config:*", "user:*", "scores:*"],
    exclude: ["cache:*"]
  }
});

await db.set("config:title", "Initial Title");
await db.set("config:title", "Updated Title");

// 1. Point-in-time reads
const old = await db.get("config:title", { at: "10m ago" }); // 'Initial Title'

// 2. Generic read-only historical view across any collection
const past = db.at("10m ago");
console.log(await past.get("config:title")); // 'Initial Title'
console.log(await past.map("users").get("42"));
console.log(await past.sorted("scores").top(10));

// 3. Inspect history audit stream
for await (const rev of db.history("config:title")) {
  console.log(rev.operation, rev.version, rev.value);
}

// 4. Atomic forward rollback (appends OP_RESTORE mutation, never log truncation)
await db.rollback("config:title", { to: "10m ago" });

// 5. Preview & apply large rollback with conflict protection
const plan = await db.scope("tenant:corp").rollbackPlan({ to: "1h ago" });
console.log(await plan.summary());
for await (const change of plan.changes()) {
  console.log(change.key, change.action);
}
await plan.apply({ onConflict: "abort" });

await db.close();
```

### 3. First-Class TimeSeries Collection

Composable, high-throughput time-series collection backed by Tencere's SkipList `OrderedIndex` and `Daktilo` WAL, with $O(1)$-memory streaming aggregations and deterministic bucketing.

```js
const cpu = db.timeseries("cpu", {
  retention: "30d",
  tags: {
    indexed: ["host", "region", "env"]
  }
});

// 1. Ingestion: point & batch
await cpu.add(42.5);
await cpu.add(51.2, {
  at: "10m ago",
  tags: { host: "api-1", region: "eu-west" }
});

await cpu.addMany([
  [Date.now() - 3000, 40.1],
  [Date.now() - 2000, 41.8],
  { at: Date.now() - 1000, value: 43.0, tags: { host: "api-2" } }
]);

// 2. Querying latest samples
const latestPoint = await cpu.latest();
const lastTen = await cpu.latest(10);

// 3. Fluent range scans & relative time
const points = await cpu
  .where({ region: "eu-west" })
  .between("24h ago", "now")
  .values();

// 4. Deterministic epoch/UTC bucketing & streaming aggregations
const hourlyAvg = await cpu
  .where({ host: "api-1" })
  .between("24h ago", "now")
  .bucket("1h")
  .avg();
// Returns: [ { start: 1727400000000, end: 1727403600000, value: 46.85, count: 120 }, ... ]

// Supported aggregations: count, sum, min, max, avg, first, last
const overallMax = await cpu.between("7d ago", "now").max();

// 5. Realtime Change-Data-Capture (CDC) Watch
for await (const sample of cpu.where({ host: "api-1" }).watch()) {
  console.log("Realtime sample:", sample.timestamp, sample.value);
}

// 6. Retention purging & stats
const { purged } = await cpu.purgeRetention();
console.log(await cpu.stats());
```

### 4. Synchronous Embedded Engine

```js
import { TencereSync } from "tencere/sync";

const db = new TencereSync("./data");

db.set("foo", "bar");
console.log(db.get("foo")); // 'bar'

const views = db.counter("page:views");
views.inc();
console.log(views.value()); // 1

db.close();
```

### 5. Multi-Node Cluster & Raft Replication

Tencere embeds Raptiye to provide high-performance, fault-tolerant Raft consensus and state replication across multiple nodes.

- **Automatic Leader Election & Heartbeats**: Dynamically elects a cluster leader; followers deterministically replicate all state transitions.
- **Leader Fencing Protection**: Mutation operations are rejected on non-leaders (`[Cluster Fencing] Node X is not leader`), guaranteeing serializable state without split-brain anomalies.
- **Configurable Ack Policies**: Mutations can wait for consensus (`ack: "quorum"` - default, `ack: "all"`, or `ack: "local"`).
- **Transport Flexibility**: Built-in production `TCPTransport` over network sockets, and `MemoryNetwork` for in-process testing.
- **Consistent Partition Routing**: 128 FNV-1a hash partitions distributed across cluster peers.

#### Multi-Node Cluster Setup (TCP)

```js
import { Tencere } from "tencere";

// Node 1 (on host 10.0.0.1:7337)
const node1 = await Tencere.open("./data-node1", {
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

// Await leader election
const leaderId = await node1.cluster.waitForLeader();
console.log("Current cluster leader:", leaderId);

// Check cluster status
console.log(node1.cluster.status());
// Output: { enabled: true, nodeId: 1, term: 1, isLeader: true, role: 'LEADER', peers: [2, 3] }

// Writes automatically replicate across quorum:
await node1.set("app:config:theme", "dark", { ack: "quorum" });

await node1.close();
```

#### In-Memory Cluster (Testing & Verification)

```js
import { Tencere } from "tencere";
import { MemoryNetwork } from "raptiye";

const net = new MemoryNetwork();

const node1 = await Tencere.open({
  cluster: { nodeId: 1, peers: [2], network: net, election: { minTimeout: 30, maxTimeout: 60 }, heartbeatInterval: 15 }
});
const node2 = await Tencere.open({
  cluster: { nodeId: 2, peers: [1], network: net, election: { minTimeout: 30, maxTimeout: 60 }, heartbeatInterval: 15 }
});

const leaderId = (await node1.cluster.waitForLeader(1000)) || (await node2.cluster.waitForLeader(1000));
const leader = leaderId === 1 ? node1 : node2;
const follower = leaderId === 1 ? node2 : node1;

await leader.set("shared:key", "clustered-data");
// Follower state is synchronized via Raft apply
console.log(await follower.get("shared:key")); // "clustered-data"

await node1.close();
await node2.close();
```

#### Cluster-Aware Remote Client (`TencereClient.cluster`)

Connect to any seed node in the cluster. The client automatically discovers the topology, routes writes to the leader, balances reads according to `readPreference`, and transparently reconnects on leader election/failover:

```js
import { TencereClient } from "tencere";

// Connect to cluster via one or more seed addresses
const cluster = await TencereClient.cluster(["10.0.0.1:7337", "10.0.0.2:7337"], {
  readPreference: "leader", // "leader" (strong consistency) | "follower" (scale reads) | "nearest"
  maxRetries: 3,
  retryDelayMs: 100
});

// Writes are automatically routed to the current cluster leader:
await cluster.set("users:101", { name: "Alice", plan: "pro" });
await cluster.increment("stats:pageviews", 1);

// Reads respect readPreference:
const user = await cluster.get("users:101");

// Cluster pipelining to the leader:
const results = await cluster.pipeline()
  .set("batch:1", "data1")
  .get("batch:1")
  .exec();

// Clean teardown
await cluster.close();
```

#### Smart Follower Redirection & Server-Side Write Forwarding

When write mutations hit a follower node:
- **Structured Error (`ERR_NOT_LEADER`)**: Followers return `code: "ERR_NOT_LEADER"` alongside `leaderId`, `leaderAddress`, and `term`.
- **Auto-Redirect Client (`autoRedirect: true`)**: Standalone `TencereClient.connect(address, { autoRedirect: true })` automatically reconnects to the reported `leaderAddress` and retries.
- **Server-Side Write Forwarding (`forwardWrites: true`)**: Servers started with `--forward-writes` (or `new TencereServer(db, { forwardWrites: true })`) transparently proxy write frames to the leader and return the result.

#### Observability, Health Checks & Telemetry Metrics

Evaluate cluster consensus health, quorum status, and Raft replication telemetry programmatically or via client:

```js
// Evaluate comprehensive cluster health
const health = await db.cluster.health({ pingPeers: true });
console.log(health.status);    // "HEALTHY" | "DEGRADED" | "QUORUM_LOST"
console.log(health.readiness); // true (accepting traffic) | false (quorum lost)
console.log(health.quorum);    // { required: 2, reachable: 3, total: 3, hasQuorum: true }
console.log(health.nodes);     // [{ id: 1, role: "leader", status: "ONLINE", latencyMs: 0.2 }, ...]

// Inspect Raft & replication telemetry metrics
const metrics = db.cluster.metrics();
console.log(metrics.commitIndex);    // Raft commit index
console.log(metrics.lastApplied);    // State machine applied index
console.log(metrics.replicationLag);  // Entries pending replication
console.log(metrics.bytes);          // { wireBytes, payloadBytes, ... }

// Client-side cluster health check over TCP:
const clientHealth = await cluster.health();
console.log(clientHealth.status);    // "HEALTHY"
```

#### Instant Test Clusters & Chaos Testing (`createTestCluster`)

For automated integration testing and failure simulation without boilerplate, use `createTestCluster`:

```js
import { createTestCluster } from "tencere/testing"; // or from "tencere"

// Spin up a 3-node in-memory cluster instantly
const cluster = await createTestCluster({ nodes: 3 });

// Leader discovery
const leader = cluster.leader; // or await cluster.waitForLeader()
console.log("Leader Node ID:", leader.cluster.nodeId);
console.log("Follower count:", cluster.followers.length);

// Write to leader
await leader.set("config:feature_flag", true);

// Chaos Testing: Simulate network split / leader isolation
cluster.isolate(leader);

// Surviving quorum automatically elects a new leader
const newLeader = await cluster.waitForLeader();
console.log("Failover new leader:", newLeader.cluster.nodeId);

// Split-brain bipartition testing
cluster.partition([1], [2, 3]);

// Heal all network partitions
cluster.heal();

// Node lifecycle: Stop and restart individual nodes
await cluster.stopNode(2);
await cluster.startNode(2);
// or: await cluster.restartNode(2);

// Real TCP Test Cluster with Cluster Client:
const tcpCluster = await createTestCluster({ nodes: 3, tcp: true, basePort: 8700 });
const client = await tcpCluster.client();
await client.set("key", "val");

await cluster.destroy();
await tcpCluster.destroy();
```

### 6. CLI & Interactive REPL

```bash
# Launch interactive REPL (connects to remote server or runs embedded)
tencere repl
tencere repl 127.0.0.1:7337
tencere repl --local ./data

# Interactive Session Example:
# tencere 127.0.0.1:7337> SET greeting "Hello world!"
# OK
# tencere 127.0.0.1:7337> TS.ADD temperature 21.4
# OK
# tencere 127.0.0.1:7337> TS.LATEST temperature
# 1) {"timestamp":1727429443000,"sequence":"12","value":21.4}
# tencere 127.0.0.1:7337> TS.QUERY temperature "1h ago" now 1m avg
# 1) {"start":1727425800000,"end":1727425860000,"value":21.4,"count":1}

# One-shot operations via terminal:
tencere get greeting
tencere set counter 42 --ttl 1h
tencere keys
tencere ping

# Spin up a local multi-node development cluster with 1 command:
tencere cluster dev --nodes 3 --base-port 7337 --memory
# Or backed by persistent node directories:
tencere cluster dev --nodes 3 --data-dir ./.tencere-cluster

# Start standalone TCP server:
tencere serve ./data --port 7337

# Start clustered server node via CLI:
tencere serve ./data-1 --port 7337 --node-id 1 --peers 2,3 --peer-addrs 2=10.0.0.2:7337,3=10.0.0.3:7337
# Or with configuration file:
tencere serve ./data-1 --cluster-config cluster.json

# Cluster Observability & Probes (Kubernetes / Docker):
tencere cluster nodes 10.0.0.1:7337            # ASCII table: Node, Role, Term, Health, Address, Latency
tencere cluster health 10.0.0.1:7337           # Exit code 0 on healthy/ready, exit code 1 on quorum loss
tencere cluster health 10.0.0.1:7337 --json    # Structured JSON for monitoring & alerts
tencere cluster metrics 10.0.0.1:7337          # Raft replication telemetry metrics

# Check cluster consensus and topology:
tencere cluster status --host 10.0.0.1 --port 7337
tencere cluster verify ./data-1

# TimeSeries inspection & queries:
tencere timeseries list ./data
tencere timeseries info temperature ./data
tencere timeseries tail temperature ./data --count 10
tencere timeseries query temperature ./data --from "1h ago" --bucket 1m --avg

# Inspect, stats, and safe backups
tencere inspect ./data [prefix]
tencere info ./data
tencere stats ./data
tencere backup ./data ./backup
tencere restore ./backup ./restored_data
```

### 7. Agent Skill (Antigravity, Gemini, Cursor & Claude)

Tencere includes a comprehensive AI agent skill definition ([`skills/tencere/SKILL.md`](file:///Users/ahmet/projects/tencere/skills/tencere/SKILL.md)) covering architectural runbooks, storage engine options, multi-node Raft clustering, coordination primitives, and invariant verifications.

Install it into your workspace or global agent configurations with a single command:

```bash
# Install to current workspace (.agents/skills/tencere) and global (~/.gemini/config/skills/tencere):
npx tencere skill install

# Or via npm script:
npm run skill:install

# Target specific environments:
tencere skill install --workspace       # Only local workspace .agents/skills/tencere
tencere skill install --global          # Only global ~/.gemini/config/skills/tencere
tencere skill install --cursor          # Also install to .cursor/rules/tencere.md
tencere skill install --claude          # Also install to .claude/skills/tencere/SKILL.md
tencere skill install --all             # Install across all supported agent environments
tencere skill show                      # Print skill markdown to stdout
```

---

## Benchmark Results

Measured on Apple Silicon (Node.js v22):

| Category / Benchmark | Operations / Sec | Latency p50 | Latency p99 |
| :--- | :---: | :---: | :---: |
| **TencereSync GET** (in-memory) | **2.86M ops/sec** | < 0.001 ms | < 0.001 ms |
| **TencereSync SET** (in-memory) | **838.11K ops/sec** | < 0.001 ms | 0.002 ms |
| **Async GET** (16 B string) | **862.76K ops/sec** | 0.001 ms | 0.003 ms |
| **Async SET** (16 B string) | **644.74K ops/sec** | 0.001 ms | 0.004 ms |
| **Async SET** (128 B string) | **514.62K ops/sec** | 0.001 ms | 0.006 ms |
| **Async SET** (1 KiB Uint8Array) | **435.77K ops/sec** | 0.001 ms | 0.011 ms |
| **Atomic Increment** (counter) | **1.11M ops/sec** | 0.001 ms | 0.002 ms |
| **Sorted: rank lookup** | **622.49K ops/sec** | 0.001 ms | 0.010 ms |
| **Sorted: top(10)** | **964.25K ops/sec** | 0.001 ms | 0.002 ms |
| **Sorted: between().limit(20)** | **587.29K ops/sec** | 0.001 ms | 0.006 ms |
| **RateLimiter** (sliding window) | **248.39K ops/sec** | 0.003 ms | 0.006 ms |
| **Idempotent deduplication** | **451.92K ops/sec** | 0.002 ms | 0.003 ms |
| **Lock: tryLock & release** | **151.84K ops/sec** | 0.004 ms | 0.023 ms |
| **Vector search** (500 docs x 128 dims, topK 10) | **9.41K ops/sec** | 0.105 ms | 0.136 ms |
| **Daktilo WAL (async durability)** | **41.56K ops/sec** | 0.020 ms | 0.080 ms |

---

## Softscope Micro-Profiling Analysis

Softscope executed microscopic function-level AST instrumentation over **290,757** function calls across 35 files (down from 457,855 calls, -36% call reduction):

- **Zero-allocation Hot Paths**: `BinaryCodec.decode` and `BinaryCodec.encode` optimized with direct Buffer manipulation and pre-allocated singletons, cutting self-time down significantly.
- **Uncontended In-Memory Fast Paths**: In-memory mode (`daktilo: null` without pending key locks) bypasses `_withKeyLock` Promise allocations and executes mutations synchronously.
- **Bounded Min-Heap Vector Top-K**: `VectorCollection.search` uses `TopKHeap`, eliminating array sort callbacks and dropping median vector query latency to **0.105 ms**.
- **Native Promise.withResolvers()**: Replaced manual resolver closure allocations in sequential lock chains with native V8 C++ resolvers.
- **Lazy Change Event Decoding**: `_applyOperation` and mutators skip previous value fetching and decoding when no active reactive listeners exist.
- **StorageEngine Optimization**: `StorageEngine.get` zero-wrapper direct entry access reduced self-time from 41 ms to 4.3 ms.
- **Memory Recycling**: Zero memory leaks observed, with slab views safely recycled through `cemalloc` allocations.

---

## License

MIT © litepacks
