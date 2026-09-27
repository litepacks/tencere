/**
 * Comprehensive benchmark target for Softscope function-level micro-profiling.
 * Exercises all primitives, collections, coordination algorithms, and sync/cluster components.
 */

import { setTimeout } from "node:timers/promises";
import { Tencere } from "../src/index.js";
import { TencereSync } from "../src/sync/index.js";
import { TencereServer } from "../src/core/server.js";
import { TencereClient } from "../src/client/index.js";
import { BinaryCodec } from "../src/core/binary-codec.js";
import { ExpiryManager, parseDuration } from "../src/core/expiry-wheel.js";
import { OrderedIndex } from "../src/core/ordered-index.js";
import { Operation, OP_SET, OP_DEL, OP_INCR, OP_PATCH } from "../src/core/operations.js";
import * as Errors from "../src/errors.js";

async function main() {
  // 1. BinaryCodec & Operations
  const sampleBuf = BinaryCodec.encode({ hello: "world", count: 42, active: true, list: [1, 2, 3] });
  BinaryCodec.decode(sampleBuf);
  const sampleOp = new Operation({ op: OP_SET, partition: 0, key: "foo", value: "bar", version: 1, timestamp: Date.now() });
  const encodedOp = sampleOp.encode();
  Operation.decode(encodedOp);

  // 2. ExpiryManager & OrderedIndex
  parseDuration("5s");
  const exp = new ExpiryManager({ onExpire: () => {} });
  exp.schedule("tmp:1", 1000);
  exp.touch("tmp:1");
  exp.cancel("tmp:1");
  exp.close();

  const idx = new OrderedIndex();
  for (let i = 0; i < 50; i++) {
    idx.insert(i * 10, `m:${i}`);
  }
  idx.rank("m:25");
  idx.score("m:25");
  idx.top(5);
  idx.bottom(5);
  idx.rangeByScore(100, 300);
  idx.delete("m:10");

  // 3. Errors taxonomy
  new Errors.TencereError("err");
  new Errors.VersionMismatchError(1, 2);
  new Errors.LockAcquireError("k", "busy");
  new Errors.LockStaleOwnerError("k", 1);
  new Errors.TimeoutError("timeout");
  new Errors.KeyNotFoundError("k");
  new Errors.DatabaseClosedError();
  new Errors.ClusterNotAvailableError("none");

  // 4. TencereSync (synchronous local operations and collections)
  const syncDb = new TencereSync();
  syncDb.setMany({ "s:1": 100, "s:2": 200 });
  syncDb.getMany(["s:1", "s:2"]);
  syncDb.has("s:1");
  syncDb.increment("s:1", 5);
  syncDb.patch("s:1", { $set: { extra: true }, $inc: { count: 1 } });
  syncDb.delete("s:2");

  const sMap = syncDb.map("s_map");
  sMap.set("m1", "val1");
  sMap.get("m1");
  sMap.has("m1");
  sMap.size();
  sMap.keys();
  sMap.values();
  sMap.entries();
  sMap.delete("m1");

  const sSet = syncDb.setCollection("s_set");
  sSet.add("item1");
  sSet.has("item1");
  sSet.size();
  sSet.members();
  sSet.delete("item1");

  const sSorted = syncDb.sorted("s_sorted");
  sSorted.set("p1", 100);
  sSorted.set("p2", 200);
  sSorted.score("p1");
  sSorted.rank("p1");
  sSorted.incr("p1", 10);
  sSorted.top(2);
  sSorted.bottom(2);
  sSorted.between(50, 250);
  sSorted.above(100);
  sSorted.below(200);
  sSorted.size();
  sSorted.delete("p1");

  const sCounter = syncDb.counter("s_cnt");
  sCounter.inc();
  sCounter.add(5);
  sCounter.dec(2);
  sCounter.value();
  sCounter.reset();

  syncDb.stats();
  syncDb.clear();
  syncDb.close();

  // 5. Clustered Tencere Database (Raft replication via MemoryTransport)
  const db = await Tencere.open({
    cluster: { nodeId: 1, peers: [] }
  });

  // KV Operations
  for (let i = 0; i < 500; i++) {
    await db.set(`k:${i}`, { title: "doc", tags: ["a", "b"] }, { ttl: "1h" });
    await db.get(`k:${i}`);
  }
  await db.has("k:1");
  await db.getMany(["k:1", "k:2"]);
  await db.setMany({ "m:1": 1, "m:2": 2 });
  await db.increment("counter:hits", 1);
  await db.patch("k:1", { $set: { verified: true } });
  await db.update("counter:hits", (v) => (v || 0) + 1);
  await db.delete("k:2");

  // Counter Primitive
  const cnt = db.counter("global_reqs");
  await cnt.inc();
  await cnt.add(10);
  await cnt.dec(3);
  await cnt.value();
  await cnt.reset();

  // KV Collection
  const kv = db.kv("session");
  await kv.set("tok_1", { uid: 123 });
  await kv.get("tok_1");
  await kv.has("tok_1");
  await kv.setMany({ tok_2: { uid: 456 } });
  await kv.getMany(["tok_1", "tok_2"]);
  await kv.keys();
  await kv.delete("tok_1");
  await kv.clear();

  // Map Collection
  const userMap = db.map("users");
  await userMap.set("u:1", { name: "Alice" });
  await userMap.get("u:1");
  await userMap.has("u:1");
  await userMap.size();
  await userMap.keys();
  await userMap.values();
  await userMap.entries();
  await userMap.delete("u:1");

  // Set Collection
  const tags = db.setCollection("tags");
  await tags.add("fast");
  await tags.add("reliable");
  await tags.has("fast");
  await tags.size();
  await tags.members();
  await tags.delete("fast");

  // Sorted Collection
  const scores = db.sorted("leaderboard");
  for (let i = 0; i < 50; i++) {
    await scores.set(`player:${i}`, (i * 31) % 1000, { score: (i * 31) % 1000, value: { id: i } });
  }
  await scores.score("player:5");
  await scores.getValue("player:5");
  await scores.rank("player:5");
  await scores.incr("player:5", 15);
  await scores.top(5);
  await scores.bottom(5);
  await scores.size();
  await scores.count();
  await scores.between(100, 500).asc().limit(10).entries();
  await scores.above(200).take(5);
  await scores.below(400).desc().entries();
  await scores.delete("player:5");

  // Queue Collection
  const queue = db.queue("tasks");
  await queue.push({ job: "email" });
  await queue.size();
  const qWorker = queue.worker(async (_job) => {}, { pollIntervalMs: 10 });
  await setTimeout(20);
  await qWorker.stop();
  await queue.close();

  // Stream Collection
  const stream = db.stream("logs");
  await stream.append({ msg: "system start" });
  await stream.append({ msg: "heartbeat" });
  await stream.head();
  for await (const _entry of stream.consume({ tail: false })) {
    break;
  }

  // Vector Search
  const docs = db.vector("vecs", { dimensions: 8 });
  for (let i = 0; i < 20; i++) {
    const v = new Float32Array(8).fill(0.1 * (i % 5));
    await docs.set(`d:${i}`, { vector: v, value: { i } });
  }
  const q = new Float32Array(8).fill(0.2);
  await docs.search(q, { topK: 3 });
  await docs.get("d:0");
  await docs.delete("d:0");

  // SemanticCache & AgentMemory
  const emb = new Float32Array(128).fill(0.5);
  await db.semantic.set("What is Tencere?", { answer: "An in-memory store" }, { embedding: emb });
  await db.semantic.get({ embedding: emb });
  await db.semantic.has("What is Tencere?");
  await db.semantic.count();
  await db.semantic.prune();
  await db.semantic.delete("What is Tencere?");

  await db.memory.add("agent_1", { content: "User prefers dark mode", embedding: emb });
  await db.memory.recall("agent_1", { embedding: emb, topK: 1 });

  // Scopes
  const scope = db.scope("tenant:acme");
  await scope.set("config", { tier: "enterprise" });
  await scope.get("config");
  await scope.has("config");
  await scope.delete("config");
  const sub = scope.subscope("billing");
  await sub.set("card", "ok");
  await sub.delete("card");

  // Coordination: Locks, Semaphores, RateLimiter, Once, Idempotent, Cache, Watch, WaitFor
  const lock = await db.tryLock("res_1");
  if (lock) await lock.release();
  await db.rateLimit("user_ip", { limit: 50, window: "10s" });

  const sem = db.semaphore("conn_pool", { permits: 2 });
  const semOwner = await sem.tryAcquire();
  if (semOwner) await sem.release(semOwner);
  await sem.run(async () => "ok");

  await db.once("init_task", async () => "done");
  await db.idempotent("idem_key", async () => "computed");
  await db.cache("cached_fn", async () => ({ fresh: true }), { ttl: "1m" });

  const waitPromise = db.waitFor("status_flag", (v) => v === "ready", { timeout: 1000 });
  await db.set("status_flag", "ready");
  await waitPromise;

  // Scheduler
  const task1 = db.schedule("cleanup").every("1h").run(() => {});
  task1.stop();
  const task2 = db.schedule("backup").at(Date.now() + 10000).run(() => {});
  task2.stop();

  // Engine stats & checkpoint
  db.stats();
  await db.checkpoint();

  // 6. History, Time Travel & Rollback subsystem profiling
  const histDb = await Tencere.open({ history: { enabled: true, retention: "1h", maxVersions: 20 } });
  await histDb.set("hist:title", "Initial");
  await histDb.set("hist:title", "Updated");
  await histDb.get("hist:title", { atSequence: 1n });
  const past = histDb.at("10m ago");
  await past.get("hist:title");
  const snap = await histDb.snapshot();
  await snap.get("hist:title");
  await snap.close();
  for await (const rev of histDb.history("hist:title")) {}
  await histDb.rollback("hist:title", { sequence: 1n });
  const plan = await histDb.rollbackPlan({ to: "10m ago" });
  await plan.summary();
  for await (const ch of plan.changes()) {}
  histDb.lockHistory("hist:lock");
  await histDb.close();

  // 7. TCP Client/Server Integration
  const server = new TencereServer(db, { port: 19876 });
  await server.start();

  const client = await TencereClient.connect("127.0.0.1:19876");
  await client.set("tcp:k1", "tcp_val");
  await client.get("tcp:k1");
  await client.delete("tcp:k1");
  await client.close();
  await server.close();

  await db.close();
}

main().catch(console.error);
