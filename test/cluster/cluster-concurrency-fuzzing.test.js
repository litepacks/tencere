import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { createTestCluster } from "../../src/index.js";
import { VersionMismatchError } from "../../src/errors.js";

test("Cluster Concurrency - High-contention concurrent increments with zero lost updates", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    const leader = await cluster.waitForLeader();
    assert.ok(leader);

    // Initial counter setup
    await leader.set("global:counter", 0, { ack: "quorum" });

    // 25 concurrent workers performing 10 increments each = 250 total
    const numWorkers = 25;
    const incsPerWorker = 10;
    const workers = [];

    for (let w = 0; w < numWorkers; w++) {
      workers.push((async () => {
        for (let i = 0; i < incsPerWorker; i++) {
          await leader.increment("global:counter", 1, { ack: "quorum" });
        }
      })());
    }

    await Promise.all(workers);

    // Allow Raft replication to commit and settle across followers
    await setTimeout(100);

    const expectedTotal = numWorkers * incsPerWorker;
    const leaderVal = await leader.get("global:counter");
    assert.equal(leaderVal, expectedTotal, `Leader value should be ${expectedTotal}`);

    // Verify all followers reached exact same state without lost updates
    for (const follower of cluster.followers) {
      const followerVal = await follower.get("global:counter");
      assert.equal(followerVal, expectedTotal, `Follower ${follower.cluster?.nodeId} value should be ${expectedTotal}`);
    }

    // Cryptographic stateHash parity verification across all 3 nodes
    const hashes = [];
    for (const node of cluster.nodes.values()) {
      const h = await node.debug.stateHash();
      hashes.push(h);
    }
    assert.equal(hashes[0], hashes[1], "Node 1 and Node 2 stateHash must match bit-for-bit");
    assert.equal(hashes[1], hashes[2], "Node 2 and Node 3 stateHash must match bit-for-bit");
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Concurrency - Multi-collection replicated mutations (KV, Map, Set, Sorted, Queue)", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    const leader = await cluster.waitForLeader();
    assert.ok(leader);

    // 1. KV Operations: Patch, Conditional, Delete
    await leader.set("app:profile", { name: "Tencere", version: 1, active: true }, { ack: "quorum" });
    await leader.patch("app:profile", {
      $set: { tier: "enterprise" },
      $inc: { version: 1 }
    });

    // 2. Map Collection Operations
    const map = leader.map("tenant:settings");
    await map.set("theme", "midnight");
    await map.set("timeout", 5000);
    await map.delete("timeout");

    // 3. Set Collection Operations
    const set = leader.setCollection("cluster:nodes:active");
    await set.add("node:1");
    await set.add("node:2");
    await set.add("node:3");
    await set.delete("node:3");

    // 4. Sorted Collection Operations
    const sorted = leader.sorted("gaming:leaderboard");
    await sorted.set("player_alpha", 1500);
    await sorted.set("player_beta", 2200);
    await sorted.incr("player_alpha", 800); // 1500 + 800 = 2300 (now 1st)

    // 5. Queue Collection Operations
    const queue = leader.queue("notifications");
    await queue.push({ user: "alice", type: "email" });
    await queue.push({ user: "bob", type: "sms" });

    // Allow replication propagation
    await setTimeout(120);

    // Verify all followers have identical synchronized state across every collection
    for (const follower of cluster.followers) {
      // Check KV
      const profile = await follower.get("app:profile");
      assert.deepEqual(profile, { name: "Tencere", version: 2, active: true, tier: "enterprise" });

      // Check Map
      const fMap = follower.map("tenant:settings");
      assert.equal(await fMap.get("theme"), "midnight");
      assert.equal(await fMap.has("timeout"), false);

      // Check Set
      const fSet = follower.setCollection("cluster:nodes:active");
      assert.equal(await fSet.has("node:1"), true);
      assert.equal(await fSet.has("node:2"), true);
      assert.equal(await fSet.has("node:3"), false);

      // Check Sorted
      const fSorted = follower.sorted("gaming:leaderboard");
      const top1 = await fSorted.top(1);
      assert.equal(top1[0].member, "player_alpha");
      assert.equal(top1[0].score, 2300);

      // Check Queue
      const fQueue = follower.queue("notifications");
      assert.equal((await fQueue.size()).ready, 2);
    }

    // Cryptographic stateHash parity
    const node1Hash = await cluster.nodes.get(1).debug.stateHash();
    const node2Hash = await cluster.nodes.get(2).debug.stateHash();
    const node3Hash = await cluster.nodes.get(3).debug.stateHash();

    assert.equal(node1Hash, node2Hash);
    assert.equal(node2Hash, node3Hash);
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Concurrency - Replicated TTL lifecycle and deterministic expiration", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    const leader = await cluster.waitForLeader();
    assert.ok(leader);

    // Set key with short TTL
    await leader.set("ephemeral:token", "secret123", { ttl: 300, ack: "quorum" });

    // Allow replication tick
    await setTimeout(50);

    // Verify key exists across all nodes
    assert.equal(await leader.get("ephemeral:token"), "secret123");
    for (const follower of cluster.followers) {
      assert.equal(await follower.get("ephemeral:token"), "secret123");
    }

    // Wait past expiration
    await setTimeout(320);

    // Assert key has expired and returns undefined on leader and followers
    assert.equal(await leader.get("ephemeral:token"), undefined);
    for (const follower of cluster.followers) {
      assert.equal(await follower.get("ephemeral:token"), undefined);
    }
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Concurrency - Conflict-free CAS and Conditional Writes across replicas", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    const leader = await cluster.waitForLeader();
    assert.ok(leader);

    // Initial value
    await leader.set("account:balance", 100, { ack: "quorum" });
    const v1Res = await leader.get("account:balance", { withVersion: true });
    assert.ok(v1Res.version > 0);

    // 1. Successful CAS with matching version
    await leader.set("account:balance", 200, { ifVersion: v1Res.version, ack: "quorum" });
    const v2Res = await leader.get("account:balance", { withVersion: true });
    assert.equal(v2Res.value, 200);
    assert.ok(v2Res.version > v1Res.version);

    // 2. Stale CAS with previous version must throw VersionMismatchError
    await assert.rejects(
      async () => {
        await leader.set("account:balance", 300, { ifVersion: v1Res.version, ack: "quorum" });
      },
      (err) => err instanceof VersionMismatchError
    );

    // 3. ifVersion: 0 (insert if not exists) on existing key must fail
    await assert.rejects(
      async () => {
        await leader.set("account:balance", 999, { ifVersion: 0, ack: "quorum" });
      },
      (err) => err instanceof VersionMismatchError
    );

    // 4. ifVersion: 0 on non-existent key must succeed
    await leader.set("account:brand_new", 500, { ifVersion: 0, ack: "quorum" });

    await setTimeout(80);

    // Verify all followers match the expected post-CAS state
    for (const follower of cluster.followers) {
      const followerEntry = await follower.get("account:balance", { withVersion: true });
      assert.equal(followerEntry.value, 200);
      assert.equal(BigInt(followerEntry.version), BigInt(v2Res.version));

      const newEntry = await follower.get("account:brand_new");
      assert.equal(newEntry, 500);
    }
  } finally {
    await cluster.destroy();
  }
});
