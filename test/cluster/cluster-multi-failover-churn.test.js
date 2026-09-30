import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { createTestCluster, TencereClient } from "../../src/index.js";
import { NotLeaderError } from "../../src/errors.js";

test("Cluster Failover - Cascading multi-generation leader failovers with log catchup and stateHash parity", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    // Phase 1: Generation 1 (initial leader)
    const leader1 = await cluster.waitForLeader();
    assert.ok(leader1);
    const leader1Id = leader1.cluster.nodeId;

    await leader1.set("generation:1", "val-gen-1", { ack: "quorum" });
    await setTimeout(60);

    // Phase 2: Kill Leader 1 -> Leader 2 elected (Term advances)
    await cluster.stopNode(leader1Id);

    const leader2 = await cluster.waitForLeader(3000);
    assert.ok(leader2);
    const leader2Id = leader2.cluster.nodeId;
    assert.notEqual(leader2Id, leader1Id);

    await leader2.set("generation:2", "val-gen-2", { ack: "quorum" });
    await setTimeout(60);

    // Phase 3: Restart Node 1 -> Node 1 must catch up generation:2 from Node 2
    await cluster.startNode(leader1Id);
    await setTimeout(80);

    const caughtUpNode1 = cluster.nodes.get(leader1Id);
    assert.equal(await caughtUpNode1.get("generation:1"), "val-gen-1");
    assert.equal(await caughtUpNode1.get("generation:2"), "val-gen-2");

    // Phase 4: Kill Leader 2 -> Leader 3 (or Node 1) elected
    await cluster.stopNode(leader2Id);

    const leader3 = await cluster.waitForLeader(3000);
    assert.ok(leader3);
    const leader3Id = leader3.cluster.nodeId;
    assert.notEqual(leader3Id, leader2Id);

    await leader3.set("generation:3", "val-gen-3", { ack: "quorum" });
    await setTimeout(60);

    // Phase 5: Bring back Node 2 -> All 3 nodes alive, verify complete convergence
    await cluster.startNode(leader2Id);
    await setTimeout(100);

    for (const [id, node] of cluster.nodes.entries()) {
      assert.equal(await node.get("generation:1"), "val-gen-1", `Node ${id} missing gen 1`);
      assert.equal(await node.get("generation:2"), "val-gen-2", `Node ${id} missing gen 2`);
      assert.equal(await node.get("generation:3"), "val-gen-3", `Node ${id} missing gen 3`);
    }

    // Cryptographic stateHash parity across all nodes after churn
    const h1 = await cluster.nodes.get(1).debug.stateHash();
    const h2 = await cluster.nodes.get(2).debug.stateHash();
    const h3 = await cluster.nodes.get(3).debug.stateHash();

    assert.equal(h1, h2, "Node 1 and Node 2 stateHash must match after cascading failovers");
    assert.equal(h2, h3, "Node 2 and Node 3 stateHash must match after cascading failovers");
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Coordination - Distributed locks, once, idempotent, and rate limiting in cluster", async () => {
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

    // 1. Distributed Lock with Monotonic Fencing Token
    let token1 = 0;
    let lockExecuted = false;

    await leader.lock("order:9001", async ({ token }) => {
      token1 = token;
      lockExecuted = true;
      assert.ok(token > 0);

      // Concurrent lock on same key should fail / return null for tryLock
      const concurrentTry = await leader.tryLock("order:9001");
      assert.equal(concurrentTry, null, "Concurrent tryLock must return null while lock is held");
    });

    assert.equal(lockExecuted, true);

    // Second lock on same key should get strictly greater token
    let token2 = 0;
    await leader.lock("order:9001", async ({ token }) => {
      token2 = token;
      assert.ok(token2 > token1, "Second token must be strictly greater than first token");
    });

    // 2. Single-Execution Guarantee (db.once)
    let executionCounter = 0;
    const oncePromises = [];
    for (let i = 0; i < 5; i++) {
      oncePromises.push(
        leader.once("task:nightly-sync:2026-09-28", async () => {
          executionCounter++;
          return "sync-done";
        })
      );
    }
    const onceResults = await Promise.all(oncePromises);
    assert.equal(executionCounter, 1, "db.once must execute exactly once among concurrent callers");
    for (const res of onceResults) {
      assert.equal(res, "sync-done");
    }

    // 3. Request Deduplication (db.idempotent)
    let idempotentRuns = 0;
    const runPayment = async () => {
      return leader.idempotent("idemp:charge:tx99", async () => {
        idempotentRuns++;
        return { charged: 50, currency: "USD" };
      });
    };

    const [charge1, charge2, charge3] = await Promise.all([runPayment(), runPayment(), runPayment()]);
    assert.equal(idempotentRuns, 1, "db.idempotent must only execute once");
    assert.deepEqual(charge1, { charged: 50, currency: "USD" });
    assert.deepEqual(charge2, { charged: 50, currency: "USD" });
    assert.deepEqual(charge3, { charged: 50, currency: "USD" });

    // 4. Atomic Rate Limiting (db.rateLimit)
    const rateLimitResults = [];
    for (let i = 0; i < 4; i++) {
      const rl = await leader.rateLimit("ip:192.168.1.100", { limit: 3, windowMs: 10000 });
      rateLimitResults.push(rl);
    }

    assert.equal(rateLimitResults[0].allowed, true);
    assert.equal(rateLimitResults[1].allowed, true);
    assert.equal(rateLimitResults[2].allowed, true);
    assert.equal(rateLimitResults[3].allowed, false, "4th request should exceed limit of 3");
    assert.equal(rateLimitResults[3].remaining, 0);
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Failover - TCP ClusterClient automatic failover and seamless write recovery", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: true,
    clusterOptions: {
      election: { minTimeout: 40, maxTimeout: 80 },
      heartbeatInterval: 15
    }
  });

  try {
    const leader1 = await cluster.waitForLeader();
    assert.ok(leader1);
    const leader1Id = leader1._testNodeId;

    // Connect via cluster client
    const client = await TencereClient.cluster(cluster.addresses, {
      maxRetries: 10,
      retryDelayMs: 60
    });

    // 1. Initial write to current leader
    await client.set("failover:key:1", "val-init");
    const initVal = await client.get("failover:key:1");
    assert.equal(initVal, "val-init");

    // 2. Abruptly stop the current leader
    await cluster.stopNode(leader1Id);

    // Wait for new leader election in cluster
    await cluster.waitForLeader(4000);

    // 3. Write through cluster client -> client automatically detects new leader and succeeds
    await client.set("failover:key:2", "val-post-failover");
    const postVal = await client.get("failover:key:2");
    assert.equal(postVal, "val-post-failover");

    // 4. Client pipeline continues to work seamlessly on new leader
    const pipeResults = await client.pipeline()
      .set("failover:p1", "hello")
      .set("failover:p2", "world")
      .exec();

    assert.equal(pipeResults.length, 2);
    assert.equal(pipeResults[0].ok, true);
    assert.equal(pipeResults[1].ok, true);

    const val1 = await client.get("failover:p1");
    const val2 = await client.get("failover:p2");
    assert.equal(val1, "hello");
    assert.equal(val2, "world");

    await client.close();
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Partition - Minority partition write rejection and healing state reconciliation", async () => {
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
    const leaderId = leader.cluster.nodeId;

    // Partition isolatedNode (a follower) away from majority: [isolated] vs [others]
    const follower = cluster.followers[0];
    const isolatedId = follower.cluster.nodeId;

    cluster.isolate(isolatedId);

    // 1. Writes on majority leader still succeed (2 out of 3 >= quorum 2)
    await leader.set("majority:key", "majority-val", { ack: "quorum" });
    assert.equal(await leader.get("majority:key"), "majority-val");

    // 2. Direct write on isolated follower should fail with NotLeaderError
    await assert.rejects(
      async () => {
        await follower.set("minority:key", "minority-val");
      },
      (err) => err instanceof NotLeaderError
    );

    // 3. Heal partition
    cluster.heal();
    await setTimeout(100);

    // 4. Follower reconciles and receives the majority write
    assert.equal(await follower.get("majority:key"), "majority-val");

    // StateHash parity check
    const h1 = await cluster.nodes.get(1).debug.stateHash();
    const h2 = await cluster.nodes.get(2).debug.stateHash();
    const h3 = await cluster.nodes.get(3).debug.stateHash();

    assert.equal(h1, h2);
    assert.equal(h2, h3);
  } finally {
    await cluster.destroy();
  }
});
