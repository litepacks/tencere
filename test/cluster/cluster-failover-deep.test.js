import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { createTestCluster, TencereClient } from "../../src/index.js";

test("Cluster Failover Deep - In-flight client writes during abrupt leader crash and transparent reconnect", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: true,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    const leader1 = await cluster.waitForLeader();
    assert.ok(leader1);
    const leader1Id = leader1._testNodeId;

    // Connect cluster client with resilient retry settings
    const client = await TencereClient.cluster(cluster.addresses, {
      maxRetries: 20,
      retryDelayMs: 40
    });

    // 1. Warm-up writes
    await client.set("warmup:1", "ready");
    assert.equal(await client.get("warmup:1"), "ready");

    // 2. Fire writes and abruptly crash the current leader midway
    const keysToWrite = 20;
    const writePromises = [];

    for (let i = 0; i < keysToWrite; i++) {
      writePromises.push(client.set(`inflight:${i}`, `value-${i}`));
      // After initiating half the writes, abruptly kill the leader
      if (i === 10) {
        await cluster.stopNode(leader1Id);
      }
    }

    // Wait for all writes to resolve via transparent reconnect / retry
    const writeResults = await Promise.allSettled(writePromises);

    // At least the post-failover writes or successfully retried writes must succeed
    const successfulWrites = writeResults.filter((r) => r.status === "fulfilled").length;
    assert.ok(
      successfulWrites > 0,
      `Expected successful writes during/after failover, got ${successfulWrites}/${keysToWrite}`
    );

    // Wait for new leader election if not already done
    const newLeader = await cluster.waitForLeader(3000);
    assert.ok(newLeader);
    assert.notEqual(newLeader._testNodeId, leader1Id);

    // 3. New writes through the client on the new leader must succeed seamlessly
    await client.set("post_crash:confirmed", "recovered");
    const confirmedVal = await client.get("post_crash:confirmed");
    assert.equal(confirmedVal, "recovered");

    await client.close();
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Failover Deep - 5-Node asymmetric split-brain quorum safety and healing convergence", async () => {
  const cluster = await createTestCluster({
    nodes: 5,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    const initialLeader = await cluster.waitForLeader();
    assert.ok(initialLeader);
    const initialLeaderId = initialLeader.cluster.nodeId;

    // Stable write on initial 5-node cluster
    await initialLeader.set("split:initial", "baseline", { ack: "quorum" });
    await setTimeout(60);

    for (const [id, node] of cluster.nodes.entries()) {
      assert.equal(await node.get("split:initial"), "baseline", `Node ${id} missing baseline`);
    }

    // Determine 2 nodes for minority and 3 nodes for majority
    const minorityNodes = [1, 2];
    const majorityNodes = [3, 4, 5];

    // Partition network into minority {1, 2} and majority {3, 4, 5}
    cluster.partition(minorityNodes, majorityNodes);

    // If initial leader is in minority, it cannot commit with quorum (needs 3 of 5)
    // Wait for majority partition to establish leadership
    let majorityLeader = null;
    const startWait = Date.now();
    while (Date.now() - startWait < 4000) {
      for (const mId of majorityNodes) {
        const node = cluster.node(mId);
        if (node && node.cluster.isLeader()) {
          majorityLeader = node;
          break;
        }
      }
      if (majorityLeader) break;
      await setTimeout(30);
    }

    assert.ok(majorityLeader, "Majority partition {3, 4, 5} must elect a leader");
    assert.ok(majorityNodes.includes(majorityLeader.cluster.nodeId));

    // Write on the majority partition with quorum ack (succeeds with 3 nodes)
    await majorityLeader.set("split:majority_key", "written_during_partition", { ack: "quorum" });
    await setTimeout(60);

    // Verify all majority nodes have the write
    for (const mId of majorityNodes) {
      const node = cluster.node(mId);
      assert.equal(await node.get("split:majority_key"), "written_during_partition");
    }

    // Verify minority nodes do NOT have the write during partition
    for (const minId of minorityNodes) {
      const node = cluster.node(minId);
      assert.equal(await node.get("split:majority_key"), undefined);
    }

    // Now heal the partition
    cluster.heal();

    // Allow catchup replication across the healed cluster
    let healed = false;
    const catchupStart = Date.now();
    while (Date.now() - catchupStart < 4000) {
      const v1 = await cluster.node(1).get("split:majority_key");
      const v2 = await cluster.node(2).get("split:majority_key");
      if (v1 === "written_during_partition" && v2 === "written_during_partition") {
        healed = true;
        break;
      }
      await setTimeout(30);
    }

    assert.ok(healed, "Minority nodes must catch up state after partition heals");

    // Cryptographic stateHash parity across ALL 5 nodes
    const h1 = await cluster.node(1).debug.stateHash();
    const h2 = await cluster.node(2).debug.stateHash();
    const h3 = await cluster.node(3).debug.stateHash();
    const h4 = await cluster.node(4).debug.stateHash();
    const h5 = await cluster.node(5).debug.stateHash();

    assert.equal(h1, h2, "Node 1 and 2 stateHash must match");
    assert.equal(h2, h3, "Node 2 and 3 stateHash must match");
    assert.equal(h3, h4, "Node 3 and 4 stateHash must match");
    assert.equal(h4, h5, "Node 4 and 5 stateHash must match");
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Failover Deep - Rolling node restarts under write load without data loss", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    let leader = await cluster.waitForLeader();
    assert.ok(leader);

    // Perform sequential rolling restart of each node while writing
    for (let step = 1; step <= 3; step++) {
      // Find a follower to restart first
      const followers = cluster.followers;
      const targetNodeId = followers.length > 0 ? followers[0].cluster.nodeId : leader.cluster.nodeId;

      await cluster.stopNode(targetNodeId);

      // Current leader writes key during node downtime
      leader = cluster.leader || (await cluster.waitForLeader(3000));
      await leader.set(`rolling:step:${step}`, `val-${step}`, { ack: "quorum" });

      // Restart target node
      await cluster.startNode(targetNodeId);
      await setTimeout(80);

      // Verify restarted node caught up
      const restartedNode = cluster.node(targetNodeId);
      assert.equal(await restartedNode.get(`rolling:step:${step}`), `val-${step}`);
    }

    // Now restart the leader itself
    const currentLeaderId = leader.cluster.nodeId;
    await cluster.stopNode(currentLeaderId);

    const newLeader = await cluster.waitForLeader(3000);
    assert.ok(newLeader);
    assert.notEqual(newLeader.cluster.nodeId, currentLeaderId);

    await newLeader.set("rolling:leader_restarted", "leader_failover_success", { ack: "quorum" });

    // Restart old leader
    await cluster.startNode(currentLeaderId);
    await setTimeout(100);

    // Verify all 3 nodes have all rolling writes
    for (const [id, node] of cluster.nodes.entries()) {
      assert.equal(await node.get("rolling:step:1"), "val-1", `Node ${id} missing step 1`);
      assert.equal(await node.get("rolling:step:2"), "val-2", `Node ${id} missing step 2`);
      assert.equal(await node.get("rolling:step:3"), "val-3", `Node ${id} missing step 3`);
      assert.equal(await node.get("rolling:leader_restarted"), "leader_failover_success", `Node ${id} missing leader restart val`);
    }

    // Verify stateHash equality
    const h1 = await cluster.node(1).debug.stateHash();
    const h2 = await cluster.node(2).debug.stateHash();
    const h3 = await cluster.node(3).debug.stateHash();
    assert.equal(h1, h2);
    assert.equal(h2, h3);
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Failover Deep - Multi-collection replication and catchup after failover", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    const leader1 = await cluster.waitForLeader();
    assert.ok(leader1);
    const leader1Id = leader1.cluster.nodeId;

    // 1. Initial collection writes on Leader 1
    const map1 = leader1.map("users");
    const set1 = leader1.setCollection("tags");
    const sorted1 = leader1.sorted("scores");
    const ts1 = leader1.timeseries("telemetry");
    const queue1 = leader1.queue("tasks");

    await map1.set("user:1", { name: "Alice", active: true }, { ack: "quorum" });
    await set1.add("vip");
    await set1.add("admin");
    await sorted1.incr("player_a", 100, { ack: "quorum" });
    await ts1.add(42.5, { tags: { sensor: "temp" }, ack: "quorum" });
    await queue1.push({ task: "job-1" }, { ack: "quorum" });

    await setTimeout(60);

    // 2. Kill Leader 1
    await cluster.stopNode(leader1Id);

    // 3. New leader elected
    const leader2 = await cluster.waitForLeader(3000);
    assert.ok(leader2);
    assert.notEqual(leader2.cluster.nodeId, leader1Id);

    // 4. Perform further mutations on Leader 2
    const map2 = leader2.map("users");
    const set2 = leader2.setCollection("tags");
    const sorted2 = leader2.sorted("scores");
    const ts2 = leader2.timeseries("telemetry");
    const queue2 = leader2.queue("tasks");

    await map2.set("user:2", { name: "Bob", active: false }, { ack: "quorum" });
    await set2.add("moderator", { ack: "quorum" });
    await sorted2.incr("player_a", 50, { ack: "quorum" });
    await sorted2.incr("player_b", 200, { ack: "quorum" });
    await ts2.add(43.1, { tags: { sensor: "temp" }, ack: "quorum" });
    await queue2.push({ task: "job-2" }, { ack: "quorum" });

    await setTimeout(60);

    // 5. Restart Node 1 and allow complete catchup
    await cluster.startNode(leader1Id);
    await setTimeout(100);

    // 6. Verify Node 1 has all initial and post-failover collection updates
    const caughtUpNode1 = cluster.node(leader1Id);
    const mapNode1 = caughtUpNode1.map("users");
    const setNode1 = caughtUpNode1.setCollection("tags");
    const sortedNode1 = caughtUpNode1.sorted("scores");
    const tsNode1 = caughtUpNode1.timeseries("telemetry");
    const queueNode1 = caughtUpNode1.queue("tasks");

    // Map verification
    assert.deepEqual(await mapNode1.get("user:1"), { name: "Alice", active: true });
    assert.deepEqual(await mapNode1.get("user:2"), { name: "Bob", active: false });

    // Set verification
    assert.equal(await setNode1.has("vip"), true);
    assert.equal(await setNode1.has("admin"), true);
    assert.equal(await setNode1.has("moderator"), true);

    // Sorted verification
    assert.equal(await sortedNode1.score("player_a"), 150);
    assert.equal(await sortedNode1.score("player_b"), 200);

    // TimeSeries verification
    const points = await tsNode1.values();
    assert.equal(points.length, 2);

    // Queue verification
    const qSize = await queueNode1.size();
    assert.equal(qSize.ready, 2);

    // StateHash parity across all 3 nodes
    const h1 = await cluster.node(1).debug.stateHash();
    const h2 = await cluster.node(2).debug.stateHash();
    const h3 = await cluster.node(3).debug.stateHash();

    assert.equal(h1, h2, "Node 1 and Node 2 stateHash must match");
    assert.equal(h2, h3, "Node 2 and Node 3 stateHash must match");
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Failover Deep - Rapid leader flapping and monotonic term advancement", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 25, maxTimeout: 50 },
      heartbeatInterval: 10
    }
  });

  try {
    const leader1 = await cluster.waitForLeader();
    const term1 = leader1.cluster.status().term;
    const leader1Id = leader1.cluster.nodeId;

    // Write on leader 1
    await leader1.set("flapping:term1", "term1_val", { ack: "quorum" });

    // Kill leader 1
    await cluster.stopNode(leader1Id);

    // Leader 2 elected -> Term must strictly advance
    const leader2 = await cluster.waitForLeader(3000);
    const term2 = leader2.cluster.status().term;
    const leader2Id = leader2.cluster.nodeId;
    assert.ok(term2 > term1, `Term 2 (${term2}) must be > Term 1 (${term1})`);

    await leader2.set("flapping:term2", "term2_val", { ack: "quorum" });

    // Revive Node 1 before killing leader 2 so quorum remains intact
    await cluster.startNode(leader1Id);
    await setTimeout(60);

    // Kill leader 2
    await cluster.stopNode(leader2Id);

    // Leader 3 elected -> Term must advance again
    const leader3 = await cluster.waitForLeader(3000);
    const term3 = leader3.cluster.status().term;
    assert.ok(term3 > term2, `Term 3 (${term3}) must be > Term 2 (${term2})`);

    await leader3.set("flapping:term3", "term3_val", { ack: "quorum" });

    // Revive Node 2
    await cluster.startNode(leader2Id);
    await setTimeout(100);

    // Verify all active nodes hold all 3 terms' data
    for (const [id, node] of cluster.nodes.entries()) {
      assert.equal(await node.get("flapping:term1"), "term1_val", `Node ${id} missing term 1`);
      assert.equal(await node.get("flapping:term2"), "term2_val", `Node ${id} missing term 2`);
      assert.equal(await node.get("flapping:term3"), "term3_val", `Node ${id} missing term 3`);
    }

    // Parity check
    const h1 = await cluster.node(1).debug.stateHash();
    const h2 = await cluster.node(2).debug.stateHash();
    const h3 = await cluster.node(3).debug.stateHash();
    assert.equal(h1, h2);
    assert.equal(h2, h3);
  } finally {
    await cluster.destroy();
  }
});
