import test from "node:test";
import assert from "node:assert/strict";
import { MemoryNetwork } from "raptiye";
import { setTimeout } from "node:timers/promises";
import { Tencere } from "../../src/index.js";
import { Operation, OP_SET } from "../../src/core/operations.js";

test("Tencere Cluster - Single-node and Multi-node Raft replication with Raptiye", async () => {
  // 1. Single-node cluster
  const db1 = await Tencere.open({
    cluster: { nodeId: 1, peers: [] }
  });

  const stats = db1.stats();
  assert.equal(stats.cluster.enabled, true);
  assert.equal(stats.cluster.nodeId, 1);
  assert.deepEqual(stats.cluster.peers, []);

  await db1.set("clustered_key", "val1");
  assert.equal(await db1.get("clustered_key"), "val1");

  // Replicate manual operation
  const op = new Operation({
    op: OP_SET,
    partition: 0,
    key: "rep_manual",
    value: "rep_val",
    version: 1,
    timestamp: Date.now()
  });
  await db1._cluster.replicate(op);

  await db1.close();

  // 2. Multi-node cluster with MemoryNetwork
  const net = new MemoryNetwork();

  const node1 = await Tencere.open({
    cluster: { nodeId: 1, peers: [2], network: net, election: { minTimeout: 30, maxTimeout: 60 }, heartbeatInterval: 15 }
  });
  const node2 = await Tencere.open({
    cluster: { nodeId: 2, peers: [1], network: net, election: { minTimeout: 30, maxTimeout: 60 }, heartbeatInterval: 15 }
  });

  try {
    assert.equal(node1.stats().cluster.enabled, true);
    assert.equal(node2.stats().cluster.enabled, true);

    const leaderId = (await node1._cluster.waitForLeader(1000)) || (await node2._cluster.waitForLeader(1000));
    const leaderNode = leaderId === 1 ? node1 : node2;
    const followerNode = leaderId === 1 ? node2 : node1;

    // Propose op through leader and verify apply handler on both
    const op2 = new Operation({
      op: OP_SET,
      partition: 0,
      key: "synced_key",
      value: "synced_value",
      version: 1,
      timestamp: Date.now()
    });
    await leaderNode._cluster.replicate(op2);
    await setTimeout(150);

    assert.equal(await leaderNode.get("synced_key"), "synced_value");
    assert.equal(await followerNode.get("synced_key"), "synced_value");
  } finally {
    await node1.close();
    await node2.close();
  }
});
