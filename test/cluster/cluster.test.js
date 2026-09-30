import test from "node:test";
import assert from "node:assert/strict";
import { MemoryNetwork } from "raptiye";
import { setTimeout } from "node:timers/promises";
import { Tencere } from "../../src/index.js";
import { Operation, OP_SET } from "../../src/core/operations.js";

test("Tencere Cluster - Single-node and Multi-node Raft replication with Raptiye", async () => {
  // 0. Cluster disabled check
  const standaloneDb = await Tencere.open();
  assert.equal(standaloneDb.cluster, null);
  await standaloneDb.close();

  // 1. Single-node cluster with public db.cluster API
  const db1 = await Tencere.open({
    cluster: { nodeId: 1, peers: [] }
  });

  assert.ok(db1.cluster, "db.cluster public property must be defined");
  assert.equal(db1.cluster.enabled, true);
  assert.equal(db1.cluster.nodeId, 1);
  assert.deepEqual(db1.cluster.peers, []);
  assert.equal(db1.cluster.isLeader(), true);
  assert.equal(db1.cluster.leaderId, 1);
  assert.equal(typeof db1.cluster.term, "number");
  assert.equal(typeof db1.cluster.role, "string");

  const stats = db1.stats();
  assert.equal(stats.cluster.enabled, true);
  assert.equal(stats.cluster.nodeId, 1);
  assert.deepEqual(stats.cluster.peers, []);

  await db1.set("clustered_key", "val1");
  assert.equal(await db1.get("clustered_key"), "val1");

  // Replicate manual operation using public db.cluster
  const op = new Operation({
    op: OP_SET,
    partition: 0,
    key: "rep_manual",
    value: "rep_val",
    version: 1,
    timestamp: Date.now()
  });
  await db1.cluster.replicate(op);

  await db1.close();

  // 2. Multi-node cluster with MemoryNetwork and public db.cluster events
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
    assert.equal(node1.cluster.nodeId, 1);
    assert.equal(node2.cluster.nodeId, 2);

    let leaderEventReceived = null;
    node1.cluster.on("leader", (evt) => {
      leaderEventReceived = evt;
    });

    const leaderId = (await node1.cluster.waitForLeader(1000)) || (await node2.cluster.waitForLeader(1000));
    assert.ok(leaderId === 1 || leaderId === 2);
    assert.ok(leaderEventReceived, "leader event should be emitted on waitForLeader");

    const leaderNode = leaderId === 1 ? node1 : node2;
    const followerNode = leaderId === 1 ? node2 : node1;

    assert.equal(leaderNode.cluster.isLeader(), true);
    assert.equal(followerNode.cluster.isLeader(), false);
    assert.equal(leaderNode.cluster.leaderId, leaderId);
    assert.equal(leaderNode.cluster.status().leaderId, leaderId);
    assert.equal(typeof leaderNode.cluster.term, "number");
    assert.equal(typeof leaderNode.cluster.role, "string");

    // Propose op through leader and verify apply handler on both
    const op2 = new Operation({
      op: OP_SET,
      partition: 0,
      key: "synced_key",
      value: "synced_value",
      version: 1,
      timestamp: Date.now()
    });
    await leaderNode.cluster.replicate(op2);
    await setTimeout(150);

    assert.equal(await leaderNode.get("synced_key"), "synced_value");
    assert.equal(await followerNode.get("synced_key"), "synced_value");
  } finally {
    await node1.close();
    await node2.close();
  }
});

test("Tencere Cluster - CLI flag and configuration parsing", async () => {
  const { parseClusterConfig } = await import("../../bin/tencere.js");

  const flags = {
    "node-id": "1",
    peers: "2, 3",
    "peer-addrs": "2=10.0.0.2:7337, 3=10.0.0.3:7337",
    port: "7337",
    host: "10.0.0.1",
    "cluster-election": "120:250",
    "cluster-heartbeat": "40"
  };

  const config = await parseClusterConfig(flags);
  assert.deepEqual(config, {
    nodeId: 1,
    peers: [2, 3],
    peerAddresses: {
      "2": "10.0.0.2:7337",
      "3": "10.0.0.3:7337"
    },
    port: 7337,
    host: "10.0.0.1",
    election: {
      minTimeout: 120,
      maxTimeout: 250
    },
    heartbeatInterval: 40
  });
});
