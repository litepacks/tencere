import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { MemoryNetwork } from "raptiye";
import { Tencere } from "../../src/index.js";
import { calculateStateHash } from "../../src/diagnostics/state-hash.js";
import { verifyInvariants } from "../../src/diagnostics/invariants.js";

test("Cluster TimeSeries - Replicated ordering, same-timestamp sequence, and invariant parity", async () => {
  const net = new MemoryNetwork();

  const clusterConfig = (id, peers) => ({
    cluster: {
      nodeId: id,
      peers,
      network: net,
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  const node1 = await Tencere.open(clusterConfig(1, [2, 3]));
  const node2 = await Tencere.open(clusterConfig(2, [1, 3]));
  const node3 = await Tencere.open(clusterConfig(3, [1, 2]));

  const nodes = [node1, node2, node3];

  try {
    const leaderId =
      (await node1.cluster.waitForLeader(1500)) ||
      (await node2.cluster.waitForLeader(1500)) ||
      (await node3.cluster.waitForLeader(1500));

    assert.ok(leaderId, "Cluster leader must be elected");
    const leader = nodes.find((n) => n.cluster.nodeId === leaderId);
    const replicas = nodes.filter((n) => n.cluster.nodeId !== leaderId);

    const ts = leader.timeseries("metrics");

    // 1. Ingest ordered, same-timestamp, and late samples
    const baseTs = 1700000000000;
    await ts.add(10.5, { at: baseTs, tags: { metric: "cpu" }, ack: "quorum" });
    await ts.add(12.0, { at: baseTs, tags: { metric: "cpu" }, ack: "quorum" }); // Same timestamp, sequential sequence
    await ts.add(15.2, { at: baseTs + 1000, tags: { metric: "cpu" }, ack: "quorum" });
    await ts.add(9.8, { at: baseTs + 500, tags: { metric: "cpu" }, ack: "quorum" }); // Late-arriving sample
    await ts.add(18.0, { at: baseTs + 2000, tags: { metric: "cpu" }, ack: "quorum" });

    await setTimeout(200);

    // 2. Query range across leader and all replicas
    const leaderPoints = await ts.between(baseTs, baseTs + 2000).values();
    assert.equal(leaderPoints.length, 5);

    for (const replica of replicas) {
      const repTs = replica.timeseries("metrics");
      const repPoints = await repTs.between(baseTs, baseTs + 2000).values();
      assert.equal(repPoints.length, 5, `Replica ${replica.cluster.nodeId} must have all 5 replicated points`);

      // Verify exact point order and values match
      for (let i = 0; i < leaderPoints.length; i++) {
        assert.equal(repPoints[i].timestamp, leaderPoints[i].timestamp);
        assert.equal(repPoints[i].value, leaderPoints[i].value);
        assert.equal(repPoints[i].sequence, leaderPoints[i].sequence);
      }
    }

    // 3. Verify monotonic (timestamp, sequence) invariants across all nodes
    for (const n of nodes) {
      const report = await verifyInvariants(n);
      assert.equal(report.valid, true, `Node ${n.cluster.nodeId} invariants must hold`);
      assert.equal(report.stats.timeSeriesPoints, 5);
    }

    // 4. Deterministic State Hash Equality
    const h1 = await node1.debug.stateHash();
    const h2 = await node2.debug.stateHash();
    const h3 = await node3.debug.stateHash();

    assert.equal(h1, h2, "TimeSeries state hash must match between node 1 and 2");
    assert.equal(h2, h3, "TimeSeries state hash must match between node 2 and 3");
  } finally {
    await node1.close();
    await node2.close();
    await node3.close();
  }
});
