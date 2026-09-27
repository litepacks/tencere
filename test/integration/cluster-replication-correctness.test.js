import test from "node:test";
import assert from "node:assert/strict";
import { MemoryNetwork } from "raptiye";
import { setTimeout } from "node:timers/promises";
import { Tencere } from "../../src/index.js";

test("3-Node Raptiye Cluster: Canonical Replication, Quorum Ack, StateHash Parity, and Rollback", async () => {
  const network = new MemoryNetwork();

  // 1. Initialize 3-node cluster
  const node1 = await Tencere.open({
    history: { enabled: true },
    cluster: { nodeId: 1, peers: [2, 3], network }
  });

  const node2 = await Tencere.open({
    history: { enabled: true },
    cluster: { nodeId: 2, peers: [1, 3], network }
  });

  const node3 = await Tencere.open({
    history: { enabled: true },
    cluster: { nodeId: 3, peers: [1, 2], network }
  });

  try {
    // Wait for Raft election and leader stabilization
    const leaderId = (await node1._cluster.waitForLeader(1000)) || (await node2._cluster.waitForLeader(1000)) || 1;
    const nodes = { 1: node1, 2: node2, 3: node3 };
    const leaderNode = nodes[leaderId] || node1;
    const replicas = [node1, node2, node3].filter((n) => n !== leaderNode);

    // 2. Client Write with Quorum Ack through elected leader
    await leaderNode.set("site:domain", "tencere.dev", {
      durability: "batch",
      ack: "quorum"
    });

    await leaderNode.increment("site:visitors", 42, { ack: "quorum" });

    // Allow network message propagation to replicas
    await setTimeout(60);

    // Verify replicas applied exact canonical mutation
    for (const r of replicas) {
      assert.equal(await r.get("site:domain"), "tencere.dev");
      assert.equal(await r.get("site:visitors"), 42);
    }

    // 3. Cluster Rollback: Owner resolves rollback, replicates forward RESTORE
    await leaderNode.set("config:theme", "light", { ack: "quorum" });
    await leaderNode.set("config:theme", "dark", { ack: "quorum" });
    await leaderNode.rollback("config:theme");
    await setTimeout(60);

    assert.equal(await leaderNode.get("config:theme"), "light");
    for (const r of replicas) {
      assert.equal(await r.get("config:theme"), "light");
    }

    assert.equal(await node1.get("config:theme"), "light");
    assert.equal(await node2.get("config:theme"), "light");
    assert.equal(await node3.get("config:theme"), "light");

    // 4. Deterministic State Hash Verification across all 3 nodes
    const hash1 = await node1.debug.stateHash();
    const hash2 = await node2.debug.stateHash();
    const hash3 = await node3.debug.stateHash();

    assert.ok(hash1 && hash1.length === 64);
    assert.equal(
      hash2,
      hash1,
      `State hash mismatch between Node 1 and Node 2!\nNode1: ${hash1}\nNode2: ${hash2}`
    );
    assert.equal(
      hash3,
      hash1,
      `State hash mismatch between Node 1 and Node 3!\nNode1: ${hash1}\nNode3: ${hash3}`
    );

    // 5. Cluster Observability
    const stats1 = node1.stats();
    assert.equal(stats1.cluster.enabled, true);
    assert.equal(stats1.cluster.nodeId, 1);
    assert.deepEqual(stats1.cluster.peers, [2, 3]);
  } finally {
    await node1.close();
    await node2.close();
    await node3.close();
  }
});
