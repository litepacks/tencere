import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { MemoryNetwork } from "raptiye";
import { Tencere } from "../../src/index.js";
import { calculateStateHash } from "../../src/diagnostics/state-hash.js";
import { verifyInvariants } from "../../src/diagnostics/invariants.js";

/**
 * Custom partitionable in-memory network for testing partitions, isolation, and healing.
 */
class PartitionableNetwork extends MemoryNetwork {
  constructor() {
    super();
    this.isolatedNodes = new Set();
  }

  isolate(nodeId) {
    this.isolatedNodes.add(Number(nodeId));
  }

  heal(nodeId) {
    if (nodeId !== undefined) {
      this.isolatedNodes.delete(Number(nodeId));
    } else {
      this.isolatedNodes.clear();
    }
  }

  deliver(from, to, buffers) {
    const fromId = Number(from);
    const toId = Number(to);
    if (this.isolatedNodes.has(fromId) || this.isolatedNodes.has(toId)) {
      // Packet dropped due to partition / network isolation
      return;
    }
    super.deliver(from, to, buffers);
  }
}

test("Cluster Resilience - Network partition, leader failover, fencing, and catch-up", async () => {
  const net = new PartitionableNetwork();

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

  const nodes = new Map([
    [1, node1],
    [2, node2],
    [3, node3]
  ]);

  try {
    // 1. Wait for initial leader
    const initialLeaderId =
      (await node1._cluster.waitForLeader(1500)) ||
      (await node2._cluster.waitForLeader(1500)) ||
      (await node3._cluster.waitForLeader(1500));

    assert.ok(initialLeaderId, "Initial cluster leader must be elected");
    const initialLeader = nodes.get(initialLeaderId);
    const otherNodes = [1, 2, 3].filter((id) => id !== initialLeaderId).map((id) => nodes.get(id));

    // 2. Successful quorum write on initial leader
    await initialLeader.set("stable_key", "value_1", { ack: "quorum" });
    await setTimeout(100);

    for (const n of [node1, node2, node3]) {
      assert.equal(await n.get("stable_key"), "value_1");
    }

    // 3. Follower write rejection test (Fencing)
    const follower = otherNodes[0];
    await assert.rejects(
      async () => {
        await follower.set("follower_key", "fail_val");
      },
      (err) => err.message.includes("Cluster Fencing") || err.message.includes("not leader"),
      "Follower must reject direct writes with Cluster Fencing error"
    );

    // 4. Partition leader away from the other two nodes
    net.isolate(initialLeaderId);

    // 5. The remaining 2 nodes have quorum (2 out of 3), elect new leader
    const survivingNodes = otherNodes;
    let newLeaderId = null;
    const startWait = Date.now();
    while (Date.now() - startWait < 2000) {
      for (const sn of survivingNodes) {
        if (sn._cluster.isLeader()) {
          newLeaderId = sn._cluster._nodeId;
          break;
        }
      }
      if (newLeaderId) break;
      await setTimeout(30);
    }

    assert.ok(newLeaderId, "Surviving quorum must elect a new leader");
    assert.notEqual(newLeaderId, initialLeaderId, "New leader must be one of surviving nodes");
    const newLeader = nodes.get(newLeaderId);

    // 6. Quorum write on new leader succeeds
    await newLeader.set("during_partition", "value_2", { ack: "quorum" });
    await setTimeout(100);

    const survivingOther = survivingNodes.find((n) => n._cluster._nodeId !== newLeaderId);
    assert.equal(await newLeader.get("during_partition"), "value_2");
    assert.equal(await survivingOther.get("during_partition"), "value_2");

    // 7. Write to isolated old leader must fail (cannot reach quorum or fenced)
    await assert.rejects(
      async () => {
        await initialLeader.set("stale_key", "stale_val", { ack: "quorum", timeoutMs: 500 });
      },
      "Isolated stale leader must fail quorum write"
    );

    // 8. Heal partition
    net.heal();
    await setTimeout(300);

    // 9. Old leader must receive updates and catch up to latest state
    let caughtUp = false;
    const catchupStart = Date.now();
    while (Date.now() - catchupStart < 2000) {
      const val = await initialLeader.get("during_partition");
      if (val === "value_2") {
        caughtUp = true;
        break;
      }
      await setTimeout(40);
    }
    assert.ok(caughtUp, "Reconnected node must catch up to latest replicated state");

    // 10. Perform another write to verify healthy cluster operation after heal
    const currentLeaderId =
      (await node1._cluster.waitForLeader(1000)) ||
      (await node2._cluster.waitForLeader(1000)) ||
      (await node3._cluster.waitForLeader(1000));
    const currentLeader = nodes.get(currentLeaderId);
    await currentLeader.set("post_heal_key", "value_3", { ack: "quorum" });
    await setTimeout(100);

    for (const n of [node1, node2, node3]) {
      assert.equal(await n.get("post_heal_key"), "value_3");
    }

    // 11. Deterministic State Hash Equality across all nodes
    const hash1 = await node1.debug.stateHash();
    const hash2 = await node2.debug.stateHash();
    const hash3 = await node3.debug.stateHash();

    assert.equal(hash1, hash2, "Node 1 and Node 2 must have identical deterministic state hash");
    assert.equal(hash2, hash3, "Node 2 and Node 3 must have identical deterministic state hash");

    // 12. Invariant assertions across all nodes
    for (const n of [node1, node2, node3]) {
      const report = await verifyInvariants(n);
      assert.equal(report.valid, true, `Node ${n._cluster._nodeId} invariants must be fully valid`);
    }
  } finally {
    await node1.close();
    await node2.close();
    await node3.close();
  }
});
