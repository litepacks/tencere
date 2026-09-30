import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { spawn } from "node:child_process";
import { createTestCluster, TestCluster, PartitionableNetwork, TencereClient } from "../../src/index.js";

test("TestCluster - In-memory cluster lifecycle, leader discovery, and replication", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    assert.equal(cluster.nodeCount, 3);
    const leader = await cluster.waitForLeader();
    assert.ok(leader);
    assert.equal(cluster.followers.length, 2);

    // Write to leader
    await leader.set("msg:1", { text: "hello cluster" });

    // Wait for quorum replication
    await setTimeout(80);

    // Verify followers synchronized state
    for (const follower of cluster.followers) {
      const val = await follower.get("msg:1");
      assert.deepEqual(val, { text: "hello cluster" });
    }

    // In-memory client proxy
    const client = await cluster.client();
    await client.set("msg:2", "via-client");
    const retrieved = await client.get("msg:2");
    assert.equal(retrieved, "via-client");

    // Wait for quorum replication to reach followers
    await setTimeout(80);

    const followerRead = await cluster.client({ readPreference: "follower" });
    const fromFollower = await followerRead.get("msg:2");
    assert.equal(fromFollower, "via-client");
  } finally {
    await cluster.destroy();
  }
});

test("TestCluster - Network chaos testing (isolate, leader re-election, heal, catchup)", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: false,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    const originalLeader = await cluster.waitForLeader();
    const originalLeaderId = originalLeader.cluster.nodeId;
    assert.ok(originalLeaderId);

    // Write before partition
    await originalLeader.set("pre_partition", 100);
    await setTimeout(60);

    // Isolate the leader (simulates network severance or partition)
    cluster.isolate(originalLeader);

    // Surviving quorum elects a new leader
    const newLeader = await cluster.waitForLeader(2000);
    assert.ok(newLeader, "A new leader should be elected by the surviving quorum");
    assert.notEqual(newLeader.cluster.nodeId, originalLeaderId, "New leader must be a different node");

    // Write to the new leader during partition
    await newLeader.set("partition_key", "written_during_split");
    await setTimeout(60);

    // Verify the isolated old leader does NOT have this write yet
    const isolatedVal = await originalLeader.get("partition_key");
    assert.equal(isolatedVal, undefined);

    // Heal the network partition
    cluster.heal();

    // Poll for catch-up replication
    let healedVal = undefined;
    const catchupStart = Date.now();
    while (Date.now() - catchupStart < 2000) {
      healedVal = await originalLeader.get("partition_key");
      if (healedVal === "written_during_split") break;
      await setTimeout(20);
    }
    assert.equal(healedVal, "written_during_split");
  } finally {
    await cluster.destroy();
  }
});

test("TestCluster - Node failure and restart simulation (stopNode, startNode, restartNode)", async () => {
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
    const follower = cluster.followers[0];
    const followerId = follower.cluster.nodeId;

    // Stop follower node
    await cluster.stopNode(followerId);
    assert.equal(cluster.node(followerId), null);
    assert.equal(cluster.nodes.size, 2);

    // Cluster should still operate with quorum of 2
    await leader.set("quorum_key", "survives_single_failure");

    // Restart the stopped node
    const restarted = await cluster.startNode(followerId);
    assert.ok(restarted);
    assert.equal(cluster.nodes.size, 3);
    assert.equal(cluster.node(followerId), restarted);

    // Test convenience restartNode
    const nodeAfterRestart = await cluster.restartNode(followerId);
    assert.ok(nodeAfterRestart);
    assert.equal(cluster.node(followerId), nodeAfterRestart);
  } finally {
    await cluster.destroy();
  }
});

test("TestCluster - Real TCP cluster with TencereClusterClient and Write Forwarding", async () => {
  const basePort = 9370;
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: true,
    basePort,
    forwardWrites: true,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    assert.equal(cluster.isTcp, true);
    assert.equal(cluster.addresses.length, 3);
    assert.equal(cluster.addresses[0], `127.0.0.1:${basePort}`);

    const leader = await cluster.waitForLeader(3000);
    assert.ok(leader);

    // Connect real TencereClusterClient to TCP cluster
    const client = await cluster.client({ minElectionTimeout: 50 });
    assert.ok(client);

    // Write through cluster client
    await client.set("tcp_cluster_key", { test: 42, active: true });
    const readBack = await client.get("tcp_cluster_key");
    assert.deepEqual(readBack, { test: 42, active: true });

    // Atomic increment over cluster
    const counter = await client.increment("tcp_counter", 5);
    assert.equal(counter, 5);

    // Follower direct connection with automatic write forwarding
    const follower = cluster.followers[0];
    const followerPort = cluster.ports.get(follower.cluster.nodeId);
    const followerClient = await TencereClient.connect(`127.0.0.1:${followerPort}`);

    try {
      // Direct write to follower TCP server is forwarded to leader and succeeds
      const fwdResult = await followerClient.set("forwarded_key", "forwarded_val");
      assert.ok(fwdResult && (fwdResult.ok === true || fwdResult === true));

      // Verify value was committed
      await setTimeout(60);
      const val = await client.get("forwarded_key");
      assert.equal(val, "forwarded_val");
    } finally {
      await followerClient.close();
    }
  } finally {
    await cluster.destroy();
  }
});

test("TestCluster - Bipartition split-brain network testing (partition and heal)", async () => {
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
    const leaderId = leader.cluster.nodeId;
    const [f1, f2] = cluster.followers;

    // Partition minority (leaderId) vs majority ([f1, f2])
    cluster.partition([leaderId], [f1.cluster.nodeId, f2.cluster.nodeId]);

    // Majority elects a new leader among themselves with a higher term
    let majorityLeader = null;
    const startWait = Date.now();
    while (Date.now() - startWait < 1500) {
      for (const f of [f1, f2]) {
        if (f.cluster.isLeader()) {
          majorityLeader = f;
          break;
        }
      }
      if (majorityLeader) break;
      await setTimeout(20);
    }
    assert.ok(majorityLeader, "Surviving majority must elect a new leader");
    assert.notEqual(majorityLeader.cluster.nodeId, leaderId);

    // Majority can commit quorum writes
    await majorityLeader.set("majority_val", "committed", { ack: "quorum" });
    await setTimeout(60);

    // Heal partition
    cluster.heal();

    // Reconnected node catches up
    let caughtUp = false;
    const start = Date.now();
    while (Date.now() - start < 2000) {
      if ((await leader.get("majority_val")) === "committed") {
        caughtUp = true;
        break;
      }
      await setTimeout(20);
    }
    assert.equal(caughtUp, true, "Minority node must catch up to latest state after healing");
  } finally {
    await cluster.destroy();
  }
});

test("CLI - tencere cluster dev spins up multi-node local cluster and shuts down cleanly", async () => {
  const binPath = new URL("../../bin/tencere.js", import.meta.url).pathname;
  const proc = spawn(process.execPath, [binPath, "cluster", "dev", "--nodes", "3", "--base-port", "9480", "--memory"], {
    stdio: ["pipe", "pipe", "pipe"]
  });

  try {
    let output = "";
    await new Promise((resolve, reject) => {
      const timeout = globalThis.setTimeout(() => {
        reject(new Error(`Timed out waiting for dev cluster output: ${output}`));
      }, 8000);

      proc.stdout.on("data", (chunk) => {
        output += chunk.toString();
        if (output.includes("Press Ctrl+C to gracefully stop the cluster.")) {
          globalThis.clearTimeout(timeout);
          resolve();
        }
      });
      proc.stderr.on("data", (chunk) => {
        output += chunk.toString();
      });
      proc.on("error", reject);
      proc.on("exit", (code) => {
        if (code !== 0 && code !== null) reject(new Error(`Process exited early with code ${code}`));
      });
    });

    assert.ok(output.includes("Node 1: 127.0.0.1:9480"));
    assert.ok(output.includes("Node 2: 127.0.0.1:9481"));
    assert.ok(output.includes("Node 3: 127.0.0.1:9482"));

    // Connect via cluster client to dev cluster
    const client = await TencereClient.cluster([
      "127.0.0.1:9480",
      "127.0.0.1:9481",
      "127.0.0.1:9482"
    ]);
    try {
      await client.set("dev_test_key", "dev_val");
      const val = await client.get("dev_test_key");
      assert.equal(val, "dev_val");
    } finally {
      await client.close();
    }
  } finally {
    proc.kill("SIGINT");
    await new Promise((r) => {
      if (proc.killed || proc.exitCode !== null) return r();
      proc.on("exit", r);
    });
  }
});


