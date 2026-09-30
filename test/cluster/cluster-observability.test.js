import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createTestCluster, Tencere, TencereClient } from "../../src/index.js";

const execFileAsync = promisify(execFile);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CLI_PATH = path.resolve(__dirname, "../../bin/tencere.js");

test("Cluster Observability - In-memory cluster health reporting (HEALTHY, DEGRADED, QUORUM_LOST)", async () => {
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

    // 1. Initial Healthy State
    const healthyStatus = await cluster.health();
    assert.equal(healthyStatus.enabled, true);
    assert.equal(healthyStatus.status, "HEALTHY");
    assert.equal(healthyStatus.readiness, true);
    assert.equal(healthyStatus.liveness, true);
    assert.equal(healthyStatus.quorum.hasQuorum, true);
    assert.equal(healthyStatus.quorum.reachable, 3);
    assert.equal(healthyStatus.quorum.required, 2);
    assert.equal(healthyStatus.quorum.total, 3);
    assert.equal(healthyStatus.nodes.length, 3);
    for (const node of healthyStatus.nodes) {
      assert.equal(node.status, "ONLINE");
    }

    // 2. Degraded State (1 follower isolated)
    const follower = cluster.followers[0];
    cluster.isolate(follower);

    const degradedStatus = await leader.cluster.health();
    assert.equal(degradedStatus.status, "DEGRADED");
    assert.equal(degradedStatus.readiness, true); // Quorum still exists (2 out of 3)
    assert.equal(degradedStatus.quorum.reachable, 2);
    assert.equal(degradedStatus.quorum.hasQuorum, true);

    const isolatedNodeReport = degradedStatus.nodes.find((n) => n.id === follower.cluster.nodeId);
    assert.ok(isolatedNodeReport);
    assert.equal(isolatedNodeReport.status, "UNREACHABLE");

    // 3. Quorum Lost State (leader isolated from all peers)
    cluster.isolate(leader);

    const lostStatus = await leader.cluster.health();
    assert.equal(lostStatus.status, "QUORUM_LOST");
    assert.equal(lostStatus.readiness, false);
    assert.equal(lostStatus.quorum.hasQuorum, false);
    assert.equal(lostStatus.quorum.reachable, 1);

    // 4. Heal and Recover
    cluster.heal();
    await cluster.waitForLeader();
    await setTimeout(80);

    const recoveredStatus = await cluster.health();
    assert.equal(recoveredStatus.status, "HEALTHY");
    assert.equal(recoveredStatus.readiness, true);
    assert.equal(recoveredStatus.quorum.hasQuorum, true);
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Observability - Raft and replication telemetry metrics", async () => {
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

    // Initial metrics
    const initialMetrics = cluster.metrics();
    assert.equal(initialMetrics.enabled, true);
    assert.equal(typeof initialMetrics.commitIndex, "number");
    assert.equal(typeof initialMetrics.lastApplied, "number");
    assert.equal(typeof initialMetrics.lastLogIndex, "number");
    assert.equal(typeof initialMetrics.replicationLag, "number");
    assert.equal(typeof initialMetrics.elections, "number");
    assert.equal(typeof initialMetrics.bytes, "object");
    assert.equal(typeof initialMetrics.bytes.wireBytes, "number");

    // Write operations to advance logs and replication
    for (let i = 1; i <= 5; i++) {
      await leader.set(`metric:key:${i}`, { val: i });
    }
    await setTimeout(80);

    const updatedMetrics = cluster.metrics();
    assert.ok(updatedMetrics.commitIndex >= 5);
    assert.ok(updatedMetrics.lastApplied >= 5);
    assert.ok(updatedMetrics.applied >= 5);
    assert.ok(updatedMetrics.bytes.wireBytes >= 0);

    // Ensure metrics are cleanly JSON-serializable (no BigInt throwing error)
    const jsonString = JSON.stringify(updatedMetrics);
    assert.ok(jsonString.includes('"commitIndex":'));
    const parsed = JSON.parse(jsonString);
    assert.equal(typeof parsed.commitIndex, "number");
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Observability - Standalone database health and metrics", async () => {
  const db = await Tencere.open();
  try {
    await db.set("local:key", "val");

    const health = await db.health();
    assert.equal(health.enabled, false);
    assert.equal(health.status, "STANDALONE");
    assert.equal(health.readiness, true);
    assert.equal(health.liveness, true);
    assert.equal(health.keys, 1);
    assert.ok(health.operations >= 1);

    const metrics = db.metrics();
    assert.equal(metrics.enabled, false);
    assert.equal(metrics.status, "STANDALONE");
    assert.ok(metrics.stats);
  } finally {
    await db.close();
  }
});

test("Cluster Observability - TCP Cluster Client health check & node latencies", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: true,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    await cluster.waitForLeader();
    const client = await cluster.client();

    // 1. Cluster Client Health Check
    const clientHealth = await client.health();
    assert.equal(clientHealth.enabled, true);
    assert.equal(clientHealth.status, "HEALTHY");
    assert.equal(clientHealth.readiness, true);
    assert.equal(clientHealth.nodes.length, 3);

    for (const node of clientHealth.nodes) {
      assert.equal(node.status, "ONLINE");
      assert.equal(typeof node.latencyMs, "number");
      assert.ok(node.latencyMs >= 0);
      assert.ok(node.address);
    }

    // 2. Single Node Client Health Check
    const leaderAddr = cluster.leader.address;
    const singleClient = await TencereClient.connect(leaderAddr);
    try {
      const nodeHealth = await singleClient.health();
      assert.equal(nodeHealth.status, "HEALTHY");
      assert.equal(nodeHealth.readiness, true);
      assert.equal(nodeHealth.liveness, true);
      assert.equal(typeof nodeHealth.latencyMs, "number");
      assert.equal(nodeHealth.address, leaderAddr);
      assert.equal(nodeHealth.cluster.enabled, true);
      assert.equal(nodeHealth.cluster.isLeader, true);
    } finally {
      await singleClient.close();
    }

    // 3. Stop 1 node and verify degraded status
    const follower = cluster.followers[0];
    const followerAddr = follower.address;
    const followerId = follower._testNodeId;

    await cluster.stopNode(followerId);
    await setTimeout(50);

    const degradedHealth = await client.health();
    assert.equal(degradedHealth.status, "DEGRADED");
    assert.equal(degradedHealth.readiness, true);
    assert.equal(degradedHealth.quorum.hasQuorum, true);
    assert.equal(degradedHealth.quorum.reachable, 2);

    const unreachableReport = degradedHealth.nodes.find((n) => n.address === followerAddr);
    assert.ok(unreachableReport);
    assert.equal(unreachableReport.status, "UNREACHABLE");

    await client.close();
  } finally {
    await cluster.destroy();
  }
});

test("Cluster Observability - CLI tencere cluster health, nodes, and metrics commands", async () => {
  const cluster = await createTestCluster({
    nodes: 3,
    tcp: true,
    clusterOptions: {
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15
    }
  });

  try {
    await cluster.waitForLeader();
    const leaderAddr = cluster.leader.address;

    // 1. CLI health check JSON
    const { stdout: healthStdout } = await execFileAsync(
      process.execPath,
      [CLI_PATH, "cluster", "health", leaderAddr, "--json"]
    );
    const healthData = JSON.parse(healthStdout);
    assert.equal(healthData.status, "HEALTHY");
    assert.equal(healthData.readiness, true);
    assert.equal(healthData.quorum.hasQuorum, true);

    // 2. CLI nodes table
    const { stdout: nodesStdout } = await execFileAsync(
      process.execPath,
      [CLI_PATH, "cluster", "nodes", leaderAddr]
    );
    assert.ok(nodesStdout.includes("Node"));
    assert.ok(nodesStdout.includes("Role"));
    assert.ok(nodesStdout.includes("Health"));
    assert.ok(nodesStdout.includes("ONLINE"));
    assert.ok(nodesStdout.includes("HEALTHY"));

    // 3. CLI metrics JSON
    const { stdout: metricsStdout } = await execFileAsync(
      process.execPath,
      [CLI_PATH, "cluster", "metrics", leaderAddr, "--json"]
    );
    const metricsData = JSON.parse(metricsStdout);
    assert.equal(metricsData.enabled, true);
    assert.equal(typeof metricsData.commitIndex, "number");
    assert.equal(typeof metricsData.replicationLag, "number");

    // 4. CLI metrics human readable
    const { stdout: metricsHuman } = await execFileAsync(
      process.execPath,
      [CLI_PATH, "cluster", "metrics", leaderAddr]
    );
    assert.ok(metricsHuman.includes("Commit Index"));
    assert.ok(metricsHuman.includes("Replication Lag"));
  } finally {
    await cluster.destroy();
  }
});
