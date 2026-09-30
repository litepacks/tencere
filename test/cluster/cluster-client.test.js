import test from "node:test";
import assert from "node:assert/strict";
import { MemoryNetwork } from "raptiye";
import { setTimeout } from "node:timers/promises";
import {
  Tencere,
  TencereServer,
  TencereClient,
  TencereClusterClient,
  NotLeaderError
} from "../../src/index.js";

test("Cluster DX - Follower ERR_NOT_LEADER structured error and metadata", async () => {
  const network = new MemoryNetwork();
  const port1 = 8551;
  const port2 = 8552;

  const node1 = await Tencere.open({
    cluster: {
      nodeId: 1,
      peers: [2],
      network,
      peerAddresses: { "1": `127.0.0.1:${port1}`, "2": `127.0.0.1:${port2}` }
    }
  });

  const node2 = await Tencere.open({
    cluster: {
      nodeId: 2,
      peers: [1],
      network,
      peerAddresses: { "1": `127.0.0.1:${port1}`, "2": `127.0.0.1:${port2}` }
    }
  });

  const server1 = new TencereServer(node1, { port: port1, host: "127.0.0.1" });
  const server2 = new TencereServer(node2, { port: port2, host: "127.0.0.1", forwardWrites: false });

  await server1.start();
  await server2.start();

  let client = null;
  try {
    const leaderId = (await node1.cluster.waitForLeader(1500)) || (await node2.cluster.waitForLeader(1500)) || 1;
    const followerPort = leaderId === 1 ? port2 : port1;
    const leaderPort = leaderId === 1 ? port1 : port2;
    const followerId = leaderId === 1 ? 2 : 1;

    client = await TencereClient.connect(`127.0.0.1:${followerPort}`);

    // Read on follower works
    const stats = await client.stats();
    assert.equal(stats.cluster.enabled, true);
    assert.equal(stats.cluster.nodeId, followerId);

    // Direct write on follower must reject with structured ERR_NOT_LEADER
    let caughtErr = null;
    try {
      await client.set("write_to_follower", "failed_val");
    } catch (err) {
      caughtErr = err;
    }

    assert.ok(caughtErr, "Write to follower must throw");
    assert.ok(caughtErr instanceof NotLeaderError, "Error must be instance of NotLeaderError");
    assert.equal(caughtErr.code, "ERR_NOT_LEADER");
    assert.equal(caughtErr.nodeId, followerId);
    assert.equal(caughtErr.leaderId, leaderId);
    assert.equal(caughtErr.leaderAddress, `127.0.0.1:${leaderPort}`);
    assert.equal(typeof caughtErr.term, "number");
  } finally {
    if (client) await client.close();
    await server1.stop();
    await server2.stop();
    await node1.close();
    await node2.close();
  }
});

test("Cluster DX - Server-side transparent write forwarding (forwardWrites: true)", async () => {
  const network = new MemoryNetwork();
  const port1 = 8561;
  const port2 = 8562;

  const node1 = await Tencere.open({
    cluster: {
      nodeId: 1,
      peers: [2],
      network,
      peerAddresses: { "1": `127.0.0.1:${port1}`, "2": `127.0.0.1:${port2}` }
    }
  });

  const node2 = await Tencere.open({
    cluster: {
      nodeId: 2,
      peers: [1],
      network,
      peerAddresses: { "1": `127.0.0.1:${port1}`, "2": `127.0.0.1:${port2}` }
    }
  });

  // Enable forwardWrites on both servers
  const server1 = new TencereServer(node1, { port: port1, host: "127.0.0.1", forwardWrites: true });
  const server2 = new TencereServer(node2, { port: port2, host: "127.0.0.1", forwardWrites: true });

  await server1.start();
  await server2.start();

  let client = null;
  try {
    const leaderId = (await node1.cluster.waitForLeader(1500)) || (await node2.cluster.waitForLeader(1500)) || 1;
    const followerPort = leaderId === 1 ? port2 : port1;
    const leaderNode = leaderId === 1 ? node1 : node2;
    const followerNode = leaderId === 1 ? node2 : node1;

    // Client connects to follower node
    client = await TencereClient.connect(`127.0.0.1:${followerPort}`);

    // Client issues write to follower - server forwards to leader transparently!
    const res = await client.set("fwd:key1", "fwd:value1");
    assert.equal(res.ok, true);

    const incRes = await client.increment("fwd:counter", 5);
    assert.equal(incRes, 5);

    // Wait for Raft replication to commit to follower
    await setTimeout(80);

    // Verify value exists on both leader and follower
    assert.equal(await leaderNode.get("fwd:key1"), "fwd:value1");
    assert.equal(await followerNode.get("fwd:key1"), "fwd:value1");
    assert.equal(await client.get("fwd:key1"), "fwd:value1");
  } finally {
    if (client) await client.close();
    await server1.stop();
    await server2.stop();
    await node1.close();
    await node2.close();
  }
});

test("Cluster DX - Standalone TencereClient autoRedirect: true", async () => {
  const network = new MemoryNetwork();
  const port1 = 8571;
  const port2 = 8572;

  const node1 = await Tencere.open({
    cluster: {
      nodeId: 1,
      peers: [2],
      network,
      peerAddresses: { "1": `127.0.0.1:${port1}`, "2": `127.0.0.1:${port2}` }
    }
  });

  const node2 = await Tencere.open({
    cluster: {
      nodeId: 2,
      peers: [1],
      network,
      peerAddresses: { "1": `127.0.0.1:${port1}`, "2": `127.0.0.1:${port2}` }
    }
  });

  const server1 = new TencereServer(node1, { port: port1, host: "127.0.0.1" });
  const server2 = new TencereServer(node2, { port: port2, host: "127.0.0.1", forwardWrites: false });

  await server1.start();
  await server2.start();

  let client = null;
  try {
    const leaderId = (await node1.cluster.waitForLeader(1500)) || (await node2.cluster.waitForLeader(1500)) || 1;
    const followerPort = leaderId === 1 ? port2 : port1;
    const leaderPort = leaderId === 1 ? port1 : port2;

    // Connect to follower with autoRedirect: true
    client = await TencereClient.connect(`127.0.0.1:${followerPort}`, { autoRedirect: true });
    assert.equal(client.port, followerPort);

    // Write should trigger ERR_NOT_LEADER, autoRedirect to leader, and succeed
    const res = await client.set("autoredirect:test", "success");
    assert.equal(res.ok, true);

    // Client's active host/port should now point to leader
    assert.equal(client.port, leaderPort);

    // Subsequent operations stay on the leader
    await client.set("autoredirect:test2", 123);
    assert.equal(await client.get("autoredirect:test2"), 123);
  } finally {
    if (client) await client.close();
    await server1.stop();
    await server2.stop();
    await node1.close();
    await node2.close();
  }
});

test("Cluster DX - TencereClient.cluster discovery, leader routing, and read preferences", async () => {
  const network = new MemoryNetwork();
  const port1 = 8581;
  const port2 = 8582;
  const port3 = 8583;

  const node1 = await Tencere.open({
    cluster: {
      nodeId: 1,
      peers: [2, 3],
      network,
      peerAddresses: {
        "1": `127.0.0.1:${port1}`,
        "2": `127.0.0.1:${port2}`,
        "3": `127.0.0.1:${port3}`
      }
    }
  });

  const node2 = await Tencere.open({
    cluster: {
      nodeId: 2,
      peers: [1, 3],
      network,
      peerAddresses: {
        "1": `127.0.0.1:${port1}`,
        "2": `127.0.0.1:${port2}`,
        "3": `127.0.0.1:${port3}`
      }
    }
  });

  const node3 = await Tencere.open({
    cluster: {
      nodeId: 3,
      peers: [1, 2],
      network,
      peerAddresses: {
        "1": `127.0.0.1:${port1}`,
        "2": `127.0.0.1:${port2}`,
        "3": `127.0.0.1:${port3}`
      }
    }
  });

  const server1 = new TencereServer(node1, { port: port1, host: "127.0.0.1" });
  const server2 = new TencereServer(node2, { port: port2, host: "127.0.0.1" });
  const server3 = new TencereServer(node3, { port: port3, host: "127.0.0.1" });

  await server1.start();
  await server2.start();
  await server3.start();

  let clusterClient = null;
  try {
    const leaderId = (await node1.cluster.waitForLeader(1500)) ||
      (await node2.cluster.waitForLeader(1500)) ||
      (await node3.cluster.waitForLeader(1500)) || 1;
    const leaderPort = leaderId === 1 ? port1 : (leaderId === 2 ? port2 : port3);
    const followerPort = leaderId === 1 ? port2 : port1;

    // Connect with a seed list pointing to follower node!
    clusterClient = await TencereClient.cluster([`127.0.0.1:${followerPort}`], {
      readPreference: "leader"
    });

    // 1. Topology discovery verification
    assert.equal(clusterClient.getLeaderAddress(), `127.0.0.1:${leaderPort}`);
    const nodes = clusterClient.getNodes();
    assert.ok(nodes.includes(`127.0.0.1:${port1}`));
    assert.ok(nodes.includes(`127.0.0.1:${port2}`));
    assert.ok(nodes.includes(`127.0.0.1:${port3}`));

    // 2. Leader write routing
    await clusterClient.set("cluster:user:1", { name: "Ahmet", role: "Dev" });
    await clusterClient.increment("cluster:metric:hits", 100);
    const patched = await clusterClient.patch("cluster:user:1", { $set: { role: "Architect" } });
    assert.equal(patched.role, "Architect");

    // 3. Cluster pipelining
    const pipeResults = await clusterClient.pipeline()
      .ping()
      .set("pipe:clustered:1", "alpha")
      .get("pipe:clustered:1")
      .increment("pipe:clustered:counter", 7)
      .exec();

    assert.equal(pipeResults[0], "PONG");
    assert.equal(pipeResults[1].ok, true);
    assert.equal(pipeResults[2], "alpha");
    assert.equal(pipeResults[3], 7);

    // Wait for Raft replication to propagate across nodes
    await setTimeout(100);

    // 4. Read preferences
    // 'leader' preference
    assert.deepEqual(await clusterClient.get("cluster:user:1"), { name: "Ahmet", role: "Architect" });

    // 'follower' preference
    clusterClient.options.readPreference = "follower";
    const valFromFollower = await clusterClient.get("cluster:user:1");
    assert.deepEqual(valFromFollower, { name: "Ahmet", role: "Architect" });

    // 'nearest' preference
    clusterClient.options.readPreference = "nearest";
    assert.equal(await clusterClient.get("pipe:clustered:1"), "alpha");

    // 5. Cluster status
    const st = clusterClient.status();
    assert.equal(st.leaderAddress, `127.0.0.1:${leaderPort}`);
    assert.equal(st.nodes.length, 3);
  } finally {
    if (clusterClient) await clusterClient.close();
    await server1.stop();
    await server2.stop();
    await server3.stop();
    await node1.close();
    await node2.close();
    await node3.close();
  }
});

test("Cluster DX - Automatic leader failover and reconnect in TencereClusterClient", async () => {
  const network = new MemoryNetwork();
  const port1 = 8591;
  const port2 = 8592;
  const port3 = 8593;

  const node1 = await Tencere.open({
    cluster: {
      nodeId: 1,
      peers: [2, 3],
      network,
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15,
      peerAddresses: {
        "1": `127.0.0.1:${port1}`,
        "2": `127.0.0.1:${port2}`,
        "3": `127.0.0.1:${port3}`
      }
    }
  });

  const node2 = await Tencere.open({
    cluster: {
      nodeId: 2,
      peers: [1, 3],
      network,
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15,
      peerAddresses: {
        "1": `127.0.0.1:${port1}`,
        "2": `127.0.0.1:${port2}`,
        "3": `127.0.0.1:${port3}`
      }
    }
  });

  const node3 = await Tencere.open({
    cluster: {
      nodeId: 3,
      peers: [1, 2],
      network,
      election: { minTimeout: 30, maxTimeout: 60 },
      heartbeatInterval: 15,
      peerAddresses: {
        "1": `127.0.0.1:${port1}`,
        "2": `127.0.0.1:${port2}`,
        "3": `127.0.0.1:${port3}`
      }
    }
  });

  const server1 = new TencereServer(node1, { port: port1, host: "127.0.0.1" });
  const server2 = new TencereServer(node2, { port: port2, host: "127.0.0.1" });
  const server3 = new TencereServer(node3, { port: port3, host: "127.0.0.1" });

  await server1.start();
  await server2.start();
  await server3.start();

  const servers = { 1: server1, 2: server2, 3: server3 };
  const nodes = { 1: node1, 2: node2, 3: node3 };

  let clusterClient = null;
  try {
    const leaderId = (await node1.cluster.waitForLeader(1500)) ||
      (await node2.cluster.waitForLeader(1500)) ||
      (await node3.cluster.waitForLeader(1500)) || 1;

    clusterClient = await TencereClient.cluster(
      [`127.0.0.1:${port1}`, `127.0.0.1:${port2}`, `127.0.0.1:${port3}`],
      { maxRetries: 5, retryDelayMs: 100 }
    );

    await clusterClient.set("cluster:failover:key", "before_crash");
    assert.equal(await clusterClient.get("cluster:failover:key"), "before_crash");

    // Stop current leader node and server
    const oldLeaderNode = nodes[leaderId];
    const oldLeaderServer = servers[leaderId];
    await oldLeaderServer.stop();
    await oldLeaderNode.close();

    // The remaining 2 nodes will elect a new leader.
    // Issuing a write through clusterClient should automatically trigger failover and succeed!
    await clusterClient.set("cluster:failover:key", "after_failover_success");
    const updated = await clusterClient.get("cluster:failover:key");
    assert.equal(updated, "after_failover_success");
  } finally {
    if (clusterClient) await clusterClient.close();
    for (const s of Object.values(servers)) {
      await s.stop().catch(() => {});
    }
    for (const n of Object.values(nodes)) {
      await n.close().catch(() => {});
    }
  }
});

