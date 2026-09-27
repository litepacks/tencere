import test from "node:test";
import assert from "node:assert/strict";
import { Tencere, TencereServer, TencereClient } from "../../src/index.js";

test("Tencere Client/Server - native binary TCP protocol", async () => {
  const db = await Tencere.open();
  const port = 7887;
  const server = new TencereServer(db, { port, host: "127.0.0.1" });
  await server.start();

  let client;
  try {
    client = await TencereClient.connect(`127.0.0.1:${port}`);

    // Ping
    const pong = await client.ping();
    assert.equal(pong, "PONG");

    // Set & Get
    await client.set("server:key1", { message: "Hello via binary wire!" });
    const val = await client.get("server:key1");
    assert.deepEqual(val, { message: "Hello via binary wire!" });

    // Has
    assert.equal(await client.has("server:key1"), true);
    assert.equal(await client.has("server:nonexistent"), false);

    // Increment
    const num = await client.increment("server:counter", 5);
    assert.equal(num, 5);

    // Patch
    const patched = await client.patch("server:key1", { $set: { author: "Ahmet" } });
    assert.equal(patched.author, "Ahmet");

    // Stats
    const stats = await client.stats();
    assert.ok(stats.keys > 0);

    // Pipelining & Coalescing
    const pipeResults = await client.pipeline()
      .ping()
      .set("pipe:1", "val1")
      .set("pipe:2", "val2")
      .get("pipe:1")
      .get("pipe:2")
      .increment("pipe:counter", 10)
      .has("pipe:1")
      .delete("pipe:1")
      .exec();

    assert.equal(pipeResults[0], "PONG");
    assert.equal(pipeResults[1].ok, true);
    assert.equal(pipeResults[2].ok, true);
    assert.equal(pipeResults[3], "val1");
    assert.equal(pipeResults[4], "val2");
    assert.equal(pipeResults[5], 10);
    assert.equal(pipeResults[6], true);
    assert.equal(pipeResults[7], true);

    assert.equal(await client.has("pipe:1"), false);
    assert.equal(await client.get("pipe:2"), "val2");
  } finally {
    if (client) await client.close();
    await server.stop();
    await db.close();
  }
});
