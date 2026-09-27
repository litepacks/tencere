import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { Tencere } from "../../src/index.js";

test("SemanticCache - collision-free hashing, TTL expiration, and management", async () => {
  const db = await Tencere.open();
  const cache = db.semantic;

  // 1. Basic get & set
  await cache.set("What is the capital of Turkey?", "Ankara", {
    embedding: [0.8, 0.6, 0.0]
  });

  const hit = await cache.get("Turkey's capital?", {
    embedding: [0.81, 0.59, 0.0],
    threshold: 0.95
  });
  assert.equal(hit, "Ankara");

  const miss = await cache.get("French cuisine?", {
    embedding: [0.0, 1.0, 0.0],
    threshold: 0.95
  });
  assert.equal(miss, null);

  // 2. Collision resistance for long shared prefixes
  const prompt1 = "Summarize the key events in the battle of Waterloo in 1815";
  const prompt2 = "Summarize the key events in the battle of Hastings in 1066";

  await cache.set(prompt1, "Waterloo response", {
    embedding: [1.0, 0.0, 0.0]
  });
  await cache.set(prompt2, "Hastings response", {
    embedding: [0.0, 1.0, 0.0]
  });

  assert.equal(await cache.has(prompt1), true);
  assert.equal(await cache.has(prompt2), true);

  const r1 = await cache.get(prompt1, { embedding: [1.0, 0.0, 0.0], threshold: 0.99 });
  const r2 = await cache.get(prompt2, { embedding: [0.0, 1.0, 0.0], threshold: 0.99 });
  assert.equal(r1, "Waterloo response");
  assert.equal(r2, "Hastings response");

  // 3. TTL Expiration
  await cache.set("Temporary query", "Ephemeral answer", {
    embedding: [0.5, 0.5, 0.5],
    ttl: "50ms"
  });

  const immediate = await cache.get("Temporary query", {
    embedding: [0.5, 0.5, 0.5]
  });
  assert.equal(immediate, "Ephemeral answer");

  await setTimeout(70);

  const expired = await cache.get("Temporary query", {
    embedding: [0.5, 0.5, 0.5]
  });
  assert.equal(expired, null, "Expired semantic cache entry should return null");
  assert.equal(await cache.has("Temporary query"), false, "has() should return false for expired prompt");

  // 4. Details option
  await cache.set("Detailed query", "Detailed answer", {
    embedding: [0.7, 0.7, 0.0],
    metadata: { source: "docs", tokens: 42 }
  });

  const detailed = await cache.get("Detailed query", {
    embedding: [0.7, 0.7, 0.0],
    details: true
  });
  assert.ok(detailed);
  assert.equal(detailed.response, "Detailed answer");
  assert.ok(detailed.score >= 0.99);
  assert.equal(detailed.metadata.source, "docs");
  assert.equal(detailed.metadata.tokens, 42);

  // 5. Options-as-first-argument flexibility
  const flexible = await cache.get({
    embedding: [0.7, 0.7, 0.0]
  });
  assert.equal(flexible, "Detailed answer");

  // 6. Delete and Clear
  const initialCount = await cache.count();
  assert.ok(initialCount >= 3);

  await cache.delete("Detailed query");
  assert.equal(await cache.has("Detailed query"), false);
  assert.equal(await cache.count(), initialCount - 1);

  await cache.clear();
  assert.equal(await cache.count(), 0);

  // 7. Error handling
  await assert.rejects(async () => {
    await cache.set("Prompt without vector", "Response", {});
  }, /requires an embedding vector/);

  assert.equal(await cache.get("No vector", {}), null);

  await db.close();
});
