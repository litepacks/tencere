import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Tencere Collections - Vector search, SemanticCache, and AgentMemory", async () => {
  const db = await Tencere.open();
  const docs = db.vector("docs", { metric: "cosine" });

  await docs.set("d1", {
    vector: [1.0, 0.0, 0.0],
    value: { title: "Introduction to AI", category: "tech" }
  });

  await docs.set("d2", {
    vector: [0.9, 0.1, 0.0],
    value: { title: "Deep Learning Foundations", category: "tech" }
  });

  await docs.set("d3", {
    vector: [0.0, 1.0, 0.0],
    value: { title: "Italian Cooking Recipes", category: "food" }
  });

  // Search closest to [1.0, 0.0, 0.0]
  const results = await docs.search([1.0, 0.0, 0.0], { topK: 2 });
  assert.equal(results.length, 2);
  assert.equal(results[0].id, "d1");
  assert.ok(results[0].score >= 0.999);
  assert.equal(results[1].id, "d2");
  assert.ok(results[1].score > 0.9);

  // Search with filter
  const foodOnly = await docs.search([1.0, 0.0, 0.0], {
    topK: 5,
    filter: (doc) => doc.value.category === "food"
  });
  assert.equal(foodOnly.length, 1);
  assert.equal(foodOnly[0].id, "d3");

  // Semantic Cache test
  await db.semantic.set("What is the capital of Turkey?", "Ankara", {
    embedding: [0.8, 0.6, 0.0]
  });

  const cacheHit = await db.semantic.get("Turkey's capital?", {
    embedding: [0.81, 0.59, 0.0],
    threshold: 0.95
  });
  assert.equal(cacheHit, "Ankara");

  const cacheMiss = await db.semantic.get("French cuisine?", {
    embedding: [0.0, 1.0, 0.0],
    threshold: 0.95
  });
  assert.equal(cacheMiss, null);

  // Agent Memory test
  await db.memory.add("agent:1", {
    content: "User prefers dark mode and concise summaries",
    importance: 0.9,
    embedding: [0.5, 0.5, 0.0]
  });

  const memories = await db.memory.recall("agent:1", {
    embedding: [0.5, 0.5, 0.0],
    topK: 1
  });
  assert.equal(memories.length, 1);
  assert.ok(memories[0].content.includes("dark mode"));

  await db.close();
});
