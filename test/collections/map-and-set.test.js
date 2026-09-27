import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Tencere Collections - Map & Set", async () => {
  const db = await Tencere.open();

  // Map
  const users = db.map("users");
  await users.set("1", { name: "Ahmet", role: "admin" });
  await users.set("2", { name: "Mehmet", role: "user" });

  assert.equal(await users.has("1"), true);
  assert.equal(await users.has("3"), false);
  assert.equal((await users.get("1")).name, "Ahmet");
  assert.equal(await users.size(), 2);

  const entries = await users.entries();
  assert.equal(entries.length, 2);

  await users.delete("2");
  assert.equal(await users.size(), 1);

  // Set
  const tags = db.setCollection("tags");
  await tags.add("fast");
  await tags.add("embedded");
  await tags.add("coordination");

  assert.equal(await tags.has("fast"), true);
  assert.equal(await tags.size(), 3);
  assert.deepEqual((await tags.members()).sort(), ["coordination", "embedded", "fast"]);

  await tags.delete("fast");
  assert.equal(await tags.has("fast"), false);

  await tags.clear();
  assert.equal(await tags.size(), 0);

  // Scope
  const tenant = db.scope("tenant:1");
  await tenant.set("config", { theme: "dark" });
  assert.deepEqual(await tenant.get("config"), { theme: "dark" });
  assert.deepEqual(await db.get("tenant:1:config"), { theme: "dark" });

  await db.close();
});
