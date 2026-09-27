import test from "node:test";
import assert from "node:assert/strict";
import { Tencere } from "../../src/index.js";

test("Tencere Collections - Stream append and async iterator consumption", async () => {
  const db = await Tencere.open();
  const stream = db.stream("orders");

  // Append items
  const e1 = await stream.append({ user: 42, amount: 100 });
  const e2 = await stream.append({ user: 43, amount: 250 });

  assert.equal(e1.sequence, 1);
  assert.equal(e2.sequence, 2);
  assert.equal(await stream.head(), 2);

  // Consume without live tailing (fromId: 0, tail: false)
  const consumed = [];
  for await (const event of stream.consume({ fromId: 0, tail: false })) {
    consumed.push(event);
  }

  assert.equal(consumed.length, 2);
  assert.equal(consumed[0].data.amount, 100);
  assert.equal(consumed[1].data.amount, 250);

  // Live consumption
  const liveConsumed = [];
  const consumerPromise = (async () => {
    for await (const event of stream.consume({ fromId: 2, tail: true })) {
      liveConsumed.push(event);
      if (liveConsumed.length === 2) break;
    }
  })();

  // Wait a tick then append
  await new Promise((r) => setTimeout(r, 20));
  await stream.append({ user: 44, amount: 300 });
  await stream.append({ user: 45, amount: 400 });

  await consumerPromise;
  assert.equal(liveConsumed.length, 2);
  assert.equal(liveConsumed[0].data.amount, 300);
  assert.equal(liveConsumed[1].data.amount, 400);

  await db.close();
});
