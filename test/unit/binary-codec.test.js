import test from "node:test";
import assert from "node:assert/strict";
import { BinaryCodec } from "../../src/core/binary-codec.js";

test("BinaryCodec - encodes and decodes primitives without loss", () => {
  // String
  const str = "hello world, Türkçemiz!";
  assert.equal(BinaryCodec.decode(BinaryCodec.encode(str)), str);

  // Int32
  assert.equal(BinaryCodec.decode(BinaryCodec.encode(42)), 42);
  assert.equal(BinaryCodec.decode(BinaryCodec.encode(-1000)), -1000);

  // Int64 / BigInt
  const big = 9007199254740995n;
  assert.equal(BinaryCodec.decode(BinaryCodec.encode(big)), big);

  // Float64
  const fl = 3.1415926535;
  assert.equal(BinaryCodec.decode(BinaryCodec.encode(fl)), fl);

  // Boolean
  assert.equal(BinaryCodec.decode(BinaryCodec.encode(true)), true);
  assert.equal(BinaryCodec.decode(BinaryCodec.encode(false)), false);

  // Null & Undefined
  assert.equal(BinaryCodec.decode(BinaryCodec.encode(null)), null);
  assert.equal(BinaryCodec.decode(BinaryCodec.encode(undefined)), undefined);

  // Uint8Array (raw binary bytes)
  const raw = new Uint8Array([1, 2, 3, 4, 255]);
  const decodedRaw = BinaryCodec.decode(BinaryCodec.encode(raw));
  assert.deepEqual(decodedRaw, raw);

  // Complex JSON
  const complex = { id: 42, user: "Ahmet", active: true, scores: [10, 20, 30] };
  assert.deepEqual(BinaryCodec.decode(BinaryCodec.encode(complex)), complex);
});
