import test from "node:test";
import assert from "node:assert/strict";
import { PassThrough } from "node:stream";
import { splitArgs, formatValue, parseInputVal, startRepl } from "../../src/cli/repl.js";

test("Tencere CLI / REPL - Argument splitting with quotes", () => {
  assert.deepEqual(splitArgs("GET foo"), ["GET", "foo"]);
  assert.deepEqual(splitArgs('SET message "Hello World" EX 60'), ["SET", "message", "Hello World", "EX", "60"]);
  assert.deepEqual(splitArgs("SET 'user key' '{\"name\": \"Ahmet\"}'"), ["SET", "user key", '{"name": "Ahmet"}']);
  assert.deepEqual(splitArgs(""), []);
});

test("Tencere CLI / REPL - Value formatting", () => {
  assert.ok(formatValue(null).includes("(nil)"));
  assert.ok(formatValue(undefined).includes("(nil)"));
  assert.ok(formatValue(123).includes("(integer) 123"));
  assert.ok(formatValue(true).includes("(boolean) true"));
  assert.equal(formatValue("test-val"), '"test-val"');
  assert.ok(formatValue(["alpha", "beta"]).includes("1) \"alpha\""));
  assert.ok(formatValue({ a: 1 }).includes('"a": 1'));
});

test("Tencere CLI / REPL - Input value parsing", () => {
  assert.equal(parseInputVal("true"), true);
  assert.equal(parseInputVal("false"), false);
  assert.equal(parseInputVal("null"), null);
  assert.equal(parseInputVal("42"), 42);
  assert.equal(parseInputVal("3.14"), 3.14);
  assert.deepEqual(parseInputVal('{"id": 1}'), { id: 1 });
  assert.equal(parseInputVal("regular string"), "regular string");
});

test("Tencere CLI / REPL - Interactive local session", async () => {
  const input = new PassThrough();
  const output = new PassThrough();

  let captured = "";
  output.on("data", (chunk) => {
    captured += chunk.toString();
  });

  const replPromise = startRepl({
    local: true, // in-memory embedded
    input,
    output
  });

  await new Promise((r) => setTimeout(r, 50));

  input.write("PING\n");
  input.write('SET fruit "apple"\n');
  input.write("GET fruit\n");
  input.write("HAS fruit\n");
  input.write("DECR fruit 1\n");
  input.write("MSET a 10 b 20\n");
  input.write("MGET a b\n");
  input.write("MAP.SET userMap name Ahmet\n");
  input.write("MAP.GET userMap name\n");
  input.write("SET.ADD mySet alpha\n");
  input.write("SET.HAS mySet alpha\n");
  input.write("SORTED.SET scores player1 100\n");
  input.write("SORTED.INCR scores player1 50\n");
  input.write("SORTED.SCORE scores player1\n");
  input.write("COUNTER.INC myCount 5\n");
  input.write("VECTOR.SET docs doc1 [0.1,0.2,0.3]\n");
  input.write("VECTOR.COUNT docs\n");
  input.write("LOCK order123\n");
  input.write("UNLOCK order123\n");
  input.write("WATCH user:*\n");
  input.write("SET user:42 John\n");
  input.write("UNWATCH\n");
  input.write("KEYS\n");
  input.write("DEL fruit\n");
  input.write("GET fruit\n");
  input.write("HELP\n");
  input.write("EXIT\n");

  await replPromise;

  assert.ok(captured.includes("PONG"));
  assert.ok(captured.includes("OK"));
  assert.ok(captured.includes('"apple"'));
  assert.ok(captured.includes("(integer) 1"));
  assert.ok(captured.includes("(nil)"));
  assert.ok(captured.includes("Core Key-Value & Mutations:"));
  assert.ok(captured.includes('"Ahmet"'));
  assert.ok(captured.includes("(integer) 150"));
  assert.ok(captured.includes("Watching changes matching 'user:*'"));
  assert.ok(captured.includes("[WATCH]"));
  assert.ok(captured.includes("stopped watching"));
});

test("Tencere CLI / REPL - History, Time Travel, and Rollback interactive commands", async () => {
  const input = new PassThrough();
  const output = new PassThrough();

  let captured = "";
  output.on("data", (chunk) => {
    captured += chunk.toString();
  });

  const replPromise = startRepl({
    local: true,
    input,
    output
  });

  await new Promise((r) => setTimeout(r, 50));

  input.write('SET site:title "V1"\n');
  input.write('SET site:title "V2"\n');
  input.write("HISTORY site:title\n");
  input.write("SNAPSHOT\n");
  input.write("ROLLBACK site:title 1\n");
  input.write("GET site:title\n");
  input.write("AT 1 GET site:title\n");
  await new Promise((r) => setTimeout(r, 60));
  input.write("EXIT\n");

  await replPromise;

  assert.ok(captured.includes("seq="));
  assert.ok(captured.includes("pinned sequence:"));
  assert.ok(captured.includes("restoredVersion:"));
  assert.ok(captured.includes('"V1"'));
});

test("Tencere CLI / REPL - Interactive remote TCP session", async () => {
  const { Tencere } = await import("../../src/index.js");
  const { TencereServer } = await import("../../src/core/server.js");

  const db = await Tencere.open();
  const server = new TencereServer(db, { port: 19899, host: "127.0.0.1" });
  await server.start();

  const input = new PassThrough();
  const output = new PassThrough();

  let captured = "";
  output.on("data", (chunk) => {
    captured += chunk.toString();
  });

  const replPromise = startRepl({
    address: "127.0.0.1:19899",
    input,
    output
  });

  await new Promise((r) => setTimeout(r, 50));

  input.write("PING\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("WATCH remote_*\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("SET remote_key 999\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("GET remote_key\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("INCR remote_key 1\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("UNWATCH\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("KEYS\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("DEL remote_key\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("EXIT\n");

  await replPromise;
  await server.stop();
  await db.close();

  assert.ok(captured.includes("PONG"));
  assert.ok(captured.includes("OK"));
  assert.ok(captured.includes("(integer) 999"));
  assert.ok(captured.includes("(integer) 1000"));
  assert.ok(captured.includes("Watching changes matching 'remote_*'"));
  assert.ok(captured.includes("[WATCH]"));
});

test("Tencere CLI / REPL - TimeSeries interactive commands", async () => {
  const input = new PassThrough();
  const output = new PassThrough();

  let captured = "";
  output.on("data", (chunk) => {
    captured += chunk.toString();
  });

  const replPromise = startRepl({
    local: true,
    input,
    output
  });

  await new Promise((r) => setTimeout(r, 50));

  input.write("TS.ADD temp 21.5\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("TS.ADD temp 22.0 sensor=kitchen\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("TS.LATEST temp\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("TS.QUERY temp\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("TS.STATS temp\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("TIMESERIES.LIST\n");
  await new Promise((r) => setTimeout(r, 40));
  input.write("EXIT\n");

  await replPromise;

  assert.ok(captured.includes("21.5"));
  assert.ok(captured.includes("22"));
  assert.ok(captured.includes('"points": 2'));
  assert.ok(captured.includes('"temp"'));
});

