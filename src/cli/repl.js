/**
 * Interactive REPL (Read-Eval-Print Loop) for Tencere.
 * Provides a responsive terminal console with tab-completion and styled output.
 */

import readline from "node:readline";
import { Tencere } from "../index.js";
import { TencereClient } from "../client/index.js";

export const COMMANDS = [
  // Core KV & Mutations
  "GET",
  "SET",
  "DEL",
  "DELETE",
  "HAS",
  "EXISTS",
  "INCR",
  "DECR",
  "MGET",
  "MSET",
  "TTL",
  "PATCH",
  "KEYS",
  "CLEAR",
  "FLUSHALL",

  // Map Collection (and shorthand aliases)
  "MAP.SET",
  "MAP.GET",
  "MAP.DEL",
  "MAP.KEYS",
  "MAP.VALS",
  "MAP.ENTRIES",
  "MAP.SIZE",
  "MAP.CLEAR",
  "HSET",
  "HGET",
  "HDEL",
  "HKEYS",
  "HVALS",
  "HGETALL",
  "HLEN",
  "HCLEAR",

  // Set Collection (and shorthand aliases)
  "SET.ADD",
  "SET.DEL",
  "SET.MEMBERS",
  "SET.HAS",
  "SET.SIZE",
  "SET.CLEAR",
  "SADD",
  "SREM",
  "SMEMBERS",
  "SISMEMBER",
  "SCARD",
  "SCLEAR",

  // Sorted Collection (and shorthand aliases)
  "SORTED.SET",
  "SORTED.INCR",
  "SORTED.SCORE",
  "SORTED.RANK",
  "SORTED.TOP",
  "SORTED.BOTTOM",
  "SORTED.DEL",
  "SORTED.SIZE",
  "SORTED.RANGE",
  "ZADD",
  "ZINCRBY",
  "ZSCORE",
  "ZRANK",
  "ZTOP",
  "ZBOTTOM",
  "ZREM",
  "ZCARD",
  "ZRANGE",

  // Counter Primitive
  "COUNTER.INC",
  "COUNTER.DEC",
  "COUNTER.GET",
  "COUNTER.RESET",

  // Queue Collection
  "QUEUE.PUSH",
  "QUEUE.SIZE",
  "LPUSH",
  "LLEN",

  // Stream Collection
  "STREAM.APPEND",
  "STREAM.HEAD",
  "XADD",
  "XLEN",

  // Vector Collection (Embeddings / Similarity Search)
  "VECTOR.SET",
  "VECTOR.GET",
  "VECTOR.DEL",
  "VECTOR.SEARCH",
  "VECTOR.COUNT",
  "VECTOR.CLEAR",

  // History & Time Travel
  "HISTORY",
  "AT",
  "ROLLBACK",
  "SNAPSHOT",
  "LOCKHISTORY",

  // Coordination & Admin
  "WATCH",
  "UNWATCH",
  "RATELIMIT",
  "LOCK",
  "UNLOCK",
  "CHECKPOINT",
  "STATS",
  "INFO",
  "PING",
  "HELP",
  "EXIT",
  "QUIT"
];

// ANSI colors
const RESET = "\x1b[0m";
const BOLD = "\x1b[1m";
const DIM = "\x1b[2m";
const GREEN = "\x1b[32m";
const RED = "\x1b[31m";
const CYAN = "\x1b[36m";
const YELLOW = "\x1b[33m";
const MAGENTA = "\x1b[35m";

/**
 * Splits command line taking quotes into account.
 *
 * @param {string} line
 * @returns {string[]}
 */
export function splitArgs(line) {
  const args = [];
  let current = "";
  let inQuotes = false;
  let quoteChar = "";

  for (let i = 0; i < line.length; i++) {
    const char = line[i];

    if (inQuotes) {
      if (char === quoteChar) {
        inQuotes = false;
      } else {
        current += char;
      }
    } else if (char === '"' || char === "'") {
      inQuotes = true;
      quoteChar = char;
    } else if (/\s/.test(char)) {
      if (current.length > 0) {
        args.push(current);
        current = "";
      }
    } else {
      current += char;
    }
  }

  if (current.length > 0) {
    args.push(current);
  }

  return args;
}

/**
 * Formats a value for REPL output.
 *
 * @param {any} val
 * @returns {string}
 */
export function formatValue(val) {
  if (val === undefined || val === null) {
    return `${DIM}(nil)${RESET}`;
  }
  if (typeof val === "boolean") {
    return `${CYAN}(boolean) ${val ? "true" : "false"}${RESET}`;
  }
  if (typeof val === "number") {
    return `${YELLOW}(integer) ${val}${RESET}`;
  }
  if (typeof val === "bigint") {
    return `${YELLOW}(integer) ${val.toString()}n${RESET}`;
  }
  if (typeof val === "string") {
    return `"${val}"`;
  }
  if (Array.isArray(val)) {
    if (val.length === 0) return `${DIM}(empty list or set)${RESET}`;
    return val
      .map(
        (item, idx) =>
          `${idx + 1}) ${typeof item === "object" && item !== null ? JSON.stringify(item, (k, v) => (typeof v === "bigint" ? v.toString() : v)) : formatValue(item)}`
      )
      .join("\n");
  }
  if (typeof val === "object") {
    return JSON.stringify(val, (k, v) => (typeof v === "bigint" ? v.toString() : v), 2);
  }
  return String(val);
}

/**
 * Parses user input value, attempting JSON parsing for primitives/objects.
 *
 * @param {string} raw
 * @returns {any}
 */
export function parseInputVal(raw) {
  if (raw === undefined) return undefined;
  if (raw === "true") return true;
  if (raw === "false") return false;
  if (raw === "null") return null;
  if (/^-?\d+$/.test(raw)) return Number(raw);
  if (/^-?\d+\.\d+$/.test(raw)) return Number(raw);

  if ((raw.startsWith("{") && raw.endsWith("}")) || (raw.startsWith("[") && raw.endsWith("]"))) {
    try {
      return JSON.parse(raw);
    } catch (_) {
      return raw;
    }
  }
  return raw;
}

async function execTarget(isLocal, db, client, target, name, method, args = []) {
  if (isLocal) {
    if (target === "map") return db.map(name)[method](...args);
    if (target === "set") return db.setCollection(name)[method](...args);
    if (target === "sorted") {
      const col = db.sorted(name);
      if (method === "between" || method === "above" || method === "below") {
        return col[method](args[0], args[1]).entries();
      }
      return col[method](...args);
    }
    if (target === "counter") return db.counter(name)[method](...args);
    if (target === "queue") return db.queue(name)[method](...args);
    if (target === "stream") return db.stream(name)[method](...args);
    if (target === "vector") return db.vector(name)[method](...args);
    if (target === "rateLimit") return db.rateLimit(name, args[0]);
    if (target === "lock") {
      const lock = await db.tryLock(name, args[0]);
      return lock ? { acquired: true, token: lock.token } : { acquired: false };
    }
    if (target === "unlock") {
      await db.delete(`__lock:${name}`);
      return true;
    }
    if (target === "ttl") return db.ttl(name);
    if (target === "getMany") return db.getMany(args[0]);
    if (target === "setMany") return db.setMany(args[0]);
    if (target === "checkpoint") {
      await db.checkpoint();
      return true;
    }
  } else {
    return client.exec(target, name, method, args);
  }
}

/**
 * Starts the interactive REPL.
 *
 * @param {object} options
 * @param {string} [options.address] Remote TCP host:port (e.g. '127.0.0.1:7337')
 * @param {string|boolean} [options.local] Local data directory path for embedded database
 * @param {NodeJS.ReadableStream} [options.input]
 * @param {NodeJS.WritableStream} [options.output]
 * @returns {Promise<void>}
 */
export async function startRepl(options = {}) {
  const isLocal = options.local !== undefined && options.local !== false;
  const outStream = options.output || process.stdout;
  const inStream = options.input || process.stdin;
  const out = (msg = "") => outStream.write(msg + "\n");

  let db = null;
  let client = null;
  let targetDesc = "";
  let activeWatchCleanup = null;

  if (isLocal) {
    const dataDir = typeof options.local === "string" ? options.local : null;
    db = await Tencere.open(dataDir, { history: { enabled: true } });
    targetDesc = `embedded (${dataDir || "memory"})`;
  } else {
    const address = options.address || "127.0.0.1:7337";
    try {
      client = await TencereClient.connect(address);
      targetDesc = address;
    } catch (err) {
      out(`${RED}Failed to connect to Tencere server at ${address}:${RESET} ${err.message}`);
      out(`${DIM}Tip: Start a server first with 'tencere serve ./data' or use local mode with 'tencere repl --local ./data'${RESET}`);
      if (!options.input) process.exit(1);
      return;
    }
  }

  const promptPrefix = isLocal ? "tencere(local)" : `tencere ${targetDesc}`;
  const promptStr = `${BOLD}${promptPrefix}> ${RESET}`;

  const completer = (line) => {
    const tokens = line.trim().split(/\s+/);
    if (tokens.length === 1) {
      const match = tokens[0].toUpperCase();
      const hits = COMMANDS.filter((c) => c.startsWith(match));
      return [hits.length ? hits : COMMANDS, tokens[0]];
    }
    return [[], line];
  };

  const rl = readline.createInterface({
    input: inStream,
    output: outStream,
    prompt: promptStr,
    completer
  });

  out(`${BOLD}${CYAN}Tencere Interactive Console${RESET}`);
  out(`Connected to: ${BOLD}${GREEN}${targetDesc}${RESET}`);
  out(`Type ${BOLD}'HELP'${RESET} for available commands, ${BOLD}'EXIT'${RESET} or Ctrl+C to quit.\n`);

  rl.prompt();

  return new Promise((resolve) => {
    let lineQueue = Promise.resolve();

    rl.on("line", (line) => {
      lineQueue = lineQueue.then(async () => {
        if (rl.closed) return;
        const trimmed = line.trim();
        if (!trimmed) {
          if (!rl.closed) rl.prompt();
          return;
        }

        const tokens = splitArgs(trimmed);
        const cmd = tokens[0].toUpperCase();
        const args = tokens.slice(1);

      try {
        switch (cmd) {
          case "EXIT":
          case "QUIT": {
            rl.close();
            return;
          }

          case "HELP": {
            out(`
${BOLD}Core Key-Value & Mutations:${RESET}
  ${CYAN}PING${RESET}                                Check connection
  ${CYAN}GET <key>${RESET}                           Get value
  ${CYAN}SET <key> <val> [ex <ttl>]${RESET}          Set value with optional TTL (e.g. 60s, 5m, 1h)
  ${CYAN}DEL <key>${RESET}                           Delete a key (alias: DELETE)
  ${CYAN}HAS <key>${RESET}                           Check if key exists (alias: EXISTS)
  ${CYAN}INCR <key> [delta]${RESET}                  Atomic increment
  ${CYAN}DECR <key> [delta]${RESET}                  Atomic decrement
  ${CYAN}MGET <key1> <key2> ...${RESET}              Get multiple keys
  ${CYAN}MSET <k1> <v1> <k2> <v2> ...${RESET}        Set multiple keys
  ${CYAN}TTL <key>${RESET}                           Get remaining TTL in seconds
  ${CYAN}PATCH <key> <jsonPatch>${RESET}             JSON Patch ($set, $inc, $unset)
  ${CYAN}KEYS [prefix]${RESET}                       List all matching keys
  ${CYAN}CLEAR / FLUSHALL${RESET}                    Flush entire database

${BOLD}Map Collection (Hashes):${RESET}
  ${CYAN}MAP.SET <name> <key> <val>${RESET} (or HSET)  Set field in map
  ${CYAN}MAP.GET <name> <key>${RESET}       (or HGET)  Get field from map
  ${CYAN}MAP.DEL <name> <key>${RESET}       (or HDEL)  Delete field from map
  ${CYAN}MAP.KEYS <name>${RESET}            (or HKEYS) List all keys in map
  ${CYAN}MAP.VALS <name>${RESET}            (or HVALS) List all values in map
  ${CYAN}MAP.ENTRIES <name>${RESET}         (or HGETALL) List all key-value entries
  ${CYAN}MAP.SIZE <name>${RESET}            (or HLEN)  Get number of entries in map
  ${CYAN}MAP.CLEAR <name>${RESET}           (or HCLEAR) Clear all entries in map

${BOLD}Set Collection:${RESET}
  ${CYAN}SET.ADD <name> <member>${RESET}    (or SADD)  Add member to set
  ${CYAN}SET.DEL <name> <member>${RESET}    (or SREM)  Remove member from set
  ${CYAN}SET.MEMBERS <name>${RESET}         (or SMEMBERS) List all members
  ${CYAN}SET.HAS <name> <member>${RESET}    (or SISMEMBER) Check membership
  ${CYAN}SET.SIZE <name>${RESET}           (or SCARD) Get set size
  ${CYAN}SET.CLEAR <name>${RESET}          (or SCLEAR) Clear all members in set

${BOLD}Sorted Set / Leaderboard:${RESET}
  ${CYAN}SORTED.SET <name> <member> <score>${RESET} (or ZADD) Set member score
  ${CYAN}SORTED.INCR <name> <member> [d]${RESET}   (or ZINCRBY) Increment member score
  ${CYAN}SORTED.SCORE <name> <member>${RESET}       (or ZSCORE) Get member score
  ${CYAN}SORTED.RANK <name> <member>${RESET}        (or ZRANK) Get member rank
  ${CYAN}SORTED.TOP <name> [limit]${RESET}          (or ZTOP) Top N members (highest first)
  ${CYAN}SORTED.BOTTOM <name> [limit]${RESET}       (or ZBOTTOM) Bottom N members (lowest first)
  ${CYAN}SORTED.RANGE <name> <min> <max>${RESET}    (or ZRANGE) Query members by score range
  ${CYAN}SORTED.DEL <name> <member>${RESET}         (or ZREM) Delete member
  ${CYAN}SORTED.SIZE <name>${RESET}                (or ZCARD) Count of members

${BOLD}Counter Primitive:${RESET}
  ${CYAN}COUNTER.INC <key> [delta]${RESET}          Increment counter
  ${CYAN}COUNTER.DEC <key> [delta]${RESET}          Decrement counter
  ${CYAN}COUNTER.GET <key>${RESET}                  Get counter value
  ${CYAN}COUNTER.RESET <key> [val]${RESET}          Reset counter

${BOLD}Queue & Stream:${RESET}
  ${CYAN}QUEUE.PUSH <name> <data>${RESET}   (or LPUSH) Push item to queue
  ${CYAN}QUEUE.SIZE <name>${RESET}          (or LLEN)  Get queue depth
  ${CYAN}STREAM.APPEND <name> <data>${RESET} (or XADD) Append to stream log
  ${CYAN}STREAM.HEAD <name>${RESET}          (or XLEN)  Get latest stream sequence

${BOLD}Vector Search (Embeddings):${RESET}
  ${CYAN}VECTOR.SET <name> <id> <vec> [val]${RESET} Set vector document
  ${CYAN}VECTOR.GET <name> <id>${RESET}             Get vector document
  ${CYAN}VECTOR.DEL <name> <id>${RESET}             Delete vector document
  ${CYAN}VECTOR.SEARCH <name> <vec> [topK]${RESET}  Top-K vector similarity search
  ${CYAN}VECTOR.COUNT <name>${RESET}                Total vector count
  ${CYAN}VECTOR.CLEAR <name>${RESET}                Clear vector collection

${BOLD}Time Travel & History:${RESET}
  ${CYAN}HISTORY <key> [limit]${RESET}             Inspect historical revisions of a key
  ${CYAN}AT <timeOrSeq> GET <key>${RESET}          Historical point-in-time read (e.g. AT '1h ago' GET foo)
  ${CYAN}ROLLBACK <key> [to|ver|seq]${RESET}       Rollback key state (forward RESTORE mutation)
  ${CYAN}SNAPSHOT${RESET}                          Pin point-in-time snapshot
  ${CYAN}LOCKHISTORY <key>${RESET}                 Inspect lock lease coordination history

${BOLD}Coordination & Administration:${RESET}
  ${CYAN}WATCH [pattern]${RESET}                     Watch database mutations live in real-time (e.g. WATCH user:*)
  ${CYAN}UNWATCH${RESET}                             Stop watching real-time mutations
  ${CYAN}RATELIMIT <key> <limit> <window>${RESET}   Atomic sliding-window rate limit (e.g. 10 1m)
  ${CYAN}LOCK <key> [ttl]${RESET}                  Acquire distributed lease lock
  ${CYAN}UNLOCK <key>${RESET}                      Release lease lock
  ${CYAN}CHECKPOINT${RESET}                        Force WAL flush & checkpoint
  ${CYAN}STATS${RESET}                             Display database performance stats
  ${CYAN}INFO${RESET}                              Summary of server & engine state
  ${CYAN}EXIT / QUIT${RESET}                       Close console
`);
            break;
          }

          case "PING": {
            if (client) {
              await client.ping();
            }
            out(`${GREEN}PONG${RESET}`);
            break;
          }

          // ---------------- Core KV ----------------
          case "GET": {
            if (!args[0]) {
              out(`${RED}(error) ERR wrong number of arguments for 'get'${RESET}`);
              break;
            }
            const val = client ? await client.get(args[0]) : await db.get(args[0]);
            out(formatValue(val));
            break;
          }

          case "SET": {
            if (args.length < 2) {
              out(`${RED}(error) ERR wrong number of arguments for 'set'${RESET}`);
              break;
            }
            const key = args[0];
            const val = parseInputVal(args[1]);
            const setOptions = {};

            for (let i = 2; i < args.length; i++) {
              const opt = args[i].toUpperCase();
              if ((opt === "EX" || opt === "TTL") && args[i + 1]) {
                setOptions.ttl = args[++i];
              }
            }

            if (client) {
              await client.set(key, val, setOptions);
            } else {
              await db.set(key, val, setOptions);
            }
            out(`${GREEN}OK${RESET}`);
            break;
          }

          case "DEL":
          case "DELETE": {
            if (!args[0]) {
              out(`${RED}(error) ERR wrong number of arguments for 'del'${RESET}`);
              break;
            }
            const deleted = client ? await client.delete(args[0]) : await db.delete(args[0]);
            out(`${YELLOW}(integer) ${deleted ? 1 : 0}${RESET}`);
            break;
          }

          case "HAS":
          case "EXISTS": {
            if (!args[0]) {
              out(`${RED}(error) ERR wrong number of arguments for 'has'${RESET}`);
              break;
            }
            const exists = client ? await client.has(args[0]) : await db.has(args[0]);
            out(`${YELLOW}(integer) ${exists ? 1 : 0}${RESET}`);
            break;
          }

          case "INCR": {
            if (!args[0]) {
              out(`${RED}(error) ERR wrong number of arguments for 'incr'${RESET}`);
              break;
            }
            const delta = args[1] !== undefined ? Number(args[1]) : 1;
            const next = client ? await client.increment(args[0], delta) : await db.increment(args[0], delta);
            out(`${YELLOW}(integer) ${next}${RESET}`);
            break;
          }

          case "DECR": {
            if (!args[0]) {
              out(`${RED}(error) ERR wrong number of arguments for 'decr'${RESET}`);
              break;
            }
            const delta = args[1] !== undefined ? Number(args[1]) : 1;
            const next = client ? await client.increment(args[0], -delta) : await db.increment(args[0], -delta);
            out(`${YELLOW}(integer) ${next}${RESET}`);
            break;
          }

          case "MGET": {
            if (args.length === 0) {
              out(`${RED}(error) ERR wrong number of arguments for 'mget'${RESET}`);
              break;
            }
            const res = client ? await client.getMany(args) : await db.getMany(args);
            const arr = args.map((k) => res[k]);
            out(formatValue(arr));
            break;
          }

          case "MSET": {
            if (args.length < 2 || args.length % 2 !== 0) {
              out(`${RED}(error) ERR Usage: MSET <key1> <val1> <key2> <val2> ...${RESET}`);
              break;
            }
            const entries = {};
            for (let i = 0; i < args.length; i += 2) {
              entries[args[i]] = parseInputVal(args[i + 1]);
            }
            if (client) {
              await client.setMany(entries);
            } else {
              await db.setMany(entries);
            }
            out(`${GREEN}OK${RESET}`);
            break;
          }

          case "TTL": {
            if (!args[0]) {
              out(`${RED}(error) ERR wrong number of arguments for 'ttl'${RESET}`);
              break;
            }
            const ms = client ? await client.ttl(args[0]) : db.ttl(args[0]);
            if (ms === -2) {
              out(`${YELLOW}(integer) -2${RESET} ${DIM}(key does not exist)${RESET}`);
            } else if (ms === -1) {
              out(`${YELLOW}(integer) -1${RESET} ${DIM}(no expiry configured)${RESET}`);
            } else {
              const sec = Math.ceil(ms / 1000);
              out(`${YELLOW}(integer) ${sec}${RESET} ${DIM}(${ms}ms remaining)${RESET}`);
            }
            break;
          }

          case "PATCH": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: PATCH <key> <jsonPatch>${RESET}`);
              break;
            }
            const patchObj = parseInputVal(args[1]);
            const patched = client ? await client.patch(args[0], patchObj) : await db.patch(args[0], patchObj);
            out(formatValue(patched));
            break;
          }

          case "KEYS": {
            const prefix = args[0] || "";
            const list = client ? await client.keys(prefix) : db.keys(prefix);
            out(formatValue(list));
            break;
          }

          case "CLEAR":
          case "FLUSHALL": {
            if (client) {
              await client.clear();
            } else {
              await db._engine.storage.clear();
            }
            out(`${GREEN}OK${RESET}`);
            break;
          }

          // ---------------- Map Collection ----------------
          case "MAP.SET":
          case "HSET": {
            if (args.length < 3) {
              out(`${RED}(error) ERR Usage: MAP.SET <mapName> <field> <value>${RESET}`);
              break;
            }
            await execTarget(isLocal, db, client, "map", args[0], "set", [args[1], parseInputVal(args[2])]);
            out(`${GREEN}OK${RESET}`);
            break;
          }

          case "MAP.GET":
          case "HGET": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: MAP.GET <mapName> <field>${RESET}`);
              break;
            }
            const val = await execTarget(isLocal, db, client, "map", args[0], "get", [args[1]]);
            out(formatValue(val));
            break;
          }

          case "MAP.DEL":
          case "HDEL": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: MAP.DEL <mapName> <field>${RESET}`);
              break;
            }
            const deleted = await execTarget(isLocal, db, client, "map", args[0], "delete", [args[1]]);
            out(`${YELLOW}(integer) ${deleted ? 1 : 0}${RESET}`);
            break;
          }

          case "MAP.KEYS":
          case "HKEYS": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: MAP.KEYS <mapName>${RESET}`);
              break;
            }
            const keys = await execTarget(isLocal, db, client, "map", args[0], "keys", []);
            out(formatValue(keys));
            break;
          }

          case "MAP.VALS":
          case "HVALS": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: MAP.VALS <mapName>${RESET}`);
              break;
            }
            const vals = await execTarget(isLocal, db, client, "map", args[0], "values", []);
            out(formatValue(vals));
            break;
          }

          case "MAP.ENTRIES":
          case "HGETALL": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: MAP.ENTRIES <mapName>${RESET}`);
              break;
            }
            const entries = await execTarget(isLocal, db, client, "map", args[0], "entries", []);
            out(formatValue(entries));
            break;
          }

          case "MAP.SIZE":
          case "HLEN": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: MAP.SIZE <mapName>${RESET}`);
              break;
            }
            const size = await execTarget(isLocal, db, client, "map", args[0], "size", []);
            out(`${YELLOW}(integer) ${size}${RESET}`);
            break;
          }

          case "MAP.CLEAR":
          case "HCLEAR": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: MAP.CLEAR <mapName>${RESET}`);
              break;
            }
            await execTarget(isLocal, db, client, "map", args[0], "clear", []);
            out(`${GREEN}OK${RESET}`);
            break;
          }

          // ---------------- Set Collection ----------------
          case "SET.ADD":
          case "SADD": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: SET.ADD <setName> <member>${RESET}`);
              break;
            }
            await execTarget(isLocal, db, client, "set", args[0], "add", [args[1]]);
            out(`${GREEN}OK${RESET}`);
            break;
          }

          case "SET.DEL":
          case "SREM": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: SET.DEL <setName> <member>${RESET}`);
              break;
            }
            const deleted = await execTarget(isLocal, db, client, "set", args[0], "delete", [args[1]]);
            out(`${YELLOW}(integer) ${deleted ? 1 : 0}${RESET}`);
            break;
          }

          case "SET.MEMBERS":
          case "SMEMBERS": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: SET.MEMBERS <setName>${RESET}`);
              break;
            }
            const members = await execTarget(isLocal, db, client, "set", args[0], "members", []);
            out(formatValue(members));
            break;
          }

          case "SET.HAS":
          case "SISMEMBER": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: SET.HAS <setName> <member>${RESET}`);
              break;
            }
            const has = await execTarget(isLocal, db, client, "set", args[0], "has", [args[1]]);
            out(`${YELLOW}(integer) ${has ? 1 : 0}${RESET}`);
            break;
          }

          case "SET.SIZE":
          case "SCARD": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: SET.SIZE <setName>${RESET}`);
              break;
            }
            const size = await execTarget(isLocal, db, client, "set", args[0], "size", []);
            out(`${YELLOW}(integer) ${size}${RESET}`);
            break;
          }

          case "SET.CLEAR":
          case "SCLEAR": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: SET.CLEAR <setName>${RESET}`);
              break;
            }
            await execTarget(isLocal, db, client, "set", args[0], "clear", []);
            out(`${GREEN}OK${RESET}`);
            break;
          }

          // ---------------- Sorted Collection ----------------
          case "SORTED.SET":
          case "ZADD": {
            if (args.length < 3) {
              out(`${RED}(error) ERR Usage: SORTED.SET <name> <member> <score> [val]${RESET}`);
              break;
            }
            const member = args[1];
            const score = Number(args[2]);
            const extraVal = args[3] !== undefined ? parseInputVal(args[3]) : undefined;
            const arg = extraVal !== undefined ? { score, value: extraVal } : score;
            await execTarget(isLocal, db, client, "sorted", args[0], "set", [member, arg]);
            out(`${GREEN}OK${RESET}`);
            break;
          }

          case "SORTED.INCR":
          case "ZINCRBY": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: SORTED.INCR <name> <member> [delta]${RESET}`);
              break;
            }
            const delta = args[2] !== undefined ? Number(args[2]) : 1;
            const newScore = await execTarget(isLocal, db, client, "sorted", args[0], "incr", [args[1], delta]);
            out(`${YELLOW}(integer) ${newScore}${RESET}`);
            break;
          }

          case "SORTED.SCORE":
          case "ZSCORE": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: SORTED.SCORE <name> <member>${RESET}`);
              break;
            }
            const score = await execTarget(isLocal, db, client, "sorted", args[0], "score", [args[1]]);
            out(formatValue(score));
            break;
          }

          case "SORTED.RANK":
          case "ZRANK": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: SORTED.RANK <name> <member>${RESET}`);
              break;
            }
            const rank = await execTarget(isLocal, db, client, "sorted", args[0], "rank", [args[1]]);
            out(formatValue(rank));
            break;
          }

          case "SORTED.TOP":
          case "ZTOP": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: SORTED.TOP <name> [limit]${RESET}`);
              break;
            }
            const limit = args[1] !== undefined ? Number(args[1]) : 10;
            const topList = await execTarget(isLocal, db, client, "sorted", args[0], "top", [limit]);
            out(formatValue(topList));
            break;
          }

          case "SORTED.BOTTOM":
          case "ZBOTTOM": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: SORTED.BOTTOM <name> [limit]${RESET}`);
              break;
            }
            const limit = args[1] !== undefined ? Number(args[1]) : 10;
            const botList = await execTarget(isLocal, db, client, "sorted", args[0], "bottom", [limit]);
            out(formatValue(botList));
            break;
          }

          case "SORTED.RANGE":
          case "ZRANGE":
          case "SORTED.BETWEEN": {
            if (args.length < 3) {
              out(`${RED}(error) ERR Usage: SORTED.RANGE <name> <min> <max>${RESET}`);
              break;
            }
            const min = Number(args[1]);
            const max = Number(args[2]);
            const list = await execTarget(isLocal, db, client, "sorted", args[0], "between", [min, max]);
            out(formatValue(list));
            break;
          }

          case "SORTED.DEL":
          case "ZREM": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: SORTED.DEL <name> <member>${RESET}`);
              break;
            }
            const deleted = await execTarget(isLocal, db, client, "sorted", args[0], "delete", [args[1]]);
            out(`${YELLOW}(integer) ${deleted ? 1 : 0}${RESET}`);
            break;
          }

          case "SORTED.SIZE":
          case "ZCARD": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: SORTED.SIZE <name>${RESET}`);
              break;
            }
            const size = await execTarget(isLocal, db, client, "sorted", args[0], "size", []);
            out(`${YELLOW}(integer) ${size}${RESET}`);
            break;
          }

          // ---------------- Counter Primitive ----------------
          case "COUNTER.INC": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: COUNTER.INC <key> [delta]${RESET}`);
              break;
            }
            const delta = args[1] !== undefined ? Number(args[1]) : 1;
            const next = await execTarget(isLocal, db, client, "counter", args[0], "inc", [delta]);
            out(`${YELLOW}(integer) ${next}${RESET}`);
            break;
          }

          case "COUNTER.DEC": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: COUNTER.DEC <key> [delta]${RESET}`);
              break;
            }
            const delta = args[1] !== undefined ? Number(args[1]) : 1;
            const next = await execTarget(isLocal, db, client, "counter", args[0], "dec", [delta]);
            out(`${YELLOW}(integer) ${next}${RESET}`);
            break;
          }

          case "COUNTER.GET": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: COUNTER.GET <key>${RESET}`);
              break;
            }
            const val = await execTarget(isLocal, db, client, "counter", args[0], "value", []);
            out(`${YELLOW}(integer) ${val}${RESET}`);
            break;
          }

          case "COUNTER.RESET": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: COUNTER.RESET <key> [val]${RESET}`);
              break;
            }
            const to = args[1] !== undefined ? Number(args[1]) : 0;
            await execTarget(isLocal, db, client, "counter", args[0], "reset", [to]);
            out(`${GREEN}OK${RESET}`);
            break;
          }

          // ---------------- Queue Collection ----------------
          case "QUEUE.PUSH":
          case "LPUSH": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: QUEUE.PUSH <queueName> <data>${RESET}`);
              break;
            }
            const job = await execTarget(isLocal, db, client, "queue", args[0], "push", [parseInputVal(args[1])]);
            out(formatValue(job));
            break;
          }

          case "QUEUE.SIZE":
          case "LLEN": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: QUEUE.SIZE <queueName>${RESET}`);
              break;
            }
            const sz = await execTarget(isLocal, db, client, "queue", args[0], "size", []);
            out(formatValue(sz));
            break;
          }

          // ---------------- Stream Collection ----------------
          case "STREAM.APPEND":
          case "XADD": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: STREAM.APPEND <streamName> <data>${RESET}`);
              break;
            }
            const event = await execTarget(isLocal, db, client, "stream", args[0], "append", [parseInputVal(args[1])]);
            out(formatValue(event));
            break;
          }

          case "STREAM.HEAD":
          case "XLEN": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: STREAM.HEAD <streamName>${RESET}`);
              break;
            }
            const head = await execTarget(isLocal, db, client, "stream", args[0], "head", []);
            out(`${YELLOW}(integer) ${head}${RESET}`);
            break;
          }

          // ---------------- Vector Collection ----------------
          case "VECTOR.SET": {
            if (args.length < 3) {
              out(`${RED}(error) ERR Usage: VECTOR.SET <name> <id> <vectorJson> [valueJson]${RESET}`);
              break;
            }
            const vector = parseInputVal(args[2]);
            const val = args[3] !== undefined ? parseInputVal(args[3]) : undefined;
            await execTarget(isLocal, db, client, "vector", args[0], "set", [args[1], { vector, value: val }]);
            out(`${GREEN}OK${RESET}`);
            break;
          }

          case "VECTOR.GET": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: VECTOR.GET <name> <id>${RESET}`);
              break;
            }
            const doc = await execTarget(isLocal, db, client, "vector", args[0], "get", [args[1]]);
            out(formatValue(doc));
            break;
          }

          case "VECTOR.DEL": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: VECTOR.DEL <name> <id>${RESET}`);
              break;
            }
            const deleted = await execTarget(isLocal, db, client, "vector", args[0], "delete", [args[1]]);
            out(`${YELLOW}(integer) ${deleted ? 1 : 0}${RESET}`);
            break;
          }

          case "VECTOR.SEARCH": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: VECTOR.SEARCH <name> <vectorJson> [topK]${RESET}`);
              break;
            }
            const queryVector = parseInputVal(args[1]);
            const topK = args[2] !== undefined ? Number(args[2]) : 5;
            const hits = await execTarget(isLocal, db, client, "vector", args[0], "search", [queryVector, { topK }]);
            out(formatValue(hits));
            break;
          }

          case "VECTOR.COUNT": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: VECTOR.COUNT <name>${RESET}`);
              break;
            }
            const cnt = await execTarget(isLocal, db, client, "vector", args[0], "count", []);
            out(`${YELLOW}(integer) ${cnt}${RESET}`);
            break;
          }

          case "VECTOR.CLEAR": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: VECTOR.CLEAR <name>${RESET}`);
              break;
            }
            await execTarget(isLocal, db, client, "vector", args[0], "clear", []);
            out(`${GREEN}OK${RESET}`);
            break;
          }

          // ---------------- TimeSeries Collection ----------------
          case "TS.ADD":
          case "TIMESERIES.ADD": {
            if (args.length < 2) {
              out(`${RED}(error) ERR Usage: TS.ADD <name> <value> [at] [tagKey=tagVal...]${RESET}`);
              break;
            }
            const name = args[0];
            const val = Number(args[1]);
            const opts = {};
            for (let i = 2; i < args.length; i++) {
              if (args[i].includes("=")) {
                if (!opts.tags) opts.tags = {};
                const [k, v] = args[i].split("=");
                opts.tags[k] = v;
              } else if (!opts.at) {
                opts.at = args[i];
              }
            }
            if (isLocal) {
              const res = await db.timeseries(name).add(val, opts);
              out(formatValue(res));
            } else {
              out(`${RED}(error) Remote TS.ADD not supported${RESET}`);
            }
            break;
          }

          case "TS.LATEST":
          case "TIMESERIES.LATEST": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: TS.LATEST <name> [count]${RESET}`);
              break;
            }
            const name = args[0];
            const count = args[1] ? Number(args[1]) : 1;
            if (isLocal) {
              const res = await db.timeseries(name).latest(count);
              out(formatValue(res));
            } else {
              out(`${RED}(error) Remote TS.LATEST not supported${RESET}`);
            }
            break;
          }

          case "TS.QUERY":
          case "TIMESERIES.QUERY": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: TS.QUERY <name> [from] [to] [bucket] [agg]${RESET}`);
              break;
            }
            const name = args[0];
            const from = args[1] || "-Infinity";
            const to = args[2] || "Infinity";
            const bucket = args[3] && args[3] !== "-" ? args[3] : null;
            const agg = args[4] && args[4] !== "-" ? args[4].toLowerCase() : null;

            if (isLocal) {
              let q = db.timeseries(name);
              if (from !== "-Infinity" || to !== "Infinity") {
                q = q.between(from === "-Infinity" ? -Infinity : from, to === "Infinity" ? Infinity : to);
              }
              if (bucket) {
                q = q.bucket(bucket);
              }
              let res;
              if (agg && typeof q[agg] === "function") {
                res = await q[agg]();
              } else {
                res = await q.values();
              }
              out(formatValue(res));
            } else {
              out(`${RED}(error) Remote TS.QUERY not supported${RESET}`);
            }
            break;
          }

          case "TS.STATS":
          case "TIMESERIES.INFO":
          case "TIMESERIES.STATS": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: TS.STATS <name>${RESET}`);
              break;
            }
            if (isLocal) {
              const st = await db.timeseries(args[0]).stats();
              out(formatValue(st));
            } else {
              out(`${RED}(error) Remote TS.STATS not supported${RESET}`);
            }
            break;
          }

          case "TIMESERIES.LIST":
          case "TS.LIST": {
            if (isLocal) {
              const stores = db._engine._timeSeriesStores ? Array.from(db._engine._timeSeriesStores.keys()) : [];
              out(formatValue(stores));
            } else {
              out(`${RED}(error) Remote TS.LIST not supported${RESET}`);
            }
            break;
          }

          // ---------------- Coordination & Admin ----------------
          case "WATCH": {
            const pattern = args[0] || "";
            if (activeWatchCleanup) {
              await activeWatchCleanup();
              activeWatchCleanup = null;
            }

            const printChange = (evt) => {
              const opColor = evt.type === "delete" ? RED : GREEN;
              const valStr = evt.value !== undefined ? ` -> ${formatValue(evt.value)}` : "";
              const prevStr = evt.previousValue !== undefined ? ` ${DIM}(prev: ${formatValue(evt.previousValue)})${RESET}` : "";
              out(`\n${MAGENTA}[WATCH]${RESET} ${opColor}${evt.type.toUpperCase()}${RESET} ${BOLD}${evt.key}${RESET}${valStr}${prevStr} ${DIM}(v:${evt.version})${RESET}`);
              if (!rl.closed) rl.prompt(true);
            };

            if (isLocal) {
              const listener = (evt) => {
                if (pattern) {
                  const matches = pattern.endsWith("*")
                    ? evt.key.startsWith(pattern.slice(0, -1))
                    : evt.key === pattern || evt.key.startsWith(pattern);
                  if (!matches) return;
                }
                printChange(evt);
              };
              db._engine.events.on("change", listener);
              activeWatchCleanup = async () => {
                db._engine.events.removeListener("change", listener);
              };
            } else {
              await client.watch(pattern, printChange);
              activeWatchCleanup = async () => {
                await client.unwatch();
              };
            }

            out(`${CYAN}Watching changes${pattern ? ` matching '${pattern}'` : ""} in real-time... (type UNWATCH to stop)${RESET}`);
            if (pattern && !pattern.includes("*")) {
              try {
                const currentVal = isLocal ? await db.get(pattern) : await client.get(pattern);
                out(`${DIM}Current value:${RESET} ${formatValue(currentVal)}`);
              } catch (_) {}
            }
            break;
          }

          case "UNWATCH": {
            if (activeWatchCleanup) {
              await activeWatchCleanup();
              activeWatchCleanup = null;
              out(`${GREEN}OK${RESET} ${DIM}(stopped watching)${RESET}`);
            } else {
              out(`${DIM}No active watch stream.${RESET}`);
            }
            break;
          }

          case "RATELIMIT": {
            if (args.length < 3) {
              out(`${RED}(error) ERR Usage: RATELIMIT <key> <limit> <window>${RESET} (e.g. RATELIMIT u1 10 1m)`);
              break;
            }
            const limit = Number(args[1]);
            const window = args[2];
            const rlRes = await execTarget(isLocal, db, client, "rateLimit", args[0], "consume", [{ limit, window }]);
            out(formatValue(rlRes));
            break;
          }

          case "LOCK": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: LOCK <key> [ttl]${RESET}`);
              break;
            }
            const ttl = args[1] || "30s";
            const lockRes = await execTarget(isLocal, db, client, "lock", args[0], "tryAcquire", [{ ttl }]);
            out(formatValue(lockRes));
            break;
          }

          case "UNLOCK": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: UNLOCK <key>${RESET}`);
              break;
            }
            await execTarget(isLocal, db, client, "unlock", args[0], null, []);
            out(`${GREEN}OK${RESET}`);
            break;
          }

          case "CHECKPOINT": {
            await execTarget(isLocal, db, client, "checkpoint", null, null, []);
            out(`${GREEN}OK${RESET}`);
            break;
          }

          case "STATS": {
            const stats = client ? await client.stats() : db.stats();
            out(JSON.stringify(stats, null, 2));
            break;
          }

          case "INFO": {
            const stats = client ? await client.stats() : db.stats();
            out(`
${BOLD}# Tencere Server & Storage${RESET}
Target:            ${targetDesc}
Keys:              ${stats.keys ?? stats.keyCount ?? 0}
Memory:            ${((stats.memoryBytes || 0) / 1024).toFixed(2)} KB
Reads/Writes:      ${stats.reads || 0} / ${stats.writes || 0}
Deletes:           ${stats.deletes || 0}
Cache Hits/Misses: ${stats.cacheHits || 0} / ${stats.cacheMisses || 0}
`);
            break;
          }

          case "HISTORY": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: HISTORY <key> [limit]${RESET}`);
              break;
            }
            if (!db || !db._engine || !db._engine.historyManager) {
              out(`${RED}(error) History is only supported in local/embedded mode${RESET}`);
              break;
            }
            const limit = args[1] ? Number(args[1]) : 20;
            const list = [];
            for await (const rev of db.history(args[0], { limit })) {
              list.push(rev);
            }
            if (list.length === 0) {
              out(`${DIM}(empty history)${RESET}`);
            } else {
              list.forEach((item, idx) => {
                const ts = new Date(item.timestamp).toISOString();
                out(`${YELLOW}${idx + 1})${RESET} seq=${item.sequence} op=${BOLD}${item.operation}${RESET} ver=${item.version} time=${ts} val=${formatValue(item.value)}`);
              });
            }
            break;
          }

          case "AT": {
            if (args.length < 3 || args[1].toUpperCase() !== "GET") {
              out(`${RED}(error) ERR Usage: AT <timeOrSeq> GET <key>${RESET}`);
              break;
            }
            if (!db || !db._engine || !db._engine.historyManager) {
              out(`${RED}(error) Time travel is only supported in local/embedded mode${RESET}`);
              break;
            }
            const target = /^\d+n?$/.test(args[0]) ? { sequence: BigInt(args[0].replace("n", "")) } : args[0];
            const past = db.at(target);
            const val = await past.get(args[2]);
            out(formatValue(val));
            break;
          }

          case "ROLLBACK": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: ROLLBACK <key> [to|version|seq]${RESET}`);
              break;
            }
            if (!db || !db._engine || !db._engine.historyManager) {
              out(`${RED}(error) Rollback is only supported in local/embedded mode${RESET}`);
              break;
            }
            const opts = {};
            if (args[1]) {
              if (/^\d+n?$/.test(args[1])) {
                opts.version = BigInt(args[1].replace("n", ""));
              } else {
                opts.to = args[1];
              }
            }
            const res = await db.rollback(args[0], opts);
            out(`${GREEN}OK${RESET} (restoredVersion: ${res.restoredVersion}, newVersion: ${res.newVersion}, seq: ${res.sequence})`);
            break;
          }

          case "SNAPSHOT": {
            if (!db || !db._engine || !db._engine.historyManager) {
              out(`${RED}(error) Snapshots are only supported in local/embedded mode${RESET}`);
              break;
            }
            const snap = await db.snapshot();
            out(`${GREEN}OK${RESET} (pinned sequence: ${snap._snapshotPin ? snap._snapshotPin.sequence : "current"})`);
            break;
          }

          case "LOCKHISTORY": {
            if (!args[0]) {
              out(`${RED}(error) ERR Usage: LOCKHISTORY <key>${RESET}`);
              break;
            }
            if (!db) {
              out(`${RED}(error) Lock history is only supported in local/embedded mode${RESET}`);
              break;
            }
            const events = db.lockHistory(args[0]);
            if (!events || events.length === 0) {
              out(`${DIM}(no lock events recorded)${RESET}`);
            } else {
              events.forEach((ev, idx) => {
                const ts = new Date(ev.timestamp).toISOString();
                const op = ev.opName || ev.operation || ev.op;
                const owner = ev.details?.ownerId || ev.owner || "-";
                const token = ev.details?.token || ev.token || "-";
                out(`${YELLOW}${idx + 1})${RESET} op=${BOLD}${op}${RESET} owner=${owner} token=${token} time=${ts}`);
              });
            }
            break;
          }

          default:
            out(`${RED}(error) ERR unknown command '${cmd}'${RESET}. Type 'HELP' for options.`);
            break;
        }
      } catch (err) {
        out(`${RED}(error) ${err.message}${RESET}`);
      }

        if (!rl.closed) {
          rl.prompt();
        }
      });
    });

    rl.on("close", async () => {
      await lineQueue;
      out("\nGoodbye!");
      if (activeWatchCleanup) {
        try {
          await activeWatchCleanup();
        } catch (_) {}
      }
      if (client) await client.close();
      if (db) await db.close();
      if (options.exitOnClose !== false && !options.input) {
        process.exit(0);
      }
      resolve();
    });
  });
}
