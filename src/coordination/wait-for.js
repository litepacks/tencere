/**
 * waitFor implementation: event-driven condition matching using reactive change events.
 */

import { parseDuration } from "../core/expiry-wheel.js";
import { TimeoutError } from "../errors.js";

/**
 * Checks whether a candidate value satisfies the given predicate or serializable condition.
 *
 * @param {any} value
 * @param {any} condition
 * @returns {boolean}
 */
export function evaluateCondition(value, condition) {
  if (value === undefined || value === null) return false;

  // 1. Function predicate
  if (typeof condition === "function") {
    try {
      return Boolean(condition(value));
    } catch (_) {
      return false;
    }
  }

  if (typeof condition !== "object" || condition === null) {
    return value === condition;
  }

  // 2. Structured 'where' operator syntax
  if (condition.where && typeof condition.where === "object") {
    for (const [field, ops] of Object.entries(condition.where)) {
      const fieldVal = value ? value[field] : undefined;
      if (typeof ops === "object" && ops !== null) {
        if (ops.eq !== undefined && fieldVal !== ops.eq) return false;
        if (ops.neq !== undefined && fieldVal === ops.neq) return false;
        if (ops.gt !== undefined && !(fieldVal > ops.gt)) return false;
        if (ops.gte !== undefined && !(fieldVal >= ops.gte)) return false;
        if (ops.lt !== undefined && !(fieldVal < ops.lt)) return false;
        if (ops.lte !== undefined && !(fieldVal <= ops.lte)) return false;
        if (ops.in !== undefined && (!Array.isArray(ops.in) || !ops.in.includes(fieldVal))) return false;
      } else {
        if (fieldVal !== ops) return false;
      }
    }
    return true;
  }

  // 3. Exact object field match (e.g. { status: 'completed' })
  for (const [k, expected] of Object.entries(condition)) {
    if (k === "timeout") continue;
    if (value[k] !== expected) return false;
  }

  return true;
}

/**
 * Awaits a key satisfying condition using event-driven change listener.
 *
 * @param {import('../index.js').Tencere} db
 * @param {string} key
 * @param {any} condition
 * @param {object} [options={}]
 * @param {string|number} [options.timeout='30s']
 * @returns {Promise<any>}
 */
export async function waitFor(db, key, condition, options = {}) {
  const timeoutMs = options.timeout
    ? parseDuration(options.timeout)
    : condition && condition.timeout
    ? parseDuration(condition.timeout)
    : 30000;

  const engine = db._engine || db;
  const targetKey = typeof db._k === "function" ? db._k(key) : key;

  // 1. Fast path: check current value
  const current = await db.get(key);
  if (evaluateCondition(current, condition)) {
    return current;
  }

  // 2. Event-driven wait
  return new Promise((resolve, reject) => {
    let settled = false;

    const timer = setTimeout(() => {
      cleanup();
      reject(new TimeoutError(`waitFor('${key}') timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    const listener = (event) => {
      if (settled) return;
      if (event.key === targetKey && evaluateCondition(event.value, condition)) {
        cleanup();
        resolve(event.value);
      }
    };

    function cleanup() {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (engine.events && typeof engine.events.removeListener === "function") {
        engine.events.removeListener("change", listener);
      }
    }

    if (engine.events && typeof engine.events.on === "function") {
      engine.events.on("change", listener);
    }

    // Double-check in case value updated right as listener was attached
    db.get(key).then((val) => {
      if (!settled && evaluateCondition(val, condition)) {
        cleanup();
        resolve(val);
      }
    }).catch(() => {});
  });
}
