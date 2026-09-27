/**
 * Centralized high-performance time and duration parsing for Tencere.
 * Used uniformly across TTL, History, Scheduler, and TimeSeries.
 */

const DURATION_CACHE = new Map([
  ["1s", 1000],
  ["5s", 5000],
  ["10s", 10000],
  ["30s", 30000],
  ["1m", 60000],
  ["5m", 300000],
  ["10m", 600000],
  ["15m", 900000],
  ["30m", 1800000],
  ["1h", 3600000],
  ["2h", 7200000],
  ["6h", 21600000],
  ["12h", 43200000],
  ["24h", 86400000],
  ["1d", 86400000],
  ["7d", 604800000],
  ["30d", 2592000000]
]);

/**
 * Parses duration strings like '100ms', '5s', '1m', '2h', '30d' into milliseconds.
 *
 * @param {string|number} duration
 * @returns {number}
 */
export function parseDuration(duration) {
  if (typeof duration === "number") {
    return duration;
  }
  if (!duration || typeof duration !== "string") {
    return 0;
  }
  const cached = DURATION_CACHE.get(duration);
  if (cached !== undefined) {
    return cached;
  }
  const match = duration.trim().match(/^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w|y)?$/i);
  if (!match) {
    const num = Number(duration);
    const parsed = isNaN(num) ? 0 : num;
    if (DURATION_CACHE.size < 256) DURATION_CACHE.set(duration, parsed);
    return parsed;
  }
  const val = parseFloat(match[1]);
  const unit = (match[2] || "ms").toLowerCase();
  let result = 0;
  switch (unit) {
    case "ms":
      result = Math.round(val);
      break;
    case "s":
      result = Math.round(val * 1000);
      break;
    case "m":
      result = Math.round(val * 60 * 1000);
      break;
    case "h":
      result = Math.round(val * 60 * 60 * 1000);
      break;
    case "d":
      result = Math.round(val * 24 * 60 * 60 * 1000);
      break;
    case "w":
      result = Math.round(val * 7 * 24 * 60 * 60 * 1000);
      break;
    case "y":
      result = Math.round(val * 365 * 24 * 60 * 60 * 1000);
      break;
    default:
      result = Math.round(val);
  }
  if (DURATION_CACHE.size < 256) {
    DURATION_CACHE.set(duration, result);
  }
  return result;
}

/**
 * Parses time selectors like '1h ago', '10m ago', 'now', ISO timestamp, or milliseconds.
 *
 * @param {string|number|Date} point
 * @param {number} [now=Date.now()]
 * @returns {number}
 */
export function parseTimePoint(point, now = Date.now()) {
  if (typeof point === "number") return point;
  if (point instanceof Date) return point.getTime();
  if (typeof point !== "string") return now;

  const trimmed = point.trim();
  if (trimmed.toLowerCase() === "now") return now;

  // Check 'X ago' format (e.g. '1h ago', '30m ago', '2d ago')
  const agoMatch = trimmed.match(/^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w|y)\s*ago$/i);
  if (agoMatch) {
    const val = parseFloat(agoMatch[1]);
    const unit = agoMatch[2].toLowerCase();
    const duration = parseDuration(`${val}${unit}`);
    return now - duration;
  }

  // Check standard ISO / date string
  const parsed = Date.parse(trimmed);
  if (!isNaN(parsed)) {
    return parsed;
  }

  // Fallback to duration parser if someone passed e.g. "1h"
  const duration = parseDuration(trimmed);
  if (duration > 0) {
    return now - duration;
  }

  return now;
}

/**
 * Deterministically aligns a timestamp to an epoch bucket boundary.
 *
 * @param {number} timestamp
 * @param {number} bucketMs
 * @returns {number}
 */
export function alignToBucket(timestamp, bucketMs) {
  if (bucketMs <= 0) return timestamp;
  return Math.floor(timestamp / bucketMs) * bucketMs;
}
