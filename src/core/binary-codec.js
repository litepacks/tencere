/**
 * High-performance binary codec for Tencere core value types.
 *
 * Types:
 *  - 0x01: BYTES (Uint8Array / Buffer)
 *  - 0x02: STRING (UTF-8)
 *  - 0x03: INT32 (4 bytes signed)
 *  - 0x04: INT64 / BIGINT (8 bytes signed)
 *  - 0x05: FLOAT64 (8 bytes double)
 *  - 0x06: BOOLEAN_TRUE
 *  - 0x07: BOOLEAN_FALSE
 *  - 0x08: NULL
 *  - 0x09: UNDEFINED
 *  - 0x0A: JSON (Complex objects / arrays)
 */

export const TYPE_BYTES = 0x01;
export const TYPE_STRING = 0x02;
export const TYPE_INT32 = 0x03;
export const TYPE_INT64 = 0x04;
export const TYPE_FLOAT64 = 0x05;
export const TYPE_BOOLEAN_TRUE = 0x06;
export const TYPE_BOOLEAN_FALSE = 0x07;
export const TYPE_NULL = 0x08;
export const TYPE_UNDEFINED = 0x09;
export const TYPE_JSON = 0x0a;

const BUF_UNDEFINED = new Uint8Array([TYPE_UNDEFINED]);
const BUF_NULL = new Uint8Array([TYPE_NULL]);
const BUF_TRUE = new Uint8Array([TYPE_BOOLEAN_TRUE]);
const BUF_FALSE = new Uint8Array([TYPE_BOOLEAN_FALSE]);
const BUF_EMPTY_STRING = Buffer.from([TYPE_STRING]);
const BUF_EMPTY_BYTES = Buffer.from([TYPE_BYTES]);

const SMALL_INT_MIN = -128;
const SMALL_INT_MAX = 255;
const SMALL_INT_CACHE = new Array(SMALL_INT_MAX - SMALL_INT_MIN + 1);
for (let i = SMALL_INT_MIN; i <= SMALL_INT_MAX; i++) {
  const b = Buffer.allocUnsafe(5);
  b[0] = TYPE_INT32;
  b.writeInt32BE(i, 1);
  SMALL_INT_CACHE[i - SMALL_INT_MIN] = b;
}

export class BinaryCodec {
  /**
   * Encodes a JS value into a binary Buffer with a 1-byte type tag.
   *
   * @param {any} val
   * @returns {Uint8Array}
   */
  static encode(val) {
    if (val === undefined) {
      return BUF_UNDEFINED;
    }
    if (val === null) {
      return BUF_NULL;
    }
    if (typeof val === "boolean") {
      return val ? BUF_TRUE : BUF_FALSE;
    }
    if (typeof val === "number") {
      if (Number.isInteger(val)) {
        if (val >= SMALL_INT_MIN && val <= SMALL_INT_MAX) {
          return SMALL_INT_CACHE[val - SMALL_INT_MIN];
        }
        if (val >= -0x80000000 && val <= 0x7fffffff) {
          const buf = Buffer.allocUnsafe(5);
          buf[0] = TYPE_INT32;
          buf.writeInt32BE(val, 1);
          return buf;
        }
      }
      // Floating point or integer outside 32-bit range
      const buf = Buffer.allocUnsafe(9);
      buf[0] = TYPE_FLOAT64;
      buf.writeDoubleBE(val, 1);
      return buf;
    }
    if (typeof val === "bigint") {
      const buf = Buffer.allocUnsafe(9);
      buf[0] = TYPE_INT64;
      buf.writeBigInt64BE(val, 1);
      return buf;
    }
    if (typeof val === "string") {
      if (val.length === 0) {
        return BUF_EMPTY_STRING;
      }
      const len = Buffer.byteLength(val);
      const buf = Buffer.allocUnsafe(1 + len);
      buf[0] = TYPE_STRING;
      buf.write(val, 1, len, "utf8");
      return buf;
    }
    if (val instanceof Uint8Array || Buffer.isBuffer(val)) {
      if (val.byteLength === 0) {
        return BUF_EMPTY_BYTES;
      }
      const buf = Buffer.allocUnsafe(1 + val.byteLength);
      buf[0] = TYPE_BYTES;
      buf.set(val, 1);
      return buf;
    }
    // Complex object or array: serialize as JSON with BigInt, Buffer, and Uint8Array support
    const jsonStr = JSON.stringify(val, (_, v) => {
      if (typeof v === "bigint") {
        return { __tencere_type: "bigint", value: v.toString() };
      }
      if (Buffer.isBuffer(v)) {
        return { __tencere_type: "buffer", value: v.toString("base64") };
      }
      if (v instanceof Uint8Array) {
        return { __tencere_type: "uint8array", value: Buffer.from(v.buffer, v.byteOffset, v.byteLength).toString("base64") };
      }
      return v;
    });
    const len = Buffer.byteLength(jsonStr);
    const buf = Buffer.allocUnsafe(1 + len);
    buf[0] = TYPE_JSON;
    buf.write(jsonStr, 1, len, "utf8");
    return buf;
  }

  /**
   * Decodes a binary Buffer into a JS value based on the 1-byte type tag.
   *
   * @param {Uint8Array} buf
   * @returns {any}
   */
  static decode(buf) {
    if (!buf || buf.byteLength === 0) {
      return undefined;
    }
    const type = buf[0];

    switch (type) {
      case TYPE_UNDEFINED:
        return undefined;
      case TYPE_NULL:
        return null;
      case TYPE_BOOLEAN_TRUE:
        return true;
      case TYPE_BOOLEAN_FALSE:
        return false;
      case TYPE_INT32:
        return (buf[1] << 24) | (buf[2] << 16) | (buf[3] << 8) | buf[4];
      case TYPE_INT64:
        return Buffer.from(buf.buffer, buf.byteOffset, buf.byteLength).readBigInt64BE(1);
      case TYPE_FLOAT64:
        return Buffer.from(buf.buffer, buf.byteOffset, buf.byteLength).readDoubleBE(1);
      case TYPE_STRING: {
        if (buf.byteLength <= 1) return "";
        return Buffer.from(buf.buffer, buf.byteOffset + 1, buf.byteLength - 1).toString("utf8");
      }
      case TYPE_BYTES: {
        if (buf.byteLength <= 1) return new Uint8Array(0);
        return new Uint8Array(buf.buffer, buf.byteOffset + 1, buf.byteLength - 1);
      }
      case TYPE_JSON: {
        const str = Buffer.from(buf.buffer, buf.byteOffset + 1, buf.byteLength - 1).toString("utf8");
        return JSON.parse(str, (_, v) => {
          if (v && typeof v === "object") {
            if (v.__tencere_type === "bigint") return BigInt(v.value);
            if (v.__tencere_type === "buffer") return Buffer.from(v.value, "base64");
            if (v.__tencere_type === "uint8array") return new Uint8Array(Buffer.from(v.value, "base64"));
            if (v.type === "Buffer" && Array.isArray(v.data)) return Buffer.from(v.data);
          }
          return v;
        });
      }
      default:
        // Fallback: raw slice
        return buf.subarray(1);
    }
  }

  /**
   * Returns human-readable type tag name.
   */
  static typeName(typeTag) {
    switch (typeTag) {
      case TYPE_BYTES:
        return "bytes";
      case TYPE_STRING:
        return "string";
      case TYPE_INT32:
        return "int32";
      case TYPE_INT64:
        return "int64";
      case TYPE_FLOAT64:
        return "float64";
      case TYPE_BOOLEAN_TRUE:
      case TYPE_BOOLEAN_FALSE:
        return "boolean";
      case TYPE_NULL:
        return "null";
      case TYPE_UNDEFINED:
        return "undefined";
      case TYPE_JSON:
        return "json";
      default:
        return "unknown";
    }
  }
}
