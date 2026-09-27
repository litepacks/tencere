/**
 * Mutation operation definitions and binary serialization for Tencere WAL (Daktilo)
 * and replication streams (Raptiye).
 */

import { BinaryCodec } from "./binary-codec.js";

export const OP_SET = 0x01;
export const OP_DEL = 0x02;
export const OP_EXPIRE = 0x03;
export const OP_INCR = 0x04;
export const OP_PATCH = 0x05;
export const OP_SORTED_SET = 0x06;
export const OP_SORTED_DEL = 0x07;
export const OP_STREAM_APPEND = 0x08;
export const OP_LOCK_ACQUIRE = 0x09;
export const OP_LOCK_RELEASE = 0x0a;
export const OP_LOCK_RENEW = 0x0b;
export const OP_LOCK_EXPIRE = 0x0c;
export const OP_CHECKPOINT = 0x0f;
export const OP_RESTORE = 0x10;
export const OP_RESTORE_BEGIN = 0x11;
export const OP_RESTORE_COMMIT = 0x12;
export const OP_TS_ADD = 0x13;
export const OP_TS_DELETE = 0x14;
export const OP_TS_CORRECT = 0x15;
export const OP_TS_BATCH = 0x16;

export const FLAG_SLIDING = 0x01;
export const FLAG_CONSUME = 0x02;
export const FLAG_HAS_VERSION = 0x04;
export const FLAG_HAS_TTL = 0x08;
export const FLAG_RESTORE = 0x10;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export class Operation {
  /**
   * @param {object} params
   * @param {number} params.op - OpCode
   * @param {number} [params.partition=0]
   * @param {string} params.key
   * @param {number} [params.flags=0]
   * @param {bigint|number} [params.version=0n]
   * @param {number} [params.ttlMs=0]
   * @param {any} [params.value]
   * @param {number} [params.timestamp=Date.now()]
   * @param {any} [params.extra]
   */
  constructor(params) {
    this.op = params.op;
    this.partition = params.partition || 0;
    this.key = params.key;
    this.flags = params.flags || 0;
    this.version = params.version !== undefined ? BigInt(params.version) : 0n;
    this.ttlMs = params.ttlMs || 0;
    this.value = params.value;
    this.timestamp = params.timestamp || Date.now();
    this.extra = params.extra || null;
  }

  /**
   * Encodes the operation into a compact binary Buffer.
   * Format:
   *  [Op: 1B] [Partition: 2B] [Flags: 2B] [Timestamp: 8B] [Version: 8B] [TTL: 4B]
   *  [KeyLen: 2B] [Key bytes]
   *  [ExtraLen: 2B] [Extra JSON bytes]
   *  [ValueLen: 4B] [Value bytes]
   *
   * @returns {Uint8Array}
   */
  encode() {
    const keyBytes = textEncoder.encode(this.key);
    const extraBytes = this.extra ? textEncoder.encode(JSON.stringify(this.extra)) : null;
    const valueBytes = this.value !== undefined ? BinaryCodec.encode(this.value) : null;

    const extraLen = extraBytes ? extraBytes.byteLength : 0;
    const valueLen = valueBytes ? valueBytes.byteLength : 0;

    const headerLen = 1 + 2 + 2 + 8 + 8 + 4 + 2 + keyBytes.byteLength + 2 + extraLen + 4 + valueLen;
    const buf = new Uint8Array(headerLen);
    const view = new DataView(buf.buffer, buf.byteOffset, headerLen);

    let offset = 0;
    buf[offset++] = this.op;

    view.setUint16(offset, this.partition, false);
    offset += 2;

    view.setUint16(offset, this.flags, false);
    offset += 2;

    view.setBigUint64(offset, BigInt(this.timestamp), false);
    offset += 8;

    view.setBigUint64(offset, BigInt(this.version), false);
    offset += 8;

    view.setUint32(offset, this.ttlMs, false);
    offset += 4;

    view.setUint16(offset, keyBytes.byteLength, false);
    offset += 2;
    buf.set(keyBytes, offset);
    offset += keyBytes.byteLength;

    view.setUint16(offset, extraLen, false);
    offset += 2;
    if (extraBytes) {
      buf.set(extraBytes, offset);
      offset += extraLen;
    }

    view.setUint32(offset, valueLen, false);
    offset += 4;
    if (valueBytes) {
      buf.set(valueBytes, offset);
      offset += valueLen;
    }

    return buf;
  }

  /**
   * Decodes an Operation from a binary Buffer.
   *
   * @param {Uint8Array} buf
   * @returns {Operation}
   */
  static decode(buf) {
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let offset = 0;

    const op = buf[offset++];
    const partition = view.getUint16(offset, false);
    offset += 2;

    const flags = view.getUint16(offset, false);
    offset += 2;

    const timestamp = Number(view.getBigUint64(offset, false));
    offset += 8;

    // Backward compatibility: detect legacy 32-bit vs standard 64-bit version
    let isLegacy32Bit = false;
    if (buf.byteLength >= 23) {
      const candidateKeyLen4 = view.getUint16(21, false);
      const offset23 = 23 + candidateKeyLen4;
      if (offset23 + 6 <= buf.byteLength) {
        const extraLen = view.getUint16(offset23, false);
        const valOffset = offset23 + 2 + extraLen;
        if (valOffset + 4 <= buf.byteLength) {
          const valLen = view.getUint32(valOffset, false);
          if (valOffset + 4 + valLen === buf.byteLength) {
            isLegacy32Bit = true;
          }
        }
      }
    }

    let version = 0n;
    if (isLegacy32Bit) {
      version = BigInt(view.getUint32(offset, false));
      offset += 4;
    } else {
      version = view.getBigUint64(offset, false);
      offset += 8;
    }

    const ttlMs = view.getUint32(offset, false);
    offset += 4;

    const keyLen = view.getUint16(offset, false);
    offset += 2;
    const key = textDecoder.decode(buf.subarray(offset, offset + keyLen));
    offset += keyLen;

    const extraLen = view.getUint16(offset, false);
    offset += 2;
    let extra = null;
    if (extraLen > 0) {
      const extraJson = textDecoder.decode(buf.subarray(offset, offset + extraLen));
      extra = JSON.parse(extraJson);
      offset += extraLen;
    }

    const valueLen = view.getUint32(offset, false);
    offset += 4;
    let value = undefined;
    if (valueLen > 0) {
      value = BinaryCodec.decode(buf.subarray(offset, offset + valueLen));
      offset += valueLen;
    }

    return new Operation({
      op,
      partition,
      flags,
      timestamp,
      version,
      ttlMs,
      key,
      extra,
      value
    });
  }
}
