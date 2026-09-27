/**
 * Compact binary codec for TimeSeries points.
 *
 * Each point occupies exactly 24 bytes:
 *  - [Timestamp: 8B int64 Big-Endian] (epoch milliseconds)
 *  - [Sequence: 8B uint64 Big-Endian] (Tencere sequence)
 *  - [Value: 8B double Big-Endian]    (IEEE 754 64-bit float)
 */

import { InvalidTimeSeriesValueError } from "../errors.js";

export const POINT_RECORD_SIZE = 24;

export class PointCodec {
  /**
   * Encodes a point into a compact 24-byte Buffer.
   *
   * @param {number} timestamp
   * @param {bigint|number} sequence
   * @param {number} value
   * @returns {Buffer}
   */
  static encode(timestamp, sequence, value) {
    const num = Number(value);
    if (typeof value !== "number" || !Number.isFinite(num)) {
      throw new InvalidTimeSeriesValueError(
        `TimeSeries value must be a finite number, received: ${value}`
      );
    }
    const buf = Buffer.allocUnsafe(POINT_RECORD_SIZE);
    buf.writeBigInt64BE(BigInt(timestamp), 0);
    buf.writeBigUint64BE(BigInt(sequence), 8);
    buf.writeDoubleBE(num, 16);
    return buf;
  }

  /**
   * Decodes a 24-byte Buffer into a point.
   *
   * @param {Buffer|Uint8Array} buf
   * @param {number} [offset=0]
   * @returns {{ timestamp: number, sequence: bigint, value: number }}
   */
  static decode(buf, offset = 0) {
    const view = Buffer.isBuffer(buf)
      ? buf
      : Buffer.from(buf.buffer, buf.byteOffset + offset, POINT_RECORD_SIZE);
    const readOffset = Buffer.isBuffer(buf) ? offset : 0;

    const ts = Number(view.readBigInt64BE(readOffset));
    const seq = view.readBigUint64BE(readOffset + 8);
    const val = view.readDoubleBE(readOffset + 16);

    return {
      timestamp: ts,
      sequence: seq,
      value: val
    };
  }
}
