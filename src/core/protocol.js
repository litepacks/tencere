/**
 * Native binary wire protocol for Tencere client/server communication.
 *
 * Frame Format:
 *  [Magic: 4B ('TNCR' = 0x544E4352)]
 *  [Version: 2B (uint16 = 1)]
 *  [RequestId: 8B (uint64)]
 *  [OpCode: 2B (uint16)]
 *  [Flags: 2B (uint16)]
 *  [PayloadLength: 4B (uint32)]
 *  [Payload: N bytes]
 */

import { BinaryCodec } from "./binary-codec.js";

export const MAGIC_TNCR = 0x544e4352;
export const PROTOCOL_VERSION = 1;
export const HEADER_SIZE = 22; // 4 + 2 + 8 + 2 + 2 + 4

export const OP_PING = 0x0001;
export const OP_GET = 0x0002;
export const OP_SET = 0x0003;
export const OP_DEL = 0x0004;
export const OP_HAS = 0x0005;
export const OP_INCR = 0x0006;
export const OP_PATCH = 0x0007;
export const OP_STATS = 0x0008;
export const OP_KEYS = 0x0009;
export const OP_CLEAR = 0x000a;
export const OP_EXEC = 0x000b;
export const OP_WATCH = 0x000c;
export const OP_UNWATCH = 0x000d;

export const RESP_OK = 0x8001;
export const RESP_ERR = 0x8002;
export const RESP_EVENT = 0x8003;

export class ProtocolFrame {
  constructor(params) {
    this.requestId = params.requestId;
    this.op = params.op;
    this.flags = params.flags || 0;
    this.payload = params.payload !== undefined ? params.payload : null;
  }

  /**
   * Encodes a protocol frame into a binary Buffer.
   *
   * @returns {Uint8Array}
   */
  encode() {
    const payloadBytes =
      this.payload !== null && this.payload !== undefined
        ? BinaryCodec.encode(this.payload)
        : new Uint8Array(0);

    const totalLen = HEADER_SIZE + payloadBytes.byteLength;
    const buf = new Uint8Array(totalLen);
    const view = new DataView(buf.buffer, buf.byteOffset, totalLen);

    let offset = 0;
    view.setUint32(offset, MAGIC_TNCR, false);
    offset += 4;

    view.setUint16(offset, PROTOCOL_VERSION, false);
    offset += 2;

    view.setBigUint64(offset, BigInt(this.requestId || 0), false);
    offset += 8;

    view.setUint16(offset, this.op, false);
    offset += 2;

    view.setUint16(offset, this.flags, false);
    offset += 2;

    view.setUint32(offset, payloadBytes.byteLength, false);
    offset += 4;

    if (payloadBytes.byteLength > 0) {
      buf.set(payloadBytes, offset);
    }

    return buf;
  }
}

/**
 * Streaming Protocol Parser handling TCP chunk fragmentation and reassembly.
 */
export class ProtocolParser {
  constructor(onFrame) {
    this.onFrame = onFrame;
    this.buffer = new Uint8Array(0);
  }

  /**
   * Pushes incoming TCP data chunks.
   *
   * @param {Uint8Array} chunk
   */
  push(chunk) {
    if (this.buffer.byteLength === 0) {
      this.buffer = chunk;
    } else {
      const combined = new Uint8Array(this.buffer.byteLength + chunk.byteLength);
      combined.set(this.buffer);
      combined.set(chunk, this.buffer.byteLength);
      this.buffer = combined;
    }

    while (this.buffer.byteLength >= HEADER_SIZE) {
      const view = new DataView(
        this.buffer.buffer,
        this.buffer.byteOffset,
        this.buffer.byteLength
      );
      const magic = view.getUint32(0, false);
      if (magic !== MAGIC_TNCR) {
        throw new Error(`Invalid protocol magic: 0x${magic.toString(16)}`);
      }

      const version = view.getUint16(4, false);
      const requestId = Number(view.getBigUint64(6, false));
      const op = view.getUint16(14, false);
      const flags = view.getUint16(16, false);
      const payloadLen = view.getUint32(18, false);

      const frameTotal = HEADER_SIZE + payloadLen;
      if (this.buffer.byteLength < frameTotal) {
        // Need more data
        break;
      }

      const payloadSlice = this.buffer.subarray(HEADER_SIZE, frameTotal);
      const payload = payloadLen > 0 ? BinaryCodec.decode(payloadSlice) : null;

      // Slice remaining buffer
      this.buffer = this.buffer.subarray(frameTotal);

      this.onFrame(new ProtocolFrame({ requestId, op, flags, payload }));
    }
  }
}
