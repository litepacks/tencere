/**
 * Reactive watch implementation with bounded buffering and backpressure.
 */

export class WatchStream {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} [pattern=""]
   * @param {object} [options={}]
   * @param {number} [options.highWaterMark=1000]
   */
  constructor(engine, pattern = "", options = {}) {
    this._engine = engine;
    this._pattern = pattern;
    this._highWaterMark = options.highWaterMark || 1000;
  }

  _matches(key) {
    if (!this._pattern) return true;
    if (this._pattern.endsWith("*")) {
      return key.startsWith(this._pattern.slice(0, -1));
    }
    return key === this._pattern || key.startsWith(this._pattern);
  }

  /**
   * Async iterator yielding database mutation changes.
   *
   * @returns {AsyncGenerator<{ key: string, type: string, value: any, previousValue: any, version: number, timestamp: number }>}
   */
  async *[Symbol.asyncIterator]() {
    const queue = [];
    let notify = null;
    let closed = false;

    const listener = (event) => {
      if (closed || !this._matches(event.key)) return;

      // Bounded buffer backpressure: drop oldest if exceeded highWaterMark
      if (queue.length >= this._highWaterMark) {
        queue.shift();
      }

      queue.push(event);

      if (notify) {
        const fn = notify;
        notify = null;
        fn();
      }
    };

    this._engine.events.on("change", listener);

    try {
      while (!closed && !this._engine.isClosed) {
        while (queue.length > 0) {
          yield queue.shift();
        }

        if (closed || this._engine.isClosed) break;

        await new Promise((resolve) => {
          notify = resolve;
        });
      }
    } finally {
      closed = true;
      this._engine.events.removeListener("change", listener);
      if (notify) {
        const fn = notify;
        notify = null;
        fn();
      }
    }
  }
}
