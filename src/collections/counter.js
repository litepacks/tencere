/**
 * First-class Counter abstraction for Tencere.
 */

export class Counter {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} key
   */
  constructor(engine, key) {
    this._engine = engine;
    this._key = key;
  }

  /**
   * Increments counter by 1 (or delta).
   *
   * @param {number} [delta=1]
   * @returns {Promise<number>}
   */
  async inc(delta = 1) {
    return this._engine.increment(this._key, delta);
  }

  /**
   * Adds specified delta to counter.
   *
   * @param {number} delta
   * @returns {Promise<number>}
   */
  async add(delta) {
    return this._engine.increment(this._key, delta);
  }

  /**
   * Decrements counter by 1 (or delta).
   *
   * @param {number} [delta=1]
   * @returns {Promise<number>}
   */
  async dec(delta = 1) {
    return this._engine.increment(this._key, -delta);
  }

  /**
   * Returns current counter value.
   *
   * @returns {Promise<number>}
   */
  async value() {
    const val = await this._engine.get(this._key);
    return val !== undefined ? Number(val) : 0;
  }

  /**
   * Resets counter to zero (or specified initial value).
   *
   * @param {number} [val=0]
   * @returns {Promise<void>}
   */
  async reset(val = 0) {
    await this._engine.set(this._key, val);
  }

  /**
   * History inspection for this counter.
   *
   * @param {object} [options={}]
   * @returns {AsyncGenerator<object>}
   */
  history(options = {}) {
    return this._engine.history(this._key, options);
  }

  /**
   * Returns a historical view of this counter.
   *
   * @param {string|number|Date|object} target
   * @returns {import('../history/view.js').HistoricalCounter}
   */
  at(target) {
    return this._engine.at(target).counter(this._key);
  }

  /**
   * Rollback counter to previous state or target version/time.
   *
   * @param {object} [options={}]
   * @returns {Promise<{ fromVersion: bigint, restoredVersion: bigint, newVersion: bigint, sequence: bigint }>}
   */
  async rollback(options = {}) {
    return this._engine.rollback(this._key, options);
  }
}
