/**
 * Internal test-only fault injection framework for Tencere.
 * Simulates system crashes, disk I/O errors, torn writes, and out-of-order failures.
 * NOT exposed in public production APIs.
 */

export class FaultInjectionError extends Error {
  constructor(point, details = {}) {
    super(`Simulated fault at point '${point}': ${JSON.stringify(details)}`);
    this.name = "FaultInjectionError";
    this.code = "ERR_SIMULATED_FAULT";
    this.point = point;
    this.details = details;
  }
}

export class FaultInjector {
  constructor() {
    this._rules = new Map();
    this._counts = new Map();
    this.enabled = false;
  }

  /**
   * Injects a fault rule.
   *
   * @param {string} point
   * @param {object} [options={}]
   * @param {number} [options.occurrence=1] Fire on Nth hit
   * @param {Error} [options.error] Custom error to throw
   */
  inject(point, options = {}) {
    this.enabled = true;
    this._rules.set(point, {
      occurrence: options.occurrence ?? 1,
      error: options.error || null
    });
    this._counts.set(point, 0);
  }

  /**
   * Clears all injected faults.
   */
  clear() {
    this._rules.clear();
    this._counts.clear();
    this.enabled = false;
  }

  /**
   * Trigger point in execution path. Throws if matching rule condition met.
   *
   * @param {string} point
   */
  trigger(point) {
    if (!this.enabled || !this._rules.has(point)) return;

    const count = (this._counts.get(point) || 0) + 1;
    this._counts.set(point, count);

    const rule = this._rules.get(point);
    if (count === rule.occurrence) {
      if (rule.error) {
        throw rule.error;
      }
      throw new FaultInjectionError(point, { count });
    }
  }
}
