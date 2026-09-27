/**
 * Set collection primitive.
 */

export class SetCollection {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} name
   */
  constructor(engine, name) {
    this._engine = engine;
    this._name = name;
    this._prefix = `__set:${name}:`;
  }

  _k(member) {
    return `${this._prefix}${member}`;
  }

  _strip(fullKey) {
    return fullKey.slice(this._prefix.length);
  }

  async add(member) {
    await this._engine.set(this._k(member), 1);
    return true;
  }

  async delete(member) {
    return this._engine.delete(this._k(member));
  }

  async has(member) {
    return this._engine.has(this._k(member));
  }

  async members() {
    const rawKeys = this._engine.storage.keys(this._prefix);
    return rawKeys.map((k) => this._strip(k));
  }

  async size() {
    return this._engine.storage.keys(this._prefix).length;
  }

  async clear() {
    const keys = this._engine.storage.keys(this._prefix);
    for (const k of keys) {
      await this._engine.delete(k);
    }
  }
}
