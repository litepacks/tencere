/**
 * Generic high-performance OrderedIndex backed by a SkipList with span tracking.
 *
 * Supports O(log N) insertions, deletions, score updates, rank lookups, and range scans.
 * Does not duplicate values; indexes (score, member) tuples.
 */

const SKIPLIST_MAXLEVEL = 32;
const SKIPLIST_P = 0.25;

class SkipListNode {
  constructor(level, score, member) {
    this.score = score;
    this.member = member;
    this.forward = new Array(level).fill(null);
    this.span = new Array(level).fill(0);
    this.backward = null;
  }
}

export class OrderedIndex {
  constructor() {
    this.header = new SkipListNode(SKIPLIST_MAXLEVEL, -Infinity, null);
    this.tail = null;
    this.length = 0;
    this.level = 1;
    this.dict = new Map(); // member -> SkipListNode
  }

  static randomLevel() {
    let level = 1;
    while (Math.random() < SKIPLIST_P && level < SKIPLIST_MAXLEVEL) {
      level += 1;
    }
    return level;
  }

  /**
   * Inserts or updates a member with a given score.
   *
   * @param {number} score
   * @param {string} member
   * @returns {number} 1 if added, 0 if updated
   */
  insert(score, member) {
    if (this.dict.has(member)) {
      const existingNode = this.dict.get(member);
      if (existingNode.score === score) {
        return 0;
      }
      this.delete(member);
    }

    const update = new Array(SKIPLIST_MAXLEVEL);
    const rank = new Array(SKIPLIST_MAXLEVEL).fill(0);
    let x = this.header;

    for (let i = this.level - 1; i >= 0; i--) {
      rank[i] = i === this.level - 1 ? 0 : rank[i + 1];
      while (
        x.forward[i] &&
        (x.forward[i].score < score ||
          (x.forward[i].score === score && x.forward[i].member < member))
      ) {
        rank[i] += x.span[i];
        x = x.forward[i];
      }
      update[i] = x;
    }

    const level = OrderedIndex.randomLevel();
    if (level > this.level) {
      for (let i = this.level; i < level; i++) {
        rank[i] = 0;
        update[i] = this.header;
        update[i].span[i] = this.length;
      }
      this.level = level;
    }

    const node = new SkipListNode(level, score, member);
    for (let i = 0; i < level; i++) {
      node.forward[i] = update[i].forward[i];
      update[i].forward[i] = node;

      node.span[i] = update[i].span[i] - (rank[0] - rank[i]);
      update[i].span[i] = rank[0] - rank[i] + 1;
    }

    for (let i = level; i < this.level; i++) {
      update[i].span[i] += 1;
    }

    node.backward = update[0] === this.header ? null : update[0];
    if (node.forward[0]) {
      node.forward[0].backward = node;
    } else {
      this.tail = node;
    }

    this.length += 1;
    this.dict.set(member, node);
    return 1;
  }

  /**
   * Deletes a member from the index.
   *
   * @param {string} member
   * @returns {boolean} true if deleted
   */
  delete(member) {
    const node = this.dict.get(member);
    if (!node) return false;

    const score = node.score;
    const update = new Array(SKIPLIST_MAXLEVEL);
    let x = this.header;

    for (let i = this.level - 1; i >= 0; i--) {
      while (
        x.forward[i] &&
        (x.forward[i].score < score ||
          (x.forward[i].score === score && x.forward[i].member < member))
      ) {
        x = x.forward[i];
      }
      update[i] = x;
    }

    for (let i = 0; i < this.level; i++) {
      if (update[i].forward[i] === node) {
        update[i].span[i] += node.span[i] - 1;
        update[i].forward[i] = node.forward[i];
      } else {
        update[i].span[i] -= 1;
      }
    }

    if (node.forward[0]) {
      node.forward[0].backward = node.backward;
    } else {
      this.tail = node.backward;
    }

    while (this.level > 1 && !this.header.forward[this.level - 1]) {
      this.level -= 1;
    }

    this.length -= 1;
    this.dict.delete(member);
    return true;
  }

  /**
   * Returns the score for a member, or undefined if not found.
   */
  score(member) {
    const node = this.dict.get(member);
    return node ? node.score : undefined;
  }

  /**
   * Checks if member exists.
   */
  has(member) {
    return this.dict.has(member);
  }

  /**
   * Returns 0-based rank of member in sorted order.
   * By default ascending (lowest score = 0).
   *
   * @param {string} member
   * @param {object} [options={}]
   * @param {boolean} [options.reverse=false] If true, highest score = 0
   * @returns {number|undefined}
   */
  rank(member, options = {}) {
    const target = this.dict.get(member);
    if (!target) return undefined;

    let rank = 0;
    let x = this.header;
    const score = target.score;

    for (let i = this.level - 1; i >= 0; i--) {
      while (
        x.forward[i] &&
        (x.forward[i].score < score ||
          (x.forward[i].score === score && x.forward[i].member <= member))
      ) {
        rank += x.span[i];
        x = x.forward[i];
      }
      if (x && x.member === member) {
        break;
      }
    }

    const ascRank = rank - 1;
    if (options.reverse) {
      return this.length - 1 - ascRank;
    }
    return ascRank;
  }

  /**
   * Returns the node at 0-based rank.
   */
  getNodeByRank(rank) {
    if (rank < 0 || rank >= this.length) return null;
    let traversed = 0;
    let x = this.header;

    for (let i = this.level - 1; i >= 0; i--) {
      while (x.forward[i] && traversed + x.span[i] <= rank + 1) {
        traversed += x.span[i];
        x = x.forward[i];
      }
      if (traversed === rank + 1) {
        return x;
      }
    }
    return null;
  }

  /**
   * Range query by score.
   *
   * @param {number} min
   * @param {number} max
   * @param {object} [options={}]
   * @param {number} [options.offset=0]
   * @param {number} [options.limit=Infinity]
   * @param {boolean} [options.reverse=false]
   * @returns {Array<{ member: string, score: number }>}
   */
  rangeByScore(min, max, options = {}) {
    const { offset = 0, limit = Infinity, reverse = false } = options;
    const result = [];
    if (min > max || this.length === 0 || limit <= 0) return result;

    if (!reverse) {
      // Find first node >= min
      let x = this.header;
      for (let i = this.level - 1; i >= 0; i--) {
        while (x.forward[i] && x.forward[i].score < min) {
          x = x.forward[i];
        }
      }
      x = x.forward[0];

      let skipped = 0;
      while (x && x.score <= max && result.length < limit) {
        if (skipped < offset) {
          skipped++;
        } else {
          result.push({ member: x.member, score: x.score });
        }
        x = x.forward[0];
      }
    } else {
      // Find last node <= max
      let x = this.header;
      for (let i = this.level - 1; i >= 0; i--) {
        while (x.forward[i] && x.forward[i].score <= max) {
          x = x.forward[i];
        }
      }

      let skipped = 0;
      while (x && x !== this.header && x.score >= min && result.length < limit) {
        if (skipped < offset) {
          skipped++;
        } else {
          result.push({ member: x.member, score: x.score });
        }
        x = x.backward;
      }
    }

    return result;
  }

  /**
   * Returns top N elements (highest scores first).
   *
   * @param {number} n
   * @returns {Array<{ member: string, score: number }>}
   */
  top(n = 10) {
    const result = [];
    let x = this.tail;
    while (x && result.length < n) {
      result.push({ member: x.member, score: x.score });
      x = x.backward;
    }
    return result;
  }

  /**
   * Returns bottom N elements (lowest scores first).
   *
   * @param {number} n
   * @returns {Array<{ member: string, score: number }>}
   */
  bottom(n = 10) {
    const result = [];
    let x = this.header.forward[0];
    while (x && result.length < n) {
      result.push({ member: x.member, score: x.score });
      x = x.forward[0];
    }
    return result;
  }

  /**
   * Clears the index.
   */
  clear() {
    this.header = new SkipListNode(SKIPLIST_MAXLEVEL, -Infinity, null);
    this.tail = null;
    this.length = 0;
    this.level = 1;
    this.dict.clear();
  }
}
