/**
 * AgentMemory: Agent memory layer combining vector similarity, importance, recency, and TTL.
 */

import crypto from "node:crypto";

export class AgentMemory {
  /**
   * @param {import('../index.js').Tencere} db
   */
  constructor(db) {
    this._db = db;
    this._vectors = db.vector("__agent_memories");
  }

  /**
   * Adds a memory record for an agent.
   *
   * @param {string} agentId
   * @param {object} item
   * @param {string} item.content
   * @param {number} [item.importance=0.5]
   * @param {number[]|Float32Array} [item.embedding]
   * @param {object} [item.metadata]
   * @returns {Promise<string>} memory id
   */
  async add(agentId, item) {
    const memId = `${agentId}:${crypto.randomUUID()}`;
    const embedding = item.embedding || new Array(128).fill(0);
    const importance = item.importance !== undefined ? item.importance : 0.5;

    await this._vectors.set(memId, {
      vector: embedding,
      value: {
        agentId,
        content: item.content,
        importance,
        metadata: item.metadata || {},
        createdAt: Date.now(),
        accessCount: 0
      }
    });

    return memId;
  }

  /**
   * Recalls top-K relevant memories for an agent.
   * Ranks by combined: similarity (50%) + importance (30%) + recency (20%).
   *
   * @param {string} agentId
   * @param {object} options
   * @param {number[]|Float32Array} options.embedding
   * @param {number} [options.topK=5]
   * @returns {Promise<Array<{ id: string, content: string, score: number, importance: number, createdAt: number }>>}
   */
  async recall(agentId, options = {}) {
    const embedding = options.embedding;
    const topK = options.topK || 5;
    const now = Date.now();

    const matches = await this._vectors.search(embedding || new Array(128).fill(0), {
      topK: topK * 3,
      filter: (doc) => doc.value && doc.value.agentId === agentId
    });

    const ranked = matches.map((m) => {
      const val = m.value;
      const sim = m.score; // 0 to 1
      const importance = val.importance || 0.5;
      const ageHours = (now - val.createdAt) / (1000 * 3600);
      const recency = Math.exp(-ageHours / 24); // exponential decay over 24h

      const compositeScore = sim * 0.5 + importance * 0.3 + recency * 0.2;
      return {
        id: m.id,
        content: val.content,
        score: compositeScore,
        similarity: sim,
        importance,
        createdAt: val.createdAt
      };
    });

    ranked.sort((a, b) => b.score - a.score);
    return ranked.slice(0, topK);
  }
}
