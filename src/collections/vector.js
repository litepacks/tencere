/**
 * Vector collection primitive for high-performance embeddings similarity search.
 * Supports cosine similarity, euclidean distance, and dot product.
 */

class TopKHeap {
  constructor(k) {
    this.k = Math.max(0, k);
    this.heap = [];
  }

  push(score, doc) {
    if (this.k <= 0) return;
    if (this.heap.length < this.k) {
      this.heap.push({ id: doc.id, score, value: doc.value, vector: doc.vector });
      this._siftUp(this.heap.length - 1);
    } else if (score > this.heap[0].score) {
      this.heap[0] = { id: doc.id, score, value: doc.value, vector: doc.vector };
      this._siftDown(0);
    }
  }

  _siftUp(i) {
    const heap = this.heap;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (heap[i].score < heap[parent].score) {
        const tmp = heap[i];
        heap[i] = heap[parent];
        heap[parent] = tmp;
        i = parent;
      } else {
        break;
      }
    }
  }

  _siftDown(i) {
    const heap = this.heap;
    const len = heap.length;
    while (true) {
      let smallest = i;
      const left = (i << 1) + 1;
      const right = left + 1;

      if (left < len && heap[left].score < heap[smallest].score) {
        smallest = left;
      }
      if (right < len && heap[right].score < heap[smallest].score) {
        smallest = right;
      }
      if (smallest !== i) {
        const tmp = heap[i];
        heap[i] = heap[smallest];
        heap[smallest] = tmp;
        i = smallest;
      } else {
        break;
      }
    }
  }

  toSortedArray() {
    return this.heap.sort((a, b) => b.score - a.score);
  }
}

export class VectorCollection {
  /**
   * @param {import('../core/engine.js').TencereEngine} engine
   * @param {string} name
   * @param {object} [options={}]
   * @param {number} [options.dimensions]
   * @param {'cosine'|'euclidean'|'dotProduct'} [options.metric='cosine']
   */
  constructor(engine, name, options = {}) {
    this._engine = engine;
    this._name = name;
    this._dimensions = options.dimensions || 0;
    this._metric = options.metric || "cosine";
    this._prefix = `__vec:${name}:`;

    // Local cached norms for cosine speedup: id -> norm
    this._norms = new Map();
    // In-memory vector document index: id -> { id, vector, norm, value }
    this._docs = new Map();
    this._loaded = false;
  }

  _vKey(id) {
    return `${this._prefix}${id}`;
  }

  _strip(key) {
    return key.slice(this._prefix.length);
  }

  _ensureLoaded() {
    if (this._loaded) return;
    this._loaded = true;
    const rawDocs = this._engine.storage.scan(this._prefix);
    for (const item of rawDocs) {
      const doc = item.value;
      if (doc && doc.id && doc.vector && !this._docs.has(doc.id)) {
        this._docs.set(doc.id, doc);
        if (doc.norm) {
          this._norms.set(doc.id, doc.norm);
        }
      }
    }
  }

  _norm(v) {
    let sum = 0;
    for (let i = 0; i < v.length; i++) {
      sum += v[i] * v[i];
    }
    return Math.sqrt(sum);
  }

  _cosine(a, b, normA, normB) {
    let dot = 0;
    const len = a.length <= b.length ? a.length : b.length;
    for (let i = 0; i < len; i++) {
      dot += a[i] * b[i];
    }
    const denom = normA * normB;
    return denom === 0 ? 0 : dot / denom;
  }

  _euclidean(a, b) {
    let sum = 0;
    const len = a.length <= b.length ? a.length : b.length;
    for (let i = 0; i < len; i++) {
      const diff = a[i] - b[i];
      sum += diff * diff;
    }
    return 1 / (1 + Math.sqrt(sum));
  }

  _dot(a, b) {
    let sum = 0;
    const len = a.length <= b.length ? a.length : b.length;
    for (let i = 0; i < len; i++) {
      sum += a[i] * b[i];
    }
    return sum;
  }

  /**
   * Sets or updates a vector document.
   *
   * @param {string} id
   * @param {object} doc
   * @param {number[]|Float32Array} doc.vector
   * @param {any} [doc.value]
   * @returns {Promise<void>}
   */
  async set(id, docOrVector, maybeValue) {
    let doc = docOrVector;
    if (Array.isArray(docOrVector) || docOrVector instanceof Float32Array) {
      doc = { vector: docOrVector, value: maybeValue };
    }
    const rawVector = doc?.vector !== undefined ? doc.vector : doc;
    const vector = Array.isArray(rawVector) ? rawVector : Array.from(rawVector);
    const norm = this._norm(vector);
    this._norms.set(id, norm);

    const record = {
      id,
      vector,
      norm,
      value: doc.value !== undefined ? doc.value : null
    };

    this._docs.set(id, record);
    await this._engine.set(this._vKey(id), record);
  }

  /**
   * Retrieves a document by id.
   *
   * @param {string} id
   * @returns {Promise<object|null>}
   */
  async get(id) {
    if (this._docs.has(id)) {
      return this._docs.get(id);
    }
    const doc = await this._engine.get(this._vKey(id));
    if (doc) {
      this._docs.set(id, doc);
    }
    return doc;
  }

  /**
   * Deletes a vector document by id.
   *
   * @param {string} id
   * @returns {Promise<boolean>}
   */
  async delete(id) {
    this._norms.delete(id);
    this._docs.delete(id);
    return this._engine.delete(this._vKey(id));
  }

  /**
   * Searches for top-K most similar documents.
   *
   * @param {number[]|Float32Array} queryVector
   * @param {object} [options={}]
   * @param {number} [options.topK=10]
   * @param {function(object): boolean} [options.filter]
   * @returns {Promise<Array<{ id: string, score: number, value: any, vector: number[] }>>}
   */
  async search(queryVector, options = {}) {
    this._ensureLoaded();
    const qVec = Array.isArray(queryVector) ? queryVector : Array.from(queryVector);
    const qNorm = this._norm(qVec);
    const topK = options.topK || 10;
    if (topK <= 0) return [];
    const filter = options.filter || null;

    const heap = new TopKHeap(topK);

    for (const doc of this._docs.values()) {
      if (!doc || !doc.vector) continue;

      if (filter && !filter(doc)) {
        continue;
      }

      let score = 0;
      if (this._metric === "cosine") {
        const dNorm = doc.norm || this._norm(doc.vector);
        score = this._cosine(qVec, doc.vector, qNorm, dNorm);
      } else if (this._metric === "euclidean") {
        score = this._euclidean(qVec, doc.vector);
      } else {
        score = this._dot(qVec, doc.vector);
      }

      heap.push(score, doc);
    }

    return heap.toSortedArray();
  }

  /**
   * Clears all vector documents in this collection.
   *
   * @returns {Promise<void>}
   */
  async clear() {
    this._ensureLoaded();
    const ids = Array.from(this._docs.keys());
    for (const id of ids) {
      await this.delete(id);
    }
  }

  /**
   * Count of vector documents.
   */
  async count() {
    this._ensureLoaded();
    return this._docs.size;
  }
}
