/**
 * TypeScript definitions for Tencere database.
 */

export interface SystemLimits {
  BINARY: {
    MAX_KEY_BYTES: number;
    MAX_EXTRA_BYTES: number;
    MAX_VALUE_BYTES: number;
    MAX_PARTITIONS: number;
    MAX_TTL_MS: number;
    MAX_VERSION: bigint;
  };
  CLUSTER: {
    MIN_NODES: number;
    RECOMMENDED_MAX_NODES: number;
    DEFAULT_HEARTBEAT_MS: number;
    DEFAULT_ELECTION_MIN_MS: number;
    DEFAULT_ELECTION_MAX_MS: number;
    MAX_RAFT_BATCH_BYTES: number;
    MAX_INFLIGHT_BATCHES: number;
  };
  TIMESERIES: {
    DEFAULT_MAX_SERIES: number;
    DEFAULT_MAX_TAGS: number;
    MAX_TAG_KEY_LENGTH: number;
    MAX_TAG_VALUE_LENGTH: number;
  };
  COORDINATION: {
    MAX_TIMEOUT_MS: number;
    DEFAULT_LOCK_TTL_MS: number;
    DEFAULT_SEMAPHORE_CAPACITY: number;
  };
  VECTOR: {
    RECOMMENDED_MAX_DIMENSIONS: number;
    DEFAULT_TOP_K: number;
  };
}

export const LIMITS: SystemLimits;

export interface TencereOptions {
  durability?: "strict" | "batch" | "async";
  batchWindowMs?: number;
  dataDir?: string;
  cluster?: ClusterConfig;
  history?: HistoryOptions;
}

export interface ClusterConfig {
  nodeId: string | number;
  peers?: (string | number)[];
  port?: number;
  host?: string;
  peerAddresses?: Record<string | number, string>;
  election?: {
    minTimeout?: number;
    maxTimeout?: number;
  };
  heartbeatInterval?: number;
  network?: any;
}

export interface ClusterMetrics {
  enabled: boolean;
  nodeId?: number;
  role?: string;
  term?: number;
  leaderId?: number | null;
  commitIndex?: number;
  lastApplied?: number;
  lastLogIndex?: number;
  replicationLag?: number;
  elections?: number;
  leaderChanges?: number;
  submitted?: number;
  committed?: number;
  applied?: number;
  bytes?: {
    wireBytes: number;
    payloadBytes: number;
    protocolOverheadBytes: number;
    copiedBytes: number;
  };
  [key: string]: any;
}

export interface ClusterNodeHealth {
  id?: number | null;
  nodeId?: number | null;
  role: string;
  address?: string | null;
  status: "ONLINE" | "UNREACHABLE";
  latencyMs?: number | null;
  isLeader: boolean;
  error?: string;
}

export interface ClusterQuorumInfo {
  required: number;
  reachable: number;
  total: number;
  hasQuorum: boolean;
}

export interface ClusterHealth {
  enabled: boolean;
  status: "HEALTHY" | "DEGRADED" | "QUORUM_LOST" | "STANDALONE";
  readiness: boolean;
  liveness: boolean;
  nodeId?: number;
  leader?: {
    id: number | null;
    address: string | null;
    isSelf: boolean;
  };
  leaderAddress?: string | null;
  term?: number;
  quorum?: ClusterQuorumInfo;
  nodes?: ClusterNodeHealth[];
  partitions?: {
    total: number;
    balanced: boolean;
  };
  metrics?: ClusterMetrics;
  message?: string;
  keys?: number;
  operations?: number;
  [key: string]: any;
}

export interface ClusterStatus {
  enabled: boolean;
  nodeId?: number;
  leaderId?: number | null;
  leaderAddress?: string | null;
  term?: number;
  isLeader?: boolean;
  role?: string;
  peers?: number[];
  peerAddresses?: Record<string | number, string>;
  metrics?: ClusterMetrics;
  [key: string]: any;
}

export class ClusterManager {
  readonly enabled: boolean;
  readonly nodeId: number;
  readonly peers: number[];
  readonly leaderId: number | null;
  readonly role: string;
  readonly term: number;
  isLeader(): boolean;
  waitForLeader(timeoutMs?: number): Promise<number | null>;
  getLeaderAddress(): string | null;
  status(): ClusterStatus;
  metrics(): ClusterMetrics;
  health(options?: { pingPeers?: boolean; timeoutMs?: number }): Promise<ClusterHealth>;
  replicate(op: any, options?: { ack?: "local" | "quorum" | "all"; timeoutMs?: number }): Promise<any>;
  stop(): Promise<void>;
  on(event: string, listener: (...args: any[]) => void): this;
  off(event: string, listener: (...args: any[]) => void): this;
  emit(event: string, ...args: any[]): boolean;
}

export interface HistoryOptions {
  enabled?: boolean;
  retention?: string | number;
  maxVersions?: number;
  include?: string[];
  exclude?: string[];
}

export interface SetOptions {
  ttl?: string | number;
  ifVersion?: bigint | number;
  ifNotExists?: boolean;
}

export interface GetOptions {
  at?: string | number | Date;
}

export interface PatchSpec {
  $set?: Record<string, any>;
  $inc?: Record<string, number>;
  $unset?: string[];
}

export interface ScopeOptions {
  delimiter?: string;
}

export interface CollectionOptions {
  [key: string]: any;
}

export interface VectorOptions {
  dimensions?: number;
  metric?: "cosine" | "euclidean" | "dot";
}

export interface TimeSeriesOptions {
  retention?: string | number;
  tags?: {
    indexed?: string[];
    maxTags?: number;
    maxKeyLength?: number;
    maxValueLength?: number;
    maxSeries?: number;
  };
}

export interface TimeSeriesAddOptions {
  at?: string | number | Date;
  tags?: Record<string, string>;
}

export interface TimeSeriesPoint {
  timestamp: number;
  sequence: bigint;
  value: number;
  tags?: Record<string, string>;
}

export type TimeSeriesBatchTuple = [number, number];

export interface TimeSeriesAddObject {
  at?: string | number | Date;
  value: number;
  tags?: Record<string, string>;
}

export interface BucketOptions {
  origin?: number;
}

export interface BucketAggregate {
  start: number;
  end: number;
  value: number | null;
  count: number;
}

export interface TimeSeriesWatchOptions {
  tags?: Record<string, string>;
}

export interface TimeSeriesDeleteOptions {
  from?: string | number | Date;
  to?: string | number | Date;
  tags?: Record<string, string>;
}

export interface TimeSeriesStats {
  points: number;
  series: number;
  oldestTimestamp: number | null;
  newestTimestamp: number | null;
  retention: string | number | null;
}

export interface LockOptions {
  ttl?: string | number;
}

export interface LockHandle {
  key: string;
  token: number;
  ownerId: string;
  ttlMs: number;
  release(): Promise<boolean>;
}

export interface OnceOptions {
  ttl?: string | number;
}

export interface IdempotencyOptions {
  ttl?: string | number;
}

export interface SemaphoreOptions {
  capacity?: number;
  timeout?: string | number;
}

export interface RateLimitOptions {
  limit: number;
  window: string | number;
}

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetMs: number;
}

export interface CacheOptions {
  ttl?: string | number;
  swr?: string | number;
}

export interface WaitForOptions {
  timeout?: string | number;
  pollInterval?: string | number;
}

export interface SnapshotOptions {
  label?: string;
  ttl?: string | number;
}

export interface SnapshotHandle {
  id: string;
  label?: string;
  timestamp: number;
  sequence: bigint;
}

export interface RollbackOptions {
  to?: string | number | Date;
  version?: bigint | number;
  sequence?: bigint | number;
}

export interface RollbackPlanOptions {
  to?: string | number | Date;
  version?: bigint | number;
  sequence?: bigint | number;
  prefix?: string;
}

export interface HistoryQueryOptions {
  limit?: number;
  from?: string | number | Date;
  to?: string | number | Date;
  prefix?: string;
}

export interface HistoricalRevision {
  key: string;
  version: bigint;
  sequence: bigint;
  timestamp: number;
  value: any;
  operation: number;
  ttlMs?: number;
}

export interface LockLeaseEvent {
  key: string;
  action: "acquire" | "release" | "renew" | "expire";
  ownerId: string;
  fencingToken: number;
  timestamp: number;
  ttlMs?: number;
}

export interface TencereStats {
  keys: number;
  expirations: number;
  ops: number;
  cacheHits: number;
  cacheMisses: number;
  daktiloHead: number;
  daktiloDurableHead: number;
  cluster?: any;
}

export class TimeSeriesQuery {
  where(tags: Record<string, string>): TimeSeriesQuery;
  between(from?: string | number | Date, to?: string | number | Date): TimeSeriesQuery;
  bucket(interval: string, options?: BucketOptions): TimeSeriesQuery;
  limit(count: number): TimeSeriesQuery;
  asc(): TimeSeriesQuery;
  desc(): TimeSeriesQuery;

  values(): Promise<TimeSeriesPoint[]>;
  iterate(): AsyncIterable<TimeSeriesPoint>;

  count(): Promise<number | BucketAggregate[]>;
  sum(): Promise<number | BucketAggregate[]>;
  min(): Promise<number | null | BucketAggregate[]>;
  max(): Promise<number | null | BucketAggregate[]>;
  avg(): Promise<number | null | BucketAggregate[]>;
  first(): Promise<TimeSeriesPoint | null | BucketAggregate[]>;
  last(): Promise<TimeSeriesPoint | null | BucketAggregate[]>;
}

export class TimeSeriesCollection {
  add(value: number, options?: TimeSeriesAddOptions): Promise<TimeSeriesPoint>;
  addMany(points: (TimeSeriesBatchTuple | TimeSeriesAddObject)[]): Promise<{ added: number }>;
  latest(): Promise<TimeSeriesPoint | null>;
  latest(count: number): Promise<TimeSeriesPoint[]>;
  between(from?: string | number | Date, to?: string | number | Date): TimeSeriesQuery;
  where(tags: Record<string, string>): TimeSeriesQuery;
  bucket(interval: string, options?: BucketOptions): TimeSeriesQuery;
  values(): Promise<TimeSeriesPoint[]>;
  iterate(): AsyncIterable<TimeSeriesPoint>;

  count(): Promise<number>;
  sum(): Promise<number>;
  min(): Promise<number | null>;
  max(): Promise<number | null>;
  avg(): Promise<number | null>;
  first(): Promise<TimeSeriesPoint | null>;
  last(): Promise<TimeSeriesPoint | null>;

  watch(options?: TimeSeriesWatchOptions): AsyncIterable<TimeSeriesPoint>;
  delete(range?: TimeSeriesDeleteOptions): Promise<{ deleted: number }>;
  correct(pointId: string, updates: { value: number }): Promise<TimeSeriesPoint>;
  purgeRetention(): Promise<{ purged: number }>;
  stats(): Promise<TimeSeriesStats>;
}

export class Counter {
  inc(delta?: number): Promise<number>;
  dec(delta?: number): Promise<number>;
  value(): Promise<number>;
  reset(val?: number): Promise<number>;
}

export class MapCollection<K = string, V = any> {
  get(key: K): Promise<V | undefined>;
  set(key: K, value: V, options?: SetOptions): Promise<void>;
  has(key: K): Promise<boolean>;
  delete(key: K): Promise<boolean>;
  entries(): Promise<[K, V][]>;
  keys(): Promise<K[]>;
  values(): Promise<V[]>;
  size(): Promise<number>;
  clear(): Promise<void>;
  watch(): AsyncIterable<{ type: "set" | "delete"; field: K; value?: V }>;
}

export class SetCollection<T = any> {
  add(member: T): Promise<boolean>;
  delete(member: T): Promise<boolean>;
  has(member: T): Promise<boolean>;
  members(): Promise<T[]>;
  size(): Promise<number>;
  clear(): Promise<void>;
}

export class SortedCollection {
  set(member: string, score: number): Promise<void>;
  incr(member: string, delta: number): Promise<number>;
  score(member: string): Promise<number | null>;
  rank(member: string): Promise<number | null>;
  top(limit?: number): Promise<{ member: string; score: number }[]>;
  bottom(limit?: number): Promise<{ member: string; score: number }[]>;
  delete(member: string): Promise<boolean>;
  size(): Promise<number>;
  between(min: number, max: number): {
    limit(n: number): Promise<{ member: string; score: number }[]>;
    values(): Promise<{ member: string; score: number }[]>;
  };
}

export class StreamCollection<T = any> {
  append(data: T): Promise<string>;
  head(): Promise<bigint>;
  read(fromId?: string, limit?: number): Promise<{ id: string; data: T }[]>;
  iterate(fromId?: string): AsyncIterable<{ id: string; data: T }>;
}

export class QueueCollection<T = any> {
  push(data: T, options?: { delay?: string | number }): Promise<string>;
  pop(options?: { timeout?: string | number }): Promise<{ id: string; data: T } | null>;
  size(): Promise<number>;
  work(worker: (data: T) => Promise<void>, options?: { concurrency?: number }): Promise<() => void>;
  close(): Promise<void>;
}

export class VectorCollection {
  set(id: string, vector: number[], metadata?: any): Promise<void>;
  get(id: string): Promise<{ id: string; vector: number[]; metadata?: any } | undefined>;
  delete(id: string): Promise<boolean>;
  search(queryVector: number[], topK?: number): Promise<{ id: string; score: number; metadata?: any }[]>;
  count(): Promise<number>;
  clear(): Promise<void>;
}

export class Scope {
  get<T = any>(key: string, options?: GetOptions): Promise<T | undefined>;
  set(key: string, value: any, options?: SetOptions): Promise<void>;
  has(key: string): Promise<boolean>;
  delete(key: string): Promise<boolean>;
  counter(key: string): Counter;
  map<K = string, V = any>(name: string): MapCollection<K, V>;
  sorted(name: string): SortedCollection;
  timeseries(name: string, options?: TimeSeriesOptions): TimeSeriesCollection;
  rollbackPlan(options?: RollbackPlanOptions): Promise<RollbackPlan>;
}

export class HistoricalView {
  get<T = any>(key: string): Promise<T | undefined>;
  has(key: string): Promise<boolean>;
  keys(prefix?: string): Promise<string[]>;
  counter(key: string): Counter;
  map<K = string, V = any>(name: string): MapCollection<K, V>;
  sorted(name: string): SortedCollection;
  timeseries(name: string): TimeSeriesCollection;
}

export class RollbackPlan {
  summary(): Promise<{ totalChanges: number; abortable: boolean }>;
  changes(): AsyncIterable<{ key: string; action: string; prevValue: any; restoreValue: any }>;
  apply(options?: { onConflict?: "abort" | "overwrite" | "skip" }): Promise<{ applied: number; conflicts: number }>;
}

export class Semaphore {
  acquire(): Promise<{ release(): Promise<void> }>;
  withLock<T>(fn: () => Promise<T>): Promise<T>;
}

export class ScheduleBuilder {
  every(interval: string | number): {
    run(task: () => Promise<void>): { stop(): void; active: boolean };
  };
  at(time: string | number | Date): {
    run(task: () => Promise<void>): { stop(): void; active: boolean };
  };
}

export class WatchStream {
  [Symbol.asyncIterator](): AsyncIterableIterator<{
    key: string;
    type: "set" | "delete" | "incr" | "patch" | "expire";
    value?: any;
    previousValue?: any;
    version: bigint | number;
    timestamp: number;
  }>;
  close(): void;
}

export class SemanticCache {
  get<T = any>(prompt: string): Promise<T | undefined>;
  set(prompt: string, value: any, options?: { ttl?: string | number }): Promise<void>;
}

export class AgentMemory {
  remember(key: string, content: string, metadata?: any): Promise<void>;
  recall(query: string, topK?: number): Promise<{ key: string; content: string; score: number }[]>;
}

export class Tencere {
  constructor(engine: any, options?: TencereOptions);
  static open(dataDir?: string | TencereOptions, options?: TencereOptions): Promise<Tencere>;
  get limits(): SystemLimits;
  static get limits(): SystemLimits;

  get<T = any>(key: string, options?: GetOptions): Promise<T | undefined>;
  set(key: string, value: any, options?: SetOptions): Promise<void>;
  has(key: string): Promise<boolean>;
  delete(key: string): Promise<boolean>;
  getMany<T = any>(keys: string[]): Promise<Map<string, T>>;
  setMany(entries: Record<string, any> | [string, any][], options?: SetOptions): Promise<void>;
  increment(key: string, delta?: number): Promise<number>;
  patch<T = any>(key: string, patchSpec: PatchSpec): Promise<T>;
  update<T = any>(key: string, updater: (val: T | undefined) => T, options?: SetOptions): Promise<T>;
  keys(prefix?: string): Promise<string[]>;
  ttl(key: string): Promise<number>;

  at(selector: string | number | Date | { timestamp?: number | string | Date; version?: bigint | number; sequence?: bigint | number }): HistoricalView;
  snapshot(options?: SnapshotOptions): Promise<SnapshotHandle>;
  rollback(key: string, options: RollbackOptions): Promise<{ restored: boolean; version: bigint; value: any }>;
  rollbackPlan(options?: RollbackPlanOptions): Promise<RollbackPlan>;
  restorePlan(snapshot: SnapshotHandle): Promise<RollbackPlan>;
  history(keyOrOptions?: string | HistoryQueryOptions, options?: HistoryQueryOptions): AsyncIterable<HistoricalRevision>;
  lockHistory(key: string): AsyncIterable<LockLeaseEvent>;
  compact(): Promise<void>;

  counter(key: string): Counter;
  scope(prefix: string, options?: ScopeOptions): Scope;
  kv(name: string, options?: CollectionOptions): any;
  map<K = string, V = any>(name: string, options?: CollectionOptions): MapCollection<K, V>;
  setCollection<T = any>(name: string): SetCollection<T>;
  sorted(name: string, options?: CollectionOptions): SortedCollection;
  stream<T = any>(name: string): StreamCollection<T>;
  queue<T = any>(name: string): QueueCollection<T>;
  vector(name: string, options?: VectorOptions): VectorCollection;
  timeseries(name: string, options?: TimeSeriesOptions): TimeSeriesCollection;

  lock<T>(key: string, optionsOrFn: LockOptions | ((handle: { token: number }) => Promise<T>), maybeFn?: (handle: { token: number }) => Promise<T>): Promise<T>;
  tryLock(key: string, options?: LockOptions): Promise<LockHandle | null>;
  once<T>(key: string, fn: () => Promise<T>, options?: OnceOptions): Promise<T>;
  idempotent<T>(key: string, fn: () => Promise<T>, options?: IdempotencyOptions): Promise<T>;
  semaphore(key: string, options?: SemaphoreOptions): Semaphore;
  rateLimit(key: string, options: RateLimitOptions): Promise<RateLimitResult>;
  cache<T>(key: string, options: CacheOptions, loader: () => Promise<T>): Promise<T>;
  schedule(name: string): ScheduleBuilder;
  watch(pattern?: string): WatchStream;
  waitFor<T = any>(key: string, condition: any, options?: WaitForOptions): Promise<T>;

  semantic: SemanticCache;
  memory: AgentMemory;
  cluster: ClusterManager | null;

  stats(): TencereStats;
  health(options?: { pingPeers?: boolean; timeoutMs?: number }): Promise<ClusterHealth>;
  metrics(): ClusterMetrics;
  checkpoint(): Promise<void>;
  close(): Promise<void>;
}

export class TencereSync {
  constructor(dataDir?: string, options?: any);
  get<T = any>(key: string): T | undefined;
  set(key: string, value: any, options?: SetOptions): void;
  has(key: string): boolean;
  delete(key: string): boolean;
  increment(key: string, delta?: number): number;
  keys(prefix?: string): string[];
  ttl(key: string): number;
  counter(key: string): any;
  map(name: string): any;
  setCollection(name: string): any;
  sorted(name: string): any;
  close(): void;
}

export interface TencereClientOptions {
  autoRedirect?: boolean;
}

export interface TencereClusterClientOptions {
  readPreference?: "leader" | "nearest" | "follower";
  maxRetries?: number;
  retryDelayMs?: number;
  refreshIntervalMs?: number;
}

export class TencereClusterClient extends EventEmitter {
  constructor(seedAddresses: string | string[], options?: TencereClusterClientOptions);
  connect(): Promise<this>;
  discoverTopology(): Promise<{ leader: string | null; nodes: string[] }>;
  get<T = any>(key: string, options?: any): Promise<T | undefined>;
  set(key: string, value: any, options?: SetOptions): Promise<any>;
  delete(key: string): Promise<boolean>;
  has(key: string): Promise<boolean>;
  increment(key: string, delta?: number): Promise<number>;
  patch(key: string, patchSpec: PatchSpec): Promise<any>;
  keys(prefix?: string): Promise<string[]>;
  stats(): Promise<any>;
  clear(): Promise<boolean>;
  ttl(key: string): Promise<number>;
  getMany<T = any>(keys: string[]): Promise<(T | undefined)[]>;
  setMany(entries: [string, any][]): Promise<boolean>;
  unlock(key: string): Promise<boolean>;
  checkpoint(): Promise<boolean>;
  ping(): Promise<string>;
  exec(target: string, name: string, method: string, args?: any[]): Promise<any>;
  watch(pattern?: string, handler?: (event: any) => void): Promise<any>;
  unwatch(): Promise<any>;
  pipeline(): any;
  getLeaderAddress(): string | null;
  getNodes(): string[];
  status(): { leaderAddress: string | null; nodes: string[]; readPreference: string; connectedClients: number };
  health(options?: { timeoutMs?: number }): Promise<ClusterHealth>;
  close(): Promise<void>;
}

export class TencereClient {
  static connect(address: string, options?: TencereClientOptions): Promise<TencereClient>;
  static cluster(seedAddresses: string | string[], options?: TencereClusterClientOptions): Promise<TencereClusterClient>;
  ping(): Promise<string>;
  health(): Promise<{
    status: "HEALTHY" | "OFFLINE";
    readiness: boolean;
    liveness: boolean;
    latencyMs: number | null;
    address: string;
    cluster: any;
    stats?: any;
    error?: string;
  }>;
  get<T = any>(key: string, options?: any): Promise<T | undefined>;
  set(key: string, value: any, options?: SetOptions): Promise<boolean>;
  delete(key: string): Promise<boolean>;
  has(key: string): Promise<boolean>;
  increment(key: string, delta?: number): Promise<number>;
  patch(key: string, patchSpec: PatchSpec): Promise<any>;
  keys(prefix?: string): Promise<string[]>;
  stats(): Promise<any>;
  clear(): Promise<boolean>;
  ttl(key: string): Promise<number>;
  getMany<T = any>(keys: string[]): Promise<(T | undefined)[]>;
  setMany(entries: [string, any][]): Promise<boolean>;
  unlock(key: string): Promise<boolean>;
  checkpoint(): Promise<boolean>;
  watch(pattern?: string, handler?: (event: any) => void): Promise<any>;
  unwatch(): Promise<any>;
  pipeline(): any;
  close(): Promise<void>;
}

export class TencereServer {
  constructor(db: Tencere, options?: { port?: number; host?: string; forwardWrites?: boolean });
  start(): Promise<void>;
  stop(): Promise<void>;
}

export class PartitionableNetwork {
  isolatedNodes: Set<number>;
  partitionPairs: Set<string>;
  isolate(nodeId: number | string): void;
  heal(nodeId?: number | string): void;
  partition(groupA: (number | string)[], groupB: (number | string)[]): void;
  deliver(from: number | string, to: number | string, buffers: any): void;
}

export interface TestClusterOptions {
  nodes?: number;
  tcp?: boolean;
  basePort?: number;
  host?: string;
  dataDir?: string | null;
  forwardWrites?: boolean;
  clusterOptions?: any;
}

export class TestCluster {
  constructor(options?: TestClusterOptions);
  nodeCount: number;
  isTcp: boolean;
  basePort: number;
  host: string;
  baseDataDir: string | null;
  forwardWrites: boolean;
  clusterOptions: any;
  network: PartitionableNetwork;
  nodes: Map<number, Tencere>;
  servers: Map<number, TencereServer>;
  ports: Map<number, number>;
  peerAddresses: Record<number, string>;
  leader: Tencere | null;
  followers: Tencere[];
  addresses: string[];

  start(): Promise<this>;
  node(id: number | string): Tencere | null;
  waitForLeader(timeoutMs?: number): Promise<Tencere>;
  isolate(nodeOrId: Tencere | number | string): void;
  heal(nodeOrId?: Tencere | number | string): void;
  partition(groupA: (number | string)[], groupB: (number | string)[]): void;
  stopNode(nodeOrId: Tencere | number | string): Promise<void>;
  startNode(id: number | string): Promise<Tencere>;
  restartNode(nodeOrId: Tencere | number | string): Promise<Tencere>;
  client(options?: any): Promise<TencereClusterClient | any>;
  health(options?: { pingPeers?: boolean; timeoutMs?: number }): Promise<ClusterHealth>;
  metrics(): ClusterMetrics;
  destroy(): Promise<void>;
  close(): Promise<void>;
}

export function createTestCluster(options?: TestClusterOptions): Promise<TestCluster>;

export * from "./src/errors.js";
export default Tencere;

