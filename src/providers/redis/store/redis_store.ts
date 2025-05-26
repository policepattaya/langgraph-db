import { createClient, type RedisClientType } from "redis";
import {
  BaseStore,
  type Operation,
  type OperationResults,
  type Item,
  type SearchItem,
  type GetOperation,
  type PutOperation,
  type SearchOperation,
  type ListNamespacesOperation,
  type IndexConfig,
} from "@langchain/langgraph-checkpoint";
import { tokenizePath, getTextAtPath } from "@langchain/langgraph-checkpoint";

/**
 * Abstract class for Redis client adapter
 * This provides a unified interface for different Redis client implementations
 */
abstract class RedisAdapter {
  abstract isConnected(): boolean;
  abstract connect(): Promise<void>;
  abstract disconnect(): Promise<void>;
  abstract get(key: string): Promise<string | null>;
  abstract set(key: string, value: string | Buffer): Promise<void>;
  abstract setEx(
    key: string,
    ttl: number,
    value: string | Buffer
  ): Promise<void>;
  abstract del(keys: string | string[]): Promise<void>;
  abstract keys(pattern: string): Promise<string[]>;
  abstract mGet(keys: string[]): Promise<(string | null)[]>;
  abstract exists(key: string): Promise<boolean>;
  abstract expire(key: string, ttl: number): Promise<boolean>;
  abstract ttl(key: string): Promise<number>;
  abstract multi(): any;
}

/**
 * NodeRedis adapter for the standard redis package
 */
class NodeRedisAdapter extends RedisAdapter {
  private client: RedisClientType;

  constructor(client: RedisClientType) {
    super();
    this.client = client;
  }

  isConnected(): boolean {
    return this.client.isOpen;
  }

  async connect(): Promise<void> {
    if (!this.client.isOpen) {
      await this.client.connect();
    }
  }

  async disconnect(): Promise<void> {
    if (this.client.isOpen) {
      await this.client.disconnect();
    }
  }

  async get(key: string): Promise<string | null> {
    return this.client.get(key);
  }

  async set(key: string, value: string | Buffer): Promise<void> {
    await this.client.set(key, value);
  }

  async setEx(key: string, ttl: number, value: string | Buffer): Promise<void> {
    await this.client.setEx(key, ttl, value);
  }

  async del(keys: string | string[]): Promise<void> {
    await this.client.del(keys);
  }

  async keys(pattern: string): Promise<string[]> {
    return this.client.keys(pattern);
  }

  async mGet(keys: string[]): Promise<(string | null)[]> {
    return this.client.mGet(keys);
  }

  async exists(key: string): Promise<boolean> {
    const result = await this.client.exists(key);
    return result > 0;
  }

  async expire(key: string, ttl: number): Promise<boolean> {
    const result = await this.client.expire(key, ttl);
    // Node-redis returns 1 (number) or true (boolean) depending on version
    return typeof result === "number" ? result === 1 : result === true;
  }

  async ttl(key: string): Promise<number> {
    return this.client.ttl(key);
  }

  multi(): any {
    return this.client.multi();
  }
}

/**
 * Generic client adapter for IORedis or other Redis clients
 */
class GenericRedisAdapter extends RedisAdapter {
  private client: any;

  constructor(client: any) {
    super();
    this.client = client;
  }

  isConnected(): boolean {
    return this.client.status === "ready";
  }

  async connect(): Promise<void> {
    // Most alternative clients auto-connect
  }

  async disconnect(): Promise<void> {
    if (typeof this.client.quit === "function") {
      await this.client.quit();
    }
  }

  async get(key: string): Promise<string | null> {
    return this.client.get(key);
  }

  async set(key: string, value: string | Buffer): Promise<void> {
    await this.client.set(key, value);
  }

  async setEx(key: string, ttl: number, value: string | Buffer): Promise<void> {
    await this.client.setex(key, ttl, value);
  }

  async del(keys: string | string[]): Promise<void> {
    await this.client.del(keys);
  }

  async keys(pattern: string): Promise<string[]> {
    return this.client.keys(pattern);
  }

  async mGet(keys: string[]): Promise<(string | null)[]> {
    return this.client.mget(keys);
  }

  async exists(key: string): Promise<boolean> {
    const result = await this.client.exists(key);
    return result > 0;
  }

  async expire(key: string, ttl: number): Promise<boolean> {
    const result = await this.client.expire(key, ttl);
    return result === 1;
  }

  async ttl(key: string): Promise<number> {
    return this.client.ttl(key);
  }

  multi(): any {
    return this.client.multi();
  }
}

/**
 * Checks if an object contains MongoDB-style filter operators
 */
function isFilterOperators(obj: any): boolean {
  return (
    typeof obj === "object" &&
    obj !== null &&
    Object.keys(obj).every(
      (key) =>
        key === "$eq" ||
        key === "$ne" ||
        key === "$gt" ||
        key === "$gte" ||
        key === "$lt" ||
        key === "$lte" ||
        key === "$in" ||
        key === "$nin"
    )
  );
}

/**
 * Compares a value from an item with a value from a filter
 */
function compareValues(itemValue: any, filterValue: any): boolean {
  if (isFilterOperators(filterValue)) {
    const operators = Object.keys(filterValue).filter((k) => k.startsWith("$"));
    return operators.every((op) => {
      const value = filterValue[op];
      switch (op) {
        case "$eq":
          return itemValue === value;
        case "$ne":
          return itemValue !== value;
        case "$gt":
          return Number(itemValue) > Number(value);
        case "$gte":
          return Number(itemValue) >= Number(value);
        case "$lt":
          return Number(itemValue) < Number(value);
        case "$lte":
          return Number(itemValue) <= Number(value);
        case "$in":
          return Array.isArray(value) ? value.includes(itemValue) : false;
        case "$nin":
          return Array.isArray(value) ? !value.includes(itemValue) : true;
        default:
          return false;
      }
    });
  }
  // If no operators, do a direct comparison
  return itemValue === filterValue;
}

/**
 * Configuration options for the Redis store.
 *
 * @example
 * ```typescript
 * // Basic configuration with default Redis URL
 * const options: RedisStoreOptions = {
 *   ttl: 3600, // Store items for 1 hour
 *   keyPrefix: "myapp:store:" // Custom prefix for Redis keys
 * };
 * ```
 *
 * @example
 * ```typescript
 * // Configuration with custom Redis URL
 * const options: RedisStoreOptions = {
 *   url: "redis://username:password@redis.example.com:6379/0",
 *   ttl: 86400, // 1 day TTL
 *   keyPrefix: "production:userstore:"
 * };
 * ```
 *
 * @example
 * ```typescript
 * // Using an existing Redis client
 * import { createClient } from "redis";
 * // or import IORedis from "ioredis";
 *
 * const redisClient = createClient({
 *   url: "redis://localhost:6379",
 *   socket: {
 *     reconnectStrategy: (retries) => Math.min(retries * 50, 1000)
 *   }
 * });
 * await redisClient.connect();
 *
 * const options: RedisStoreOptions = {
 *   client: redisClient,
 *   ttl: 7200 // 2 hours TTL
 * };
 * ```
 *
 * @example
 * ```typescript
 * // With vector indexing for semantic search
 * import { OpenAIEmbeddings } from "@langchain/openai";
 *
 * const options: RedisStoreOptions = {
 *   index: {
 *     dims: 1536, // OpenAI embedding dimensions
 *     embeddings: new OpenAIEmbeddings({ modelName: "text-embedding-3-small" }),
 *     fields: ["content", "summary"], // Fields to index for vector search
 *   }
 * };
 * ```
 */
export interface RedisStoreOptions {
  /** Redis URL (default: redis://localhost:6379) */
  url?: string;
  /** Pre-configured Redis client (can be either node-redis or ioredis client) */
  client?: any;
  /** Optional TTL in seconds for stored items */
  ttl?: number;
  /** Prefix for Redis keys (default: "langgraph:store:") */
  keyPrefix?: string;
  /** Configuration for vector indexing and search */
  index?: IndexConfig;
}

/**
 * A Redis-based key-value store for LangGraph state.
 *
 * This store persists data in Redis, providing durable storage for graph state.
 * It can be used as a standalone key-value store or with LangGraph's checkpointing.
 *
 * @example
 * ```typescript
 * // Basic key-value storage
 * const store = new RedisStore();
 * await store.put(["users", "123"], "prefs", { theme: "dark" });
 * const item = await store.get(["users", "123"], "prefs");
 * ```
 *
 * @example
 * ```typescript
 * // Vector search with embeddings
 * import { OpenAIEmbeddings } from "@langchain/openai";
 * const store = new RedisStore({
 *   index: {
 *     dims: 1536,
 *     embeddings: new OpenAIEmbeddings({ modelName: "text-embedding-3-small" }),
 *   }
 * });
 *
 * // Store documents
 * await store.put(["docs"], "doc1", { text: "Python tutorial" });
 * await store.put(["docs"], "doc2", { text: "TypeScript guide" });
 *
 * // Search by similarity
 * const results = await store.search(["docs"], { query: "python programming" });
 * ```
 * 
 * @example
 * ```typescript
 * // Custom Redis configuration
 * const store = new RedisStore({
 *   url: "redis://username:password@redis.example.com:6379",
 *   ttl: 86400, // 1 day TTL
 *   keyPrefix: "myapp:store:"
 * });
 * ```
 * 
 * @example
 * ```typescript
 * // Using with IORedis client
 * import IORedis from "ioredis";
 * 
 * const redisClient = new IORedis("redis://localhost:6379");
 * const store = new RedisStore({
 *   client: redisClient,
 *   ttl: 3600 // 1 hour TTL
 * });
 * ```
 *
 * @example
 * ```typescript
 * // Advanced filtering with MongoDB-style operators
 * const store = new RedisStore();
 * 
 * // Add some items
 * await store.put(["products"], "p1", { name: "Widget", price: 10, inStock: true });
 * await store.put(["products"], "p2", { name: "Gadget", price: 20, inStock: true });
 * await store.put(["products"], "p3", { name: "Tool", price: 15, inStock: false });
 * 
 * // Search with filters
 * const expensiveProducts = await store.search(["products"], {
 *   filter: { price: { $gt: 12 } }
 * });
 * 
 * const inStockProducts = await store.search(["products"], {
 *   filter: { inStock: true }
 * });
 * 
 * const specificProducts = await store.search(["products"], {
 *   filter: { name: { $in: ["Widget", "Tool"] } }
 * });
 * ```
 * 
 * @example
 * ```typescript
 * // Batching operations for efficiency
 * const store = new RedisStore();
 * 
 * const results = await store.batch([
 *   // Get operation 
 *   { namespace: ["users"], key: "user1" },
 *   // Put operation
 *   { namespace: ["users"], key: "user2", value: { name: "Alice" } },
 *   // Search operation
 *   { namespacePrefix: ["products"], filter: { category: "electronics" }, limit: 5 }
 * ]);
 * 
 * // results[0] = Get result
 * // results[1] = null (put operations return null)
 * // results[2] = Search results
 * ```
 * 
 * @example
 * ```typescript
 * // Using the legacy key-value API
 * const store = new RedisStore();
 * 
 * // Set a value
 * await store.set("session:user123", { authenticated: true });
 * 
 * // Get a value
 * const session = await store.getLegacy("session:user123");
 * 
 * // Check if key exists
 * const exists = await store.has("session:user123");
 * 
 * // Set TTL for a key
 * await store.setTTL("session:user123", 1800); // 30 minutes
 * 
 * // Get remaining TTL
 * const remainingTtl = await store.getTTL("session:user123");
 * 
 * // List all keys
 * const allKeys = await store.keys();
 * 
 * // Clear all data
 * await store.clear();
 * ```
 *
 * @example
 * ```typescript
 * // Properly starting and stopping the store
 * import { createGraph } from "@langchain/langgraph";
 * 
 * const store = new RedisStore();
 * 
 * // Register for proper lifecycle management with a graph
 * const builder = createGraph();
 * builder.addPersistenceOptions({ 
 *   stateStore: store
 * });
 *
 * // Or manually control the lifecycle
 * store.start(); // Connect to Redis
 *
 * try {
 *   // Use the store...
 * } finally {
 *   store.stop(); // Disconnect when done
 * }
 * ```
 */
export class RedisStore extends BaseStore {
  client: RedisAdapter;
  ttl?: number;
  keyPrefix: string;
  private clientOwned: boolean = false;
  private isConnecting: boolean = false;
  private connectionPromise: Promise<unknown> | null = null;
  private _redisConfig?: IndexConfig & {
    __tokenizedFields?: [string, string[]][];
  };
  // In-memory storage for fast access and fallback
  private data: Map<string, Map<string, Item>> = new Map();
  private vectors: Map<string, Map<string, Map<string, number[]>>> = new Map();

  /**
   * Creates a new Redis store.
   *
   * @param options - Configuration options for the Redis store.
   */
  constructor(options: RedisStoreOptions = {}) {
    super();

    if (options.client) {
      // If a client is provided, detect its type and create the appropriate adapter
      if ("isOpen" in options.client) {
        // Node-redis client
        this.client = new NodeRedisAdapter(options.client);
      } else {
        // IORedis or other client
        this.client = new GenericRedisAdapter(options.client);
      }
      this.clientOwned = false;
    } else {
      // Create a new node-redis client
      const redisClient = createClient({
        url: options.url || "redis://localhost:6379",
      });
      this.client = new NodeRedisAdapter(redisClient as RedisClientType);
      this.clientOwned = true;
    }

    this.ttl = options.ttl;
    this.keyPrefix = options.keyPrefix || "langgraph:store:";

    if (options.index) {
      this._redisConfig = {
        ...options.index,
        __tokenizedFields: (options.index.fields ?? ["$"]).map((p) => [
          p,
          p === "$" ? [p] : tokenizePath(p),
        ]),
      };
    }
  }

  /**
   * Filter items based on a filter condition
   * @param items Items to filter
   * @param filter Filter condition
   * @returns Filtered items
   */
  filterItems(items: Item[], filter?: Record<string, any>): Item[] {
    if (!filter) return items;

    return items.filter((item) => {
      const value = item.value;

      // Check each filter condition
      return Object.entries(filter).every(([path, condition]) => {
        if (path === "$") {
          // Special case for the root
          return compareValues(value, condition);
        }

        // Extract the value at path
        const fieldValue = getTextAtPath(value, path);
        return compareValues(fieldValue, condition);
      });
    });
  }

  /**
   * Score results based on similarity to a query embedding
   * @param items Items to score
   * @param queryEmbedding The query embedding to compare against
   * @returns Scored items
   */
  async scoreResults(
    items: Item[],
    queryEmbedding: number[]
  ): Promise<SearchItem[]> {
    const result: SearchItem[] = [];

    for (const item of items) {
      const vectors = await this.getVectors(item);
      if (vectors.length > 0) {
        // Find the best matching vector for this item
        let bestScore = -Infinity;

        for (const vector of vectors) {
          // Use the cosineSimilarity method
          const similarity = this.cosineSimilarity(vector, queryEmbedding);

          if (similarity > bestScore) {
            bestScore = similarity;
          }
        }

        result.push({ ...item, score: bestScore });
      } else {
        result.push({ ...item, score: undefined });
      }
    }

    // Sort by score
    result.sort((a, b) => (b.score || 0) - (a.score || 0));

    return result;
  }

  /**
   * Paginate results
   * @param items Items to paginate
   * @param offset Offset to start from
   * @param limit Maximum number of items to return
   * @returns Paginated items
   */
  paginateResults(
    items: SearchItem[],
    offset: number = 0,
    limit: number = 10
  ): SearchItem[] {
    return items.slice(offset, offset + limit);
  }

  /**
   * Insert vector embeddings
   * @param texts The texts to embed
   * @param items The items containing the texts
   * @param embeddings Optional pre-computed embeddings
   * @returns Promise that resolves when vectors are inserted
   */
  async insertVectors(
    texts: string[],
    items: [string[], string, string][],
    embeddings?: number[][]
  ): Promise<void> {
    if (!this._redisConfig?.embeddings || texts.length === 0) {
      return;
    }

    // Generate embeddings if not provided
    const vectors =
      embeddings || (await this._redisConfig.embeddings.embedDocuments(texts));

    // Create a mapping of text to embeddings
    const textEmbeddings: Record<string, [string[], string, string][]> = {};
    texts.forEach((text, i) => {
      if (!textEmbeddings[text]) {
        textEmbeddings[text] = [];
      }
      textEmbeddings[text].push(items[i]);
    });

    // Store vectors in Redis
    await this.storeVectors(textEmbeddings, vectors);

    // Also update in-memory cache for faster access
    texts.forEach((_text, i) => {
      const embedding = vectors[i];
      const [namespace, key, field] = items[i];

      // Initialize nested maps if they don't exist
      if (!this.vectors.has(namespace.join(":"))) {
        this.vectors.set(namespace.join(":"), new Map());
      }

      const nsMap = this.vectors.get(namespace.join(":"))!;
      if (!nsMap.has(key)) {
        nsMap.set(key, new Map());
      }

      nsMap.get(key)!.set(field, embedding);
    });
  }

  /**
   * Ensures the Redis client is connected.
   * This method handles concurrent connection attempts by using a promise.
   */
  async connect(): Promise<void> {
    if (this.client.isConnected()) {
      return;
    }

    if (this.isConnecting && this.connectionPromise) {
      await this.connectionPromise;
      return;
    }

    try {
      this.isConnecting = true;
      this.connectionPromise = this.client.connect();
      await this.connectionPromise;
    } catch (error) {
      throw new Error(
        `Failed to connect to Redis: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    } finally {
      this.isConnecting = false;
      this.connectionPromise = null;
    }
  }

  /**
   * Disconnects the Redis client if it was created by this store.
   */
  async disconnect(): Promise<void> {
    if (this.clientOwned) {
      try {
        await this.client.disconnect();
      } catch (error) {
        console.error("Error disconnecting from Redis:", error);
      }
    }
  }

  /**
   * Execute multiple operations in a single batch.
   * Implementation of BaseStore abstract method.
   *
   * @param operations Array of operations to execute
   * @returns Promise resolving to results matching the operations
   */
  async batch<Op extends readonly Operation[]>(
    operations: Op
  ): Promise<OperationResults<Op>> {
    await this.connect();

    const results: any[] = [];

    for (let i = 0; i < operations.length; i++) {
      const op = operations[i];

      if ("key" in op && "namespace" in op && !("value" in op)) {
        // GetOperation
        results.push(await this.getOperation(op as GetOperation));
      } else if ("namespacePrefix" in op) {
        // SearchOperation
        results.push(await this.redisSearchOperation(op as SearchOperation));
      } else if ("value" in op && "key" in op && "namespace" in op) {
        // PutOperation
        await this.putOperation(op as PutOperation);
        results.push(null);
      } else if ("matchConditions" in op) {
        // ListNamespacesOperation
        results.push(
          await this.listNamespacesOperation(op as ListNamespacesOperation)
        );
      } else {
        results.push(null);
      }
    }

    return results as OperationResults<Op>;
  }

  /**
   * Get an item by namespace and key.
   * Implementation of the BaseStore abstract method.
   */
  async get(namespace: string[], key: string): Promise<Item | null> {
    // First try to get from Redis
    const redisItem = await this.getOperation({ namespace, key });

    if (redisItem) {
      // Update in-memory cache
      const namespaceKey = namespace.join(":");
      if (!this.data.has(namespaceKey)) {
        this.data.set(namespaceKey, new Map());
      }
      this.data.get(namespaceKey)!.set(key, redisItem);

      return redisItem;
    }

    // Fall back to in-memory cache
    const namespaceKey = namespace.join(":");
    return this.data.get(namespaceKey)?.get(key) || null;
  }

  /**
   * Put an item into storage.
   * Implementation of the BaseStore abstract method.
   */
  async put(
    namespace: string[],
    key: string,
    value: Record<string, any>,
    index?: string[]
  ): Promise<void> {
    const putOp: PutOperation = {
      namespace,
      key,
      value,
      index,
    };

    await this.putOperation(putOp);

    // Also update in-memory cache
    const namespaceKey = namespace.join(":");
    if (!this.data.has(namespaceKey)) {
      this.data.set(namespaceKey, new Map());
    }

    const now = new Date();
    const item: Item = {
      namespace,
      key,
      value,
      createdAt: now,
      updatedAt: now,
    };

    this.data.get(namespaceKey)!.set(key, item);
  }

  /**
   * Delete an item from storage.
   * Implementation of the BaseStore abstract method.
   */
  async delete(namespace: string[], key: string): Promise<void> {
    await this.put(namespace, key, null as any);

    // Also remove from in-memory cache
    const namespaceKey = namespace.join(":");
    this.data.get(namespaceKey)?.delete(key);
  }

  /**
   * Search for items.
   * Implementation of the BaseStore abstract method.
   */
  async search(
    namespacePrefix: string[],
    options: {
      filter?: Record<string, any>;
      limit?: number;
      offset?: number;
      query?: string;
    } = {}
  ): Promise<SearchItem[]> {
    return this.redisSearchOperation({
      namespacePrefix,
      filter: options.filter,
      limit: options.limit,
      offset: options.offset,
      query: options.query,
    });
  }

  /**
   * Start the store - connect to Redis.
   * Implementation of the BaseStore abstract method.
   */
  start(): void {
    // Connect on start (async, but we can't make start async)
    this.connect().catch((e) => {
      console.error("Error connecting to Redis:", e);
    });
  }

  /**
   * Stop the store - disconnect from Redis.
   * Implementation of the BaseStore abstract method.
   */
  stop(): void {
    // Disconnect on stop
    this.disconnect().catch((e) => {
      console.error("Error disconnecting from Redis:", e);
    });
  }

  /**
   * Gets a namespaced key prefix for use with pattern matching.
   */
  private getNamespacePrefix(namespacePrefix: string[]): string {
    return `${this.keyPrefix}${namespacePrefix.join(":")}`;
  }

  /**
   * Generates a Redis key from namespace and item key
   */
  private getRedisKey(namespace: string[], key: string): string {
    return `${this.keyPrefix}${namespace.join(":")}:${key}`;
  }

  /**
   * Creates a standardized item from Redis data.
   */
  private createItem(
    namespace: string[],
    key: string,
    value: Record<string, any>,
    metadata?: Record<string, any>
  ): Item {
    const now = new Date();
    return {
      namespace,
      key,
      value,
      createdAt:
        metadata && metadata.createdAt
          ? typeof metadata.createdAt === "string"
            ? new Date(metadata.createdAt)
            : metadata.createdAt
          : now,
      updatedAt:
        metadata && metadata.updatedAt
          ? typeof metadata.updatedAt === "string"
            ? new Date(metadata.updatedAt)
            : metadata.updatedAt
          : now,
    };
  }

  /**
   * Stores vector embeddings in Redis
   */
  private async storeVectors(
    texts: Record<string, [string[], string, string][]>,
    embeddings: number[][]
  ): Promise<void> {
    const pipeline = this.client.multi();
    let embeddingIndex = 0;

    for (const [text, metadata] of Object.entries(texts)) {
      const embedding = embeddings[embeddingIndex++];
      if (!embedding) {
        throw new Error(`No embedding found for text: ${text}`);
      }

      for (const [namespace, key, field] of metadata) {
        const redisKey = `${this.keyPrefix}vector:${namespace.join(
          ":"
        )}:${key}:${field}`;
        pipeline.set(redisKey, JSON.stringify(embedding));

        if (this.ttl) {
          pipeline.expire(redisKey, this.ttl);
        }
      }
    }

    await pipeline.exec();
  }

  /**
   * Retrieves vector embeddings for an item
   */
  private async getVectors(item: Item): Promise<number[][]> {
    const pattern = `${this.keyPrefix}vector:${item.namespace.join(":")}:${
      item.key
    }:*`;
    const keys = await this.client.keys(pattern);

    if (keys.length === 0) {
      return [];
    }

    const values = await this.client.mGet(keys);
    return values.filter(Boolean).map((value) => JSON.parse(value as string));
  }

  /**
   * Redis Get operation implementation
   */
  private async getOperation(op: GetOperation): Promise<Item | null> {
    try {
      const redisKey = this.getRedisKey(op.namespace, op.key);
      const data = await this.client.get(redisKey);

      if (!data) {
        return null;
      }

      const parsed = JSON.parse(data);

      // Try to get metadata
      const metadataKey = `${redisKey}:metadata`;
      const metadataStr = await this.client.get(metadataKey);
      let metadata = {};

      if (metadataStr) {
        try {
          metadata = JSON.parse(metadataStr);
        } catch (e) {
          console.warn(`Error parsing metadata for ${redisKey}:`, e);
        }
      }

      const item = this.createItem(
        op.namespace,
        op.key,
        parsed.value || parsed,
        metadata
      );

      return item;
    } catch (e) {
      console.warn(
        `Error in getOperation for ${op.namespace.join(":")}/${op.key}:`,
        e
      );
      return null;
    }
  }

  /**
   * Redis Put operation implementation
   */
  private async putOperation(op: PutOperation): Promise<void> {
    try {
      const redisKey = this.getRedisKey(op.namespace, op.key);
      const pipeline = this.client.multi();

      if (op.value === null) {
        // Delete operation
        pipeline.del(redisKey);
        pipeline.del(`${redisKey}:metadata`);

        // Also delete any vector embeddings
        const vectorPattern = `${this.keyPrefix}vector:${op.namespace.join(
          ":"
        )}:${op.key}:*`;
        const vectorKeys = await this.client.keys(vectorPattern);

        if (vectorKeys.length > 0) {
          pipeline.del(vectorKeys);
        }
      } else {
        // Save item
        const now = new Date();
        const metadata = {
          createdAt: now.toISOString(),
          updatedAt: now.toISOString(),
        };

        // Check if item already exists to preserve createdAt
        const existingData = await this.client.get(`${redisKey}:metadata`);
        if (existingData) {
          try {
            const existingMetadata = JSON.parse(existingData);
            if (existingMetadata.createdAt) {
              metadata.createdAt = existingMetadata.createdAt;
            }
          } catch (e) {
            console.warn(`Error parsing existing metadata for ${redisKey}:`, e);
          }
        }

        // Store item and metadata
        pipeline.set(redisKey, JSON.stringify(op.value));
        pipeline.set(`${redisKey}:metadata`, JSON.stringify(metadata));

        if (this.ttl) {
          pipeline.expire(redisKey, this.ttl);
          pipeline.expire(`${redisKey}:metadata`, this.ttl);
        }

        // Process vectors if we have embeddings configured and indexing is enabled
        if (this._redisConfig?.embeddings && op.index !== false) {
          // Extract texts and prepare for embedding
          const [texts, items] = this.extractTexts([op]);

          if (texts.length > 0) {
            // We'll handle vector storage after the main transaction completes
            await pipeline.exec();
            await this.insertVectors(texts, items);
            return;
          }
        }
      }

      await pipeline.exec();
    } catch (e) {
      throw new Error(
        `Failed to put item: ${e instanceof Error ? e.message : String(e)}`
      );
    }
  }

  /**
   * Execute search on Redis keys.
   */
  private async redisSearchOperation(
    op: SearchOperation
  ): Promise<SearchItem[]> {
    await this.connect();

    try {
      // Build the pattern for key search
      const pattern = `${this.getNamespacePrefix(op.namespacePrefix)}*`;
      const keys = await this.client.keys(pattern);

      if (keys.length === 0) {
        return [];
      }

      // Get all non-metadata keys
      const dataKeys = keys.filter(
        (key) => !key.endsWith(":metadata") && !key.includes("vector:")
      );

      if (dataKeys.length === 0) {
        return [];
      }

      // Load all the data
      const values = await this.client.mGet(dataKeys);
      const items: Item[] = [];

      // Parse and create items for each key
      dataKeys.forEach((key, index) => {
        const value = values[index];
        if (!value) return;

        // Extract namespace and itemKey from the Redis key
        const keyWithoutPrefix = key.substring(this.keyPrefix.length);
        const parts = keyWithoutPrefix.split(":");
        const itemKey = parts.pop() || "";
        const namespace = parts;

        try {
          const parsedValue = JSON.parse(value);
          const item = this.createItem(namespace, itemKey, parsedValue);
          items.push(item);
        } catch (e) {
          console.warn(`Failed to parse value for key ${key}:`, e);
        }
      });

      // Apply filter using filterItems method
      const filtered = this.filterItems(items, op.filter);

      let result: SearchItem[];

      // Apply vector search if query is provided and index is configured
      if (op.query && this._redisConfig?.embeddings) {
        const queryEmbedding = await this._redisConfig.embeddings.embedQuery(
          op.query
        );

        // Score results using the scoreResults method
        result = await this.scoreResults(filtered, queryEmbedding);
      } else {
        // No vector search, use items as is
        result = filtered.map((item) => ({ ...item, score: undefined }));
      }

      // Apply pagination using paginateResults method
      return this.paginateResults(result, op.offset, op.limit);
    } catch (error) {
      console.warn(`Error in redisSearchOperation:`, error);
      return [];
    }
  }

  /**
   * List namespaces with filtering based on match conditions.
   */
  private async listNamespacesOperation(
    op: ListNamespacesOperation
  ): Promise<string[][]> {
    await this.connect();

    try {
      // Get all keys with the store prefix
      const keys = await this.client.keys(`${this.keyPrefix}*`);

      // Filter out metadata and vector keys, extract namespaces
      const namespaces = new Set<string>();
      for (const key of keys) {
        if (key.endsWith(":metadata") || key.includes("vector:")) {
          continue;
        }

        // Extract namespace from key format: keyPrefix:namespace:key
        const parts = key.substring(this.keyPrefix.length).split(":");
        if (parts.length > 1) {
          // Combine all parts except the last one (the key)
          const namespace = parts.slice(0, -1).join(":");
          namespaces.add(namespace);
        }
      }

      // Convert namespaces to arrays
      const result = Array.from(namespaces).map((ns) => ns.split(":"));

      // Filter by match conditions if specified
      if (op.matchConditions && op.matchConditions.length > 0) {
        return result.filter((namespace) =>
          op.matchConditions!.some((condition) =>
            this.doesMatch(namespace, condition.path, condition.matchType)
          )
        );
      }

      return result;
    } catch (error) {
      console.warn(`Error in listNamespaces:`, error);
      return [];
    }
  }

  /**
   * Returns the store's index configuration
   */
  get indexConfig(): IndexConfig | undefined {
    return this._redisConfig;
  }

  /**
   * Legacy method for direct key-value access
   */
  async getLegacy<T>(key: string): Promise<T | undefined> {
    await this.connect();

    try {
      const fullKey = `${this.keyPrefix}${key}`;
      const value = await this.client.get(fullKey);

      if (!value) {
        return undefined;
      }

      return JSON.parse(value) as T;
    } catch (error) {
      console.warn(`Error in getLegacy for ${key}:`, error);
      return undefined;
    }
  }

  /**
   * Legacy method for direct key-value setting
   */
  async set<T>(key: string, value: T): Promise<void> {
    await this.connect();

    try {
      const fullKey = `${this.keyPrefix}${key}`;
      let serializedValue: string;

      if (typeof value === "string") {
        serializedValue = value;
      } else {
        serializedValue = JSON.stringify(value);
      }

      if (this.ttl) {
        await this.client.setEx(fullKey, this.ttl, serializedValue);
      } else {
        await this.client.set(fullKey, serializedValue);
      }
    } catch (error) {
      throw new Error(
        `Failed to set value for ${key}: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }

  /**
   * Legacy method for checking key existence
   */
  async has(key: string): Promise<boolean> {
    await this.connect();

    try {
      const fullKey = `${this.keyPrefix}${key}`;
      return await this.client.exists(fullKey);
    } catch (error) {
      console.warn(`Error in has check for ${key}:`, error);
      return false;
    }
  }

  /**
   * Legacy method for clearing all keys
   */
  async clear(): Promise<void> {
    await this.connect();

    try {
      const keys = await this.client.keys(`${this.keyPrefix}*`);
      if (keys.length > 0) {
        await this.client.del(keys);
      }
    } catch (error) {
      throw new Error(
        `Failed to clear keys: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }

  /**
   * Legacy method for listing keys
   */
  async keys(): Promise<string[]> {
    await this.connect();

    try {
      const keys = await this.client.keys(`${this.keyPrefix}*`);
      return keys
        .filter((key) => !key.endsWith(":metadata") && !key.includes("vector:"))
        .map((key: string) => key.substring(this.keyPrefix.length));
    } catch (error) {
      console.warn(`Error listing keys:`, error);
      return [];
    }
  }

  /**
   * Retrieves the time-to-live (TTL) for a key.
   */
  async getTTL(key: string): Promise<number> {
    await this.connect();

    try {
      const fullKey = `${this.keyPrefix}${key}`;
      return await this.client.ttl(fullKey);
    } catch (error) {
      console.warn(`Error getting TTL for ${key}:`, error);
      return -1;
    }
  }

  /**
   * Sets the time-to-live (TTL) for a key.
   */
  async setTTL(key: string, ttl: number): Promise<boolean> {
    await this.connect();

    try {
      const fullKey = `${this.keyPrefix}${key}`;
      return await this.client.expire(fullKey, ttl);
    } catch (error) {
      console.warn(`Error setting TTL for ${key}:`, error);
      return false;
    }
  }

  /**
   * Extract texts for embedding from put operations
   * Used internally by batch method
   */
  private extractTexts(
    ops: PutOperation[]
  ): [string[], [string[], string, string][]] {
    if (!ops.length || !this._redisConfig) {
      return [[], []];
    }

    const texts: string[] = [];
    const items: [string[], string, string][] = [];

    for (const op of ops) {
      if (op.value !== null && op.index !== false) {
        const paths =
          op.index === undefined
            ? this._redisConfig.__tokenizedFields ?? []
            : op.index.map((ix) => [ix, tokenizePath(ix)]);

        for (const [path, field] of paths as [string, string[]][]) {
          const fieldStr = typeof field === "string" ? field : field.join(".");
          const extractedTexts = getTextAtPath(op.value, fieldStr);

          if (extractedTexts.length) {
            if (extractedTexts.length > 1) {
              extractedTexts.forEach((text, i) => {
                texts.push(text);
                items.push([op.namespace, op.key, `${path}.${i}`]);
              });
            } else {
              texts.push(extractedTexts[0]);
              items.push([op.namespace, op.key, path]);
            }
          }
        }
      }
    }

    return [texts, items];
  }

  /**
   * Calculate cosine similarity between two vectors
   */
  private cosineSimilarity(vec1: number[], vec2: number[]): number {
    if (vec1.length !== vec2.length) {
      throw new Error("Vectors must have the same dimensionality");
    }

    let dotProduct = 0;
    let mag1 = 0;
    let mag2 = 0;

    for (let i = 0; i < vec1.length; i++) {
      dotProduct += vec1[i] * vec2[i];
      mag1 += vec1[i] * vec1[i];
      mag2 += vec2[i] * vec2[i];
    }

    mag1 = Math.sqrt(mag1);
    mag2 = Math.sqrt(mag2);

    return dotProduct / (mag1 * mag2);
  }

  /**
   * Check if a namespace matches a pattern
   */
  private doesMatch(
    namespace: string[],
    pattern: string[],
    matchType: "prefix" | "suffix"
  ): boolean {
    if (matchType === "prefix") {
      if (pattern.length > namespace.length) return false;

      return pattern.every((part, i) => part === "*" || part === namespace[i]);
    } else if (matchType === "suffix") {
      const offset = namespace.length - pattern.length;
      if (offset < 0) return false;

      return pattern.every(
        (part, i) => part === "*" || part === namespace[offset + i]
      );
    }

    return false;
  }
}
