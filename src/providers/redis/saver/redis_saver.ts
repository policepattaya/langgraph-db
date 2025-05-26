import {
  BaseCheckpointSaver,
  Checkpoint,
  CheckpointListOptions,
  CheckpointTuple,
  getCheckpointId,
  WRITES_IDX_MAP,
} from "@langchain/langgraph-checkpoint";
import type { SerializerProtocol } from "@langchain/langgraph-checkpoint";
import type {
  CheckpointMetadata,
  PendingWrite,
  SendProtocol,
} from "@langchain/langgraph-checkpoint";
import { TASKS } from "@langchain/langgraph-checkpoint";
import type { RunnableConfig } from "@langchain/core/runnables";
import { createClient, type RedisClientType } from "redis";

/**
 * Extended PendingWrite interface with the actual structure used at runtime
 */
interface ExtendedPendingWrite extends PendingWrite {
  channel: string;
  writeType?: string;
  value: unknown;
}

/**
 * Type for pendingWrites tuples returned from Redis
 */
type PendingWriteTuple = [string, string, unknown];

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
}

/**
 * Generates a standard Redis key for a checkpoint
 */
function _generateKey(
  threadId: string,
  checkpointNamespace: string,
  checkpointId: string
): string {
  return `langgraph:${threadId}:${checkpointNamespace}:${checkpointId}`;
}

/**
 * Generates a standard Redis key for a write entry
 */
function _generateWritesKey(
  key: string,
  taskId: string,
  channel: string,
  writeIdx: number
): string {
  return `${key}:writes:${taskId}:${channel}:${writeIdx}`;
}

/**
 * Configuration options for the Redis checkpoint saver.
 *
 * @example
 * ```typescript
 * // Basic configuration with default Redis URL
 * const options: RedisCheckpointSaverOptions = {
 *   ttl: 3600 // Store checkpoints for 1 hour
 * };
 * ```
 *
 * @example
 * ```typescript
 * // Configuration with custom Redis URL
 * const options: RedisCheckpointSaverOptions = {
 *   url: "redis://username:password@redis.example.com:6379/0",
 *   ttl: 86400 // 1 day TTL
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
 * const options: RedisCheckpointSaverOptions = {
 *   client: redisClient,
 *   ttl: 7200 // 2 hours TTL
 * };
 * ```
 */
export interface RedisCheckpointSaverOptions {
  /** Redis URL (default: redis://localhost:6379) */
  url?: string;
  /** Pre-configured Redis client (can be either node-redis or ioredis client) */
  client?: any;
  /** Optional TTL in seconds for stored checkpoints */
  ttl?: number;
}

/**
 * A Redis implementation of the BaseCheckpointSaver for LangGraph.
 *
 * This saver stores checkpoint data in Redis, providing durable persistence
 * across application restarts. It supports both the node-redis and ioredis clients.
 *
 * @example
 * ```typescript
 * import { RedisSaver } from "langgraph-db/providers/redis";
 *
 * // Create a Redis-backed checkpoint saver
 * const saver = new RedisSaver({
 *   url: "redis://localhost:6379",
 *   ttl: 3600 // 1 hour TTL (optional)
 * });
 *
 * // Use with a LangGraph
 * const graph = new StateGraph({
 *   channels: { input: { value: "" } },
 *   nodes: {}, // define your nodes here
 *   checkpointer: saver
 * });
 * ```
 *
 * @example
 * ```typescript
 * // Using with IORedis
 * import { RedisSaver } from "langgraph-db/providers/redis";
 * import IORedis from "ioredis";
 *
 * // Create an IORedis client
 * const ioredisClient = new IORedis("redis://localhost:6379");
 *
 * // Create a Redis-backed checkpoint saver with IORedis
 * const saver = new RedisSaver({
 *   client: ioredisClient,
 *   ttl: 3600 // 1 hour TTL (optional)
 * });
 * ```
 * 
 * @example
 * ```typescript
 * // Retrieving checkpoints
 * import { RedisSaver } from "langgraph-db/providers/redis";
 * 
 * const saver = new RedisSaver();
 * await saver.connect();
 * 
 * // Get a specific checkpoint by ID
 * const checkpoint = await saver.getTuple({
 *   configurable: {
 *     thread_id: "user_123",
 *     checkpoint_id: "abc123",
 *     checkpoint_ns: "conversation"
 *   }
 * });
 * 
 * // Or get the most recent checkpoint for a thread
 * const latestCheckpoint = await saver.getTuple({
 *   configurable: {
 *     thread_id: "user_123",
 *     checkpoint_ns: "conversation"
 *   }
 * });
 * ```
 * 
 * @example
 * ```typescript
 * // Listing checkpoints with filtering
 * import { RedisSaver } from "langgraph-db/providers/redis";
 * 
 * const saver = new RedisSaver();
 * 
 * // List all checkpoints for a thread with filtering
 * const checkpoints = [];
 * for await (const checkpoint of saver.list({
 *   configurable: { thread_id: "user_123" }
 * }, {
 *   filter: { status: "completed" },  // Filter by metadata
 *   limit: 10  // Limit to 10 results
 * })) {
 *   checkpoints.push(checkpoint);
 * }
 * ```
 * 
 * @example
 * ```typescript
 * // Manual cleanup when done
 * import { RedisSaver } from "langgraph-db/providers/redis";
 * 
 * const saver = new RedisSaver();
 * 
 * try {
 *   // Use the saver...
 * } finally {
 *   // Properly disconnect when done
 *   await saver.disconnect();
 * }
 * ```
 */
export class RedisSaver extends BaseCheckpointSaver {
  client: RedisAdapter;
  ttl?: number;
  private clientOwned: boolean = false;
  private isConnecting: boolean = false;
  private connectionPromise: Promise<void> | null = null;

  storage: Record<
    string,
    Record<string, Record<string, [Uint8Array, Uint8Array, string | undefined]>>
  > = {};
  writes: Record<string, Record<string, [string, string, Uint8Array]>> = {};

  /**
   * Creates a new Redis checkpoint saver.
   *
   * @param options - Configuration options for Redis
   * @param serde - Optional serializer implementation
   */
  constructor(
    options: RedisCheckpointSaverOptions = {},
    serde?: SerializerProtocol
  ) {
    super(serde);

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
   * Disconnects the Redis client if it was created by this saver.
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
   * Sets a value in Redis with optional TTL.
   *
   * @param key - The Redis key
   * @param value - The value to store
   */
  private async _setWithTtl(
    key: string,
    value: string | Buffer
  ): Promise<void> {
    await this.connect();
    try {
      if (this.ttl) {
        await this.client.setEx(key, this.ttl, value);
      } else {
        await this.client.set(key, value);
      }
    } catch (error) {
      throw new Error(
        `Failed to store data in Redis: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }

  /**
   * Gets pending sends from the parent checkpoint.
   */
  async _getPendingSends(
    threadId: string,
    checkpointNs: string,
    parentCheckpointId?: string
  ): Promise<SendProtocol[]> {
    const pendingSends: SendProtocol[] = [];

    if (parentCheckpointId !== undefined) {
      try {
        const parentKey = _generateKey(
          threadId,
          checkpointNs,
          parentCheckpointId
        );
        const writeKeys = await this.client.keys(
          `${parentKey}:writes:*:${TASKS}:*`
        );

        const loadedSends = await Promise.all(
          writeKeys.map(async (key: string) => {
            try {
              const data = await this.client.get(key);
              if (data) {
                return this.serde.loadsTyped(
                  "json",
                  Buffer.from(data)
                ) as SendProtocol;
              }
            } catch (error) {
              console.warn(`Error loading pending send from ${key}:`, error);
            }
            return null;
          })
        );

        pendingSends.push(...(loadedSends.filter(Boolean) as SendProtocol[]));
      } catch (error) {
        console.warn(
          `Error retrieving pending sends for parent ${parentCheckpointId}:`,
          error
        );
      }
    }

    return pendingSends;
  }

  /**
   * Retrieves a checkpoint tuple by config.
   *
   * @param config - The runnable config
   * @returns The checkpoint tuple or undefined if not found
   */
  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    if (!config.configurable?.thread_id) {
      throw new Error("thread_id is required in the config");
    }

    const thread_id = config.configurable.thread_id as string;
    const checkpoint_ns = (config.configurable?.checkpoint_ns as string) ?? "";
    let checkpoint_id = getCheckpointId(config);

    await this.connect();

    try {
      if (checkpoint_id) {
        // Get specific checkpoint
        const key = _generateKey(thread_id, checkpoint_ns, checkpoint_id);
        const checkpoint = await this.client.get(`${key}:checkpoint`);
        const metadata = await this.client.get(`${key}:metadata`);
        const parentCheckpointId = await this.client.get(`${key}:parent`);

        if (checkpoint && metadata) {
          const pending_sends = await this._getPendingSends(
            thread_id,
            checkpoint_ns,
            parentCheckpointId || undefined
          );

          const deserializedCheckpoint = {
            ...(await this.serde.loadsTyped("json", Buffer.from(checkpoint))),
            pending_sends,
          };

          // Get pending writes
          const writeKeys = await this.client.keys(`${key}:writes:*`);
          const pendingWrites = (await Promise.all(
            writeKeys
              .map(async (writeKey: string) => {
                try {
                  const parts = writeKey.split(":");
                  const taskId = parts[parts.length - 3];
                  const channel = parts[parts.length - 2];
                  const value = await this.client.get(writeKey);

                  if (value) {
                    return [
                      taskId,
                      channel,
                      await this.serde.loadsTyped("json", Buffer.from(value)),
                    ] as PendingWriteTuple;
                  }
                } catch (error) {
                  console.warn(
                    `Error loading pending write from ${writeKey}:`,
                    error
                  );
                }
                return null;
              })
              .filter(Boolean)
          )) as PendingWriteTuple[];

          const checkpointTuple: CheckpointTuple = {
            config,
            checkpoint: deserializedCheckpoint,
            metadata: await this.serde.loadsTyped(
              "json",
              Buffer.from(metadata)
            ),
            pendingWrites: pendingWrites,
          };

          if (parentCheckpointId) {
            checkpointTuple.parentConfig = {
              configurable: {
                thread_id,
                checkpoint_ns,
                checkpoint_id: parentCheckpointId,
              },
            };
          }

          return checkpointTuple;
        }
      } else {
        // Get latest checkpoint
        const pattern = _generateKey(thread_id, checkpoint_ns, "*");
        const keys = await this.client.keys(`${pattern}:checkpoint`);

        if (keys.length > 0) {
          // Sort keys to get the latest checkpoint (by ID)
          keys.sort((a, b) => b.localeCompare(a));

          const key = keys[0].replace(":checkpoint", "");
          const parts = key.split(":");
          checkpoint_id = parts[parts.length - 1];

          const checkpoint = await this.client.get(`${key}:checkpoint`);
          const metadata = await this.client.get(`${key}:metadata`);
          const parentCheckpointId = await this.client.get(`${key}:parent`);

          if (checkpoint && metadata) {
            const pending_sends = await this._getPendingSends(
              thread_id,
              checkpoint_ns,
              parentCheckpointId || undefined
            );

            const deserializedCheckpoint = {
              ...(await this.serde.loadsTyped("json", Buffer.from(checkpoint))),
              pending_sends,
            };

            // Get pending writes
            const writeKeys = await this.client.keys(`${key}:writes:*`);
            const pendingWrites = (await Promise.all(
              writeKeys
                .map(async (writeKey: string) => {
                  try {
                    const parts = writeKey.split(":");
                    const taskId = parts[parts.length - 3];
                    const channel = parts[parts.length - 2];
                    const value = await this.client.get(writeKey);

                    if (value) {
                      return [
                        taskId,
                        channel,
                        await this.serde.loadsTyped("json", Buffer.from(value)),
                      ] as PendingWriteTuple;
                    }
                  } catch (error) {
                    console.warn(
                      `Error loading pending write from ${writeKey}:`,
                      error
                    );
                  }
                  return null;
                })
                .filter(Boolean)
            )) as PendingWriteTuple[];

            const checkpointTuple: CheckpointTuple = {
              config: {
                configurable: {
                  thread_id,
                  checkpoint_id,
                  checkpoint_ns,
                },
              },
              checkpoint: deserializedCheckpoint,
              metadata: await this.serde.loadsTyped(
                "json",
                Buffer.from(metadata)
              ),
              pendingWrites: pendingWrites,
            };

            if (parentCheckpointId) {
              checkpointTuple.parentConfig = {
                configurable: {
                  thread_id,
                  checkpoint_ns,
                  checkpoint_id: parentCheckpointId,
                },
              };
            }

            return checkpointTuple;
          }
        }
      }
    } catch (error) {
      throw new Error(
        `Failed to retrieve checkpoint: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }

    return undefined;
  }

  /**
   * Lists checkpoints matching the given config and options.
   *
   * @param config - The runnable config
   * @param options - Listing options
   * @returns An async generator of checkpoint tuples
   */
  async *list(
    config: RunnableConfig,
    options?: CheckpointListOptions
  ): AsyncGenerator<CheckpointTuple> {
    const { before, limit: listLimit, filter } = options ?? {};
    const threadIds = config.configurable?.thread_id
      ? [config.configurable?.thread_id as string]
      : [];

    try {
      await this.connect();

      if (threadIds.length === 0) {
        // List all thread IDs
        const keys = await this.client.keys("langgraph:*:*:*:checkpoint");
        const threadIdSet = new Set<string>();

        for (const key of keys) {
          const parts = key.split(":");
          if (parts.length >= 4) {
            threadIdSet.add(parts[1]);
          }
        }

        threadIds.push(...threadIdSet);
      }

      let remaining = listLimit;

      for (const threadId of threadIds) {
        const configCheckpointNamespace = config.configurable
          ?.checkpoint_ns as string;
        const configCheckpointId = config.configurable?.checkpoint_id as string;

        // Get all checkpoint namespaces for this thread
        const checkpointNamespaces: string[] = [];

        if (configCheckpointNamespace !== undefined) {
          checkpointNamespaces.push(configCheckpointNamespace);
        } else {
          const pattern = `langgraph:${threadId}:*:*:checkpoint`;
          const keys = await this.client.keys(pattern);

          for (const key of keys) {
            const parts = key.split(":");
            if (parts.length >= 4) {
              checkpointNamespaces.push(parts[2]);
            }
          }
        }

        for (const checkpointNamespace of [...new Set(checkpointNamespaces)]) {
          // Get all checkpoints for this namespace
          const pattern = `langgraph:${threadId}:${checkpointNamespace}:*:checkpoint`;
          const keys = await this.client.keys(pattern);

          // Sort checkpoints by ID (descending)
          keys.sort((a, b) => {
            const aId = a.split(":")[3];
            const bId = b.split(":")[3];
            return bId.localeCompare(aId);
          });

          for (const key of keys) {
            try {
              const parts = key.split(":");
              const checkpointId = parts[3];

              // Filter by checkpoint ID from config
              if (configCheckpointId && checkpointId !== configCheckpointId) {
                continue;
              }

              // Filter by checkpoint ID from before config
              if (
                before &&
                before.configurable?.checkpoint_id &&
                checkpointId >= before.configurable.checkpoint_id
              ) {
                continue;
              }

              // Parse metadata
              const baseKey = `langgraph:${threadId}:${checkpointNamespace}:${checkpointId}`;
              const metadataStr = await this.client.get(`${baseKey}:metadata`);

              if (!metadataStr) continue;

              const metadata = await this.serde.loadsTyped(
                "json",
                Buffer.from(metadataStr)
              );

              // Apply filter if specified
              if (
                filter &&
                !Object.entries(filter).every(
                  ([key, value]) => metadata[key] === value
                )
              ) {
                continue;
              }

              // Limit search results
              if (remaining !== undefined) {
                if (remaining <= 0) break;
                remaining -= 1;
              }

              // Get checkpoint data
              const checkpointData = await this.client.get(
                `${baseKey}:checkpoint`
              );
              const parentCheckpointId = await this.client.get(
                `${baseKey}:parent`
              );

              if (!checkpointData) continue;

              // Get pending sends
              const pending_sends = await this._getPendingSends(
                threadId,
                checkpointNamespace,
                parentCheckpointId || undefined
              );

              // Get pending writes
              const writeKeys = await this.client.keys(`${baseKey}:writes:*`);
              const pendingWrites = (await Promise.all(
                writeKeys
                  .map(async (writeKey: string) => {
                    try {
                      const parts = writeKey.split(":");
                      const taskId = parts[parts.length - 3];
                      const channel = parts[parts.length - 2];
                      const value = await this.client.get(writeKey);

                      if (value) {
                        return [
                          taskId,
                          channel,
                          await this.serde.loadsTyped(
                            "json",
                            Buffer.from(value)
                          ),
                        ] as PendingWriteTuple;
                      }
                    } catch (error) {
                      console.warn(
                        `Error loading pending write from ${writeKey}:`,
                        error
                      );
                    }
                    return null;
                  })
                  .filter(Boolean)
              )) as PendingWriteTuple[];

              const deserializedCheckpoint = {
                ...(await this.serde.loadsTyped(
                  "json",
                  Buffer.from(checkpointData)
                )),
                pending_sends,
              };

              const checkpointTuple: CheckpointTuple = {
                config: {
                  configurable: {
                    thread_id: threadId,
                    checkpoint_ns: checkpointNamespace,
                    checkpoint_id: checkpointId,
                  },
                },
                checkpoint: deserializedCheckpoint,
                metadata,
                pendingWrites: pendingWrites,
              };

              if (parentCheckpointId) {
                checkpointTuple.parentConfig = {
                  configurable: {
                    thread_id: threadId,
                    checkpoint_ns: checkpointNamespace,
                    checkpoint_id: parentCheckpointId,
                  },
                };
              }

              yield checkpointTuple;
            } catch (error) {
              console.warn(`Error processing checkpoint ${key}:`, error);
              // Skip this checkpoint but continue processing others
              continue;
            }
          }
        }
      }
    } catch (error) {
      throw new Error(
        `Failed to list checkpoints: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }

  /**
   * Stores a checkpoint with its configuration and metadata.
   *
   * @param config - The runnable config
   * @param checkpoint - The checkpoint data
   * @param metadata - The checkpoint metadata
   * @returns Updated runnable config
   */
  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata
  ): Promise<RunnableConfig> {
    if (!config.configurable?.thread_id) {
      throw new Error("thread_id is required in the config");
    }

    try {
      await this.connect();

      const thread_id = config.configurable.thread_id as string;
      const checkpoint_ns =
        (config.configurable?.checkpoint_ns as string) ?? "";
      const checkpoint_id = checkpoint.id;

      const key = _generateKey(thread_id, checkpoint_ns, checkpoint_id);

      // Use the serializer to convert the data to JSON format
      const [, checkpointBytes] = this.serde.dumpsTyped(checkpoint);
      const [, metadataBytes] = this.serde.dumpsTyped(metadata);

      // Convert Uint8Array to Buffer for Redis compatibility
      const checkpointBuffer = Buffer.from(checkpointBytes);
      const metadataBuffer = Buffer.from(metadataBytes);

      // Store checkpoint data
      await this._setWithTtl(`${key}:checkpoint`, checkpointBuffer);
      await this._setWithTtl(`${key}:metadata`, metadataBuffer);

      // Store parent reference if available
      const parentCheckpointId = config.configurable?.checkpoint_id as
        | string
        | undefined;
      if (parentCheckpointId) {
        await this._setWithTtl(`${key}:parent`, parentCheckpointId);
      }

      // Update the in-memory compatible structures to maintain the correct interface
      if (!this.storage[thread_id]) {
        this.storage[thread_id] = {};
      }
      if (!this.storage[thread_id][checkpoint_ns]) {
        this.storage[thread_id][checkpoint_ns] = {};
      }
      this.storage[thread_id][checkpoint_ns][checkpoint_id] = [
        checkpointBytes,
        metadataBytes,
        parentCheckpointId,
      ];

      return {
        ...config,
        configurable: {
          ...config.configurable,
          checkpoint_id,
        },
      };
    } catch (error) {
      throw new Error(
        `Failed to store checkpoint: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }

  /**
   * Stores intermediate writes linked to a checkpoint.
   *
   * @param config - The runnable config
   * @param writes - The pending writes
   * @param taskId - The task ID
   */
  async putWrites(
    config: RunnableConfig,
    writes: PendingWrite[],
    taskId: string
  ): Promise<void> {
    if (!config.configurable?.thread_id) {
      throw new Error("thread_id is required in the config");
    }

    if (!config.configurable?.checkpoint_id) {
      return; // Nothing to do without a checkpoint ID
    }

    try {
      await this.connect();

      const thread_id = config.configurable.thread_id as string;
      const checkpoint_ns =
        (config.configurable?.checkpoint_ns as string) ?? "";
      const checkpoint_id = config.configurable.checkpoint_id as string;

      const key = _generateKey(thread_id, checkpoint_ns, checkpoint_id);

      const promises: Promise<unknown>[] = [];

      // First update Redis storage
      for (const [writeIdx, write] of writes.entries()) {
        // Calculate special index for error writes
        let index = writeIdx;

        // Cast to ExtendedPendingWrite to access the properties we need
        const extendedWrite = write as ExtendedPendingWrite;
        const channel = extendedWrite.channel || "";
        const writeType = extendedWrite.writeType;
        const value = extendedWrite.value;

        if (channel === "_error" && writeType) {
          const errorIdx = WRITES_IDX_MAP[writeType];
          if (errorIdx !== undefined) {
            index = errorIdx;
          }
        }

        const writeKey = _generateWritesKey(key, taskId, channel, index);

        // Serialize the write value
        const [, writeBytes] = this.serde.dumpsTyped(value);

        // Convert to Buffer for Redis
        const writeBuffer = Buffer.from(writeBytes);

        promises.push(this._setWithTtl(writeKey, writeBuffer));

        // Also update our in-memory structure to maintain compatibility with MemorySaver
        if (!this.writes[key]) {
          this.writes[key] = {};
        }
        // Use a unique ID for each write that combines the channel and index
        const writeId = `${channel}:${index}`;
        this.writes[key][writeId] = [taskId, channel, writeBytes];
      }

      await Promise.all(promises);
    } catch (error) {
      throw new Error(
        `Failed to store writes: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }
}
