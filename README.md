# LangGraph-DB

<!-- ![NPM License](https://img.shields.io/npm/l/langgraph-db) -->

![npm](https://img.shields.io/npm/v/langgraph-db)

A powerful memory backend for [LangGraph.js](https://langchain-ai.github.io/langgraphjs/) that provides short-term and long-term memory for your agents using flexible storage providers.

## Features

- **Multiple Storage Providers**: Seamlessly integrate with Redis, MongoDB, Prisma, and more (Redis currently implemented)
- **Memory Management**: Easily handle both short-term and long-term memory for LLM agents
- **Checkpoint Support**: Built on top of LangGraph's checkpoint system for reliable state management
- **Flexible Architecture**: Abstract base classes allow for easy extension with new providers
- **TypeScript-First**: Fully typed API for improved developer experience

### Technical Architecture

LangGraph-DB implements the checkpoint interfaces from LangGraph.js, featuring two core components:

1. **Saver**: Manages short-term memory (thread-level)
2. **Store**: Handles long-term persistence (cross-thread)

Both components are implemented using an adapter pattern, allowing seamless integration with various storage backends while maintaining a consistent API.

## Installation

```bash
npm install langgraph-db
```

## Quick Start

### Redis Provider

```typescript
import { RedisStore, RedisSaver } from "langgraph-db";

// Create a Redis store for persistent memory
const store = new RedisStore({
  url: "redis://localhost:6379",
  ttl: 3600, // Optional TTL in seconds
});

// Create a Redis checkpoint saver
const saver = new RedisSaver({
  url: "redis://localhost:6379",
  ttl: 3600, // Optional TTL in seconds
});

// Use with LangGraph
import { StateGraph, Checkpoint } from "langchain/langgraph";

const graph = new StateGraph({
  channels: {
    // Your channels here
  },
  // Configure with Redis persistence
  checkpointer: new Checkpoint({
    store,
    saver,
  }),
});
```

### Using an Existing Redis Client

```typescript
import { createClient } from "redis";
import { RedisStore, RedisSaver } from "langgraph-db";

// Use your existing Redis client
const redisClient = createClient({
  url: "redis://localhost:6379",
});

// Pass the client directly to the store and saver
const store = new RedisStore({ client: redisClient });
const saver = new RedisSaver({ client: redisClient });
```

## Supported Providers

| Provider      | Status         |
| ------------- | -------------- |
| Redis/Upstash | ✅ Available   |
| MongoDB       | 🔜 Coming Soon |
| Prisma        | 🔜 Coming Soon |

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

Distributed under the MIT License. See `LICENSE` for more information.

## Links

- [Documentation](https://langgraph-db.pratikpatil.me)
- [GitHub Repository](https://github.com/0xpratikpatil/langgraph-db)
- [NPM Package](https://www.npmjs.com/package/langgraph-db)
