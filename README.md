# langgraph-db

![langgraph-db](https://img.shields.io/badge/langgraph--db-v1.0.0-blue.svg) ![GitHub Releases](https://img.shields.io/badge/releases-latest-yellow.svg)

Welcome to **langgraph-db**! This repository serves as a powerful memory backend for LangGraph.js, designed to enhance your agents with both short-term and long-term memory capabilities. By utilizing flexible storage providers, langgraph-db ensures that your agents can efficiently manage and access memory, improving their performance and effectiveness.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Supported Storage Providers](#supported-storage-providers)
- [Installation](#installation)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)
- [Releases](#releases)

## Features

- **Short-term Memory**: Quickly store and retrieve temporary data for immediate tasks.
- **Long-term Memory**: Persist important information for future use, enhancing agent learning.
- **Flexible Storage Options**: Choose from various storage providers to fit your needs.
- **TypeScript Support**: Built with TypeScript for better type safety and developer experience.
- **Integration with LangChain**: Seamlessly work with LangChain and other AI frameworks.

## Getting Started

To get started with langgraph-db, you will need to set up your environment and install the necessary dependencies. Follow these steps:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/policepattaya/langgraph-db.git
   cd langgraph-db
   ```

2. **Install Dependencies**:
   ```bash
   npm install
   ```

3. **Configure Your Storage Provider**: Depending on your choice of storage, you will need to configure the connection settings. Refer to the [Supported Storage Providers](#supported-storage-providers) section for details.

## Usage

Here’s a simple example of how to use langgraph-db in your project:

```typescript
import { LangGraphDB } from 'langgraph-db';

// Initialize the memory backend
const memory = new LangGraphDB({
    provider: 'mongodb', // or 'redis'
    connectionString: 'your_connection_string_here'
});

// Store short-term memory
memory.storeShortTerm('key', 'value');

// Retrieve short-term memory
const value = memory.retrieveShortTerm('key');
console.log(value); // Output: value

// Store long-term memory
memory.storeLongTerm('key', 'persistent_value');

// Retrieve long-term memory
const persistentValue = memory.retrieveLongTerm('key');
console.log(persistentValue); // Output: persistent_value
```

## Supported Storage Providers

langgraph-db supports various storage options to suit different use cases:

- **MongoDB**: A popular NoSQL database that offers flexibility and scalability.
- **Redis**: An in-memory data structure store, ideal for caching and quick access.
- **Prisma**: An ORM that simplifies database interactions and migrations.
- **Vector Databases**: For advanced memory management and similarity searches.

Choose the provider that best fits your application's requirements.

## Installation

To install langgraph-db, you can use npm:

```bash
npm install langgraph-db
```

Make sure to have Node.js installed on your machine. You can check your Node.js version with:

```bash
node -v
```

If you encounter any issues during installation, please refer to the troubleshooting section in the documentation.

## Contributing

We welcome contributions to langgraph-db! If you would like to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature/YourFeature`).
3. Make your changes and commit them (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature/YourFeature`).
5. Open a pull request.

Please ensure that your code follows the project's coding standards and includes appropriate tests.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For questions or feedback, please reach out to the maintainers:

- **Email**: support@langgraph-db.com
- **GitHub**: [policepattaya](https://github.com/policepattaya)

## Releases

For the latest releases, visit our [Releases](https://github.com/policepattaya/langgraph-db/releases) section. Download and execute the necessary files to keep your installation up to date.

---

Thank you for checking out langgraph-db! We hope it helps you build smarter agents with enhanced memory capabilities.