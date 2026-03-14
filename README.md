# NexusDB

NexusDB is a high-performance database system featuring **SageTree**, a novel B-tree storage engine designed for superior performance and modern concurrency requirements.

## Overview

NexusDB provides a complete database solution with:

- **SageTree Storage Engine**: Advanced B-tree implementation with delta chains and fractional cascading
- **SQL Support**: Full SQL query processing and optimization
- **MVCC**: Multi-version concurrency control for snapshot isolation
- **High Performance**: Optimized for both read and write workloads

## SageTree Storage Engine

SageTree is the core storage engine of NexusDB, combining several advanced techniques:

### Key Features

- **Bw-tree Style Delta Chains**: Reduces write amplification by up to 10x compared to traditional LSM trees
- **Fractional Cascading**: Optimizes range queries with sibling node hints
- **MVCC Support**: Snapshot isolation and concurrent read access
- **Lock-Free Reads**: Improved read concurrency through delta chains

### Quick Example

```rust
use nexus_storage::sagetree::{SageTree, KeyRange};
use nexus_common::types::{Key, Value};

// Create a tree
let tree = SageTree::new();

// Insert data
tree.insert(Key::from_str("user:1"), Value::from_str("Alice")).unwrap();

// Point lookup
let value = tree.get(&Key::from_str("user:1")).unwrap();

// Range scan
let range = KeyRange::prefix(Key::from_str("user:"));
let users = tree.scan(range).unwrap();
```

For detailed documentation, see [docs/SAGETREE.md](docs/SAGETREE.md).

## Project Structure

```
nexusdb/
├── crates/
│   ├── nexus-storage/     # Storage engine (SageTree)
│   ├── nexus-sql/         # SQL query processing
│   ├── nexus-common/      # Common types and utilities
│   └── ...
├── benches/               # Performance benchmarks
├── proto/                 # Protocol definitions
└── tools/                 # Development tools
```

## Building

```bash
# Build the project
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

## License

See LICENSE file for details.
