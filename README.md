# Tessera

Compact data structures and utilities for tracking metadata changes in document-like data.

## Overview

Tessera provides space-efficient data structures for storing and querying modification metadata over hierarchical (tree-shaped) data, such as JSON documents. It focuses on compact binary serialization that preserves full structural information while minimizing storage.

## Getting Started

### Prerequisites

- JDK 11+
- sbt 1.x

### Installation

```bash
git clone <repo-url>
cd tessera
sbt compile
```

### Quick Example

```scala
import com.pawelzabczynski.tessera.tree.TimestampTree
import java.time.Instant

val baseTime = Instant.parse("2025-12-26T15:45:00.000Z")
val tree = TimestampTree("root", baseTime)

// Track modifications at specific paths
tree.put(List("root", "users", "alice", "email"), baseTime.plusSeconds(60))
tree.put(List("root", "users", "bob", "name"),    baseTime.plusSeconds(120))

// Query last modification time (relative to base, in millis)
tree.modifyAt(List("root", "users", "alice", "email")) // 60000 [ms]
tree.modifyAt(List("root", "users"))                    // 120000 [ms] (max of subtree, latest update)

// Serialize to compact binary and restore
val bytes    = tree.serialize()
val restored = TimestampTree.deserialize(bytes)
```

## Features

- **TimestampTree** -- tree structure tracking per-path modification timestamps with subtree-level queries
- **Succinct binary serialization** -- balanced parenthesis topology encoding (2 bits/node), delta+zigzag+varint compressed timestamps
- **Round-trip fidelity** -- `deserialize(serialize(tree))` preserves all structural and temporal data

## Binary Format

```
+---------+---------+------+---------+---------+----------+
| version | rootLen | root | nodeCnt | leafCnt | timeBase |
| 1b      | 1b      | Nb   | 2b      | 2b      | 8b       |
+---------+---------+------+---------+---------+----------+
| topology (balanced parenthesis, 2 bits/node)             |
+----------------------------------------------------------+
| node IDs (XXHash32, 4b each, DFS pre-order)              |
+----------------------------------------------------------+
| timestamps (leaf-only, delta+zigzag+varint, DFS order)   |
+----------------------------------------------------------+
```

## API Reference

| Type | Description |
|---|---|
| `TimestampTree` | Main tree structure. Factory via `apply(root, baseTime)`, deserialize via `deserialize(bytes)`. |
| `TimestampNode.LeafNode` | Leaf node holding an `id` and `modifiedAt` timestamp. |
| `TimestampNode.DataNode` | Internal node holding an `id` and a mutable set of child IDs. |

### Key methods

| Method | Description                                                                               |
|---|-------------------------------------------------------------------------------------------|
| `put(path, timestamp)` | Insert or update a path with a modification timestamp.                                    |
| `modifyAt(path)` | Query the latest modification time at a path (max/newest over subtree for internal nodes). |
| `serialize()` | Compact binary serialization of the full tree.                                            |
| `TimestampTree.deserialize(bytes)` | Reconstruct a tree from its serialized form.                                              |

## Contributing

Contributions are welcome. Please open an issue before submitting large changes.

## License

TBD