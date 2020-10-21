# Roadmap

I'd love to put some work into the APIs and to fix issues / bugs users might run into. However, this would need actual users. Afterwards I'd love to release version 1.0.0.

From there on or even before releasing 1.0.0 I'd love to put work into our [**web front-end**](https://github.com/sirixdb/sirix-svelte-frontend) in order to be able to visualize differences between revisions, to execute queries, to be able to update databases and resources and to open and display (an overview) about specific revisions.

# Roadmap for 1.0.0
- For JSON: Implement the copy-operations from other resources
- A way to serialize the first revision of a resource, then diff every two consecutive revisions, provide diff-files, which describe the changes and be able to apply these
- SirixDB Kotlin Client for interacting with the SirixDB-server
- Fixing bugs

## Near future

- **Sharding** I'll have a look into how best to write and read from a distributed transaction log based on Apache BookKeeper most probably. Main goal is to shard SirixDB databases, that is replicate resources, partition a database... *However a community discussion would be best*.

- **Rewrite rules for the query compiler** We have to figure out how to rewrite the AST in Brackit to automatically take indexes and various statistics into account. In the future: Cost based optimizer. However I'm no query compiler expert, so as always a community effort would be awesome

## In the long run

- **Full text index** Would be awesome to provide full text indexing and querying capabilitie

- **Support for storing and querying other data (Graphs?!)**

- **Implement other index structures**


