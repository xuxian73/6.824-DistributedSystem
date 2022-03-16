# Sharded Key/Value Service
The course project of 6.824 Distributed System. The project implement the following function.

* Lab2: implement Raft algorithm, a replicated state machine protocol.
* Lab3: build a fault-tolerant key/value storage service using Raft library in Lab2.
* Lab4: build a sharded storage system able to shift shards among replica groups.
* Challenge1: Garbage collection: When a replica group loses ownership of a shard, eliminate the keys that it lost from its database.
* Challenge2: Handle client operations for keys in unaffected shards during a configuration change.