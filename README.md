# Raft

An implementation of the [Raft](https://raft.github.io) distributed consensus algorithm in Go.

This implementation tries to implement Raft with:

- Leader election
- Log replication
- Persistence
- Membership changes (joint consensus)
- Log compaction (snapshotting)

## Roadmap

- [x] API server
- [x] Persistence (with bbolt)
- [x] gRPC transport
- [x] KV store (as an example)
- [x] Snapshotting
- [ ] Full tests
- [ ] Replication optimization
- [ ] Improve API server
- [ ] Internal metrics
- [ ] Logger optimization
