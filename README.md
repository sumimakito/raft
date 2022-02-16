# Raft

![workflow-test](https://github.com/sumimakito/raft/actions/workflows/test.yml/badge.svg) [![codecov](https://codecov.io/gh/SumiMakito/raft/branch/main/graph/badge.svg)](https://codecov.io/gh/SumiMakito/raft)

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

## License

This implementation is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
