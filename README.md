# kv-store

A Raft-based distributed key-value store built from scratch in Python.

## Features
- [x] In-memory KV store with HTTP API
- [x] Write-Ahead Log for crash recovery
- [x] TTL expiry + LRU eviction
- [x] Log replication with majority commit (Raft)
- [x] Leader election with terms and votes (Raft)
- [x] Benchmark suite + latency dashboard

## Stack
Python, Flask

## Run locally
```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python app.py
```

## API
```
POST /set   body: {"key": "foo", "value": "bar"}
GET  /get   query: ?key=foo
```

## Run a 3-node cluster
```bash
PEERS=http://localhost:8000,http://localhost:8001,http://localhost:8002
python app.py --port 8000 --peers $PEERS
python app.py --port 8001 --peers $PEERS
python app.py --port 8002 --peers $PEERS
```
`GET /role` on any node shows its Raft state, term, leader, log length and
commit index. `--capacity` sets the LRU size (default 10000).

## Consensus (Raft)
Replication and leader election follow the Raft algorithm (`raft.py`):

- **Election:** a follower that hears no heartbeat for a randomized 1-2 s
  starts an election for a new term. A node votes once per term, and only for
  a candidate whose log is at least as up to date as its own.
- **Replication:** the leader appends each write to its log and sends it to
  followers with `AppendEntries`. The write is acknowledged to the client only
  after a majority has stored it, then applied to the in-memory store.
- **Durability:** each node persists its term and vote (`raft_<port>.json`) and
  its log (`raft_<port>.log`, the write-ahead log) with fsync before replying.
- **Catch-up:** a node that was down or has a diverging log is brought back in
  line by the leader backing up to the last matching entry and resending.
- **Safety:** writes to a follower return 403 with the leader's address; a
  leader that cannot reach a majority returns 503 instead of acknowledging.

Reads are served from the local node and can be slightly stale on a follower.
Not implemented: log compaction/snapshots and cluster membership changes.

```bash
python test_raft.py    # 3-node end-to-end test: failover, catch-up, quorum loss, restart
```

## Benchmark
```bash
python benchmark.py --url http://localhost:8000 --count 1000
```
Prints p50/p95/p99 latency and throughput for SET and GET, and writes a latency
dashboard to `dashboard.html`.
