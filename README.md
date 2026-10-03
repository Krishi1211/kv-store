# kv-store

A distributed key-value store built from scratch in Python.

## Features
- [x] In-memory KV store with HTTP API
- [x] Write-Ahead Log for crash recovery
- [x] TTL expiry + LRU eviction
- [x] Primary-replica replication
- [x] Leader election + heartbeats
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
The lowest address starts as primary and replicates every write to the others.
Replicas reject writes with a 403 that names the current primary. If the primary
stops sending heartbeats for 3 seconds, the highest-priority live replica promotes
itself. `GET /role` shows each node's role. `--capacity` sets the LRU size (default 10000).

## Benchmark
```bash
python benchmark.py --url http://localhost:8000 --count 1000
```
Prints p50/p95/p99 latency and throughput for SET and GET, and writes a latency
dashboard to `dashboard.html`.
