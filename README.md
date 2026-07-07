# kv-store

A distributed key-value store built from scratch in Python.

## Features (in progress)
- [x] In-memory KV store with HTTP API
- [x] Write-Ahead Log for crash recovery
- [ ] TTL expiry + LRU eviction
- [ ] Primary-replica replication
- [ ] Leader election + heartbeats
- [ ] Benchmark suite + latency dashboard

## Stack
Python, Flask

## Run locally
```bash
python3 -m venv venv && source venv/bin/activate
pip install flask
python app.py
```

## API
```
POST /set   body: {"key": "foo", "value": "bar"}
GET  /get   query: ?key=foo
```