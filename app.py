from flask import Flask, request, jsonify
from wal import wal_append, wal_replay, set_wal_file
from collections import OrderedDict
import time
import threading
import argparse
from replication import replication_bp, NodeState, is_primary, replicate_write
from election import election_bp, start_election_threads

app = Flask(__name__)

MAX_KEYS = 10000  # default LRU capacity, override with --capacity

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()
        self.ttl = {}  # key -> expiry timestamp
        self.lock = threading.Lock()

    def _is_expired(self, key):
        if key in self.ttl:
            if time.time() > self.ttl[key]:
                self._delete(key)
                return True
        return False

    def get(self, key):
        with self.lock:
            if key not in self.cache or self._is_expired(key):
                return None
            self.cache.move_to_end(key)
            return self.cache[key]

    def set(self, key, value, ttl_seconds=None):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if ttl_seconds:
                self.ttl[key] = time.time() + ttl_seconds
            elif key in self.ttl:
                del self.ttl[key]
            if len(self.cache) > self.capacity:
                evicted, _ = self.cache.popitem(last=False)
                self.ttl.pop(evicted, None)
                print(f"LRU evicted: {evicted}")

    def delete(self, key):
        with self.lock:
            self._delete(key)

    def _delete(self, key):
        self.cache.pop(key, None)
        self.ttl.pop(key, None)

    def keys(self):
        with self.lock:
            return [k for k in self.cache if not self._is_expired(k)]


store = LRUCache(capacity=MAX_KEYS)

# WAL replay will be executed on startup after port configuration

@app.route("/set", methods=["POST"])
def set_key():
    if not is_primary():
        return jsonify({"error": f"Write rejected: Node is a read-only replica. Current primary: {NodeState.primary_address}"}), 403
    data = request.json
    if not data or "key" not in data or "value" not in data:
        return jsonify({"error": "key and value required"}), 400
    ttl = data.get("ttl")
    wal_append("set", data["key"], data["value"])
    store.set(data["key"], data["value"], ttl_seconds=ttl)
    replicate_write("set", data["key"], data["value"], ttl)
    return jsonify({"ok": True})

@app.route("/get", methods=["GET"])
def get_key():
    key = request.args.get("key")
    if not key:
        return jsonify({"error": "key required"}), 400
    value = store.get(key)
    if value is None:
        return jsonify({"error": "key not found or expired"}), 404
    return jsonify({"value": value})

@app.route("/delete", methods=["DELETE"])
def delete_key():
    if not is_primary():
        return jsonify({"error": f"Write rejected: Node is a read-only replica. Current primary: {NodeState.primary_address}"}), 403
    key = request.args.get("key")
    if not key:
        return jsonify({"error": "key required"}), 400
    wal_append("delete", key)
    store.delete(key)
    replicate_write("delete", key)
    return jsonify({"ok": True})

@app.route("/keys", methods=["GET"])
def get_keys():
    return jsonify({"keys": store.keys()})

app.register_blueprint(replication_bp)
app.register_blueprint(election_bp)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--peers", type=str, default="")
    parser.add_argument("--capacity", type=int, default=MAX_KEYS)
    args = parser.parse_args()
    store.capacity = args.capacity

    port = args.port
    
    # Configure NodeState
    NodeState.store = store
    NodeState.my_address = f"http://localhost:{port}"
    NodeState.last_heartbeat_time = time.time()
    
    if args.peers:
        NodeState.peers = sorted([p.strip() for p in args.peers.split(",") if p.strip()])
    else:
        NodeState.peers = [NodeState.my_address]
        
    if len(NodeState.peers) <= 1:
        NodeState.role = "primary"
        NodeState.primary_address = NodeState.my_address
    else:
        if NodeState.my_address == NodeState.peers[0]:
            NodeState.role = "primary"
            NodeState.primary_address = NodeState.my_address
        else:
            NodeState.role = "replica"
            NodeState.primary_address = NodeState.peers[0]

    print(f"Starting node on port {port} as {NodeState.role.upper()}...")
    print(f"Peers: {NodeState.peers}")

    # Set up and replay WAL
    set_wal_file(f"wal_{port}.log")
    raw = wal_replay()
    for k, v in raw.items():
        store.set(k, v)
    print(f"Restored {len(raw)} keys from WAL ({f'wal_{port}.log'})")

    # Start replication/election threads
    start_election_threads()

    app.run(port=port, debug=False)