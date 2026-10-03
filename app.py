from flask import Flask, request, jsonify
from wal import set_wal_file
from collections import OrderedDict
import time
import threading
import argparse
from raft import raft_bp, node

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

WRITE_ERRORS = {
    "not_leader": (403, "Write rejected: this node is not the Raft leader."),
    "lost_leadership": (503, "Leadership changed before the write was committed. Retry against the new leader."),
    "no_quorum": (503, "Write timed out: a majority of nodes is not reachable."),
}

def write(op, key, value=None, ttl=None):
    ok, error = node.propose(op, key, value, ttl)
    if ok:
        return jsonify({"ok": True})
    status, message = WRITE_ERRORS[error]
    return jsonify({"error": message, "leader": node.status()["primary"]}), status

@app.route("/set", methods=["POST"])
def set_key():
    data = request.json
    if not data or "key" not in data or "value" not in data:
        return jsonify({"error": "key and value required"}), 400
    return write("set", data["key"], data["value"], data.get("ttl"))

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
    key = request.args.get("key")
    if not key:
        return jsonify({"error": "key required"}), 400
    return write("delete", key)

@app.route("/keys", methods=["GET"])
def get_keys():
    return jsonify({"keys": store.keys()})

app.register_blueprint(raft_bp)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--peers", type=str, default="")
    parser.add_argument("--capacity", type=int, default=MAX_KEYS)
    args = parser.parse_args()
    store.capacity = args.capacity

    port = args.port
    my_address = f"http://localhost:{port}"
    peers = [p.strip() for p in args.peers.split(",") if p.strip()]

    # The Raft log is this node's write-ahead log. Committed entries are
    # re-applied to the in-memory store once a leader confirms them.
    set_wal_file(f"raft_{port}.log")
    node.configure(my_address, peers, store, meta_file=f"raft_{port}.json")
    print(f"Starting Raft node {my_address} (term {node.current_term}, {len(node.log)} log entries)")
    print(f"Peers: {node.peers}")
    node.start()

    app.run(port=port, debug=False, threaded=True)
