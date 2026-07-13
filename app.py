from flask import Flask, request, jsonify
from wal import wal_append, wal_replay
from collections import OrderedDict
import time
import threading

app = Flask(__name__)

MAX_KEYS = 5  # LRU limit for testing, raise later

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

# replay WAL into LRU cache on startup
raw = wal_replay()
for k, v in raw.items():
    store.set(k, v)
print(f"Restored {len(raw)} keys from WAL")


@app.route("/set", methods=["POST"])
def set_key():
    data = request.json
    if not data or "key" not in data or "value" not in data:
        return jsonify({"error": "key and value required"}), 400
    ttl = data.get("ttl")
    wal_append("set", data["key"], data["value"])
    store.set(data["key"], data["value"], ttl_seconds=ttl)
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
    key = request.args.get("key")
    if not key:
        return jsonify({"error": "key required"}), 400
    wal_append("delete", key)
    store.delete(key)
    return jsonify({"ok": True})

@app.route("/keys", methods=["GET"])
def get_keys():
    return jsonify({"keys": store.keys()})

if __name__ == "__main__":
    app.run(port=5000, debug=True)