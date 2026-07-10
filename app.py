from flask import Flask, request, jsonify
from wal import wal_append, wal_replay

app = Flask(__name__)
store = wal_replay()  # restore state on startup

print(f"Restored {len(store)} keys from WAL")

@app.route("/set", methods=["POST"])
def set_key():
    data = request.json
    if not data or "key" not in data or "value" not in data:
        return jsonify({"error": "key and value required"}), 400
    wal_append("set", data["key"], data["value"])
    store[data["key"]] = data["value"]
    return jsonify({"ok": True})

@app.route("/get", methods=["GET"])
def get_key():
    key = request.args.get("key")
    if not key:
        return jsonify({"error": "key required"}), 400
    if key not in store:
        return jsonify({"error": "key not found"}), 404
    return jsonify({"value": store[key]})

@app.route("/delete", methods=["DELETE"])
def delete_key():
    key = request.args.get("key")
    if not key or key not in store:
        return jsonify({"error": "key not found"}), 404
    wal_append("delete", key)
    del store[key]
    return jsonify({"ok": True})

@app.route("/keys", methods=["GET"])
def get_keys():
    return jsonify({"keys": list(store.keys())})

if __name__ == "__main__":
    app.run(port=5000, debug=True)