import os
import json

WAL_FILE = "wal.log"

def wal_append(operation, key, value=None):
    with open(WAL_FILE, "a") as f:
        entry = {"op": operation, "key": key, "value": value}
        f.write(json.dumps(entry) + "\n")

def wal_replay():
    store = {}
    if not os.path.exists(WAL_FILE):
        return store
    with open(WAL_FILE, "r") as f:
        for line in f:
            entry = json.loads(line.strip())
            if entry["op"] == "set":
                store[entry["key"]] = entry["value"]
            elif entry["op"] == "delete":
                store.pop(entry["key"], None)
    return store