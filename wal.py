import os
import json

WAL_FILE = "wal.log"

def set_wal_file(filename):
    global WAL_FILE
    WAL_FILE = filename


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


# ---- Raft log persistence ----------------------------------------------------
# The Raft log is the write-ahead log: one JSON entry per line, fsynced before
# the node answers the RPC that produced it.

def wal_append_entry(entry):
    with open(WAL_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
        f.flush()
        os.fsync(f.fileno())

def wal_load():
    entries = []
    if not os.path.exists(WAL_FILE):
        return entries
    with open(WAL_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                break  # torn final write from a crash: ignore it and everything after
    return entries

def wal_rewrite(entries):
    tmp = WAL_FILE + ".tmp"
    with open(tmp, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, WAL_FILE)
