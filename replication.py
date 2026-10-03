import requests
from flask import Blueprint, request, jsonify
from wal import wal_append

replication_bp = Blueprint('replication', __name__)

class NodeState:
    role = "replica"
    primary_address = None
    peers = []
    my_address = None
    store = None
    last_heartbeat_time = 0.0

@replication_bp.route("/replicate", methods=["POST"])
def replicate():
    data = request.json
    if not data or "op" not in data or "key" not in data:
        return jsonify({"error": "Invalid replication data"}), 400
    
    op = data["op"]
    key = data["key"]
    value = data.get("value")
    ttl = data.get("ttl")
    
    # Apply to local WAL and memory store
    wal_append(op, key, value)
    if op == "set":
        NodeState.store.set(key, value, ttl_seconds=ttl)
    elif op == "delete":
        NodeState.store.delete(key)
        
    return jsonify({"ok": True})

def replicate_write(op, key, value=None, ttl=None):
    """
    Called by primary on every write to replicate the operation to all peers.
    """
    payload = {"op": op, "key": key, "value": value, "ttl": ttl}
    for peer in NodeState.peers:
        if peer == NodeState.my_address:
            continue
        try:
            # Send replication request to the peer
            response = requests.post(f"{peer}/replicate", json=payload, timeout=2.0)
            if response.status_code != 200:
                print(f"Replication failed to {peer}: {response.status_code}")
        except Exception as e:
            print(f"Failed to connect to replica {peer} for replication: {e}")

def is_primary():
    return NodeState.role == "primary"
