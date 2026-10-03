import time
import threading
import requests
from flask import Blueprint, request, jsonify
from replication import NodeState

election_bp = Blueprint('election', __name__)

HEARTBEAT_TIMEOUT = 3.0
HEARTBEAT_INTERVAL = 1.0

@election_bp.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.json or {}
    primary = data.get("primary")
    if primary:
        NodeState.last_heartbeat_time = time.time()
        # If we were previously primary, step down if a valid primary sends a heartbeat
        if NodeState.role == "primary" and primary != NodeState.my_address:
            # Deterministic conflict resolution: if the other primary has a higher priority (lower port/address)
            if primary < NodeState.my_address:
                print(f"[Election] Stepping down. Higher priority primary {primary} detected.")
                NodeState.role = "replica"
        NodeState.primary_address = primary
    return jsonify({"ok": True})

@election_bp.route("/ping", methods=["GET"])
def ping():
    return jsonify({"ok": True})

@election_bp.route("/role", methods=["GET"])
def role():
    return jsonify({
        "role": NodeState.role,
        "primary": NodeState.primary_address,
        "my_address": NodeState.my_address
    })

def primary_heartbeat_thread():
    """
    Sends heartbeats to replicas periodically if we are the primary.
    """
    while True:
        if NodeState.role == "primary":
            for peer in NodeState.peers:
                if peer == NodeState.my_address:
                    continue
                try:
                    requests.post(
                        f"{peer}/heartbeat",
                        json={"primary": NodeState.my_address},
                        timeout=0.5
                    )
                except Exception:
                    pass
        time.sleep(HEARTBEAT_INTERVAL)

def replica_monitor_thread():
    """
    Monitors heartbeats and triggers election if primary is dead.
    """
    while True:
        if NodeState.role == "replica":
            elapsed = time.time() - NodeState.last_heartbeat_time
            if elapsed > HEARTBEAT_TIMEOUT:
                print(f"[Election] Heartbeat timeout! Elapsed: {elapsed:.2f}s. Entering election...")
                
                # Determine priority based on sorted peers
                sorted_peers = sorted(NodeState.peers)
                try:
                    my_index = sorted_peers.index(NodeState.my_address)
                except ValueError:
                    my_index = 0
                
                # Check all peers before us in the list
                is_highest_alive = True
                for i in range(my_index):
                    peer = sorted_peers[i]
                    try:
                        resp = requests.get(f"{peer}/ping", timeout=0.5)
                        if resp.status_code == 200:
                            is_highest_alive = False
                            print(f"[Election] Higher priority peer {peer} is alive. Waiting...")
                            break
                    except Exception:
                        pass
                
                if is_highest_alive:
                    print(f"[Election] Promoting self to primary: {NodeState.my_address}")
                    NodeState.role = "primary"
                    NodeState.primary_address = NodeState.my_address
                else:
                    # Reset heartbeat timer to give the higher-priority node time to promote
                    NodeState.last_heartbeat_time = time.time()
                    
        time.sleep(0.5)

def start_election_threads():
    t1 = threading.Thread(target=primary_heartbeat_thread, daemon=True)
    t2 = threading.Thread(target=replica_monitor_thread, daemon=True)
    t1.start()
    t2.start()
