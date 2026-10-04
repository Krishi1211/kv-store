"""Raft consensus for the KV store: leader election, log replication, commit.

Follows the Raft paper (Ongaro & Ousterhout). Each node keeps a persistent
term, vote and log; a write is acknowledged only after a majority of nodes
have stored it. Nodes that were down catch up from the leader's log when they
return. Not implemented: log compaction/snapshots and membership changes.
"""
import json
import os
import random
import threading
import time

import requests
from flask import Blueprint, request, jsonify

from wal import wal_load, wal_append_entry, wal_rewrite

raft_bp = Blueprint("raft", __name__)

HEARTBEAT_INTERVAL = 0.15
ELECTION_TIMEOUT = (1.0, 2.0)   # randomized per node, per round
RPC_TIMEOUT = 0.5
COMMIT_TIMEOUT = 5.0
MAX_BATCH = 500

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


class RaftNode:
    def __init__(self):
        self.lock = threading.RLock()
        self.commit_cond = threading.Condition(self.lock)
        self.my_address = None
        self.peers = []          # other nodes only
        self.store = None
        self.meta_file = None

        # persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []            # entries {"term", "op", "key", "value", "expires_at"}; index is position + 1

        # volatile state
        self.state = FOLLOWER
        self.leader_address = None
        self.commit_index = 0
        self.last_applied = 0
        self.election_deadline = 0.0
        self.next_index = {}
        self.match_index = {}
        self.wakeups = {}        # peer -> Event, set when there is something new to send

    # ---- setup -------------------------------------------------------------

    def configure(self, my_address, peers, store, meta_file):
        self.my_address = my_address
        self.peers = [p for p in peers if p != my_address]
        self.store = store
        self.meta_file = meta_file
        if os.path.exists(meta_file):
            with open(meta_file) as f:
                meta = json.load(f)
            self.current_term = meta["current_term"]
            self.voted_for = meta["voted_for"]
        self.log = wal_load()
        self._reset_election_timer()

    def start(self):
        threading.Thread(target=self._election_loop, daemon=True).start()
        for peer in self.peers:
            self.wakeups[peer] = threading.Event()
            threading.Thread(target=self._replicate_loop, args=(peer,), daemon=True).start()

    # ---- helpers (call with lock held) -------------------------------------

    def _persist_meta(self):
        tmp = self.meta_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"current_term": self.current_term, "voted_for": self.voted_for}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.meta_file)

    def _reset_election_timer(self):
        self.election_deadline = time.time() + random.uniform(*ELECTION_TIMEOUT)

    def _last_log(self):
        return len(self.log), (self.log[-1]["term"] if self.log else 0)

    def _majority(self):
        return (len(self.peers) + 1) // 2 + 1

    def _step_down(self, term):
        """Any RPC carrying a newer term turns this node into a follower."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._persist_meta()
        if self.state != FOLLOWER:
            print(f"[Raft] {self.state} -> follower (term {self.current_term})")
        self.state = FOLLOWER

    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            if entry["op"] == "set":
                expires_at = entry.get("expires_at")
                if expires_at is None:
                    self.store.set(entry["key"], entry["value"])
                elif expires_at > time.time():
                    self.store.set(entry["key"], entry["value"], ttl_seconds=expires_at - time.time())
                else:
                    self.store.delete(entry["key"])
            elif entry["op"] == "delete":
                self.store.delete(entry["key"])
        self.commit_cond.notify_all()

    # ---- client writes ------------------------------------------------------

    def propose(self, op, key, value=None, ttl=None):
        """Appends a write on the leader and blocks until a majority has it.

        Returns (ok, error). Only the leader accepts writes.
        """
        with self.lock:
            if self.state != LEADER:
                return False, "not_leader"
            entry = {
                "term": self.current_term, "op": op, "key": key, "value": value,
                "expires_at": time.time() + ttl if ttl else None,
            }
            self.log.append(entry)
            wal_append_entry(entry)
            index, term = len(self.log), self.current_term
            if not self.peers:
                self.commit_index = index
                self._apply_committed()
        for event in self.wakeups.values():
            event.set()

        deadline = time.time() + COMMIT_TIMEOUT
        with self.lock:
            while self.last_applied < index:
                if self.current_term != term or self.state != LEADER:
                    return False, "lost_leadership"
                remaining = deadline - time.time()
                if remaining <= 0:
                    return False, "no_quorum"
                self.commit_cond.wait(remaining)
            return True, None

    # ---- elections ----------------------------------------------------------

    def _election_loop(self):
        while True:
            time.sleep(0.05)
            with self.lock:
                if self.state == LEADER or time.time() < self.election_deadline:
                    continue
                self.state = CANDIDATE
                self.current_term += 1
                self.voted_for = self.my_address
                self._persist_meta()
                self._reset_election_timer()
                term = self.current_term
                last_index, last_term = self._last_log()
                print(f"[Raft] election timeout, standing for term {term}")
            self._run_election(term, last_index, last_term)

    def _run_election(self, term, last_index, last_term):
        votes = [1]  # own vote

        def ask(peer):
            try:
                reply = requests.post(f"{peer}/raft/request_vote", json={
                    "term": term, "candidate": self.my_address,
                    "last_log_index": last_index, "last_log_term": last_term,
                }, timeout=RPC_TIMEOUT).json()
            except Exception:
                return
            with self.lock:
                if reply["term"] > self.current_term:
                    self._step_down(reply["term"])
                    return
                if self.state != CANDIDATE or self.current_term != term:
                    return
                if reply["vote_granted"]:
                    votes[0] += 1
                    if votes[0] >= self._majority():
                        self._become_leader()

        threads = [threading.Thread(target=ask, args=(p,), daemon=True) for p in self.peers]
        for t in threads:
            t.start()
        with self.lock:
            if not self.peers and self.state == CANDIDATE and self.current_term == term:
                self._become_leader()

    def _become_leader(self):
        print(f"[Raft] elected leader for term {self.current_term}")
        self.state = LEADER
        self.leader_address = self.my_address
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0
        # A leader may only commit entries from its own term, so start the term
        # with a no-op. Committing it also commits everything before it.
        entry = {"term": self.current_term, "op": "noop", "key": None, "value": None, "expires_at": None}
        self.log.append(entry)
        wal_append_entry(entry)
        if not self.peers:
            self.commit_index = len(self.log)
            self._apply_committed()
        for event in self.wakeups.values():
            event.set()

    def handle_request_vote(self, req):
        with self.lock:
            if req["term"] > self.current_term:
                self._step_down(req["term"])
            granted = False
            if req["term"] == self.current_term and self.voted_for in (None, req["candidate"]):
                last_index, last_term = self._last_log()
                up_to_date = (req["last_log_term"], req["last_log_index"]) >= (last_term, last_index)
                if up_to_date:
                    granted = True
                    self.voted_for = req["candidate"]
                    self._persist_meta()
                    self._reset_election_timer()
            return {"term": self.current_term, "vote_granted": granted}

    # ---- log replication ----------------------------------------------------

    def _replicate_loop(self, peer):
        event = self.wakeups[peer]
        while True:
            event.wait(HEARTBEAT_INTERVAL)
            event.clear()
            with self.lock:
                if self.state != LEADER:
                    continue
                term = self.current_term
                next_index = self.next_index[peer]
                prev_index = next_index - 1
                prev_term = self.log[prev_index - 1]["term"] if prev_index > 0 else 0
                entries = self.log[prev_index:prev_index + MAX_BATCH]
                commit = self.commit_index
            try:
                reply = requests.post(f"{peer}/raft/append_entries", json={
                    "term": term, "leader": self.my_address,
                    "prev_log_index": prev_index, "prev_log_term": prev_term,
                    "entries": entries, "leader_commit": commit,
                }, timeout=RPC_TIMEOUT).json()
            except Exception:
                continue
            with self.lock:
                if reply["term"] > self.current_term:
                    self._step_down(reply["term"])
                    continue
                if self.state != LEADER or self.current_term != term:
                    continue
                if reply["success"]:
                    self.match_index[peer] = max(self.match_index[peer], prev_index + len(entries))
                    self.next_index[peer] = self.match_index[peer] + 1
                    self._advance_commit()
                    if self.next_index[peer] <= len(self.log):
                        event.set()  # more to send, do not wait for the next heartbeat
                else:
                    # follower's log diverges or is short: back up and retry at once
                    self.next_index[peer] = max(1, min(reply.get("conflict_index", prev_index), prev_index))
                    event.set()

    def _advance_commit(self):
        matches = sorted(list(self.match_index.values()) + [len(self.log)], reverse=True)
        candidate = matches[self._majority() - 1]
        if candidate > self.commit_index and self.log[candidate - 1]["term"] == self.current_term:
            self.commit_index = candidate
            self._apply_committed()
            for event in self.wakeups.values():
                event.set()  # tell followers about the new commit index promptly

    def handle_append_entries(self, req):
        with self.lock:
            if req["term"] < self.current_term:
                return {"term": self.current_term, "success": False}
            self._step_down(req["term"])
            self.leader_address = req["leader"]
            self._reset_election_timer()

            prev_index, prev_term = req["prev_log_index"], req["prev_log_term"]
            if prev_index > len(self.log):
                return {"term": self.current_term, "success": False, "conflict_index": len(self.log) + 1}
            if prev_index > 0 and self.log[prev_index - 1]["term"] != prev_term:
                # skip back over the whole conflicting term in one step
                bad_term = self.log[prev_index - 1]["term"]
                conflict = prev_index
                while conflict > 1 and self.log[conflict - 2]["term"] == bad_term:
                    conflict -= 1
                return {"term": self.current_term, "success": False, "conflict_index": conflict}

            index = prev_index
            for entry in req["entries"]:
                index += 1
                if index <= len(self.log):
                    if self.log[index - 1]["term"] == entry["term"]:
                        continue
                    # conflicting uncommitted suffix: drop it and take the leader's entries
                    self.log = self.log[:index - 1]
                    wal_rewrite(self.log)
                self.log.append(entry)
                wal_append_entry(entry)

            if req["leader_commit"] > self.commit_index:
                self.commit_index = min(req["leader_commit"], prev_index + len(req["entries"]))
                self._apply_committed()
            return {"term": self.current_term, "success": True}

    # ---- status ---------------------------------------------------------------

    def status(self):
        with self.lock:
            return {
                # "role"/"primary" keep the pre-Raft /role shape that benchmark.py reads
                "role": "primary" if self.state == LEADER else "replica",
                "primary": self.leader_address,
                "my_address": self.my_address,
                "state": self.state,
                "term": self.current_term,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
            }


node = RaftNode()


@raft_bp.route("/raft/request_vote", methods=["POST"])
def request_vote():
    return jsonify(node.handle_request_vote(request.json))


@raft_bp.route("/raft/append_entries", methods=["POST"])
def append_entries():
    return jsonify(node.handle_append_entries(request.json))


@raft_bp.route("/role", methods=["GET"])
def role():
    return jsonify(node.status())


@raft_bp.route("/ping", methods=["GET"])
def ping():
    return jsonify({"ok": True})
