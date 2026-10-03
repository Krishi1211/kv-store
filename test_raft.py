"""End-to-end Raft test: starts a 3-node cluster on ports 8100-8102 and checks
election, replication, failover, restart catch-up, quorum loss and recovery.

    python test_raft.py
"""
import subprocess, time, requests, sys, os, tempfile
P=os.path.dirname(os.path.abspath(__file__)); os.chdir(tempfile.mkdtemp(prefix="kv-raft-test-"))
PEERS="http://localhost:8100,http://localhost:8101,http://localhost:8102"
ports=[8100,8101,8102]; procs={}
def start(p): procs[p]=subprocess.Popen([sys.executable,P+"/app.py","--port",str(p),"--peers",PEERS],stdout=open(f"node_{p}.log","a"),stderr=subprocess.STDOUT)
def stop(p): procs[p].kill(); procs[p].wait(); del procs[p]
def role(p):
    try: return requests.get(f"http://localhost:{p}/role",timeout=1).json()
    except Exception: return None
def leader(timeout=10):
    end=time.time()+timeout
    while time.time()<end:
        ls=[p for p in procs if (role(p) or {}).get("state")=="leader"]
        if len(ls)==1 and all((role(p) or {}).get("primary")==f"http://localhost:{ls[0]}" for p in procs): return ls[0]
        time.sleep(0.2)
    raise SystemExit("no single leader: "+str({p:role(p) for p in procs}))
def put(p,k,v,**kw): return requests.post(f"http://localhost:{p}/set",json={"key":k,"value":v,**kw},timeout=8)
def get(p,k): 
    r=requests.get(f"http://localhost:{p}/get",params={"key":k},timeout=2); return r.json().get("value")
def wait(cond,what,timeout=8):
    end=time.time()+timeout
    while time.time()<end:
        if cond(): return
        time.sleep(0.1)
    raise SystemExit("FAILED: "+what)
for p in ports: start(p)
L=leader(); print("1 leader elected:",L,"term",role(L)["term"])
assert put(L,"a","1").status_code==200
F=[p for p in ports if p!=L]
wait(lambda: all(get(p,"a")=="1" for p in ports),"replicate a"); print("2 write visible on all nodes")
r=put(F[0],"x","y"); assert r.status_code==403 and r.json()["leader"].endswith(str(L)); print("3 follower rejects write, names leader")
t=role(L)["term"]; stop(L); L2=leader(); assert L2!=L and role(L2)["term"]>t; print("4 leader killed -> new leader",L2,"term",role(L2)["term"])
for i in range(20): assert put(L2,f"k{i}",str(i)).status_code==200
assert requests.delete(f"http://localhost:{L2}/delete",params={"key":"a"}).status_code==200
start(L); wait(lambda: role(L) and role(L)["last_applied"]==role(L2)["last_applied"],"old leader catch-up")
assert get(L,"k19")=="19" and get(L,"a") is None and role(L)["state"]=="follower"; print("5 restarted node caught up on 21 missed writes, log",role(L)["log_length"])
others=[p for p in ports if p!=L2]
for p in others: stop(p)
r=put(L2,"lonely","1"); assert r.status_code==503, r.text; print("6 no quorum -> write refused:",r.status_code)
for p in others: start(p)
L3=leader(); wait(lambda: len({role(p)["last_applied"] for p in ports})==1 and len({role(p)["log_length"] for p in ports})==1,"converge after minority write")
print("7 cluster converged; uncommitted write 'lonely' =",{p:get(p,'lonely') for p in ports})
assert len({get(p,'lonely') for p in ports})==1
for p in list(procs): stop(p)
for p in ports: start(p)
L4=leader(); wait(lambda: all(get(p,"k7")=="7" for p in ports),"recover after full restart"); print("8 full cluster restart recovered data from logs; leader",L4,"term",role(L4)["term"])
assert put(L4,"ttlkey","v",ttl=1).status_code==200; time.sleep(1.5); assert all(get(p,"ttlkey") is None for p in ports); print("9 ttl expiry ok")
out=subprocess.run([sys.executable,P+"/benchmark.py","--url",f"http://localhost:{F[0]}","--count","300"],capture_output=True,text=True).stdout
print("\n".join(l for l in out.splitlines() if "p50" in l or "Throughput" in l or "Results" in l))
for p in list(procs): stop(p)
print("ALL RAFT CHECKS PASSED")
