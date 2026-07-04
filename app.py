from flask import Flask, request, jsonify

app = Flask(__name__)
store = {}

@app.route("/get", methods=["GET"])
def get():
    key = request.args.get("key")
    return jsonify({"value": store.get(key)})

@app.route("/set", methods=["POST"])
def set():
    data = request.json
    store[data["key"]] = data["value"]
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(port=5000)