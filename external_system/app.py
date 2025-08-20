from flask import Flask, request, jsonify
import json

app = Flask(__name__)

@app.route("/receive_data", methods=["POST"])
def receive_data():
    data = request.json
    if data:
        print(f"[External System] Received data: {json.dumps(data, indent=2)}")
        return jsonify({"status": "success", "message": "Data received"}), 200
    else:
        return jsonify({"status": "error", "message": "No data received"}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)


