from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/submit', methods=['POST'])
def submit():
    data = request.get_json()  # Get JSON data from the request
    if not data:
        return jsonify({"message": "No data received"}), 400
    
    # Example: Do something with the data and return a response
    # For simplicity, let's just echo the data back
    return jsonify({"received_data": data}), 200

if __name__ == '__main__':
    app.run(port=5000)