from flask import Flask, request, jsonify
from person_finder import process_crm_contact

app = Flask(__name__)

@app.route('/submit', methods=['POST'])
def submit():
    data = request.get_json()  # Get JSON data from the request
    if not data:
        return jsonify({"message": "No data received"}), 400
    
    # Example: Do something with the data and return a response
    # For simplicity, let's just echo the data back
    contact_data = {
      'contact_name': "Daniel Olea",
      'contact_email': "dani@scalaros.com",
      'company_name': "Scalar",
      'contact_linkedin_url': None,
    }

    result = process_crm_contact(contact_data)

    return jsonify({"exps": result}), 200

if __name__ == '__main__':
    app.run(port=5000)