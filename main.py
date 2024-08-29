from flask import Flask, request, jsonify
from person_finder import process_crm_contact
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from bson import ObjectId

app = Flask(__name__)

# Load environment variables from .env file
load_dotenv()

# Initialize MongoDB client
mongo_client = MongoClient(os.environ.get('MONGO_CONNECTION_STRING'))
db = mongo_client.get_database('scalar-sales')  # Replace with your database name
contacts_collection = db.get_collection('contacts')

@app.route('/accounts/<account_id>/submit', methods=['POST'])
def submit(account_id):
    data = request.get_json()
    if not data:
        return jsonify({"message": "No data received"}), 400
    
    # Convert account_id to ObjectId
    try:
        account_object_id = ObjectId(account_id)
    except Exception as e:
        return jsonify({"message": "Invalid accountId format"}), 400

    # Query MongoDB for contacts with the given account_id
    contacts = contacts_collection.find({"accountId": account_object_id})
    processed_contacts = []

    for contact in contacts:
        contact_data = {
            'contact_name': contact.get('name'),
            'contact_email': contact.get('email'),
            'company_name': contact.get('company'),
            'contact_linkedin_url': contact.get('linkedin_url'),
        }

        # Process each contact
        processed_contact = process_crm_contact(contact_data)
        processed_contacts.append(processed_contact)

    return jsonify({"processed_contacts": processed_contacts}), 200

if __name__ == '__main__':
    app.run(port=5000)