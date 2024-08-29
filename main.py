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
    # data = request.get_json()
    # if not data:
    #     return jsonify({"message": "No data received"}), 400
    
    try:
        account_object_id = ObjectId(account_id)
    except Exception as e:
        return jsonify({"message": "Invalid accountId format"}), 400

    # Query MongoDB for contacts with the given account_id
    contacts = contacts_collection.find({"accountId": account_object_id}).limit(300)
    processed_contacts = []

    for contact in contacts:
        contact_data = {
            'contact_name': contact.get('rawObject', {}).get('full_name'),
            'contact_email': contact.get('rawObject', {}).get('email'),
            'company_name': contact.get('rawObject', {}).get('company_name'),
            'contact_linkedin_url': contact.get('rawObject', {}).get('linkedin_url', None),
        }

        # Process each contact
        processed_contact = process_crm_contact(contact_data)
        processed_contacts.append(processed_contact)

    return jsonify({"processed_contacts": processed_contacts}), 200

if __name__ == '__main__':
    app.run(port=5000)