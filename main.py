import os
import argparse
import getpass
from person_finder import process_crm_contact
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import ObjectId

# Load environment variables from .env file
load_dotenv()

# Initialize MongoDB client
mongo_client = MongoClient(os.environ.get('MONGO_CONNECTION_STRING'))
db = mongo_client.get_database('scalar-sales')  # Replace with your database name
contacts_collection = db.get_collection('contacts')


# Function to process the given company ID
def process_company_contacts(company_id, user, password):
    try:
        try:
            account_object_id = ObjectId(company_id)
        except Exception as e:
            print("Invalid company ID format.")
            return

        # Query MongoDB for contacts with the given account_id
        contacts = contacts_collection.find({"accountId": account_object_id}).limit(300)
        processed_contacts = []

        for index, contact in enumerate(contacts):
            print(f"Starting processing of contact #{index}")
            contact_data = {
                'contact_name': contact.get('rawObject', {}).get('full_name'),
                'contact_email': contact.get('rawObject', {}).get('email'),
                'company_name': contact.get('rawObject', {}).get('company_name'),
                'contact_linkedin_url': contact.get('rawObject', {}).get('linkedin_url', None),
            }

            print(f"Processing contact: {contact_data}")

            # Process each contact
            processed_contact = process_crm_contact(contact_data, user, password)
            processed_contacts.append(processed_contact)

        print("Processed Contacts:")
        for contact in processed_contacts:
            print(contact)
    except Exception as e:
        print(f"An error occurred: {e}")


def main():
    accountId = input("Account id: ")

    user = input("Enter user: ")
    password = getpass.getpass("Enter password: ")

    # Note: Use the user and password as needed in your logic
    print(f"user: {user}, password: ***")

    process_company_contacts(accountId, user, password)


if __name__ == '__main__':
    main()