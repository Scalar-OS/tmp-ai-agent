import pandas as pd
from pymongo import MongoClient
import re
from dotenv import load_dotenv
import os
from bson import regex
from linkedin_api import Linkedin
from explorer import getExperiences
import json
from openai import OpenAI
import traceback
import time
from datetime import datetime
from Levenshtein import distance as levenshtein_distance
import getpass
from bson import ObjectId

load_dotenv()

client = OpenAI(
    api_key=os.environ.get("OPENAI_LOCAL_API_KEY")
)

# Step 1: Read the CSV File
def read_csv(file_path):
    df = pd.read_csv(file_path)
    return df

# Step 2: Filter Records where "contact_job_status" equals "changed_jobs"
def filter_records(df):
    return df[df['contact_job_status'] == 'changed_jobs']

# Step 3: MongoDB Connection
def connect_to_mongodb(uri, db_name):
    client = MongoClient(uri)
    db = client[db_name]
    return db

def clean_company_name(name):
    """
    Cleans a company name by removing job type noise.
    Examples:
    'Damm · Full-time' -> 'damm'
    'Grifols · Full-time' -> 'grifols'
    'Europastry · Full-time or K-Visual Magazine · Freelance' -> ['europastry', 'k-visual magazine']
    """
    # Convert to lowercase
    name = name.lower()
    # Split by ' or ' to handle multiple company names
    names = name.split(' or ')
    # Remove noise after ' · ' and trim whitespace
    cleaned_names = [re.split(r'\s·\s', n)[0].strip() for n in names]

    #TODO: For now pick the first
    return cleaned_names[0]

# Step 4: Search with Enhanced Aggregation Query
def search_companies(db, company_name):
    # python_regex = re.compile(r".*" + re.escape(company_name) + r".*", re.IGNORECASE)
    search_results = db['salesforce-accounts-records'].find({ "$text": { "$search": company_name } }).limit(10)
    close_accounts = list(search_results)

    closest_account = None
    most_recent_oppty = None
    if(len(close_accounts) > 0):
        closest_account = closest_match(company_name, close_accounts)

        pipeline = [
            { '$match': { 'AccountId': closest_account['Id'] }},
            { '$project': { 
                'opportunity_stage': '$StageName', 
                'opportunity_stage_date': '$CloseDate',
                'opportunity_id': "$Id",
                'opportunity_name': "$Name",
                'opportunity_type': "$Type",
                'opportunity_created_date': "$CreatedDate",
                'opportunity_closed_date': "$CloseDate",
            }},
            { '$group': {  # Grouping by opportunity Id (which seems appropriate for the context)
                '_id': '$opportunity_id', 
                'opportunity_stage': { '$first': '$opportunity_stage' },
                'opportunity_stage_date': { '$first': '$opportunity_stage_date' },
                'opportunity_id': { '$first': '$opportunity_id' },
                'opportunity_name': { '$first': '$opportunity_name' },
                'opportunity_type': { '$first': '$opportunity_type' },
                'opportunity_created_date': { '$first': '$opportunity_created_date' },
                'opportunity_closed_date': { '$first': '$opportunity_closed_date' }
            }},
            { '$project': {  # Reformatting the output structure if needed
                '_id': 0,  # Set to 0 to exclude '_id' from the final output
                'opportunity_id': '$_id', 
                'opportunity_stage': 1, 
                'opportunity_stage_date': 1,
                'opportunity_id': 1,
                'opportunity_name': 1,
                'opportunity_type': 1,
                'opportunity_created_date': 1,
                'opportunity_closed_date': 1,
            }}
        ]
        results = db['salesforce-opportunities'].aggregate(pipeline)
        list_results = list(results)
        most_recent_oppty = get_most_recent_opportunity(list_results)

    return {"account": closest_account, "last_oppty": most_recent_oppty }

# Calculate Levenshtein distances
def closest_match(query, candidates):
    return min(candidates, key=lambda x: levenshtein_distance(query.lower(), x["Name"].lower()))


# Step 5: Retrieve and Process Most Relevant and Recent Opportunities
def process_opportunities(results):
    opportunities = {}
    for result in results:
        account_name = result['Name']
        oppty_date = pd.to_datetime(result['Date'])
        if account_name not in opportunities or opportunities[account_name]['Date'] < oppty_date:
            opportunities[account_name] = {
                'Id': result['Id'],
                'Name': result['Name'],
                'Stage': result['Stage'],
                'Date': oppty_date
            }
    return list(opportunities.values())

def parse_json_from_chatgpt(message: str):
    """
    Extracts the IDs from the given message containing JSON array at the end.
    
    Args:
    - message (str): The input message containing IDs.
    
    Returns:
    - List[str]: A list of extracted IDs.
    """
    # Define a pattern to extract the JSON array at the end of the message
    pattern = r'```json\s*.*\s*```'
    
    # Search for the pattern in the message
    match = re.search(pattern, message, re.DOTALL)
    if match:
        json_array_str = match.group(0)
        # Clean the string to parse as JSON
        json_array_str = json_array_str.replace('```json', '').replace('```', '').strip()
        # Parse the JSON string to a Python list
        ids_list = json.loads(json_array_str)
        return ids_list
    else:
        return []
    
def get_most_recent_opportunity(opportunities):
    if not opportunities:
        return {
            "opportunity_stage": None,
            "opportunity_id": None,
            "opportunity_created_date": None,
            "opportunity_stage_date": None,
        }

    # Sort opportunities by opportunity_stage_date
    sorted_opportunities = sorted(
        opportunities,
        key=lambda x: datetime.strptime(x['opportunity_stage_date'], '%Y-%m-%d')
    )

    # Return the most recent opportunity
    return sorted_opportunities[-1]

# Main program
def main():
    accountId = input("Account id: ")

    mongo_uri = os.environ.get("MONGO_CONNECTION_STRING")
    db_name = 'scalar-sales'

    # Connect to MongoDB
    db = connect_to_mongodb(mongo_uri, db_name)

    try:
        company_id = ObjectId(company_id)
    except Exception as e:
        print("Invalid company ID format.")
        return

    leads_collection = db['leads']
    rows = leads_collection.find({"contact_job_status": "changed_jobs", "contact_match_likelihood": {'$gte': 0.75}, "company_id": company_id }).limit(100)

    context = ''

    # Iterate through filtered records and search MongoDB

    for index, row in enumerate(rows):
        try:
            start_time = time.time()
            new_companies = row.get("new_companies", "")
            linkedin_url = row.get("contact_linkedin_url", "")
            if not isinstance(new_companies, str):
                #not sure
                continue

            experiences = row['experiences']

            final_opportunities = []
            company_name = clean_company_name(new_companies)
            
            result = search_companies(db, company_name.strip())
            new_oppty = result["last_oppty"]

            if(result['account'] is not None and 'customer' in result['account']['Account_stage__c'].lower()):
                scoring = {'score': 0, 'rationale': [{"direction": "very bad", "text": "Already a client"}]}
                ai_rationale = 'The new company is already a client'
            else:
                context = json.dumps({"experiences": experiences, "crm_opportunities": result["last_oppty"], "target_company": company_name})

                system_prompt = """
                    You are a world-class sales prospector. Your task is to assign a score from 0 to 100 to different leads, where 0 is the worst and 100 is the best, in order to prioritize them effectively. Here are the rules for scoring:

        Criteria for Scoring:
        You need to consider all the following as a whole and reason about the relevance towards scoring a lead. You will find that you have good things and bad things. So to decide the final score, first analize all the rules that apply and then reflect on how relevant are ones compared to other to come up with a final score between 0 and 100. By default you should consider all leads as starting with a score of 80.

        Rules
        Status in CRM:
        Is considered bad if a "Closed Lost" opportunit happened in the target company less than 9 months ago
        Is considered very good in no opportunities are present
        Must be 0 if an opportunity for target_company is in the stage "Closed won"

        Employee's Role:
        Is considered bad If the employee was present at target_company during a closed/lost opportunity (start of opportunity is before the start of the employee at that company
        It is considered be very bad if the opportunity was closed lost and the opportunity started 3 months or more after the person joined target_company

        Regarding the Employee:
        It is considered good if the new role at target_company is related to HR, Compensation and benefits or similar
        It is considered good if the new role at target_company is a director or senior manager or similar roles with greater responsibility
        It is considered good if the new role at target_company seems as a decision maker in a purchase decision of a employee benefit SaaS - add points

        Time Since Job Change:
        It is considered neutral if their newest job started more than a year ago
        It is considered very good if their newest job started between 3 and 6 months
        It is considered good if their job description includes "employee satisfaction," "benefits," or related terms

        Additional Rules:
        If the target company field is empty (''): Assign a score of 50
        If the opportunity is already close-won: assign a final score of 0, and the rational must be simply "new company is already close-won", direction: "bad"

        Process:
        1/ Analize and assess all the information available using the provided rules
        In this stage you must reason about how the input information relates to the rules. Consider ALL rules before progressing to the next step
        2/ Evaluate the result of the past step and reason about the relevance of each rule
        Once you have reasoned about which rules are relevant for the input, then reason about how relevant are those rules when scoring a lead
        3/ Come up with a score
        4/ Create a JSON following the requirement below

        Input Format:
        The input will be a JSON dictionary with three fields: experiences, crm_opportunities, and target_company.
        experiences contains the lead’s roles, positions, time at those positions, and role descriptions (some may be optional).
        crm_opportunities represent all the opportunities associated with the target company.
        Output Format:
        Return a JSON with a score (from 0 to 100) and a rationale array. Each item in the rationale array should be an object with:
        direction: "good", "neutral", or "bad"
        text: Brief explanation for that specific rationale
        Example JSON Input:
        {
        "experiences": [ ... ],
        "crm_opportunities": [ ... ],
        "target_company": "Example Corp"
        }
        Example JSON Output:
        {
        "score": 75,
        "rationale": [
            {
            "text": "Current role is decision maker in HR",
            "direction": "good"
            },
            {
            "text": "Target company has not been contacted before",
            "direction": "good"
            }
        ]

        }
        Instructions for GPT-4o:
        Use the above criteria to analyze the provided JSON input and return the score and rationale as specified.
                    """

                messages = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": context}
                    ]

                response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=messages
                )

                ai_rationale=response.choices[0].message.content
                scoring = parse_json_from_chatgpt(response.choices[0].message.content)

            leads_collection.update_one(
                {"_id": row['_id']}, 
                {
                    "$set": {
                        "score": scoring['score'], 
                        "summary_rationale": '\n'.join(list(map(lambda r: f"{'(+)' if r['direction'] == 'good' else '(-)' if r['direction'] == 'bad' else '(++)' if r['direction'] == 'very good' else '(--)' if  r['direction'] == 'very bad' else '(~)'} {r['text']}", scoring['rationale']))),
                        "full_rationale": ai_rationale,
                        'new_company_oppty_stage': new_oppty["opportunity_stage"] if new_oppty is not None else None,
                        'new_company_oppty_id': new_oppty["opportunity_id"] if new_oppty is not None else None,
                        "new_company_oppty_created_date": new_oppty["opportunity_created_date"] if new_oppty is not None else None,
                        'new_company_oppty_stage_date': new_oppty["opportunity_stage_date"] if new_oppty is not None else None,
                        "new_companies": result['account']['Name'] if result['account'] is not None else company_name,
                        "new_company_account_id": result['account']['Id'] if result['account'] is not None else None,
                        "new_company_stage": result['account']['Account_stage__c'] if result['account'] is not None else None,
                        "new_company_employees": result['account']['Rango_de_empleados__c'] if result['account'] is not None else None,
                        "new_company_revenue": result['account']['Revenue_range__c'] if result['account'] is not None else None,
                        "new_company_commercial_field": result['account']['Commercial_field__c'] if result['account'] is not None else None,
                        "new_company_industry": result['account']['Industry_Goodfit__c'] if result['account'] is not None else None,
                        "new_company_subindustry": result['account']['Subindustry__c'] if result['account'] is not None else None,
                        "new_company_owner_id": result['account']['OwnerId'] if result['account'] is not None else None,
                        "new_company_sdr_id": result['account']['SDR__c'] if result['account'] is not None else None,
                    }
                }
            )

            print(f"Total time [{index}]: {round(time.time() - start_time, 2)}")
        except Exception as e:
            print(e)
            traceback.print_exc()

    

if __name__ == "__main__":
    main()