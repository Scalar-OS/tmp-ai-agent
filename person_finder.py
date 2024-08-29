from linkedin_api import Linkedin
import difflib
import time
from dotenv import load_dotenv
import os
import json
from explorer import getExperiences
from openai import OpenAI
import re
import traceback

load_dotenv()

client = OpenAI(
    api_key=os.environ.get("OPENAI_LOCAL_API_KEY")
)
    
def find_best_match(hits, past_company):
    best_hit = {"ratio": 0, "hit": None}
    for hit in hits:
        for e in hit["exps"]:
            r = difflib.SequenceMatcher(None, past_company.lower().strip(), e["company_name"].lower().strip()).ratio()
            #print(past_company, e["company_name"], r)
            if best_hit["ratio"] < r:
                best_hit["ratio"] = r
                best_hit["hit"] = hit
    
    return best_hit

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

    json_output_block = message.split('>>>>')[-1]
    
    # Search for the pattern in the message
    match = re.search(pattern, json_output_block, re.DOTALL)
    if match:
        json_array_str = match.group(0)
        # Clean the string to parse as JSON
        json_array_str = json_array_str.replace('```json', '').replace('```', '').strip()
        # Parse the JSON string to a Python list
        ids_list = json.loads(json_array_str)
        return ids_list
    else:
        return []
    
def find_largest_non_null_image(data):
    max_value = None
    max_number = -1

    pattern = re.compile(r'^img_(\d+)_(\d+)$')

    for key, value in data.items():
        if value:  # Check if the value is non-null
            match = pattern.match(key)
            if match:
                numeric_part_1 = int(match.group(1))
                numeric_part_2 = int(match.group(2))
                number = max(numeric_part_1, numeric_part_2)
                
                if number > max_number:
                    max_number = number
                    max_value = value

    return max_value

def process_crm_contact(row):
    # If this starts to fail, go to ~/.linkedin_api/cookies and delete the file with your email
    # https://github.com/tomquirk/linkedin-api/issues/357
    api = Linkedin(
        os.getenv('LINKEDIN_USER'), 
        os.getenv('LINKEDIN_PWD')
    )

    ai_rationale = None
    try:
        email = row['contact_email']
        if row['contact_name'] is None:
            local_part = row['contact_email'].split('@')[0]
            name = local_part.replace('.', ' ')
        else:
            # avoid errors with LinkedIn API by removing all symbols
            # Unicode flag to accept tildes and non-ASCII valid letters
            # \s matches any whitespace character
            name = re.sub(r'[^\w\s]', '', row['contact_name'], re.UNICODE)
        target_company = re.sub(r'[^\w\s]', '', row['company_name'], re.UNICODE)

        search_time = time.time()
        linkedin_provided = False
        if row['contact_linkedin_url'] is not None and len(row['contact_linkedin_url'].split('https://www.linkedin.com/in/')) == 2:
            public_id = row['contact_linkedin_url'].split('https://www.linkedin.com/in/')[1].replace('/', '')
            profile = api.get_profile(public_id=public_id)
            profile['name'] = ' '.join([profile['firstName'], profile['lastName']])
            search = [profile]
            linkedin_provided = True
        else: 
            search = api.search_people(keywords=f'{name} {target_company}', limit=5)

            if len(search) == 0:
                # Sometimes a search with name + company fails, but just a name, although more noisy, returns the person
                search = api.search_people(keywords=f'{name}', limit=5)

        best_match = None
        if search:
            hits = []
            experiences_by_id = {}

            # ChatGPT allucinates on IDs, so we are trying to make them simple numbers to help
            id_to_index = {}            
            for idx, s in enumerate(search):
                id_to_index[s["urn_id"]] = idx
                exps = getExperiences(urn_id=f"urn:li:fsd_profile:{s['urn_id']}")
                experiences_by_id[idx] = exps
                hits.append({"id":id_to_index[s["urn_id"]], "name": s["name"] ,"job_experiences": list(map(lambda e: {"dates": e['dates'], "company_name": e["company_name"]}, exps))})

            index_to_id = {v: k for k, v in id_to_index.items()}
            
            system_prompt = """
            You are a world-class expert on sales prospecting working as a Senior Business Development Representative (BDR). Your goal is to find past buyers of your product on LinkedIn.

Task Details:
Objective: Identify past buyers on LinkedIn using their name (or variations of it), surname (or variations), and the company they worked for.

Input:

Past Buyer Name, Past Buyer Email and Company come from a CRM. The CRM is the golden source but might contain errors or not belong to a person. You should use the Past Buyer Name along with
Past Buyer Email to have a better idea of the actual name of the person. You should rely more on Past Buyer Name and use Past Buyer Email to clear out doubts or complete the name.

Name Matching Rules:
Allowed Variations:

Middle Names: Individuals with middle names are acceptable (e.g., "Daniel Martín" and "Daniel Javier Martín" are considered the same person). Consider the name origin (Arabic, Hispanic, etc.) to assess whether each word is a name or a surname, and remember that only names can be middle names.
Additional Surnames at the End: If the LinkedIn profile shows an additional surname at the end which was not originally provided (e.g., "Javier Perez" and "Javier Perez Llorca" are considered the same person).
Not Allowed Variations:

Middle Surnames: Differences in the middle position of surnames must be considered less likely to be the same person (e.g., "Daniel Martín" and "Daniel Romero Martín" are likely not the same person because "Romero" is an additional middle surname).
If Past Buyer Name doesn’t seem like a person’s name (e.g., "Oficina Madrid"), assign a likelihood of below 0.5.
Process:
Identify the Person:

Look for matching name variations according to the rules above, in conjunction with the listed company in their experiences.
The likelihood and the selection of the match must reflect a combination of name match and experience match. For example, an unclear name match coupled with a matching experience should increase the likelihood, but not necessarily to certainty levels.
However, if the fullname seems to belong to another person likelihood should drop significantly. For example, someone with a matching experience, but significantly different fullname must have a low likelihood

Assess Job Status:

Determine if they are still working at the past company or have changed jobs based on their employment timeline. If there are more than one parallel current experiences (that is, more than 1 experience with no ending date), you should only considered
the job_status as changed_jobs if the experience at the Company has ended. For example, if Company is Coca Cola, and someone has an experience at Coca Cola from 2013 to Present and a later experience as professor at an online course from 2022 to present,
since both are active, and Company is Coca Cola which is still a current experience, the job_status should be 'still_working'
Also consider the possibility that Company is part of a group, for example Letgo and OLX, in which case your analysis must consider them as the same if it makes sense
Requirements:
JSON Structure:

id: The ID of the closest match.
likelihood: Certainty of the match with LinkedIn as a decimal (0 to 1).
job_status: 'still_working', 'changed_jobs', or 'might_have_changed_jobs' based on the past company experience.
Accuracy Determinants:

High Likelihood: Ensure high accuracy through name variation matches, employment timelines, and company associations.
Job Status Determination: Consider if the experience is ongoing or ended based on explicit dates.
Output Example:
>>>>>>>>>>
```json
{
  "id": "123434", 
  "likelihood": 0.83, 
  "job_status": "changed_jobs"
}```
Answer Format:

If likelihood is 0, set id to '0' and job_status to null.
Example to Follow:

Review the JSON ID and experiences to identify and summarize:
If needed, reason through possible name variations and match according to the rules.
Confirm the past company's last known role and check for new experiences post the past company tenure.
Structured Response Format:
>>>>>>>>>>
```json
{
  "id": "ID_HERE", 
  "likelihood": DECIMAL_HERE, 
  "job_status": "STATUS_HERE"
}```

Clarify your reasoning briefly if required before providing the final JSON output.
Example Reasoning:
Case:

Provided Buyer Name: "hmesbah Mesbah"
LinkedIn Profile Name: "Houda M."
The name "hmesbah Mesbah" could contain errors from the CRM. Given the repetition of "Mesbah," it is possible that the actual name is "Houda Mesbah" with first initial "H." or a similar pattern. The profile name "Houda M." might correspond to "Houda Mesbah" where "M" stands for Mesbah.

Experience:
Provided Company: "Boslan Ingenieria Y Consultoria"
LinkedIn Profile Experience: -"June 2023 - May 2024 · 1 yr" at "Boslan Ingenieria Y Consultoria"
The LinkedIn profile shows past experience at the matching company.

The output in JSON must always start with "```json" and end with "```". It must be the last part of the answer and be sepparated from the rest of the answer by at least 5 ">", like this: >>>>>>>>>>

Job Status Determination:

The career timeline indicates they are currently working at "Eva Seguros" as of May 2024.
Conclusion: While the name match isn’t certain, the matched experience justifies a moderate likelihood.

Final JSON Output:
>>>>>>>>>>
```json
{
  "id": "123456", 
  "likelihood": 0.6, 
  "job_status": "changed_jobs"
}```
Case:

Provided Buyer Name: "Oficina Boslan" (which does not seem like a person’s name)
Provided Company: "Boslan Ingenieria Y Consultoria"
This is not a person's name, so the likelihood must be below 0.2.

Final JSON Output:
>>>>>>>>>>
```json
{
  "id": "0", 
  "likelihood": 0.1, 
  "job_status": null
}````

            """

            messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Past Buyer Name:{name}; Past Buyer Email:{email}; Company: {target_company}\n\n---- JSON ----\n\n{json.dumps(hits)}"}
             ]
            #print(messages)
            ai_time = time.time()
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=messages
            )

            ai_rationale=response.choices[0].message.content
            best_match = parse_json_from_chatgpt(response.choices[0].message.content)

        new_companies = ''
        new_roles = ''
        linkedin_url = ''
        job_status = ''
        contact_status = 'not-matched'
        likelihood = 0
        experiences = []
        linkedin_photo_url = None
        

        if best_match is not None and best_match["likelihood"] >= .2:
            likelihood = best_match["likelihood"]
            experiences = experiences_by_id[int(best_match["id"])]
        
            profile = api.get_profile(urn_id=index_to_id[int(best_match["id"])])
            # #print(f"LinkedIn profile: {round(time.time() - profile_time, 2)}s")
            # #print(json.dumps(profile))
            linkedin_url = f"https://www.linkedin.com/in/{profile.get('public_id')}/"
            linkedin_photo_url = f"{profile.get('displayPictureUrl')}{find_largest_non_null_image(profile)}"
            
            job_status = best_match['job_status']
            if likelihood >= .5:
                contact_status='matched'
            elif likelihood >= .2:
                contact_status='unlikely-match'

            if job_status != "still_working":
                exp =  experiences_by_id[int(best_match["id"])]
                if(exp[0]['dates'] == None):
                    job_status = 'still_working'
                else:
                    present_exps = list(filter(lambda e: e['dates'].find('Present') != -1, exp))
                    if len(present_exps) == 0:
                        job_status = "unemployed"
                    else:
                        new_companies = ' or '.join(list(map(lambda e: e['company_name'], present_exps)))
                        new_roles = ' or '.join(list(map(lambda e: e['role'], present_exps)))
            
        result = {
            "past_company": target_company,
            "contact_status": contact_status,
            "contact_name": name,
            "contact_linkedin_url": linkedin_url,
            "linkedin_photo_url": linkedin_photo_url,
            "contact_job_status": job_status,
            "contact_match_likelihood": likelihood,
            "new_companies": new_companies,
            "new_roles": new_roles,
            "experiences": experiences,
            "ai_rationale_matching": ai_rationale,
            "linkedin_provided": linkedin_provided,
        }

        total_time = round(time.time() - search_time, 2)
        print(f"Tracking of {name}: {total_time}s")

        return result
    except Exception as e:
        print(f'EXCEPTION {e}')
        traceback.print_exc()
        return {
            "past_company": target_company,
            "contact_status": 'error',
            "contact_name": row['contact_name'],
            "contact_linkedin_url": '',
            "linkedin_photo_url": '',
            "contact_job_status": '',
            "contact_match_likelihood": 0,
            "new_companies": '',
            "new_roles": '',
            "experiences": [],
            "ai_rationale": ai_rationale,
        }