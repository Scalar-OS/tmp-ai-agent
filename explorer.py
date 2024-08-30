import urllib.parse
from linkedin_api import Linkedin
import time
from dotenv import load_dotenv
import os
import requests
from json_extractor import extract_info, detail_to_dict
import urllib
import csv
import re

load_dotenv()

def getRawExperiences(urn_id):

    # Grab any graphql request from linkedin > copy as cURL > https://curlconverter.com/python/
    # Do this above on the previous method too
    cookies = {
        # REPLACE
    }

    headers = {
        # REPLACE
    }

    response = requests.get(
        f'https://www.linkedin.com/voyager/api/graphql?includeWebMetadata=true&variables=(profileUrn:{urllib.parse.quote_plus(urn_id)},sectionType:experience,locale:en_US)&queryId=voyagerIdentityDashProfileComponents.c5223e9bb9d73b470206c0203725a036',
        cookies=cookies,
        headers=headers,
    )

    #print(json.dumps(response.json()))

    return response.json()

def merge_dicts(dict_item, replacement):
    new_dict = dict_item.copy()  # Start with a copy of dict_item
    for key, value in replacement.items():
        if value is not None or key not in new_dict:
            new_dict[key] = value
    return new_dict

def replace_and_flatten(list_of_dicts, replacement_dict):
    result = []

    for dict_item in list_of_dicts:
        if 'linking_key' in dict_item:
            linking_key_value = dict_item['linking_key']
            if linking_key_value in replacement_dict:
                replacement_objects = replacement_dict[linking_key_value]
                for replacement in replacement_objects:
                    # Merge the two dictionaries
                    new_dict = merge_dicts(dict_item, replacement)
                    # Remove linking_key from the new dictionary
                    new_dict.pop('linking_key', None)
                    result.append(new_dict)
            else:
                dict_item.pop('linking_key', None)
                result.append(dict_item)
        else:
            result.append(dict_item)

    return result

def has_field_pagedListComponent(d):
    """
    Recursively check if a dictionary (or nested dictionaries) contains the field '*pagedListComponent'.
    """
    if isinstance(d, dict):
        for key, value in d.items():
            if key == '*pagedListComponent':
                return True
            if has_field_pagedListComponent(value):
                return True
    elif isinstance(d, list):
        for item in d:
            if has_field_pagedListComponent(item):
                return True
    return False

def filter_dicts_with_field(dicts):
    """
    Filter a list of dictionaries, returning only those that have a nested field '*pagedListComponent'.
    """
    return [d for d in dicts if has_field_pagedListComponent(d)]

def filter_dicts_without_field(dicts, filtered_dicts):
    """
    Filter a list of dictionaries, excluding the ones that have a nested field '*pagedListComponent'.
    """
    return [d for d in dicts if d not in filtered_dicts]

def exp_to_json(list):

    linking_key, dates, role, location, company_name, company_linkedin_url, description, skills, total_time = [None] * 9

    # Relaxed regex pattern to capture LinkedIn period dates
    pattern = re.compile(r"([\w\s]*\d{4})\s*-\s*([\w\s]*\d{4}|Present)\s*·\s*(\d+\s*yrs?\.?\s*\d*\s*mos?\.?|\d+\s*yrs?\.?|\d+\s*mos?\.?)")
    pattern2 = re.compile(r"Skills:.+")

    #TODO: gestionar career breaks, ignore if company name == career break (lang?)
    #TODO: modality (full time) + total time (separar)

    if any("urn:li:" in s for s in list):
        linking_key = [s for s in list if "urn:li:" in s][0]
        location = list[0] if "urn:li:" not in list[0] else None
        company_name = list[-3]
        company_linkedin_url = list[-2]
        total_time = list[-1]
    else:
        dates_indexes = [i for i, line in enumerate(list) if pattern.match(line)]
        dates_index = dates_indexes[0] if dates_indexes else None
        dates = list[dates_index] if dates_index is not None else None

        role_index = len(list) - 3
        role = list[role_index]

        location = list[0] if dates_indexes and dates_indexes[0] == 1 else None
        company_name = list[-1]
        company_linkedin_url = list[-2]

        skills_indexes = [i for i, line in enumerate(list) if pattern2.match(line)]
        skills_index = skills_indexes[0] if skills_indexes else None
        skills = list[skills_index] if skills_index is not None else None
        
        if dates_index is not None:
            if dates_index + 1 == role_index or dates_index + 1 == skills_index:
                description = None
            else:
                description = list[dates_index + 1]

        total_time = None

    return {
        'linking_key': linking_key,
        'dates': dates,
        'role': role,
        'location': location,
        'company_name': company_name,
        'company_linkedin_url': company_linkedin_url,
        'description': description,
        'skills': skills,
        'total_time': total_time
    }

def getExperiences(urn_id):

    response_raw = getRawExperiences(urn_id)
    #response_raw = getExperiences(conn['entityUrn'])

    # Relevant information only apperas in field "included"
    experiences_raw = list(filter(lambda obj: 'components' in obj, response_raw['included']))
    experiences = filter_dicts_with_field(experiences_raw)
    experiences_detail = filter_dicts_without_field(experiences_raw, experiences)
    if len(experiences) == 0:
        experiences = experiences_detail.copy()
        experiences_detail = []

    experiences_detail_text = extract_info(experiences_detail, linking_key="entityUrn")
    experiences_detail_dict = detail_to_dict(experiences_detail_text, keyword='urn:li:fsd_profilePagedListComponent')

    experiences_text = extract_info(experiences, linking_key="*pagedListComponent")
    experiences_json = [exp_to_json(exp) for exp in experiences_text]
    complete_experiences = replace_and_flatten(experiences_json, experiences_detail_dict)
    return complete_experiences
        
