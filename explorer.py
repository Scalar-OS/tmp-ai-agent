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
        'bcookie': '"v=2&9eea89ba-722f-477a-84e3-3e6d8e678d84"',
        'bscookie': '"v=1&20220906081743417c0602-7506-4356-8852-0e6bdb335ca0AQEFn_05jcNzFmIoikMmW1zqyhQ2ILdQ"',
        'li_alerts': 'e30=',
        'li_theme': 'light',
        'li_theme_set': 'app',
        'li_sugr': '4ee71373-67cd-4197-9aa9-7232704f70f3',
        'timezone': 'Europe/Madrid',
        'dfpfpt': 'c85e0cc9d1304ddcba56e1a8319ea589',
        'VID': 'V_2024_02_05_11_567954',
        '_uetvid': 'fe6ec5c0abe411eeb9cf0339d942b7c2',
        'ajs_user_id': '530',
        'ajs_anonymous_id': '8c83f5f3-a011-438c-b84a-07a581256c87',
        'ajs_group_id': '293',
        's_fid': '2D10B75662F051D3-01E38D594F2F5E54',
        'visit': 'v=1&M',
        'li_gc': 'MTswOzE3MTI2NTUyNTU7MjswMjFX4QImcDVCFBcu2ZDjxzh4WZTBY+icw0BXiGzDcCDbYw==',
        'li_rm': 'AQFuetDQTmNCGgAAAZCS9fV4J8nm15ljp0Azuys435SYayIOWk_zCTqzFYL1GMQ4Tp0HBF_qcdaNrjE8Yvse9Go_BlM9xvcr93B4wEbUtBKFWnOwbToEJ27yQ1lYZHsgzQXp9UyJrsKvrEWYfb9oQk_SFkNlA9I0BECc7q4tKIm5Y8La8ODthzgoncSMk419TYkQQK4NZ_k1MY7Ro1qGabDN0P7faGhT70K8wLaY1B71cNzObyIot3ReNBkqTL-Go18DgJ34CMe-KAIznC_-26RzEFXwj7ZrQFWxdN9p6cpVJtfiqbcfAO8Srrocp8HRfX7ELYE69GPLbnrLB98',
        'g_state': '{"i_l":0}',
        'mbox': 'PC#168ae17f557a4962b31729b81729e625.37_0#1736854308|session#a442e5d6420b4a128652d58fa8d65fa8#1721304168',
        's_ips': '754',
        'aam_uuid': '65186641928165972290944689059145236147',
        'AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg': '1',
        'AMCV_14215E3D5995C57C0A495C55%40AdobeOrg': '-637568504%7CMCIDTS%7C19956%7CMCMID%7C65375001142020493180996169135698103672%7CMCAAMLH-1724768232%7C6%7CMCAAMB-1724768232%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1724170632s%7CNONE%7CMCCIDH%7C-1042117722%7CvVersion%7C5.1.1',
        'gpv_pn': 'www.linkedin.com%2Fpremium%2Fproducts%2F',
        's_plt': '1.12',
        's_pltp': 'www.linkedin.com%2Fpremium%2Fproducts%2F',
        's_tp': '1583',
        's_ppv': 'www.linkedin.com%2Fpremium%2Fproducts%2F%2C48%2C48%2C754%2C1%2C2',
        's_tslv': '1724274065655',
        's_cc': 'true',
        'sdsc': '22%3A1%2C1724413528529%7EJAPP%2C0ozfEACI6ltcDTAUNog798XoVEbo%3D',
        'fptctx2': 'taBcrIH61PuCVH7eNCyH0B9zcK90d%252bIeoo1r5v7Zc25%252bhrebb1qgOOP9pIXly9Xzd9WQbVsnEGOw8K54cJpm8F%252bX6ZLTIA6HrA3Yf7ZSLSM85nDRmvWPNm5CqNfKOF464OuX3LxGY82SwwRfFIKNcsSRczRBUeBmLKUIt08k9HTSRjpL%252fQcAJAW1JSzEFEVdDl2UjglBbTridVp3fABStBGFzlgfGHhEr2GRMd%252b%252bgMlZERRfp2j5I6cojNmQt17LD1aIS01PzQAJT9DlvR%252fn65C3RNLHzeR5o294nfbF3U29WZ26AsCRLOX5bRYVyM87ofE8Z%252bpUh0f7IIn7Y0ULez%252fA3LNahWnFlK%252f%252fz8LXarY%253d',
        'UserMatchHistory': 'AQKjHFwWvr13vAAAAZGdY2F_0UGCNZAdzh5gP7df7nTMT72K5uqMy6_VXbRUth3Qx9TeTLgUDVcxPQCFQPxUeHK7iHIfHhKdWDPcApm0prmljJX622Q0nxti3NEeh-j1EgLYxP4yHmt8svAb3jRojurBfcy1VQec-Ed3EosFBdJ9a-TlWkXn2syJ-CQovTbAbde4mlCIuw02Rtx-qWaUPEMTJ6H5FkauMJmYudd8qB1o0NhpDcoSXgYgh5WUZRwrzhqOLzMzeqPFEXlkkBFbwZ-gwRoxxlqaYGtC0UE',
        'AnalyticsSyncHistory': 'AQL42cyIQ-7pggAAAZGdY2F_ms-Vay8sUmxOsYUPv2XhtUdArumfTx1fFnwbqMAHDXVGDAsv-_Tu4TzIFpjRiQ',
        'lms_ads': 'AQGAuu8PgJYbuwAAAZGdY2JTqn1JffBLY0Hme33fHtIXxC79R3dgocju7CXXy7IpCZWiwhAspJod2Dg5wXGkfx6oLdV3V5pA',
        'lms_analytics': 'AQGAuu8PgJYbuwAAAZGdY2JTqn1JffBLY0Hme33fHtIXxC79R3dgocju7CXXy7IpCZWiwhAspJod2Dg5wXGkfx6oLdV3V5pA',
        'lang': 'v=2&lang=en-us',
        'li_at': 'AQEDAQyUAAUCdVDvAAABkZ-6sYcAAAGRw8c1h00AG-6BOhS51yupuWJeVghdQrnj9MbLP3USCQF37HKLkYxgw3rwIr7XSQO8cPGjg5D7-HQN0yJQk01GO3fpHVhg2WpFGuUlcWw8vue-r89XnexHDbXs',
        'liap': 'true',
        'JSESSIONID': '"ajax:4292814622984848896"',
        'lidc': '"b=VB25:s=V:r=V:a=V:p=V:g=4559:u=307:x=1:i=1724999861:t=1725073590:v=2:sig=AQHecpcKEO5stfIjSa6F5_Kgcw838HYW"',
        'li_mc': 'MTsyMTsxNzI1MDAzMzg3OzI7MDIx41t0mc0nr6H27ijqawuIAAaDRFjWEEz1rwzis5CNeG0=',
    }

    headers = {
        'accept': 'application/vnd.linkedin.normalized+json+2.1',
        'accept-language': 'en,es-ES;q=0.9,es;q=0.8',
        'cache-control': 'no-cache',
        'csrf-token': 'ajax:4292814622984848896',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.linkedin.com/mynetwork/grow/',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'x-li-lang': 'en_US',
        'x-li-page-instance': 'urn:li:page:d_flagship3_people;OLzhFJUvRhqm1IAO/sE3FQ==',
        'x-li-track': '{"clientVersion":"1.13.22405","mpVersion":"1.13.22405","osName":"web","timezoneOffset":2,"timezone":"Europe/Madrid","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":2,"displayWidth":2880,"displayHeight":1800}',
        'x-restli-protocol-version': '2.0.0',
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
        
