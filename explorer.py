import urllib.parse
from linkedin_api import Linkedin
import difflib
from datetime import datetime
from enum import Enum
import time
import warnings
from dotenv import load_dotenv
import os
import streamlit as st
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import requests
from json_extractor import extract_info, detail_to_dict
import urllib
import csv
import re

load_dotenv()

def getConnectionProfile(skip=0, pageResults=40):

    # Grab any graphql request from linkedin > copy as cURL > https://curlconverter.com/python/
    # Do this below on the next method too
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
        'gpv_pn': 'www.linkedin.com%2Flearning%2Fcertificates%2F3505135148ab0144039103eef02af863d5a989a1dac029dfa2c8d04817a6eb5e',
        's_ips': '754',
        's_tp': '1137',
        's_tslv': '1723544506842',
        'UserMatchHistory': 'AQKxJp9p6ugWGAAAAZFlwaSnnGIzWs91xLajPUfPs2IdhVWOC6WV2fVFLiDUIJzC4QYOUUAfpotY3BQTAXIeGy__tdkIFkIAPAGUVWyGBdw7ML2uuFXJF2SssJNkSp8kp3PHsYNF6uwIIF9rCntmYGQ7aJhsuFxdhCP9mSpilHIB7J-rdkqcVUi1f54WOiER5Bp32TjHuhqu8usQPzgnk8rt3gsea3CorWwAnoF036cMSVZpxj8yoyWHdi8Zoqwdh9zY_FwHjm4jge2vmCjvy41LjikEEw_oukugEWE',
        'AnalyticsSyncHistory': 'AQJgclM3JqfezwAAAZFlwaSnT3r-eTUgCXokYk9LxCln8Kd2TMsr7wt7rza4Ak9H59Buq9lkhiP1_dy6uz8CTg',
        'lms_ads': 'AQFHEOIxMRyDZgAAAZFlwaWx7Sxd8xM1bJGxp-Tc4v7lgQA_g9mLxTn-s_53xBZaoeqboYuGi7qFZL1ElMSHuS8bHW3-ptu5',
        'lms_analytics': 'AQFHEOIxMRyDZgAAAZFlwaWx7Sxd8xM1bJGxp-Tc4v7lgQA_g9mLxTn-s_53xBZaoeqboYuGi7qFZL1ElMSHuS8bHW3-ptu5',
        'fptctx2': 'taBcrIH61PuCVH7eNCyH0B9zcK90d%252bIeoo1r5v7Zc25%252bhrebb1qgOOP9pIXly9Xzd9WQbVsnEGOw8K54cJpm8F%252bX6ZLTIA6HrA3Yf7ZSLSM85nDRmvWPNm5CqNfKOF464OuX3LxGY82SwwRfFIKNcu%252fd8cG6O2bIZ%252fIdzKx%252bY8EHkzi5%252f008L5EwLGqGb2lKhMa1vEtHQU0RsuzJb2w1qtKg2jYTgAD4mSH3FIRlP2OgzB6PmsJWKRnnr1vdxZ140OJRg%252bdTpUqx0Nb2XHCVOF0jT%252bN1Yp1ghjapbv8d5WkU0aWrAj%252bLSODGoArhxL6Wq5yQH%252f%252fFkBDVFV8AFNNIwBxf4PD3W5U1xjKUI6nbsAc%253d',
        'lang': 'v=2&lang=en-us',
        'JSESSIONID': '"ajax:1260589603359156689"',
        'liap': 'true',
        'li_at': 'AQEDAQyUAAUFTEj4AAABkXAmGdgAAAGRlDKd2FYARkWw2-eEDacDbpF1fDxm8iwpTVks1pZKV_gVnQVtydQv_i7zKrr1dfhUjWqSRjEEZ_McGefUAXT4I8v9y3kzB7g7kCLBSQ1b-q9CRrzm1zOW7eRt',
        'aam_uuid': '65186641928165972290944689059145236147',
        'AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg': '1',
        'AMCV_14215E3D5995C57C0A495C55%40AdobeOrg': '-637568504%7CMCIDTS%7C19956%7CMCMID%7C65375001142020493180996169135698103672%7CMCAAMLH-1724768232%7C6%7CMCAAMB-1724768232%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1724170632s%7CNONE%7CMCCIDH%7C-1042117722%7CvVersion%7C5.1.1',
        'lidc': '"b=VB25:s=V:r=V:a=V:p=V:g=4555:u=290:x=1:i=1724170869:t=1724250396:v=2:sig=AQG-Lel-eqFsASPXcfiW88QLxd5XC5Nf"',
        'li_mc': 'MTsyMTsxNzI0MTczMzYwOzI7MDIxl2AKw8B12Ck6WCE69xV9hAnOcf0APa8W2t+RRm/H39Q=',
    }

    headers = {
        'accept': 'application/vnd.linkedin.normalized+json+2.1',
        'accept-language': 'en,es-ES;q=0.9,es;q=0.8',
        'cache-control': 'no-cache',
        'csrf-token': 'ajax:1260589603359156689',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.linkedin.com/company/getclearco/people/',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'x-li-lang': 'en_US',
        'x-li-page-instance': 'urn:li:page:companies_company_people_index;bbbcc109-7278-462d-b73d-873df4928926',
        'x-li-pem-metadata': 'Voyager - Organization - Member=organization-people-card',
        'x-li-track': '{"clientVersion":"1.13.21804","mpVersion":"1.13.21804","osName":"web","timezoneOffset":2,"timezone":"Europe/Madrid","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":2560,"displayHeight":1440}',
        'x-restli-protocol-version': '2.0.0',
    }


    params = {
        'decorationId': 'com.linkedin.voyager.dash.deco.web.mynetwork.ConnectionListWithProfile-16',
        'count': pageResults,
        'q': 'search',
        'sortType': 'RECENTLY_ADDED',
        'start': skip,
    }

    response = requests.get(
        'https://www.linkedin.com/voyager/api/relationships/dash/connections',
        params=params,
        cookies=cookies,
        headers=headers,
    )

    return response.json()

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
        'gpv_pn': 'www.linkedin.com%2Flearning%2Fcertificates%2F3505135148ab0144039103eef02af863d5a989a1dac029dfa2c8d04817a6eb5e',
        's_ips': '754',
        's_tp': '1137',
        's_tslv': '1723544506842',
        'UserMatchHistory': 'AQKxJp9p6ugWGAAAAZFlwaSnnGIzWs91xLajPUfPs2IdhVWOC6WV2fVFLiDUIJzC4QYOUUAfpotY3BQTAXIeGy__tdkIFkIAPAGUVWyGBdw7ML2uuFXJF2SssJNkSp8kp3PHsYNF6uwIIF9rCntmYGQ7aJhsuFxdhCP9mSpilHIB7J-rdkqcVUi1f54WOiER5Bp32TjHuhqu8usQPzgnk8rt3gsea3CorWwAnoF036cMSVZpxj8yoyWHdi8Zoqwdh9zY_FwHjm4jge2vmCjvy41LjikEEw_oukugEWE',
        'AnalyticsSyncHistory': 'AQJgclM3JqfezwAAAZFlwaSnT3r-eTUgCXokYk9LxCln8Kd2TMsr7wt7rza4Ak9H59Buq9lkhiP1_dy6uz8CTg',
        'lms_ads': 'AQFHEOIxMRyDZgAAAZFlwaWx7Sxd8xM1bJGxp-Tc4v7lgQA_g9mLxTn-s_53xBZaoeqboYuGi7qFZL1ElMSHuS8bHW3-ptu5',
        'lms_analytics': 'AQFHEOIxMRyDZgAAAZFlwaWx7Sxd8xM1bJGxp-Tc4v7lgQA_g9mLxTn-s_53xBZaoeqboYuGi7qFZL1ElMSHuS8bHW3-ptu5',
        'fptctx2': 'taBcrIH61PuCVH7eNCyH0B9zcK90d%252bIeoo1r5v7Zc25%252bhrebb1qgOOP9pIXly9Xzd9WQbVsnEGOw8K54cJpm8F%252bX6ZLTIA6HrA3Yf7ZSLSM85nDRmvWPNm5CqNfKOF464OuX3LxGY82SwwRfFIKNcu%252fd8cG6O2bIZ%252fIdzKx%252bY8EHkzi5%252f008L5EwLGqGb2lKhMa1vEtHQU0RsuzJb2w1qtKg2jYTgAD4mSH3FIRlP2OgzB6PmsJWKRnnr1vdxZ140OJRg%252bdTpUqx0Nb2XHCVOF0jT%252bN1Yp1ghjapbv8d5WkU0aWrAj%252bLSODGoArhxL6Wq5yQH%252f%252fFkBDVFV8AFNNIwBxf4PD3W5U1xjKUI6nbsAc%253d',
        'lang': 'v=2&lang=en-us',
        'JSESSIONID': '"ajax:1260589603359156689"',
        'liap': 'true',
        'li_at': 'AQEDAQyUAAUFTEj4AAABkXAmGdgAAAGRlDKd2FYARkWw2-eEDacDbpF1fDxm8iwpTVks1pZKV_gVnQVtydQv_i7zKrr1dfhUjWqSRjEEZ_McGefUAXT4I8v9y3kzB7g7kCLBSQ1b-q9CRrzm1zOW7eRt',
        'aam_uuid': '65186641928165972290944689059145236147',
        'AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg': '1',
        'AMCV_14215E3D5995C57C0A495C55%40AdobeOrg': '-637568504%7CMCIDTS%7C19956%7CMCMID%7C65375001142020493180996169135698103672%7CMCAAMLH-1724768232%7C6%7CMCAAMB-1724768232%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1724170632s%7CNONE%7CMCCIDH%7C-1042117722%7CvVersion%7C5.1.1',
        'lidc': '"b=VB25:s=V:r=V:a=V:p=V:g=4555:u=290:x=1:i=1724170869:t=1724250396:v=2:sig=AQG-Lel-eqFsASPXcfiW88QLxd5XC5Nf"',
        'li_mc': 'MTsyMTsxNzI0MTczMzYwOzI7MDIxl2AKw8B12Ck6WCE69xV9hAnOcf0APa8W2t+RRm/H39Q=',
    }

    headers = {
        'accept': 'application/vnd.linkedin.normalized+json+2.1',
        'accept-language': 'en,es-ES;q=0.9,es;q=0.8',
        'cache-control': 'no-cache',
        # 'cookie': 'bcookie="v=2&9eea89ba-722f-477a-84e3-3e6d8e678d84"; bscookie="v=1&20220906081743417c0602-7506-4356-8852-0e6bdb335ca0AQEFn_05jcNzFmIoikMmW1zqyhQ2ILdQ"; li_alerts=e30=; li_theme=light; li_theme_set=app; li_sugr=4ee71373-67cd-4197-9aa9-7232704f70f3; timezone=Europe/Madrid; dfpfpt=c85e0cc9d1304ddcba56e1a8319ea589; VID=V_2024_02_05_11_567954; _uetvid=fe6ec5c0abe411eeb9cf0339d942b7c2; ajs_user_id=530; ajs_anonymous_id=8c83f5f3-a011-438c-b84a-07a581256c87; ajs_group_id=293; s_fid=2D10B75662F051D3-01E38D594F2F5E54; visit=v=1&M; li_gc=MTswOzE3MTI2NTUyNTU7MjswMjFX4QImcDVCFBcu2ZDjxzh4WZTBY+icw0BXiGzDcCDbYw==; li_rm=AQFuetDQTmNCGgAAAZCS9fV4J8nm15ljp0Azuys435SYayIOWk_zCTqzFYL1GMQ4Tp0HBF_qcdaNrjE8Yvse9Go_BlM9xvcr93B4wEbUtBKFWnOwbToEJ27yQ1lYZHsgzQXp9UyJrsKvrEWYfb9oQk_SFkNlA9I0BECc7q4tKIm5Y8La8ODthzgoncSMk419TYkQQK4NZ_k1MY7Ro1qGabDN0P7faGhT70K8wLaY1B71cNzObyIot3ReNBkqTL-Go18DgJ34CMe-KAIznC_-26RzEFXwj7ZrQFWxdN9p6cpVJtfiqbcfAO8Srrocp8HRfX7ELYE69GPLbnrLB98; g_state={"i_l":0}; mbox=PC#168ae17f557a4962b31729b81729e625.37_0#1736854308|session#a442e5d6420b4a128652d58fa8d65fa8#1721304168; gpv_pn=www.linkedin.com%2Flearning%2Fcertificates%2F3505135148ab0144039103eef02af863d5a989a1dac029dfa2c8d04817a6eb5e; s_ips=754; s_tp=1137; s_tslv=1723544506842; UserMatchHistory=AQKxJp9p6ugWGAAAAZFlwaSnnGIzWs91xLajPUfPs2IdhVWOC6WV2fVFLiDUIJzC4QYOUUAfpotY3BQTAXIeGy__tdkIFkIAPAGUVWyGBdw7ML2uuFXJF2SssJNkSp8kp3PHsYNF6uwIIF9rCntmYGQ7aJhsuFxdhCP9mSpilHIB7J-rdkqcVUi1f54WOiER5Bp32TjHuhqu8usQPzgnk8rt3gsea3CorWwAnoF036cMSVZpxj8yoyWHdi8Zoqwdh9zY_FwHjm4jge2vmCjvy41LjikEEw_oukugEWE; AnalyticsSyncHistory=AQJgclM3JqfezwAAAZFlwaSnT3r-eTUgCXokYk9LxCln8Kd2TMsr7wt7rza4Ak9H59Buq9lkhiP1_dy6uz8CTg; lms_ads=AQFHEOIxMRyDZgAAAZFlwaWx7Sxd8xM1bJGxp-Tc4v7lgQA_g9mLxTn-s_53xBZaoeqboYuGi7qFZL1ElMSHuS8bHW3-ptu5; lms_analytics=AQFHEOIxMRyDZgAAAZFlwaWx7Sxd8xM1bJGxp-Tc4v7lgQA_g9mLxTn-s_53xBZaoeqboYuGi7qFZL1ElMSHuS8bHW3-ptu5; fptctx2=taBcrIH61PuCVH7eNCyH0B9zcK90d%252bIeoo1r5v7Zc25%252bhrebb1qgOOP9pIXly9Xzd9WQbVsnEGOw8K54cJpm8F%252bX6ZLTIA6HrA3Yf7ZSLSM85nDRmvWPNm5CqNfKOF464OuX3LxGY82SwwRfFIKNcu%252fd8cG6O2bIZ%252fIdzKx%252bY8EHkzi5%252f008L5EwLGqGb2lKhMa1vEtHQU0RsuzJb2w1qtKg2jYTgAD4mSH3FIRlP2OgzB6PmsJWKRnnr1vdxZ140OJRg%252bdTpUqx0Nb2XHCVOF0jT%252bN1Yp1ghjapbv8d5WkU0aWrAj%252bLSODGoArhxL6Wq5yQH%252f%252fFkBDVFV8AFNNIwBxf4PD3W5U1xjKUI6nbsAc%253d; lang=v=2&lang=en-us; JSESSIONID="ajax:1260589603359156689"; liap=true; li_at=AQEDAQyUAAUFTEj4AAABkXAmGdgAAAGRlDKd2FYARkWw2-eEDacDbpF1fDxm8iwpTVks1pZKV_gVnQVtydQv_i7zKrr1dfhUjWqSRjEEZ_McGefUAXT4I8v9y3kzB7g7kCLBSQ1b-q9CRrzm1zOW7eRt; aam_uuid=65186641928165972290944689059145236147; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19956%7CMCMID%7C65375001142020493180996169135698103672%7CMCAAMLH-1724768232%7C6%7CMCAAMB-1724768232%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1724170632s%7CNONE%7CMCCIDH%7C-1042117722%7CvVersion%7C5.1.1; lidc="b=VB25:s=V:r=V:a=V:p=V:g=4555:u=290:x=1:i=1724170869:t=1724250396:v=2:sig=AQG-Lel-eqFsASPXcfiW88QLxd5XC5Nf"; li_mc=MTsyMTsxNzI0MTczMzYwOzI7MDIxl2AKw8B12Ck6WCE69xV9hAnOcf0APa8W2t+RRm/H39Q=',
        'csrf-token': 'ajax:1260589603359156689',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.linkedin.com/company/getclearco/people/',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'x-li-lang': 'en_US',
        'x-li-page-instance': 'urn:li:page:companies_company_people_index;bbbcc109-7278-462d-b73d-873df4928926',
        'x-li-pem-metadata': 'Voyager - Organization - Member=organization-people-card',
        'x-li-track': '{"clientVersion":"1.13.21804","mpVersion":"1.13.21804","osName":"web","timezoneOffset":2,"timezone":"Europe/Madrid","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":2560,"displayHeight":1440}',
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
        

def start():
    api = Linkedin(
        os.getenv('LINKEDIN_USER'), 
        os.getenv('LINKEDIN_PWD')
    )

    connections = getConnectionProfile(pageResults=100)

    with open('./network_explorer/output/network.csv', mode='w', newline='') as csv_file:
        first_time = True
        for conn in connections['included'][0:5]:
            search_time = time.time()

            complete_experiences = getExperiences("urn:li:fsd_profile:ACoAAAOn1UABKMHC3S-I0qpjm-wCtKtWmxvwv4M")
            #complete_experiences = getExperiences(conn['entityUrn'])
            
            for exp in complete_experiences:
                exp.update({'firstName':conn['firstName'], 'lastName': conn['lastName'], 'publicIdentifier': conn['publicIdentifier'], 'entityUrn': conn['entityUrn']})
                
                #print(exp)
                if first_time:
                    writer = csv.DictWriter(csv_file, fieldnames=exp.keys())
                    writer.writeheader()
                    first_time = False
                writer.writerow(exp)
        
            #print(f"LinkedIn network: {round(time.time() - search_time, 2)}s")


#start()
