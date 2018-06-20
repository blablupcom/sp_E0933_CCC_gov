# -*- coding: utf-8 -*-

#### IMPORTS 1.0

import os, json
import re
import scraperwiki
import urllib2
from datetime import datetime
from bs4 import BeautifulSoup


#### FUNCTIONS 1.2
import requests

def validateFilename(filename):
    filenameregex = '^[a-zA-Z0-9]+_[a-zA-Z0-9]+_[a-zA-Z0-9]+_[0-9][0-9][0-9][0-9]_[0-9QY][0-9]$'
    dateregex = '[0-9][0-9][0-9][0-9]_[0-9QY][0-9]'
    validName = (re.search(filenameregex, filename) != None)
    found = re.search(dateregex, filename)
    if not found:
        return False
    date = found.group(0)
    now = datetime.now()
    year, month = date[:4], date[5:7]
    validYear = (2000 <= int(year) <= now.year)
    if 'Q' in date:
        validMonth = (month in ['Q0', 'Q1', 'Q2', 'Q3', 'Q4'])
    elif 'Y' in date:
        validMonth = (month in ['Y1'])
    else:
        try:
            validMonth = datetime.strptime(date, "%Y_%m") < now
        except:
            return False
    if all([validName, validYear, validMonth]):
        return True


def validateURL(url):
    try:
        r = requests.get(url)
        count = 1
        while r.status_code == 500 and count < 4:
            print ("Attempt {0} - Status code: {1}. Retrying.".format(count, r.status_code))
            count += 1
            r = requests.get(url)
        sourceFilename = r.headers.get('Content-Disposition')

        if sourceFilename:
            ext = os.path.splitext(sourceFilename)[1].replace('"', '').replace(';', '').replace(' ', '')
        else:
            ext = os.path.splitext(url)[1]
        validURL = r.status_code == 200
        validFiletype = ext.lower() in ['.csv', '.xls', '.xlsx']
        return validURL, validFiletype
    except:
        print ("Error validating URL.")
        return False, False


def validate(filename, file_url):
    validFilename = validateFilename(filename)
    validURL, validFiletype = validateURL(file_url)
    if not validFilename:
        print filename, "*Error: Invalid filename*"
        print file_url
        return False
    if not validURL:
        print filename, "*Error: Invalid URL*"
        print file_url
        return False
    if not validFiletype:
        print filename, "*Error: Invalid filetype*"
        print file_url
        return False
    return True


def convert_mth_strings ( mth_string ):
    month_numbers = {'JAN': '01', 'FEB': '02', 'MAR':'03', 'APR':'04', 'MAY':'05', 'JUN':'06', 'JUL':'07', 'AUG':'08', 'SEP':'09','OCT':'10','NOV':'11','DEC':'12' }
    for k, v in month_numbers.items():
        mth_string = mth_string.replace(k, v)
    return mth_string


#### VARIABLES 1.0

entity_id = "E0933_CCC_gov"
url = "https://www.carlisle.gov.uk/open-data/DesktopModules/DocumentViewer/API/ContentService/GetFolderDescendants?parentId=2114&sortOrder=&searchText="
errors = 0
data = []
ua = {'requestverificationtoken':'i3_cHuEClJ7QYaW579uxiAdR0m-il3z1uPP0g-9NyhnITv5P2WkfpK7fMq07abe_x1G8xomRp2dZA_EuF_uZqzGlNcc2Qe0p3l5yLBdpUlDAM9fwsRHsaJUO6gA1',
      'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36',
      'x-requested-with':'XMLHttpRequest', 'moduleid':'8297', 'tabid':'1600'}

#### READ HTML 1.0

html = requests.get(url, headers=ua)
json_soup = json.loads(html.text)

#### SCRAPE DATA

items = json_soup['Items']
for item in items:
    item_key = item['key']
    year_url = 'https://www.carlisle.gov.uk/open-data/DesktopModules/DocumentViewer/API/ContentService/GetFolderContent?startIndex=0&numItems=100&sort=Name+asc&folderId={}'
    year_html = requests.get(year_url.format(item_key), headers=ua)
    year_json_soup = json.loads(year_html.text)
    year_items = year_json_soup['Items']
    for year_item in year_items:
        url = 'https://www.carlisle.gov.uk'+year_item['Url']
        file_name = year_item['Name']
        # print file_name.split('_')
        if '.csv' in file_name:
            csvYr = file_name.split('_')[-1][:4]
            csvMth = file_name.split('_')[-2][:3]
            if '201' in csvMth:
                csvMth = 'Apr'
                csvYr = '2011'
            if '20' not in csvYr:
                csvYr = '20'+csvYr.split('.')[0]
            csvMth = convert_mth_strings(csvMth.upper())
            data.append([csvYr, csvMth, url])


#### STORE DATA 1.0

for row in data:
    csvYr, csvMth, url = row
    filename = entity_id + "_" + csvYr + "_" + csvMth
    todays_date = str(datetime.now())
    file_url = url.strip()

    valid = validate(filename, file_url)

    if valid == True:
        scraperwiki.sqlite.save(unique_keys=['l'], data={"l": file_url, "f": filename, "d": todays_date })
        print filename
    else:
        errors += 1

if errors > 0:
    raise Exception("%d errors occurred during scrape." % errors)


#### EOF
