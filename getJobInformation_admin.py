#############################################################################
#   Author: junghoon@rescale.com                                            #
#   Last updated: Jul 11, 2023                                              #
#   Description: Get a job information by each of users                     #
#   How to use: Just run this script                                        #
#############################################################################


import requests
import json
from datetime import datetime
import pytz
import os
import pandas as pd


def getcurrenttime(location):
    utc_now = datetime.now(pytz.utc)
    local_timezone = pytz.timezone(location)
    local_now = utc_now.astimezone(local_timezone)
    local_now_str = local_now.strftime('%Y-%m-%d %H:%M:%S %Z%z')
    return local_now_str


def getapisettings():
    home_path = os.path.expanduser("~")
    apiconfig = home_path + '/' + '.config' + '/' + 'rescale' + '/' + 'apiconfig'
    if (os.path.isfile(apiconfig)):
        f = open(apiconfig, "r")
        lines = f.readlines()
        f.close()
        apibaseurl = lines[1].split("=")[1].rstrip('\n').lstrip().replace("'","")
        apikey = lines[2].split("=")[1].rstrip('\n').lstrip().replace("'","")
        token = 'Token ' + apikey
    return apibaseurl, token


def getnumberofjobs(apibaseurl, token):
    check_njobs = requests.get(
        apibaseurl + '/api/v2/jobs/',
        headers = {'Authorization': token}
    )
    check_njobs_dict = json.loads(check_njobs.text)
    count = check_njobs_dict['count']
    return count


def getlistofjobs(apibaseurl, token, count):
    current_page = 1
    last_page = False
    list_jobid = [0 for i in range(count)]
    list_starttime = [0 for i in range(count)]
    list_d = [0 for i in range(count)]
    while (not(last_page)):
        check_jobid = requests.get(
            apibaseurl + '/api/v2/jobs/',
            headers = {'Authorization': token},
            params = {'page': current_page}
        )
        check_jobid_dict = json.loads(check_jobid.text)

        if current_page == 1:
            for i in range(0, 10):
                list_jobid[i] = check_jobid_dict['results'][i]['id']
        elif count > current_page * 10:
            indexup = (current_page - 1) * 10
            for i in range(0, 10):
                list_jobid[i + indexup] = check_jobid_dict['results'][i]['id']
        else:
            indexup = (current_page - 1) * 10
            uplimit = count % indexup
            for i in range(0, uplimit):
                list_jobid[i + indexup] = check_jobid_dict['results'][i]['id']
        
        if (check_jobid_dict['next'] == None):
            last_page = True
        else:
            current_page += 1
    return list_jobid


def getjobinfo(apibaseurl, token, count, list_jobid):
    list_id = []
    list_billingPriorityValue = []
    list_elapsedWalltimeSeconds = []
    list_owner = []
    list_isTemplate = []
    list_projectId = []
    list_userTags = []
    list_analysisName = []
    list_analysisVersionName = []
    list_slots = []
    list_coresPerSlot = []
    list_coreType = []
    for jobid in list_jobid:
        req_jobinfo = requests.get(
            apibaseurl + '/api/v2/jobs/' + jobid,
            headers = {'Authorization': token}
        )
        req_jobinfo_dict = json.loads(req_jobinfo.text)
        list_id.append(req_jobinfo_dict["id"])
        list_billingPriorityValue.append(req_jobinfo_dict["billingPriorityValue"])
        list_elapsedWalltimeSeconds.append(req_jobinfo_dict["elapsedWalltimeSeconds"])
        list_owner.append(req_jobinfo_dict["owner"])
        list_isTemplate.append(req_jobinfo_dict["isTemplate"])
        list_projectId.append(req_jobinfo_dict["projectId"])
        user_tags = req_jobinfo_dict["userTags"]
        tag_name = [tag["name"] for tag in user_tags]
        list_userTags.append(tag_name)
        list_analysisName.append(req_jobinfo_dict["jobanalyses"][0]["analysis"]["name"])
        list_analysisVersionName.append(req_jobinfo_dict["jobanalyses"][0]["analysis"]["versionName"])
        list_slots.append(req_jobinfo_dict["jobanalyses"][0]["hardware"]["slots"])
        list_coresPerSlot.append(req_jobinfo_dict["jobanalyses"][0]["hardware"]["coresPerSlot"])
        list_coreType.append(req_jobinfo_dict["jobanalyses"][0]["hardware"]["coreType"])
    df = pd.DataFrame({'jobid': list_id, 'priority': list_billingPriorityValue, 'uptime': list_elapsedWalltimeSeconds,
                       'owner': list_owner, 'template': list_isTemplate, 'projectid': list_projectId,
                       'tags': list_userTags, 'software': list_analysisName, 'version': list_analysisVersionName,
                       'coretype': list_coreType, 'nslots': list_slots, 'coresperslot': list_coresPerSlot})
    df['ncores'] = df['nslots'] * df['coresperslot']
    df['core-hour'] = df['uptime'] * df['ncores'] / 3600
    df.drop(['uptime', 'nslots', 'coresperslot'], axis=1)
    df = df[['jobid', 'template', 'owner', 'projectid', 'tags', 'core-hour', 'software', 'version', 'coretype', 'ncores', 'priority']]
    return df


def main():
    path_to_save = 'dataframe'
    location = 'Asia/Seoul'
    local_now_str = getcurrenttime(location)
    print('Current time is ' + local_now_str)
    apibaseurl, token = getapisettings()
    print('Reading the apisettings file is done')
    count = getnumberofjobs(apibaseurl, token)
    print('The total number of jobs(includind shared) is ' + str(count))
    list_jobid = getlistofjobs(apibaseurl, token, count)
    print('Creating the list of jobid is done')
    df = getjobinfo(apibaseurl, token, count, list_jobid)
    print('Creating the dataframe is done')
    owner_name = df['owner'].value_counts().idxmax()
    df.to_csv(path_to_save + '/' + owner_name + '_' + local_now_str + '.csv', mode="w", index=False)
    print('The dataframe is saved to ' + owner_name + '_' + local_now_str + '.csv')


if __name__ == '__main__':
    main()
