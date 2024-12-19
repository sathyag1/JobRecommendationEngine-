import CommonLayer as CL
import DataLayer as DL
from datetime import date, timedelta
today = date.today()
import datetime
datetime.datetime.now()
now = datetime.datetime.now()

import pandas as pd
import json
import requests
import spacy
nlp = spacy.load('en_core_web_sm')
import os
Elk_Endpoint = os.getenv('ELK_ENDPOINT')
ELK_USERNAME = os.getenv('ELK_USERNAME')
ELK_PASSWORD = os.getenv('ELK_PASSWORD')
ELK_INDEX = os.getenv('ELK_INDEX')
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import connections
es=connections.create_connection(hosts=[Elk_Endpoint],timeout=1200,http_auth=(ELK_USERNAME,ELK_PASSWORD))

print(es.ping())
def get_resultforjoblist(location,gender,maximum_experience,domainjobs,qualification,skills,maximum_salary,profilepath):
    
    maximum_experience = int(float(maximum_experience))
    maximum_salary = int(maximum_salary)

    
    qualification_encoded = CL.getqualification_master_encoding(qualification)
    edu_lmt="lte"
   
    print(edu_lmt)
    print(qualification_encoded)

    ###location###
    place=CL.listto_string(location)
    
    ###preferencejob###
    preferredjobs=[]
    for i in domainjobs:
        for values in i.values():
            if values != "undefined":
               preferredjobs.append(values)                
#         print(preferredjobs)

    skillval=[]
    for i in skills: 
        for key, value in i.items():
            if key == "preferred_skill_name" and value != "undefined":
                # if value != "undefined":
                skillval.append(value)
                   
    print(skillval)
    
    skillset = ','.join(str(e) for e in skillval)
    preferredjobs = ','.join(str(e) for e in preferredjobs)
    locationmatch = ','.join(str(e) for e in place)
    
    if profilepath == '':
        print('userjourney1')
        userjourney="1"
        skill_vec = CL.similarity_match([skillset])
    else:
        print('userjourney2')
        userjourney="2"
        content = requests.get(profilepath).content
        text = CL.clean_text(CL.readpdf(content))
        embed_user = CL.convert_sentancevec(text)
        skill_vec=CL.convert_array(embed_user)
        
    print('condition1')
    condition_cnt = 1
    
    must_query,should_query = get_querystringforpreferencefields(preferredjobs,gender,locationmatch,maximum_experience,
                                      qualification_encoded,edu_lmt,skillset,maximum_salary,qualification,condition_cnt)
    
    dfresult=DL.getelkdata(must_query,should_query)
   
    checkval=dfresult.shape[0]
    print(checkval)

    checkval=dfresult.shape[0]
    print(checkval)
    
    if checkval == 0 or checkval <= 10:
        print('condition2')
        condition_cnt = 2
      
        must_query,should_query = get_querystringforpreferencefields(preferredjobs,gender,locationmatch,maximum_experience,
                                      qualification_encoded,edu_lmt,skillset,maximum_salary,qualification,condition_cnt)
    
        dfresult=DL.getelkdata(must_query,should_query)
        print(len(dfresult.index))
        checkval=dfresult.shape[0]
        print(checkval)
        
        if checkval == 0 or checkval <= 10:
            print( print('condition3'))
            condition_cnt = 3
           
            must_query,should_query = get_querystringforpreferencefields(preferredjobs,gender,locationmatch,maximum_experience,
                                      qualification_encoded,edu_lmt,skillset,maximum_salary,qualification,condition_cnt)
            dfresult=DL.getelkdata(must_query,should_query)
            print(len(dfresult.index))
            checkval=dfresult.shape[0]
            print(checkval)
            if checkval == 0 or checkval <= 10:
                print('condition4')
                condition_cnt = 4
                must_query,should_query = get_querystringforpreferencefields(preferredjobs,gender,locationmatch,maximum_experience,
                                      qualification_encoded,edu_lmt,skillset,maximum_salary,qualification,condition_cnt)
                dfresult=DL.getelkdata(must_query,should_query)
                
                print(len(dfresult.index))
                checkval=dfresult.shape[0]
                print(checkval)
                
                if checkval == 0 or checkval <= 10:
                    print('condition5')
                    condition_cnt = 5
                  
                    must_query,should_query = get_querystringforpreferencefields(preferredjobs,gender,locationmatch,maximum_experience,
                                      qualification_encoded,edu_lmt,skillset,maximum_salary,qualification,condition_cnt)
                    dfresult=DL.getelkdata(must_query,should_query)
                    print(len(dfresult.index))
                    checkval=dfresult.shape[0]
                    print(checkval)   
                    if checkval == 0 or checkval <= 10:
                        print('condition6')
                        condition_cnt = 6
                  
                        must_query,should_query = get_querystringforpreferencefields(preferredjobs,gender,locationmatch,maximum_experience,
                                      qualification_encoded,edu_lmt,skillset,maximum_salary,qualification,condition_cnt)
                        dfresult=DL.getelkdata(must_query,should_query)
                        print(len(dfresult.index))
                        checkval=dfresult.shape[0]
                        print(checkval) 
                        if checkval == 0 or checkval <= 10:
                            print('Last-condition7')
                            condition_cnt = 7
                       
                            must_query,should_query = get_querystringforpreferencefields(preferredjobs,gender,locationmatch,maximum_experience,
                                      qualification_encoded,edu_lmt,skillset,maximum_salary,qualification,condition_cnt)
    
                            dfresult=DL.getelkdata(must_query,should_query)
                            print(len(dfresult.index))
                            checkval=dfresult.shape[0]
                            print(checkval)
                    
                            job_recommendation_relevancy = get_latandlogdata(place,dfresult,maximum_salary,maximum_experience,qualification_encoded,
                                                                    skill_vec,preferredjobs,userjourney)
                        
                            #return job_recommendation_relevancy
                        else:
                            job_recommendation_relevancy = get_latandlogdata(place,dfresult,maximum_salary,maximum_experience,qualification_encoded,
                                                                    skill_vec,preferredjobs,userjourney)
                            #return job_recommendation_relevancy
                    else:
                        
                        job_recommendation_relevancy = get_latandlogdata(place,dfresult,maximum_salary,maximum_experience,qualification_encoded,
                                                                    skill_vec,preferredjobs,userjourney)
                        
                        #return job_recommendation_relevancy
                            
                else:
                        
                    job_recommendation_relevancy = get_latandlogdata(place,dfresult,maximum_salary,maximum_experience,qualification_encoded,
                                                                    skill_vec,preferredjobs,userjourney)
                    
                    #return job_recommendation_relevancy
            
            else:
                job_recommendation_relevancy = get_latandlogdata(place,dfresult,maximum_salary,maximum_experience,qualification_encoded,
                                                                    skill_vec,preferredjobs,userjourney)
                
                #return job_recommendation_relevancy 
            
        
        else:

            job_recommendation_relevancy = get_latandlogdata(place,dfresult,maximum_salary,maximum_experience,qualification_encoded,
                                                                    skill_vec,preferredjobs,userjourney)
            #return job_recommendation_relevancy 
    else:
    
        job_recommendation_relevancy = get_latandlogdata(place,dfresult,maximum_salary,maximum_experience,qualification_encoded,
                                                                    skill_vec,preferredjobs,userjourney)

    return job_recommendation_relevancy 


def get_resultforfilter(req_dict):
    location=req_dict['location']
    gender=req_dict['gender']
    maximum_experience=req_dict['maximum_experience']
    domainjobs=req_dict['domainjobs']
    qualification=req_dict['qualification']
    skills=req_dict['skills']
    minimum_salary=req_dict['minimum_salary']
    maximum_salary=req_dict['maximum_salary']
    job_title=req_dict['job_title']
    job_type=req_dict['job_type']
    language=req_dict['language']
    shift_type=req_dict['shift_type']
    industries=req_dict['industries']
    mode_of_work=req_dict['mode_of_work']
    jobpostdate=req_dict['jobpostdate']
    profilepath=req_dict['profilepath']
    
    maximum_experience = int(float(maximum_experience))
    minimum_salary = int(minimum_salary)
    maximum_salary = int(maximum_salary)
    
    
    qualification_encoded = CL.getqualification_master_encoding(qualification)
    # print(qualification)
    edu_lmt=CL.getedu_lmt(qualification)

    # print(edu_lmt)
    # print(qualification_encoded)
    
                  
    
    ###location###
    
    place=CL.listto_string(location)
    # print(place)
    
     ###preferencejob###
    preferredjobs=[]
    for i in domainjobs:
        for values in i.values():
            if values != "undefined":
               preferredjobs.append(values)                
    
    # print(preferredjobs)
    preferredjobs = ','.join(str(e) for e in preferredjobs)
    ###jobtitle###      
            
    jobtitle=CL.inputto_list(job_title)        
    ###jobtype###
                   
    jobtype=CL.inputto_list(job_type)   

   ###language###
                
    language_value=CL.inputto_list(language)
   ###shifttype###
                
    shifttype=CL.inputto_list(shift_type)
   ###modeofwork###
    
    modeofwork=CL.inputto_list(mode_of_work)        
   ###industries_names###
                  
    industries_names=CL.inputto_list(industries)
    ###skills###
    
    skillval=[]
    for i in skills: 
        for key, value in i.items():
             if key == "preferred_skill_name" and value != "undefined":
                skillval.append(value)
                
    # print(skillval)
    skillset = ','.join(str(e) for e in skillval)
    pastthreedays = []
    if jobpostdate :
        if jobpostdate == "Anytime":
            pastthreedays = [""]
        elif jobpostdate == "Today":
            pastthreedays.append(today.strftime("%Y-%m-%d"))
        else :  
            day_cnt = 0
            for i in jobpostdate.split():
                if i.isdigit():
                    day_cnt=i
                    
            day_cnt = int(day_cnt)
            # print(day_cnt)
            if day_cnt != 0:
                for i in range(day_cnt):  
                    date=today - timedelta(days = i+1)
                    pastthreedays.append(date.strftime("%Y-%m-%d"))
            else:
                pastthreedays = [""]
    else:
        pastthreedays = [""]
       
    
    

    locationmatch = ','.join(str(e) for e in place)
    
    if profilepath == '':
        print('userjourney1')
        userjourney="1"
        skill_vec = CL.similarity_match([skillset])
    else:
        print('userjourney2')
        userjourney="2"
        content = requests.get(profilepath).content
        text = CL.clean_text(CL.readpdf(content))
        embed_user = CL.convert_sentancevec(text)
        skill_vec=CL.convert_array(embed_user)
      
    input_dict={'place':place,
                'gender':gender,
                'edu_lmt':edu_lmt,
                'locationmatch':locationmatch,
                'preferredjobs':preferredjobs,
                'minimum_salary':minimum_salary,
                'maximum_salary':maximum_salary,
                'maximum_experience':maximum_experience,
                'qualification_encoded':qualification_encoded,
                'jobtype':jobtype,
                'shifttype':shifttype,
                'modeofwork':modeofwork,
                'pastthreedays':pastthreedays,
                'language_value':language_value,
                'skillset':skillset,
                'industries_names':industries_names,
                'jobtitle':jobtitle
                }
    condition_cnt = 1
    matchquery,matchqueryforshould = get_querystringforfilter(input_dict,condition_cnt)

    dfresult=DL.getfilterelkdata(gender,skillset,matchquery,matchqueryforshould)
    
    print(len(dfresult.index))
    checkdfval=dfresult.shape[0]
    print(checkdfval)   
    
    if checkdfval == 0 :
        
        if industries_names != [''] and industries_names != ['All'] and  industries_names != ['', 'All']:
                print("Industryreturn")
                isempty = dfresult.empty
                    
                if isempty is True:
                    print('no match found')
                    return dfresult
            
        condition_cnt = 2
        
        matchquery,matchqueryforshould = get_querystringforfilter(input_dict,condition_cnt)
        dfresult=DL.getfilterelkdata(gender,skillset,matchquery,matchqueryforshould)
    
        print(len(dfresult.index))
        checkdfval=dfresult.shape[0]
        print(checkdfval)   

        if checkdfval == 0 :
            
            isempty = dfresult.empty
            
            if isempty is True:
                print('no match found')
                return dfresult   
          
        else:
              job_recommendation_relevancy = get_latandlogdata(place,dfresult,maximum_salary,maximum_experience,qualification_encoded,
                                                                    skill_vec,preferredjobs,userjourney)
        
              return job_recommendation_relevancy 
    else:
        
        job_recommendation_relevancy = get_latandlogdata(place,dfresult,maximum_salary,maximum_experience,qualification_encoded,
                                                                    skill_vec,preferredjobs,userjourney)
        
        return job_recommendation_relevancy 
    
def getsavedjoblist(job_number):    
    
    print(job_number)
    jobno_list=[]
    for i in job_number:
        for values in i.values():
            jobno_list.append(values)  
    
    print(jobno_list)     
    dfresult=DL.getsavedjoblistfrom_elk(jobno_list)
    dfresult = dfresult[["JOB_NUMBER","JOB_DOMAIN","JOB_TITLE","MINIMUM_EXPERIENCE","MAXIMUM_EXPERIENCE","MINIMUM_SALARY","MAXIMUM_SALARY","JOB_WORK_LOCATION_1"
                                    ,"LOGO_URL","REQUIREMENT","COMPANY_NAME","COMPANY_ADDRESS","SOURCE","WORK_MODE_TYPE","SHIFT_TYPE","JOB_DESCRIPTION","JOB_POST_DATE",
                                     "QUALIFICATION","APPLY_VIA","LINK_TO_APPLY","NUMBER_OF_OPENINGS","ADDITIONAL_INFO","SKILL_REQUIRED","RESPONSIBILITY"
                                    ]]
    dfresult = dfresult.drop_duplicates()
    return dfresult   
   
def get_latandlogdata(place,dfresult,maximum_salary,maximum_experience,qualification_encoded,
                                                                    skill_vec,preferredjobs,userjourney):
    if place != [] and place != ['All'] :
        counter = 0
        for i in place:
            counter = counter + 1
            print(counter)
            dfresult = add_distance(dfresult,counter,i)
            dfresult = get_min_distance(dfresult,counter)

    else:
        
        dfresult['DISTANCE'] = 0
        
    
    dfresult['WBSC'] = dfresult.apply(lambda row: CL.matchingscore(row,maximum_salary,maximum_experience,qualification_encoded,
                                                                skill_vec,preferredjobs,place,userjourney), axis=1)

    #Recommendation based on Revelancy score #Top 10
    job_recommendation_relevancy = dfresult[["JOB_NUMBER","JOB_DOMAIN","JOB_TITLE","MINIMUM_EXPERIENCE","MAXIMUM_EXPERIENCE","MINIMUM_SALARY","MAXIMUM_SALARY","JOB_WORK_LOCATION_1"
                                    ,"LOGO_URL","REQUIREMENT","COMPANY_NAME","COMPANY_ADDRESS","SOURCE","WORK_MODE_TYPE","SHIFT_TYPE","GENDER","JOB_DESCRIPTION","JOB_POST_DATE","QUALIFICATION","APPLY_VIA","LINK_TO_APPLY",
                                    'WBSC'
                                    ]]
        
    job_recommendation_relevancy = job_recommendation_relevancy.drop_duplicates()
    job_recommendation_relevancy = job_recommendation_relevancy.sort_values(["WBSC"], ascending = False, ignore_index=True).head(400)

        
    return job_recommendation_relevancy

def add_distance(dfresult,counter,i):
    print(i)
    user_lat,user_long=CL.get_latlong(i)
    counter = str(counter)
    city_name = "DISTANCE" + counter
    print(city_name)
    dfresult['loc1']=dfresult[["LATITUDE","LONGITUDE"]].apply(lambda x: CL.get_distance( x["LATITUDE"], x["LONGITUDE"],user_lat,user_long),axis=1)
    dfresult['loc2']=dfresult[["LATITUDE2","LONGITUDE2"]].apply(lambda x: CL.get_distance( x["LATITUDE2"], x["LONGITUDE2"],user_lat,user_long),axis=1)
    dfresult['loc3']=dfresult[["LATITUDE3","LONGITUDE3"]].apply(lambda x: CL.get_distance( x["LATITUDE3"], x["LONGITUDE3"],user_lat,user_long),axis=1)
    dfresult['loc4']=dfresult[["LATITUDE4","LONGITUDE4"]].apply(lambda x: CL.get_distance( x["LATITUDE4"], x["LONGITUDE4"],user_lat,user_long),axis=1)
    # print(loc2,loc3,loc4)
    dfresult[city_name]=dfresult[['loc1','loc2','loc3','loc4']].min(axis=1)   
    return dfresult


def get_min_distance(dfresult,counter):
    if counter == 1:
        dfresult['DISTANCE'] = dfresult['DISTANCE1']  
    elif counter == 2:
        dfresult['DISTANCE'] = dfresult[['DISTANCE1','DISTANCE2']].min(axis=1)   
    elif counter == 3:
        dfresult['DISTANCE'] = dfresult[['DISTANCE1','DISTANCE2','DISTANCE3']].min(axis=1)   
    elif counter == 4:
        dfresult['DISTANCE'] = dfresult[['DISTANCE1','DISTANCE2','DISTANCE3','DISTANCE4']].min(axis=1) 
    elif counter == 5:
        dfresult['DISTANCE'] = dfresult[['DISTANCE1','DISTANCE2','DISTANCE3','DISTANCE4','DISTANCE5']].min(axis=1) 
    elif counter == 6:
        dfresult['DISTANCE'] = dfresult[['DISTANCE1','DISTANCE2','DISTANCE3','DISTANCE4','DISTANCE5','DISTANCE6']].min(axis=1) 
    elif counter == 7:
        dfresult['DISTANCE'] = dfresult[['DISTANCE1','DISTANCE2','DISTANCE3','DISTANCE4','DISTANCE5','DISTANCE6','DISTANCE7']].min(axis=1) 
    elif counter == 8:
        dfresult['DISTANCE'] = dfresult[['DISTANCE1','DISTANCE2','DISTANCE3','DISTANCE4','DISTANCE5','DISTANCE6','DISTANCE7','DISTANCE8']].min(axis=1) 
    elif counter == 9:
        dfresult['DISTANCE'] = dfresult[['DISTANCE1','DISTANCE2','DISTANCE3','DISTANCE4','DISTANCE5','DISTANCE6','DISTANCE7','DISTANCE8','DISTANCE9']].min(axis=1) 
    elif counter > 9:
        dfresult['DISTANCE'] = dfresult['DISTANCE1']  
    else:
        dfresult['DISTANCE'] = 0
    return dfresult
    
    
def get_querystringforfilter(input_dict,condition_cnt):
    place=input_dict['place']
    gender=input_dict['gender']
    edu_lmt=input_dict['edu_lmt']
    locationmatch=input_dict['locationmatch']
    preferredjobs=input_dict['preferredjobs']
    minimum_salary=input_dict['minimum_salary']
    maximum_salary=input_dict['maximum_salary']
    maximum_experience=input_dict['maximum_experience']
    qualification_encoded=input_dict['qualification_encoded']
    jobtype=input_dict['jobtype']
    shifttype=input_dict['shifttype']
    modeofwork=input_dict['modeofwork']
    pastthreedays=input_dict['pastthreedays']
    language_value=input_dict['language_value']
    skillset=input_dict['skillset']
    industries_names=input_dict['industries_names']
    jobtitle=input_dict['jobtitle']
    print(condition_cnt)
    matchquery=[]
    
    if industries_names != [''] and industries_names != ['All'] and  industries_names != ['', 'All']:
        print("withIndustries")
        industries_names = ','.join(str(e) for e in industries_names)
        if condition_cnt == 1:
            matchquery.append(
                {
                    "multi_match": {
                        "query": industries_names,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
            
    else:    
        print("withDomain")
        if condition_cnt == 1:
           matchquery.append(
                {
                    "multi_match": {
                        "query": preferredjobs,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        elif condition_cnt == 2:
            matchquery.append(
                {
                    "multi_match": {
                        "query": skillset,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )     
    matchquery.append({"term": { "ACTIVE_FLAG": "true"}},)
    matchquery.append({"range": { "QUALIFICATION_ENCODED": { edu_lmt : qualification_encoded}}},)
    
    print(gender)
    if gender != '' and gender != 'Others' and  gender != 'Non-binary' and 'Does not want' not in gender:
       matchquery.append(
                {
                    "multi_match": {
                        "query": gender,
                        "type": "most_fields",
                        "fields": [
                            "GENDER",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )   
    if maximum_experience != 0 :
       #matchquery.append({"range": { "MINIMUM_EXPERIENCE": { "gte" : maximum_experience}}}, )
        matchquery.append({"bool":
        {"should": [
          
            {"range": {"MINIMUM_EXPERIENCE": {"lte": maximum_experience}}},
            {"bool":{"must_not": [{"exists": {"field"   :"MINIMUM_EXPERIENCE"}}]}}
          
            ]}},)
        matchquery.append({"bool":
        {"should": [
          
            {"range": {"MAXIMUM_EXPERIENCE": {"gte": maximum_experience}}},
            {"bool":{"must_not": [{"exists": {"field"   :"MAXIMUM_EXPERIENCE"}}]}}
          
            ]}},)    
    if place != [] and place != ['All'] and  place != ['', 'All']:
       matchquery.append(
                {
                    "multi_match": {
                        "query": locationmatch,
                        "type": "most_fields",
                        "fields": [
                            "JOB_WORK_LOCATION_1",
                            "JOB_WORK_LOCATION_2",
                            "JOB_WORK_LOCATION_3",
                            "JOB_WORK_LOCATION_4",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
    
    if maximum_salary != 0 or  minimum_salary != 0:
        print(maximum_salary)
        matchquery.append({"range": { 
                                    "MINIMUM_SALARY" : {
                                            "gte" : minimum_salary,
                                            "lte" : maximum_salary
                                        }}},)
        matchquery.append({"range": { 
                                    "MAXIMUM_SALARY" : {
                                            "lte" : maximum_salary
                                        }}},)
    else:
        print('0')
        
    print(jobtype)
    if jobtype != [''] and jobtype != ['All'] and  jobtype != ['', 'All']:
        if len(jobtype)==1:
            jobtype_names = ','.join(str(e) for e in jobtype)
            matchquery.append(
                {
                    "multi_match": {
                        "query": jobtype_names,
                        "type": "phrase",
                        "fields": [
                            "EMPLOYEMENT_TYPE",
                            "WORK_PLACE",
                            "WORK_MODE_TYPE",
                            "SHIFT_TYPE",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
            
        else:
            jobtype_names = '|'.join(str(e) for e in jobtype)
            matchquery.append({
                "simple_query_string": {
                                "query": jobtype_names,
                                "flags": "OR|AND|PREFIX",
                                "fields": [
                                "WORK_MODE_TYPE.keyword",
                                "EMPLOYEMENT_TYPE.keyword",
                                "WORK_PLACE.keyword",
                                "SHIFT_TYPE.keyword", 
                                "JOB_DESCRIPTION"
                                ]
                                }
                            })
     
    if shifttype != ['']:
        if len(shifttype) == 1:
           shifttype_names = ','.join(str(e) for e in shifttype)
           matchquery.append(
                    {
                        "multi_match": {
                            "query": shifttype_names,
                            "type": "phrase",
                            "fields": [
                                "SHIFT_TYPE",
                                "WORK_PLACE",
                                "WORK_MODE_TYPE",
                                "EMPLOYEMENT_TYPE",
                                "JOB_DESCRIPTION"
                            ]
                        }
                    }
                )
        else:
            shifttype_names = '|'.join(str(e) for e in shifttype)
            matchquery.append({
                "simple_query_string": {
                                "query": shifttype_names,
                                "flags": "OR|AND|PREFIX",
                                "fields": [
                                "WORK_MODE_TYPE.keyword",
                                "EMPLOYEMENT_TYPE.keyword",
                                "WORK_PLACE.keyword",
                                "SHIFT_TYPE.keyword", 
                                "JOB_DESCRIPTION"
                                ]
                                }
                            })
         
    if language_value != ['']:
        language_names = ','.join(str(e) for e in language_value)
        matchquery.append(
                {
                    "multi_match": {
                        "query": language_names,
                        "type": "most_fields",
                        "fields": [
                            "LANGUAGE",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
        
    if modeofwork != ['']:
        if len(modeofwork)==1:
            MODEOFWORK_NAMES = ','.join(str(e) for e in modeofwork)
            matchquery.append(
                {
                    "multi_match": {
                        "query": MODEOFWORK_NAMES,
                        "type": "phrase",
                        "fields": [
                            "WORK_MODE_TYPE",
                            "WORK_PLACE",
                            "SHIFT_TYPE",
                            "EMPLOYEMENT_TYPE",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
        else:
            modeofwork_names = '|'.join(str(e) for e in modeofwork)
            matchquery.append({
                "simple_query_string": {
                                "query": modeofwork_names,
                                "flags": "OR|AND|PREFIX",
                                "fields": [
                                "WORK_MODE_TYPE.keyword",
                                "EMPLOYEMENT_TYPE.keyword",
                                "WORK_PLACE.keyword",
                                "SHIFT_TYPE.keyword", 
                                "JOB_DESCRIPTION"
                                ]
                                }
                            })
        
    if pastthreedays != ['']:
        matchquery.append({"terms": { "JOB_POST_DATE":pastthreedays}})
        
    matchqueryforshould=[]
    
    if industries_names != [''] and industries_names != ['All'] and  industries_names != ['', 'All']:
        print("withIndustries")
        matchqueryforshould.append(
                {
                    "multi_match": {
                        "query": industries_names,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
         
        if jobtitle != [''] and jobtitle != ['All'] and  jobtitle != ['Others'] :
            jobtitle = ','.join(str(e) for e in jobtitle)
            matchqueryforshould.append(
                {
                    "multi_match": {
                        "query": jobtitle,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
    else:
        print("withDomain")
        
        matchqueryforshould.append(
                {
                    "multi_match": {
                        "query": preferredjobs,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        matchqueryforshould.append(
                {
                    "multi_match": {
                        "query": skillset,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )

        
        if jobtitle != [''] and jobtitle != ['All'] and  jobtitle != ['Others'] :
            jobtitle = ','.join(str(e) for e in jobtitle)
            matchqueryforshould.append(
                {
                    "multi_match": {
                        "query": jobtitle,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
   
    return matchquery,matchqueryforshould

def get_querystringforpreferencefields(preferredjobs,gender,locationmatch,maximum_experience,
                                      qualification_encoded,edu_lmt,skillset,maximum_salary,qualification,condition_cnt):
    must_query = []
    should_query = []
    print(condition_cnt)
    if condition_cnt == 1:
        must_query.append(
                {
                    "multi_match": {
                        "query": preferredjobs,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        must_query.append({"range": { "QUALIFICATION_ENCODED": { edu_lmt : qualification_encoded}}},)
        must_query.append({"range": { "MINIMUM_EXPERIENCE": { "gte" : maximum_experience}}}, )
        must_query.append({"term": { "ACTIVE_FLAG": "true"}} )
        should_query.append(
                {
                    "multi_match": {
                        "query": skillset,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        should_query.append({"term": { "GENDER.keyword": gender}},)
        should_query.append({"range": { "MAXIMUM_SALARY": { "gte": maximum_salary}}},)
        should_query.append(
                {
                    "multi_match": {
                        "query": locationmatch,
                        "type": "most_fields",
                        "fields": [
                            "JOB_WORK_LOCATION_1",
                            "JOB_WORK_LOCATION_2",
                            "JOB_WORK_LOCATION_3",
                            "JOB_WORK_LOCATION_4",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
    elif condition_cnt == 2:
        must_query.append(
                {
                    "multi_match": {
                        "query": skillset,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        must_query.append({"range": { "QUALIFICATION_ENCODED": { edu_lmt : qualification_encoded}}},)
        must_query.append({"range": { "MINIMUM_EXPERIENCE": { "gte" : maximum_experience}}}, )
        must_query.append({"term": { "ACTIVE_FLAG": "true"}} )
        should_query.append(
                {
                    "multi_match": {
                        "query": preferredjobs,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        should_query.append({"term": { "GENDER.keyword": gender}},)
        should_query.append({"range": { "MAXIMUM_SALARY": { "gte": maximum_salary}}},)
        should_query.append(
                {
                    "multi_match": {
                        "query": locationmatch,
                        "type": "most_fields",
                        "fields": [
                            "JOB_WORK_LOCATION_1",
                            "JOB_WORK_LOCATION_2",
                            "JOB_WORK_LOCATION_3",
                            "JOB_WORK_LOCATION_4",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
    elif condition_cnt == 3:
        must_query.append(
                {
                    "multi_match": {
                        "query": preferredjobs,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        must_query.append({"term": { "ACTIVE_FLAG": "true"}} )
        should_query.append(
                {
                    "multi_match": {
                        "query": skillset,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        should_query.append({"range": { "QUALIFICATION_ENCODED": { edu_lmt : qualification_encoded}}},)
        should_query.append({"range": { "MINIMUM_EXPERIENCE": { "gte" : maximum_experience}}}, )
        should_query.append({"term": { "GENDER.keyword": gender}},)
        should_query.append({"range": { "MAXIMUM_SALARY": { "gte": maximum_salary}}},)
        should_query.append(
                {
                    "multi_match": {
                        "query": locationmatch,
                        "type": "most_fields",
                        "fields": [
                            "JOB_WORK_LOCATION_1",
                            "JOB_WORK_LOCATION_2",
                            "JOB_WORK_LOCATION_3",
                            "JOB_WORK_LOCATION_4",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
    elif condition_cnt == 4:
        must_query.append(
                {
                    "multi_match": {
                        "query": skillset,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        must_query.append({"term": { "ACTIVE_FLAG": "true"}} )
        should_query.append({"range": { "QUALIFICATION_ENCODED": { edu_lmt : qualification_encoded}}},)
        should_query.append({"range": { "MINIMUM_EXPERIENCE": { "gte" : maximum_experience}}}, )
        should_query.append({"term": { "GENDER.keyword": gender}},)
        should_query.append({"range": { "MAXIMUM_SALARY": { "gte": maximum_salary}}},)
        should_query.append(
                {
                    "multi_match": {
                        "query": locationmatch,
                        "type": "most_fields",
                        "fields": [
                            "JOB_WORK_LOCATION_1",
                            "JOB_WORK_LOCATION_2",
                            "JOB_WORK_LOCATION_3",
                            "JOB_WORK_LOCATION_4",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
    elif condition_cnt == 5:
        must_query.append({"term": { "ACTIVE_FLAG": "true"}} )
        should_query.append(
                {
                    "multi_match": {
                        "query": preferredjobs,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        should_query.append({"range": { "QUALIFICATION_ENCODED": { edu_lmt : qualification_encoded}}},)
        should_query.append({"range": { "MINIMUM_EXPERIENCE": { "gte" : maximum_experience}}}, )
        should_query.append({"term": { "GENDER.keyword": gender}},)
        should_query.append({"range": { "MAXIMUM_SALARY": { "gte": maximum_salary}}},)
        should_query.append(
                {
                    "multi_match": {
                        "query": locationmatch,
                        "type": "most_fields",
                        "fields": [
                            "JOB_WORK_LOCATION_1",
                            "JOB_WORK_LOCATION_2",
                            "JOB_WORK_LOCATION_3",
                            "JOB_WORK_LOCATION_4",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
    
    elif condition_cnt == 6:
        must_query.append({"term": { "ACTIVE_FLAG": "true"}} )
        should_query.append(
                {
                    "multi_match": {
                        "query": qualification,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DESCRIPTION",
                            "QUALIFICATION"
                        ]
                    }
                }
            )
        should_query.append({"range": { "QUALIFICATION_ENCODED": { edu_lmt : qualification_encoded}}},)
        should_query.append({"range": { "MINIMUM_EXPERIENCE": { "gte" : maximum_experience}}}, )
        should_query.append({"term": { "GENDER.keyword": gender}},)
        should_query.append({"range": { "MAXIMUM_SALARY": { "gte": maximum_salary}}},)
        should_query.append(
                {
                    "multi_match": {
                        "query": locationmatch,
                        "type": "most_fields",
                        "fields": [
                            "JOB_WORK_LOCATION_1",
                            "JOB_WORK_LOCATION_2",
                            "JOB_WORK_LOCATION_3",
                            "JOB_WORK_LOCATION_4",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
    
    elif condition_cnt == 7:
        must_query.append({"term": { "ACTIVE_FLAG": "true"}} )
        should_query.append(
                {
                    "multi_match": {
                        "query": skillset,
                        "type": "most_fields",
                        "fields": [
                            "JOB_DOMAIN",
                            "SKILL_REQUIRED",
                            "JOBTITLE",
                            "INDUSTRY"
                        ]
                    }
                }
            )
        should_query.append({"range": { "QUALIFICATION_ENCODED": { edu_lmt : qualification_encoded}}},)
        should_query.append({"range": { "MINIMUM_EXPERIENCE": { "gte" : maximum_experience}}}, )
        should_query.append({"term": { "GENDER.keyword": gender}},)
        should_query.append({"range": { "MAXIMUM_SALARY": { "gte": maximum_salary}}},)
        should_query.append(
                {
                    "multi_match": {
                        "query": locationmatch,
                        "type": "most_fields",
                        "fields": [
                            "JOB_WORK_LOCATION_1",
                            "JOB_WORK_LOCATION_2",
                            "JOB_WORK_LOCATION_3",
                            "JOB_WORK_LOCATION_4",
                            "JOB_DESCRIPTION"
                        ]
                    }
                }
            )
    
    return must_query,should_query
    

#freetext functions
def get_freetext_search(freetext):
    print("in get free text search")
    searchtext=freetext
    
    freetext_raw=freetext
    if 'IT' not in freetext_raw:
        
        freetext_raw=freetext_raw.lower()
        freetext_raw=' '.join([str(token.lemma_) for token in nlp(freetext) if not token.is_stop ] )
    
    
    for i in ["jobs","job","Job","Jobs"]:
        freetext_raw=str(freetext_raw).replace(i,"")
        searchtext=searchtext.replace(i,"")
    print("after removing job related: ",freetext_raw)
    freetext = CL.clean_text(freetext)
    freetext=freetext.replace("job","")
    print("after clean text",freetext)
    
        
    # for workmodetype
    query_body = {
        "size": 0, 
        "aggs": {
            "workmodes":{
                "terms": {
                    "field": "WORK_MODE_TYPE.keyword","size": 1000                   }} } }
    response = es.search(index=ELK_INDEX,body=query_body)
    data=""
    data = [x for x in response["aggregations"]["workmodes"]["buckets"]]
    json_string=""
    json_string = json.dumps(data)
    wmresult = pd.read_json(json_string, orient ='records')
    workmodelist=wmresult["key"].tolist()
    
    '''for i in ['',' ']:
        if i in workmodelist:
            workmodelist.remove(i)'''
    for i in workmodelist:
        if len(i)<1:
            workmodelist.remove(i)
    workmodelist = [x.lower() for x in workmodelist]
    workmodelist=list(set(workmodelist))
    wmt=""
    for each in workmodelist:
        #if each in ' '.join([str(token.lemma_).lower() for token in nlp(freetext_raw)]):
        if each in searchtext.lower():
            print(each)
            wmt += each+" "
            searchtext=searchtext.replace(each,"")
    wmt=wmt.strip()
    print("extracted workmode",wmt,"searchtext",searchtext)

    ##########
    
    #for job domains

    query_body = {
        "size": 0, 
        "aggs": {
            "job_domains":{
                "terms": {
                    "field": "JOB_DOMAIN.keyword","size": 1000
                    }} } }
    response = es.search(index=ELK_INDEX,body=query_body,size=400)
    data=""
    data = [x for x in response["aggregations"]["job_domains"]["buckets"]]
    json_string=""
    json_string = json.dumps(data)
    jdresult = pd.read_json(json_string, orient ='records')
    job_domainlist=jdresult["key"].tolist()
    job_domainlist=list(set(job_domainlist))
    
    text_jobdomain=''
    for each in job_domainlist:
        if each in freetext_raw and each!=' ' and each !='':
            text_jobdomain += each+" "
    
    
    text_jobdomain=text_jobdomain.rstrip(" ")
    text_jobdomain=text_jobdomain.strip()
    print("found job domain",text_jobdomain)
    

    # for jobtitles
    query_body = {
        "size": 0, 
        "aggs": {
            "jobtitles":{
                "terms": {
                    "field": "JOB_TITLE.keyword","size":1000                    }} } }
    response = es.search(index=ELK_INDEX,body=query_body)
    data=""
    data = [x for x in response["aggregations"]["jobtitles"]["buckets"]]
    json_string=""
    json_string = json.dumps(data)
    jtresult = pd.read_json(json_string, orient ='records')
    job_title_list=jtresult["key"].tolist()
    
    
    job_title_list = [x.lower() for x in job_title_list]
    
    if 't' in job_title_list:
        job_title_list.remove('t')
    job_title_list=list(set(job_title_list))
    
    text_job_title=''
    textjtlist=[]
    for each in job_title_list:
        if each in freetext and each!='' and each!=' ' and len(each)>1:
            print(each)
            text_job_title +=each+" "
            textjtlist.append(each)
    print("list of len jobtitle:",len(textjtlist),textjtlist)
    jtdict={}
    for each in textjtlist:
        print(len(each.split(" ")))        
        if each!= '':
            jtdict[each]=len(each.split(" "))

    print("job titles found dictionary:",jtdict, "is dictionary emplty",bool(jtdict))  
    
    if bool(jtdict) is False:
        jobtitlefreetext=""
    else:
        jobtitlefreetext = max(zip(jtdict.values(), jtdict.keys()))[1]
    
    #location related
    
    indian_city_db = ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Ahmedabad', 'Chennai', 'Kolkata', 'Surat', 'Pune', 'Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Thane', 'Bhopal', 'Visakhapatnam', 'Pimpri-Chinchwad', 'Patna', 'Vadodara', 'Ghaziabad', 'Ludhiana', 'Agra', 'Nashik', 'Faridabad', 'Meerut', 'Rajkot', 'Kalyan-Dombivli', 'Vasai-Virar', 'Varanasi', 'Srinagar', 'Aurangabad', 'Dhanbad', 'Amritsar', 'Navi Mumbai', 'Allahabad', 'Howrah', 'Ranchi', 'Gwalior', 'Jabalpur', 'Coimbatore', 'Vijayawada', 'Jodhpur', 'Madurai', 'Raipur', 'Kota', 'Chandigarh', 'Guwahati', 'Solapur', 'Hubli–Dharwad', 'Mysore', 'Tiruchirappalli', 'Bareilly', 'Aligarh', 'Tiruppur', 'Gurgaon', 'Moradabad', 'Jalandhar', 'Bhubaneswar', 'Salem', 'Warangal', 'Mira-Bhayandar', 'Jalgaon', 'Guntur', 'Thiruvananthapuram', 'Bhiwandi', 'Saharanpur', 'Gorakhpur', 'Bikaner', 'Amravati', 'Noida', 'Jamshedpur', 'Bhilai', 'Cuttack', 'Firozabad', 'Kochi', 'Nellore', 'Bhavnagar', 'Dehradun', 'Durgapur', 'Asansol', 'Rourkela', 'Nanded', 'Kolhapur', 'Ajmer', 'Akola', 'Gulbarga', 'Jamnagar', 'Ujjain', 'Loni', 'Siliguri', 'Jhansi', 'Ulhasnagar', 'Jammu', 'Sangli-Miraj & Kupwad', 'Mangalore', 'Erode', 'Belgaum', 'Kurnool', 'Ambattur', 'Rajahmundry', 'Tirunelveli', 'Malegaon', 'Gaya', 'Udaipur', 'Karur', 'Kakinada', 'Davanagere', 'Kozhikode', 'Maheshtala', 'Rajpur Sonarpur', 'Bokaro', 'South Dum Dum', 'Bellary', 'Patiala', 'Gopalpur', 'Agartala', 'Bhagalpur', 'Muzaffarnagar', 'Bhatpara', 'Panihati', 'Latur', 'Dhule', 'Rohtak', 'Sagar', 'Korba', 'Bhilwara', 'Berhampur', 'Muzaffarpur', 'Ahmednagar', 'Mathura', 'Kollam', 'Avadi', 'Kadapa', 'Anantapuram[21]', 'Kamarhati', 'Bilaspur', 'Sambalpur', 'Shahjahanpur', 'Satara', 'Bijapur', 'Rampur', 'Shimoga', 'Chandrapur', 'Junagadh', 'Thrissur', 'Alwar', 'Bardhaman', 'Kulti', 'Nizamabad', 'Parbhani', 'Tumkur', 'Khammam', 'Uzhavarkarai', 'Bihar Sharif', 'Panipat', 'Darbhanga', 'Bally', 'Aizawl', 'Dewas', 'Ichalkaranji', 'Karnal', 'Bathinda', 'Jalna', 'Eluru', 'Barasat', 'Kirari Suleman Nagar', 'Purnia', 'Satna', 'Mau', 'Sonipat', 'Farrukhabad', 'Durg', 'Imphal', 'Ratlam', 'Hapur', 'Arrah', 'Anantapur', 'Karimnagar', 'Etawah', 'Ambarnath', 'North Dum Dum', 'Bharatpur', 'Begusarai', 'New Delhi', 'Gandhidham', 'Baranagar', 'Tiruvottiyur', 'Pondicherry', 'Sikar', 'Thoothukudi', 'Rewa', 'Mirzapur', 'Raichur', 'Pali', 'Ramagundam', 'Silchar', 'Haridwar', 'Vijayanagaram', 'Tenali', 'Nagercoil', 'Sri Ganganagar', 'Karawal Nagar', 'Mango', 'Thanjavur', 'Bulandshahr', 'Uluberia', 'Katni', 'Sambhal', 'Singrauli', 'Nadiad', 'Secunderabad', 'Naihati', 'Yamunanagar', 'Bidhannagar', 'Pallavaram', 'Bidar', 'Munger', 'Panchkula', 'Burhanpur', 'Kharagpur', 'Dindigul', 'Gandhinagar', 'Hospet', 'Nangloi Jat', 'Malda', 'Ongole', 'Deoghar', 'Chhapra', 'Puri', 'Haldia', 'Khandwa', 'Nandyal', 'Morena', 'Amroha', 'Anand', 'Bhind', 'Bhalswa Jahangir Pur', 'Madhyamgram', 'Bhiwani', 'Berhampore', 'Ambala', 'Morbi', 'Fatehpur', 'Raebareli', 'Khora, Ghaziabad', 'Chittoor', 'Bhusawal', 'Orai', 'Bahraich', 'Phusro', 'Vellore', 'Mehsana', 'Raiganj', 'Sirsa', 'Danapur', 'Serampore', 'Sultan Pur Majra', 'Guna', 'Jaunpur', 'Panvel', 'Shivpuri', 'Surendranagar Dudhrej', 'Unnao', 'Chinsurah', 'Alappuzha', 'Kottayam', 'Machilipatnam', 'Shimla', 'Midnapore', 'Adoni', 'Udupi', 'Katihar', 'Proddatur', 'Budaun', 'Mahbubnagar', 'Saharsa', 'Dibrugarh', 'Jorhat', 'Hazaribagh', 'Hindupur', 'Nagaon', 'Hajipur', 'Sasaram', 'Giridih', 'Bhimavaram', 'Port Blair', 'Kumbakonam', 'Bongaigaon', 'Raigarh', 'Dehri', 'Madanapalle', 'Siwan', 'Bettiah', 'Ramgarh', 'Tinsukia', 'Guntakal', 'Srikakulam', 'Motihari', 'Dharmavaram', 'Medininagar', 'Gudivada', 'Phagwara', 'Pudukkottai', 'Hosur', 'Narasaraopet', 'Suryapet', 'Miryalaguda', 'Anantnag', 'Tadipatri', 'Karaikudi', 'Kishanganj', 'Gangavathi', 'Jamalpur', 'Ballia', 'Kavali', 'Tadepalligudem', 'Amaravati', 'Buxar', 'Tezpur', 'Jehanabad', 'Aurangabad', 'Gangtok', 'Vasco Da Gama']

    
    indian_loc = []
    for city in indian_city_db: indian_loc.append(city.lower())
    
    
    location = []
    for i in freetext.split():
        if i in indian_loc:
            location.append(i)

    qlocation = ''
    # freetext in case of IT in search
    flag=0
    if 'IT' in searchtext:
        flag=1
    searchtext=searchtext.lower()
    for i in location:
        print(i)
        qlocation += i+" "    
        freetext=freetext.replace(i,"")        
        searchtext=searchtext.replace(i,"")
    qlocation=qlocation.rstrip(" ")
    print(qlocation,"search text",searchtext)
    
    searchtext=' '.join([str(token) for token in nlp(searchtext) if not token.is_stop ] )
    print(qlocation,"search text",searchtext)
    
    #removing word job from freetext
    freetext=freetext.replace("job","")
    gender=""
    if "female" in freetext:
        freetext= freetext.replace("female","")
        gender="female"
    elif "male" in freetext:
        freetext= freetext.replace("male","")
        gender="male"
    matchquery=[]
    must_matchquery=[]
    if  len(text_job_title)!=0:
        print(text_jobdomain," jobtitle coming in query")
        matchquery.append({"match":{"JOB_TITLE":jobtitlefreetext}})
    if len(text_jobdomain)!=0:
        print("*** job domain line 304:",text_jobdomain,";",len(text_jobdomain))
        matchquery.append({"match":{"JOB_DOMAIN":text_jobdomain}})
    else:
        print("*** no job domain found")
    if gender !="":
        must_matchquery.append({"match":{"GENDER":gender}})
    
   
    #if wmt !="":
    if len(wmt)>1:    
        must_matchquery.append({"match_phrase": {
                        "WORK_MODE_TYPE": wmt
                    }})
    must_matchquery.append( {"match": {
                        "ACTIVE_FLAG": "true"
                    }})
    matchqueryfilter=[]
    if qlocation!='': 
        matchqueryfilter.append(
                {
                    "multi_match": {
                        "query": qlocation,
                        "type": "most_fields",
                        "fields": [
                            "JOB_WORK_LOCATION_1",
                            "JOB_WORK_LOCATION_2",
                            "JOB_WORK_LOCATION_3",
                            "JOB_WORK_LOCATION_4"
                        ]
                    }
                }
            )
   
    if len(text_job_title)!=0 and len(text_jobdomain)!=0:
        
        result=DL.getfreetextdata(matchquery,must_matchquery,matchqueryfilter)
                
    elif len(text_job_title)!=0 or len(text_jobdomain)!=0:
        must_matchquery=[];matchquery=[]
        if  len(text_job_title)!=0:
            print(text_jobdomain," jobtitle coming in query")
            must_matchquery.append({"match":{"JOB_TITLE":jobtitlefreetext}})
        if len(text_jobdomain)!=0:
            print("*** job domain line 304:",text_jobdomain,";",len(text_jobdomain))
            must_matchquery.append({"match":{"JOB_DOMAIN":text_jobdomain}})
        #if wmt !="":
        if len(wmt)>1:    
            must_matchquery.append({"match_phrase": {
                        "WORK_MODE_TYPE": wmt
                    }})
        must_matchquery.append( {"match": {
                        "ACTIVE_FLAG": "true"
                    }})
        if gender !="":
            must_matchquery.append({"match":{"GENDER":gender}})
            
        result=DL.getfreetextdata(matchquery,must_matchquery,matchqueryfilter)
    else:
        must_matchquery=[]
        print("insed else case final one")
        if flag==1:
            searchtext=searchtext+"IT"
        if len(searchtext)!=0:
            must_matchquery.append({"multi_match": {
                                "query": searchtext,
                                "type": "most_fields",
                                "fields": ["SKILL_REQUIRED",                            
                            "JOB_TITLE","JOB_DOMAIN","JOB_DESCRIPTION"
                            ]
                                }})
        #if wmt !="":
        if len(wmt)>1:
            print("in line 1354")
            must_matchquery.append({"match_phrase": {
                        "WORK_MODE_TYPE": wmt
                    }})
        must_matchquery.append( {"match": {
                        "ACTIVE_FLAG": "true"
                    }})
        if gender !="":
            must_matchquery.append({"match":{"GENDER":gender}})
        
        result=DL.getfreetextdata(matchquery,must_matchquery,matchqueryfilter)
        
    
    final_df=result
    final_df=final_df[['JOB_NUMBER','SKILL_REQUIRED', 'JOB_DOMAIN', 'JOB_TITLE', 'GENDER','WORK_MODE_TYPE',\
                        'SHIFT_TYPE',"REQUIREMENT","COMPANY_ADDRESS","SOURCE","JOB_DESCRIPTION","LINK_TO_APPLY",\
                        'COMPANY_NAME','JOB_WORK_LOCATION_1', 'JOB_WORK_LOCATION_2', "QUALIFICATION",\
                            'JOB_WORK_LOCATION_3', 'JOB_WORK_LOCATION_4',"LOGO_URL","JOB_POST_DATE","APPLY_VIA",\
                                "MINIMUM_EXPERIENCE","MAXIMUM_EXPERIENCE","MINIMUM_SALARY","MAXIMUM_SALARY"]]

    return final_df


