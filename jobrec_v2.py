import os
import pandas as pd
import numpy as np
import glob
import re
import fitz
import CommonLayer as CL
import DataLayer as DL
import BusinessLayer as BL
from dotenv import load_dotenv
from elasticapm.contrib.flask import ElasticAPM
from elasticapm.handlers.logging import LoggingHandler
import logging
import traceback
from authlib.jose import jwt
from functools import wraps
import time

load_dotenv()

public_key = os.getenv('PUBLIC_KEY')
Service_Name = os.getenv('SERVICE_NAME')
Server_Url = os.getenv('SERVER_URL')
Secret_token = os.getenv('SECRET_TOKEN')
Environment = os.getenv('ENVIRONMENT')
log_level = os.getenv('LOG_LEVEL')
Errormsg = "No records found"
from flask import Flask, jsonify, request
from flask_restful import Resource, Api
from flask_cors import CORS
import json
key = '-----BEGIN PUBLIC KEY-----\n' + public_key + '\n-----END PUBLIC KEY-----'
key_binary = key.encode('ascii')


from sentence_transformers import SentenceTransformer,util
model = SentenceTransformer('all-MiniLM-L6-v2')

import spacy
nlp = spacy.load('en_core_web_sm')

import logging
logging.getLogger().setLevel(logging.CRITICAL)

# creating the flask app for post method
app = Flask(__name__)

CORS(app)
cors = CORS(app, resources={ 
    r"/*": { 
        "Access_Control_Allow_Origin": "*",
        "origins": "*",
        "allowedHeaders" :'Content-Type, Authorization, Origin, X-Requested-With, Accept',
        "method":['GET','POST','PATCH','DELETE','PUT']
           }
})

app.config['ELASTIC_APM'] = {
    'SERVICE_NAME':Service_Name,
    'SERVER_URL': Server_Url,
    'SECRET_TOKEN': Secret_token,
    'ENVIRONMENT' : Environment,
    'LOG_LEVEL' : log_level 
}

apm = ElasticAPM(app)

# Custom middleware to Protection header to all responses
@app.after_request
def add_security_headers(resp):
    resp.headers['Content-Security-Policy']='default-src \'self\''
    resp.headers['X-XSS-Protection'] = '1; mode=block'
    resp.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains; preload'
    resp.headers['X-Content-Type-Options'] = 'nosniff'
    resp.headers['X-Frame-Options'] = 'SAMEORIGIN'
    return resp

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'msg':'OK!'})

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if "Authorization" in request.headers:
            token = request.headers['Authorization']
        if not token:
            return jsonify({"message": "Token is missing "}), 401

        try:
            claims = jwt.decode(token, key_binary)
            claims.validate()
            if (claims is not None ) and (time.time() < claims['exp']):
                print("Token Verified!!! ")

            else:
                print("Token is expired!!")

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            return jsonify({'msg':'some token error!!!'}), 401
        return f(*args, **kwargs)
    return decorated



@app.route('/Getsavedjoblist', methods=['POST'])
@token_required
def Getsavedjoblist():
    try:
        job_number=request.json['jobnumbers']
        print(job_number)
        dfresult=BL.getsavedjoblist(job_number)    
        
        json_str = dfresult.to_json(orient='records')
        json_records = json.loads(json_str)
        return jsonify(json_records)  
    
    except Exception as e:
            print(e)
            print(traceback.format_exc())
            return jsonify({'msg':Errormsg })

@app.route('/joblist1', methods=['POST'])
@token_required
def joblist1():
    try:
        
        location = request.json['preferred_work_location']
        gender  = request.json['gender']
        maximum_experience=request.json['total_work_experience']
        domainjobs=request.json['preferred_job_domain']
        qualification=request.json['qualification']
        skills=request.json['preferred_skill']
        maximum_salary=request.json['last_month_salary']
        profilepath=request.json['resume_url']
        pagenumber=request.json['page_num']
        
        print("profile value:",profilepath)
        
        page_size = 30
        
        print(maximum_salary)
        
        job_recommendation_relevancy=BL.get_resultforjoblist(location,gender,maximum_experience,domainjobs,qualification,skills,maximum_salary,profilepath)    
        job_cnt= job_recommendation_relevancy.shape[0]
        job_recommendation_relevancy = CL.paginate_dataframe(job_recommendation_relevancy, page_size, pagenumber)

        json_str = job_recommendation_relevancy.to_json(orient='records')
        
        json_records = json.loads(json_str)
        return jsonify(json_records,job_cnt)   
    
    except Exception as e:
            print(e)
            print(traceback.format_exc())
            return jsonify({'msg':Errormsg })

'''Start Filter'''

@app.route('/Filter', methods=['POST'])
@token_required
def Filter():
    try:
                
        req_dict={
            'location':request.json['preferred_work_location'],
            'gender':request.json['gender'],
            'maximum_experience':request.json['total_work_experience'],
            'domainjobs':request.json['preferred_job_domain'],
            'qualification':request.json['qualification'],
            'skills':request.json['preferred_skill'],
            'minimum_salary':request.json['minimum_salary'],
            'maximum_salary':request.json['maximum_salary'],
            'job_title':request.json['jobtitles'],
            'job_type':request.json['jobtypes'],
            'language':request.json['languages'],
            'shift_type':request.json['shifttypes'],
            'industries':request.json['industries'],
            'mode_of_work':request.json['modeforworks'],
            'jobpostdate':request.json['jobpostedDate'],
            'profilepath':request.json['resume_url']            
            }
        pagenumber=request.json['page_num']
        
        page_size = 30       
        print("input qualification",req_dict['qualification'])
        
        job_recommendation_relevancy=BL.get_resultforfilter(req_dict)
        job_cnt= job_recommendation_relevancy.shape[0]
        job_recommendation_relevancy = CL.paginate_dataframe(job_recommendation_relevancy, page_size, pagenumber)
        
        isempty = job_recommendation_relevancy.empty
        
        if isempty is True:
            return jsonify({'msg':Errormsg})
        else:

            json_str = job_recommendation_relevancy.to_json(orient='records')
            json_records = json.loads(json_str)#
            return jsonify(json_records,job_cnt)   
        
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        
        return jsonify({'msg':Errormsg })


@app.route('/jobfreetext', methods=['POST'])
@token_required
def jobfreetext():
    try:
        freetext = request.json['freetext']
        pgnum = request.json['page_num']

        page_size = 30

        result=BL.get_freetext_search(freetext)
       

        result_cnt=result.shape[0]
        result = CL.paginate_dataframe(result, page_size, pgnum)


        if result.empty is True:
            return jsonify({'msg':'No records found, empty result'})

        else:
            job_list=result.to_json(orient='records')
            json_records = json.loads(job_list)
            return jsonify(json_records,result_cnt)
        
    except Exception as e:  
        print(e)
        print(traceback.format_exc())
        return jsonify({'msg':Errormsg })
    
'''API Call'''
if __name__ == '__main__':
    handler = LoggingHandler(client=apm.client)
    handler.setLevel(logging.WARN)
    app.logger.addHandler(handler)
    app.run(host='0.0.0.0', port=5000)
    # app.run()
   
   





