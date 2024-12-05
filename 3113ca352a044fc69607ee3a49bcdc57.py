"""
This code is written by Kushagra Behere
on the  Date 10-10-2024

# Usage:
This script fetches the instance shape details

version 1.0.0

"""
# Importing required libraries
import time
import requests as re
import pandas as pd
import xml.etree.ElementTree as ET
from MessageSender import MessageSender, MessageType

# Defining Klera meta out dict
klera_meta_out = {
    "Instance Shape": {
        "DSTID": "Instance_Shape_V1.0.0",
        "Primary Key": {
            "isrowid": True,
            "isvisible": True,
            "datatype": "STRING"
        }
    },
    "Error Info": {
        "DSTID": "Instance_Shape_Error_Info_V1.0.0",
        "Primary Key": {
            "isrowid": True,
            "isvisible": True,
            "datatype": "STRING"
        }
    }
}
# Defining Klera inputs variables
klera_in_details = {
    "Instance_Shape_Url": {
        "description": "Instance Shape Url",
        "datatype": ["STRING"],
        "argtype": "Data",
        "multiplerows" : True,
        "masked": False,
        "required": True
    },
    "Username": {
        "description": "Username",
        "datatype": ["STRING"],
        "argtype": "Param",
        "masked": False,
        "required": True
    },
    "Password": {
        "description": "Password",
        "datatype": ["STRING"],
        "argtype": "Param",
        "masked": True,
        "required": True
    }
}
# Defining script input variables
'''
Instance_Shape_Url= ""

'''
# Defining data dicts
kleradict={}
klera_error_dict ={}

# Defining request timeout (s)
timeout = 600

# Flag to check for the script abort status
abort = False

# Defining API call headers
headers = {
          'Accept':'application/rdf+xml',
          'OSLC-Core-Version': "2.0"
          }

# Defining the list of keys to parse
main_key_list= [
        "itemId" ,
        "stateId" ,
        "label" ,
        "id" ,
        "parentItemId" ,
        "startDate" ,
        "endDate" ,
        "archived" ,
        "current" , 
        "hasDeliverable" , 
        "completed" , 
        "iterationTypeItemId",
        "leaf"]

# Logic starts
count = 0
auth=(Username,Password)
http_status_code = None
http_error_message = None

for instance_shape_url in Instance_Shape_Url:

    try:
        http_status_code = None
        http_error_message = None
        
        url = instance_shape_url
        
        try:
            response = re.get(url,auth=auth, timeout=timeout, headers=headers, verify = False)
        except re.exceptions.Timeout:
            raise Exception("Request Timeout")

        if response.status_code!=200:
            http_status_code = response.status_code
            http_error_message = response.text
            raise Exception
        
        # XML parsing logic starts
        artifact_details = ET.fromstring(response.text)
        
        resource_shape = artifact_details[0]

        for item in resource_shape:
            if(item.tag.rsplit('}',1)[1]=="title"):
                kleradict.setdefault('Primary Key',[]).append(str(instance_shape_url))
                kleradict.setdefault('Context Instance Shape Url',[]).append(str(instance_shape_url))
                kleradict.setdefault('Resolved Value',[]).append(str(item.text))
                break
                
        # Sending data to Klera in chunk of 100 records
        if count >=100:
            out_df = pd.DataFrame(data=kleradict)
            out_dict = {'Instance Shape': out_df}
            klera_dst = [out_dict]

            data_block = {}
            data_block['klera_dst'] = klera_dst
            data_block['klera_meta_out'] = klera_meta_out

            klera_message_block = {}
            klera_message_block['message_type'] = MessageType.DATA
            klera_message_block['data_block'] = data_block

            klera_message_sender.push_data_to_queue(klera_message_block)

            kleradict.clear()
            
            count=0

    except Exception as e:
        # Sending error information dataset to Klera in case of an exception
        klera_error_dict.setdefault('Primary Key',[]).append(str(instance_shape_url))
        klera_error_dict.setdefault('Context Instance Shape Url Id',[]).append(str(instance_shape_url))
        klera_error_dict.setdefault('Http Status Code',[]).append(str(http_status_code))
        klera_error_dict.setdefault('Exception Details',[]).append(str(e))
        klera_error_dict.setdefault('Http Error Message',[]).append(http_error_message)

        out_df2 = pd.DataFrame(data=klera_error_dict)
        out_dict2 = {'Error Info': out_df2}
        klera_dst = [out_dict2]

        data_block = {}
        data_block['klera_dst'] = klera_dst
        data_block['klera_meta_out'] = klera_meta_out

        klera_message_block = {}
        klera_message_block['message_type'] = MessageType.DATA
        klera_message_block['data_block'] = data_block

        klera_message_sender.push_data_to_queue(klera_message_block)

        klera_error_dict.clear()

        if e == "Request Timeout":
            abort = True
            break

# Sending the remaining data to Klera
if(kleradict):
    out_df = pd.DataFrame(data=kleradict)
    out_dict = {'Instance Shape': out_df}
    klera_dst = [out_dict]

    data_block = {}
    data_block['klera_dst'] = klera_dst
    data_block['klera_meta_out'] = klera_meta_out

    klera_message_block = {}
    klera_message_block['message_type'] = MessageType.DATA
    klera_message_block['data_block'] = data_block

    klera_message_sender.push_data_to_queue(klera_message_block)

    kleradict.clear()

klera_dst = []