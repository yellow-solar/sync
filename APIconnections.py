""" Create objects to fetch CRM data from all suppliers  

        - connect to api with user and password
        - fetch data and return csv
        - create iterator with mapped headings
"""

# system
import os, re, sys, json
from io import StringIO
import csv

# 3rd party
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

from googleapi.gmail import Gmail

# config 
from config import config
# email config
gmail = Gmail('googleservice/mail-93851bb46b8d.json', 'system@yellow.africa')


class providerAPI:
    """ initalise provider API object to make snapshot, api updates etc. """
    def __init__(self, provider):
        # get config for provider
        cfg = config("providers")[provider]
        # create api connection
        self.user = cfg.get('username',None)
        self.pswrd = cfg.get('password',None)
        self.headers = cfg.get('headers',None)
        self.snapshoturl = cfg.get('snapshoturl',None)
        self.apiurl = cfg.get('apiurl',None)
        self.sep = cfg.get('sep',",")
        self.tables = cfg['tables']

    def pullSnapshot(self, tableurl):
        """ Download table from snapshot URL and correct for bad characters """
        # crete url based on tablename
        url = self.snapshoturl+"/"+tableurl
        if (self.headers is None):
            snapshot = requests.get(
                f"{self.snapshoturl}/{tableurl}", 
                auth=HTTPBasicAuth(self.user, self.pswrd),
                timeout = 10,
                )
        else:
            snapshot = requests.get(
                url, 
                headers = self.headers,
                timeout = 10,
                )
        # If successful then return the string
        if snapshot.status_code == 200:
            file_ = snapshot.content.decode('utf-8')
            # lines = file_.splitlines()
            return(file_)
        else:
            raise ValueError(
                "Request to " + tablename 
                + " failed with error code: " + str(snapshot.status_code)
                )

# Zoho API data
class ZohoAPI:
    def __init__(self, owner, auth_token, application_name):
        self.owner = owner
        self.auth_token = auth_token
        self.application_name = application_name
        self.scope = 'creatorapi'
        self.format_type = 'json'
        self.APIheader = {
            'authtoken': self.auth_token, 
            'scope': self.scope,
        } 
        self.RPCheader = {
            **self.APIheader,
            **{'zc_ownername': self.owner}
        } 
        self.baseUrl = 'https://creator.zoho.com/api'
        self.addUrl = '/record/add'
        self.deleteUrl = '/record/delete'
        self.editUrl = '/record/update'
        self.rpcUrl = '/xml/write' 
        #Create xml base for Zoho RPC API 
        self.rpcBaseXML = (
            "<ZohoCreator><applicationlist><application name={0}><formlist>" +
            "<form name={1}>{2}</form></formlist></application></applicationlist>" +
            "</ZohoCreator>"
        )

    #  function to get form
    def get(self, form, payload={}):
        url = self.baseUrl + '/' + self.format_type + '/' + self.application_name + '/' + 'view/' + form
        parameters = {**self.RPCheader,**payload}
        r = requests.get(url, params = parameters, timeout = 10)
        return(r)

    #  function to add single row
    def add(self, form, payload):
        url = self.baseUrl + '/' + self.owner + '/' + self.format_type + '/' + self.application_name + '/' + 'form/' + form + self.addUrl 
        headers = {**self.APIheader,**payload}
        r = requests.post(url, data = headers)
        return(r)
    
    #  function to delete based on condition
    def delete(self, form, condition):
        url = self.baseUrl + '/' + self.owner + '/' + self.format_type + '/' + self.application_name + '/' + 'form/' + form + self.deleteUrl 
        headers = {**self.APIheader,**{"criteria":condition}}
        r = requests.post(url, data = headers)
        return(r)
    
    # function to add many rows at once
    def rpcAdd(self, xml):
        payload =  {**self.RPCheader,**{"XMLString":xml}}
        r = requests.post(self.baseUrl + self.rpcUrl, data = payload)
        return(r)

    # function to create the xml structure for the rpc
    def createXmlFromDF(self, form, data, update_id = None):
        ### WARNING - MAX LENGTH OF AROUND 1million characters
        # Create a series of formatted xml strings of the data
        if update_id is None:
            xml_data = data.apply(self._pandas_xml_add, axis=1)
        else: 
            xml_data = data.apply(self._pandas_xml_update_id, axis=1, update_id=update_id)
        #Join every row in the series to create one long xml
        xml_data = xml_data.str.cat(sep='')
        # Add to the xml base format 
        xml = self.rpcBaseXML.format('"'+self.application_name+'"','"'+form+'"', xml_data)        
        return(xml)
    
    # function to create the xml structure for the rpc
    def createXmlFromDicts(self, form, data, update_id = None):
        ### WARNING - MAX LENGTH OF AROUND 1million characters
        # Create a series of formatted xml strings of the data
        if update_id is None:
            xml_data = map(self._pandas_xml_list_add,data)
        else: 
            xml_data = map(self._pandas_xml_list_update_id, data, [update_id]*len(data))
        #Join every row in the series to create one long xml
        xml_data = "".join(list(xml_data))
        # Add to the xml base format 
        xml = self.rpcBaseXML.format('"'+self.application_name+'"','"'+form+'"', xml_data)        
        return(xml)
    
    def _pandas_xml_add(self, row):
        data_xml = ['<add>']
        for field in row.index:
            if row[field] is not None and row[field]!='':
                data_xml.append('<field name="{0}"><value>{1}</value></field>'.format(field, row[field]))
        data_xml.append('</add>')
        return("".join(data_xml))

    def _pandas_xml_list_add(self, row):
        data_xml = ['<add>']
        for index in row.keys():
            if row[index] is not None and row[index]!='':
                data_xml.append('<field name="{0}"><value>{1}</value></field>'.format(index, row[index]))
        data_xml.append('</add>')
        return("".join(data_xml))
        
    def _pandas_xml_update_id(self, row, update_id):
        data_xml = [f'<update>']
        data_xml.append(f'<criteria>{update_id}=="{row[update_id]}"</criteria>')
        data_xml.append('<newvalues>')
        for field in row.index:
            if row[field] is not None and row[field]!='' and field != update_id:
                data_xml.append('<field name="{0}"><value>{1}</value></field>'.format(field, row[field]))
            elif row[index] is None:
                data_xml.append('<field name="{0}"><value></value></field>'.format(index, row[index]))
        data_xml.append('</newvalues>')
        data_xml.append('</update>')
        return("".join(data_xml))

    def _pandas_xml_list_update_id(self, row, update_id):
        data_xml = [f'<update>']
        data_xml.append(f'<criteria>{update_id}=="{row[update_id]}"</criteria>')
        data_xml.append('<newvalues>')
        for index in row.keys():
            if row[index] is not None and row[index]!='' and index != update_id:
                data_xml.append('<field name="{0}"><value>{1}</value></field>'.format(index, row[index]))
            elif row[index] is None:
                data_xml.append('<field name="{0}"><value></value></field>'.format(index, row[index]))
        data_xml.append('</newvalues>')
        data_xml.append('</update>')
        return("".join(data_xml))

    def rpcUploadChunk(self, data, form, update_on = None):
        # if data is list, use the function to create xml from list of dicts
        if type(data) is list:
            createXml = self.createXmlFromDicts
        # else if DF use the DF function
        elif type(y) is pd.core.frame.DataFrame:
            createXml = self.createXmlFromDF

        # if update then put the update criteria
        if update_on is not None:
            xml_string = createXml(form, data, update_on)
        else:
            xml_string = createXml(form, data)

        # call the upload xml chunk with check function to capture errors 
        response = self.rpcUploadWithCheck(xml_string)
        return(response)

    def rpcUploadWithCheck(self, xml):
        """ Function to upload dataframe to zoho 
                # GIVE XML
        
            # The XML RPC API can probably take up 1.4m. characters maximum.
            # Have to make sure the slice length use keeps each xml under this limit then
            # WRITE AN IF TO CHECK IF XMLSTRING < 1,350,000 TO ENSURE IT WILL WORK
            # the relationship depends on # columns, # columns with values etc.
            # The return value is a list, each value in the list is a JSON structured 
            # as {response:{status:<status_code>, text:<XMLresponse>}}
        """
        # Send query to Zoho
        rpc_request = self.rpcAdd(xml)
        
        # Standard output results 
        # First check if request is successfull
        if rpc_request.status_code==200:
            # Check if the response text has an error list and fail 
            if "errorlist" not in rpc_request.text:
                # Every row has a response and you can check it's status in the XML response
                root=ET.fromstring(rpc_request.text)
                for status in root.iter('status'):
                    if status.text != 'Success':
                        print("Failed with status: " + status.text)
                        gmail.quick_send(
                            to = 'ben@yellow.africa, ross@yellow.africa',
                            subject = f"Zoho sync event entry failed",
                            text = f"An entry was not successful in rpc response from Zoho. See AWS log for details <br>",
                        )
                        raise Exception("An entry was not successful in rpc response from Zoho")

            else:
                gmail.quick_send(
                    to = 'ben@yellow.africa, ross@yellow.africa',
                    subject = f"Zoho sync event entry failed",
                    text = f"Received errorlist in rpc response from Zoho. See AWS log for details <br>",
                )
                raise Exception("Received errorlist in rpc response from Zoho")

        else:
            print(rpc_request.text)
            print(f"Error {rpc_request.status_code}: see rpc request text for more detail")
            gmail.quick_send(
                to = 'ben@yellow.africa, ross@yellow.africa',
                subject = f"Zoho sync event entry failed",
                text = f"Upload Request failed. See AWS log for details <br>",
            )
            raise ValueError(f"Request failed with error code {rpc_request.status_code}")
        
        # If all is good, then process the IDs for return
        return(rpc_request.text)

    def zohoToDF(self,report_request, form_link):
        if report_request.status_code == 200:
            if "errorlist" in report_request.text.lower():
                raise Exception (f"Errorlist return in Zoho request")
            elif "no such view" in report_request.text.lower():
                raise Exception("No such table or form link")
            else:
                try:
                    form_json = json.loads(report_request.text) 
                except:
                    raise
        
        else:
            raise Exception (
        f"Request returned error code {report_request.status_code} in Zoho request for {form_link}"
        )

        # Convert JSON to pandas dataframe and return
        df = json_normalize(form_json[form_link])
        return(df)

    def xmlFindIDs(self, response, external_id):
        """ extract Zoho ID and External ID from RPC XML insert response """
        soup = BeautifulSoup(response, 'lxml')
        rows = []
        for result_xml in soup.find_all('result'):
            row = ([value_xml.value.string for 
                value_xml in result_xml.find_all(attrs={"name": ['ID',external_id]})
                ])
            rows.append(row)
        df = pd.DataFrame(rows, columns = [external_id,'ID'])
        return(df) 
        
