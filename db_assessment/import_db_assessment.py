# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Basic python built-in libraries to enable read, write and manipulate files in the OS
import json
import os
import glob
import sys
import pandas as pd
import datetime
# ct stores current time
ct = datetime.datetime.now()
import rules_engine as rengine
# Manages command line flags and arguments
import argparse

# Regular expression
import re

# Big Query Library Used to Import CSV files
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

client = None # Declare this at the top after import statements

# Setting client info for Google APIs
import set_client_info

# Rules engine
import rules_engine

# Importing Optimus Prime Version
import version

# Information for analytics and tool improvement
__version__= version.__version__

# Messages handling
import logging
logging.getLogger().setLevel(level=logging.INFO)

# Beautiful table 
from beautifultable import BeautifulTable

# pandas for dataframe 
import pandas as pd
import numpy as np 

#For processing of Beautiful Table Data
import sqlite3



def get_bigqueryClient():
    global client
    if not client:
        client = bigquery.Client(client_info=set_client_info.get_http_client_info())
    return client

def getVersion():

    return __version__


def consolidateLos(args, transformersTablesSchema):

# This function intents to consolidate the collected files into a single large file to facilidate importing the data to Big Query

    # Creating Hash Table with all expected tableName schemas to be imported
    tableSchemas = {}
    tableSchemas = getBQJobConfig(transformersTablesSchema, 'REGULAR')

    # Counting all processed files
    fileCounter = 0

    # For all expected tables we will look for related OS files. So, we will process all files related to a given expected tableName, then move to the next
    for tableName in tableSchemas:

        fileCounter = fileCounter + 1

        # Using the expected tableName to look for files in the OS in the directory passed in -fileslocation (default dbResults)
        csvFilesLocationPattern = str(getattr(args,'fileslocation')) + '/opdb*' + str(tableName) + '*.log'

        # Generating a list with all found OS filenames
        fileList = getAllFilesByPattern(csvFilesLocationPattern)

        # To control how many files are being processed and identify the first processed file since it needs to bring the headers
        fileTableCounter = 0

        # Processing one file at a time for the expected tableName
        for fileName in fileList:

            # File Counter
            fileTableCounter = fileTableCounter + 1

            # Final table name from the CSV file names
            tableName = getObjNameFromFiles(fileName,'__',1)

            # Filename to be used to name consolidated file
            targetFileNameConsolidated = str(getattr(args,'fileslocation')) + '/opalldb__' + str(tableName) + '__consolidate.log'

            # Checks if file already exists in the first matching file found because the other files need to append to existent one.
            if fileTableCounter == 1:

                # If already exists delete the file
                if os.path.exists(targetFileNameConsolidated):
                    
                    print('The file {} already exists. It is going to be overwritten.'.format(targetFileNameConsolidated))
                    os.remove(targetFileNameConsolidated)

            # This is the file that will be used to be consolidated
            fileConsolidated = open(targetFileNameConsolidated,'a')

            # This file was found in the OS. The content of this file will be merged/consolidated into fileConsolidated
            fileToBeConsolidated = open(fileName, 'r')

            # Breaking it down into lines because first two lines must be skipped for all of the files (expect first file merged)
            # Since those files are expected to be small (< 10k lines) no performance issue is expected
            linesToBeConsolidated = []
            linesToBeConsolidated = fileToBeConsolidated.readlines()

            # To control how many lines are being processed and identify the first processed lines since it needs to skip it eventually
            lineCounter = 0
            for line in linesToBeConsolidated:

                # Line counters to be used to skip unecessary lines
                lineCounter = lineCounter + 1

                # Not processing first lines due to expected CSV headers. Except for the first file.
                if lineCounter <= 2 and fileTableCounter > 1:
                    
                    continue
                
                # Writting up the line from linesToBeConsolidated into fileConsolidated
                fileConsolidated.write(line)


            # Closing file handle
            fileToBeConsolidated.close()

            # Closing file handle
            fileConsolidated.close()

    print ('\nThe total files consolidated are {}. \nAll files are located in {}'.format(str(fileCounter),str(getattr(args,'fileslocation'))))

    return True


def createOptimusPrimeViewsTransformers(gcpProjectName,bqDataset,view_name,view_query):
# This function intents to create all views found in the opViews directory. The views creation must follow opConfig/transformers.json

    client = bigquery.Client()

    if gcpProjectName is None:
        # In case projectname is not provided in the arguments
        view_id = str(client.project) + '.' + str(bqDataset) + '.' + view_name
        gcpProjectName = str(client.project)
    else:
        # If projectname is provided in the arguments
        view_id = str(gcpProjectName) + '.' + str(bqDataset) + '.' + view_name
    
    # Creating the JOB to create view in Big Query
    view = bigquery.Table(view_id)

    # Extracting the view text and replacing the string ${dataset}/${projectname} by the proper dataset independent of case sensitive
    pattern = re.compile(re.escape('${dataset}'), re.IGNORECASE)
    view_query = pattern.sub(str(bqDataset), view_query)
    pattern = re.compile(re.escape('${projectname}'), re.IGNORECASE)
    view_query = pattern.sub(str(gcpProjectName), view_query)
    #source_id = 'optimusprime-migrations.consolidate_test.dbsummary'
    #view_query = f"SELECT pkey, dbid FROM `{source_id}`"

    view.view_query = view_query

    try:
        # Make an API request to create the view.
        view = client.create_table(view)
        print("Created {}: {}".format(view.table_type,str(view.reference)))
        print("\n")
    except Conflict as error:
        print("View {} already exists.\n".format(str(view.reference)))
        #view = client.update_table(view, ['view_query'])
        return False
    except:
        print("View {} count not be created. See DDL below:\n".format(str(view.reference)))
        print(view_query)
        return False

    return True

def createOptimusPrimeViewsFromOS(gcpProjectName,bqDataset):
# This function intents to create all views found in the opViews directory. The views creation must follow opViews/<filename> order

    #print ('\nPreparing to create Optimus Prime SQL Views\n')
    
    # store all files found in the OS
    fileList = []

    # Searching for all matching files in the default views location
    filePattern = 'opViews/optimus_createView*.sql'

    # List with all views to be created
    fileList = getAllFilesByPattern(filePattern)
    
    if len(fileList) == 0:
        #print('\nWARNING: No views found to be created at expected location: {}. Please make sure you the location is correct.'.format(filePattern))
        # Returns False if cannot create views    
        return False
    
    else:

        client = bigquery.Client()

        # Sorting list to make sure the proper view creation
        fileList.sort()

        # Looping to iterate all view files found in the OS to be created. Also, to extract the proper view name out of them.
        for viewFileName in fileList:

            # Extracting the proper view name to be created in Big Query based out of OS view filename
            view_name = str(getObjNameFromFiles(viewFileName,'__',1)).replace('.sql','')

            print ('Preparing to process {} and create the view name {}'.format(viewFileName,view_name))

            
            if gcpProjectName is None:
                # In case projectname is not provided in the arguments
                view_id = str(client.project) + '.' + str(bqDataset) + '.' + view_name
            else:
                # If projectname is provided in the arguments
                view_id = str(gcpProjectName) + '.' + str(bqDataset) + '.' + view_name
            
            # Creating the JOB to create view in Big Query
            view = bigquery.Table(view_id)

            # Extracting the view text and replacing the string ${dataset} by the proper dataset
            with open(viewFileName, "r") as view_content:
                view.view_query = view_content.read().replace('${dataset}',str(bqDataset))

            try:
                # Make an API request to create the view.
                view = client.create_table(view)
                print("Created {}: {}".format(view.table_type,str(view.reference)))
                print("\n")
            except Conflict as error:
                print("View {} already exists.\n".format(str(view.reference)))


        return True 
    

def getAllFilesByPattern(filePattern):
# This function intends to get the name of all files in the OS and return a list of strings

    # Get all matching files and creates a list returning it   
    return glob.glob(filePattern)

def importAllDataframeToBQ(args,gcpProjectName,bqDataset,transformersTablesSchema,dbAssessmentDataframes,transformersParameters,importresults):

    # Tracking tableNames Imported to Big Query
    tablesImported = {}

    if args.fromdataframe:

        print ('\nPreparing to import DATAFRAMES to BigQuery\n')

        # Creating Hash Table with all expected table schemas to be imported
        tableSchemas = {}
        
        # Always AUTO because we never know the column order in which the dataframe will be
        #transformersTablesSchema = rules_engine.processSchemaDetection('AUTO',transformersTablesSchema, None, str(tableName).lower(), df)
        
        #tableSchemas = getBQJobConfig(transformersTablesSchema,'DATAFRAME')



        for tableName in dbAssessmentDataframes:

            print ('\nThe dataframe {} is being imported to Big Query.'.format(tableName))

            if str(tableName).lower() in transformersParameters["do_not_import"]:

                print ('Table name {} is being SKPIPED accordingly with transformers.json do_not_import parameter')

                continue

            if str(tableName).lower()  =="opkeylog":
                df = dbAssessmentDataframes[tableName]
                df['CMNT']= transformersParameters['importcomment']
                df['LOADTOBQDATE']= ct
                df['JOBPARAMS'] = str(vars(args))

            # Import the given CSV fileName into
            sucessImport,importresults = importDataframeToBQ(gcpProjectName,bqDataset,str(tableName).lower(),tableSchemas,dbAssessmentDataframes[tableName],transformersParameters,args,importresults)
            if sucessImport:
                tablesImported[str(tableName).lower()] = "IMPORTED_FROM_DATAFRAME"


        return True, tablesImported,importresults

    else:

        return False, tablesImported,importresults


def importDataframeToBQ(gcpProjectName,bqDataset,tableName,tableSchemas,df,transformersParameters,args,importresults):

    # Getting table schema
    try:

        # in case there is nothing to be imported
        if str(tableName).lower() in transformersParameters["do_not_import"]:

            return True

        # Creating Hash Table with all expected table schemas to be imported
        tableSchemas = {}
        transformersTablesSchemaDataframe = {}
        

        dfColumns = df.columns
        dfNewColumns = []

        # Changing column names that are not supported in Big Query.
        # Ideally this fix should be in the collection script
        for column in dfColumns:

            column = column.replace('(1)','')
            column = column.replace('(X=5%)','')
            column = column.replace('#','')

            dfNewColumns.append(column)

        df.columns = dfNewColumns

        # Always AUTO because we never know the column order in which the dataframe will be
        transformersTablesSchemaDataframe = rules_engine.processSchemaDetection('FILLGAP',transformersTablesSchemaDataframe, None, str(tableName).lower(), df)
        
        tableSchemas = getBQJobConfig(transformersTablesSchemaDataframe,'DATAFRAME')


        schema = tableSchemas[str(tableName).lower()]

    except KeyError:
        # In case there is not expected table schema found in getBQJobConfig function
        print ('\nWARNING: The dataframe "{}" could not be imported to Big Query.'.format(tableName))
        print ('The table name "{}" cannot be imported because it does not have table schema in transformers.json. So, it will be skipped.\n'.format(tableName))
        importresults=populateBT(tableName,df,'importDataframeToBQ','isFile','fromimportDataframeToBQ',-1,importresults,args)
        return False,importresults

    try:
        df = df.astype(str)
    except:
        print ('\nWARNING: The dataframe "{}" could not be converted to STRING.'.format(tableName))

    if str(tableName).lower() =="opkeylog":
        # Construct a BigQuery client object with API Call to track Tool usage
        client = bigquery.Client(client_info=set_client_info.get_http_client_info())
    else:
        client = bigquery.Client()

    # Adding Project and Dataset based on arguments 
    # table_id to the ID of the table to create.
    if gcpProjectName is not None:
        table_id = str(gcpProjectName) + '.' + str(bqDataset) + '.' + str(tableName)
    
    # In case projectname was passed as argument. Then, it tries to get the default project for the [service] account being used
    else:
        table_id = str(client.project) + '.' + str(bqDataset) + '.' + str(tableName)

    # Changed default to from WRITE_TRUNCATE to WRITE_APPEND in args.loadtype. 
    write_disposition=str(args.loadtype).upper()
    schema_updateOptions=[]
    file_format=bigquery.SourceFormat.CSV
    if str(tableName).lower() =="opkeylog":
        ## OpkeyLog is a load stats table so rows would be appended and if any schema change is there, the update of schema would be allowed
        schema_updateOptions = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        write_disposition="WRITE_APPEND"

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=schema,
        schema_update_options = schema_updateOptions,
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition=write_disposition
        #,
        #field_delimiter = ";",
        #source_format = file_format
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

    importresults=populateBT(tableName,df,'importDataframeToBQ','isFile','fromimportDataframeToBQ',-1,importresults,args)

    # Returns True if sucessfull 
    return True,importresults

def adddetails(fileName,args,params,tableHeader):
    df = pd.read_csv(fileName, sep=str(args.sep), skiprows=2, na_values='n/a', keep_default_na=True, skipinitialspace = True, names = tableHeader, index_col=False)
    if params['importcomment']:
        df["CMNT"] = params['importcomment']
    df['LOADTOBQDATE']= ct
    df['JOBPARAMS'] = str(vars(args))
    df.to_csv(fileName,index=False, sep=str(args.sep))
    line=""
    with open(fileName, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)

def importAllCSVsToBQ(gcpProjectName,bqDataset,fileList,transformersTablesSchema,skipLeadingRows,transformersParameters,args,importresults):
# This function receives a list of files to import to Big Query, then it calls importCSVToBQ to import table/file by table/file

    print ('\nPreparing to upload CSV files\n')

    # Creating Hash Table with all expected table schemas to be imported
    tableSchemas = {}
    tableSchemas = getBQJobConfig(transformersTablesSchema, 'REGULAR')

    fileList.sort()

    # Getting the name of the target table_name to import the data based on the filename from OS
    for fileName in fileList:
        
        # Default Big Query Job Configurations for Optimus Prime CSV files
        autoDetect = 'True'

        # Final table name from the CSV file names
        tableName = getObjNameFromFiles(fileName,'__',1)

        importTable = True
        doNotImportList = [table.strip().lower() for table in transformersParameters['do_not_import']]

        if str(tableName).lower()  =="opkeylog":
            ##skipLeadingRows=1
            tableHeaders = rengine.getDFHeadersFromTransformers(str(tableName).lower(),transformersTablesSchema)
            tableHeader = [header.upper() for header in tableHeaders]
            adddetails(fileName,args,transformersParameters,tableHeader)

        if tableName.lower() not in doNotImportList:

            # Import the given CSV fileName into 
            print ('\nThe filename {} is being imported to Big Query.'.format(fileName))

            sucessImport, importresults=importCSVToBQ(gcpProjectName,bqDataset,tableName,fileName,skipLeadingRows,autoDetect,tableSchemas,args,importresults)

        else:

            print ('\nThe filename {} is being SKIPPED accordingly with parameter {} from transformers.json.'.format(fileName,'do_not_import'))
            
    return True,importresults

def importCSVToBQ(gcpProjectName,bqDataset,tableName,fileName,skipLeadingRows,autoDetect,tableSchemas,args,importresults):
# This function will import the CSV file into the Big Query using the proper project.dataset.tablename
# A Big Query Job is created for it

    # Getting table schema
    try:
        schema = tableSchemas[tableName]
    except KeyError:
        # In case there is not expected table schema found in getBQJobConfig function
        print ('\nWARNING: The filename "{}" could not be imported to Big Query.'.format(fileName))
        print ('The table name "{}" cannot be imported because it does not have table schema in transformers.json. So, it will be skipped.\n'.format(tableName))
        return False

    if str(tableName).lower() =="opkeylog":
        # Construct a BigQuery client object with API Call to track Tool usage
        client = bigquery.Client(client_info=set_client_info.get_http_client_info(), project=gcpProjectName)
    else:
        # Construct a BigQuery client object.
        client = bigquery.Client(project=gcpProjectName)


    # Adding Project and Dataset based on arguments 
    # table_id to the ID of the table to create.
    if gcpProjectName is not None:
        table_id = str(gcpProjectName) + '.' + str(bqDataset) + '.' + str(tableName)
    
    # In case projectname was passed as argument. Then, it tries to get the default project for the [service] account being used
    else:
        table_id = str(client.project) + '.' + str(bqDataset) + '.' + str(tableName)
    
    schema_updateOptions=[]
    field_delimiter = str(args.sep)
    write_disposition = str(args.loadtype).upper()
    
    if str(tableName).lower() == "opkeylog":
        ## OpkeyLog is a load stats table so rows would be appended and if any schema change is there, the update of schema would be allowed
        schema_updateOptions = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
   
   # OP Internal Configuration Files
    elif str(tableName).lower() in ("optimusconfig_bms_machinesizes","optimusconfig_network_to_gcp"):
        write_disposition = "WRITE_TRUNCATE"
        field_delimiter = ","

    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=skipLeadingRows,
        schema_update_options = schema_updateOptions,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter = field_delimiter,
        write_disposition = write_disposition
    )


    with open(fileName, "rb") as source_file:

        try:
            load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        except Exception as importErr:
            print ('\n FAILED: Optimus Prime could not import the filename "{}" into "{}" because of the error "{}".\n'.format(fileName,table_id,importErr))

            print ('   Table Schema = {}'.format(schema))

            importresults=populateBT(tableName,'isFile','importDataframeToBQ',fileName,'fromimportCSVToBQ',-1,importresults,args)

            return False,importresults

    try:
        load_job.result()  # Waits for the job to complete.
    except Exception as genericLoadErr:
        print ('\n FAILED: Optimus Prime could not import the filename "{}" into "{}" because of the error "{}".\n'.format(fileName,table_id,genericLoadErr))
        importresults=populateBT(tableName,'isFile','importDataframeToBQ',fileName,'fromimportCSVToBQ',-1,importresults,args)
        return False,importresults

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows into: {}".format(destination_table.num_rows,destination_table.reference))

    importresults=populateBT(tableName,'isFile','importDataframeToBQ',fileName,'fromimportCSVToBQ',destination_table.num_rows,importresults,args)

    # returns True if processing is successfully
    return True,importresults


def getTableRef(dataset,tableName,projectName):
    
    if projectName:
        return f"{projectName}.{dataset}.{tableName}"

    return  f"{client.project}.{dataset}.{tableName}"

def getObjNameFromFiles(fileName,splitterChar,pos):
    # This function returns a string based on a string splitted(Created a list) by a given character. Then, it returns the desired index position of the list.

    #return fileName.split(splitterChar)[pos]
    splits = fileName.split(splitterChar)

    if len(splits) >= pos:
        
        return splits[pos]
    
    return None    


def getBQJobConfig(tableSchemas,jobType):
    
    bqTablesJobConfig = {}

    for tableName in tableSchemas:

        bqTablesJobConfig[tableName] =  []

        for schemaField in tableSchemas[tableName]:
        
            if jobType == 'REGULAR':
                
                bqTablesJobConfig[tableName].append(bigquery.SchemaField(str(schemaField[0]), str(schemaField[1])))

            elif jobType == 'DATAFRAME':
                
                #bqTablesJobConfig[tableName].append(bigquery.SchemaField(str(schemaField[0]).upper(), 'bigquery.enums.SqlTypeNames.' + str(schemaField[1])))
                bqTablesJobConfig[tableName].append(bigquery.SchemaField(str(schemaField[0]).upper(), str(schemaField[1])))
    
    return bqTablesJobConfig


def createDataSet(datasetName,gcpProjectName):
# Always try to create the dataset

    # Construct a BigQuery client object.
    client = bigquery.Client(project=gcpProjectName)
    if gcpProjectName is None:
        # In case the user did NOT pass the project name in the arguments
        dataset_id = "{}.{}".format(client.project,datasetName)
    else:
        # In case tge use DID pass the project name in the arguments
        dataset_id = "{}.{}".format(gcpProjectName,datasetName)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location =  client.location

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    try:
        dataset = client.create_dataset(dataset)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
        
    except Conflict as error:
        # If dataset already exists
        print('Dataset {} already exists.'.format(dataset_id))

def deleteDataSet(datasetName,gcpProjectName):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set dataset_id=datasetName to the ID of the dataset to create.
    if gcpProjectName is None:
        # In case the user did NOT pass the project name in the arguments
        dataset_id = "{}.{}".format(client.project,datasetName)
    else:
        # In case tge use DID pass the project name in the arguments
        dataset_id = "{}.{}".format(gcpProjectName,datasetName)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location =  client.location

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    try:
        dataset = client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)  # Make an API request.
        print("Deleted dataset {}".format(dataset_id))
        
    except Conflict as error:
        # If dataset already exists
        print('Failed to delete dataset {}.'.format(dataset_id))

def insertErrors(invalidfiles,op_df,gcpProjectName,bq_dataset):
    from google.cloud.exceptions import NotFound
    tableid = "operrors"
    try:
        pkey = op_df['PKEY'].iloc[0]
        bq_client = bigquery.Client()
        try:
            table = bq_client.get_table("{}.{}.{}".format(gcpProjectName,bq_dataset,tableid))
        except NotFound:
            schema = [
                bigquery.SchemaField("PKEY", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("LOADDATE", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("FILENAME", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("ERROR", "STRING", mode="REQUIRED"),
            ]
            table = bigquery.Table(gcpProjectName+"."+bq_dataset +"."+tableid , schema=schema)
            table = bq_client.create_table(table)  # Make an API request.
        rows =[]
        for filename, error in invalidfiles.items():
            basename =  os.path.basename(filename)
            rows_to_insert = {u"PKEY": pkey, u"LOADDATE": str(ct), u"FILENAME": basename, u"ERROR": error}
            rows.append(rows_to_insert)
        errors = bq_client.insert_rows_json(table, rows)
    except Exception as pushErr:
        print ('\nWARNING: Issues while pusing Errors into operrors table with error ', pushErr)


def populateBT(tableName,df,dataframeornot,invalidfiles,btsource,rowsimported,importresults,args):
    # Fuction to populate the importresults list which will be used to print using Beautiful Table 
    # rowsimported of <0 is used to indicate a FAILED status 
    tmpdataFrame=pd.DataFrame()

    if 'opConfig/' in invalidfiles:
        return importresults
    
    if btsource=='invalidfiles': # when called from runMain
            for fileName, error in invalidfiles.items():
                tmpdataFrame=pd.DataFrame() 
                tmpdataFramedict = {"Target Table":getObjNameFromFiles(fileName,'__',1),"Distinct Pkey":getObjNameFromFiles(fileName,'__',2),"Import Status":"FAILED","Loaded rows":0}
                tmpdataFrame = tmpdataFrame.append(tmpdataFramedict, ignore_index = True)
                if len(tmpdataFrame) >0:
                    importresults = pd.concat([importresults, tmpdataFrame], ignore_index = True, axis = 0)
    else:
        if args.fromdataframe:  # when called from importDataframeToBQ
            if dataframeornot is not None:
                if 'PKEY' in df.columns.to_list():
                    df.reset_index(drop=True,inplace=True) 
                    pkeygroupby=df.groupby(['PKEY']).size()
                    pkeycount=pkeygroupby.to_dict()
                    for pkeyname,pkeyname_rowcount in pkeycount.items():
                        tmpdataFrame=pd.DataFrame() 
                        tmpdataFramedict = {"Target Table":tableName,"Distinct Pkey":pkeyname,"Import Status":"SUCCESS","Loaded rows":pkeyname_rowcount}
                        tmpdataFrame = tmpdataFrame.append(tmpdataFramedict, ignore_index = True)  
                        if len(tmpdataFrame) >0:
                            importresults = pd.concat([importresults, tmpdataFrame], ignore_index = True, axis = 0)

        else:
            fileName=invalidfiles  # when called from importCSVToBQ

            if 'opdbt' not in fileName: 
                if rowsimported >=0:
                    if len(importresults)==0:
                        tmpdataFrame=pd.DataFrame() 
                        tmpdataFramedict = {"Target Table":tableName,"Distinct Pkey":getObjNameFromFiles(fileName,'__',2),"Import Status":"SUCCESS","Loaded rows":rowsimported}
                        tmpdataFrame = tmpdataFrame.append(tmpdataFramedict, ignore_index = True) 
                    else:
                        if tableName in importresults['Target Table'].values and  'SUCCESS' in importresults['Import Status'].values:
                            # this is needed as bq functions check for rows already loaded and not the new ones only
                            ExistingRowsInDataframe=importresults[importresults['Target Table'].str.contains(tableName ) & importresults['Import Status'].str.contains("SUCCESS")]['Loaded rows'].sum()
                            newrows4dataframe=rowsimported-ExistingRowsInDataframe 

                            tmpdataFrame=pd.DataFrame() 
                            tmpdataFramedict = {"Target Table":tableName,"Distinct Pkey":getObjNameFromFiles(fileName,'__',2),"Import Status":"SUCCESS","Loaded rows":newrows4dataframe}
                            tmpdataFrame = tmpdataFrame.append(tmpdataFramedict, ignore_index = True)
                        else:
                            tmpdataFrame=pd.DataFrame() 
                            tmpdataFramedict = {"Target Table":tableName,"Distinct Pkey":getObjNameFromFiles(fileName,'__',2),"Import Status":"SUCCESS","Loaded rows":rowsimported}
                            tmpdataFrame = tmpdataFrame.append(tmpdataFramedict, ignore_index = True)  

                else:
                    tmpdataFrame=pd.DataFrame() 
                    tmpdataFramedict = {"Target Table":tableName,"Distinct Pkey":getObjNameFromFiles(fileName,'__',2),"Import Status":"FAILED","Loaded rows":0}
                    tmpdataFrame = tmpdataFrame.append(tmpdataFramedict, ignore_index = True)

                if len(tmpdataFrame) >0:
                    importresults = pd.concat([importresults, tmpdataFrame], ignore_index = True, axis = 0)
  
    
    return importresults


def printBTResults(importresults):
    # Fuction to print the import logs present in  btImportLogTable /btImportLogFinalTable

    #Create and load the output bt table
    btImportLogFinalTable = BeautifulTable()
    btImportLogFinalTable = BeautifulTable(maxwidth=300)
    btImportLogFinalTable.columns.header = ["Target Table","Distinct Pkey","Import Status","Loaded rows"]

    # To group by table name, import status, count of distinct pkeys and sum of rows 
    importresultsagg=importresults.groupby(["Target Table","Import Status"])['Loaded rows'].agg(['size','sum']).reset_index(drop=False)
    importresultsfinal=importresultsagg.rename(columns={'size':'Distinct Pkey','sum':'Loaded rows'})

    #convert float type to int type 
    importresultsfinal['Loaded rows'] = importresultsfinal['Loaded rows'].astype(int)

    #swap for correcting to match the expected order of columns 
    importresultsfinal=importresultsfinal[["Target Table","Distinct Pkey","Import Status","Loaded rows"]]

    #insert into beautiful table 
    for index, row in importresultsfinal.iterrows():
        btImportLogFinalTable.rows.append(row)

    btImportLogFinalTable.set_style(BeautifulTable.STYLE_BOX_ROUNDED)
    print('\n\n Import Completed....\n')
    print('\n Import Summary \n\n')
    print(btImportLogFinalTable)
    
