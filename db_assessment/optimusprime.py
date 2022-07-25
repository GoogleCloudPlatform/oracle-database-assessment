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
import sys

# Manages command line flags and arguments
import argparse

# Setting client info for Google APIs
import set_client_info

# Import to Big Query
import import_db_assessment

# Rules engine
import rules_engine

# Importing Optimus Prime Version
import version

# Import remote functionality
from remote import runRemote

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

def getVersion():

    return __version__


def runMain(args):
# Main function

    # Pre-Tasks before trying to import any data

    # STEP: Readin JSON file configuration (rules and parameters)

    # Import Json with parameters and rules
    transformerConfiguration = rules_engine.getRulesFromJSON(str(args.transformersconfig))

    # Assigning the rules and parameter {} variables
    transformerRulesConfig = {}
    transformerRulesConfig = transformerConfiguration['rules']
    transformersParameters = {}
    transformersParameters = transformerConfiguration['parameters']
    transformersTablesSchema = None
    transformersTablesSchemaConfig = {}
    transformersTablesSchemaConfig = transformerConfiguration['tableschemas']

    # SM: 04/28/2022:[Bug#90] If we only intended to run the process for recreating views
    transformersParameters['recreateviews'] = True # setting false by default, will set true if Recreateview = YES
    if str(args.recreateviews).upper() == 'YES':

        gcpProjectName = str(args.projectname)
        bqDataset = str(args.dataset)

        if import_db_assessment.checkDataSetExists(bqDataset, gcpProjectName):
            
            transformersParameters['dbversion'] = str(args.dbversion)
            transformersParameters['optimuscollectionversion'] = str(args.collectionversion)
            transformersParameters['recreateviews'] = True # setting this as parameter to that we can handle recreation in import_db_assesment.createOptimusPrimeViewsTransformers. If already exists, it returns error by default
            rulesAlreadyExecuted = [] # keeping it, since it is evaluated in rules engine. although keeping it blank for now as for recreate views this should not impact
            print ('\n\n Recreating views from project {} and BigQuery dataset {}\n\n'.format(gcpProjectName, bqDataset))
            viewTransformerConfiguration = {}
            viewTransformerConfiguration = {rule:config for rule, config in transformerRulesConfig.items() if "create-view" in rule}
            # Create Optimus Prime Views
            # tested with vReport_sysstat_io_summary. added new column iops_total_perc100_2. Existing View: OK, New View: OK
            transformerParameterResults, transformersRulesVariables, fileList, dbAssessmentDataframes = rules_engine.runRules("2",viewTransformerConfiguration, None, None, args, None, None, None, rulesAlreadyExecuted, transformersParameters, gcpProjectName, bqDataset)

            print ('\n\n Views created. Thank YOU for using Optimus Prime!\n\n')
        else:
            print ('\n\n Error: Dataset {}.{} does not exist!\n\n'.format(gcpProjectName, bqDataset))

    # For all cases in which those attributes are <> None it means the user wants to import data to Big Query
    # No need to further messaging for mandatory options because this is being done in argumentsParser function
    if args.dataset is not None and args.collectionid is not None:


        # This is broken needs to be fixed in upcoming versions
        if args.consolidatelogs:
            # It is True if no fatal errors were found
            resConsolidation = import_db_assessment.consolidateLos(args,transformersTablesSchema)

        # STEP 1: Import customer database assessment data

        # Optimus Prime Search Pattern to find the target CSV files to be processed
        # The default location will be dbResults if not overwritten by the argument -fileslocation
        csvFilesLocationPattern = str(args.fileslocation) + '/*' + str(args.collectionid).replace(' ','') + '.log'

        # Append csvFilesLocationPattern if there are filterbysqlversion and/or filterbydbversion flag
        if args.filterbysqlversion and args.filterbysqlversion is not None:
            csvFilesLocationPattern = csvFilesLocationPattern.replace(str(args.fileslocation) + '/*',str(args.fileslocation) + '/*_' + str(args.filterbysqlversion) + '*')

        fileList = []
        if args.filterbydbversion and args.filterbydbversion is not None:
            for dbversion in args.filterbydbversion.split(","):
                dbversion = dbversion.replace(".","")
                newcsvFilesLocationPattern = csvFilesLocationPattern.replace(str(args.fileslocation) + '/*',str(args.fileslocation) + '/*__' + dbversion + '*')
                fileListfordbversion = import_db_assessment.getAllFilesByPattern(newcsvFilesLocationPattern)
                fileList.extend(fileListfordbversion)
        else:
            # Getting a list of files from OS based on the pattern provided
            # This is the default directory to have all customer database results from oracle_db_assessment.sql
            fileList = import_db_assessment.getAllFilesByPattern(csvFilesLocationPattern)

        skipvalidations = False
        if args.skipvalidations and args.skipvalidations is not None:
            skipvalidations = True

        # In case there is no matching file in the OS
        if len(fileList) == 0:
            sys.exit('\nERROR: There is not matching CSV file found to be processed using: {}\n'.format(csvFilesLocationPattern))

        #  Make sure there are not 11.2 or 11.1 database versions being imported along with other database versions.
        dbversionslist = set([f.split("__")[2].split("_")[0] for f in fileList])
        outliers = len([version for version in dbversionslist if version not in ['111','112']])
        if ("111" in dbversionslist or "112" in dbversionslist) and outliers > 0:
            sys.exit('\nERROR:  Importing other versions along with 11.1 and 11.2 is not supported. Please use flag fileterbydbversion to filter database versions, For example: -filterbydbversion "12.1,12.2,18.0,19.1"\n')

        sqlversionslist = set([f.split("__")[2].split("_")[1] for f in fileList])
        if len(sqlversionslist) > 1:
            sys.exit('\nERROR:  Importing multiple SQL versions is not supported. Please use flag fileterbysqlversion to filter SQL versions, For example: -filterbysqlversion 2.0.3"\n')

        # Getting file pattern for find config files in the OS to be imported
        csvFilesLocationPatternOPConfig = 'opConfig/*.csv'

        # Getting a list of files from OS based on the pattern provided
        fileListOPConfig = import_db_assessment.getAllFilesByPattern(csvFilesLocationPatternOPConfig)

        # Variable to track the collection id. To be used mostly when new CSV files are generated from processing rules
        collectionKey = import_db_assessment.getObjNameFromFiles(str(fileList[0]),'__',2)
        transformersParameters['collectionKey'] = collectionKey

        # Verify if the script has any version on it (only old script versions should not have 3 parts)
        if args.dbversion is not None:
            transformersParameters['dbversion'] = str(args.dbversion)
        elif len(collectionKey.split('_')) >= 3 and args.dbversion is None: # bug #23. Changed == to >=.
            transformersParameters['dbversion'] = import_db_assessment.getObjNameFromFiles(collectionKey,'_',0)
        else:
            print ('\nFATAL ERRROR: Please use -dbversion and -collectionversion. \nI.E -dbversion 122 -collectionversion 2.0.3\n')
            sys.exit()

        if args.importcomment is not None:
            transformersParameters['importcomment'] = str(args.importcomment)

        if len(collectionKey.split('_')) >= 3: # bug #23. Changed == to >=.
            transformersParameters['optimuscollectionversion'] = import_db_assessment.getObjNameFromFiles(collectionKey,'_',1)
        else:
            transformersParameters['optimuscollectionversion'] = args.collectionversion

        # If this valus is set it has precende over everything else
        if args.collectionversion != '0.0.0':
            transformersParameters['optimuscollectionversion'] = args.collectionversion

        try:
            # Automatically try to select the right file separator accordingly with the SQL Script version
            if int(str(transformersParameters['optimuscollectionversion']).replace('.','')) < 205:
                args.sep = ","
        except:
            None

        print('\nSource Database Version: {} \nCollection Script Version: {}\n'.format(transformersParameters['dbversion'],transformersParameters['optimuscollectionversion']))

        try:
            # Adjusting the tableschemas from transformers.json accordingly with the database version
            for dbVersion in transformersTablesSchemaConfig[transformersParameters['optimuscollectionversion']].keys():

                if transformersParameters['dbversion'] in dbVersion:
                    transformersTablesSchema = transformersTablesSchemaConfig[transformersParameters['optimuscollectionversion']][dbVersion]

            # If we could not find any matching for tableSchemas
            if transformersTablesSchema is None:
                print('\n FAILURE: Optimus Prime could not find in transformers.json matching for table schema configuration for "optimuscollectionversion={}" and "dbversion={}"\n'.format(transformersParameters['optimuscollectionversion'],transformersParameters['dbversion']))
                sys.exit()
        except KeyError:
            print('\n FAILURE: Optimus Prime could not find in transformers.json matching for table schema configuration for "optimuscollectionversion={}" and "dbversion={}"\n'.format(transformersParameters['optimuscollectionversion'],transformersParameters['dbversion']))
            sys.exit()
        
        # Import the CSV files into Big Query
        gcpProjectName = args.projectname
        bqDataset = str(args.dataset)
        
        # Delete the dataset before importing new data
        if args.deletedataset:
            if args.projectname is not None:
                import_db_assessment.deleteDataSet(bqDataset,gcpProjectName)
            else:
                sys.exit('\nWARNING: The database {} will not be deleted because the option -projectname is omitted. \nPlease try again either providing -projectname OR removing -deletedataset.\n\n'.format(args.deletedataset))
        
        # Create the dataset to import the CSV data
        import_db_assessment.createDataSet(bqDataset,gcpProjectName)

        


        # STEP: Processing parameters which create internal variables(transformersParameters) to be used in later stages

        #####transformerParameterResults, transformersParameters = rules_engine.runRules(transformerRulesConfig, None, None)

        # STEP: Loading all CSV files in memory into dataframes

        dbAssessmentDataframes = {}
        invalidfiles = {}
        dbAssessmentDataframes, transformersTablesSchema = rules_engine.getAllDataFrames(fileList, 1, collectionKey, args, transformersTablesSchema, dbAssessmentDataframes, transformersParameters,invalidfiles,skipvalidations)
        dbAssessmentDataframes, transformersTablesSchema = rules_engine.getAllDataFrames(fileListOPConfig, 0, collectionKey, args, transformersTablesSchema, dbAssessmentDataframes, transformersParameters,invalidfiles,skipvalidations)

        # STEP: Reshape Dataframes when necessary based on the transformersParameters

        dbAssessmentDataframes, fileList, transformersTablesSchema, rulesAlreadyExecuted = rules_engine.getAllReShapedDataframes(dbAssessmentDataframes, transformersTablesSchema, transformersParameters, transformerRulesConfig, args, collectionKey, fileList)

        # STEP: Run rules engine

        transformerParameterResults, transformersRulesVariables, fileList, dbAssessmentDataframes = rules_engine.runRules("1",transformerRulesConfig, dbAssessmentDataframes, None, args, collectionKey, transformersTablesSchema, fileList, rulesAlreadyExecuted, transformersParameters, gcpProjectName, bqDataset)


        # STEP: Import ALL data to Big Query
        # Local Variable store to avoid Global parameters
        importresults=pd.DataFrame()

        # Eliminating duplicated entries from transformers.json processing
        fileList = list(set(fileList))
        if len(invalidfiles)>0:
            print("Below are Invalid Files \n")
            [print(key,':',value) for key, value in invalidfiles.items()]
            fileList  = [file for file in fileList if file not in invalidfiles.keys()]
            ## Insert Invalid Files to BQ
            if "OPKEYLOG" in dbAssessmentDataframes.keys():
                op_df = dbAssessmentDataframes["OPKEYLOG"]
                import_db_assessment.insertErrors(invalidfiles,op_df,gcpProjectName,bqDataset)
                importresults=import_db_assessment.populateBT('notabname','nodataframe','yes',invalidfiles,'invalidfiles',-1,importresults,args)

        if args.fromdataframe:

            sucessImported, tablesImported,importresults = import_db_assessment.importAllDataframeToBQ(args,gcpProjectName,bqDataset,transformersTablesSchema,dbAssessmentDataframes,transformersParameters,importresults)

        else:

            # Import the CSV data found in the OS
            sucessImported,importresults=import_db_assessment.importAllCSVsToBQ(gcpProjectName,bqDataset,fileList,transformersTablesSchema,2,transformersParameters,args,importresults)
            # Import all Optimus Prime CSV configutation
            sucessImported,importresults=import_db_assessment.importAllCSVsToBQ(gcpProjectName,bqDataset,fileListOPConfig,transformersTablesSchema,1,transformersParameters,args,importresults)

        transformerParameterResults, transformersRulesVariables, fileList, dbAssessmentDataframes = rules_engine.runRules("2",transformerRulesConfig, dbAssessmentDataframes, None, args, collectionKey, transformersTablesSchema, fileList, rulesAlreadyExecuted, transformersParameters, gcpProjectName, bqDataset)

        # Create Optimus Prime Views
        import_db_assessment.createOptimusPrimeViewsFromOS(gcpProjectName,bqDataset)

        # Call BT for import summary table
        import_db_assessment.printBTResults(importresults)
        print ('\n\n Thank YOU for using Optimus Prime!\n\n')

def argumentsParser():
# function to handle all arguments to be used in cli mode for this code and enforces mandatory options

    # Creating an argpaser object
    parser = argparse.ArgumentParser()

    # Name of dataset to be created and have the data imported
    parser.add_argument("-dataset", type=str, default=None, help="name of the Big Query dataset to import all CSV files. If do not exists it will be created if exists the data is appended")

    # GCP project name to be used with the dataset
    parser.add_argument("-projectname", type=str, default=None, help="name of the Google Cloud project name used for the Big Query dataset")

    # OS csv files location to be imported to Big Query
    parser.add_argument("-fileslocation", type=str, default='dbResults', help="optimus prime files location to be imported")

    # OS csv files location to be imported to Big Query
    parser.add_argument("-transformersconfig", type=str, default='opConfig/transformers.json', help="location of transformers.json file with all parameters and rules")

    # Optimus collection ID is the number in the final part of the generated CSV files. For example: dbResults/opdb_dbfeatures_ol79-orcl-db02.ORCLCDB.ORCLCDB.180603.log. Collection ID is: 180603
    parser.add_argument("-collectionid", type=str, default=None, help="optimus prime collection id from CSV files OR 'consolidate' for consolidated logs")

    # Separator for the logs being processed
    parser.add_argument("-sep", type=str, default=';', help="separator string in the files to be processed. The default is: ; (semicomma)")

    parser.add_argument("-dbversion", type=str, default=None, help="database version to be processed")

    parser.add_argument("-collectionversion", type=str, default='0.0.0', help="script collection version used")
    

    parser.add_argument("-schemadetection", type=str, default='FILLGAP', help="How Optimus Prime will handle table schemas to be imported to Big Query")
    # Auto: Uses the columns found in the CSV file to import the data to BQ
    # Manual: Uses the configuration file from JSON
    # FillGaps: Uses manual and whenever a schema is missing then we use Auto for that

    # If this is present in the command line it will take value as true otherwise it will always be false
    parser.add_argument("-deletedataset", default=False, help="Delete dataset before importing new data. WARNING: It will delete all data in the dataset!", action="store_true")

    parser.add_argument("-loadtype", type=str, default="WRITE_APPEND", help="Choose the BQ Load Type. Options are: WRITE_TRUNCATE, WRITE_APPEND and WRITE_EMPTY. The WRITE_APPEND is the default option.")

    parser.add_argument("-fromdataframe", default=False, help="Import dataframes to Big Query instead of CSV files.", action="store_true")
    
    parser.add_argument("-consolidatedataframes", default=False, help="Consolidate CSV files before importing.", action="store_true")

    parser.add_argument("-remote", default=False, help="Leverage remote API", action="store_true")

    parser.add_argument("-remoteurl", type=str, default="https://op-api-3qhhvv7zvq-uc.a.run.app", help="Leverage remote API")
    

    # Consolidates different collection IDs found in the OS (dbResults/*log) into a single CSV per file type. 
    # For example: dbResults has 52 files. Meaning, 2 collection IDs (each one has 26 different file types). 
    # After the consolidation it produces 26 *consolidatedlogs.log which would have data from both collection IDs 
    parser.add_argument("-cl", "--consolidatelogs", default=False, help="consolidate all CSV files opdb*log found in dbResults/ directory", action="store_true")

    # Increase logging output level
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")

    parser.add_argument("-importcomment", type=str, default='', help="Comment for the Import")


    # SM: 04/28/2022:[Bug#90] Recreate views without loading the data:
    parser.add_argument("-recreateviews", default=False, help="Recreate views without loading the data (Yes/No)")

    parser.add_argument("-filterbydbversion", type=str, default='', help="To import only specific db version")
    parser.add_argument("-filterbysqlversion", type=str, default='', help="To import only specific SQL version")
    parser.add_argument("-skipvalidations",  default=False, help="To skip all the file Validations", action="store_true")

    # Execute the parse_args() method. Variable args is a namespace type
    args = parser.parse_args()

    # SM: 04/28/2022:[Bug#90] Check project name and big query dataset name
    if str(args.recreateviews).upper() == "YES":
        # In case there is not dataset parameter set or with valid content in the arguments. It is required during view recreates
        if (args.dataset is None or args.dataset == ''):
            sys.exit('\nERROR: -dataset not provided. It is required during view recreates\n')
        
        # In case project name/project id is not provided. It is required during view recreates
        elif args.projectname is None:
            print ('\nWARNING: -projectname not provided. It is required during view recreates\n')

        # In case project name/project id is not provided. It is required during view recreates
        elif args.dbversion is None:
            print ('\nWARNING: -dbversion not provided. It is required during view recreates\n')

        # In case project name/project id is not provided. It is required during view recreates
        elif args.collectionversion is None:
            print ('\nWARNING: -collectionversion name not provided. It is required during view recreates\n')
    
    # If not using -cl flag
    if args.consolidatelogs == False:

        # In case there is not dataset parameter set or with valid content in the arguments
        if (args.dataset is None or args.dataset == ''):
            sys.exit('\nERROR: The parameter -dataset cannot be omitted and it must have a valid name.\n')
        
        # In case project name/project id is not provided
        elif args.projectname is None:
            print ('\nWARNING: Google Cloud project name not provided. Optimus Prime will try to get it automatically from Google Big Query API call.\n')

        # In case optimus collection id is omitted
        elif args.collectionid is None:
            sys.exit('\nERROR: The parameter -collectionid cannot be omitted. Please provide the collection id from CSV files.\n')

    # Returns a namespace object with all arguments and its values
    return args

if __name__ == '__main__':

    # Handling arguments
    args = argumentsParser()

    if(args.remote):
        runRemote(args)
    else:
        # Call main function
        runMain(args)
