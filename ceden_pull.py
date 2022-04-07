import pandas as pd
import numpy as np
import wget, zipfile, os, shutil, re, time, gc, sys
from sqlalchemy import create_engine

from utils import DotDict, send_mail

# TODO wrap this in a function and call it with parameters
#   skip_download default false
#   cutoff year default 1999
#   datatypes default 'all'

eng = create_engine(os.environ.get("DB_CONNECTION_STRING"))

# TODO download the sampling location info and put into a ceden table (it is a csv file 🤗)
parquet_links = {
    # benthic is both MI (bug) taxonomy (data for CSCI) and algae taxonomy (data for ASCI)
    "benthic"   : "https://data.ca.gov/dataset/c14a017a-8a8c-42f7-a078-3ab64a873e32/resource/eb61f9a1-b1c6-4840-99c7-420a2c494a43/download/benthicdata_parquet_2022-01-07.zip",
    "chemistry" : "https://data.ca.gov/dataset/28d7a81d-6458-47bd-9b79-4fcbfbb88671/resource/f4aa224d-4a59-403d-aad8-187955aa2e38/download/waterchemistrydata_parquet_2022-01-07.zip",
    "habitat"   : "https://data.ca.gov/dataset/f5edfd1b-a9b3-48eb-a33e-9c246ab85adf/resource/0184c4d0-1e1d-4a33-92ad-e967b5491274/download/habitatdata_parquet_2022-01-07.zip",
    "toxicity"  : "https://data.ca.gov/dataset/c5a4ab7e-4d9b-4b31-bc08-807984d44102/resource/a6c91662-d324-43c2-8166-a94dddd22982/download/toxicitydata_parquet_2022-01-07.zip",
}

# store report in list to email at the end
report = []

# TODO we need try except blocks
# TODO This is a rough draft. There may be a lot of ways to improve this code, and it might need some restructuring

for datatype, link in parquet_links.items():
    newfolder = f'rawdata/ceden_{datatype}_allyears'

    # TODO if the download attempt returns a 404 or something like that, then we should have the thing send an email so we can look into it
    # basically skip downloading if we specified to skip, or if the data is not there
    # TODO for some reason the sys argv thing is not working correctly
    if (sys.argv[-1] not in ('--skip-download', '--no-download')) or not os.path.exists(newfolder):

        if os.path.exists(newfolder):
            print("clear the previous raw data and replace with the other data")
            shutil.rmtree(newfolder)
            os.mkdir(newfolder)
        
        print(f'downloading {datatype} data')
        wget.download(link, f'{newfolder}.zip')

        with zipfile.ZipFile(f'{newfolder}.zip', 'r') as z:
            print(f'extracting data to {newfolder}')
            z.extractall(newfolder)
            print('done')
        

    # we will discard data older than the specified cut off year, in this case 1999 (the year should come from Rafi or Eric or someone like that)
    # There is no particular reason why it needs to be sorted
    # right now for testing we will do 2020
    cutoffyear = 2021
    discards = sorted([folder for folder in os.listdir(newfolder) if int(re.sub('[^0-9]', '', folder)) < cutoffyear])
    print(discards)

    print(f'Reading in all {datatype} data')
    print(f'This may take a few minutes')
    starttime = time.time()

    # reads in all the parquet files at once into a dataframe
    ceden_data = pd.concat(
        [
            pd.read_parquet(os.path.join(newfolder, subfolder, filename))
            for subfolder in os.listdir(newfolder)
            for filename in os.listdir(os.path.join(newfolder, subfolder))
        ],
        ignore_index = True
    )
    print(f"Reading {datatype} data took {time.time() - starttime} seconds")
    
    # lowercase the column names
    ceden_data.columns = [c.lower() for c in ceden_data.columns]

    # add an id column
    # of course it is not a true ESRI "objectid" column but it will be helpful to have some kind of row identifier
    ceden_data['objectid'] = ceden_data.index
    
    # Create schema to create table
    # TODO speed this up. the code is not efficient since it runs the same function twice for the same result
    # i think we should just have it be varchar500 regardless..
    # but best would be to get max character length and set it to that
    schema = ({
        col: 
        "TIMESTAMP" 
        if str(typ).startswith('datetime') 
        else "INT" if str(typ).startswith('int') 
        else "NUMERIC(38,8)" if str(typ).startswith('float') 
        #else f"VARCHAR({int(ceden_data[col].str.len().max()) if not pd.isnull(ceden_data[col].str.len().max()) else 5})"
        else f"VARCHAR(500)"
        for col, typ in list(zip(ceden_data.dtypes.index, ceden_data.dtypes.values))
    })

    # recreate table every time to avoid errors on insert. schema gets recreated accordong to the data that was downloaded
    # TODO write this to a .sql file to save
    # that way if we dont want to re download the data we also dont have to re create this sql command (which takes a few seconds)
    create_tbl_sql = """
            DROP TABLE IF EXISTS ceden_{}; 
            CREATE TABLE ceden_{} ({});
        """.format(
            datatype,
            datatype,
            ",\n".join([f'{col} {typ}' for col, typ in schema.items()])
        )

    print(create_tbl_sql)
    eng.execute(create_tbl_sql)
    
    
    # write csv to tmp directory with no index or headers
    tmpcsvpath = f'/tmp/{datatype}.csv'
    print(f"write csv to {tmpcsvpath} with no index or headers")
    ceden_data.to_csv(tmpcsvpath, index = False, header = False)
    print("done")
    

    # TODO error checking - there is no good way right now in this script to check if the command executed successfully or not
    # This will ensure the data is copied with correct corresponding columns
    # psql can execute since it authenticates with PGPASSWORD environment variable
    sqlcmd = f'psql -h {os.environ.get("DB_HOST")} \
            -d {os.environ.get("DB_NAME")} \
            -U {os.environ.get("DB_USER")} \
            -c "\copy ceden_{datatype} ({",".join(ceden_data.columns)}) \
            FROM \'{tmpcsvpath}\' csv\"'
    
    print(f"load records to ceden_{datatype}")

    # TODO related with the ealier "TODO" is that we can pretty much only tell that it failed if the exit code was non zero
    # At least we can catch if it failed, and which datatype was the one that failed, which is a start
    # we can email if the exitcode is non zero and include which datatype failed, but the next thing would be to capture some kind of error message
    code = os.system(sqlcmd)
    print(f"exit code: {code}")
    print(f"done")

    os.remove(tmpcsvpath)
    msg = f"Loaded {len(ceden_data)} records to ceden_{datatype}." if code == 0 else f'Error loading records to ceden_{datatype}'
    report.append(msg)
    print(msg)

    # delete variables and collect garbage to hopefully free memory
    del ceden_data
    del schema
    gc.collect()

# TODO send with AWS just because
# anyways we want to start heading that direction so we should figure it out and implement it
send_mail('admin@checker.sccwrp.org', ['robertb@sccwrp.org'], "CEDEN DATA SYNC REPORT", '\n'.join(report), server = '192.168.1.18')


# TODO
# My recommendation is to put these in ceden tables and move them to unified with SQL
# we should create views that query the ceden tables to make them resemble the unified tables
# A sample of what the view definition could be
# -- these would split the analyte and fractionnames i think, or something like this
# SELECT 
#   "substring" (
# 	    REPLACE ( analyte, '\s' :: TEXT, '' :: TEXT ),
# 	    1,
# 	    strpos(
# 		    REPLACE ( analyte, '\s' :: TEXT, '' :: TEXT ),
# 		    ',' :: TEXT 
# 	    ) - 1 
#   ) AS analytename, 
#   
#   reverse (
# 	    "substring" (
# 			REPLACE ( reverse ( analyte ), '\s' :: TEXT, '' :: TEXT ),
# 			1,
# 			strpos(
# 				REPLACE ( reverse ( analyte ), '\s' :: TEXT, '' :: TEXT ),
# 				',' :: TEXT 
# 			) - 1 
# 		) 
# 	) AS fractionname,
#   unit AS unitname ... 
#   ...
#  vw_ceden_chemistry

# moving the data could look like 
# INSERT INTO unified_chemistry (SELECT * FROM vw_ceden_chemistry) ON CONFLICT ON CONSTRAINT unified_chemistry_pkey DO NOTHING 
# instead of do nothing, we can also figure a way to make it update the records which are not part of the primary key
# https://stackoverflow.com/questions/36359440/postgresql-insert-on-conflict-update-upsert-use-all-excluded-values
# looks like we would use python to generate the sql

# but first thing should probably be to move the ceden data into their own tables





