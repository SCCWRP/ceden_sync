import pandas as pd
import numpy as np
import wget, zipfile, os, shutil, re, time, gc, sys
from sqlalchemy import create_engine

from utils import DotDict

eng = create_engine(os.environ.get("DB_CONNECTION_STRING"))

parquet_links = {
    "chemistry" : "https://data.ca.gov/dataset/28d7a81d-6458-47bd-9b79-4fcbfbb88671/resource/f4aa224d-4a59-403d-aad8-187955aa2e38/download/waterchemistrydata_parquet_2022-01-07.zip",
    "habitat"   : "https://data.ca.gov/dataset/f5edfd1b-a9b3-48eb-a33e-9c246ab85adf/resource/0184c4d0-1e1d-4a33-92ad-e967b5491274/download/habitatdata_parquet_2022-01-07.zip"
}

# dictionary will be a storage container for all the massive dataframes
all_dfs = {}
schemas = {}


# TODO This is a rough draft. There may be a lot of ways to improve this code, and it might need some restructuring

for datatype, link in parquet_links.items():
    newfolder = f'rawdata/ceden_{datatype}_allyears'

    # TODO if the download attempt returns a 404 or something like that, then we should have the thing send an email so we can look into it
    # basically skip downloading if we specified to skip, or if the data is not there
    if (sys.argv[-1] not in ('--skip-download', '--no-download')) or not os.path.exists(newfolder):
        print("clear the previous raw data and replace with the other data")
        shutil.rmtree('rawdata')
        os.mkdir('rawdata')
        
        print(f'downloading {datatype} data')
        wget.download(link, f'{newfolder}.zip')

        with zipfile.ZipFile(f'{newfolder}.zip', 'r') as z:
            print(f'extracting data to {newfolder}')
            z.extractall(newfolder)
            print('done')
        

    # we will discard data older than the specified cut off year, in this case 1999 (the year should come from Rafi or Eric or someone like that)
    # There is no particular reason why it needs to be sorted
    # right now for testing we will do 2020
    cutoffyear = 2020
    discards = sorted([folder for folder in os.listdir(newfolder) if int(re.sub('[^0-9]', '', folder)) < cutoffyear])

    # dir_to_rm = directory to remove
    for dir_to_rm in discards:
        shutil.rmtree(os.path.join(newfolder, dir_to_rm))

    print(f'Reading in all {datatype} data')
    print(f'This may take a few minutes')
    starttime = time.time()

    # reads in all the parquet files at once into a dataframe
    df = pd.concat(
        [
            pd.read_parquet(os.path.join(newfolder, subfolder, filename))
            for subfolder in os.listdir(newfolder)
            for filename in os.listdir(os.path.join(newfolder, subfolder))
        ],
        ignore_index = True
    )
    print(f"Reading {datatype} data took {time.time() - starttime} seconds")

    # store the dataframe in a variable
    exec(f'all_{datatype} = df')
    
    # lowercase the column names
    exec(f'all_{datatype}.columns = [c.lower() for c in all_{datatype}.columns]')

    # add an id column
    # of course it is not a true ESRI "objectid" column but it will be helpful to have some kind of row identifier
    exec(f"all_{datatype}['objectid'] = all_{datatype}.index")
    
    # store in all_dfs
    exec(f"all_dfs['{datatype}'] = all_{datatype}")

    # Create schema to create table
    schemas[datatype] = ({
        col: 
        "TIMESTAMP" 
        if str(typ).startswith('datetime') 
        else "INT" if str(typ).startswith('int') 
        else "NUMERIC(38,8)" if str(typ).startswith('float') 
        else f"VARCHAR({int(all_dfs[datatype][col].str.len().max()) if not pd.isnull(all_dfs[datatype][col].str.len().max()) else 5})"
        for col, typ in list(zip(all_dfs[datatype].dtypes.index, all_dfs[datatype].dtypes.values))
    })

    # recreate table every time to avoid errors on insert. schema gets recreated accordong to the data that was downloaded
    create_tbl_sql = """
            DROP TABLE IF EXISTS ceden_{}; 
            CREATE TABLE ceden_{} ({});
        """.format(
            datatype,
            datatype,
            ",\n".join([f'{col} {typ}' for col, typ in schemas[datatype].items()])
        )

    print(create_tbl_sql)
    eng.execute(create_tbl_sql)
    
    
    # write csv to tmp directory with no index or headers
    tmpcsvpath = f'/tmp/{datatype}.csv'
    print(f"write csv to {tmpcsvpath} with no index or headers")
    all_dfs[datatype].to_csv(tmpcsvpath, index = False, header = False)
    print("done")
    

    # TODO error checking - there is no good way right now in this script to check if the command executed successfully or not
    # This will ensure the data is copied with correct corresponding columns
    # psql can execute since it authenticates with PGPASSWORD environment variable
    sqlcmd = f'psql -h {os.environ.get("DB_HOST")} \
            -d {os.environ.get("DB_NAME")} \
            -U {os.environ.get("DB_USER")} \
            -c "\copy ceden_{datatype} ({",".join(all_dfs[datatype].columns)}) \
            FROM \'{tmpcsvpath}\' csv\"'
    
    print(f"load records to ceden_{datatype}")

    # TODO related with the ealier "TODO" is that we can pretty much only tell that it failed if the exit code was non zero
    # At least we can catch if it failed, and which datatype was the one that failed, which is a start
    # we can email if the exitcode is non zero and include which datatype failed, but the next thing would be to capture some kind of error message
    code = os.system(sqlcmd)
    print(f"exit code: {code}")
    print(f"done")

    os.remove(tmpcsvpath)
    print(f"hopefully loaded {len(all_dfs[datatype])} records to ceden_{datatype}.")





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





