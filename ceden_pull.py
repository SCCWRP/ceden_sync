import pandas as pd
import numpy as np
import wget, zipfile, os, shutil, re, time, gc, sys
from sqlalchemy import create_engine
import requests

from utils import DotDict, send_mail, exception_handler

# TODO wrap this in a function and call it with parameters
#   skip_download default false
#   cutoff year default 1999
#   datatypes default 'all'

@exception_handler
def ceden_pull(parquet_links, eng, cutoffyear = 1999):
    # store report in list to email at the end
    report = []

    # TODO we need try except blocks
    # TODO This is a rough draft. There may be a lot of ways to improve this code, and it might need some restructuring

    # Purpose:  Check if download links work.
    # Notes:    parquet_links is coerced into a list and then iterated through because we may delete a key if 
    #           its associated link is broken. If you iterate through an dic.items() object, an error is triggered.
    #           python doesn't like that you are changing the size of the dictionary while iterating through.
    for datatype in list(parquet_links):
        link = parquet_links[datatype]
        try:
            r = requests.get(link,stream=True)
            contenttype = r.headers['Content-Type']

            # The if statement catches most non-downloadable types but there are a few exceptions that trigger an error.
            # In principle, we are adding another case to all possible exceptions. 
            if contenttype != 'binary/octet-stream': 
                raise Exception(f'Content Type was not what we expected. We expected a binary stream but got {contenttype}')
        except Exception as e:
            print(f"Exception occurred trying to download {datatype} data")
            print(e)
            
            # add to report
            report.append(f"Error downloading data for {datatype}\nLink: {link}\nError message: {e}\n\n")

            # Delete key from dictionary
            del parquet_links[datatype]
            
            continue

    for datatype, link in parquet_links.items():
        newfolder = f'rawdata/ceden_{datatype}_allyears'

        # TODO if the download attempt returns a 404 or something like that, then we should have the thing send an email so we can look into it
        # basically skip downloading if we specified to skip, or if the data is not there
        # TODO for some reason the sys argv thing is not working correctly
        if (sys.argv[-1] not in ('--skip-download', '--no-download')) or not os.path.exists(newfolder):

            # this was originally here for the case where they didn't specify skip download, but the data is there
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
        discards = sorted([folder for folder in os.listdir(newfolder) if int(re.sub('[^0-9]', '', folder)) < cutoffyear])
        print(discards)
        
        # 
        for folder in discards:
            shutil.rmtree(os.path.join(newfolder, folder))

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
    
    # send_mail('admin@checker.sccwrp.org', ['kevinl@sccwrp.org'], "CEDEN DATA SYNC REPORT", '\n'.join(report), server = '192.168.1.18')
    return report







