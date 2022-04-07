import pandas as pd
import numpy as np
import wget, zipfile, os, shutil, re, time, gc, sys
from sqlalchemy import create_engine
import requests

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
    "toxicity"  : "https://careerkarma.com/blog/python-concatenate-strings/#:~:text=The%20%2B%20operator%20lets%20you%20combine,strings%20you%20want%20to%20merge.&text=This%20code%20concatenates%2C%20or%20merges,Hello%20%E2%80%9D%20and%20%E2%80%9CWorld%E2%80%9D.",
}

# store report in list to email at the end
report = []

# TODO we need try except blocks
# TODO This is a rough draft. There may be a lot of ways to improve this code, and it might need some restructuring

broke_links = []
broken = False

for datatype in list(parquet_links):
    link = parquet_links[datatype]
    try:
        r = requests.get(link,stream=True)
        contenttype = r.headers['Content-Type']
        if contenttype != 'binary/octet-stream': 
            raise Exception(f'Content Type was not what we expected. We expected a binary stream but got {contenttype}')
    except Exception as e:
        
        print(f"Exception occurred trying to download {datatype} data")
        print(e)
        
        # add to report
        report.append(f"Error downloading data for {datatype}\n Link: {link}\nError message: {e}\n\n")

        # Delete key from dictionary
        del parquet_links[datatype]
        
        continue

sys.exit()

try:
    #if "Content-Length" in r.headers:
    file_size = int(r.headers["Content-Length"])
except KeyError:
    # Just a class that I defined to raise an exception if the URL was not downloadable
    print("Not Downloadable")