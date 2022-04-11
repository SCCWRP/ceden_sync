# Main function - now configured only for PHAB for testing purposes

import os
from sqlalchemy import create_engine
from ceden_pull import ceden_pull
from translate import translated_view
from load import upsert
from utils import send_mail

eng = create_engine(os.environ.get("DB_CONNECTION_STRING"))

# TODO download the sampling location info and put into a ceden table (it is a csv file 🤗)
parquet_links = {
    # benthic is both BMI (bug) taxonomy (data for CSCI) and algae taxonomy (data for ASCI)
    "benthic"   : "https://data.ca.gov/dataset/c14a017a-8a8c-42f7-a078-3ab64a873e32/resource/eb61f9a1-b1c6-4840-99c7-420a2c494a43/download/benthicdata_parquet_2022-01-07.zip",
    "chemistry" : "https://data.ca.gov/dataset/28d7a81d-6458-47bd-9b79-4fcbfbb88671/resource/f4aa224d-4a59-403d-aad8-187955aa2e38/download/waterchemistrydata_parquet_2022-01-07.zip",
    "habitat"   : "https://data.ca.gov/dataset/f5edfd1b-a9b3-48eb-a33e-9c246ab85adf/resource/0184c4d0-1e1d-4a33-92ad-e967b5491274/download/habitatdata_parquet_2022-01-07.zip",
    "toxicity"  : "https://data.ca.gov/dataset/c5a4ab7e-4d9b-4b31-bc08-807984d44102/resource/a6c91662-d324-43c2-8166-a94dddd22982/download/toxicitydata_parquet_2022-01-07.zip",
}

# cutoff year for the data, so that data older than 1999 is ignored (2020 for testing purposes)
CUTOFF_YEAR = 2020

pull_report = ceden_pull(parquet_links, eng, cutoffyear=CUTOFF_YEAR)

translation_args = {
    'dest_table'          : 'unified_phab',
    'src_base_table'      : 'ceden_habitat',
    'translator_table'    : 'ceden_xwalk',
    'translated_viewname' : 'vw_transl_ceden_habitat',
    'eng'                 : eng,
    'return_df'           : False
}
test = translated_view(**translation_args)

upsert_sql = upsert('vw_translated_ceden_habitat','unified_phab', eng, conditions = {'record_origin':'CEDEN'})
print(upsert_sql)
eng.execute(upsert_sql)

send_mail('admin@checker.sccwrp.org', ['kevinl@sccwrp.org'], "CEDEN DATA SYNC REPORT", '\n'.join(pull_report), server = '192.168.1.18')