# Main function - now configured only for PHAB for testing purposes

import os
from sqlalchemy import create_engine
from ceden_pull import ceden_pull
from translate import translated_view
from load import upsert
from utils import DotDict, send_mail

global eng
eng = create_engine(os.environ.get("DB_CONNECTION_STRING"))

# TODO download the sampling location info and put into a ceden table (it is a csv file 🤗)
parquet_links = {
    # benthic is both BMI (bug) taxonomy (data for CSCI) and algae taxonomy (data for ASCI)
    #"benthic"   : "https://data.ca.gov/dataset/c14a017a-8a8c-42f7-a078-3ab64a873e32/resource/eb61f9a1-b1c6-4840-99c7-420a2c494a43/download/benthicdata_parquet_2022-01-07.zip",
    #"chemistry" : "https://data.ca.gov/dataset/28d7a81d-6458-47bd-9b79-4fcbfbb88671/resource/f4aa224d-4a59-403d-aad8-187955aa2e38/download/waterchemistrydata_parquet_2022-01-07.zip",
    "habitat"   : "https://data.ca.gov/dataset/f5edfd1b-a9b3-48eb-a33e-9c246ab85adf/resource/0184c4d0-1e1d-4a33-92ad-e967b5491274/download/habitatdata_parquet_2022-01-07.zip",
    #"toxicity"  : "https://data.ca.gov/dataset/c5a4ab7e-4d9b-4b31-bc08-807984d44102/resource/a6c91662-d324-43c2-8166-a94dddd22982/download/toxicitydata_parquet_2022-01-07.zip",
}

# cutoff year for the data, so that data older than 1999 is ignored (2020 for testing purposes)
CUTOFF_YEAR = 2020

pull_report = ceden_pull(parquet_links, eng, cutoffyear=CUTOFF_YEAR)

translation_args = DotDict({
    'dest_table'          : 'unified_phab',
    'src_base_table'      : 'ceden_habitat',
    'translator_table'    : 'ceden_xwalk',
    'translated_viewname' : 'vw_transl_ceden_habitat',
    'parquet_link'        : "https://data.ca.gov/dataset/f5edfd1b-a9b3-48eb-a33e-9c246ab85adf/resource/0184c4d0-1e1d-4a33-92ad-e967b5491274/download/habitatdata_parquet_2022-01-07.zip",
    'cutoffyear'          : 2020
})

translate_report = translated_view(**translation_args)

update_report = upsert(translation_args.translated_viewname, translation_args.dest_table, eng, conditions = {'record_origin':'CEDEN'})

report = ["CEDEN SYNC REPORT FOR some datatype which will later come from a variable name", *pull_report, *translate_report, *update_report]

# TODO We need a table of meta data for this sync routine so we can build a dashboard/webpage
# There are many many components to the SMC data pipeline and it gets complicated, so we need some kind of reporting web page/web tool for everything
# The CEDEN sync, Checker data sync, CSCI, PHABMetrics, IPI, ASCI, the conductivity report, the nutrient report...
# and there may be others that i cant think of right now

# may want to add current_timestamp to the last_edited_date to the translator table so it shows up in the unified table as such

send_mail('admin@checker.sccwrp.org', ['robertb@sccwrp.org'], "CEDEN DATA SYNC REPORT", '\n\n'.join(report), server = '192.168.1.18')