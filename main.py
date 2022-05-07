# Main function - now configured only for PHAB for testing purposes

import os, sys
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

translation_args = DotDict({
    "habitat" : DotDict({
        'dest_table'          : 'unified_phab',
        'src_base_table'      : 'ceden_habitat',
        'translator_table'    : 'ceden_xwalk',
        'translated_viewname' : 'vw_transl_ceden_habitat',
        'parquet_link'        : "https://data.ca.gov/dataset/f5edfd1b-a9b3-48eb-a33e-9c246ab85adf/resource/0184c4d0-1e1d-4a33-92ad-e967b5491274/download/habitatdata_parquet_2022-01-07.zip",
        'cutoffyear'          : 2015
    }),
    "chemistry" : DotDict({
        'dest_table'          : 'unified_chemistry',
        'src_base_table'      : 'ceden_chemistry',
        'translator_table'    : 'ceden_xwalk',
        'translated_viewname' : 'vw_transl_ceden_chemistry',
        'parquet_link'        : "https://data.ca.gov/dataset/28d7a81d-6458-47bd-9b79-4fcbfbb88671/resource/f4aa224d-4a59-403d-aad8-187955aa2e38/download/waterchemistrydata_parquet_2022-01-07.zip",
        'cutoffyear'          : 2015
    }),
    "benthic" : DotDict({
        "taxonomy": DotDict({
            'dest_table'          : 'unified_taxonomy',
            'src_base_table'      : 'ceden_benthic',
            'translator_table'    : 'ceden_xwalk',
            'translated_viewname' : 'vw_transl_ceden_taxonomy',
            'conditions'          : {
                "lu_collectionmethodxwalk.collectionmethodname": ('BMI_RWB', 'BMI_RWB_MCM', 'BMI_TRC', 'BMI_SNARL', 'BMI_CSBP_Comp', 'BMI_CSBP_Trans')
            }
        }),
        "algae": DotDict({
            'dest_table'          : 'unified_algae',
            'src_base_table'      : 'ceden_benthic',
            'translator_table'    : 'ceden_xwalk',
            'translated_viewname' : 'vw_transl_ceden_algae',
            'conditions'          : {
                "lu_collectionmethodxwalk.collectionmethodname": ('Algae_EMAP', 'Algae_EPA_NWS', 'Algae_RWB', 'Algae_SWAMP', 'Algae_RWB1_SFEel', 'Algae_SNARL')
            }
        }),
        'parquet_link'        : "https://data.ca.gov/dataset/c14a017a-8a8c-42f7-a078-3ab64a873e32/resource/eb61f9a1-b1c6-4840-99c7-420a2c494a43/download/benthicdata_parquet_2022-04-14.zip",
        'cutoffyear'          : 2000
    })
})
report = []
for dtype in translation_args.keys():

    args = translation_args[dtype]

    #pull_report = []
    pull_report = ceden_pull(dtype, args.parquet_link, eng, cutoffyear = args.cutoffyear) #, skip_download = True)

    # Two translations are needed for the benthic datatype, since it goes from one CEDEN table to two unified tables
    if dtype == "benthic":
        translate_report = [
            *translated_view(eng = eng, **args.taxonomy), 
            *translated_view(eng = eng, **args.algae)
        ]
        update_report = [
            *upsert(args.taxonomy.translated_viewname, args.taxonomy.dest_table, eng, conditions = {'record_origin':'CEDEN'}),
            *upsert(args.algae.translated_viewname, args.algae.dest_table, eng, conditions = {'record_origin':'CEDEN'})
        ]
    else: 
        update_report = upsert(args.translated_viewname, args.dest_table, eng, conditions = {'record_origin':'CEDEN'})
        translate_report = translated_view(eng = eng, **args) \


    report = [*report, f"----- CEDEN SYNC REPORT FOR {dtype} -----\n", *pull_report, *translate_report, *update_report, '\n\n']

# TODO We need a table of meta data for this sync routine so we can build a dashboard/webpage
# There are many many components to the SMC data pipeline and it gets complicated, so we need some kind of reporting web page/web tool for everything
# The CEDEN sync, Checker data sync, CSCI, PHABMetrics, IPI, ASCI, the conductivity report, the nutrient report...
# and there may be others that i cant think of right now

# may want to add current_timestamp to the last_edited_date to the translator table so it shows up in the unified table as such

send_mail('admin@checker.sccwrp.org', ['robertb@sccwrp.org'], "CEDEN DATA SYNC REPORT", '\n\n'.join(report), server = '192.168.1.18')