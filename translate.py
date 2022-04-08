import pandas as pd
from sqlalchemy import create_engine
import re, os

eng = create_engine(os.environ.get("DB_CONNECTION_STRING"))

dest_table = 'unified_phab'
src_base_table = 'ceden_habitat'

xwalk = pd.read_sql(f"SELECT * FROM ceden_xwalk WHERE src_base_table = '{src_base_table}'", eng)

cols = ',\n\t'.join(
    xwalk.apply(
        lambda row:
        f"'{row['special_value']}' AS {row['dest_column']}" if not pd.isnull(row['special_value']) 
        else f"{row['special_function']}  AS {row['dest_column']}"
        #else re.sub('__.*__', row[re.search('__(.*)__', str(row['special_function'])).groups()[0]], str(row['special_function'])) 
        if (not pd.isnull(row['special_function']))
        else f"{row['src_table']}.{row['src_column']} AS {row['dest_column']}"
        ,
        axis = 1
    )
    .values
)

# assertion is made that join columns are same amount and that they are in the correct respective order in each column
joins = '\n'.join(
    xwalk[xwalk.src_table != src_base_table].apply(
        lambda row:
        "LEFT JOIN {} ON {}".format(
            row['src_table'],
            ','.join(
                [
                    f"{row['src_table']}.{srcjoincol} = {row['src_base_table']}.{basejoincol}"
                    for srcjoincol, basejoincol in 
                    list(
                        zip( row['src_join_columns'].split(','), row['base_join_columns'].split(',') )
                    )
                ]
            )
        ),
        
        axis = 1
    )
    .unique()
)

sql = f"CREATE OR REPLACE VIEW vw_translated_ceden_habitat AS SELECT \n\t{cols} \nFROM {src_base_table} {joins}"
print(sql)
eng.execute(sql)
