import pandas as pd
from sqlalchemy import create_engine
import os, sys

from utils import registration_id, primary_key, exception_handler, build_where_clause

eng = create_engine(os.environ.get("DB_CONNECTION_STRING"))

@exception_handler
def translated_view(dest_table, src_base_table, translator_table, translated_viewname, eng, conditions = None, *args, **kwargs):
    """
    dest table would be the destination table, src_base_table is the table that we are translating
    It is called src_base_table since it is not a simple translation of those columns but rather a combination of translating and joining with others
    eng is the database connection
    translator table is the table which will be used to translate the data
    translated viewname is the name of the view that will be created
    return_df specifies whether or not the translated dataframe should be returned by the function
    """

    report = []

    required_translation_cols = (
        'dest_table','dest_column','src_base_table','src_table','src_column', 'base_join_columns','src_join_columns','special_function','special_value','primary_key'
    )

    #xwalk = pd.read_sql(f"SELECT * FROM {translator_table} WHERE dest_table = '{dest_table}' ; ", eng)
    xwalk = pd.read_sql(
        f"""
        SELECT
            {translator_table}.*,
            infocols.data_type AS dtype
        FROM
            {translator_table}
            INNER JOIN (SELECT * FROM information_schema.COLUMNS WHERE table_name = '{dest_table}') infocols ON {translator_table}.dest_column = infocols.COLUMN_NAME 
            WHERE {translator_table}.dest_table = '{dest_table}'
        """,
        eng
    )

    assert not xwalk.empty, f"the crosswalk table has no records for the destination table {dest_table}"

    # raise an exception if the translation table is missing required columns
    assert \
        set(required_translation_cols).issubset(set(xwalk.columns)), \
        f"Translation table missing required translation columns: {', '.join(set(required_translation_cols) - set(xwalk.columns))}"
    
    # building the columns names to go after the SELECT statement
    cols = ',\n\t'.join(
        xwalk[
            ( (~pd.isnull(xwalk.src_column)) & (~pd.isnull(xwalk.src_table)) & (~pd.isnull(xwalk.dest_column)) ) 
            | (~pd.isnull(xwalk.special_function)) 
            | (~pd.isnull(xwalk.special_value))
        ].apply(
            lambda row:
            f"'{row['special_value']}'::{row['dtype']} AS {row['dest_column']}" if not pd.isnull(row['special_value']) 
            else f"{row['special_function']}::{row['dtype']} AS {row['dest_column']}"
            #else re.sub('__.*__', row[re.search('__(.*)__', str(row['special_function'])).groups()[0]], str(row['special_function'])) 
            if (not pd.isnull(row['special_function']))
            else f"{row['src_table']}.{row['src_column']}::{row['dtype']} AS {row['dest_column']}"
            if (pd.isnull(row['default_value']))
            else f"COALESCE({row['src_table']}.{row['src_column']}::{row['dtype']}, '{row['default_value']}') AS {row['dest_column']}"
            ,
            axis = 1
        )
        .values
    )
    # print(cols)

    # Add in the part to select the objectid in such a way to not conflict with the objecid unique key that ESRI makes
    reg_id = registration_id(dest_table, eng)
    if not pd.isnull(reg_id):
        cols = f"row_number() OVER (ORDER BY 1::integer) + (( SELECT max(i{reg_id}.base_id) AS max FROM i{reg_id})) AS objectid, {cols}"

    # assertion is made that join columns are same amount and that they are in the correct respective order in each column
    # building the part of the query that will join the data
    joins = xwalk[(xwalk.src_table != xwalk.src_base_table) & (~pd.isnull(xwalk.src_table)) & (~pd.isnull(xwalk.src_base_table))].apply(
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
    
    if not joins.empty:
        joins = '\n'.join(joins.unique())
    else:
        joins = ''


    # print(joins)
    eng.execute(f'DROP VIEW IF EXISTS {translated_viewname};')
    sql = f"CREATE VIEW {translated_viewname} AS SELECT \n\t{cols} \nFROM {src_base_table} {joins}"

    # conditions would be an argument with a "where" statement essentially, to limit the translated view only to certain rows, which makes sense for taxonomy
    if conditions is not None:
        assert type(conditions) == dict, 'Error creating the WHERE statement in the translated view function - the "conditions" argument is not a dictionary'
        sql += f" {build_where_clause(conditions)}"
    
    print("The following sql command will be executed")
    print(sql)
    print("waiting")
    try:
        eng.execute(sql)
        report.append(f"The view {translated_viewname} has been created")
    except Exception as e:
        report.append(f"Error creating view {translated_viewname}:\n{str(e)[:250]}")
    print(f"The view {translated_viewname} has been created")

    # Create the view to view duplicated records of that data from CEDEN, since duplicates will not be loaded to unified
    dupsql = f"""
    CREATE OR REPLACE VIEW {translated_viewname}_dup AS SELECT
            *
        FROM
            (
            SELECT
                *,
                COUNT ( * ) OVER ( PARTITION BY {', '.join(primary_key(dest_table, eng))} ) AS COUNT
            FROM
                sde.{dest_table} 
            ) tmpcount
        WHERE
            tmpcount.COUNT > 1
    """

    try:
        print("SQL to be executed to create the duplicated data view:")
        print(dupsql)
        eng.execute(dupsql)
        report.append(f"The view for the duplicate records, {translated_viewname}_dup has been created")
        print(f"The view for the duplicate records, {translated_viewname}_dup has been created")
    except Exception as e:
        print("Exception occurred")
        print(e)
        report.append(f"Error creating view {translated_viewname}_dup:\n{str(e)[:250]}")

    return report
    


@exception_handler
def benthic_translated_view(dataype, dest_table, src_base_table, translator_table, translated_viewname, eng, *args, **kwargs):
    """
    dest table would be the destination table, src_base_table is the table that we are translating
    It is called src_base_table since it is not a simple translation of those columns but rather a combination of translating and joining with others
    eng is the database connection
    translator table is the table which will be used to translate the data
    translated viewname is the name of the view that will be created
    return_df specifies whether or not the translated dataframe should be returned by the function
    """

    report = []

    required_translation_cols = (
        'dest_table','dest_column','src_base_table','src_table','src_column', 'base_join_columns','src_join_columns','special_function','special_value','primary_key'
    )

    #xwalk = pd.read_sql(f"SELECT * FROM {translator_table} WHERE dest_table = '{dest_table}' ; ", eng)
    xwalk = pd.read_sql(
        f"""
        SELECT
            {translator_table}.*,
            infocols.data_type AS dtype
        FROM
            {translator_table}
            INNER JOIN (SELECT * FROM information_schema.COLUMNS WHERE table_name = '{dest_table}') infocols ON {translator_table}.dest_column = infocols.COLUMN_NAME 
            WHERE {translator_table}.dest_table = '{dest_table}'
        """,
        eng
    )

    assert not xwalk.empty, f"the crosswalk table has no records for the destination table {dest_table}"

    # raise an exception if the translation table is missing required columns
    assert \
        set(required_translation_cols).issubset(set(xwalk.columns)), \
        f"Translation table missing required translation columns: {', '.join(set(required_translation_cols) - set(xwalk.columns))}"
    
    # building the columns names to go after the SELECT statement
    cols = ',\n\t'.join(
        xwalk[
            ( (~pd.isnull(xwalk.src_column)) & (~pd.isnull(xwalk.src_table)) & (~pd.isnull(xwalk.dest_column)) ) 
            | (~pd.isnull(xwalk.special_function)) 
            | (~pd.isnull(xwalk.special_value))
        ].apply(
            lambda row:
            f"'{row['special_value']}'::{row['dtype']} AS {row['dest_column']}" if not pd.isnull(row['special_value']) 
            else f"{row['special_function']}::{row['dtype']} AS {row['dest_column']}"
            #else re.sub('__.*__', row[re.search('__(.*)__', str(row['special_function'])).groups()[0]], str(row['special_function'])) 
            if (not pd.isnull(row['special_function']))
            else f"{row['src_table']}.{row['src_column']}::{row['dtype']} AS {row['dest_column']}"
            ,
            axis = 1
        )
        .values
    )
    # print(cols)

    # Add in the part to select the objectid in such a way to not conflict with the objecid unique key that ESRI makes
    reg_id = registration_id(dest_table, eng)
    if not pd.isnull(reg_id):
        cols = f"row_number() OVER (ORDER BY 1::integer) + (( SELECT max(i{reg_id}.base_id) AS max FROM i{reg_id})) AS objectid, {cols}"

    # assertion is made that join columns are same amount and that they are in the correct respective order in each column
    # building the part of the query that will join the data
    joins = xwalk[(xwalk.src_table != xwalk.src_base_table) & (~pd.isnull(xwalk.src_table)) & (~pd.isnull(xwalk.src_base_table))].apply(
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
    
    if not joins.empty:
        joins = '\n'.join(joins.unique())
    else:
        joins = ''


    # print(joins)
    eng.execute(f'DROP VIEW IF EXISTS {translated_viewname};')
    sql = f"CREATE VIEW {translated_viewname} AS SELECT \n\t{cols} \nFROM {src_base_table} {joins}"
    
    print("The following sql command will be executed")
    print(sql)
    print("waiting")
    try:
        eng.execute(sql)
        report.append(f"The view {translated_viewname} has been created")
    except Exception as e:
        report.append(f"Error creating view {translated_viewname}:\n{str(e)[:250]}")
    print(f"The view {translated_viewname} has been created")

    # Create the view to view duplicated records of that data from CEDEN, since duplicates will not be loaded to unified
    dupsql = f"""
    CREATE OR REPLACE VIEW {translated_viewname}_dup AS SELECT
            *
        FROM
            (
            SELECT
                *,
                COUNT ( * ) OVER ( PARTITION BY {', '.join(primary_key(dest_table, eng))} ) AS COUNT
            FROM
                sde.{dest_table} 
            ) tmpcount
        WHERE
            tmpcount.COUNT > 1
    """

    try:
        print("SQL to be executed to create the duplicated data view:")
        print(dupsql)
        eng.execute(dupsql)
        report.append(f"The view for the duplicate records, {translated_viewname}_dup has been created")
        print(f"The view for the duplicate records, {translated_viewname}_dup has been created")
    except Exception as e:
        print("Exception occurred")
        print(e)
        report.append(f"Error creating view {translated_viewname}_dup:\n{str(e)[:250]}")

    return report
    

    


