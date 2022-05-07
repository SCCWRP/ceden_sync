from doctest import debug_script
import pandas as pd
from sqlalchemy import create_engine

from utils import primary_key, exception_handler, update_rowid

@exception_handler
def upsert(src, dest, eng, *args, update = True, conditions = dict(), **kwargs):
    # src is the "from" table and dest is the destination table ("to")

    assert type(conditions) == dict, "conditions keyword arg must be a dictionary"
    # assert src columns is a subset of destination columns
    # assert destination table has a primary key
    # assert that the primary key columns of destination table are a subset of the columns of the source table

    insertcols = pd.read_sql(
        f"""
        SELECT 
            CONCAT('CAST(', column_name::character varying, ' AS ', data_type::character varying, ')') AS expression,
            column_name AS colname
        FROM 
            information_schema.columns 
        WHERE 
            table_name = '{dest}' 
        AND 
            column_name IN (
                SELECT column_name FROM information_schema.columns WHERE table_name = '{src}'
            )
        """,
        eng
    )

    pkey = primary_key(dest, eng)

    sql = f"""
        INSERT INTO \n\t{dest}
            ({','.join(insertcols.colname.values)}) 
            (SELECT DISTINCT ON ({','.join(pkey)}) {','.join(insertcols.expression.values)} FROM {src}) \n\t
        ON CONFLICT ON CONSTRAINT {dest}_pkey \n"""

    if update:
        sql += 'DO UPDATE SET \n\t'
        cols = pd.read_sql(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' AND column_name NOT IN ('{}')".format(
                    src,
                    "', '".join(pkey)
                ), 
                eng
            ) \
            .values

        sql+= ',\n\t'.join([f"{col} = CAST(EXCLUDED.{col} AS {dtype})" for col, dtype in cols])
        sql += '\n'

        if len(conditions) > 0:
            sql += ' WHERE '
            sql += ' AND '.join(["{}.{} IN ('{}')".format(dest, col, "','".join(val)) if type(val) in (tuple, list, set) else f"{dest}.{col} = '{val}'" for col, val in conditions.items()])
    else:
        sql += 'DO NOTHING;'

    print(f"The following SQL will be executed to move data from {src} to {dest}")
    print(sql)

    try:
        update_rowid(dest, src, eng)
        eng.execute(sql)
        update_rowid(dest, src, eng)
        print(f"Data has been transferred from {src} to {dest}, with the exception of duplicate records")
        return [f"Data has been transferred from {src} to {dest}, with the exception of duplicate records"]
    except Exception as e:
        print(e)
        return [f"Error moving data from {src} to {dest}:\n{str(e)[:500]}"]



