# email routine imports
import smtplib, inspect
from sqlite3 import register_adapter
import pandas as pd
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from email import encoders

# The email function we copy pasted from stackoverflow
def send_mail(send_from, send_to, subject, text, filename=None, server="localhost"):
    msg = MIMEMultipart()
    
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    
    msg_content = MIMEText(text)
    msg.attach(msg_content)
    
    if filename is not None:
        attachment = open(filename,"rb")
        p = MIMEBase('application','octet-stream')
        p.set_payload((attachment).read())
        encoders.encode_base64(p)
        p.add_header('Content-Disposition','attachment; filename= %s' % filename.split("/")[-1])
        msg.attach(p)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

def primary_key(table, eng):
    '''
    table is the tablename you want the primary key for
    eng is the database connection
    '''

    sql = f'''
        SELECT
            tc.TABLE_NAME,
            C.COLUMN_NAME,
            C.data_type 
        FROM
            information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage AS ccu USING ( CONSTRAINT_SCHEMA, CONSTRAINT_NAME )
            JOIN information_schema.COLUMNS AS C ON C.table_schema = tc.CONSTRAINT_SCHEMA 
            AND tc.TABLE_NAME = C.TABLE_NAME 
            AND ccu.COLUMN_NAME = C.COLUMN_NAME 
        WHERE
            constraint_type = 'PRIMARY KEY' 
            AND tc.TABLE_NAME = '{table}';
    '''

    return pd.read_sql(sql, eng).column_name.tolist()



# Get the registration id from the geodatabase
def registration_id(tablename, conn):
    reg_ids = pd.read_sql(f"SELECT registration_id, table_name FROM sde.sde_table_registry WHERE table_name = '{tablename}';", conn).registration_id.values
    
    if (len(reg_ids) > 0):
        return reg_ids[0]
    
    return None
    


# Get what the next object ID would be for the table
def next_objectid(tablename, conn):
    reg_id = registration_id(tablename, conn)
    if reg_id:
        if not pd.read_sql(f"SELECT * FROM information_schema.tables WHERE table_name = 'i{reg_id}'", conn).empty:
            return pd.read_sql(f"SELECT base_id FROM i{reg_id}", conn).base_id.values[0]

    # default row id when missing is -220, i think
    return -220




def update_rowid(dest_table, src_table, conn, dest_rowid_column = 'objectid', src_rowid_column = 'objectid'):
    
    sql = f"""
        SELECT MAX(tfinal.objectid) AS objectid FROM 
            (
                SELECT t1.{dest_rowid_column} AS objectid FROM {dest_table} t1 UNION SELECT t2.{src_rowid_column} AS objectid FROM {src_table} t2
            ) tfinal
        """
    
    next_id = pd.read_sql(sql, conn).objectid.values

    if len(next_id) > 0:
        next_id = next_id[0]
    else:
        # inspect.stack()[0][3] gets the current function name
        raise Exception(f"Error in {inspect.stack()[0][3]}: Query to get next objectid came up empty:\n{sql}")

    reg_id = registration_id(dest_table, conn)
    if reg_id:
        conn.execute(f"UPDATE i{reg_id} SET base_id = {next_id}, last_id = {next_id - 1};")
        return next_id
    return -1
    



def exception_handler(func):
    def callback(*args, **kwargs):
        try:
            report = func(*args, **kwargs)
            return report
        except Exception as e:
            # send_mail('admin@checker.sccwrp.org', ['kevinl@sccwrp.org'], "ERROR WITH CEDEN DATA SYNC", f"Error occurred in {func.__name__}:\n{str(e)[:1000]}", server = '192.168.1.18')
            
            # Returns a list to match the output type of the other functions, which also return lists
            # the lists are to be combined to one list for the final report.
            # returning the string in the list ensures it can be included in the report
            print(e)
            return [f'Unexpected error in {func.__name__}:\nArguments: {args}\n{str(e)[:1000]}']
    return callback

class DotDict(dict):     
    """dot.notation access to dictionary attributes"""      
    def __getattr__(*args):         
        val = dict.get(*args)         
        return DotDict(val) if type(val) is dict else val      
        
    __setattr__ = dict.__setitem__     
    __delattr__ = dict.__delitem__ 