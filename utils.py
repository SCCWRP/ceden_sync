# email routine imports
import smtplib
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

    
class DotDict(dict):     
    """dot.notation access to dictionary attributes"""      
    def __getattr__(*args):         
        val = dict.get(*args)         
        return DotDict(val) if type(val) is dict else val      
        
    __setattr__ = dict.__setitem__     
    __delattr__ = dict.__delitem__ 