import smtplib
import pandas as pd
from utils.utility import getMYSQLConnection, killFile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

class mailClass():
    def __init__(self,conf):
        self.from_  = conf["mail"]["from"]
        self.to     = conf["mail"]["to"]
        self.pass_  = conf["mail"]["pass"]
        self.conf   = conf

    def exportToCSV(self,tbName):
        with getMYSQLConnection(self.conf) as con:            
            df = pd.read_sql(f"SELECT * FROM {tbName}",con)
            df.to_csv(self.path)        
        
    def sendMail(self,
                 body=None,
                 subject='Your Mail is served.',
                 tbName=None):

        #Setup the MIME
        message = MIMEMultipart()
        message['From'] = self.from_
        message['To'] = self.to
        message['Subject'] = subject
        if not body is None:
            message.attach(MIMEText(body, 'plain'))

        if not tbName is None:
            self.path = f'{tbName}.csv'
            killFile(self.path)
            self.exportToCSV(tbName)
            payload = MIMEBase('application', 'octate-stream')
            with open(self.path, 'rb') as f: # Open the file as binary mode
                payload.set_payload((f).read())
            encoders.encode_base64(payload) #encode the attachment
            #add payload header with filename
            payload.add_header('Content-Decomposition', 'attachment', filename=self.path)
            message.attach(payload)
            killFile(self.path)

        #Create SMTP session for sending the mail
        session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
        session.starttls() #enable security
        session.login(self.from_, self.pass_) #login with mail_id and password
        text = message.as_string()
        session.sendmail(self.from_, self.to, text)
        session.quit()

        print('Mail Sent')
        
#%%
if __name__ == '__main__':
    mail = mailClass(conf)
    mail.sendMail(subject="help",body='testing',tbName='decimals')