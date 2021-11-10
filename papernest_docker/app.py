import psycopg2
import pandas as pd
from ftplib import FTP
from io import BytesIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import yaml


conf= yaml.safe_load(open('cred.yml'))

database_db = conf['db']['database']
user_db = conf['db']['user']
host_db = conf['db']['host']
password_db = conf['db']['password']
port_db = conf['db']['port']

host_ftp = conf['ftp']['host']
port_ftp = conf['ftp']['port']
user_ftp = conf['ftp']['user']
pwd_ftp = conf['ftp']['passwd']

mail_address = conf['mail']['adress']
mail_pwd = conf['mail']['pwd']


#function for the database extraction
def db_extract():
    con = psycopg2.connect(
        database=database_db,
        user=user_db,
        host=host_db,
        password=password_db,
        port=port_db
        )
    cur = con.cursor()

    #retrieving latest creation date
    cur.execute('SELECT "CreationDate" \
        FROM clients_crm \
        ORDER BY "CreationDate" DESC \
        LIMIT 1')
    latest_date = cur.fetchall()

    #retrieving client info from that date
    cur.execute('SELECT * \
        FROM clients_crm \
        WHERE "CreationDate"=(%s)',
        (latest_date))
    data = cur.fetchall()

    cols = []
    for elt in cur.description:
        cols.append(elt[0])
    df = pd.DataFrame(data=data, columns=cols)
    con.close()
    return(df)


#function for the ftp file extraction
def ftp_extract():
    server = FTP()
    server.connect(
        host=host_ftp,
        port=port_ftp)
    server.login(
        user=user_ftp,
        passwd=pwd_ftp
    )
    server.cwd("files")

    f = BytesIO()
    server.retrbinary("RETR raw_calls.csv", f.write)
    f.seek(0)
    df = pd.read_csv(f)
    server.close()
    return(df)


#function to clean the ftp file
def clean_ftp_data(data):
    data_clean = data.copy(deep=True)
    data_clean = data_clean.dropna(axis=0)
    data_clean['incoming_number'] = data_clean['incoming_number'].astype(str)
    data_clean['incoming_number'] = data_clean['incoming_number'].str[:-2]
    data_clean['incoming_number'] = \
        data_clean['incoming_number'].apply(lambda x: '0' + x)
    data_temp = data_clean.groupby('incoming_number').mean().reset_index()
    data_clean = \
        data_clean.merge(data_temp[['incoming_number','duration_in_sec']], 
            how='left', left_on='incoming_number', right_on='incoming_number',
            suffixes=('', '_mean'))
    data_clean = data_clean.sort_values(by=["date"])
    data_clean = \
        data_clean.drop_duplicates(subset=['incoming_number'], keep='last')
    return(data_clean[['incoming_number', 'duration_in_sec_mean', 'date']])


#merging the 2 extract files
def merge_db_ftp(ftp_data, db_data):
    data_merged = ftp_data.merge(db_data, 
        how='outer', 
        left_on='incoming_number',
        right_on='PhoneNumber')
    return(data_merged)


#function to send a mail
def send_mail():
    gmail_user = mail_address
    mdp = mail_pwd

    sender_address = "nhan.test.papernest@gmail.com"
    receiver_address = "alexandre.colicchio@papernest.com"

    message = """ 
    Bonjour,
    
    Voici le fichier final.
    Cordialement.
    
    Nhan Nguyen
    """

    msg = MIMEMultipart()
    msg['From'] = sender_address
    msg['To'] = receiver_address
    msg['Subject'] = 'Test technique Nhan Nguyen'
    msg.attach(MIMEText(message, 'plain'))
    filename = "extract.csv"
    attachment = open(filename, "rb")
    p = MIMEBase('application', 'octet-stream')
    p.set_payload((attachment).read())  
    encoders.encode_base64(p)
    p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    msg.attach(p)
    text = msg.as_string()

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.login(gmail_user, mdp)
        server.sendmail(sender_address, receiver_address, text)
        print("Success")
    except:
        print("Failure")


def main():
    db_data = db_extract()
    ftp_data = ftp_extract()
    ftp_data_clean = clean_ftp_data(ftp_data)
    final_data = merge_db_ftp(ftp_data_clean, db_data)
    final_data.to_csv('extract.csv')
    send_mail()


if __name__ == '__main__':
    main()
