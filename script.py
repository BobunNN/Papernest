import psycopg2
import pandas as pd
from ftplib import FTP
from io import BytesIO


def db_extract():
    con = psycopg2.connect(
        database='souscritootest',
        user='testread',
        host="souscritootest.cuarmbocgsq7.eu-central-1.rds.amazonaws.com",
        password="testread",
        port="5432"
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


def ftp_extract():
    server = FTP()
    server.connect(
        host="35.157.119.136",
        port=21)
    server.login(
        user="candidate",
        passwd="XHf8CAwZFzTtfK7qxZ"
    )
    server.cwd("files")

    f = BytesIO()
    server.retrbinary("RETR raw_calls.csv", f.write)
    f.seek(0)
    df = pd.read_csv(f)
    server.close()
    return(df)


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
    print(data_clean.shape[0])
    data_clean = \
        data_clean.drop_duplicates(subset=['incoming_number'], keep='last')
    return data_clean


def merge_db_ftp(ftp_data, db_data):
    data_merged = ftp_data.merge(db_data, 
        how='outer', 
        left_on='incoming_number',
        right_on='PhoneNumber')
    return(data_merged)


def main():
    db_data = db_extract()
    ftp_data = ftp_extract()
    ftp_data_clean = clean_ftp_data(ftp_data)
    final_data = merge_db_ftp(ftp_data_clean, db_data)
    print(final_data)

if __name__ == '__main__':
    main()