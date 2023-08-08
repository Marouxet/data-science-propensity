from google.cloud import bigquery, storage
import pickle
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
import pandas as pd
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from query import QUERY

project_id = ''

# Function to query in GA dataset
def query_parser(week):
    # During development, you can pass an string in format YYYY-MM-AA 
    # to query on the week starting that day
   
    query = QUERY.format(start_date = week)

    client_bq = bigquery.Client(project = project_id)
    df = client_bq.query(query).to_dataframe()

    # Steps outside pipeline
    df = df.dropna(axis=0)
    df = df.set_index("user_id")

    print('Query ready')
    return df

# Function to get the last trained model using metadata table in bigquery
def get_model(week):
    query = """
    SELECT 
        file_name_in_bucket 
    FROM 
        `project.dataset.ma_model_training_metadata` 
    WHERE 
        training_date < "{week}"
    ORDER BY
        training_date DESC 
    LIMIT 1
    """.format(week=week)
    
    client_bq = bigquery.Client(project = project_id)
    df = client_bq.query(query).to_dataframe()
    print(df.iloc[0,0])
    return df.iloc[0,0]

def get_bqtable():
    query = "SELECT * from  `project.dataset.ma_predictions_table`"
    client_bq = bigquery.Client(project = project_id)
    df = client_bq.query(query).to_dataframe().set_index('user_id')
    return df

# Function to load something from a bucket in gcp
def load_from_bucket(file_name,bucket_name):
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)

    blob = bucket.get_blob(file_name)
    pkl = blob.download_as_string()
    loaded = pickle.loads(pkl)

    print('Object loaded')
    return loaded

# Function to process the original dataframe
def processing(df,processor):
  X = processor.fit_transform(df)
  ids  = df.index
  return (X,ids)

# Function to apply the model
def predict(model,X,ids):

    # Hago predicciones sobre X
    y_predict = pd.DataFrame(
        model.predict_proba(X),
        index = ids, 
        columns = ['class 0', 'class 1']
    )

    # Me quedo con la clase 1 - 1000 elementos
    y_predict = y_predict[['class 1']]
    return y_predict

def send_mail(mail_content, mail_subject, attach_file_name, receiver_address):


    #Reference: https://realpython.com/python-send-email/#adding-attachments-using-the-email-package

    #The mail addresses and password
    sender_address = ''
    sender_pass = '' # APP PSW, setear en google account


    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = mail_subject

    # Add body to email
    message.attach(MIMEText(mail_content, 'plain'))

    # Open PDF file in binary mode
    with open(attach_file_name, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {attach_file_name}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

   # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_address, sender_pass)
        server.sendmail(sender_address, receiver_address, text)

    return f'Mail Sent to {receiver_address}'