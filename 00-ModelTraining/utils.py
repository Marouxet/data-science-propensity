from google.cloud import bigquery, storage
import pickle

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score,precision_score,recall_score,roc_auc_score

from imblearn.under_sampling import RandomUnderSampler
from imblearn.over_sampling import SMOTE

import pandas as pd

import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from query import QUERY


project_id = ''

# Query
def query_parser(trigger_date):
  
    query = QUERY.format(start_date = trigger_date)
    client_bq = bigquery.Client(project = project_id)
    df = client_bq.query(query).to_dataframe()
    return df

# Preprocesing using SKLEARN pipeline
def preprocessing(df):

    # Drop null rows
    df = df.dropna(axis=0)
    # Set user_id as index
    df = df.set_index("user_id")
    # Format label as int (sometimes it's a string)
    df['label'] = df['label'].apply(int)

    # Pipeline
    # First I generate list of features for each group (based on transformation needed)
    numeric_features = [
                        'transferencias',
                        'distinct_sections',
                        'pf'
    ]

    categorical_features = [
                        'user_type'
    ]

    passthrough_features = [
                            'acciones_ars', 
                            'bonos_usd', 
                            'bonos_ars', 
                            'plazos_fijos_usd',
                            'plazos_fijos_ars',
                            'fima_usd', 
                            'fima_ars',
                            'cedears_ars', 
                            'cedears_usd', 
                            'inversor',
                            'seccion_inversiones',
    ]

    # Now I define the diferent transformers I will use
    numeric_transformer = Pipeline(steps=[
        ('scaler', StandardScaler())
    ])
    categorical_transformer = Pipeline(steps=[
        ('encoder', OneHotEncoder())
    ])

    # Now I generate the ColumnTransformer for all the features
    preprocessor = ColumnTransformer(
            transformers=[
            ('numeric', numeric_transformer, numeric_features),
            ('categorical', categorical_transformer, categorical_features),
            ('passthrough','passthrough',passthrough_features)
    ],
            verbose=False
    ) 

    # Apply pipeline
    data_processed= preprocessor.fit_transform(df.drop("label", axis = 1))

    return data_processed

def split_train_test(X,y):
    # Split in train y test following a methodology pre-defined for this model
    X_train, X_test, y_train, y_test = train_test_split(
        X, 
        y, 
        test_size=0.2, 
        random_state=42,
        stratify=y
    )

    return X_train, X_test, y_train, y_test 

def balancing_training_data(X_train,y_train):
    # Balancing dataset to fit model
    rus = RandomUnderSampler(sampling_strategy=0.4)
    smote = SMOTE()

    X_rus, y_rus = rus.fit_resample(X_train, y_train.astype('int'))
    X_smote, y_smote = smote.fit_resample(X_rus, y_rus.astype('int'))

    return X_smote,y_smote

def fit_model(X,y):
    # Training a Random Forest Classifier -> Change this for the best model after tunning hiperparameters
    rfc = RandomForestClassifier().fit(X, y.values.ravel())
    return rfc

def model2bucket(model,bucket_name, trigger_date):

    '''
    This function create a name for the model, that works as an ID. 
    Then it save the model in a pickle and after that it uploads the pickle to the bucket in GCP

    * model is the SKLEARN object
    * bucket_name is the name of the bucket where the model must be uploaded

    '''
   
    # Create model name and name of the file to save the model locally in temporal folder
    model_name = "prefix-" + trigger_date + ".pkl"
    local_model_filename = '/tmp/' + model_name

    # Save model in a pickle locally 
    with open(local_model_filename, 'wb') as file:
        pickle.dump(model, file)
    
    # Upload model to Google Cloud Storage using helper function
    upload_blob(bucket_name, local_model_filename, model_name)

    return bucket_name, model_name

def model_evaluation(model,X_test,y_test):

    y_predict = model.predict(X_test)
    y_proba = model.predict_proba(X_test)

    f1 = f1_score(y_test.astype('int'),y_predict)
    precision = precision_score(y_test.astype('int'),y_predict)
    recall = recall_score(y_test.astype('int'),y_predict)
    roc_auc = roc_auc_score(y_test.astype('int'),y_proba[:, 1])

    return (f1,precision,recall,roc_auc)


# Function to download something from a bucket in gcp
def load_from_bucket(file_name,bucket_name):
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)

    blob = bucket.get_blob(file_name)
    pkl = blob.download_as_string()
    loaded = pickle.loads(pkl)

    print('Object loaded')
    return loaded


# Function to upload something to a bucket in gcp
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    return True


def send_mail(mail_content, mail_subject):

  #The mail addresses and password
  sender_address = ''
  sender_pass = '' # APP PSW, setear en google account
  receiver_address = ''

  #Setup the MIME
  message = MIMEMultipart()
  message['From'] = sender_address
  message['To'] = receiver_address
  message['Subject'] = mail_subject

  # Add body to email
  message.attach(MIMEText(mail_content, 'plain'))

  #  convert message to string
  text = message.as_string()

  #Log in to server using secure context and send email
  context = ssl.create_default_context()
  with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
      server.login(sender_address, sender_pass)
      server.sendmail(sender_address, receiver_address, text)

  return f'Mail Sent to {receiver_address}'
