import functions_framework
import pandas as pd
import pandas_gbq 
import numpy as np
from utils import *
from datetime import date

@functions_framework.http
def modelo(request):

    if "trigger_date" not in request.args:
        trigger_date = date.today().strftime('%Y-%m-%d')
    else:
        trigger_date = request.args["trigger_date"]
   
    # The cloud function is triggered when the table is updated in bigquery, no params needed.
    df = query_parser(trigger_date)

    # Get X and y
    X = preprocessing(df)
    y = df[['label']]
    print("query done")
    
    # Split in train and test
    X_train, X_test, y_train, y_test = split_train_test(X,y)
    print("split train test done")
    
    # Balancing training data
    X_train_b, y_train_b = balancing_training_data(X_train,y_train)
    
    print("balancing done")
    
    # Fitting model
    model = fit_model(X_train_b, y_train_b)
    print("model fitting done")

    # Saving model in Google cloud storage and geeting bucket name
    bucket_name = 'bads-training'
    bucket_name,model_name = model2bucket(model,bucket_name,trigger_date)
    print(f'model {model_name} saved in the bucket {bucket_name}')
        
    # Metrics
    f1,precision,recall,roc_auc = model_evaluation(model,X_test,y_test)
    print("metrics done")
    
    # Save metadata to bigquery
    
    metadata = pd.DataFrame([[trigger_date, model_name,f1,precision,recall,roc_auc]])
    metadata.columns =["training_date", "file_name_in_bucket","f1_score","precision","recall","roc_acu"]
    
    pandas_gbq.to_gbq(
        metadata,
        "project.dataset.ma_model_training_metadata", 
        project_id='', 
        if_exists='append',
    )

     
    # Envio mail con contenido 
    mail_content = f"""
    Model fitted and saved to Google Cloud Storage.

    * GCP Bucketin:{bucket_name}
    * Folder and name:{model_name}

    Model metrics

    * F1: {np.round(f1,4)}
    * Precision : {np.round(precision,4)}
    * Recall : {np.round(recall,4)}
    * ROC AUC Score : {np.round(roc_auc,4)}

    """
    mail_subject = f'Model fitted and saved to Google Cloud Storage'
    
    message = send_mail(mail_content, mail_subject)
    return "DONE"