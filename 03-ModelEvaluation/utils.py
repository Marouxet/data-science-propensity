from google.cloud import bigquery
from query import QUERY
from sklearn.metrics import f1_score,precision_score,recall_score,roc_auc_score,confusion_matrix


project_id = ''

# Function to query in GA dataset
def parse_query(week):
    # During development, you can pass an string in format YYYY-MM-AA 
    # to query on the week starting that day
   
    query = QUERY.format(start_date = week)

    client_bq = bigquery.Client(project = project_id)
    df = client_bq.query(query).to_dataframe()
    return df

def get_metrics(df,th=0.5):

    # Make predictions based on proba and th
    df['prediction'] = 0
    df.loc[df["predicted_value"] >= th,'prediction'] = 1

    model = df.loc[0 ,"model"]
    predicted_week = df.loc[0 ,"predicted_week"]

    roc_auc = roc_auc_score(df.true_value.astype('int'),df.predicted_value)
    f1 = f1_score(df.true_value.astype('int'),df.prediction)
    precision = precision_score(df.true_value.astype('int'),df.prediction)
    recall = recall_score(df.true_value.astype('int'),df.prediction)

    tn, fp, fn, tp = confusion_matrix(df.true_value.astype('int'),df.prediction).ravel()
    
    return (model,predicted_week,f1,precision,recall,roc_auc,tn,fp,fn,tp)
