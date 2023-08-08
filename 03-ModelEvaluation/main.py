import functions_framework
from utils import *
from datetime import date, timedelta
import pandas as pd
import pandas_gbq

@functions_framework.http
def main(request):

    if "week" not in request.args:
        week = (date.today()-timedelta(1)).strftime('%Y-%m-%d')
    else:
        week = request.args["week"]
   
    # Get predictions and true values
    df = parse_query(week)
    # Print head -> DEV
    df.head()

    # Get model metrics based on probabilies
    model,predicted_week,f1,precision,recall,roc_auc,tn,fp,fn,tp = get_metrics(df,th=0.5)
    # print values
    print((model,predicted_week,f1,precision,recall,roc_auc,tn,fp,fn,tp))

    # Save metadata to bigquery
    
    metadata = pd.DataFrame([[model,predicted_week,f1,precision,recall,roc_auc,tn,fp,fn,tp]])
    metadata.columns =["file_name_in_bucket", "predicted_week","f1_score","precision","recall","roc_acu",'tn','fp','fn','tp']
    
    pandas_gbq.to_gbq(
        metadata,
        "dataset.ma_model_evaluation", 
        project_id='', 
        if_exists='append',
    )

    return "OK"