import functions_framework
from utils import *
import pandas_gbq
from datetime import date, timedelta

@functions_framework.http
def predecir(request):
    # We can pass a week parameter in the URL to make prediction for the week starting that day
    # If no parameter is passed, predictions are made for the week starting in the current day.

    if "week" not in request.args:
        week = (date.today()-timedelta(1)).strftime('%Y-%m-%d')
    else:
        week = request.args["week"]
    # Query using current_date by default (this is run weekly) or using one particular week, passing
    # the starting day of the week in format "YYYY-MM-DD"
    df = query_parser(week = week)
    
    # Get processor
    preprocessor = load_from_bucket('preprocessor.pkl','bucket_name')

    # Get last model (based on metadata table)
    model_name = get_model(week)

    model = load_from_bucket(model_name,'bucket_name')

    # Preprocessing data 
    X, ids = processing(df,preprocessor)

    # Make predictions
    y_predict = predict(model,X,ids)
   
    # Prepare data to save it in bigquey
    y_predict = y_predict.reset_index()
    y_predict.columns = ['user_id','proba']
    y_predict['model'] = model_name
    y_predict['predicted_week'] = week

    
    
    # Load data already in bigquery (last predictions)
    data_old = get_bqtable()
    data_old = data_old.rename(columns={"proba":"old_proba","model":"old_model","predicted_week":"old_predicted_week"})

    # Concatenate old and new predictions in one dataframe
    data  = pd.concat([data_old,y_predict.set_index('user_id')], axis = 1)
    
    # replace nulls values in new columns with old values
    data.loc[data.proba.isna(),'proba'] = data.loc[data.proba.isna(),'old_proba']
    data.loc[data.model.isna(),'model'] = data.loc[data.model.isna(),'old_model']
    data.loc[data.predicted_week.isna(),'predicted_week'] = data.loc[data.predicted_week.isna(),'old_predicted_week']

  
    data = data[['proba','model','predicted_week']].reset_index()
   
 
    
    # Save predictions to bigquery
    pandas_gbq.to_gbq(
        data,
        "dataset.ma_predictions_table", 
        project_id='project_id', 
        if_exists='replace',
    )
    
    return "OK"