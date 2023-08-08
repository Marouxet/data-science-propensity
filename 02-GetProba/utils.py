from google.cloud import bigquery

project_id = ''

# Function to query in GA dataset
def get_proba(user_id):

    query = f"""
            SELECT 
                proba
            FROM 
                `project.dataset.ma_predictions_table` 
            WHERE 
                user_id = "{user_id}"
    """
    client_bq = bigquery.Client(project = project_id)
    df = client_bq.query(query).to_dataframe()
    if df.shape[0] == 0:
        return f"No data about user {user_id}"
    return str(df.iloc[0,0])