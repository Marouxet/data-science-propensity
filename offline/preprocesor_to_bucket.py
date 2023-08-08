from google.cloud import storage
import pickle
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer



# Create the Pipeline
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


# Save model in a pickle locally 
local_model_filename = "mariano_preprocessor.pkl"
with open(local_model_filename, 'wb') as file:
    pickle.dump(preprocessor, file)

# Upload pickle to storage
storage_client = storage.Client(project='')
bucket = storage_client.bucket("bucket")
blob = bucket.blob("mariano_preprocessor.pkl")
blob.upload_from_filename(local_model_filename)

