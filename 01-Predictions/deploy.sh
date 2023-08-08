gcloud functions deploy bads_t5_ma_01_predictions \
--runtime=python39 \
--trigger-http \
--entry-point=predecir \
--allow-unauthenticated \
--memory=8192mb \
--service-account=ba-datascience@mightyhive-data-science-poc.iam.gserviceaccount.com \
--timeout=540s