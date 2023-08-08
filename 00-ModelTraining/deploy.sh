gcloud functions deploy ma_00_model_training \
--runtime=python39 \
--trigger-http \
--entry-point=modelo \
--allow-unauthenticated \
--memory=8192mb \
--service-account=service_account_mail \
--timeout=540s