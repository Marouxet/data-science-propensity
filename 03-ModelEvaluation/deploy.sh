gcloud functions deploy ma_03_model_evaluation \
--runtime=python39 \
--trigger-http \
--entry-point=main \
--allow-unauthenticated \
--memory=8192mb \
--service-account=service_account_mail \
--timeout=540s