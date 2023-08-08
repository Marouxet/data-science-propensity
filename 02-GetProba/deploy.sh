gcloud functions deploy ma_02_get_proba \
--runtime=python39 \
--trigger-http \
--entry-point=main \
--allow-unauthenticated \
--memory=8192mb \
--service-account=service_account_mail \
--timeout=540s