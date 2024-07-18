/*
  Create an external stage over the dev Parquet data in S3
*/
CREATE OR REPLACE STAGE {{ parquet_stage_name }}
  URL = 's3://recover-dev-processed-data/{{ git_branch }}/parquet/'
  STORAGE_INTEGRATION = recover_dev_s3;
