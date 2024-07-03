/*
  Create an external stage over the Parquet data in S3
*/
CREATE OR REPLACE STAGE {{ parquet_stage_name }}
  URL = 's3://recover-processed-data/{{ git_branch }}/parquet/'
  STORAGE_INTEGRATION = recover_prod_s3;
