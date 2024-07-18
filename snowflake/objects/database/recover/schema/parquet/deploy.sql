/*
  Create a parquet schema (if it doesn't yet exist) and deploy all child objects.
*/
CREATE SCHEMA IF NOT EXISTS parquet;
USE SCHEMA parquet;

SET parquet_file_format_name = 'parquet_format';
SET parquet_prod_stage_name = 'parquet_prod_s3';
SET parquet_dev_stage_name = 'parquet_dev_s3';

EXECUTE IMMEDIATE
    FROM './file_format/deploy.sql'
    USING (
        parquet_file_format_name => $parquet_file_format_name
    );
EXECUTE IMMEDIATE
    FROM './stage/deploy.sql'
    USING (
        git_branch => '{{ git_branch }}',
        parquet_prod_stage_name => $parquet_prod_stage_name,
        parquet_dev_stage_name => $parquet_dev_stage_name
    );
EXECUTE IMMEDIATE
    FROM './table/deploy.sql';
