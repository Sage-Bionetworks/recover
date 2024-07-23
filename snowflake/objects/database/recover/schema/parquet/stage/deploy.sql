/*
  Deploy all stages under the `parquet` schema.
*/
EXECUTE IMMEDIATE
    FROM './parquet_prod_s3.sql'
    USING (
        git_branch => '{{ git_branch }}',
        parquet_stage_name => '{{ parquet_prod_stage_name }}'
    );
EXECUTE IMMEDIATE
    FROM './parquet_dev_s3.sql'
    USING (
        git_branch => '{{ git_branch }}',
        parquet_stage_name => '{{ parquet_dev_stage_name }}'
    );
