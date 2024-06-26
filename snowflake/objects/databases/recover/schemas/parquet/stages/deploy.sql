/*
  Deploy all stages under the `parquet` schema.
*/
EXECUTE IMMEDIATE
    FROM './parquet_s3.sql'
    USING (
        git_branch => '{{ git_branch }}',
        parquet_stage_name => '{{ parquet_stage_name }}'
    );
