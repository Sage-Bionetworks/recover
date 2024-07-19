/*
  Create a parquet schema (if it doesn't yet exist) and deploy all child objects.

  Jinja templating variables:
  git_branch - The name of the git branch from which we are deploying.
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
EXECUTE IMMEDIATE
$$
BEGIN
    IF ('{{ git_branch }}' = 'main') THEN
        -- Our procedures will reference the prod stage
        EXECUTE IMMEDIATE
            FROM './procedure/deploy.sql'
            USING (
                stage_name => $parquet_prod_stage_name,
                file_format => $parquet_file_format_name
            );
    ELSE
        EXECUTE IMMEDIATE
            FROM './procedure/deploy.sql'
            USING (
                stage_name => $parquet_dev_stage_name,
                file_format => $parquet_file_format_name
            );
    END IF;
END;
$$;
