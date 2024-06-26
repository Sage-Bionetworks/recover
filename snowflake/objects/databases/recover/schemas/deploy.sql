/*
  Deploy schemas and their child objects.
*/
EXECUTE IMMEDIATE
    FROM './parquet/deploy.sql'
    USING (
        git_branch => '{{ git_branch }}'
    );
