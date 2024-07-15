/*
  Create a recover database (if it doesn't yet exist) for an environment and
  deploy all child objects.
*/
CREATE DATABASE IF NOT EXISTS recover_{{ environment }};
USE DATABASE recover_{{ environment }};

EXECUTE IMMEDIATE
    FROM './schema/deploy.sql'
    USING (
        git_branch => '{{ git_branch }}'
    );
