/*
  This is the primary deployment script for RECOVER Snowflake objects.

  Deployments are made to a specific environment. An environment is analogous
  to a database. Account-level objects are not covered by this script. We assume
  that the following objects have already been created at the account level:

    - ROLE `RECOVER_DATA_ENGINEER`
        * A ROLE with the appropriate privelages to create and deploy
          objects in a database.
    - WAREHOUSE `RECOVER_XSMALL`
    - API INTEGRATION `RECOVER_GIT`
        * An API INTEGRATION with an API_ALLOWED_PREFIXES value
          containing 'https://github.com/Sage-Bionetworks/' and
          API_PROVIDER = GIT_HTTPS_API.
    - STORAGE INTEGRATION `RECOVER_PROD_S3`
        * An S3 storage integration which allows access to the
          S3 buckets in the RECOVER production account.

  Additionally, we assume that the following databases have already been created
  when deploying to the "staging" or "main" environment, respectively:

    - DATABASE `RECOVER_STAGING`
    - DATABASE `RECOVER_MAIN`

  To deploy to an environment identified by your current git branch,
  we can invoke this file with the Snowflake CLI like so:

  `snow sql -D "environment=$(git rev-parse --abbrev-ref HEAD)" -f {this file}`
*/

/*
  Configure the runner
*/
USE ROLE RECOVER_DATA_ENGINEER;
USE WAREHOUSE RECOVER_XSMALL;

/*
  Configure and bootstrap the database if this is a dev environment.

  Since we typically want to use the branch name as the environment identifier,
  but only A-Za-z0-9 and _ and $ characters are allowed in Snowflake identifier
  names and we have the convention of naming the branch after its associated
  Jira ticket (e.g., etl-123), we substitute a likely hyphen in the `environment`
  variable passed at runtime.
*/
SET safe_environment_identifier = (SELECT REPLACE('&{ environment }', '-', '_'));
SET database_identifier = CONCAT('recover_', $safe_environment_identifier);

EXECUTE IMMEDIATE
$$
BEGIN
    IF ('&{ environment }' != 'main' AND '&{ environment }' != 'staging') THEN
        -- This is a dev environment. Deploy from scratch.
        CREATE OR REPLACE DATABASE IDENTIFIER($database_identifier);
    END IF;
END;
$$;
USE DATABASE IDENTIFIER($database_identifier);

/*
  Create an external stage over the RECOVER Git repository so that we can
  use EXECUTE IMMEDIATE FROM statements.
*/
CREATE OR REPLACE GIT REPOSITORY recover_git_repository
    ORIGIN = 'https://github.com/Sage-Bionetworks/recover.git'
    API_INTEGRATION = RECOVER_GIT;

/*
  Deploy our database and all its child objects.
*/
EXECUTE IMMEDIATE
    FROM @recover_git_repository/branches/&{ environment }/snowflake/objects/database/recover/deploy.sql
    USING (
        environment => $safe_environment_identifier,
        git_branch => '&{ environment }'
    );
