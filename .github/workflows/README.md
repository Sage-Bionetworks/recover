# recover github workflows

## Overview

Recover ETL has four github workflows:

- workflows/upload-and-deploy.yaml
- workflows/upload-and-deploy-to-prod-main.yaml
- workflows/codeql-analysis.yml
- workflows/cleanup.yaml

| Workflow name                  | Scenario it's run                                            |
|:-------------------------------|:-------------------------------------------------------------|
| upload-and-deploy              | on-push from feature branch, feature branch merged into main |
| upload-and-deploy-to-prod-main | whenever a new tag is created                                |
| codeql-analysis                | on-push from feature branch, feature branch merged into main |
| cleanup                        | feature branch deleted                                       |


## upload-files

Copies pilot data sets from ingestion bucket to input data bucket for use in integration test. Note that this behavior assumes that there are files in the ingestion bucket. Could add updates to make this robust and throw an error if the ingestion bucket path is empty.

## upload-and-deploy

Here are some more detailed descriptions and troubleshooting tips for some jobs within each workflow:

### Current Testing Related Jobs

#### nonglue-unit-tests

See [testing](/tests/README.md) for more info on the background behind these tests. Here, both the `recover-dev-input-data` and `recover-dev-processed-data` buckets' synapse folders are tested for STS access every time something is pushed to the feature branch and when the feature branch is merged to main.

This is like an integration test and because it depends on connection to Synapse, sometimes the connection will be stalled, broken, etc. Usually this test will only take 1 min or less. Sometimes just re-running this job will do the trick.

#### pytest-docker

This sets up and uploads the two docker images to ECR repository.
**Note: A ECR repo called `pytest` would need to exist in the AWS account we are pushing docker images to prior to running this GH action.**

Some behavioral aspects to note - there were limitations with the matrix method in Github action jobs thus had to unmask account id to pass it as an output for `glue-unit-tests` to use. The matrix method at this time [see issue thread](https://github.com/orgs/community/discussions/17245) doesn't support dynamic job outputs and the workaround seemed more complex to implement, thus we weren't able to pass the path of the uploaded docker container directly and had to use a static output. This leads us to use `steps.login-ecr.outputs.registry` which contains account id directly so the output could be passed and the docker container could be found and used.

#### glue-unit-tests

See [testing](/tests/README.md) for more info on the background behind these tests.

For the JSON to Parquet tests sometimes there may be a scenario where a github workflow gets stopped early due to an issue/gets canceled.

With the current way when the `test_json_to_parquet.py` run, sometimes the glue table, glue crawler role and other resources may have been created already for the given branch (and didn’t get deleted because the test didn’t run all the way through) and will error out when the github workflow gets triggered again because it hits the `AlreadyExistsException`. This is currently resolved manually by deleting the resource(s) that has been created in the AWS account and re-running the github jobs that failed.

### Adding Test Commands to Github Workflow Jobs

After developing and running tests locally, you need to ensure the tests are run in the CI pipeline. To add your tests to under the `upload-and-deploy` job:

Add your test commands under the appropriate job (see above for summaries on the specific testing related jobs), for example:

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      # Other steps...
      - name: Run tests
        run: |
          pytest tests/

```

### sceptre-deploy-develop

### integration-test-develop-cleanup

This is responsible for cleaning up any data locations that are used by integration
tests. This is used after `sceptre-deploy-develop`, but before
`integration-test-develop`. Cleans these locations:

* `s3://recover-dev-input-data/$GITHUB_REF_NAME/`
* `s3://recover-dev-intermediate-data/$GITHUB_REF_NAME/json/`


### integration-test-develop

This builds the S3 to JSON lambda and triggers it with the pilot data so that the Recover ETL Glue Workflow will start running and processing the pilot data. **Note** that this will run with every push to the feature branch so it would be good to wait until one run of the Glue workflow finishes running as we cannot have more than 1 concurrent Glue Workflow run.

Note that using `sam build` or `sam invoke` here means that the Github workflow will **NOT** throw any error even if the build or invoke fails. You will have to check the Github workflow run logs or check the Glue Workflow run to make sure the workflow was started.

This integration test means that you have to wait until the glue workflow has finished to completion and review the resulting parquet datasets and compare parquet comparison results of the current run prior to merging this into `main`.

### sceptre-deploy-staging

Here that we are **NOT** configuring a S3 event notification configuration for our `prod/staging` space because we plan to submit data to `staging` "manually" after merging a PR into main and triggering the GitHub workflow.

### integration-test-staging-cleanup

This is responsible for cleaning up any data locations that are used by integration
tests during the staging run. This is used after `sceptre-deploy-staging`, but before
`integration-test-staging`. Cleans these locations:

* `s3://recover-input-data/staging/`
* `s3://recover-intermediate-data/staging/json/`


## upload-and-deploy-to-prod-main

This runs **ONLY** when we create a new tag.

### sts-access-test

Here, both the (prod) `recover-input-data` and `recover-processed-data` buckets' synapse folders are tested for STS access every time a new tag is created for main.

This is like an integration test and because it depends on connection to Synapse, sometimes the connection will be stalled, broken, etc. Usually this test will only take 1 min or less. Sometimes just re-running this job will do the trick.

## cleanup

This deletes **ONLY** the following:

- **namespaced** stacks
- cloudformation artifacts (e.g: scripts)
