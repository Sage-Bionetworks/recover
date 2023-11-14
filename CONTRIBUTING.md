# Contributing

By contributing, you are agreeing that we may redistribute your work under this [license](LICENSE).

# Table of contents
- [Getting started as a developer](#getting-started-as-a-developer)
- [The development life cycle](#the-development-life-cycle)
   * [Jira](#jira)
   * [Developing and testing locally](#developing-and-testing-locally)
   * [Testing remotely](#testing-remotely)
   * [Code review](#code-review)
- [Code style](#code-style)

# Getting started as a developer

If you are onboarding to the RECOVER project as a developer, we provide [onboarding documentation via Confluence](https://sagebionetworks.jira.com/wiki/spaces/IBC/pages/2808053761/RECOVER+ETL#Getting-Started). There you will find everything you need to know about gaining write access to this GitHub repository and our AWS environments (and more)! An overview of the system architecture is [here](https://sagebionetworks.jira.com/wiki/spaces/IBC/pages/2741075969/RMHDR-48+RECOVER+ETL+Design). Documentation is only accessible to Sage Bionetworks employees.

# The development life cycle

To keep work organized and predictable, developers follow specific development patterns. For a high level overview of development patterns within the DPE team, see the [DPE Handbook](https://sagebionetworks.jira.com/wiki/spaces/IBC/pages/2667905251/DPE+handbook).

## Jira

Before there was work, there was the _work request_. Work requests a.k.a. tickets for this repository are tracked in the [ETL Jira project](https://sagebionetworks.jira.com/jira/software/c/projects/ETL/boards/190). No development work should be done without first having a Jira ticket which outlines the context and goals of the work to be done.

## Developing and testing locally

1. Clone this repository (do not fork). Create a feature branch off the `main` branch. Branch names must contain the identifier of the corresponding Jira ticket (e.g., `etl-574`). Optionally, add keywords describing the work that is being done to the branch name (e.g., `etl-574-add-contributing-doc`).

> [!IMPORTANT]
> Keep branch names as short as possible and use safe characters ([a-z], hyphens or underscore). We use the branch name to namespace resources in AWS -- as described below. Since AWS imposes limits on the name length and character sets of some resources, the deployment may fail if the branch name is too long or uses invalid characters. For this same reason, it's not possible to provide a comprehensive list of safe characters, so err on the side of caution.

2. We use [pipenv](https://pipenv.pypa.io/en/latest/) to manage our development environment. Simply run the below code from the repository root to create a virtual environment and install all dependencies:
```
pipenv shell
pipenv install --dev
```

3. Make any number of commits that solve the problem or leave the project in a better place. These commits may need to be consolidated later on during code review.

4. If anything in the `src` directory is modified, tests in the `tests` directory must be added or updated. We use [pytest](https://docs.pytest.org/en) to run our unit tests. Any convention supported by pytest for organizing tests is acceptable, but please do consider the organization of existing tests before attempting anything drastic. Documentation on how to run tests can be found [here](tests/README.md).

5. Once you are happy with your changes and all tests are passing, push the local branch to a remote branch of the same name. This will trigger the relevant [CI/CD actions](.github/workflows) and deploy the branch to its own namespaced stack in [the development environment](https://sagebionetworks.jira.com/wiki/spaces/IBC/pages/2808053761/RECOVER+ETL#AWS). Progress can be monitored via the [Actions](https://github.com/Sage-Bionetworks/recover/actions) tab.

## Testing remotely

Once the branch is deployed to the develop AWS environment (either via the CI/CD workflow or [manually](https://sagebionetworks.jira.com/wiki/spaces/IBC/pages/2964390126/RECOVER+tutorial#3.-Starting-the-workflow-manually%3A)), we can run integration tests. Test events are submitted to the pipeline automatically via CI/CD using the first method described below, although there are various entrypoints available for us to inject these events into the pipeline.

### Submitting test events via an S3 copy operation

This is the preferred method for submitting test events because it mimics how events propagate in the production environment.

Assuming that the namespaced [S3 Event Config Lambda](config/develop/namespaced/s3-event-config-lambda.yaml) has run at least once (either automatically via CI/CD or [manually](src/lambda_function/s3_event_config)), submit test events to the pipeline by copying objects from `s3://recover-dev-ingestion/pilot-data/` to the appropriate namespaced location in `s3://recover-dev-input-data`. This will result in a bucket event notification being sent to the [SQS queue](https://github.com/Sage-Bionetworks/recover/blob/main/config/develop/namespaced/sqs-queue.yaml), which is polled by the [S3 to Glue Lambda](src/lambda_function/s3_to_glue), which then triggers the [S3 to JSON Glue workflow](config/develop/namespaced/glue-workflow.yaml). All of these resources (excepting the S3 bucket) are specific to the stack's namespace.

### Submitting test events to the S3 to Glue Lambda

To submit SQS events directly to the [S3 to Glue Lambda](src/lambda_function/s3_to_glue), follow the instructions in its README.

### Submitting test events to the S3 to JSON Glue workflow

To run the S3 to JSON Glue workflow directly, either edit the workflow by adding a `--messages` workflow argument which matches the format of the S3 to Glue Lambda, or start a workflow run and add the `--messages` argument programmatically (mimicking the behavior of the S3 to Glue Lambda).

### JSON to Parquet Workflow

Once the S3 to JSON workflow has successfully completed, the JSON to Parquet workflow will need to be run manually to produce the Parquet datasets, which are written to a namespaced location in `s3://recover-dev-processed-data`. This workflow does not require any configuration once deployed and can be run directly without passing any additional workflow run arguments.

### Evaluating correctness of integration tests

We do not yet have a complete, automated data quality framework for our integration tests. Instead, in addition to hand checks that everything is operating normally, we have some tools that allow us to compare results within and across workflow runs.

#### Parquet dataset comparison

To evaluate what effect this branch's changes had on the test data, we have Glue jobs which run as part of the JSON to Parquet workflow which compare the Parquet datasets within this branch's `{namespace}` to those of the `main` namespace. Data reports are written to `s3://recover-dev-processed-data/{namespace}/comparison_result`.

#### Record count comparion

We use a structured logging framework in our Glue jobs so that logs can be consumed in a programmatic way. At the time of writing, this is limited to [a script](src/scripts/consume_logs/consume_logs.py) that will compare the count of lines read versus the count of lines written for each NDJSON file in each S3 to JSON workflow run within a given time frame.

## Code review

Once integration testing is complete,  submit a pull request against the `main` branch. At least one approving review is required to merge, although it is common courtesy to give everyone on the [review team](https://github.com/orgs/Sage-Bionetworks/teams/recover) time to approve the PR.

Before and/or after code review, clean up your commit history. If the `main` branch has changed since you last pushed your branch, [rebase](https://git-scm.com/docs/git-rebase) on main. If you have multiple commits, make sure that each commit is focused on delivering one complete feature. If you need to consolidate commits, consider doing an interactive rebase or a [`git merge --squash`](https://git-scm.com/docs/git-merge#Documentation/git-merge.txt---squash) if you are more comfortable with that method.

### Post review

Once the pull request has been approved, we expect _you_ to merge. Although this pulls your commits into the `main` branch, it does not yet deploy your changes to the `main` production pipeline. RECOVER data has FISMA restrictions, but only our production account is FISMA compliant. Since there is no guarantee that the test data provided to us (which doesn't have FISMA restrictions) perfectly models the production dataset, we maintain a `staging` namespace in the production account which enables us to test changes on production data before pulling those changes into the `main` namespace. Merging into `main` will deploy the changes to the `staging` namespace. To complete deployment to the `main` namespace of production, we push a new tag with a specific format to this repository, which will trigger [this GitHub action](.github/workflows/upload-and-deploy-to-prod-main.yaml). There is a diagram of this process [here](https://sagebionetworks.jira.com/wiki/spaces/IBC/pages/2863202319/ETL-390+RECOVER+Integration+Testing#Implementation).

# Code style

Code ought to conform to [`PEP8`](https://peps.python.org/pep-0008/), but may deviate if it helps with readability. There are many popular libraries for validating against PEP8, including [pylint](https://pypi.org/project/pylint/) and [black](https://pypi.org/project/black/) (which is a code formatter).

We also encourage [never nesting](https://www.youtube.com/watch?v=CFRhGnuXG-4), a slight misnomer which refers to never nesting code beyond three levels. This helps with readability and testing.
