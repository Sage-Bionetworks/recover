# recover
A data pipeline for processing data from the RECOVER project.


## Setting up the development environment

The development environment is managed using [pipenv](https://pipenv.pypa.io/en/latest/basics/). Run the following command in the same folder as the `Pipfile` and start a new shell subprocess utilizing this environment:

```
pipenv install --dev
pipenv shell
```

## How this repo is organized

This repository contains all the resources needed to build a production-ready deployment from scratch.

### `src/`

The `src` directory contains job scripts and reference files and is divided into subdirectories by AWS service. Additionally, there is a [`scripts`](src/scripts) subdirectory, which contains scripts meant to be run _outside_ of the deployed infrastructure. You can think of these as convenience scripts to speed up the completion of common tasks. The [`bootstrap_trigger`](src/scripts/bootstrap_trigger/bootstrap_trigger.py) script is particularly important, as it is a stand-in for [a currently missing Bridge feature](https://sagebionetworks.jira.com/browse/BRIDGE-3013).


### `tests/`

This directory contains unit tests and testing data. Tests can be run with [pytest](https://docs.pytest.org/). Detailed instructions on how to run the tests are included in [the `tests` directory README](tests/README.md).

### `templates/`

These are CloudFormation templates for the AWS resources we deploy.

### `config/`

These are configuration files which make use of the CloudFormation templates in `templates/` See the `Deployment` section below for a detailed description of how to deploy the stacks.

### `.github`

This directory contains the scripts for our Github Actions CI/CD. See [the `workflows` directory README](.github/workflows/README.md) for a detailed description of the Github Actions this repo uses.

## Deployment

Deployment is a two-step process.

### Upload S3 resources

Some of the AWS resources which we deploy -- including Glue, Lambda, and EC2 resources -- reference objects in S3. If you forget to upload these objects in advance of deployment, the pipeline may break or you may unwittingly be running old code. All S3 resources are organized within their own _namespace_, which allows multiple developers to simultaneously work on the pipeline without stepping on each other's toes. By convention, we use the branch name as the namespace. Run the following script to sync these objects to their respective namespaced location in S3:

```
python src/scripts/manage_artifacts/artifacts.py --upload --ref my-namespace
```

Additionally, Glue jobs currently reference a separate S3 location for their script. We have this extra step because at some point in the future we may want to have multiple versions of a Glue job running concurrently within the same namespace. This could occur if, for example, future digital health apps or future versions of existing apps change the format of the data they upload. We still need to support the processing of data in the old format, and so different jobs will reference different script versions -- and hence different S3 locations -- for their script.

The version which is used with deployment is tracked in the `latest_version` field of [the sceptre global config](config/config.yaml). Your `--ref` argument should match this value.

```
python src/scripts/manage_artifacts/artifacts.py --upload --ref 'v0.1'
```

### Deployment with sceptre

All deployments are deployed within a namespace. By default, the namespace is `bridge-downstream`, but you should pass the same namespace you used above when you uploaded the S3 resources. Importantly, namespaces are not mutually exclusive in the resources they utilize. Stacks from different namespaces will still write their output to the same S3 bucket, for example. If you are deploying a stack to the development account entirely from scratch, then you would invoke sceptre like so:

```
sceptre --var namespace='my-namespace' launch develop
```

If you did not make any modifications to the shared resources (those in `config/develop/` but not in `config/develop/namespaced/`), then it's sufficient to deploy only the namespaced stacks:

```
sceptre --var namespace='my-namespace' launch develop/namespaced
```

Similarly, to deploy to the production account with the default `bridge-downstream` namespace:

```
sceptre launch prod
```

You'll notice that we don't specify a namespace here and that there is no `config/prod/namespaced/` folder. This is because namespaces are only useful because they allow us to deploy different versions of the same type of resource to the same account. Only one version of BridgeDownstream should ever run in production -- the _production_ version -- and so there is effectively only one namespace in the production environment, that is, the default namespace: `main`.

For more information on using sceptre, see [the docs](https://docs.sceptre-project.org/).

## CI/CD

Github Actions is used to run QA tools, tests, and automatically deploy stacks to their respective namespace upon pushing to a branch and opening and merging a PR. To see which workflows are triggered upon these events, see [the workflows directory](.github/workflows). By convention, the namespace the stack is deployed to will match the branch name.
