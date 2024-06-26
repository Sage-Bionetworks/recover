# Dispatch Lambda

The dispatch Lambda polls the input-to-dispatch SQS queue and publishes to the dispatch SNS topic.
Its purpose is to inspect an export and dispatch each file as a separate job to eventually be consumed
by the dispatch-to-raw Lambda.

## Development

The Serverless Application Model Command Line Interface (SAM CLI) is an
extension of the AWS CLI that adds functionality for building and testing
Lambda applications.

To use the SAM CLI, you need the following tools.

* SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community)

You may need the following for local testing.
* [Python 3 installed](https://www.python.org/downloads/)

You will also need to configure your AWS credentials, if you have not already done so.

## Creating a local build

Use the SAM CLI to build and test your lambda locally.
Build your application with the `sam build` command.

```bash
cd src/lambda_function/dispatch/
sam build
```

## Tests

Tests are available in `tests/test_dispatch_lambda.py`.
