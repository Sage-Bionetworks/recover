# Raw Lambda

The raw Lambda polls the dispatch-to-raw SQS queue and uploads an object to the raw S3 bucket.
Its purpose is to decompress a single file from an export, recompress that file, and store it to S3.

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
cd src/lambda_function/raw/
sam build
```

## Tests

Tests are available in `tests/test_raw_lambda.py`.
