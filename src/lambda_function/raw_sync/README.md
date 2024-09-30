# Raw Sync Lambda

The raw sync Lambda ensures that the input and raw S3 buckets are synchronized
by verifying that all non-zero sized JSON in each export in the input bucket
have a corresponding object in the raw bucket.

If a JSON file from an export is found to not have a corresponding object in the raw bucket,
the file is submitted to the raw Lambda (via the dispatch SNS topic) for processing.

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
cd src/lambda_function/raw_sync/
sam build
```

## Tests

Tests are available in `tests/test_lambda_raw_sync.py`.
