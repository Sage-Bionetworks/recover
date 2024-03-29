# s3_to_glue lambda

The s3_to_glue lambda is triggered by a SQS event and polls the SQS queue
for S3 event messages of files to submit to a glue workflow. If there are files to submit,
it starts the S3-to-Json workflow.

## Development

The Serverless Application Model Command Line Interface (SAM CLI) is an
extension of the AWS CLI that adds functionality for building and testing
Lambda applications.

To use the SAM CLI, you need the following tools.

* SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community)

You may need the following for local testing.
* [Python 3 installed](https://www.python.org/downloads/)

You will also need to configure your AWS credentials,
if you have not already done so.

## Creating a local build

Use the SAM CLI to build and test your lambda locally.
Build your application with the `sam build` command.

```bash
cd src/lambda_function/s3_to_glue/
sam build
```

## Test events

### Creating/modifying test events

The file `single-record.json` in `src/lambda_function/s3_to_glue/events` contains a
dummy event for an S3 event trigger. You can generate your own test events
for single or multiple records with the script at
`src/lambda_function/s3_to_glue/events/generate_test_event.py`.

Sample working function call:

```bash
python src/lambda_function/s3_to_glue/events/generate_test_event.py
--output-directory ./src/lambda_function/s3_to_glue/events/
--input-bucket recover-dev-input-data
--input-key-prefix <folder_under_recover-dev-input-data_containing_test_data>
```

### Invoking test events

To test the lambda function locally, run the following command from the lambda directory.
Use `single-record.json` or your own test event to trigger the S3 to JSON Glue workflow.
The file `test-env-vars.json` contains
the environment variables that are expected by the lambda script.

Don't forget to update the value of this variables if applicable to mirror the stack deployed as part of a feature branch

To invoke the lambda with the test event:

```bash
cd src/lambda_function/s3_to_glue
sam local invoke -e events/single-record.json --env-vars test-env-vars.json
```

## Launching Lambda stack in AWS

There are two main stacks involved in the s3 to glue lambda. They are the
`s3 to glue lambda role` stack and the `s3 to glue lambda` stack.

### Sceptre

#### Launching in development

1. Create/update the following s3 to glue lambda role [sceptre](https://github.com/Sceptre/sceptre) config file:
`config/develop/namespaced/s3-to-glue-lambda-role.yaml`

2. Create/update the following s3 to glue lambda role [sceptre](https://github.com/Sceptre/sceptre) template file:
`templates/s3-to-glue-lambda-role.yaml`

3. Create/update the following s3 to glue lambda [sceptre](https://github.com/Sceptre/sceptre) config file:
`config/develop/namespaced/s3-to-glue-lambda.yaml`

4. Create/update the following s3 to glue lambda [sceptre](https://github.com/Sceptre/sceptre) template file:
`src/lambda_function/template.yaml`

5. Run the following command to create the lambda stack in your AWS account. Note this will
also create the lambda s3 to glue IAM role stack as well:

```shell script
sceptre --var namespace='test-namespace' launch develop/namespaced/s3-to-glue-lambda.yaml
```

#### Launching in production
