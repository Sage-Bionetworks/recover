# s3_to_glue lambda

The s3_to_glue lambda is triggered daily by a scheduled event and checks the source
S3 bucket for files to submit to a glue workflow. If there are files to submit,
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
cd src/lambda
sam build
```

## Test events

### Creating/modifying test events

The `test-trigger-event.json` at `src/lambda_function/events` contains a
dummy event of what a scheduled event trigger using CloudWatch would look
like as our lambda function receives it. The part of the event that is actually used
is the `time` key in the file whose date portion is used in the lambda function to
compare against files in the S3 bucket we are checking against since only files that were
modified/created the same date as the trigger event would be submitted by the lambda to
the s3-to-glue workflow. You can modify the value for the `time` to test whether files
get submitted or not.

### Invoking test events

To test the lambda function locally, run the following command from the lambda directory.
Use `test-trigger-event.json` to trigger the Glue workflow. The file `test-env-vars.json` contains
the namespace environment variable that is expected by the lambda script.
Don't forget to update the value of this variable
if you are testing a stack deployed as part of a feature branch.

To invoke the lambda with the test event:

```bash
cd src/lambda
sam local invoke -e events/test-trigger-event.json --env-vars test-env-vars.json
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
sceptre --var namespace='test-namespace' launch develop/namespace/s3-to-glue-lambda.yaml
```

#### Launching in production
