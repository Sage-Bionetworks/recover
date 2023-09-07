# s3_event_config lambda

The s3_event_config lambda is triggered by a github action during deployment or manually through the AWS console.

It will then put a S3 event notification configuration into
the input data bucket which allows the input data bucket to
trigger a specific destination type with S3 new object notifications whenever new objects are added
to it and eventually lead to the start of the S3-to-JSON workflow.

Currently **only** the following destination types are supported:

- Lambda Function
- SQS queue

## Event format

The events that will trigger the s3-event-config-lambda
should be in the form of something like:

```
{
    "RequestType": "Create"
}
```

Where the allowed RequestType values are:
- "Create"
- "Update"
- "Delete"

You can test the lambda by going to the AWS console for the lambda function, pasting the above sample event in and triggering the function. Any updates should then be visible in the input bucket's event config to confirm it was successful.

## Launching Lambda stack in AWS

There are two main stacks involved in the s3_event_config lambda. They are the
`s3_event_config lambda role` stack and the `s3_event_config lambda` stack.

Note that they depend on the `s3 to json` lambda stacks.

### Sceptre

#### Launching in development

Run the following command to create the lambda stack in your AWS account. Note this will
also create the lambda event config IAM role stack as well as well as any other dependencies of this stack:

```shell script
sceptre --var namespace='test-namespace' launch develop/namespaced/s3-event-config-lambda.yaml
```
