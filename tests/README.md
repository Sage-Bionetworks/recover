### Running tests
Tests are defined in the `tests` folder in this project.

#### Running tests using Docker
All tests can be run inside a Docker container which includes all the necessary
Glue/Spark dependencies and simulates the environment which the Glue jobs
will be run in. Dockerfiles for AWS versions 3.0 and 4.0 are included in the `tests` directory.

To run tests locally, first configure your AWS credentials, then launch and attach
to the relevant docker container (see following commands)

These scripts needs to be run inside a Docker container:

- Under AWS 3.0 (Dockerfile.aws_glue_3)
  - test_compare_parquet_datasets.py
  - test_s3_to_json.py

- Under AWS 4.0 (Dockerfile.aws_glue_4)
  - test_json_to_parquet.py (Note that these tests deploys test resources to aws and will take several min to run)

Run the following commands to run tests for:

1. Navigate to the directory with the Dockerfile

```shell script
cd tests
```

2. Build the docker image from the Dockerfile relevant to your tests

```shell script
docker build -f <name_of_dockerfile> -t <some_name_for_container> .
```

3. Run the newly built image. You can pick port numbers of your choice:

```shell script
docker run --rm -it \
  -v ~/.aws:/home/glue_user/.aws \
  -v ~/recover/:/home/glue_user/workspace/recover \
  -e DISABLE_SSL=true -p 4040:4040 -p 18080:18080 <some_name_for_container>
```

4. Navigate to your repo in the image

```shell script
cd <repo name>
```

5. Finally run the following (now that you are inside the running container)
to execute the tests:

##### Running tests for json to parquet

You will need to specify a namespace variable when running the json to parquet tests so that
the AWS resources are created and run under this namespace.
You can find your test related resources here (for reviewing/troubleshooting):

`s3://<recover_artifacts_bucket_name>/<namespace>/tests/test_json_to_parquet`

```shell script
python3 -m pytest <path_to_your_specific_script> --namespace <name_of_namespace_to_use> -v
```

##### Running tests for everything else

```shell script
python3 -m pytest <path_to_your_specific_script> -v
```

#### Running tests using pipenv
Use [pipenv](https://pipenv.pypa.io/en/latest/index.html) to install the
[pytest](https://docs.pytest.org/en/latest/) and run tests locally outside of
a Docker image.

Here are the tests you can run locally using `pipenv`. You'll run into an error with
pytest with other tests because they have to be run in a Dockerfile with the AWS glue environment:

- test_s3_to_glue_lambda.py
- test_s3_event_config_lambda.py
- test_setup_external_storage.py

#### Running tests for lambda
Run the following command from the repo root to run tests for the lambda functions (in develop).

```shell script
python3 -m pytest tests/test_s3_to_glue_lambda.py -v
```

```shell script
python3 -m pytest tests/test_s3_event_config_lambda.py -v
```

#### Running tests for setup external storage
Run the following command from the repo root to run the integration test for the setup external storage script to check that the STS
access has been set for a given synapse folder (in develop).


```shell script
python3 -m pytest tests/test_setup_external_storage.py
--test-bucket <put_bucket_name_here>
--test-synapse-folder-id <put_synapse_folder_id_here>
--namespace <put_namespace_here>
--test_sts_permission <put_the_type_of_permission_to_test_here>
```
