### Running tests
Tests are defined in the `tests` folder in this project.

#### Running tests using Docker
All tests can be run inside a Docker container which includes all the necessary
Glue/Spark dependencies and simulates the environment which the Glue jobs
will be run in. A Dockerfile is included in the `tests` directory

To run tests locally, first configure your AWS credentials, then launch and attach
to the docker container (see following commands)

Run the following commands to run tests for the s3_to_json script (in develop).

1. Navigate to the directory with the Dockerfile

```shell script
cd tests
```

2. Build the docker image from the Dockerfile

```shell script
docker build -t <some_name_for_container> .
```

3. Run the newly built image:

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

```shell script
python3 -m pytest
```

#### Running tests using pipenv
Use [pipenv](https://pipenv.pypa.io/en/latest/index.html) to install the
[pytest](https://docs.pytest.org/en/latest/) and run tests locally outside of
a Docker image.

Here are the tests you can run locally using a pipenv. You'll run into an error with
pytest with other tests because they have to be run in a Dockerfile:

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
