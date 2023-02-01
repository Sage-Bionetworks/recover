### Running tests
Tests are defined in the `tests` folder in this project.

#### Running tests using Docker
Tests are run inside a Docker container which includes all the necessary
Glue/Spark dependencies and simulates the environment which the Glue jobs
will be run in. A Dockerfile is included in the tests/ directory
To run tests locally, first configure your AWS credentials, then launch and attach
to the docker container by referencing the following example command as a template:

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

4. Finally run the following (now that you are inside the running container)
to execute the tests:

```shell script
python3 -m pytest
```

#### Running tests using pipenv
Use [pipenv](https://pipenv.pypa.io/en/latest/index.html) to install the
[pytest](https://docs.pytest.org/en/latest/) and run tests locally outside of
a Docker image.

Run the following command from the repo root to run tests for the lambda function (in develop).
You can run this locally or inside the docker image.

```shell script
python3 -m pytest tests/lambda_function/ -v
```
