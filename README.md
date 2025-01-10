# dspace-submission-composer
An application for creating messages for the [DSpace Submission Service application](https://github.com/MITLibraries/dspace-submission-service).

# Application Description

Description of the app

## Development

- To preview a list of available Makefile commands: `make help`
- To install with dev dependencies: `make install`
- To update dependencies: `make update`
- To run unit tests: `make test`
- To lint the repo: `make lint`
- To run the app: `pipenv run dsc --help`

### Running a Local MinIO Server

[MinIO is an object storage solution that provides an Amazon Web Services S3-compatible API and supports all core S3 features](https://min.io/docs/minio/kubernetes/upstream/). The MinIO server acts as a "local S3 file system", allowing the app to access data on disk through an S3 interface. Since the MinIO server runs in a Docker container, it can be easily started when needed and stopped when not in use. Any data stored in the MinIO server will persist as long as the files exist in the directory specified for `MINIO_S3_LOCAL_STORAGE`.

Several DSC workflows involve reading metadata CSV files and searching for bitstreams stored on S3. Through the use of a local MinIO server, tests can be performed for these workflows using data on disk. 

1. Configure your `.env` file. The following environment variables must also be set:
   ```text
   MINIO_S3_LOCAL_STORAGE=# full file system path to the directory where MinIO stores its object data on the local disk
   MINIO_ROOT_USER=# username for root user account for MinIO server
   MINIO_ROOT_PASSWORD=# password for root user account MinIO server
   ```

2. Create an AWS profile `minio`. When prompted for an "AWS Access Key ID" and "AWS Secret Access Key", pass the values set for the `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` environment variables, respectively.
   
   ```shell
   aws configure --profile minio
   ```

3. Launch a local MinIO server via Docker container by running the Makefile command: 
   ```shell 
   make start-minio-server
   ```

   The API is accessible at: http://127.0.0.1:9000.
   The WebUI is accessible at: http://127.0.0.1:9001.

4. On your browser, navigate to the WebUI and sign into the local MinIO server. Create a bucket in the local MinIO server named after the S3 bucket for DSC workflows (i.e., "dsc").

## Environment Variables

### Required

```shell
SENTRY_DSN=### If set to a valid Sentry DSN, enables Sentry exception monitoring. This is not needed for local development.
WORKSPACE=### Set to `dev` for local development, this will be set to `stage` and `prod` in those environments by Terraform.
AWS_REGION_NAME=### Default AWS region.
DSS_INPUT_QUEUE=### The DSS SQS input queue to which submission messages are sent.
DSC_SOURCE_EMAIL=### The email address from which SES emails are sent.
```

### Optional

```shell
LOG_LEVEL=### Logging level. Defaults to 'INFO'.
MINIO_S3_LOCAL_STORAGE=# full file system path to the directory where MinIO stores its object data on the local disk
MINIO_S3_URL=# endpoint for MinIO server API; default is "http://localhost:9000/"
MINIO_S3_CONTAINER_URL=# endpoint for the MinIO server when acccessed from inside a Docker container; default is "http://host.docker.internal:9000/"
MINIO_ROOT_USER=# username for root user account for MinIO server
MINIO_ROOT_PASSWORD=# password for root user account MinIO server
```


