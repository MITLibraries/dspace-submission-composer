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

## Environment Variables

### Required

```shell
SENTRY_DSN=### If set to a valid Sentry DSN, enables Sentry exception monitoring. This is not needed for local development.
WORKSPACE=### Set to `dev` for local development, this will be set to `stage` and `prod` in those environments by Terraform.
AWS_REGION_NAME=### Default AWS region.
```

### Optional

```shell
LOG_LEVEL=### Logging level. Defaults to 'INFO'.
```


