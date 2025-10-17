# Understanding and Running the DSC Workflow

_This documentation describes the DSC workflow and how to run the application._

**DISCLAIMER**: While the CLI application is runnable on its own, the DSO Step Function offers a simplified user interface for running the full ETL pipeline. For more details on the DSO Step Function and how to use it, see https://mitlibraries.atlassian.net/wiki/spaces/IN/pages/4690542593/DSpace+Submission+Orchestrator+DSO.

## The DSC Workflow

The DSC workflow consists of the following key steps:

1. Create a batch
2. Queue a batch for ingest
3. Ingest items into DSpace
4. Inspect ingest results

It's important to note that DSC is not responsible for ingesting items into DSpace; this task is handled by _DSS_. The DSC CLI provides commands for all other steps in the DSC workflow. 

### Create a batch
DSC processes deposits in "batches", a collection of item submissions grouped by a unique identifier. DSC requires that the item submission assets (metadata and bitstream files) are uploaded to a "folder" in S3, named after the batch ID. While some requestors may upload the submission assets to S3 themselves, in other cases, these files need to be retrieved (via API requests) and uploaded during the batch creation step. 

At the end of this step:
* If all item submission assets are complete:
   - A batch folder with complete item submission assets exists in the DSO S3 bucket
   - Each item submission in the batch is recorded in DynamoDB (with `status="batch_created"`)
   - **[OPTIONAL]** An email is sent reporting the number of created item submissions. The email includes a CSV file with the batch records from DynamoDB.
* If any item submission assets were invalid (missing metadata and/or bitstreams):
   - A batch folder with incomplete item submission assets exists in the DSO S3 bucket
   - **[OPTIONAL]** An email is sent reporting that zero item submissions were created. The email
     includes a CSV file describing the failing item submissions with the corresponding error message.

**Data syncing**

✨ If the batch folder was already created (i.e., an S3 bucket in a different deployment environment), DSC can sync the data and avoid repeating data retrieval steps. 

### Queue a batch for ingest
DSC retrieves the batch records from DynamoDB, and for each item submission, it performs the following steps:
* Determine whether the item submission should be sent to the DSS input queue
* Map/transform the source metadata to follow the Dublin Core schema
* Create and upload a metadata JSON file in the batch folder (under `dspace_metadata/`)
* Send a message to the DSS input queue

  Note: The message is structured in accordance with the [Submission Message Specification](https://github.com/MITLibraries/dspace-submission-service/blob/main/docs/specifications/submission-message-specification.md).

At the end of this step:
* Batch records in DynamoDB are updated. Updates are made to the folllowing fields:
    - `status`: Indicates submit status
    - `status_details`: Set to error messages (if message failed to send)
    - `last_run_date`: Set to current run date
    - `submit_attempts`: Increments by 1
* **[OPTIONAL]** An email is sent reporting the counts for each submission status. The email includes a CSV file with the batch records from DynamoDB, reflecting the latest information.

### Run DSS
DSS consumes the submission messages from the input queue in SQS. DSS uses a client to interact with DSpace. For each item submission, DSS reads the metadata JSON file and bitstreams from S3, using the information provided in the message, and creates an item with bitstreams in DSpace.

At the end of this step:
* Result messages are written to the output queue for DSC (`dss-output-dsc`).

  Note: The message is structured in accordance with the [Result Message Specification](https://github.com/MITLibraries/dspace-submission-service/blob/main/docs/specifications/result-message-specification.md).

### Inspect ingest results
DSC consumes result messages from its output queue, parsing the messages to determine whether the associated item was ingested into DSpace. It then loops through the batch records from DynamoDB, updating those that have a corresponding result message. Additional steps with the item submission may be performed on behalf of the requestor (e.g., custom reports).

At the end of this step:
* Batch records in DynamoDB are updated. Updates are made to the folllowing fields:
    - `dspace_handle`: Set to generated DSpace handle (if item was ingested)
    - `status`: Indicates ingest status
    - `status_details`: Set to error messages (if item failed ingest)
    - `last_result_message`: Set to result message string
    - `last_run_date`: Set to current run date
    - `ingest_attempts`: Increments by 1 
- Result messages are deleted from the queue
    - If any errors occur during the processing of result message, the result message will remain in the queue.
- An email is sent reporting the counts for each ingest status. The email includes a CSV file with the batch records from DynamoDB, reflecting the latest information.

## The DSC CLI

### `pipenv run dsc`

```text
Usage: -c [OPTIONS] COMMAND [ARGS]...

  DSC CLI.

Options:
  -w, --workflow-name TEXT  The workflow to use for the batch of DSpace
                            submissions  [required]
  -b, --batch-id TEXT       A unique identifier for the workflow run, also
                            used as an S3 prefix for workflow run files
                            [required]
  -v, --verbose             Pass to log at debug level instead of info
  --help                    Show this message and exit.

Commands:
  create     Create a batch of item submissions.
  finalize   Process the result messages from the DSS output queue...
  reconcile  Reconcile bitstreams with item identifiers from the metadata.
  submit     Send a batch of item submissions to the DSpace Submission...
  sync       Sync data between two directories using the aws s3 sync...
```

### `pipenv run dsc -w <workflow-name> -b <batch-id> create`

```text
Usage: -c create [OPTIONS]

  Create a batch of item submissions.

Options:
  --sync-data / --no-sync-data  Invoke 'sync' CLI command.
  --sync-dry-run                Display the operations that would be performed
                                using the sync command without actually
                                running them
  -s, --sync-source TEXT        Source directory formatted as a local
                                filesystem path or an S3 URI in
                                s3://bucket/prefix form
  -d, --sync-destination TEXT   Destination directory formatted as a local
                                filesystem path or an S3 URI in
                                s3://bucket/prefix form
  -e, --email-recipients TEXT   The recipients of the batch creation results
                                email as a comma-delimited string
  --help                        Show this message and exit.
```

**Important:** If the boolean flag `--sync-data` is set, the `sync` CLI command is invoked, which executes a basic [`aws s3 sync`](https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html) command.

### `pipenv run dsc -w <workflow-name> -b <batch-id> submit`

```text
Usage: -c submit [OPTIONS]

  Send a batch of item submissions to DSS.

Options:
  -c, --collection-handle TEXT  The handle of the DSpace collection to which
                                the batch will be submitted  [required]
  -e, --email-recipients TEXT   The recipients of the submission results email
                                as a comma-delimited string
  --help                        Show this message and exit.
```

### `pipenv run dsc -w <workflow-name> -b <batch-id> finalize`

```text
Usage: -c finalize [OPTIONS]

  Process the result messages from the DSC output queue.

Options:
  -e, --email-recipients TEXT  The recipients of the submission results email
                               as a comma-delimited string  [required]
  --help                       Show this message and exit.
```

### `pipenv run dsc -w <workflow-name> -b <batch-id> sync`

```text
Usage: -c sync [OPTIONS]

  Sync data between two directories using the aws s3 sync command.

  If 'source' and 'destination' are not provided, the method will derive
  values based on the required '--batch-id / -b' and 'workflow-name / -w'
  options and S3 bucket env vars:

      * source: batch path in S3_BUCKET_SYNC_SOURCE

      * destination: batch path in S3_BUCKET_SUBMISSION_ASSETS

  This command accepts both local file system paths and S3 URIs in
  s3://bucket/prefix form. It synchronizes the contents of the source
  directory to the destination directory, and is configured to delete files in
  the destination that are not present in the source exclude files in the
  dspace_metadata/ directory.

  Although the aws s3 sync command recursively copies files, it ignores empty
  directories from the sync.

Options:
  -s, --source TEXT       Source directory formatted as a local filesystem
                          path or an S3 URI in s3://bucket/prefix form
  -d, --destination TEXT  Destination directory formatted as a local
                          filesystem path or an S3 URI in s3://bucket/prefix
                          form
  --dry-run               Display the operations that would be performed using
                          the sync command without actually running them
  --help                  Show this message and exit.
```