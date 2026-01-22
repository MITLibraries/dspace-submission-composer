# Understanding and Running DSC

This documentation describes the DSC application and how to run it.

**DISCLAIMER**: While the CLI application is runnable on its own, the DSO Step Function offers a simplified user interface for running the full ETL pipeline. For more details on the DSO Step Function and how to use it, see https://mitlibraries.atlassian.net/wiki/spaces/IN/pages/4690542593/DSpace+Submission+Orchestrator+DSO.

Running DSC consists of the following key steps:

1. Create a batch
2. Queue a batch for ingest
3. _Items are ingested into DSpace by DSS*_
4. Analyze ingest results

***Important:** DSC is not responsible for ingesting items into DSpace nor does it execute this process. This task is handled by [DSS](https://github.com/MITLibraries/dspace-submission-service), which is invoked via the [DSO Step Function](https://github.com/MITLibraries/dspace-submission-service).

### Create a batch
DSC processes deposits in "batches", a collection of item submissions grouped by a unique identifier. Generally, assets for batches are provided in one of two ways:

1. Requestors upload raw metadata and bitstreams (see [How To: Batch deposits with DSO for Content Owners](https://mitlibraries.atlassian.net/wiki/spaces/INF/pages/4411326470/How-To+Batch+deposits+with+DSpace+Submission+Orchestrator+DSO+for+Content+Owners))
2. DSC workflows retrieve raw metadata and/or bitstreams programmatically (via API requests)

Once all assets are stored in a "folder" in S3, DSC verifies that metadata and bitstreams have been provided for each item submission. It is only after _all_ item submissions have been verified that DSC will establish the batch by recording each item in DynamoDB.

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

### Items are ingested into DSpace by DSS
📌 **Reminder:** DSS is not executed by DSC and requires separate invocation.

DSS consumes the submission messages from the input queue in SQS. DSS uses a client to interact with DSpace. For each item submission, DSS reads the metadata JSON file and bitstreams from S3, using the information provided in the message, and creates an item with bitstreams in DSpace.

At the end of this step:
* Result messages are written to the output queue for DSC (`dss-output-dsc`).

  Note: The message is structured in accordance with the [Result Message Specification](https://github.com/MITLibraries/dspace-submission-service/blob/main/docs/specifications/result-message-specification.md).

### Analyze ingest results
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
  create    Create a batch of item submissions.
  finalize  Analyze ingest results for a given batch.
  submit    Queue a batch of item submissions for DSS.
  sync      Sync data between two directories using the aws s3 sync command.
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

  Queue a batch of item submissions for DSS.

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

  Analyze ingest results for a given batch.

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