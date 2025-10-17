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

What the step function does with each key step....

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

----
Each step is explained in greater detail in the sections below. 
CLI commands are defined to mirror the names of the workflow steps (with the exception of running DSS). The next sections cover each step in more detail. 

prepare items for DSpace...
submit items into DSpace......
creating submission packages.....
DSS ingests the SIPs...

While the CLI is the main entry point for DSC, the workflow modules handles the core functionality invoked by the CLI. 