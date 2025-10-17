# Understanding and Running the DSC Workflow

_This documentation describes the DSC workflow and how to run the application._

**DISCLAIMER**: While the CLI application is runnable on its own, the DSO Step Function offers a simplified user interface for running the full ETL pipeline. 

# The DSC Workflow

The DSC workflow consists of the following key steps:

1. Create a batch
2. Queue a batch for ingest
3. Ingest items into DSpace
4. Inspect ingest results

It's important to note that DSC is not responsible for ingesting items into DSpace; this task is handled by _DSS_. The DSC CLI provides commands for all other steps in the DSC workflow. 

What the step function does with each key step....

# Create a batch
DSC processes deposits in "batches", a collection of item submissions grouped by a unique identifier. DSC requires that the item submission assets (metadata and bitstream files) are uploaded to a "folder" in S3, named after the batch ID. While some requestors may upload the submission assets to S3 themselves, in other cases, these files need to be retrieved (via API requests) and uploaded during the batch creation step. 

At the end of this step:
* If all item submission assets are complete:
   - A batch folder with complete item submission assets exists in the DSO S3 bucket
   - Each item submission in the batch is recorded in DynamoDB (with `status="batch_created"`)
   - [OPTIONAL] An email is sent reporting the number of created item submissions. The email 
     includes a CSV file with the batch records from DynamoDB.
* If any item submission assets were invalid (missing metadata and/or bitstreams):
   - A batch folder with incomplete item submission assets exists in the DSO S3 bucket
   - [OPTIONAL] An email is sent reporting that zero item submissions were created. The email
     includes a CSV file indicating the failed item submissions and the corresponding error message.

# Queue a batch for ingest
# Finalize 

----
Each step is explained in greater detail in the sections below. 
CLI commands are defined to mirror the names of the workflow steps (with the exception of running DSS). The next sections cover each step in more detail. 

prepare items for DSpace...
submit items into DSpace......
creating submission packages.....
DSS ingests the SIPs...

While the CLI is the main entry point for DSC, the workflow modules handles the core functionality invoked by the CLI. 