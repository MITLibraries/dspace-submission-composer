# ruff: noqa: TD002, TD003, FIX002

import datetime
import logging
import subprocess
from time import perf_counter

import click

from dsc.config import Config
from dsc.db.models import ItemSubmissionDB
from dsc.reports import CreateReport, FinalizeReport, SubmitReport
from dsc.workflows.base import Workflow

logger = logging.getLogger(__name__)
CONFIG = Config()


@click.group()
@click.pass_context
@click.option(
    "-w",
    "--workflow-name",
    help="The workflow to use for the batch of DSpace submissions",
    required=True,
)
@click.option(
    "-b",
    "--batch-id",
    help="A unique identifier for the workflow run, also used as an S3 prefix for "
    "workflow run files",
    required=True,
)
@click.option(
    "-v", "--verbose", is_flag=True, help="Pass to log at debug level instead of info"
)
def main(
    ctx: click.Context,
    workflow_name: str,
    batch_id: str,
    verbose: bool,  # noqa: FBT001
) -> None:
    ctx.ensure_object(dict)
    ctx.obj["start_time"] = perf_counter()
    workflow_class = Workflow.get_workflow(workflow_name)
    workflow = workflow_class(batch_id=batch_id)

    root_logger = logging.getLogger()
    logger.info(CONFIG.configure_logger(root_logger, verbose=verbose))
    logger.info(CONFIG.configure_sentry())
    CONFIG.check_required_env_vars()

    logger.info("Running process")
    ItemSubmissionDB.set_table_name(CONFIG.item_submissions_table_name)

    ctx.obj["workflow"] = workflow


@main.result_callback()
@click.pass_context
def post_main_group_subcommand(
    ctx: click.Context,
    *_args: tuple,
    **_kwargs: dict,
) -> None:
    """Callback for any work to perform after a main sub-command completes."""
    logger.info("Application exiting")
    logger.info(
        "Total time elapsed: %s",
        str(
            datetime.timedelta(seconds=perf_counter() - ctx.obj["start_time"]),
        ),
    )


# TODO: Remove 'reconcile' CLI command after DSC step function is updated
@main.command()
@click.pass_context
def reconcile(ctx: click.Context) -> None:
    """Reconcile bitstreams with item identifiers from the metadata."""
    workflow = ctx.obj["workflow"]

    reconciled = workflow.reconcile_items()

    if not reconciled:
        logger.error("Failed to reconcile bitstreams and metadata")
        ctx.exit(1)


@main.command()
@click.pass_context
@click.option("--sync-data/--no-sync-data", default=False)
@click.option(
    "--sync-dry-run",
    is_flag=True,
    help=(
        "Display the operations that would be performed using the "
        "sync command without actually running them"
    ),
)
@click.option(
    "-s",
    "--sync-source",
    help=(
        "Source directory formatted as a local filesystem path or "
        "an S3 URI in s3://bucket/prefix form"
    ),
)
@click.option(
    "-d",
    "--sync-destination",
    help=(
        "Destination directory formatted as a local filesystem path or "
        "an S3 URI in s3://bucket/prefix form"
    ),
)
@click.option(
    "-e",
    "--email-recipients",
    help="The recipients of the batch creation results email as a comma-delimited string",
    default=None,
)
def create(
    ctx: click.Context,
    *,
    sync_data: bool = False,
    sync_dry_run: bool = False,
    sync_source: str | None = None,
    sync_destination: str | None = None,
    email_recipients: str | None = None,
) -> None:
    """Create a batch of item submissions."""
    workflow = ctx.obj["workflow"]

    if sync_data:
        try:
            ctx.invoke(
                sync,
                source=sync_source,
                destination=sync_destination,
                dry_run=sync_dry_run,
            )
        except click.exceptions.Exit as exception:
            logger.error(  # noqa: TRY400
                "Failed to sync data, cannot proceed with batch creation"
            )
            ctx.exit(exception.exit_code)

    workflow.create_batch(synced=sync_data)

    if email_recipients:
        workflow.send_report(
            report_class=CreateReport, email_recipients=email_recipients.split(",")
        )


# data sync command
@main.command()
@click.pass_context
@click.option(
    "-s",
    "--source",
    help=(
        "Source directory formatted as a local filesystem path or "
        "an S3 URI in s3://bucket/prefix form"
    ),
)
@click.option(
    "-d",
    "--destination",
    help=(
        "Destination directory formatted as a local filesystem path or "
        "an S3 URI in s3://bucket/prefix form"
    ),
)
@click.option(
    "--dry-run",
    is_flag=True,
    help=(
        "Display the operations that would be performed using the "
        "sync command without actually running them"
    ),
)
def sync(
    ctx: click.Context,
    source: str | None = None,
    destination: str | None = None,
    *,
    dry_run: bool = False,
) -> None:
    """Sync data between two directories using the aws s3 sync command.

    If 'source' and 'destination' are not provided, the method will derive values
    based on the required '--batch-id / -b' and 'workflow-name / -w' options and
    S3 bucket env vars:
        * source: batch path in S3_BUCKET_SYNC_SOURCE
        * destination: batch path in S3_BUCKET_SUBMISSION_ASSETS

    This command accepts both local file system paths and S3 URIs in
    s3://bucket/prefix form. It synchronizes the contents of the source directory
    to the destination directory, and is configured to:
        * --delete: delete files in the destination that are not present in the source
        * --exclude metadata/*: exclude files in the dspace_metadata/ directory

    Although the aws s3 sync command recursively copies files, it ignores
    empty directories from the sync.
    """
    if source and destination:
        logger.info(f"Using provided source={source} and destination={destination}")
    elif CONFIG.s3_bucket_sync_source:
        workflow = ctx.obj["workflow"]
        source = f"s3://{CONFIG.s3_bucket_sync_source}/{workflow.batch_path}"
        destination = f"s3://{CONFIG.s3_bucket_submission_assets}/{workflow.batch_path}"
    else:
        raise click.UsageError(
            "Either provide '--source / -s' and '--destination / -d' "
            "or set the S3_BUCKET_SYNC_SOURCE environment variable"
        )

    logger.info(f"Syncing data from {source} to {destination}")

    args = [
        "aws",
        "s3",
        "sync",
        source,
        destination,
        "--delete",
        "--exclude",
        "dspace_metadata/*",
    ]

    # add optional args
    if dry_run:
        args.append("--dryrun")

    process = subprocess.Popen(  # noqa: S603
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    # log process output (stdout and stderr) in real-time
    if process.stdout:
        for line in process.stdout:
            if line:
                logger.info(line)
    else:
        logger.info("No changes detected in source, no sync required")

    if process.stderr:
        for line in process.stderr:
            if line:
                logger.error(line)

    # wait for the process to complete
    process.wait()
    return_code = process.returncode

    if return_code != 0:
        logger.error(f"Failed to sync (exit code: {return_code})")
        ctx.exit(return_code)  # exit with the same code as subprocess
    else:
        logger.info("Sync completed successfully")


@main.command()
@click.pass_context
@click.option(
    "-c",
    "--collection-handle",
    help="The handle of the DSpace collection to which the batch will be submitted",
    required=True,
)
@click.option(
    "-e",
    "--email-recipients",
    help="The recipients of the submission results email as a comma-delimited string",
    default=None,
)
def submit(
    ctx: click.Context,
    collection_handle: str,
    email_recipients: str | None = None,
) -> None:
    """Send a batch of item submissions to the DSpace Submission Service (DSS)."""
    workflow = ctx.obj["workflow"]
    workflow.submit_items(collection_handle)

    if email_recipients:
        workflow.send_report(
            report_class=SubmitReport, email_recipients=email_recipients.split(",")
        )


@main.command()
@click.pass_context
@click.option(
    "-e",
    "--email-recipients",
    help="The recipients of the submission results email as a comma-delimited string",
    required=True,
)
def finalize(ctx: click.Context, email_recipients: str) -> None:
    """Process the result messages from the DSS output queue according the workflow."""
    workflow = ctx.obj["workflow"]
    workflow.finalize_items()
    workflow.send_report(
        report_class=FinalizeReport, email_recipients=email_recipients.split(",")
    )
