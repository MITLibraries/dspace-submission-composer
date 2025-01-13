import logging
from datetime import timedelta
from io import StringIO
from time import perf_counter

import click

from dsc.config import Config
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
    "-c",
    "--collection-handle",
    help="The handle of the DSpace collection to which the batch will be submitted",
    required=True,
)
@click.option(
    "-b",
    "--batch-id",
    help="The S3 prefix for the batch of DSpace submission files",
    required=True,
)
@click.option(
    "-e",
    "--email-recipients",
    help="The recipients of the submission results email as a comma-delimited string, "
    "this will override the workflow class's default value for this attribute",
    required=True,
)
@click.option(
    "-s",
    "--s3-bucket",
    help="The S3 bucket containing the DSpace submission files, "
    "this will override the workflow class's default value for this attribute",
)
@click.option(
    "-o",
    "--output-queue",
    help="The SQS output queue for the DSS result messages, "
    "this will override the workflow class's default value for this attribute",
)
@click.option(
    "-v", "--verbose", is_flag=True, help="Pass to log at debug level instead of info"
)
def main(
    ctx: click.Context,
    workflow_name: str,
    collection_handle: str,
    batch_id: str,
    email_recipients: str,
    s3_bucket: str | None,
    output_queue: str | None,
    verbose: bool,  # noqa: FBT001
) -> None:
    ctx.ensure_object(dict)
    ctx.obj["start_time"] = perf_counter()
    workflow = Workflow.load(
        workflow_name=workflow_name,
        collection_handle=collection_handle,
        batch_id=batch_id,
        email_recipients=email_recipients,
        s3_bucket=s3_bucket,
        output_queue=output_queue,
    )
    ctx.obj["workflow"] = workflow

    stream = StringIO()
    root_logger = logging.getLogger()
    logger.info(CONFIG.configure_logger(root_logger, stream, verbose=verbose))
    logger.info(CONFIG.configure_sentry())
    CONFIG.check_required_env_vars()
    ctx.obj["stream"] = stream

    logger.info("Running process")


@main.result_callback()
@click.pass_context
def post_main_group_subcommand(
    ctx: click.Context,
    *_args: tuple,
    **_kwargs: dict,
) -> None:
    """Callback for any work to perform after a main sub-command completes."""
    logger.info(
        "Total time elapsed: %s",
        str(
            timedelta(seconds=perf_counter() - ctx.obj["start_time"]),
        ),
    )


@main.command()
@click.pass_context
def reconcile(ctx: click.Context) -> None:
    """Reconcile bitstreams with item identifiers from the metadata."""
    workflow = ctx.obj["workflow"]
    no_bitstreams, no_item_identifiers = workflow.reconcile_bitstreams_and_metadata()

    if no_bitstreams:
        logger.error(f"No bitstreams found for these item identifiers: {no_bitstreams}")
    if no_item_identifiers:
        logger.error(
            f"No item identifiers found for these bitstreams: {no_item_identifiers}"
        )
    else:
        logger.info("All item identifiers and bitstreams successfully matched")


@main.command()
@click.pass_context
def deposit(ctx: click.Context) -> None:
    """Send a batch of item submissions to the DSpace Submission Service (DSS)."""
    workflow = ctx.obj["workflow"]
    logger.debug(f"Beginning submission of batch ID: {workflow.batch_id}")
    submission_results = workflow.run()
    logger.debug(f"Results of submission: {submission_results}")
