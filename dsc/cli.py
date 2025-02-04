import datetime
import logging
from time import perf_counter

import click

from dsc.config import Config
from dsc.exceptions import ReconcileError
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
    help="The S3 prefix for the batch of DSpace submission files",
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
    ctx.obj["workflow"] = workflow

    root_logger = logging.getLogger()
    logger.info(CONFIG.configure_logger(root_logger, verbose=verbose))
    logger.info(CONFIG.configure_sentry())
    CONFIG.check_required_env_vars()

    logger.info("Running process")


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


@main.command()
@click.pass_context
def reconcile(ctx: click.Context) -> None:
    """Reconcile bitstreams with item identifiers from the metadata."""
    workflow = ctx.obj["workflow"]
    try:
        workflow.reconcile_bitstreams_and_metadata()
    except ReconcileError:
        logger.info("Reconcile failed.")
        ctx.exit(1)


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
    help="The recipients of the submission results email as a comma-delimited string, "
    "this will override the workflow class's default value for this attribute",
    required=True,
)
def submit(
    ctx: click.Context,
    collection_handle: str,
    email_recipients: str,  # noqa: ARG001
) -> None:
    """Send a batch of item submissions to the DSpace Submission Service (DSS)."""
    workflow = ctx.obj["workflow"]
    logger.debug(f"Beginning submission of batch ID: {workflow.batch_id}")
    workflow.submit_items(collection_handle)
    # TODO(): workflow.report_results(email_recipients.split(",")) #noqa:FIX002, TD003


@main.command()
@click.pass_context
@click.option(
    "-e",
    "--email-recipients",
    help="The recipients of the submission results email as a comma-delimited string, "
    "this will override the workflow class's default value for this attribute",
    required=True,
)
def finalize(ctx: click.Context, email_recipients: str) -> None:
    """Process the result messages from the DSS output queue according the workflow."""
    workflow = ctx.obj["workflow"]
    workflow.process_results()
    workflow.report_results(email_recipients.split(","))
