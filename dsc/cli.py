import datetime
import logging
from time import perf_counter

import click

from dsc.config import Config
from dsc.reports import FinalizeReport, ReconcileReport, SubmitReport
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
@click.option(
    "-e",
    "--email-recipients",
    help="The recipients of the reconcile results email as a comma-delimited string",
    default=None,
)
def reconcile(ctx: click.Context, email_recipients: str | None = None) -> None:
    """Reconcile bitstreams with item identifiers from the metadata."""
    workflow = ctx.obj["workflow"]

    reconciled = workflow.reconcile_bitstreams_and_metadata()

    if email_recipients:
        workflow.send_report(
            report_class=ReconcileReport, email_recipients=email_recipients.split(",")
        )

    if not reconciled:
        logger.error("Failed to reconcile bitstreams and metadata")
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
    "--skip-items",
    help="Item identifiers to exclude from DSS submissions as a comma-delimited string",
    default=None,
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
    skip_items: list | None = None,
    email_recipients: str | None = None,
) -> None:
    """Send a batch of item submissions to the DSpace Submission Service (DSS)."""
    workflow = ctx.obj["workflow"]
    workflow.submit_items(collection_handle, skip_items)

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
    workflow.process_ingest_results()
    workflow.send_report(
        report_class=FinalizeReport, email_recipients=email_recipients.split(",")
    )


@main.command()
@click.pass_context
@click.option(
    "-i",
    "--items",
    help=(
        "Item identifiers specifying which DSpace metadata JSON files "
        "should be removed, formatted as a comma-delimited string"
    ),
    default=None,
)
@click.option(
    "--remove-all",
    help="Pass to delete all DSpace metadata JSON files in batch folder",
    is_flag=True,
)
@click.option("--execute/--no-execute", help="Pass to perform deletions", default=False)
def remove_dspace_metadata(
    ctx: click.Context,
    items: str | None = None,
    *,
    remove_all: bool = False,
    execute: bool = False,
) -> None:
    """Bulk delete DSpace metadata JSON files from batch folder in S3."""
    workflow = ctx.obj["workflow"]
    workflow.remove_dspace_metadata(
        item_identifiers=items, remove_all=remove_all, execute=execute
    )
