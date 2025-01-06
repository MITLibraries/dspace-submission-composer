import logging
from datetime import timedelta
from io import StringIO
from time import perf_counter

import click

from dsc.config import Config
from dsc.workflows.base import BaseWorkflow

logger = logging.getLogger(__name__)
CONFIG = Config()


@click.group()
@click.pass_context
@click.option(
    "-w",
    "--workflow_name",
    help="The workflow to use for the batch of DSpace submissions",
    required=True,
)
@click.option(
    "-b",
    "--batch_id",
    help="The S3 prefix for the batch of DSpace submissions",
    required=True,
)
@click.option(
    "-c",
    "--collection_handle",
    help="The handle of the DSpace collection to which the batch will be submitted",
    required=True,
)
@click.option(
    "-v", "--verbose", is_flag=True, help="Pass to log at debug level instead of info"
)
def main(
    ctx: click.Context,
    workflow_name: str,
    collection_handle: str,
    batch_id: str,
    verbose: bool,  # noqa: FBT001
) -> None:
    ctx.ensure_object(dict)
    ctx.obj["start_time"] = perf_counter()

    ctx.obj["workflow"] = BaseWorkflow.load(workflow_name, collection_handle, batch_id)

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
