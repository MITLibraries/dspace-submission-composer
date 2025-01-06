import logging
from datetime import timedelta
from io import StringIO
from time import perf_counter

import click

from dsc.config import Config
from dsc.utilities import (
    build_bitstream_dict,
    match_bitstreams_to_item_identifiers,
    match_item_identifiers_to_bitstreams,
)
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


@main.command()
@click.pass_context
@click.option(
    "-f",
    "--file_type",
    help="Optional parameter to filter bitstreams to specified file type",
)
def reconcile(ctx: click.Context, file_type: str) -> None:
    """Match files in the S3 directory with entries in the batch metadata."""
    workflow = ctx.obj["workflow"]

    bitstream_dict = build_bitstream_dict(
        workflow.s3_bucket, file_type, workflow.batch_path
    )

    # extract item identifiers from batch metadata
    item_identifiers = [
        workflow.get_item_identifier(item_metadata)
        for item_metadata in workflow.item_metadata_iter()
    ]

    # reconcile item identifiers against S3 files
    item_identifier_matches = match_item_identifiers_to_bitstreams(
        bitstream_dict.keys(), item_identifiers
    )
    file_matches = match_bitstreams_to_item_identifiers(
        bitstream_dict.keys(), item_identifiers
    )
    no_bitstreams = set(item_identifiers) - set(item_identifier_matches)
    no_identifiers = set(bitstream_dict.keys()) - set(file_matches)

    logger.info(
        f"Item identifiers and bitstreams successfully matched: {item_identifier_matches}"
    )
    if no_bitstreams:
        logger.error(
            f"No bitstreams found for the following item identifiers: {no_bitstreams}"
        )
    if no_identifiers:
        logger.error(
            f"No item identifiers found for the following bitstreams: {no_identifiers}"
        )
