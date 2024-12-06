import logging
from datetime import timedelta
from io import StringIO
from time import perf_counter

import click

from dsc.config import Config

logger = logging.getLogger(__name__)
CONFIG = Config()


@click.command()
@click.option(
    "-v", "--verbose", is_flag=True, help="Pass to log at debug level instead of info"
)
def main(*, verbose: bool) -> None:
    start_time = perf_counter()
    stream = StringIO()
    root_logger = logging.getLogger()
    logger.info(CONFIG.configure_logger(root_logger, stream, verbose=verbose))
    logger.info(CONFIG.configure_sentry())
    CONFIG.check_required_env_vars()
    logger.info("Running process")

    # Do things here!

    elapsed_time = perf_counter() - start_time
    logger.info(
        "Total time to complete process: %s", str(timedelta(seconds=elapsed_time))
    )
