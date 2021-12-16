import logging
import pathlib
import sys

import click
from pydantic import ValidationError

import datahub as datahub_package
from datahub.cli.generate_report.report_generator import ReportGenerator
from datahub.configuration.config_loader import load_config_file

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    help="Config file in .toml or .yaml format.",
    required=True,
)
def generate_report(config: str) -> None:
    """Generate reports from DataHub."""
    logger.debug("DataHub CLI version: %s", datahub_package.nice_version_name())

    config_file = pathlib.Path(config)
    config = load_config_file(config_file)

    try:
        logger.debug(f"Using config: {config}")
        report_generator = ReportGenerator.create(config)
    except ValidationError as e:
        click.echo(e, err=True)
        sys.exit(1)

    logger.info("Starting generate report")
    ret = report_generator.generate()
    logger.info("Finished generate report")
    sys.exit(ret)
