# TODO impl https://jira.team.affirm.com/browse/DF-1739

import logging
import os
import pathlib
import sys

import click
from click_default_group import DefaultGroup
from pydantic import ValidationError
from tabulate import tabulate

import datahub as datahub_package
from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.pipeline import Pipeline

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup)
def generate_report() -> None:
    """Ingest metadata into DataHub."""
    pass


@generate_report.command(default=True)
def run(config: str) -> None:
    """Ingest metadata into DataHub."""
    logger.debug("DataHub CLI version: %s", datahub_package.nice_version_name())

    config_file = pathlib.Path(config)
    pipeline_config = load_config_file(config_file)

    try:
        logger.debug(f"Using config: {pipeline_config}")
        pipeline = Pipeline.create(pipeline_config)
    except ValidationError as e:
        click.echo(e, err=True)
        sys.exit(1)

    logger.info("Starting metadata generate_report")
    logger.info(os.environ["S3_OUTPUT_PATH_ROOT"])
    pipeline.run()
    logger.info("Finished metadata generate_report")
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)
