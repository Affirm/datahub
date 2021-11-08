import logging
import pathlib
import sys

import click
from click_default_group import DefaultGroup
from pydantic import ValidationError

import datahub as datahub_package
from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.classifier_pipeline import ClassifierPipeline

logger = logging.getLogger(__name__)

@click.group(cls=DefaultGroup)
def classify() -> None:
    """Classify metadata in DataHub."""
    pass


@classify.command(default=True)
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    help="Config file in .toml or .yaml format.",
    required=True,
)
def run(config: str) -> None:
    """Classify metadata in DataHub."""
    logger.debug("DataHub CLI version: %s", datahub_package.nice_version_name())

    config_file = pathlib.Path(config)
    pipeline_config = load_config_file(config_file)

    try:
        logger.debug(f"Using config: {pipeline_config}")
        pipeline = ClassifierPipeline.create(pipeline_config)
    except ValidationError as e:
        click.echo(e, err=True)
        sys.exit(1)

    logger.info("Starting metadata classify")
    pipeline.run()
    logger.info("Finished metadata classify")
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)