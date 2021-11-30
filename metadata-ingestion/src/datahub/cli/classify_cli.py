import logging
import os
import pathlib
import sys

import boto3
import click
from click_default_group import DefaultGroup
from pydantic import ValidationError

import datahub as datahub_package
from datahub.classification.classifier_pipeline import ClassifierPipeline
from datahub.configuration.config_loader import load_config_file
from datahub.utilities.metrics import (
    CloudWatchDatahubCustomMetricReporter,
    DatahubCustomEnvironment,
    DatahubCustomMetricReporter,
    DatahubCustomNamespace,
)

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
    """Classify datasetes in DataHub."""
    logger.debug("DataHub CLI version: %s", datahub_package.nice_version_name())

    config_file = pathlib.Path(config)
    pipeline_config = load_config_file(config_file)

    pipeline_config.update(
        {
            "num_shards": os.environ["NUM_SHARDS"].split("-")[1],
            "shard_id": os.environ["SHARD_ID"].split("-")[1],
        }
    )

    reporter: DatahubCustomMetricReporter = CloudWatchDatahubCustomMetricReporter(
        boto3.client("cloudwatch"),
        DatahubCustomEnvironment[pipeline_config["source"]["type"].upper()],
        DatahubCustomNamespace.DATAHUB_PII_CLASSIFICATION,
    )
    try:
        pipeline = ClassifierPipeline.create(
            pipeline_config, boto3.client("dynamodb"), boto3.client("kms"), reporter
        )
    except ValidationError as e:
        click.echo(e, err=True)
        sys.exit(1)

    logger.info("Starting metadata classification")
    pipeline.run()
    logger.info("Finished metadata classification")
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)
