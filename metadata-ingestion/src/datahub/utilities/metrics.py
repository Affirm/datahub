from abc import abstractmethod
from enum import Enum
from logging import Logger, getLogger
from typing import Any, List, Optional

logger: Logger = getLogger(__name__)


class DatahubCustomEnvironment(Enum):
    MYSQL = 0
    SNOWFLAKE = 1


class DatahubCustomMetric(Enum):
    NEW_DATASET_CLASSIFIED = 0
    NEW_DATASET_CLASSIFIED_AS_PII = 2
    EXISTING_DATASET_CLASSIFIED = 3
    EXISTING_DATASET_CLASSIFIED_AS_PII = 4
    DATASET_CLASSIFICATION_SKIPPED = 5
    DATASET_CLASSIFICATION_FAILED = 6
    SINGLE_DATASET_CLASSIFICATION_TIME = 7
    ALL_DATASETS_CLASSIFICATION_TIME = 8


class DatahubCustomNamespace(Enum):
    DATAHUB_PII_CLASSIFICATION = 0


class DatahubCustomMetricReporter:
    @abstractmethod
    def zero(self, metric: DatahubCustomMetric) -> None:
        pass

    @abstractmethod
    def increment(self, metric: DatahubCustomMetric) -> None:
        pass

    @abstractmethod
    def duration_millis(self, metric: DatahubCustomMetric, millis: float) -> None:
        pass


class CloudWatchDatahubCustomMetricReporter(DatahubCustomMetricReporter):

    cloudwatch_client: Any  # boto3 doesnt have better typing
    dimensions: List[object]
    metric_namespace: DatahubCustomNamespace

    def __init__(
        self,
        cloudwatch_client: Any,
        env: DatahubCustomEnvironment,
        metric_namespace: DatahubCustomNamespace,
    ):
        self.cloudwatch_client = cloudwatch_client
        self.dimensions = self._build_dimensions(env)
        self.metric_namespace = metric_namespace

    def zero(self, metric: DatahubCustomMetric) -> None:
        self._emit_metric(metric, 0, None)

    def increment(self, metric: DatahubCustomMetric) -> None:
        self._emit_metric(metric, 1, None)

    def duration_millis(self, metric: DatahubCustomMetric, millis: float) -> None:
        self._emit_metric(metric, millis, "Milliseconds")

    def _build_dimensions(self, env: DatahubCustomEnvironment) -> List[object]:
        return [{"Name": "Environment", "Value": env.name}]

    def _emit_metric(
        self, metric: DatahubCustomMetric, count: float, unit: Optional[str]
    ) -> None:
        self.cloudwatch_client.put_metric_data(
            Namespace=self.metric_namespace.name,
            MetricData=[
                {
                    "MetricName": metric.name,
                    "Dimensions": self.dimensions,
                    "Unit": "None" if not unit else unit,
                    "Value": count,
                }
            ],
        )
