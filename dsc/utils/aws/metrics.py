"""AWS CloudWatch metrics client for workflow submission tracking."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import boto3

from dsc.config import METRICS_NAMESPACE

logger = logging.getLogger(__name__)


UNIT_VALUES = frozenset(
    [
        "Seconds",
        "Microseconds",
        "Milliseconds",
        "Bytes",
        "Kilobytes",
        "Megabytes",
        "Gigabytes",
        "Terabytes",
        "Bits",
        "Kilobits",
        "Megabits",
        "Gigabits",
        "Terabits",
        "Percent",
        "Count",
        "Bytes/Second",
        "Kilobytes/Second",
        "Megabytes/Second",
        "Gigabytes/Second",
        "Terabytes/Second",
        "Bits/Second",
        "Kilobits/Second",
        "Megabits/Second",
        "Gigabits/Second",
        "Terabits/Second",
        "Count/Second",
    ]
)


@dataclass
class Metric:
    """A class representing a single metric to be published to CloudWatch."""

    name: str
    value: int
    unit: str
    dimensions: dict[str, str] | None = None


class MetricsClient:
    """A simple client to record metrics to AWS CloudWatch."""

    def __init__(self, allowed_metrics: set[str] | None = None) -> None:
        """Initialize the MetricsClient."""
        self.namespace = METRICS_NAMESPACE
        self.allowed_metrics: set[str] | None = allowed_metrics
        self._cloudwatch = boto3.client("cloudwatch")
        self.batch_metrics: list[Metric] = []

    def publish_single_metric(
        self,
        metric: Metric,
    ) -> None:
        """Publish a single metric to CloudWatch."""
        self._validate_metric(metric)
        self._push_metric_data([metric])

    def _validate_metric(
        self,
        metric: Metric,
    ) -> bool:
        """Validate that a metric has required fields and allowed unit.

        Args:
            metric: The Metric instance to validate.
        """
        if not all(hasattr(metric, attr) for attr in ["name", "value", "unit"]):
            raise ValueError(
                f"Metric must have 'name', 'value', and 'unit' attributes. Invalid "
                f"metric: {metric}"
            )
        self._allowed_metric(metric.name)
        self._validate_metric_unit(metric.unit)
        return True

    def _allowed_metric(self, metric_name: str) -> bool:
        """Check if a metric name is in the allowed list of metrics for the application.

        Args:
            metric_name: The name of the metric to check.
        """
        if self.allowed_metrics and metric_name not in self.allowed_metrics:
            raise ValueError(
                f"Metric name '{metric_name}' is not in the allowed list of metrics: "
                f"{', '.join(self.allowed_metrics)}"
            )
        return True

    def _validate_metric_unit(self, unit: str) -> bool:
        """Validate that metric unit is allowed by AWS CloudWatch.

        Args:
            unit: The unit to validate.

        Raises:
            ValueError: If unit is not allowed by AWS CloudWatch.
        """
        if unit not in UNIT_VALUES:
            raise ValueError(
                f"Invalid unit '{unit}'. Must be one of: {', '.join(UNIT_VALUES)}"
            )
        return True

    def _push_metric_data(self, metrics: list[Metric]) -> None:
        """Push metrics to CloudWatch.

        Args:
            metrics: List of metric instances to push.
        """
        if not metrics:
            logger.info("No metrics to publish.")
            return

        try:
            metric_data = []
            for metric in metrics:
                metric_dict = {
                    "MetricName": metric.name,
                    "Value": metric.value,
                    "Unit": metric.unit,
                }
                if metric.dimensions:
                    metric_dict["Dimensions"] = [
                        {"Name": key, "Value": value}
                        for key, value in metric.dimensions.items()
                    ]
                metric_data.append(metric_dict)

            self._cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=metric_data,
            )
            logger.info(
                f"Published {len(metrics)} metric(s) to CloudWatch namespace "
                f"'{self.namespace}'."
            )
        except Exception:
            logger.exception(
                f"Failed to publish {len(metrics)} metric(s) to CloudWatch: "
            )
            raise

    def add_metric_to_batch(self, metric: Metric) -> None:
        """Add a metric to the batch queue.

        Args:
            metric: The metric to add to the batch.
        """
        self._validate_metric(metric)
        self.batch_metrics.append(metric)

    def publish_batch_metrics(self, batch_size: int = 20) -> None:
        """Publish a batch of metrics to CloudWatch.

        Clears the batch queue after publishing.

        Args:
            batch_size: Number of metrics to publish in each batch.
        """
        if not self.batch_metrics:
            logger.info("No metrics to publish.")
            return

        try:
            # Re-validate all metrics before publishing to CloudWatch
            for metric in self.batch_metrics:
                self._validate_metric(metric)

            for x in range(0, len(self.batch_metrics), batch_size):
                batch = self.batch_metrics[x : x + batch_size]
                self._push_metric_data(batch)
        finally:
            self.batch_metrics.clear()
