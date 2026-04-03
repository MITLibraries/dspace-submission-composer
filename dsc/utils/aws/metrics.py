"""AWS CloudWatch metrics client for workflow submission tracking."""

from __future__ import annotations

import logging

import boto3

from dsc.config import METRICS, METRICS_NAMESPACE

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


class MetricsClient:
    """A simple client to record metrics to AWS CloudWatch."""

    def __init__(self) -> None:
        """Initialize the MetricsClient."""
        self.cloudwatch = boto3.client("cloudwatch")
        self.batch_metrics: list[dict] = []

    def publish_single_metric(
        self,
        metric_name: str,
        value: int,
        unit: str,
        metric_dimensions: dict[str, str] | None = None,
    ) -> None:
        """Publish a single metric to CloudWatch.

        Args:
            metric_name: The name of the metric to publish.
            value: The value of the metric.
            unit: The unit of the metric.
            metric_dimensions: Optional dictionary of dimension names and values.

        Raises:
            ValueError: If unit is invalid.
        """
        metric_data = self._validate_and_build_metric_data(
            metric_name, value, unit, metric_dimensions
        )
        self._push_metric_data([metric_data])

    def _validate_and_build_metric_data(
        self,
        metric_name: str,
        value: int,
        unit: str,
        metric_dimensions: dict[str, str] | None = None,
    ) -> dict:
        """Validate and build a metric data dictionary for CloudWatch.

        Args:
            metric_name: The name of the metric.
            value: The value of the metric.
            unit: The unit of the metric.
            metric_dimensions: Optional dictionary of dimension names and values.

        Returns:
            A metric data dictionary formatted for CloudWatch.
        """
        self._approved_metric(metric_name)
        self._validate_unit(unit)
        dimensions = [
            {"Name": name, "Value": dim_value}
            for name, dim_value in (
                metric_dimensions.items() if metric_dimensions else []
            )
        ]
        return {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit,
            "Dimensions": dimensions,
        }

    def _approved_metric(self, metric_name: str) -> bool:
        """Check if a metric name is in the approved list of metrics for the application.

        Args:
            metric_name: The name of the metric to check.
        """
        if metric_name not in METRICS:
            raise ValueError(
                f"Metric name '{metric_name}' is not in the approved list of metrics: "
                f"{', '.join(METRICS)}"
            )
        return True

    def _validate_unit(self, unit: str) -> None:
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

    def _push_metric_data(self, metric_data: list[dict]) -> None:
        """Push metric data to CloudWatch.

        Args:
            metric_data: List of metric dictionaries to push.
        """
        try:
            self.cloudwatch.put_metric_data(
                Namespace=METRICS_NAMESPACE, MetricData=metric_data
            )
            logger.info(f"Published metric with {metric_data} to CloudWatch.")
        except Exception:
            logger.exception(
                f"Failed to publish metric with {metric_data} to CloudWatch."
            )

    def add_metric_to_batch(
        self,
        metric_name: str,
        value: int,
        unit: str,
        metric_dimensions: dict[str, str] | None = None,
    ) -> None:
        """Add a metric to the batch for later publishing.

        Args:
            metric_name: The name of the metric.
            value: The value of the metric.
            unit: The unit of the metric.
            metric_dimensions: Optional dictionary of dimension names and values.

        Raises:
            ValueError: If unit is invalid.
        """
        metric_data = self._validate_and_build_metric_data(
            metric_name, value, unit, metric_dimensions
        )
        self.batch_metrics.append(metric_data)

    def publish_batch_metrics(self, batch_size: int = 20) -> None:
        """Publish all accumulated batch metrics to CloudWatch.

        Raises:
            ValueError: If any metric has an invalid unit or missing required fields.
        """
        if not self.batch_metrics:
            logger.info("No metrics to publish.")
            return

        # Validate all metrics before publishing
        for metric in self.batch_metrics:
            if not all(key in metric for key in ["MetricName", "Value", "Unit"]):
                raise ValueError(
                    f"Each metric must contain 'MetricName', 'Value', and 'Unit'. "
                    f"Invalid metric: {metric}"
                )
            self._approved_metric(metric["MetricName"])
            self._validate_unit(metric["Unit"])

        for x in range(0, len(self.batch_metrics), batch_size):
            self._push_metric_data(self.batch_metrics[x : x + batch_size])
        self.batch_metrics.clear()
