"""AWS CloudWatch metrics client for workflow submission tracking."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import boto3

logger = logging.getLogger(__name__)

CLOUDWATCH_METRICS_LIMIT = 1000

UNIT_VALUES = frozenset(
    [
        "Bits",
        "Bits/Second",
        "Bytes",
        "Bytes/Second",
        "Count",
        "Count/Second",
        "Gigabits",
        "Gigabits/Second",
        "Gigabytes",
        "Gigabytes/Second",
        "Kilobits",
        "Kilobits/Second",
        "Kilobytes",
        "Kilobytes/Second",
        "Megabits",
        "Megabits/Second",
        "Megabytes",
        "Megabytes/Second",
        "Milliseconds",
        "Microseconds",
        "None",
        "Percent",
        "Seconds",
        "Terabits",
        "Terabits/Second",
        "Terabytes",
        "Terabytes/Second",
    ]
)


@dataclass
class Metric:
    """A class representing a single metric to be published to CloudWatch."""

    name: str
    value: int
    unit: str
    dimensions: dict[str, str] | None = None
    namespace: str | None = None


class MetricsClient:
    """A simple client to record metrics to AWS CloudWatch."""

    def __init__(
        self, namespace: str | None = None, allowed_metrics: set[str] | None = None
    ) -> None:
        """Initialize the MetricsClient."""
        self.namespace = namespace
        self.allowed_metrics: set[str] | None = allowed_metrics
        self._cloudwatch = boto3.client("cloudwatch")
        self.batch_metrics: list[Metric] = []

    def publish_metric(
        self,
        metric: Metric,
    ) -> None:
        """Publish a single metric to CloudWatch."""
        self._validate_metric(metric)
        self._publish_metrics([metric])

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
                f"Invalid unit '{unit}'. Must be one of: {', '.join(sorted(UNIT_VALUES))}"
            )
        return True

    def _publish_metrics(self, metrics: list[Metric]) -> None:
        """Publish metrics to CloudWatch.

        Automatically chunks metrics if the list exceeds CloudWatch's limit
        of 1000 metrics per request.

        Args:
            metrics: List of metric instances to publish.
        """
        if not metrics:
            logger.info("No metrics to publish.")
            return

        # Defensively chunk metrics if they exceed CloudWatch's limit
        if len(metrics) > CLOUDWATCH_METRICS_LIMIT:
            logger.info(
                f"Splitting {len(metrics)} metrics into chunks of "
                f"{CLOUDWATCH_METRICS_LIMIT} for CloudWatch compliance."
            )
            for i in range(0, len(metrics), CLOUDWATCH_METRICS_LIMIT):
                chunk = metrics[i : i + CLOUDWATCH_METRICS_LIMIT]
                self._publish_metrics(chunk)
            return

        try:
            # Validate all metrics and ensure consistent namespace
            namespaces = set()
            metric_data = []
            for metric in metrics:
                self._validate_namespace(metric)
                selected_namespace = metric.namespace or self.namespace
                namespaces.add(selected_namespace)

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

            # Ensure all metrics resolve to the same namespace
            if len(namespaces) > 1:
                raise ValueError(  # noqa: TRY301
                    f"Cannot publish metrics with different namespaces in a single "
                    f"request. Found: {namespaces}"
                )

            selected_namespace = namespaces.pop()
            self._cloudwatch.put_metric_data(
                Namespace=selected_namespace,
                MetricData=metric_data,
            )
            logger.info(
                f"Published {len(metrics)} metric(s) to CloudWatch namespace "
                f"'{selected_namespace}'."
            )
        except Exception:
            logger.exception(
                f"Failed to publish {len(metrics)} metric(s) to CloudWatch: "
            )
            raise

    def _validate_namespace(self, metric: Metric) -> bool:
        """Validate metric has a namespace or the client has a default namespace."""
        if not metric.namespace and not self.namespace:
            raise ValueError(
                f"Metric '{metric.name}' must have a namespace if no default "
                f"namespace is set for the MetricsClient."
            )
        return True

    def add_metrics_to_batch(self, metrics: list[Metric]) -> None:
        """Add metrics to the batch queue.

        Args:
            metrics: The metrics to add to the batch.
        """
        for metric in metrics:
            self._validate_metric(metric)
            self.batch_metrics.append(metric)

    def publish_metrics_batch(self, batch_size: int = 20) -> None:
        """Publish a batch of metrics to CloudWatch.

        Clears the batch queue after successful publishing.

        Args:
            batch_size: Number of metrics to publish in each batch. Must be less than
            CloudWatch's limit of 1000 metrics per request.

        Raises:
            Exception: If publishing fails, metrics remain in the batch queue
                for retry or manual handling.
        """
        if not self.batch_metrics:
            logger.info("No metrics to publish.")
            return

        try:
            for x in range(0, len(self.batch_metrics), batch_size):
                batch = self.batch_metrics[x : x + batch_size]
                self._publish_metrics(batch)
        except Exception:
            # Keep only the unpublished metrics (starting from the failed batch)
            self.batch_metrics = self.batch_metrics[x:]
            raise

        # Clear only if all batches published successfully
        self.batch_metrics.clear()
