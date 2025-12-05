"""Pub/Sub client utilities for triggering asynchronous processing."""

import logging
import os

from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


def trigger_processing(topic_name: str = "process-responses", project_id: str = "gcp-demos-411520") -> None:
    """Publish a message to trigger response processing via Cloud Function.

    Args:
        topic_name: Name of the Pub/Sub topic (default: "process-responses")
        project_id: GCP project ID (default: "gcp-demos-411520")
    """
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)

        # Publish a simple trigger message
        future = publisher.publish(topic_path, b"trigger_processing")
        message_id = future.result()

        logger.info(f"Published processing trigger to {topic_name} (message ID: {message_id})")

    except Exception as e:
        logger.error(f"Failed to publish processing trigger: {e}")
        raise
