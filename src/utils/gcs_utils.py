"""GCS utility helpers for uploading source files and reading from GCS."""

from __future__ import annotations

from pathlib import Path

from google.cloud import storage


def upload_file(bucket_name: str, source_path: str, destination_blob: str) -> str:
    """Upload a local file to GCS and return the gs:// URI."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_path)
    return f"gs://{bucket_name}/{destination_blob}"


def download_file(bucket_name: str, blob_name: str, destination_path: str) -> None:
    """Download a GCS object to a local path."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(destination_path)


def list_blobs(bucket_name: str, prefix: str = "") -> list:
    """List all blobs in a GCS bucket under the given prefix."""
    client = storage.Client()
    return [b.name for b in client.list_blobs(bucket_name, prefix=prefix)]
