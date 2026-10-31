"""BigQuery utility helpers for schema creation and table existence checks."""

from __future__ import annotations

from typing import List, Optional

from google.cloud import bigquery


def ensure_dataset(client: bigquery.Client, project: str, dataset_id: str, location: str = "US") -> None:
    """Create the BigQuery dataset if it does not already exist."""
    dataset_ref = f"{project}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def table_exists(client: bigquery.Client, project: str, dataset_id: str, table_id: str) -> bool:
    full_table_id = f"{project}.{dataset_id}.{table_id}"
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False


def create_table_from_schema(
    client: bigquery.Client,
    project: str,
    dataset_id: str,
    table_id: str,
    schema: List[bigquery.SchemaField],
    partition_field: Optional[str] = None,
    clustering_fields: Optional[List[str]] = None,
) -> bigquery.Table:
    full_table_id = f"{project}.{dataset_id}.{table_id}"
    table = bigquery.Table(full_table_id, schema=schema)
    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(field=partition_field)
    if clustering_fields:
        table.clustering_fields = clustering_fields
    return client.create_table(table, exists_ok=True)


def run_query(client: bigquery.Client, sql: str) -> bigquery.table.RowIterator:
    """Execute a BigQuery SQL statement and return the result iterator."""
    job = client.query(sql)
    return job.result()
