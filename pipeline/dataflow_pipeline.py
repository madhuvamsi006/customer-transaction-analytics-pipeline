"""
Apache Beam pipeline targeting Google Cloud Dataflow.

Reads source files from GCS, parses each entity block, applies Bronze-layer
enrichment (hash, metadata), and writes raw records to BigQuery bronze tables.
Silver and Gold transformations are SQL-based and run via BigQuery jobs after
the Beam ingestion step completes (ELT pattern).

Usage:
    python pipeline/dataflow_pipeline.py \
        --input_path gs://my-bucket/day0.txt \
        --batch_id BATCH_20240101_D0 \
        --bq_project my-gcp-project \
        --bq_dataset bank_bronze \
        --runner DataflowRunner \
        --project my-gcp-project \
        --region us-central1 \
        --temp_location gs://my-bucket/temp \
        --staging_location gs://my-bucket/staging
"""

from __future__ import annotations

import argparse
import hashlib
import logging
from typing import Dict, Generator, List

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

logger = logging.getLogger(__name__)

ENTITY_DELIMITER = "[ENTITY:"


# ---------------------------------------------------------------------------
# DoFns
# ---------------------------------------------------------------------------

class ParseInterleavedFile(beam.DoFn):
    """
    Read and emit (entity_name, row_dict) pairs from an interleaved flat file.
    Emits records tagged by entity name so downstream steps can route them.
    """

    def process(self, file_content: str) -> Generator:
        current_entity = None
        columns: List[str] = []
        header_seen = False

        for raw_line in file_content.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith(ENTITY_DELIMITER) and line.endswith("]"):
                current_entity = line[len(ENTITY_DELIMITER):-1]
                columns = []
                header_seen = False
                continue
            if current_entity is None:
                continue
            if not header_seen:
                columns = [c.strip() for c in line.split(",")]
                header_seen = True
                continue
            values = [v.strip() for v in line.split(",")]
            if len(values) < len(columns):
                values += [""] * (len(columns) - len(values))
            row = dict(zip(columns, values))
            yield (current_entity, row)


class AddBronzeMetadata(beam.DoFn):
    """Enrich each (entity, row) with bronze pipeline metadata columns."""

    def __init__(self, source_file: str, batch_id: str, load_timestamp: str):
        self.source_file = source_file
        self.batch_id = batch_id
        self.load_timestamp = load_timestamp

    def process(self, element):
        entity_name, row = element
        raw_values = list(row.values())
        row_hash = hashlib.sha256("|".join(raw_values).encode()).hexdigest()
        enriched = {
            **row,
            "_source_file": self.source_file,
            "_batch_id": self.batch_id,
            "_load_timestamp": self.load_timestamp,
            "_entity_name": entity_name,
            "_row_hash": row_hash,
        }
        yield (entity_name, enriched)


class RouteByEntity(beam.DoFn):
    """Tag each element with a side-output tag matching its entity name."""

    KNOWN_ENTITIES = [
        "Customers", "Accounts", "Transactions", "Branches",
        "AccountTypes", "TransactionTypes", "Loans", "LoanPayments", "Cards",
    ]

    def process(self, element, *args, **kwargs):
        entity_name, row = element
        if entity_name in self.KNOWN_ENTITIES:
            yield beam.pvalue.TaggedOutput(entity_name, row)


# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------

def build_bq_schema(entity_name: str) -> Dict:
    """Return a BigQuery JSON schema dict for a bronze entity table."""
    base_fields = [
        {"name": "_source_file", "type": "STRING", "mode": "NULLABLE"},
        {"name": "_batch_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "_load_timestamp", "type": "STRING", "mode": "NULLABLE"},
        {"name": "_entity_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "_row_hash", "type": "STRING", "mode": "NULLABLE"},
    ]
    # All business columns are STRING at bronze level; Silver handles casts
    business_columns = _entity_columns(entity_name)
    fields = [
        {"name": col, "type": "STRING", "mode": "NULLABLE"}
        for col in business_columns
    ] + base_fields
    return {"fields": fields}


def _entity_columns(entity_name: str) -> List[str]:
    column_map = {
        "Customers": ["CustomerID","FirstName","LastName","Email","Phone","Address",
                      "City","State","Country","PostalCode","DateOfBirth","JoinDate",
                      "CustomerType","Status"],
        "Accounts": ["AccountID","CustomerID","AccountTypeID","BranchID","SortCode",
                     "AccountNumber","OpenDate","CloseDate","Balance","Currency","Status"],
        "Transactions": ["TransactionID","AccountID","TransactionTypeID","TransactionDate",
                         "Amount","Currency","Description","Channel","ReferenceNo","Status"],
        "Branches": ["BranchID","BranchName","SortCode","Address","City","State","ManagerID","Phone"],
        "AccountTypes": ["AccountTypeID","AccountTypeName","InterestRate","OverdraftLimit",
                         "MinBalance","Description"],
        "TransactionTypes": ["TransactionTypeID","TransactionTypeName","Description","Direction"],
        "Loans": ["LoanID","CustomerID","AccountID","LoanType","Principal","InterestRate",
                  "StartDate","EndDate","EMIAmount","Currency","Status"],
        "LoanPayments": ["PaymentID","LoanID","PaymentDate","Amount","Currency","Status","Remarks"],
        "Cards": ["CardID","AccountID","CustomerID","CardType","CardNumber","ExpiryDate",
                  "Status","IssueDate","DailyLimit"],
    }
    return column_map.get(entity_name, [])


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--batch_id", required=True)
    parser.add_argument("--bq_project", required=True)
    parser.add_argument("--bq_dataset", default="bank_bronze")
    known_args, pipeline_args = parser.parse_known_args(argv)

    from datetime import datetime
    load_ts = datetime.utcnow().isoformat()

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    entities = RouteByEntity.KNOWN_ENTITIES

    with beam.Pipeline(options=options) as p:
        file_content = (
            p
            | "ReadFile" >> beam.io.ReadFromText(known_args.input_path,
                                                  skip_header_lines=0)
            | "CombineLines" >> beam.combiners.ToList()
            | "JoinLines" >> beam.Map(lambda lines: "\n".join(lines))
        )

        parsed = (
            file_content
            | "ParseEntities" >> beam.ParDo(ParseInterleavedFile())
            | "AddMetadata" >> beam.ParDo(
                AddBronzeMetadata(known_args.input_path, known_args.batch_id, load_ts)
            )
        )

        tagged = parsed | "Route" >> beam.ParDo(
            RouteByEntity()
        ).with_outputs(*entities)

        for entity_name in entities:
            table_id = (
                f"{known_args.bq_project}:{known_args.bq_dataset}"
                f".bronze_{entity_name.lower()}"
            )
            (
                tagged[entity_name]
                | f"Write{entity_name}" >> WriteToBigQuery(
                    table=table_id,
                    schema=build_bq_schema(entity_name),
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                )
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
