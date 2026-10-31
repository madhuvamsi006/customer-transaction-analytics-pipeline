"""
Loads project_config.yaml and exposes typed helpers used across all pipeline layers.
Every script imports this — no hardcoded project values anywhere else.
"""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


def _find_config(path: Optional[str] = None) -> Path:
    """Resolve config file path: CLI arg → env var → repo root default."""
    if path:
        return Path(path)
    env_path = os.environ.get("PIPELINE_CONFIG")
    if env_path:
        return Path(env_path)
    # Walk up from this file to find project_config.yaml at repo root
    current = Path(__file__).resolve()
    for parent in [current, *current.parents]:
        candidate = parent / "project_config.yaml"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "project_config.yaml not found. Set PIPELINE_CONFIG env var or pass --config."
    )


@dataclass
class GCPConfig:
    project_id: str
    region: str
    zone: str
    service_account: str


@dataclass
class GCSConfig:
    bucket: str
    input_prefix: str
    output_prefix: str
    config_prefix: str
    temp_prefix: str
    staging_prefix: str
    source_files: dict

    def input_uri(self, day: str) -> str:
        key = f"day{day}"
        rel = self.source_files.get(key, f"day{day}/day{day}.txt")
        return f"gs://{self.bucket}/{self.input_prefix}/{rel}"

    def temp_uri(self) -> str:
        return f"gs://{self.bucket}/{self.temp_prefix}"

    def staging_uri(self) -> str:
        return f"gs://{self.bucket}/{self.staging_prefix}"


@dataclass
class BigQueryConfig:
    project_id: str
    dataset: str
    location: str
    table_prefixes: dict
    write_mode: dict

    def table(self, layer: str, name: str) -> str:
        prefix = self.table_prefixes.get(layer, "")
        return f"{self.project_id}.{self.dataset}.{prefix}{name}"


@dataclass
class DataprocConfig:
    cluster_name: str
    master_machine_type: str
    worker_machine_type: str
    num_workers: int
    image_version: str
    optional_components: list
    enable_component_gateway: bool
    auto_delete_cluster: bool
    idle_delete_ttl: str


@dataclass
class DataflowConfig:
    machine_type: str
    max_workers: int
    disk_size_gb: int
    use_public_ips: bool
    enable_streaming_engine: bool
    subnetwork: str


@dataclass
class EngineConfig:
    type: str
    dataproc: DataprocConfig
    dataflow: DataflowConfig


@dataclass
class SCD2Config:
    effective_start_col: str
    effective_end_col: str
    is_current_col: str
    surrogate_key_col: str


@dataclass
class StructuringConfig:
    near_threshold_pct: float
    window_days: int
    min_occurrences: int
    channels: list


@dataclass
class DormantConfig:
    inactivity_days: int
    burst_window_days: int
    burst_min_transactions: int


@dataclass
class PipelineConfig:
    batch_id_prefix: str
    high_value_threshold: float
    structuring: StructuringConfig
    dormant: DormantConfig
    scd2: SCD2Config
    replay_safe: bool
    quarantine_enabled: bool

    def batch_id(self, day: int) -> str:
        return f"{self.batch_id_prefix}_DAY{day}"


@dataclass
class ProjectConfig:
    gcp: GCPConfig
    gcs: GCSConfig
    bigquery: BigQueryConfig
    engine: EngineConfig
    pipeline: PipelineConfig
    environment: str

    @classmethod
    def load(cls, path: Optional[str] = None) -> "ProjectConfig":
        config_path = _find_config(path)
        with open(config_path) as f:
            raw = yaml.safe_load(f)

        gcp_raw = raw["gcp"]
        gcp = GCPConfig(
            project_id=gcp_raw["project_id"],
            region=gcp_raw["region"],
            zone=gcp_raw["zone"],
            service_account=gcp_raw.get("service_account", ""),
        )

        gcs_raw = raw["gcs"]
        gcs = GCSConfig(
            bucket=gcs_raw["bucket"],
            input_prefix=gcs_raw.get("input_prefix", "input"),
            output_prefix=gcs_raw.get("output_prefix", "output"),
            config_prefix=gcs_raw.get("config_prefix", "config"),
            temp_prefix=gcs_raw.get("temp_prefix", "temp"),
            staging_prefix=gcs_raw.get("staging_prefix", "staging"),
            source_files=gcs_raw.get("source_files", {}),
        )

        bq_raw = raw["bigquery"]
        bq = BigQueryConfig(
            project_id=gcp_raw["project_id"],
            dataset=bq_raw["dataset"],
            location=bq_raw.get("location", "US"),
            table_prefixes=bq_raw.get("table_prefixes", {}),
            write_mode=bq_raw.get("write_mode", {}),
        )

        eng_raw = raw["engine"]
        dp_raw = eng_raw.get("dataproc", {})
        df_raw = eng_raw.get("dataflow", {})

        dataproc = DataprocConfig(
            cluster_name=dp_raw.get("cluster_name", "txn-pipeline-cluster"),
            master_machine_type=dp_raw.get("master_machine_type", "n1-standard-4"),
            worker_machine_type=dp_raw.get("worker_machine_type", "n1-standard-4"),
            num_workers=dp_raw.get("num_workers", 2),
            image_version=dp_raw.get("image_version", "2.1-debian11"),
            optional_components=dp_raw.get("optional_components", []),
            enable_component_gateway=dp_raw.get("enable_component_gateway", True),
            auto_delete_cluster=dp_raw.get("auto_delete_cluster", True),
            idle_delete_ttl=dp_raw.get("idle_delete_ttl", "3600s"),
        )

        dataflow = DataflowConfig(
            machine_type=df_raw.get("machine_type", "n1-standard-4"),
            max_workers=df_raw.get("max_workers", 5),
            disk_size_gb=df_raw.get("disk_size_gb", 100),
            use_public_ips=df_raw.get("use_public_ips", False),
            enable_streaming_engine=df_raw.get("enable_streaming_engine", False),
            subnetwork=df_raw.get("subnetwork", ""),
        )

        engine = EngineConfig(type=eng_raw["type"], dataproc=dataproc, dataflow=dataflow)

        pl_raw = raw["pipeline"]
        st_raw = pl_raw.get("structuring", {})
        dm_raw = pl_raw.get("dormant", {})
        s2_raw = pl_raw.get("scd2", {})

        pipeline = PipelineConfig(
            batch_id_prefix=pl_raw.get("batch_id_prefix", "BATCH"),
            high_value_threshold=pl_raw.get("high_value_threshold", 100000),
            structuring=StructuringConfig(
                near_threshold_pct=st_raw.get("near_threshold_pct", 0.05),
                window_days=st_raw.get("window_days", 3),
                min_occurrences=st_raw.get("min_occurrences", 2),
                channels=st_raw.get("channels", ["Branch", "ATM"]),
            ),
            dormant=DormantConfig(
                inactivity_days=dm_raw.get("inactivity_days", 90),
                burst_window_days=dm_raw.get("burst_window_days", 7),
                burst_min_transactions=dm_raw.get("burst_min_transactions", 3),
            ),
            scd2=SCD2Config(
                effective_start_col=s2_raw.get("effective_start_col", "eff_start_date"),
                effective_end_col=s2_raw.get("effective_end_col", "eff_end_date"),
                is_current_col=s2_raw.get("is_current_col", "is_current"),
                surrogate_key_col=s2_raw.get("surrogate_key_col", "sk_id"),
            ),
            replay_safe=pl_raw.get("replay_safe", True),
            quarantine_enabled=pl_raw.get("quarantine_enabled", True),
        )

        return cls(
            gcp=gcp,
            gcs=gcs,
            bigquery=bq,
            engine=engine,
            pipeline=pipeline,
            environment=raw.get("environment", "dev"),
        )
