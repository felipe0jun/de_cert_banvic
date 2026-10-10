"""
# banvic_ingestion

Pipeline ELT do BanVic: do arquivo legado `banvic_data.zip` ao PostgreSQL.

Fluxo:
1. `wait_for_source_zip`  - FileSensor espera o zip aparecer em data/source.
2. `extract_zip_to_landing` - descompacta em data/landing, valida os 7 CSVs e
   conta linhas (XCom) para a verificação final.
3. `load_raw.*`           - um pod Meltano (tap-csv -> target-postgres) por
   tabela, respeitando as dependências de chave estrangeira.
4. `validate_raw`         - compara linhas do CSV x linhas em `raw.<tabela>`
   e grava um registro de auditoria.
5. `build_staging_views`  - views tipadas e limpas no esquema `stg` (CEP/CPF
   normalizados, timestamps em horário local, valores arredondados, flags de anomalia).
6. `run_quality_checks`   - checagens de qualidade sobre `stg`, gravadas em
   `dq.check_results`; falha só em severidade `error`, alerta em `warn`.

Idempotência: o extract sobrescreve o landing, o Meltano faz upsert pelas
chaves primárias e as views são recriadas (DROP IF EXISTS + CREATE). Reexecutar
não duplica. O zip deve ser um snapshot completo: linhas removidas na fonte não
são apagadas no destino.
"""

from __future__ import annotations

import csv
import logging
import os
import shutil
import tempfile
import zipfile
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG, task, task_group
from kubernetes.client import models as k8s

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("BANVIC_DATA_DIR", "/opt/airflow/data"))
SOURCE_ZIP = DATA_DIR / "source" / "banvic_data.zip"
LANDING_DIR = DATA_DIR / "landing"

K8S_NAMESPACE = os.environ.get("BANVIC_K8S_NAMESPACE", "banvic")
MELTANO_IMAGE = os.environ.get("BANVIC_MELTANO_IMAGE", "banvic/meltano:local")
MELTANO_SECRET = os.environ.get("BANVIC_MELTANO_SECRET", "meltano-target-postgres")
NODE_DATA_PATH = "/mnt/banvic/data"

DW_CONN_ID = "banvic_dw"
RAW_SCHEMA = "raw"
DQ_SCHEMA = "dq"
AUDIT_TABLE = f"{DQ_SCHEMA}.ingestion_audit"
DQ_SQL = Path(__file__).parent / "sql" / "dq_checks.sql"

TABLES: dict[str, list[str]] = {
    "agencias": [],
    "clientes": [],
    "colaboradores": [],
    "colaborador_agencia": ["colaboradores", "agencias"],
    "contas": ["clientes", "agencias", "colaboradores"],
    "propostas_credito": ["clientes", "colaboradores"],
    "transacoes": ["contas"],
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

def _count_csv_rows(path: Path) -> int:
    """Conta registros respeitando quebras de linha dentro de aspas."""
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader, None)
        return sum(1 for _ in reader)

with DAG(
    dag_id="banvic_ingestion",
    description="Ingestão dos CSVs do ERP BanVic para o PostgreSQL (raw + stg).",
    doc_md=__doc__,
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["banvic", "elt", "meltano"],
) as dag:

    wait_for_source_zip = FileSensor(
        task_id="wait_for_source_zip",
        filepath=str(SOURCE_ZIP),
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 30,
        mode="reschedule",
    )

    @task(task_id="extract_zip_to_landing")
    def extract_zip_to_landing() -> dict[str, int]:
        if not zipfile.is_zipfile(SOURCE_ZIP):
            raise AirflowException(f"{SOURCE_ZIP} não é um zip válido")

        expected = {f"{t}.csv" for t in TABLES}
        LANDING_DIR.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=LANDING_DIR, prefix=".extract-") as tmp:
            with zipfile.ZipFile(SOURCE_ZIP) as zf:
                names = {Path(n).name for n in zf.namelist() if n.endswith(".csv")}
                missing = expected - names
                if missing:
                    raise AirflowException(f"CSVs ausentes no zip: {sorted(missing)}")
                for member in zf.namelist():
                    if Path(member).name in expected:
                        target = Path(tmp) / Path(member).name
                        with zf.open(member) as src, target.open("wb") as dst:
                            shutil.copyfileobj(src, dst)

            counts: dict[str, int] = {}
            for name in sorted(expected):
                tmp_file = Path(tmp) / name
                counts[name.removesuffix(".csv")] = _count_csv_rows(tmp_file)
                os.replace(tmp_file, LANDING_DIR / name)

        log.info("Linhas por tabela no CSV: %s", counts)
        return counts

    data_volume = k8s.V1Volume(
        name="banvic-data",
        host_path=k8s.V1HostPathVolumeSource(path=NODE_DATA_PATH, type="Directory"),
    )
    data_mount = k8s.V1VolumeMount(
        name="banvic-data", mount_path="/project/data", read_only=True
    )

    @task_group(group_id="load_raw")
    def load_raw() -> None:
        ops: dict[str, KubernetesPodOperator] = {}
        for table in TABLES:
            ops[table] = KubernetesPodOperator(
                task_id=f"load_{table}",
                name=f"meltano-load-{table.replace('_', '-')}",
                namespace=K8S_NAMESPACE,
                image=MELTANO_IMAGE,
                image_pull_policy="IfNotPresent",
                cmds=["meltano"],
                arguments=[
                    "el", "tap-csv", "target-postgres",
                    "--select", f"{table}.*",
                ],
                env_from=[
                    k8s.V1EnvFromSource(
                        secret_ref=k8s.V1SecretEnvSource(name=MELTANO_SECRET)
                    )
                ],
                volumes=[data_volume],
                volume_mounts=[data_mount],
                container_resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "100m", "memory": "256Mi"},
                    limits={"memory": "1Gi"},
                ),
                in_cluster=True,
                get_logs=True,
                log_events_on_failure=True,
                on_finish_action="delete_pod",
                startup_timeout_seconds=300,
            )
        for table, deps in TABLES.items():
            for dep in deps:
                ops[dep] >> ops[table]

    @task(task_id="validate_raw", retries=0)
    def validate_raw(expected: dict[str, int], **context) -> None:
        hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
        mismatches = []
        rows = []
        for table, csv_rows in expected.items():
            db_rows = hook.get_first(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table}")[0]
            status = "ok" if db_rows == csv_rows else "mismatch"
            log.info("%-22s csv=%7d db=%7d %s", table, csv_rows, db_rows, status)
            rows.append((context["run_id"], table, csv_rows, db_rows, status))
            if status != "ok":
                mismatches.append(f"{table}: csv={csv_rows} db={db_rows}")

        hook.run(
            f"""
            CREATE SCHEMA IF NOT EXISTS {DQ_SCHEMA};
            CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                run_id      text        NOT NULL,
                table_name  text        NOT NULL,
                csv_rows    integer     NOT NULL,
                db_rows     integer     NOT NULL,
                status      text        NOT NULL,
                validated_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (run_id, table_name)
            )
            """
        )
        hook.insert_rows(
            table=AUDIT_TABLE,
            rows=rows,
            target_fields=["run_id", "table_name", "csv_rows", "db_rows", "status"],
            replace=True,
            replace_index=["run_id", "table_name"],
        )
        if mismatches:
            raise AirflowException("Divergência CSV x banco: " + "; ".join(mismatches))

    build_staging_views = SQLExecuteQueryOperator(
        task_id="build_staging_views",
        conn_id=DW_CONN_ID,
        sql="sql/stg_views.sql",
    )

    @task(task_id="run_quality_checks", retries=0)
    def run_quality_checks(**context) -> None:
        hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
        hook.run(DQ_SQL.read_text(), parameters={"run_id": context["run_id"]})
        results = hook.get_records(
            "SELECT check_name, severity, failed_rows, total_rows, detail "
            "FROM dq.check_results WHERE run_id = %(run_id)s "
            "ORDER BY severity, check_name",
            parameters={"run_id": context["run_id"]},
        )
        errors = []
        for name, severity, failed, total, detail in results:
            line = f"[{severity:5}] {name:<50} {failed:>7}/{total:<7} {detail}"
            if failed and severity == "error":
                log.error(line)
                errors.append(name)
            elif failed and severity == "warn":
                log.warning(line)
            else:
                log.info(line)
        if errors:
            raise AirflowException(f"Checks de qualidade com erro: {errors}")

    counts = extract_zip_to_landing()
    raw_loaded = load_raw()
    validated = validate_raw(counts)
    quality = run_quality_checks()

    wait_for_source_zip >> counts >> raw_loaded >> validated
    validated >> build_staging_views >> quality
