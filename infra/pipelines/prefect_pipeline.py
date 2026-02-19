#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

ROOT = Path(__file__).resolve().parents[2]


@task(retries=2, retry_delay_seconds=10)
def run_cmd(cmd: list[str], cwd: Path = ROOT) -> None:
    logger = get_run_logger()
    logger.info("Running: %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    if proc.stdout:
        logger.info(proc.stdout.strip())
    if proc.returncode != 0:
        logger.error(proc.stderr.strip())
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")


@task
def validate_files(import_thai_dump: bool, thai_dump_path: str) -> None:
    logger = get_run_logger()
    required = [
        ROOT / "etl" / "jobs" / "build_traceability_mart.py",
        ROOT / "sql" / "api" / "funds_API.sql",
        ROOT / "etl" / "jobs" / "export_dashboard_payload.py",
    ]
    for f in required:
        if not f.exists():
            raise FileNotFoundError(f"Required file missing: {f}")
    if import_thai_dump:
        dump = ROOT / thai_dump_path
        if not dump.exists():
            raise FileNotFoundError(f"Thai dump file missing: {dump}")
    logger.info("Validation passed")


@flow(name="fund-data-auto-pipeline")
def fund_data_auto_pipeline(
    import_thai_dump: bool = False,
    thai_dump_path: str = "data/dumps/อะไรก็ได้ที่ไม่เหมือนเดิม.sql",
    mysql_host: str = "127.0.0.1",
    mysql_port: int = 3307,
    mysql_user: str = "root",
    mysql_password: str = "",
) -> None:
    load_dotenv(ROOT / ".env", override=False)
    logger = get_run_logger()

    mysql_host = os.getenv("MYSQL_HOST", mysql_host)
    mysql_port = int(os.getenv("MYSQL_PORT", str(mysql_port)))
    mysql_user = os.getenv("MYSQL_USER", mysql_user)
    mysql_password = os.getenv("MYSQL_PASSWORD", mysql_password)

    logger.info("Starting pipeline for host=%s port=%s", mysql_host, mysql_port)
    validate_files(import_thai_dump, thai_dump_path)

    if import_thai_dump:
        run_cmd(
            [
                sys.executable,
                str(ROOT / "etl" / "tools" / "import_sql_dump.py"),
                str(ROOT / thai_dump_path),
                "--target-db",
                "raw_thai_funds",
                "--host",
                mysql_host,
                "--port",
                str(mysql_port),
                "--user",
                mysql_user,
                "--password",
                mysql_password,
            ]
        )

    run_cmd([sys.executable, str(ROOT / "etl" / "jobs" / "build_traceability_mart.py")])

    run_cmd(
        [
            sys.executable,
            str(ROOT / "etl" / "tools" / "mysql_apply_sql.py"),
            str(ROOT / "sql" / "api" / "funds_API.sql"),
            "--host",
            mysql_host,
            "--port",
            str(mysql_port),
            "--user",
            mysql_user,
            "--password",
            mysql_password,
        ]
    )

    run_cmd([sys.executable, str(ROOT / "etl" / "jobs" / "export_dashboard_payload.py")])
    logger.info("Pipeline completed")


if __name__ == "__main__":
    # Auto scheduler for local machine when someone runs this file directly.
    fund_data_auto_pipeline.serve(
        name="fund-data-auto",
        cron="0 */6 * * *",
        tags=["mysql", "prefect", "fund-data"],
    )
