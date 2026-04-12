from __future__ import annotations

import subprocess
import sys
import time

from app.database.connection import get_connection
from app.database.schema import initialize_database
from app.pipeline.collector import collect, finalize_batch
from app.pipeline.ingestor import ingest
from app.pipeline.processor import process


def format_duration(seconds: float) -> str:
    total = int(seconds)
    minutes = total // 60
    secs = total % 60
    return f"{minutes}m {secs}s"


def run_pipeline() -> None:
    conn = get_connection()
    initialize_database(conn)

    while True:
        batch = collect()

        if batch is None:
            break

        snapshot_name, snapshot_root, zip_path = batch

        print(f"[main] iniciando lote {snapshot_name}")
        start_time = time.perf_counter()

        processed_files = process(conn, [(snapshot_name, snapshot_root)])
        ingest(conn, processed_files)

        elapsed = time.perf_counter() - start_time
        duration_str = format_duration(elapsed)

        finalize_batch(snapshot_root, zip_path)

        print(f"[main] lote {snapshot_name} finalizado em {duration_str}")

    conn.close()
    print("[main] pipeline finalizado")


def run_web() -> None:
    print("[main] iniciando interface web...")
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", "app/web/interface.py"],
        check=True,
    )


def main() -> None:
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        print("Missing 1 argument. Usage: python3 app.main [pipeline|web|all]")
        return

    if mode == "pipeline":
        run_pipeline()
    elif mode == "web":
        run_web()
    elif mode == "all":
        run_pipeline()
        run_web()
    else:
        print("Modo inválido. Use: pipeline, web ou all")


if __name__ == "__main__":
    main()