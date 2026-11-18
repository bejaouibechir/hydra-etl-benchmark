"""DAG « sûr » avec publication par génération : chaque run écrit dans son dossier
output/gen/<génération>/, puis un seul fichier (output/manifest.json) est basculé.
Mêmes secours et mêmes timeouts que daily_revenue_safe ; seule la publication change.

PROBE_PAUSE (secondes, test seulement) : pause après la copie de chaque rapport, pour
pouvoir viser ces instants avec un arrêt brutal. Sans elle, le code ne change pas."""
import os
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HOME.parent / "shared"))
from publish_generation import REPORTS as OUTPUTS, publish as switch_manifest  # noqa: E402


def build(task_id: str, orders: str, stage: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        execution_timeout=timedelta(minutes=10),
        bash_command=(f"cd {HOME} && rm -rf {stage} && mkdir -p {stage} && python ingest.py {orders} "
                      f"&& cd dbt && dbt run --profiles-dir . --threads 1 "
                      f"--vars '{{out_dir: {stage}}}'"),
    )


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def daily_revenue_safe_gen():
    @task
    def clean():
        shutil.rmtree(HOME / "staging", ignore_errors=True)

    main = build("build_main", "data/orders.csv", f"{HOME}/staging/main")
    backup = build("build_backup", "data/backup/orders.csv", f"{HOME}/staging/backup")
    backup.trigger_rule = "one_failed"

    @task(trigger_rule="one_success")
    def publish():
        """Écrit les deux rapports d'un même run dans leur génération, puis bascule le manifeste."""
        generation = os.environ["GEN"]
        pause = float(os.environ.get("PROBE_PAUSE", "0"))
        for stage in ("main", "backup"):
            d = HOME / "staging" / stage
            if all((d / f).exists() for f in OUTPUTS):
                target = HOME / "output" / "gen" / generation
                target.mkdir(parents=True, exist_ok=True)
                for f in OUTPUTS:
                    shutil.copy(d / f, target / f)
                    time.sleep(pause)
                switch_manifest(HOME / "output", generation)
                return stage
        raise RuntimeError("no complete set of reports to publish")

    clean() >> main >> backup
    [main, backup] >> publish()


daily_revenue_safe_gen()
