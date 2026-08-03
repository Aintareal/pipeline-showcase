import base64
import inspect
import json
import os
import sys

# spark_python_task execs this file directly (no __file__, no package context) —
# now that this is a direct job task entrypoint (V2's gold_and_publish.py orchestrator
# is gone), it needs its own repo-root sys.path fix rather than inheriting one from
# an importer.
try:
    _this_file = os.path.abspath(__file__)
except NameError:
    _this_file = os.path.abspath(inspect.currentframe().f_code.co_filename)
sys.path.insert(0, os.path.dirname(os.path.dirname(_this_file)))

import requests
from datetime import datetime, timezone
from pyspark.sql import SparkSession
# dbutils is auto-injected in notebooks but NOT in spark_python_task scripts —
# this SDK shim provides it in both contexts.
from databricks.sdk.runtime import dbutils
from src.common.paths import TBL_GOLD_SUMMARY, TBL_GOLD_TOP_ITEMS, TBL_GOLD_HOURLY, TBL_GOLD_DAILY

spark = SparkSession.builder.getOrCreate()

GITHUB_REPO = "Aintareal/pipeline-showcase"
GITHUB_PATH = "docs/data.json"


def build_snapshot() -> dict:
    s = spark.sql(f"SELECT * FROM {TBL_GOLD_SUMMARY}").collect()[0].asDict()
    top_items = [r.asDict() for r in spark.sql(f"SELECT item_name, order_count FROM {TBL_GOLD_TOP_ITEMS} ORDER BY item_rank").collect()]
    hourly = [{"hour": r["hour"].strftime("%Y-%m-%dT%H:%M:%SZ"), "count": r["order_count"]}
              for r in spark.sql(f"SELECT hour, order_count FROM {TBL_GOLD_HOURLY} ORDER BY hour").collect()]
    daily = [{"date": r["day"].strftime("%Y-%m-%d"), "count": r["order_count"]}
             for r in spark.sql(f"SELECT day, order_count FROM {TBL_GOLD_DAILY} ORDER BY day").collect()]

    rejection_rate_cumulative = (s["rejection_count_cumulative"] /
        (s["rejection_count_cumulative"] + s["order_count_cumulative"])) if (s["order_count_cumulative"] or s["rejection_count_cumulative"]) else 0.0
    denom_24h = s["rejection_count_rolling_24h"] + s["order_count_rolling_24h"]
    rejection_rate_rolling_24h = (s["rejection_count_rolling_24h"] / denom_24h) if denom_24h else 0.0

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "summary": {
            "order_count_cumulative": s["order_count_cumulative"],
            "order_count_rolling_24h": s["order_count_rolling_24h"],
            "total_amount_cumulative": s["total_amount_cumulative"],
            "avg_order_value_cumulative": s["avg_order_value_cumulative"],
            "rejection_count_cumulative": s["rejection_count_cumulative"],
            "rejection_rate_cumulative": round(rejection_rate_cumulative, 4),
            "rejection_count_rolling_24h": s["rejection_count_rolling_24h"],
            "rejection_rate_rolling_24h": round(rejection_rate_rolling_24h, 4),
        },
        "top_items": top_items,
        "hourly_volume_24h": hourly,
        "daily_volume_all_time": daily,
        "scd2": {"active_rows": s["active_silver_rows"], "superseded_rows": s["superseded_silver_rows"]},
    }


def publish(snapshot: dict):
    pat = dbutils.secrets.get(scope="pipeline-showcase", key="github-pat")
    headers = {"Authorization": f"Bearer {pat}", "Accept": "application/vnd.github+json"}
    base_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}"

    get_resp = requests.get(base_url, headers=headers)
    sha = get_resp.json().get("sha") if get_resp.status_code == 200 else None

    content_b64 = base64.b64encode(json.dumps(snapshot, indent=2).encode("utf-8")).decode("ascii")
    body = {"message": f"chore: update gold snapshot ({snapshot['generated_at']})", "content": content_b64, "branch": "main"}
    if sha:
        body["sha"] = sha

    put_resp = requests.put(base_url, headers=headers, json=body)
    put_resp.raise_for_status()


def run():
    publish(build_snapshot())


if __name__ == "__main__":
    run()
