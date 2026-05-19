"""
Report helpers for state notebook Cell 5.

Contains the bits you don't need to look at while debugging counts/console:
- Spark table schemas
- Create-run, create-results, and create-perf-results functions
- CSV / XLSX / HTML file writers

The console summary and high-level flow stay in the notebook cell.
"""
import os
import uuid
from datetime import datetime
from dataclasses import asdict

import pandas as pd


def _result_to_row(r):
    try:
        return asdict(r)
    except TypeError as e:
        print(f"REPORT_WRITER_WARNING: non-dataclass item  type={type(r).__name__}  error={e}  value={repr(r)[:300]}")
        return {
            "test_name":       f"<NON_DATACLASS type={type(r).__name__}>",
            "test_field":      "",
            "test_from_state": "",
            "status":          "ERROR",
            "message":         f"Non-dataclass item reached report writer (asdict failed). type={type(r).__name__}  value={repr(r)[:1500]}",
        }


# --------------------------------------------------------------------
# Schemas (runs table has NO count columns — counts are derived from
# the results table on demand in reports).
# --------------------------------------------------------------------
def run_and_result_table_schemas():
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    runs_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("run_user", StringType(), True),
        StructField("run_start_datetime", TimestampType(), True),
        StructField("run_end_datetime", TimestampType(), True),
        StructField("run_by_automation_name", StringType(), True),
        StructField("run_tag", StringType(), True),
        StructField("run_status", StringType(), True),
        StructField("state_under_test", StringType(), True),
    ])

    results_schema = StructType([
        StructField("result_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("test_name", StringType(), True),
        StructField("test_field", StringType(), True),
        StructField("test_from_state", StringType(), True),
        StructField("status", StringType(), True),
        StructField("message", StringType(), True),
    ])
    return runs_schema, results_schema


def perf_table_schema():
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   IntegerType, FloatType)
    return StructType([
        StructField("perf_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("state_under_test", StringType(), True),
        StructField("test_from_state", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("elapsed_seconds", FloatType(), True),
        StructField("result_count", IntegerType(), True),
    ])


# --------------------------------------------------------------------
# Spark table writers
# --------------------------------------------------------------------
def create_run(spark, run_user, run_start_datetime, run_by_automation_name,
               run_tag, run_status, state_under_test,
               runs_table="test_automation_runs2"):
    """Insert one run record. Returns the new run_id."""
    runs_schema, _ = run_and_result_table_schemas()
    run_id = str(uuid.uuid4())
    run_end_datetime = datetime.utcnow()
    row = [(run_id, run_user, run_start_datetime, run_end_datetime,
            run_by_automation_name, run_tag, run_status, state_under_test)]
    spark.createDataFrame(row, schema=runs_schema) \
         .write.mode("append").option("mergeSchema", "true") \
         .saveAsTable(runs_table)
    return run_id


def create_results(spark, run_id, all_test_results,
                   results_table="test_automation_results2"):
    """Insert result rows for the given run_id."""
    if not all_test_results:
        return 0
    _, results_schema = run_and_result_table_schemas()
    rows = []
    for r in all_test_results:
        rows.append((
            str(uuid.uuid4()),
            run_id,
            getattr(r, "test_name", "") or "",
            getattr(r, "test_field", "") or "",
            getattr(r, "test_from_state", "") or "",
            getattr(r, "status", "") or "",
            (getattr(r, "message", "") or "")[:8000],
        ))
    spark.createDataFrame(rows, schema=results_schema) \
         .write.mode("append").option("mergeSchema", "true") \
         .saveAsTable(results_table)
    return len(rows)


def create_perf_results(spark, run_id, perf_timings, state_under_test,
                        perf_table="test_automation_perfresults2"):
    """Insert performance timing rows for the given run_id.

    perf_timings is a list of dicts with keys:
        test_name, test_from_state, elapsed_seconds, result_count
    """
    if not perf_timings:
        return 0
    schema = perf_table_schema()
    rows = []
    for p in perf_timings:
        rows.append((
            str(uuid.uuid4()),
            run_id,
            state_under_test,
            str(p.get("test_from_state", "")),
            str(p.get("test_name", "")),
            float(p.get("elapsed_seconds", 0.0)),
            int(p.get("result_count", 0)),
        ))
    spark.createDataFrame(rows, schema=schema) \
         .write.mode("append").option("mergeSchema", "true") \
         .saveAsTable(perf_table)
    return len(rows)


# --------------------------------------------------------------------
# File writers (CSV / XLSX / HTML)
# --------------------------------------------------------------------
STATUS_COLOURS = {
    "PASS": "background-color:#d5f5e3;color:#27ae60;font-weight:bold;",
    "FAIL": "background-color:#fadbd8;color:#e74c3c;font-weight:bold;",
    "NO_DATA": "background-color:#fef9e7;color:#f39c12;",
    "ERROR": "background-color:#fdecea;color:#922b21;font-weight:bold;",
}


def _style_status(val):
    return STATUS_COLOURS.get(str(val).upper(), "")


def _safe_state_name(state):
    """Strip parens so folder names don't produce // paths."""
    return str(state).replace("(", "_").replace(")", "").replace(" ", "_")


def build_report_folder(test_results_path, state_under_test, timestamp=None):
    """Build ONE folder for this run and return (folder, timestamp, safe_name).
    Call once per notebook run; pass the result into every writer so all
    three files share the same folder and timestamp."""
    if timestamp is None:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe = _safe_state_name(state_under_test)
    folder = os.path.join(test_results_path, f"{timestamp}-{safe}")
    os.makedirs(folder, exist_ok=True)
    return folder, timestamp, safe


def write_csv(all_test_results, state_under_test, folder, timestamp):
    safe = _safe_state_name(state_under_test)
    path = os.path.join(folder, f"test_results_{safe}_{timestamp}.csv")
    df = pd.DataFrame([_result_to_row(r) for r in all_test_results])
    df.to_csv(path, index=False)
    return path


def write_xlsx(all_test_results, state_under_test, folder, timestamp):
    safe = _safe_state_name(state_under_test)
    path = os.path.join(folder, f"test_results_{safe}_{timestamp}.xlsx")
    df = pd.DataFrame([_result_to_row(r) for r in all_test_results])
    try:
        df.style.applymap(_style_status, subset=["status"]).to_excel(
            path, index=False, engine="openpyxl")
    except Exception:
        df.to_excel(path, index=False, engine="openpyxl")
    return path


def write_html(all_test_results, state_under_test, folder, timestamp, counts=None):
    safe = _safe_state_name(state_under_test)
    path = os.path.join(folder, f"test_results_{safe}_{timestamp}.html")
    df = pd.DataFrame([_result_to_row(r) for r in all_test_results])

    if counts is None:
        counts = {"PASS": 0, "FAIL": 0, "NO_DATA": 0, "ERROR": 0, "TOTAL": len(all_test_results)}

    header = f"""
    <div style="font-family:Arial;padding:10px;">
      <h2>{state_under_test} - Test Results</h2>
      <p>Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
      <div style="display:flex;gap:10px;margin-bottom:15px;">
        <div style="background:#ecf0f1;padding:10px;border-radius:4px;"><b>Total:</b> {counts['TOTAL']}</div>
        <div style="background:#d5f5e3;padding:10px;border-radius:4px;"><b>Pass:</b> {counts['PASS']}</div>
        <div style="background:#fadbd8;padding:10px;border-radius:4px;"><b>Fail:</b> {counts['FAIL']}</div>
        <div style="background:#fef9e7;padding:10px;border-radius:4px;"><b>No Data:</b> {counts['NO_DATA']}</div>
        <div style="background:#fdecea;padding:10px;border-radius:4px;"><b>Error:</b> {counts['ERROR']}</div>
      </div>
    </div>
    """
    try:
        table_html = df.style.applymap(_style_status, subset=["status"]).to_html()
    except Exception:
        table_html = df.to_html(index=False)

    with open(path, "w", encoding="utf-8") as f:
        f.write("<html><body>" + header + table_html + "</body></html>")
    return path


def count_by_status(all_test_results):
    counts = {"PASS": 0, "FAIL": 0, "NO_DATA": 0, "ERROR": 0}
    for r in all_test_results:
        s = str(getattr(r, "status", "")).upper().strip()
        if s in counts:
            counts[s] += 1
    counts["TOTAL"] = len(all_test_results)
    return counts
