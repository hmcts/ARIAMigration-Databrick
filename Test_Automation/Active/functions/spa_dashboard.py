# dashboard.py
# Single-page HTML dashboard (Runs + Run Details + Compare) for Databricks notebooks.

from databricks.sdk.runtime import *
from datetime import datetime
import os
import json
import gzip
import base64

from pyspark.sql import functions as F

OUTPUT_DIR = "/dbfs/tmp"
MAX_RUNS = 20

# Easy UI tuning at top of file
STATE_COL_WIDTH = 240
USER_COL_WIDTH = 140
AUTOMATION_COL_WIDTH = 140
TAG_COL_WIDTH = 150
RUN_ID_COL_WIDTH = 160
START_COL_WIDTH = 135
END_COL_WIDTH = 135

RESULT_TESTFIELD_COL_WIDTH = 150
RESULT_TESTSTATE_COL_WIDTH = 110
RESULT_MESSAGE_COL_WIDTH = 520

RUNS_PAGE_LENGTH = 15
RESULTS_PAGE_LENGTH = 15


def _safe_dt_str(v):
    if v is None:
        return ""
    try:
        if hasattr(v, "to_pydatetime"):
            v = v.to_pydatetime()
        if hasattr(v, "strftime"):
            return v.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return str(v)


def _compress_json_for_html(obj):
    json_text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    compressed = gzip.compress(json_text.encode("utf-8"))
    return base64.b64encode(compressed).decode("ascii")


def generate_dashboard_single_page(
    runs_table,
    results_table,
    output_dir: str = OUTPUT_DIR,
    max_runs: int = MAX_RUNS
):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")

    runs_pdf = spark.table(runs_table).toPandas()

    if "run_start_datetime" in runs_pdf.columns:
        runs_pdf = runs_pdf.sort_values(
            by="run_start_datetime",
            ascending=False,
            na_position="last"
        )
    elif "run_end_datetime" in runs_pdf.columns:
        runs_pdf = runs_pdf.sort_values(
            by="run_end_datetime",
            ascending=False,
            na_position="last"
        )

    runs_pdf = runs_pdf.head(max_runs).copy()

    runs_list = []
    all_results = {}

    for _, run in runs_pdf.iterrows():
        run_id = run.get("run_id")
        if not run_id:
            continue

        results_pdf = (
            spark.table(results_table)
            .filter(F.col("run_id") == str(run_id))
            .toPandas()
        )

        if "status" not in results_pdf.columns:
            results_pdf["status"] = None
        if "result_id" not in results_pdf.columns:
            results_pdf["result_id"] = None
        if "test_name" not in results_pdf.columns:
            results_pdf["test_name"] = None
        if "test_field" not in results_pdf.columns:
            results_pdf["test_field"] = None
        if "test_from_state" not in results_pdf.columns:
            results_pdf["test_from_state"] = None
        if "message" not in results_pdf.columns:
            results_pdf["message"] = None
        if "run_id" not in results_pdf.columns:
            results_pdf["run_id"] = str(run_id)

        results_pdf["status"] = (
            results_pdf["status"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
        )

        calc_pass = int(results_pdf["status"].isin(["PASS", "PASSED"]).sum())
        calc_fail = int(results_pdf["status"].isin(["FAIL", "FAILED"]).sum())
        calc_error = int(results_pdf["status"].isin(["ERROR"]).sum())
        calc_no_data = int(results_pdf["status"].isin(["NO_DATA", "NO DATA", "NODATA"]).sum())
        calc_total = int(len(results_pdf))

        run_dict = run.to_dict()
        for k in ["run_start_datetime", "run_end_datetime"]:
            if k in run_dict:
                run_dict[k] = _safe_dt_str(run_dict[k])

        run_dict["run_status"] = str(run_dict.get("run_status") or "").strip().upper()
        run_dict["total_passed"] = calc_pass
        run_dict["total_failed"] = calc_fail
        run_dict["total_error"] = calc_error
        run_dict["total_no_data"] = calc_no_data
        run_dict["total_tests"] = calc_total

        runs_list.append(run_dict)
        all_results[str(run_id)] = json.loads(results_pdf.to_json(orient="records"))

    runs_b64 = _compress_json_for_html(runs_list)
    all_results_b64 = _compress_json_for_html(all_results)

    html_path = f"{output_dir}/test_dashboard_single-{timestamp}.html"

    html = """
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>ARIA Test Runs Dashboard</title>

<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.jsdelivr.net/npm/pako@2.1.0/dist/pako.min.js"></script>

<style>
:root {
  --brand: #1f4e79;
  --bg-soft: #f7f9fc;
  --border: #d9e2ec;
  --text: #1f2933;
  --muted: #52606d;
  --pass-bg: #e8f7ee;
  --fail-bg: #fdecec;
  --error-bg: #fff0f0;
  --nodata-bg: #f5f7fa;
  --pill-bg: #eef2f7;
}

* { box-sizing: border-box; }

body {
  margin: 0;
  font-family: Arial, sans-serif;
  background: #fff;
  color: var(--text);
}

.banner {
  background: var(--brand);
  color: white;
  padding: 18px 20px;
  font-size: 22px;
  font-weight: bold;
}

.tabs {
  display: flex;
  gap: 10px;
  padding: 10px 20px 0 20px;
  border-bottom: 1px solid var(--border);
  background: #fafbfd;
}

.tab {
  padding: 10px 12px;
  border: 1px solid var(--border);
  border-bottom: none;
  border-top-left-radius: 8px;
  border-top-right-radius: 8px;
  background: #f2f5f8;
  cursor: pointer;
  font-weight: bold;
  color: #334e68;
}

.tab.active {
  background: #ffffff;
  color: var(--brand);
}

.panel { display: none; padding: 20px; }
.panel.active { display: block; }

h2 {
  margin-top: 0;
  margin-bottom: 8px;
}

.subtle {
  color: var(--muted);
}

.summary-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin: 10px 0 16px 0;
}

.summary-pill {
  background: var(--pill-bg);
  border: 1px solid var(--border);
  border-radius: 999px;
  padding: 7px 12px;
  font-size: 13px;
  color: #243b53;
}

a.link {
  color: var(--brand);
  text-decoration: none;
  font-weight: bold;
}
a.link:hover { text-decoration: underline; }

.status-chip {
  display: inline-block;
  padding: 3px 8px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: bold;
}

.status-chip.pass-chip {
  background: var(--pass-bg);
  color: #147d3f;
}

.status-chip.fail-chip {
  background: var(--fail-bg);
  color: #c81e1e;
}

.status-chip.error-chip {
  background: var(--error-bg);
  color: #b42318;
}

.status-chip.no-data-chip {
  background: var(--nodata-bg);
  color: #475467;
}

.missing {
  color: #7b8794;
  font-style: italic;
}

.toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
  margin: 0 0 14px 0;
}

.checkbox-wrap {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-left: 8px;
}

button, select {
  padding: 8px 12px;
  border-radius: 8px;
  border: 1px solid #bcccdc;
  background: #fff;
}

button {
  cursor: pointer;
}
button:hover {
  background: #f8fafc;
}

label {
  font-weight: 600;
  color: #334e68;
}

.card {
  border: 1px solid var(--border);
  border-radius: 10px;
  background: #fff;
  padding: 14px;
  box-shadow: 0 1px 2px rgba(16,24,40,0.04);
}

.table-shell {
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 12px;
  background: #fff;
  overflow-x: auto;
}

.split {
  display: flex;
  flex-wrap: wrap;
  gap: 22px;
  margin: 12px 0 16px 0;
}

.left {
  flex: 1 1 420px;
  min-width: 320px;
}

.right {
  width: 320px;
  height: 320px;
}

.kv {
  display: flex;
  gap: 10px;
  padding: 6px 0;
  border-bottom: 1px dashed #e5e7eb;
}
.kv:last-child { border-bottom: none; }

.k {
  min-width: 160px;
  font-weight: bold;
  color: #334e68;
}
.v {
  flex: 1;
  overflow-wrap: anywhere;
  color: #111827;
}

.runs-meta {
  margin-bottom: 14px;
}

.runs-meta p {
  margin: 4px 0;
}

.dataTables_wrapper .dataTables_filter,
.dataTables_wrapper .dataTables_length {
  margin-bottom: 10px;
}

table.dataTable {
  width: 100% !important;
  table-layout: auto;
}

table.dataTable thead th {
  background: #f8fafc;
  color: #243b53;
  border-bottom: 1px solid var(--border) !important;
  vertical-align: top;
}

table.dataTable thead tr.filter-row th {
  background: #ffffff;
  padding: 4px 3px;
}

.filter-select {
  width: 100%;
  min-width: 0;
  font-size: 12px;
  padding: 4px 6px;
  border-radius: 6px;
  border: 1px solid #d0d5dd;
  background: #fff;
  height: 32px;
}

table.dataTable tbody td {
  vertical-align: top;
  border-bottom: 1px solid #eef2f7;
}

td.col-runid, th.col-runid {
  width: __RUN_ID_COL_WIDTH__px;
  min-width: __RUN_ID_COL_WIDTH__px;
}

td.col-user, th.col-user {
  width: __USER_COL_WIDTH__px;
  min-width: __USER_COL_WIDTH__px;
}

td.col-start, th.col-start,
td.col-end, th.col-end {
  width: __START_COL_WIDTH__px;
  min-width: __START_COL_WIDTH__px;
}

td.col-state, th.col-state {
  width: __STATE_COL_WIDTH__px;
  min-width: __STATE_COL_WIDTH__px;
}

td.col-automation, th.col-automation {
  width: __AUTOMATION_COL_WIDTH__px;
  min-width: __AUTOMATION_COL_WIDTH__px;
}

td.col-tag, th.col-tag {
  width: __TAG_COL_WIDTH__px;
  min-width: __TAG_COL_WIDTH__px;
}

td.col-pass, th.col-pass,
td.col-fail, th.col-fail,
td.col-total, th.col-total,
td.col-errorcount, th.col-errorcount,
td.col-nodatacount, th.col-nodatacount {
  width: 62px;
  min-width: 62px;
  text-align: center;
}

td.col-resultid, th.col-resultid {
  width: 70px;
  min-width: 70px;
}

td.col-runid2, th.col-runid2 {
  width: 80px;
  min-width: 80px;
}

td.col-testfield, th.col-testfield {
  width: __RESULT_TESTFIELD_COL_WIDTH__px;
  min-width: __RESULT_TESTFIELD_COL_WIDTH__px;
}

td.col-teststate, th.col-teststate {
  width: __RESULT_TESTSTATE_COL_WIDTH__px;
  min-width: __RESULT_TESTSTATE_COL_WIDTH__px;
}

td.col-message, th.col-message {
  width: __RESULT_MESSAGE_COL_WIDTH__px;
  min-width: __RESULT_MESSAGE_COL_WIDTH__px;
}

.wrap-cell {
  white-space: normal;
  overflow-wrap: break-word;
  word-break: normal;
  line-height: 1.35;
}

.wrap-cell-anywhere {
  white-space: normal;
  overflow-wrap: anywhere;
  word-break: break-word;
  line-height: 1.35;
}

.clamp-2 {
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.clamp-3 {
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.clamp-4 {
  display: -webkit-box;
  -webkit-line-clamp: 4;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.mono-small {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
}

.compare-shell {
  border: 1px solid var(--border);
  border-radius: 10px;
  overflow: auto;
  max-height: 70vh;
  background: #fff;
}

table.compare {
  border-collapse: collapse;
  width: 100%;
}

table.compare th,
table.compare td {
  border: 1px solid #d9e2ec;
  padding: 8px;
  text-align: left;
  vertical-align: top;
}

table.compare th {
  background: #f8fafc;
  position: sticky;
  top: 0;
  z-index: 2;
}

.match-row { background-color: #f8fffb; }
.diff-row { background-color: #fffdf7; }

td.col-error, th.col-error {
  min-width: 260px;
  max-width: 420px;
  overflow-wrap: anywhere;
}

.compare-status {
  min-width: 88px;
}

.compare-meta {
  color: var(--muted);
  margin-bottom: 10px;
}

.status-note {
  color: var(--muted);
  margin-bottom: 8px;
}

.resize-note {
  color: var(--muted);
  font-size: 12px;
  margin: 0 0 10px 0;
}
</style>
</head>

<body>

<div class="banner">ARIA Test Runs Dashboard</div>

<div class="tabs">
  <div class="tab active" data-panel="panel-runs">Runs</div>
  <div class="tab" data-panel="panel-run">Run Details</div>
  <div class="tab" data-panel="panel-compare">Compare</div>
</div>

<div id="panel-runs" class="panel active">
  <h2>Test Runs Overview</h2>
  <div class="runs-meta subtle">
    <p>Generated: __TIMESTAMP__</p>
    <p>Showing latest __MAX_RUNS__ runs</p>
  </div>
  <div class="status-note" id="loadInfo">Loading dashboard data...</div>
  <div class="summary-strip" id="runsSummaryStrip"></div>
  <div class="resize-note">Widths can be adjusted at the top of this Python file.</div>

  <div class="table-shell">
    <table id="runsTable" class="display" style="width:100%">
      <thead>
        <tr class="main-header-row">
          <th class="col-runid">Run ID</th>
          <th class="col-user">User</th>
          <th>Status</th>
          <th class="col-state">State Under Test</th>
          <th class="col-pass">Pass</th>
          <th class="col-fail">Fail</th>
          <th class="col-errorcount">Error</th>
          <th class="col-nodatacount">No Data</th>
          <th class="col-total">Total</th>
          <th class="col-start">Start</th>
          <th class="col-end">End</th>
          <th class="col-automation">Automation</th>
          <th class="col-tag">Tag</th>
        </tr>
        <tr class="filter-row">
          <th class="col-runid"></th>
          <th class="col-user"></th>
          <th></th>
          <th class="col-state"></th>
          <th class="col-pass"></th>
          <th class="col-fail"></th>
          <th class="col-errorcount"></th>
          <th class="col-nodatacount"></th>
          <th class="col-total"></th>
          <th class="col-start"></th>
          <th class="col-end"></th>
          <th class="col-automation"></th>
          <th class="col-tag"></th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<div id="panel-run" class="panel">
  <div class="toolbar">
    <button id="backToRunsBtn">Back</button>
    <button id="downloadRunExcelBtn">Download Excel</button>
    <span id="runTitle" style="font-weight:bold;"></span>
  </div>

  <div id="runEmpty" style="display:block; color:#555;">
    Click a <b>Run ID</b> on the Runs tab to view all test results.
  </div>

  <div id="runDetailsWrap" style="display:none;">
    <div class="split">
      <div class="left">
        <div class="card" id="runSummaryCard"></div>
      </div>
      <div class="right card">
        <canvas id="pieChart"></canvas>
      </div>
    </div>

    <div class="resize-note">Widths can be adjusted at the top of this Python file.</div>

    <div class="table-shell">
      <table id="resultsTable" class="display" style="width:100%">
        <thead>
          <tr class="main-header-row">
            <th class="col-resultid">Result ID</th>
            <th class="col-runid2">Run ID</th>
            <th>Test Name</th>
            <th>Status</th>
            <th class="col-testfield">Test Field</th>
            <th class="col-teststate">Test From State</th>
            <th class="col-message">Message</th>
          </tr>
          <tr class="filter-row">
            <th class="col-resultid"></th>
            <th class="col-runid2"></th>
            <th></th>
            <th></th>
            <th class="col-testfield"></th>
            <th class="col-teststate"></th>
            <th class="col-message"></th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
</div>

<div id="panel-compare" class="panel">
  <h2>Compare Runs</h2>

  <div class="toolbar">
    <label for="runA">Run A:</label>
    <select id="runA"><option value="">-- Select Run A --</option></select>

    <label for="runB">Run B:</label>
    <select id="runB"><option value="">-- Select Run B --</option></select>

    <button id="compareBtn">Compare</button>

    <label class="checkbox-wrap">
      <input type="checkbox" id="showOnlyDifferences">
      <span>Show only differences</span>
    </label>

    <label class="checkbox-wrap">
      <input type="checkbox" id="showOnlyFailsBoth">
      <span>Show only fails on both sides</span>
    </label>
  </div>

  <div class="compare-meta" id="compareMeta"></div>
  <div id="compareResults"></div>
</div>

<script>
(function() {
  var RUNS_B64 = "__RUNS_B64__";
  var ALL_RESULTS_B64 = "__ALL_RESULTS_B64__";

  function decodeCompressedJson(b64) {
    const binary = atob(b64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    const decompressed = pako.ungzip(bytes);
    const text = new TextDecoder("utf-8").decode(decompressed);
    return JSON.parse(text);
  }

  function escapeHtml(value) {
    if (value === null || value === undefined) return "";
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function normalizeStatus(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim().toUpperCase();
  }

  function normalizeCellValue(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }

  function escapeRegex(str) {
    return str.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&');
  }

  function statusBadge(d) {
    const status = normalizeStatus(d);
    if (status === 'PASS' || status === 'PASSED') return '<span class="status-chip pass-chip">PASS</span>';
    if (status === 'FAIL' || status === 'FAILED') return '<span class="status-chip fail-chip">FAIL</span>';
    if (status === 'ERROR') return '<span class="status-chip error-chip">ERROR</span>';
    if (status === 'NO_DATA' || status === 'NO DATA' || status === 'NODATA') return '<span class="status-chip no-data-chip">NO_DATA</span>';
    return '<span class="missing">' + escapeHtml(d ?? '') + '</span>';
  }

  function shortText(value, clampClass) {
    return '<div class="wrap-cell ' + clampClass + '" title="' + escapeHtml(value ?? '') + '">' + escapeHtml(value ?? '') + '</div>';
  }

  function fullWrapText(value) {
    return '<div class="wrap-cell-anywhere" title="' + escapeHtml(value ?? '') + '">' + escapeHtml(value ?? '') + '</div>';
  }

  function renderRunLink(d) {
    if (!d) return '';
    return '<a href="#" class="link run-link mono-small" data-run="' + escapeHtml(d) + '">' + escapeHtml(d) + '</a>';
  }

  function buildRunsSummaryStrip(runs) {
    const totalRuns = runs.length;
    const failedRuns = runs.filter(r => normalizeStatus(r.run_status) === 'FAIL' || normalizeStatus(r.run_status) === 'FAILED').length;
    const passedRuns = runs.filter(r => normalizeStatus(r.run_status) === 'PASS' || normalizeStatus(r.run_status) === 'PASSED').length;
    const errorRuns = runs.filter(r => normalizeStatus(r.run_status) === 'ERROR').length;
    const noDataRuns = runs.filter(r => ['NO_DATA', 'NO DATA', 'NODATA'].includes(normalizeStatus(r.run_status))).length;
    const totalTests = runs.reduce((acc, r) => acc + Number(r.total_tests || 0), 0);

    return ''
      + '<div class="summary-pill">Runs: ' + totalRuns + '</div>'
      + '<div class="summary-pill">Passed Runs: ' + passedRuns + '</div>'
      + '<div class="summary-pill">Failed Runs: ' + failedRuns + '</div>'
      + '<div class="summary-pill">Error Runs: ' + errorRuns + '</div>'
      + '<div class="summary-pill">No Data Runs: ' + noDataRuns + '</div>'
      + '<div class="summary-pill">Tests Across Loaded Runs: ' + totalTests + '</div>';
  }

  function computeTotals(results, runObj) {
    if (runObj) {
      const total_tests = Number(runObj.total_tests || 0);
      const total_passed = Number(runObj.total_passed || 0);
      const total_failed = Number(runObj.total_failed || 0);
      const total_error = Number(runObj.total_error || 0);
      const total_no_data = Number(runObj.total_no_data || 0);
      const pass_percent = total_tests
        ? Math.round((total_passed / total_tests) * 1000) / 10
        : 0;

      return {
        total_tests,
        total_passed,
        total_failed,
        total_error,
        total_no_data,
        pass_percent
      };
    }

    const normalized = results.map(r => normalizeStatus(r.status));
    const total_tests = results.length;
    const total_passed = normalized.filter(s => s === 'PASS' || s === 'PASSED').length;
    const total_failed = normalized.filter(s => s === 'FAIL' || s === 'FAILED').length;
    const total_error = normalized.filter(s => s === 'ERROR').length;
    const total_no_data = normalized.filter(s => s === 'NO_DATA' || s === 'NO DATA' || s === 'NODATA').length;

    const pass_percent = total_tests
      ? Math.round((total_passed / total_tests) * 1000) / 10
      : 0;

    return {
      total_tests,
      total_passed,
      total_failed,
      total_error,
      total_no_data,
      pass_percent
    };
  }

  function buildSummaryCard(runObj, totals, resultCount) {
    const fields = [
      ['Run ID', runObj.run_id],
      ['User', runObj.run_user],
      ['Start', runObj.run_start_datetime],
      ['End', runObj.run_end_datetime],
      ['Automation', runObj.run_by_automation_name],
      ['Tag', runObj.run_tag],
      ['Status', runObj.run_status],
      ['State Under Test', runObj.state_under_test],
      ['Total Passed', totals.total_passed],
      ['Total Failed', totals.total_failed],
      ['Total Error', totals.total_error],
      ['Total No Data', totals.total_no_data],
      ['Total Tests', totals.total_tests],
      ['Loaded Results Rows', resultCount],
      ['Pass %', totals.pass_percent + '%']
    ];

    let html = '';
    fields.forEach(([k, v]) => {
      html += '<div class="kv"><div class="k">' + escapeHtml(k) + '</div><div class="v">' + escapeHtml(v ?? '') + '</div></div>';
    });
    return html;
  }

  function renderPie(runObj, results) {
    const totals = computeTotals(results, runObj);
    const labels = ['PASS', 'FAIL', 'ERROR', 'NO_DATA'];
    const values = [
      totals.total_passed,
      totals.total_failed,
      totals.total_error,
      totals.total_no_data
    ];

    if (pieChart) {
      pieChart.destroy();
      pieChart = null;
    }

    pieChart = new Chart(document.getElementById('pieChart'), {
      type: 'pie',
      data: {
        labels: labels,
        datasets: [{
          data: values,
          backgroundColor: ['#7bd389', '#f29b9b', '#f97066', '#cbd5e1']
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom' }
        }
      }
    });
  }

  function buildCompareOptionLabel(run) {
    const runId = run.run_id || '';
    const start = run.run_start_datetime || '';
    const user = run.run_user || '';
    const state = run.state_under_test || '';
    return runId + ' | ' + start + ' | ' + user + ' | ' + state;
  }

  function populateCompareDropdowns() {
    $('#runA, #runB').empty().append('<option value="">-- Select --</option>');
    runsData.forEach(run => {
      const rid = String(run.run_id || '');
      const label = buildCompareOptionLabel(run);
      const option = '<option value="' + escapeHtml(rid) + '">' + escapeHtml(label) + '</option>';
      $('#runA, #runB').append(option);
    });
  }

  function initColumnFilters(table, tableSelector, filterDefs) {
    const api = table.api ? table.api() : table;
    const filterHeader = $(tableSelector + ' thead tr.filter-row th');

    filterDefs.forEach(def => {
      const th = filterHeader.eq(def.index);
      if (!th.length) return;
      if (def.skip) {
        th.html('');
        return;
      }

      const values = new Set();
      api.column(def.index).data().each(function(d) {
        const raw = def.valueFn ? def.valueFn(d) : normalizeCellValue(d);
        if (raw !== '') values.add(raw);
      });

      let sorted = Array.from(values);
      if (def.sortNumeric) {
        sorted.sort((a, b) => Number(a) - Number(b));
      } else {
        sorted.sort((a, b) => String(a).localeCompare(String(b)));
      }

      let html = '<select class="filter-select" data-col="' + def.index + '">';
      html += '<option value="">All</option>';
      sorted.forEach(v => {
        html += '<option value="' + escapeHtml(v) + '">' + escapeHtml(v) + '</option>';
      });
      html += '</select>';

      th.html(html);
    });

    $(tableSelector + ' thead').off('change.filterSelect');
    $(tableSelector + ' thead').on('change.filterSelect', '.filter-select', function() {
      const colIdx = Number($(this).data('col'));
      const val = $(this).val() || '';

      if (!val) {
        api.column(colIdx).search('', true, false).draw();
        return;
      }

      const regex = '^' + escapeRegex(val) + '$';
      api.column(colIdx).search(regex, true, false).draw();
    });
  }

  function showPanel(panelId) {
    $('.panel').removeClass('active');
    $('#' + panelId).addClass('active');

    $('.tab').removeClass('active');
    $('.tab[data-panel="' + panelId + '"]').addClass('active');
  }

  let runsData = [];
  let allResults = {};
  let runsTable = null;
  let resultsTable = null;
  let currentRunId = null;
  let pieChart = null;
  let lastCompareRows = [];

  $(document).on('click', '.tab', function() {
    showPanel($(this).data('panel'));
  });

  function showRun(runId) {
    currentRunId = String(runId);

    const runObj = runsData.find(r => String(r.run_id) === currentRunId);
    const results = allResults[currentRunId] || [];
    const totals = computeTotals(results, runObj);

    $('#runTitle').text('Run: ' + currentRunId);

    if (runObj) {
      $('#runSummaryCard').html(buildSummaryCard(runObj, totals, results.length));
    } else {
      $('#runSummaryCard').html('<div class="missing">Run details not found in runs list.</div>');
    }

    renderPie(runObj, results);

    if (resultsTable) {
      resultsTable.clear();
      resultsTable.rows.add(results);
      resultsTable.draw();

      $('#resultsTable thead tr.filter-row th').empty();
      initColumnFilters(resultsTable, '#resultsTable', [
        { index: 0, valueFn: d => normalizeCellValue(d) },
        { index: 1, valueFn: d => normalizeCellValue(d) },
        { index: 2, valueFn: d => normalizeCellValue(d) },
        { index: 3, valueFn: d => normalizeStatus(d) },
        { index: 4, valueFn: d => normalizeCellValue(d) },
        { index: 5, valueFn: d => normalizeCellValue(d) },
        { index: 6, valueFn: d => normalizeCellValue(d) }
      ]);
    } else {
      resultsTable = $('#resultsTable').DataTable({
        data: results,
        autoWidth: false,
        columns: [
          { data:'result_id', className:'col-resultid', render: d => shortText(d ?? '', 'clamp-3 mono-small') },
          { data:'run_id', className:'col-runid2', render: d => shortText(d ?? '', 'clamp-3 mono-small') },
          { data:'test_name', render: d => shortText(d ?? '', 'clamp-3') },
          { data:'status', render: d => statusBadge(d) },
          { data:'test_field', className:'col-testfield', render: d => shortText(d ?? '', 'clamp-4') },
          { data:'test_from_state', className:'col-teststate', render: d => shortText(d ?? '', 'clamp-3') },
          { data:'message', className:'col-message', render: d => shortText(d ?? '', 'clamp-4') }
        ],
        pageLength: __RESULTS_PAGE_LENGTH__
      });

      initColumnFilters(resultsTable, '#resultsTable', [
        { index: 0, valueFn: d => normalizeCellValue(d) },
        { index: 1, valueFn: d => normalizeCellValue(d) },
        { index: 2, valueFn: d => normalizeCellValue(d) },
        { index: 3, valueFn: d => normalizeStatus(d) },
        { index: 4, valueFn: d => normalizeCellValue(d) },
        { index: 5, valueFn: d => normalizeCellValue(d) },
        { index: 6, valueFn: d => normalizeCellValue(d) }
      ]);
    }

    $('#runEmpty').hide();
    $('#runDetailsWrap').show();
    showPanel('panel-run');
  }

  function renderCompareTable() {
    const showOnlyDifferences = $('#showOnlyDifferences').is(':checked');
    const showOnlyFailsBoth = $('#showOnlyFailsBoth').is(':checked');

    let filteredRows = lastCompareRows.filter(row => {
      if (showOnlyDifferences && row.statusA === row.statusB) return false;
      if (showOnlyFailsBoth && !(row.statusA === 'FAIL' && row.statusB === 'FAIL')) return false;
      return true;
    });

    $('#compareMeta').text('Rows shown: ' + filteredRows.length);

    let html = '<div class="compare-shell"><table class="compare">'
      + '<tr>'
      + '<th>#</th>'
      + '<th>Test Name</th>'
      + '<th>Field</th>'
      + '<th class="compare-status">Run A</th>'
      + '<th class="col-error">Error Message A</th>'
      + '<th class="compare-status">Run B</th>'
      + '<th class="col-error">Error Message B</th>'
      + '</tr>';

    let rowNum = 1;

    filteredRows.forEach(row => {
      html += '<tr class="' + row.rowClass + '">'
        + '<td>' + rowNum + '</td>'
        + '<td><div class="wrap-cell">' + escapeHtml(row.testName) + '</div></td>'
        + '<td><div class="wrap-cell">' + escapeHtml(row.testField) + '</div></td>'
        + '<td>' + escapeHtml(row.statusA) + '</td>'
        + '<td class="col-error"><div class="wrap-cell">' + escapeHtml(row.messageA) + '</div></td>'
        + '<td>' + escapeHtml(row.statusB) + '</td>'
        + '<td class="col-error"><div class="wrap-cell">' + escapeHtml(row.messageB) + '</div></td>'
        + '</tr>';
      rowNum++;
    });

    html += '</table></div>';
    $('#compareResults').html(html);
  }

  $(document).ready(function() {
    try {
      runsData = decodeCompressedJson(RUNS_B64);
      allResults = decodeCompressedJson(ALL_RESULTS_B64);

      $('#loadInfo').text('Loaded ' + runsData.length + ' runs');
      $('#runsSummaryStrip').html(buildRunsSummaryStrip(runsData));

      if ($.fn.DataTable.isDataTable('#runsTable')) {
        $('#runsTable').DataTable().destroy();
      }

      runsTable = $('#runsTable').DataTable({
        data: runsData,
        autoWidth: false,
        order: [[9, 'desc']],
        columns: [
          {
            data:'run_id',
            className:'col-runid',
            render: d => renderRunLink(d)
          },
          {
            data:'run_user',
            className:'col-user',
            render: d => shortText(d ?? '', 'clamp-3')
          },
          {
            data:'run_status',
            render: d => statusBadge(d)
          },
          {
            data:'state_under_test',
            className:'col-state',
            render: d => fullWrapText(d ?? '')
          },
          { data:'total_passed', className:'col-pass', render: d => escapeHtml(d ?? '') },
          { data:'total_failed', className:'col-fail', render: d => escapeHtml(d ?? '') },
          { data:'total_error', className:'col-errorcount', render: d => escapeHtml(d ?? '') },
          { data:'total_no_data', className:'col-nodatacount', render: d => escapeHtml(d ?? '') },
          { data:'total_tests', className:'col-total', render: d => escapeHtml(d ?? '') },
          {
            data:'run_start_datetime',
            className:'col-start',
            render: d => shortText(d ?? '', 'clamp-2')
          },
          {
            data:'run_end_datetime',
            className:'col-end',
            render: d => shortText(d ?? '', 'clamp-2')
          },
          {
            data:'run_by_automation_name',
            className:'col-automation',
            render: d => shortText(d ?? '', 'clamp-3')
          },
          {
            data:'run_tag',
            className:'col-tag',
            render: d => shortText(d ?? '', 'clamp-4')
          }
        ],
        pageLength: __RUNS_PAGE_LENGTH__
      });

      initColumnFilters(runsTable, '#runsTable', [
        { index: 0, valueFn: d => normalizeCellValue(d) },
        { index: 1, valueFn: d => normalizeCellValue(d) },
        { index: 2, valueFn: d => normalizeStatus(d) },
        { index: 3, valueFn: d => normalizeCellValue(d) },
        { index: 4, skip: true },
        { index: 5, skip: true },
        { index: 6, skip: true },
        { index: 7, skip: true },
        { index: 8, skip: true },
        { index: 9, valueFn: d => normalizeCellValue(d) },
        { index: 10, valueFn: d => normalizeCellValue(d) },
        { index: 11, valueFn: d => normalizeCellValue(d) },
        { index: 12, valueFn: d => normalizeCellValue(d) }
      ]);

      populateCompareDropdowns();
    } catch (err) {
      $('#loadInfo').text('Error loading dashboard data: ' + String(err));
      console.error(err);
    }
  });

  $(document).on('click', 'a.run-link', function(e) {
    e.preventDefault();
    const runId = $(this).data('run');
    showRun(runId);
  });

  $('#backToRunsBtn').on('click', function() {
    showPanel('panel-runs');
  });

  $('#downloadRunExcelBtn').on('click', function() {
    if (!currentRunId) {
      alert('Select a run first.');
      return;
    }

    const rows = allResults[currentRunId] || [];
    if (!rows.length) {
      alert('No results to export for this run.');
      return;
    }

    const header = ["result_id", "run_id", "test_name", "status", "test_field", "test_from_state", "message"];
    const exportRows = rows.map(r => ({
      result_id: r.result_id ?? "",
      run_id: r.run_id ?? "",
      test_name: r.test_name ?? "",
      status: r.status ?? "",
      test_field: r.test_field ?? "",
      test_from_state: r.test_from_state ?? "",
      message: r.message ?? ""
    }));

    const ws = XLSX.utils.json_to_sheet(exportRows, { header: header });
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Results");
    XLSX.writeFile(wb, "run_" + currentRunId + "_results.xlsx");
  });

  $('#compareBtn').on('click', function() {
    const runA = $('#runA').val();
    const runB = $('#runB').val();

    if (!runA || !runB || runA === runB) {
      alert('Select two different runs');
      return;
    }

    const resultsA = allResults[runA] || [];
    const resultsB = allResults[runB] || [];

    const mapA = {};
    const mapB = {};

    resultsA.forEach(r => {
      const key = (r.test_name ?? '') + '|' + (r.test_field ?? '');
      if (!mapA[key]) mapA[key] = [];
      mapA[key].push(r);
    });

    resultsB.forEach(r => {
      const key = (r.test_name ?? '') + '|' + (r.test_field ?? '');
      if (!mapB[key]) mapB[key] = [];
      mapB[key].push(r);
    });

    const allKeys = new Set([...Object.keys(mapA), ...Object.keys(mapB)]);
    const sortedKeys = Array.from(allKeys).sort((a, b) => a.localeCompare(b));

    lastCompareRows = [];

    sortedKeys.forEach(key => {
      const rowsA = mapA[key] || [{ status: 'MISSING', message: '' }];
      const rowsB = mapB[key] || [{ status: 'MISSING', message: '' }];
      const maxLen = Math.max(rowsA.length, rowsB.length);

      const parts = key.split('|');
      const testName = parts[0] || '';
      const testField = parts[1] || '';

      for (let i = 0; i < maxLen; i++) {
        const rowA = rowsA[i] || { status: 'MISSING', message: '' };
        const rowB = rowsB[i] || { status: 'MISSING', message: '' };

        const statusA = normalizeStatus(rowA.status ?? 'MISSING');
        const statusB = normalizeStatus(rowB.status ?? 'MISSING');
        const messageA = rowA.message ?? '';
        const messageB = rowB.message ?? '';

        lastCompareRows.push({
          testName: testName,
          testField: testField,
          statusA: statusA,
          statusB: statusB,
          messageA: messageA,
          messageB: messageB,
          rowClass: (statusA === statusB) ? 'match-row' : 'diff-row'
        });
      }
    });

    renderCompareTable();
    showPanel('panel-compare');
  });

  $('#showOnlyDifferences').on('change', function() {
    renderCompareTable();
  });

  $('#showOnlyFailsBoth').on('change', function() {
    renderCompareTable();
  });
})();
</script>

</body>
</html>
"""

    html = html.replace("__RUNS_B64__", runs_b64)
    html = html.replace("__ALL_RESULTS_B64__", all_results_b64)
    html = html.replace("__TIMESTAMP__", timestamp)
    html = html.replace("__MAX_RUNS__", str(max_runs))
    html = html.replace("__STATE_COL_WIDTH__", str(STATE_COL_WIDTH))
    html = html.replace("__USER_COL_WIDTH__", str(USER_COL_WIDTH))
    html = html.replace("__AUTOMATION_COL_WIDTH__", str(AUTOMATION_COL_WIDTH))
    html = html.replace("__TAG_COL_WIDTH__", str(TAG_COL_WIDTH))
    html = html.replace("__RUN_ID_COL_WIDTH__", str(RUN_ID_COL_WIDTH))
    html = html.replace("__START_COL_WIDTH__", str(START_COL_WIDTH))
    html = html.replace("__END_COL_WIDTH__", str(END_COL_WIDTH))
    html = html.replace("__RESULT_TESTFIELD_COL_WIDTH__", str(RESULT_TESTFIELD_COL_WIDTH))
    html = html.replace("__RESULT_TESTSTATE_COL_WIDTH__", str(RESULT_TESTSTATE_COL_WIDTH))
    html = html.replace("__RESULT_MESSAGE_COL_WIDTH__", str(RESULT_MESSAGE_COL_WIDTH))
    html = html.replace("__RUNS_PAGE_LENGTH__", str(RUNS_PAGE_LENGTH))
    html = html.replace("__RESULTS_PAGE_LENGTH__", str(RESULTS_PAGE_LENGTH))

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Dashboard written: {html_path}")
    print(f"Showing latest {len(runs_list)} runs")
    print(f"Compressed runs payload chars: {len(runs_b64)}")
    print(f"Compressed all_results payload chars: {len(all_results_b64)}")

    return html_path, html