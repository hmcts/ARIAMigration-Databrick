# dashboard.py
# Single-page HTML dashboard (Runs + Run Details + Compare) for Databricks notebooks.
# Brace-safe (no f-string curly brace escaping needed)
#
# Notebook usage:
#   import importlib, src.dashboard as dashboard
#   importlib.reload(dashboard)
#   html_path, html_str = dashboard.generate_dashboard_single_page()
#   displayHTML(html_str)

from functions.db_utils import get_runs, get_results
from datetime import datetime
import os
import json

OUTPUT_DIR = "/dbfs/tmp"


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


def generate_dashboard_single_page(output_dir: str = OUTPUT_DIR):
    """
    Generates ONE HTML page that includes:
      - Runs table (sortable/searchable)
      - Run details view (summary + pie + all test results)
      - Compare view (pick two runs, row-numbered diff table)
    Column order (Runs):
      Run ID | Status | Pass | Fail | Total | User | Start | End | State Under Test | Automation | Tag
    Width tweaks:
      - Run ID 20% thinner (144px)
      - State Under Test +25% wider (193px)
      - Automation ~25% narrower (120px)
      - Pass/Fail/Total thin
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")

    runs_pdf = get_runs().toPandas()

    runs_list = []
    all_results = {}  # run_id -> list[dict] (all test results for that run)

    for _, run in runs_pdf.iterrows():
        run_id = run.get("run_id")
        if not run_id:
            continue

        results_pdf = get_results(run_id).toPandas()

        total_tests = len(results_pdf)
        total_passed = int((results_pdf["status"] == "PASS").sum()) if "status" in results_pdf.columns else 0
        total_failed = int((results_pdf["status"] == "FAIL").sum()) if "status" in results_pdf.columns else 0

        run_dict = run.to_dict()
        for k in ["run_start_datetime", "run_end_datetime"]:
            if k in run_dict:
                run_dict[k] = _safe_dt_str(run_dict[k])

        run_dict.update(
            {
                "total_passed": total_passed,
                "total_failed": total_failed,
                "total_tests": total_tests,
            }
        )
        runs_list.append(run_dict)

        # Store ALL test results for the run in the single-page HTML (can be large!)
        all_results[str(run_id)] = json.loads(results_pdf.to_json(orient="records", date_format="iso"))

    runs_json = json.dumps(runs_list)
    all_results_json = json.dumps(all_results)

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
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>

<style>
  body { margin:0; font-family: Arial; background:#fff; }

  .banner {
    background:#1f4e79;
    color:white;
    padding:18px 20px;
    font-size:22px;
    font-weight:bold;
  }

  .tabs {
    display:flex;
    gap:10px;
    padding:10px 20px 0 20px;
    border-bottom:1px solid #e5e5e5;
    background:#fafafa;
  }

  .tab {
    padding:10px 12px;
    border:1px solid #e5e5e5;
    border-bottom:none;
    border-top-left-radius:6px;
    border-top-right-radius:6px;
    background:#f3f3f3;
    cursor:pointer;
    font-weight:bold;
    color:#333;
  }

  .tab.active { background:#ffffff; }
  .panel { display:none; padding:20px; }
  .panel.active { display:block; }

  a.link { color:#1f4e79; text-decoration:none; font-weight:bold; }
  a.link:hover { text-decoration:underline; }

  .pass { color:green; font-weight:bold; }
  .fail { color:red; font-weight:bold; }
  .missing { color:#666; font-style:italic; }

  .toolbar {
    display:flex;
    flex-wrap:wrap;
    gap:10px;
    align-items:center;
    margin: 0 0 10px 0;
  }

  button {
    padding:8px 12px;
    cursor:pointer;
    border-radius:6px;
    border:1px solid #ccc;
    background:#fff;
  }
  button:hover { background:#f5f5f5; }

  /* Runs table width constraints */
  td.col-runid, th.col-runid { width:144px; max-width:144px; overflow-wrap:anywhere; }
  td.col-user,  th.col-user  { width:144px; max-width:144px; overflow-wrap:anywhere; }
  td.col-state, th.col-state { width:193px; max-width:193px; overflow-wrap:anywhere; } /* +25% */
  td.col-auto,  th.col-auto  { width:120px; max-width:120px; overflow-wrap:anywhere; } /* -25%-ish */

  td.col-pass, th.col-pass,
  td.col-fail, th.col-fail,
  td.col-total, th.col-total {
    width:55px;
    max-width:55px;
    text-align:center;
  }

  /* Run details layout */
  .split {
    display:flex;
    flex-wrap:wrap;
    gap:22px;
    margin: 14px 0 18px 0;
  }

  .left {
    flex: 1 1 280px;
    max-width: 460px;
  }

  .right {
    width: 280px;
    height: 280px;
  }

  .card {
    border:1px solid #ddd;
    border-radius:8px;
    background:#f9f9f9;
    padding:12px;
  }

  .kv {
    display:flex;
    gap:10px;
    padding:4px 0;
    border-bottom:1px dashed #e2e2e2;
  }
  .kv:last-child { border-bottom:none; }
  .k { min-width:150px; font-weight:bold; color:#333; }
  .v { flex:1; overflow-wrap:anywhere; color:#111; }

  /* Compare table styling */
  table.compare {
    border-collapse: collapse;
    width: 100%;
    margin-top: 14px;
  }
  table.compare th, table.compare td {
    border: 1px solid #ccc;
    padding: 6px;
    text-align:left;
  }
  table.compare th { background:#f2f2f2; }
  .match-row { background-color:#d4edda; }
  .diff-row { background-color:#fff3cd; }
</style>
</head>

<body>
  <div class="banner">ARIA Test Runs Dashboard</div>

  <div class="tabs">
    <div class="tab active" data-panel="panel-runs">Runs</div>
    <div class="tab" data-panel="panel-run">Run Details</div>
    <div class="tab" data-panel="panel-compare">Compare</div>
  </div>

  <!-- RUNS -->
  <div id="panel-runs" class="panel active">
    <h2 style="margin-top:0;">Test Runs Overview</h2>
    <p>Generated: __TIMESTAMP__</p>

    <table id="runsTable" class="display" style="width:100%;">
      <thead>
        <tr>
          <th class="col-runid">Run ID</th>
          <th>Status</th>
          <th class="col-pass">Pass</th>
          <th class="col-fail">Fail</th>
          <th class="col-total">Total</th>
          <th class="col-user">User</th>
          <th>Start</th>
          <th>End</th>
          <th class="col-state">State Under Test</th>
          <th class="col-auto">Automation</th>
          <th>Tag</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>

    <p style="margin-top:10px; color:#555;">
      Tip: click a Run ID to open the <b>Run Details</b> tab.
    </p>
  </div>

  <!-- RUN DETAILS -->
  <div id="panel-run" class="panel">
    <div class="toolbar">
      <button id="backToRunsBtn">⬅ Back to Runs</button>
      <button id="downloadRunExcelBtn">📥 Download Excel (this run)</button>
      <span id="runTitle" style="font-weight:bold;"></span>
    </div>

    <div id="runDetailsWrap" style="display:none;">
      <div class="split">
        <div class="left">
          <div class="card" id="runSummaryCard"></div>
        </div>
        <div class="right">
          <canvas id="pieChart"></canvas>
        </div>
      </div>

      <table id="resultsTable" class="display" style="width:100%;">
        <thead>
          <tr>
            <th>Result ID</th>
            <th>Run ID</th>
            <th>Test Name</th>
            <th>Test Field</th>
            <th>Test From State</th>
            <th>Status</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>

    <div id="runEmpty" style="display:block; color:#555;">
      Select a run from the <b>Runs</b> tab to view details.
    </div>
  </div>

  <!-- COMPARE -->
  <div id="panel-compare" class="panel">
    <h2 style="margin-top:0;">Compare Runs</h2>

    <div class="toolbar">
      <label>Run A:</label>
      <select id="runA"><option value="">--Select--</option></select>

      <label>Run B:</label>
      <select id="runB"><option value="">--Select--</option></select>

      <button id="compareBtn">Compare</button>
    </div>

    <div id="compareResults"></div>
  </div>

<script>
  const runsData = __RUNS_JSON__;
  const allResults = __ALL_RESULTS_JSON__;

  // Tabs
  function showPanel(panelId) {
    $('.panel').removeClass('active');
    $('#' + panelId).addClass('active');

    $('.tab').removeClass('active');
    $('.tab[data-panel="' + panelId + '"]').addClass('active');
  }

  $(document).on('click', '.tab', function() {
    showPanel($(this).data('panel'));
  });

  // Globals
  let runsTable = null;
  let resultsTable = null;
  let pieChart = null;
  let currentRunId = null;

  function statusBadge(d) {
    if (d === 'PASS') return '<span class="pass">PASS</span>';
    if (d === 'FAIL') return '<span class="fail">FAIL</span>';
    return '<span class="missing">' + (d ?? '') + '</span>';
  }

  function computeTotals(results) {
    const total_tests = results.length;
    let total_passed = 0;
    let total_failed = 0;

    results.forEach(r => {
      if (r.status === 'PASS') total_passed++;
      if (r.status === 'FAIL') total_failed++;
    });

    const pass_percent = total_tests ? Math.round((total_passed / total_tests) * 1000) / 10 : 0;
    return { total_tests, total_passed, total_failed, pass_percent };
  }

  function buildSummaryCard(runObj, totals) {
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
      ['Total Tests', totals.total_tests],
      ['Pass %', totals.pass_percent + '%']
    ];

    let html = '';
    fields.forEach(([k,v]) => {
      html += `<div class="kv"><div class="k">${k}</div><div class="v">${(v ?? '')}</div></div>`;
    });
    return html;
  }

  function buildPie(results) {
    const counts = {};
    results.forEach(r => {
      const s = r.status ?? 'UNKNOWN';
      counts[s] = (counts[s] || 0) + 1;
    });

    const labels = Object.keys(counts);
    const values = labels.map(l => counts[l]);

    if (pieChart) {
      pieChart.destroy();
      pieChart = null;
    }

    pieChart = new Chart(document.getElementById('pieChart'), {
      type: 'pie',
      data: { labels: labels, datasets: [{ data: values }] },
      options: {
        responsive: false,
        maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom' } }
      }
    });
  }

  function showRun(runId) {
    currentRunId = String(runId);
    const runObj = runsData.find(r => String(r.run_id) === String(runId));
    const results = allResults[String(runId)] || [];

    if (!runObj) {
      $('#runTitle').text('Run not found: ' + runId);
      $('#runDetailsWrap').hide();
      $('#runEmpty').show();
      showPanel('panel-run');
      return;
    }

    const totals = computeTotals(results);
    $('#runTitle').text('Run: ' + runId);

    $('#runSummaryCard').html(buildSummaryCard(runObj, totals));
    buildPie(results);

    if (resultsTable) {
      resultsTable.clear();
      resultsTable.rows.add(results);
      resultsTable.draw();
    } else {
      resultsTable = $('#resultsTable').DataTable({
        data: results,
        columns: [
          { data: 'result_id' },
          { data: 'run_id' },
          { data: 'test_name' },
          { data: 'test_field' },
          { data: 'test_from_state' },
          { data: 'status', render: function(d){ return statusBadge(d); } },
          { data: 'message' }
        ],
        pageLength: 15
      });
    }

    $('#runEmpty').hide();
    $('#runDetailsWrap').show();
    showPanel('panel-run');
  }

  // Init
  $(document).ready(function() {
    // Runs table
    runsTable = $('#runsTable').DataTable({
      data: runsData,
      columns: [
        {
          data: 'run_id',
          className: 'col-runid',
          render: function(d) {
            if (!d) return '';
            return '<a href="#" class="link run-link" data-runid="' + d + '">' + d + '</a>';
          }
        },
        { data: 'run_status', render: function(d){ return statusBadge(d); } },
        { data: 'total_passed', className: 'col-pass' },
        { data: 'total_failed', className: 'col-fail' },
        { data: 'total_tests', className: 'col-total' },
        { data: 'run_user', className: 'col-user' },
        { data: 'run_start_datetime' },
        { data: 'run_end_datetime' },
        { data: 'state_under_test', className: 'col-state' },
        { data: 'run_by_automation_name', className: 'col-auto' },
        { data: 'run_tag' }
      ],
      order: [[6,'desc']],   // Start desc
      pageLength: 15,
      autoWidth: false
    });

    // Compare dropdowns
    Object.keys(allResults).forEach(rid => {
      $('#runA, #runB').append(`<option value="${rid}">${rid}</option>`);
    });
  });

  // Run link click
  $(document).on('click', 'a.run-link', function(e) {
    e.preventDefault();
    showRun($(this).data('runid'));
  });

  // Back button
  $('#backToRunsBtn').on('click', function() {
    showPanel('panel-runs');
  });

  // Excel download (current run)
  $('#downloadRunExcelBtn').on('click', function() {
    if (!currentRunId) {
      alert('Select a run first.');
      return;
    }
    const tbl = document.getElementById("resultsTable");
    const wb = XLSX.utils.table_to_book(tbl, { sheet: "Results" });
    XLSX.writeFile(wb, "run_" + currentRunId + "_results.xlsx");
  });

  // Compare
  $('#compareBtn').on('click', function() {
    const runA = $('#runA').val();
    const runB = $('#runB').val();
    if (!runA || !runB || runA === runB) {
      alert("Select two different runs");
      return;
    }

    const resultsA = allResults[runA] || [];
    const resultsB = allResults[runB] || [];

    const mapA = {};
    const mapB = {};

    // group duplicates by test_name|test_field
    resultsA.forEach(r => {
      const key = (r.test_name ?? '') + "|" + (r.test_field ?? '');
      if (!mapA[key]) mapA[key] = [];
      mapA[key].push(r);
    });
    resultsB.forEach(r => {
      const key = (r.test_name ?? '') + "|" + (r.test_field ?? '');
      if (!mapB[key]) mapB[key] = [];
      mapB[key].push(r);
    });

    const allKeys = new Set([...Object.keys(mapA), ...Object.keys(mapB)]);
    const sortedKeys = Array.from(allKeys).sort((a,b) => a.localeCompare(b));

    let html = '<table class="compare"><tr><th>#</th><th>Test Name</th><th>Field</th><th>Run A Status</th><th>Run B Status</th></tr>';
    let rowNum = 1;

    sortedKeys.forEach(key => {
      const rowsA = mapA[key] || [{ status: "MISSING" }];
      const rowsB = mapB[key] || [{ status: "MISSING" }];
      const maxLen = Math.max(rowsA.length, rowsB.length);

      const parts = key.split("|");
      const testName = parts[0] || "";
      const testField = parts[1] || "";

      for (let i = 0; i < maxLen; i++) {
        const statusA = rowsA[i] ? (rowsA[i].status ?? "MISSING") : "MISSING";
        const statusB = rowsB[i] ? (rowsB[i].status ?? "MISSING") : "MISSING";

        const rowClass = (statusA === statusB) ? "match-row" : "diff-row";

        const classA = (statusA === "PASS") ? "pass" : ((statusA === "FAIL") ? "fail" : "missing");
        const classB = (statusB === "PASS") ? "pass" : ((statusB === "FAIL") ? "fail" : "missing");

        html += `<tr class="${rowClass}">
          <td>${rowNum}</td>
          <td>${testName}</td>
          <td>${testField}</td>
          <td class="${classA}">${statusA}</td>
          <td class="${classB}">${statusB}</td>
        </tr>`;
        rowNum++;
      }
    });

    html += "</table>";
    $('#compareResults').html(html);

    // auto-switch to compare tab so user sees output
    showPanel('panel-compare');
  });
</script>

</body>
</html>
"""

    html = html.replace("__RUNS_JSON__", runs_json)
    html = html.replace("__ALL_RESULTS_JSON__", all_results_json)
    html = html.replace("__TIMESTAMP__", timestamp)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ Single-page dashboard written: {html_path}")
    return html_path, html