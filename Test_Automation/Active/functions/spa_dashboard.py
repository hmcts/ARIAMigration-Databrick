# dashboard.py
# Single-page HTML dashboard (Runs + Run Details + Compare) for Databricks notebooks.
# Notebook-safe version using compressed embedded JSON to reduce HTML size.

from functions.db_utils import get_runs, get_results
from datetime import datetime
import os
import json
import gzip
import base64

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


def _compress_json_for_html(obj):
    json_text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    compressed = gzip.compress(json_text.encode("utf-8"))
    return base64.b64encode(compressed).decode("ascii")


def generate_dashboard_single_page(output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")

    runs_pdf = get_runs().toPandas()

    runs_list = []
    all_results = {}

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

.checkbox-wrap {
display:flex;
align-items:center;
gap:6px;
margin-left:8px;
}

button {
padding:8px 12px;
cursor:pointer;
border-radius:6px;
border:1px solid #ccc;
background:#fff;
}
button:hover { background:#f5f5f5; }

td.col-user, th.col-user {
width:120px; max-width:120px; overflow-wrap:anywhere;
}

td.col-state, th.col-state {
width:156px; max-width:156px; overflow-wrap:anywhere;
}

td.col-pass, th.col-pass,
td.col-fail, th.col-fail,
td.col-total, th.col-total {
width:45px; max-width:45px; text-align:center;
}

td.col-resultid, th.col-resultid { width:70px; max-width:70px; overflow-wrap:anywhere; }
td.col-runid2,  th.col-runid2  { width:70px; max-width:70px; overflow-wrap:anywhere; }

td.col-testfield, th.col-testfield {
width:150px; max-width:150px; overflow-wrap:anywhere;
}

td.col-teststate, th.col-teststate {
width:150px; max-width:150px; overflow-wrap:anywhere;
}

.split {
display:flex;
flex-wrap:wrap;
gap:22px;
margin: 12px 0 16px 0;
}

.left {
flex: 1 1 280px;
max-width: 520px;
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

table.compare {
border-collapse: collapse;
width: 100%;
margin-top: 14px;
}
table.compare th, table.compare td {
border: 1px solid #ccc;
padding: 6px;
text-align:left;
vertical-align: top;
}
table.compare th { background:#f2f2f2; }
.match-row { background-color:#d4edda; }
.diff-row { background-color:#fff3cd; }

td.col-error, th.col-error {
min-width: 220px;
max-width: 320px;
overflow-wrap:anywhere;
}

.status-note {
color:#666;
margin-bottom:8px;
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
  <h2 style="margin-top:0;">Test Runs Overview</h2>
  <p>Generated: __TIMESTAMP__</p>
  <div class="status-note" id="loadInfo">Loading dashboard data...</div>

  <table id="runsTable" class="display" style="width:100%">
    <thead>
      <tr>
        <th>Run ID</th>
        <th class="col-user">User</th>
        <th>Status</th>
        <th class="col-pass">Pass</th>
        <th class="col-fail">Fail</th>
        <th class="col-total">Total</th>
        <th>Start</th>
        <th>End</th>
        <th class="col-state">State Under Test</th>
        <th>Automation</th>
        <th>Tag</th>
      </tr>
    </thead>
    <tbody></tbody>
  </table>
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
      <div class="right">
        <canvas id="pieChart"></canvas>
      </div>
    </div>

    <table id="resultsTable" class="display" style="width:100%">
      <thead>
        <tr>
          <th class="col-resultid">Result ID</th>
          <th class="col-runid2">Run ID</th>
          <th>Test Name</th>
          <th>Status</th>
          <th class="col-testfield">Test Field</th>
          <th class="col-teststate">Test From State</th>
          <th>Message</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<div id="panel-compare" class="panel">
  <h2 style="margin-top:0;">Compare Runs</h2>

  <div class="toolbar">
    <label>Run A:</label>
    <select id="runA"><option value="">--Select--</option></select>

    <label>Run B:</label>
    <select id="runB"><option value="">--Select--</option></select>

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

  <div id="compareResults"></div>
</div>

<script>
const RUNS_B64 = "__RUNS_B64__";
const ALL_RESULTS_B64 = "__ALL_RESULTS_B64__";

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

let runsData = [];
let allResults = {};
let runsTable = null;
let resultsTable = null;
let currentRunId = null;
let pieChart = null;
let lastCompareRows = [];

function showPanel(panelId) {
  $('.panel').removeClass('active');
  $('#' + panelId).addClass('active');

  $('.tab').removeClass('active');
  $('.tab[data-panel="' + panelId + '"]').addClass('active');
}

$(document).on('click', '.tab', function() {
  showPanel($(this).data('panel'));
});

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

function renderPie(results) {
  const totals = computeTotals(results);
  const labels = ['PASS','FAIL'];
  const values = [totals.total_passed, totals.total_failed];

  if (pieChart) {
    pieChart.destroy();
    pieChart = null;
  }

  pieChart = new Chart(document.getElementById('pieChart'), {
    type:'pie',
    data:{ labels: labels, datasets:[{ data: values }] },
    options:{
      responsive:false,
      maintainAspectRatio:false,
      plugins:{ legend:{ position:'bottom' } }
    }
  });
}

function showRun(runId) {
  currentRunId = String(runId);

  const runObj = runsData.find(r => String(r.run_id) === currentRunId);
  const results = allResults[currentRunId] || [];
  const totals = computeTotals(results);

  $('#runTitle').text('Run: ' + currentRunId);

  if (runObj) {
    $('#runSummaryCard').html(buildSummaryCard(runObj, totals));
  } else {
    $('#runSummaryCard').html('<div class="missing">Run details not found in runs list.</div>');
  }

  renderPie(results);

  if (resultsTable) {
    resultsTable.clear();
    resultsTable.rows.add(results);
    resultsTable.draw();
  } else {
    resultsTable = $('#resultsTable').DataTable({
      data: results,
      autoWidth: false,
      columns: [
        { data:'result_id', className:'col-resultid' },
        { data:'run_id', className:'col-runid2' },
        { data:'test_name' },
        { data:'status', render: function(d){ return statusBadge(d); } },
        { data:'test_field', className:'col-testfield' },
        { data:'test_from_state', className:'col-teststate' },
        { data:'message' }
      ],
      pageLength: 15
    });
  }

  $('#runEmpty').hide();
  $('#runDetailsWrap').show();
  showPanel('panel-run');
}

function renderCompareTable() {
  const showOnlyDifferences = $('#showOnlyDifferences').is(':checked');
  const showOnlyFailsBoth = $('#showOnlyFailsBoth').is(':checked');

  let html = '<table class="compare"><tr><th>#</th><th>Test Name</th><th>Field</th><th>Run A Status</th><th>Error Message A</th><th>Run B Status</th><th>Error Message B</th></tr>';
  let rowNum = 1;

  lastCompareRows.forEach(row => {
    if (showOnlyDifferences && row.statusA === row.statusB) {
      return;
    }

    if (showOnlyFailsBoth && !(row.statusA === 'FAIL' && row.statusB === 'FAIL')) {
      return;
    }

    html += '<tr class="' + row.rowClass + '">'
      + '<td>' + rowNum + '</td>'
      + '<td>' + row.testName + '</td>'
      + '<td>' + row.testField + '</td>'
      + '<td class="' + row.classA + '">' + row.statusA + '</td>'
      + '<td class="col-error">' + row.messageA + '</td>'
      + '<td class="' + row.classB + '">' + row.statusB + '</td>'
      + '<td class="col-error">' + row.messageB + '</td>'
      + '</tr>';

    rowNum++;
  });

  html += '</table>';
  $('#compareResults').html(html);
}

$(document).ready(function() {
  try {
    runsData = decodeCompressedJson(RUNS_B64);
    allResults = decodeCompressedJson(ALL_RESULTS_B64);

    $('#loadInfo').text('Loaded ' + runsData.length + ' runs');

    runsTable = $('#runsTable').DataTable({
      data: runsData,
      autoWidth: false,
      order: [[6, 'desc']],
      columns: [
        {
          data:'run_id',
          render: function(d){
            if (!d) return '';
            return '<a href="#" class="link run-link" data-run="' + d + '">' + d + '</a>';
          }
        },
        { data:'run_user', className:'col-user' },
        { data:'run_status', render: function(d){ return statusBadge(d); } },
        { data:'total_passed', className:'col-pass' },
        { data:'total_failed', className:'col-fail' },
        { data:'total_tests', className:'col-total' },
        { data:'run_start_datetime' },
        { data:'run_end_datetime' },
        { data:'state_under_test', className:'col-state' },
        { data:'run_by_automation_name' },
        { data:'run_tag' }
      ],
      pageLength: 15
    });

    Object.keys(allResults).forEach(rid => {
      $('#runA, #runB').append('<option value="' + rid + '">' + rid + '</option>');
    });
  } catch (err) {
    $('#loadInfo').text('Error loading dashboard data: ' + String(err));
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
  const sortedKeys = Array.from(allKeys).sort((a,b) => a.localeCompare(b));

  lastCompareRows = [];

  sortedKeys.forEach(key => {
    const rowsA = mapA[key] || [{ status: 'MISSING', message: '' }];
    const rowsB = mapB[key] || [{ status: 'MISSING', message: '' }];
    const maxLen = Math.max(rowsA.length, rowsB.length);

    const parts = key.split('|');
    const testName = parts[0] || '';
    const testField = parts[1] || '';

    for (let i=0; i<maxLen; i++) {
      const rowA = rowsA[i] || { status: 'MISSING', message: '' };
      const rowB = rowsB[i] || { status: 'MISSING', message: '' };

      const statusA = rowA.status ?? 'MISSING';
      const statusB = rowB.status ?? 'MISSING';
      const messageA = rowA.message ?? '';
      const messageB = rowB.message ?? '';

      lastCompareRows.push({
        testName: testName,
        testField: testField,
        statusA: statusA,
        statusB: statusB,
        messageA: messageA,
        messageB: messageB,
        rowClass: (statusA === statusB) ? 'match-row' : 'diff-row',
        classA: (statusA === 'PASS') ? 'pass' : ((statusA === 'FAIL') ? 'fail' : 'missing'),
        classB: (statusB === 'PASS') ? 'pass' : ((statusB === 'FAIL') ? 'fail' : 'missing')
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
</script>

</body>
</html>
"""

    html = html.replace("__RUNS_B64__", runs_b64)
    html = html.replace("__ALL_RESULTS_B64__", all_results_b64)
    html = html.replace("__TIMESTAMP__", timestamp)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Dashboard written: {html_path}")
    print(f"Compressed runs payload chars: {len(runs_b64)}")
    print(f"Compressed all_results payload chars: {len(all_results_b64)}")

    return html_path, html