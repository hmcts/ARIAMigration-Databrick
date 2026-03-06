# dashboard.py
# Scalable dashboard for Databricks notebooks / DBFS FileStore
#
# Option 1:
# - index.html shows all runs
# - each run has its own run_<run_id>.html
# - compare.html compares two runs in-browser using embedded summaries/results
#
# This avoids:
# - giant single HTML payloads
# - CORS issues from fetch()
# - notebook output size limits from returning huge HTML strings

from functions.db_utils import get_runs, get_results
from datetime import datetime
import os
import json

OUTPUT_DIR = "/dbfs/FileStore/aria_dashboard"


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


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _write_text(path: str, text: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _json_records_from_pandas(pdf):
    return json.loads(pdf.to_json(orient="records"))


def _compute_totals(results_records):
    total_tests = len(results_records)
    total_passed = sum(1 for r in results_records if r.get("status") == "PASS")
    total_failed = sum(1 for r in results_records if r.get("status") == "FAIL")
    pass_percent = round((total_passed / total_tests) * 100, 1) if total_tests else 0.0
    return {
        "total_tests": total_tests,
        "total_passed": total_passed,
        "total_failed": total_failed,
        "pass_percent": pass_percent,
    }


def _build_run_summary(run_dict, totals):
    summary = dict(run_dict)
    summary.update(totals)
    return summary


def _run_page_html(run_summary, results_json, timestamp):
    run_json = json.dumps(run_summary)

    html = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>ARIA Test Run __RUN_ID__</title>

<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

<style>
  body { margin:0; font-family: Arial, sans-serif; background:#fff; }
  .banner { background:#1f4e79; color:white; padding:18px 20px; font-size:22px; font-weight:bold; }
  .content { padding:20px; }
  .toolbar {
    display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:14px;
  }
  .btn, button {
    display:inline-block;
    padding:8px 12px;
    border:1px solid #ccc;
    border-radius:6px;
    background:#fff;
    text-decoration:none;
    color:#222;
    cursor:pointer;
    font-size:14px;
  }
  .btn:hover, button:hover { background:#f5f5f5; }
  .split {
    display:flex;
    flex-wrap:wrap;
    gap:22px;
    margin:12px 0 18px 0;
  }
  .left {
    flex:1 1 320px;
    max-width:560px;
  }
  .right {
    width:300px;
    height:300px;
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
  .k {
    min-width:170px;
    font-weight:bold;
    color:#333;
  }
  .v {
    flex:1;
    overflow-wrap:anywhere;
    color:#111;
  }
  .pass { color:green; font-weight:bold; }
  .fail { color:red; font-weight:bold; }
  .missing { color:#666; font-style:italic; }

  td.col-resultid, th.col-resultid { width:90px; max-width:90px; overflow-wrap:anywhere; }
  td.col-runid, th.col-runid { width:90px; max-width:90px; overflow-wrap:anywhere; }
  td.col-testfield, th.col-testfield { width:160px; max-width:160px; overflow-wrap:anywhere; }
  td.col-teststate, th.col-teststate { width:160px; max-width:160px; overflow-wrap:anywhere; }
</style>
</head>
<body>
  <div class="banner">ARIA Test Runs Dashboard</div>

  <div class="content">
    <div class="toolbar">
      <a class="btn" href="/files/aria_dashboard/index.html">⬅ Back to Runs</a>
      <button id="downloadExcelBtn">📥 Download Excel (all results)</button>
      <span style="font-weight:bold;">Run: __RUN_ID__</span>
      <span style="color:#666;">Generated: __TIMESTAMP__</span>
    </div>

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
          <th class="col-resultid">Result ID</th>
          <th class="col-runid">Run ID</th>
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

<script>
  const runSummary = __RUN_SUMMARY_JSON__;
  const resultsData = __RESULTS_JSON__;
  let pieChart = null;

  function statusBadge(d) {
    if (d === 'PASS') return '<span class="pass">PASS</span>';
    if (d === 'FAIL') return '<span class="fail">FAIL</span>';
    return '<span class="missing">' + (d ?? '') + '</span>';
  }

  function buildSummaryCard(runObj) {
    const fields = [
      ['Run ID', runObj.run_id],
      ['User', runObj.run_user],
      ['Start', runObj.run_start_datetime],
      ['End', runObj.run_end_datetime],
      ['Automation', runObj.run_by_automation_name],
      ['Tag', runObj.run_tag],
      ['Status', runObj.run_status],
      ['Message', runObj.run_message],
      ['State Under Test', runObj.state_under_test],
      ['Total Passed', runObj.total_passed],
      ['Total Failed', runObj.total_failed],
      ['Total Tests', runObj.total_tests],
      ['Pass %', (runObj.pass_percent ?? 0) + '%']
    ];

    let html = '';
    fields.forEach(([k, v]) => {
      html += '<div class="kv"><div class="k">' + k + '</div><div class="v">' + (v ?? '') + '</div></div>';
    });
    return html;
  }

  function renderPie(runObj) {
    if (pieChart) {
      pieChart.destroy();
      pieChart = null;
    }

    pieChart = new Chart(document.getElementById('pieChart'), {
      type: 'pie',
      data: {
        labels: ['PASS', 'FAIL'],
        datasets: [{
          data: [runObj.total_passed || 0, runObj.total_failed || 0]
        }]
      },
      options: {
        responsive: false,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom' }
        }
      }
    });
  }

  $(document).ready(function() {
    $('#runSummaryCard').html(buildSummaryCard(runSummary));
    renderPie(runSummary);

    $('#resultsTable').DataTable({
      data: resultsData,
      autoWidth: false,
      order: [[2, 'asc']],
      columns: [
        { data: 'result_id', className: 'col-resultid' },
        { data: 'run_id', className: 'col-runid' },
        { data: 'test_name' },
        { data: 'status', render: function(d) { return statusBadge(d); } },
        { data: 'test_field', className: 'col-testfield' },
        { data: 'test_from_state', className: 'col-teststate' },
        { data: 'message' }
      ],
      pageLength: 15
    });

    $('#downloadExcelBtn').on('click', function() {
      if (!resultsData.length) {
        alert('No results to export for this run.');
        return;
      }

      const header = [
        "result_id",
        "run_id",
        "test_name",
        "status",
        "test_field",
        "test_from_state",
        "message"
      ];

      const exportRows = resultsData.map(r => ({
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
      XLSX.writeFile(wb, "run_" + (runSummary.run_id ?? "unknown") + "_results.xlsx");
    });
  });
</script>
</body>
</html>
"""
    html = html.replace("__RUN_ID__", str(run_summary.get("run_id", "")))
    html = html.replace("__TIMESTAMP__", timestamp)
    html = html.replace("__RUN_SUMMARY_JSON__", run_json)
    html = html.replace("__RESULTS_JSON__", results_json)
    return html


def _index_page_html(runs_json, timestamp):
    html = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>ARIA Test Runs Dashboard</title>

<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>

<style>
  body { margin:0; font-family: Arial, sans-serif; background:#fff; }

  .banner {
    background:#1f4e79;
    color:white;
    padding:18px 20px;
    font-size:22px;
    font-weight:bold;
  }

  .content { padding:20px; }

  .toolbar {
    display:flex;
    flex-wrap:wrap;
    gap:10px;
    align-items:center;
    margin-bottom:14px;
  }

  .btn {
    display:inline-block;
    padding:8px 12px;
    border:1px solid #ccc;
    border-radius:6px;
    background:#fff;
    text-decoration:none;
    color:#222;
    cursor:pointer;
    font-size:14px;
  }

  .btn:hover { background:#f5f5f5; }

  .link {
    color:#1f4e79;
    text-decoration:none;
    font-weight:bold;
  }

  .link:hover { text-decoration:underline; }

  .pass { color:green; font-weight:bold; }
  .fail { color:red; font-weight:bold; }
  .missing { color:#666; font-style:italic; }

  td.col-user, th.col-user {
    width:144px;
    max-width:144px;
    overflow-wrap:anywhere;
  }

  td.col-state, th.col-state {
    width:156px;
    max-width:156px;
    overflow-wrap:anywhere;
  }

  td.col-pass, th.col-pass,
  td.col-fail, th.col-fail,
  td.col-total, th.col-total {
    width:50px;
    max-width:50px;
    text-align:center;
  }
</style>
</head>
<body>
  <div class="banner">ARIA Test Runs Dashboard</div>

  <div class="content">
    <div class="toolbar">
      <a class="btn" href="/files/aria_dashboard/compare.html" target="_blank">⬍ Open Compare</a>
      <span style="color:#666;">Generated: __TIMESTAMP__</span>
    </div>

    <h2 style="margin-top:0;">Test Runs Overview</h2>

    <table id="runsTable" class="display" style="width:100%;">
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

<script>
  const runsData = __RUNS_JSON__;

  function statusBadge(d) {
    if (d === 'PASS') return '<span class="pass">PASS</span>';
    if (d === 'FAIL') return '<span class="fail">FAIL</span>';
    return '<span class="missing">' + (d ?? '') + '</span>';
  }

  $(document).ready(function() {
    $('#runsTable').DataTable({
      data: runsData,
      autoWidth: false,
      order: [[6, 'desc']],
      columns: [
        {
          data: 'run_id',
          render: function(d) {
            if (!d) return '';
            return '<a class="link" href="/files/aria_dashboard/run_' + d + '.html" target="_blank">' + d + '</a>';
          }
        },
        { data: 'run_user', className: 'col-user' },
        { data: 'run_status', render: function(d) { return statusBadge(d); } },
        { data: 'total_passed', className: 'col-pass' },
        { data: 'total_failed', className: 'col-fail' },
        { data: 'total_tests', className: 'col-total' },
        { data: 'run_start_datetime' },
        { data: 'run_end_datetime' },
        { data: 'state_under_test', className: 'col-state' },
        { data: 'run_by_automation_name' },
        { data: 'run_tag' }
      ],
      pageLength: 15
    });
  });
</script>
</body>
</html>
"""
    html = html.replace("__RUNS_JSON__", runs_json)
    html = html.replace("__TIMESTAMP__", timestamp)
    return html


def _compare_page_html(compare_runs_json, timestamp):
    html = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>ARIA Test Runs Compare</title>

<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>

<style>
  body { margin:0; font-family: Arial, sans-serif; background:#fff; }
  .banner { background:#1f4e79; color:white; padding:18px 20px; font-size:22px; font-weight:bold; }
  .content { padding:20px; }
  .toolbar {
    display:flex;
    flex-wrap:wrap;
    gap:10px;
    align-items:center;
    margin-bottom:14px;
  }
  .btn {
    display:inline-block;
    padding:8px 12px;
    border:1px solid #ccc;
    border-radius:6px;
    background:#fff;
    text-decoration:none;
    color:#222;
    cursor:pointer;
    font-size:14px;
  }
  .btn:hover { background:#f5f5f5; }
  .pass { color:green; font-weight:bold; }
  .fail { color:red; font-weight:bold; }
  .missing { color:#666; font-style:italic; }

  label { font-weight:bold; }

  table.compare {
    border-collapse: collapse;
    width: 100%;
    margin-top: 14px;
  }

  table.compare th, table.compare td {
    border: 1px solid #ccc;
    padding: 6px;
    text-align:left;
    vertical-align:top;
  }

  table.compare th { background:#f2f2f2; }
  .match-row { background-color:#d4edda; }
  .diff-row { background-color:#fff3cd; }
</style>
</head>
<body>
  <div class="banner">ARIA Test Runs Dashboard</div>

  <div class="content">
    <div class="toolbar">
      <a class="btn" href="/files/aria_dashboard/index.html">⬅ Back to Runs</a>
      <span style="color:#666;">Generated: __TIMESTAMP__</span>
    </div>

    <h2 style="margin-top:0;">Compare Runs</h2>

    <div class="toolbar">
      <label for="runA">Run A:</label>
      <select id="runA"><option value="">--Select--</option></select>

      <label for="runB">Run B:</label>
      <select id="runB"><option value="">--Select--</option></select>

      <label><input type="checkbox" id="onlyDiffs"> show only differences</label>
      <label><input type="checkbox" id="onlyFailsBoth"> only show fails on both sides</label>

      <button id="compareBtn">Compare</button>
    </div>

    <div id="compareResults"></div>
  </div>

<script>
  const compareRuns = __COMPARE_RUNS_JSON__;

  function statusBadge(d) {
    if (d === 'PASS') return '<span class="pass">PASS</span>';
    if (d === 'FAIL') return '<span class="fail">FAIL</span>';
    return '<span class="missing">' + (d ?? '') + '</span>';
  }

  function getRunPayload(runId) {
    return compareRuns.find(r => String(r.run_id) === String(runId));
  }

  function buildCompareTable(payloadA, payloadB, onlyDiffs, onlyFailsBoth) {
    const resultsA = payloadA.results || [];
    const resultsB = payloadB.results || [];

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

    let html = '<table class="compare"><tr><th>#</th><th>Test Name</th><th>Field</th><th>Run A Status</th><th>Run B Status</th><th>Run A Error</th><th>Run B Error</th></tr>';
    let rowNum = 1;

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

        const statusA = rowA.status ?? 'MISSING';
        const statusB = rowB.status ?? 'MISSING';
        const errA = rowA.message ?? '';
        const errB = rowB.message ?? '';

        const same = statusA === statusB && errA === errB;
        if (onlyDiffs && same) continue;
        if (onlyFailsBoth && !(statusA === 'FAIL' && statusB === 'FAIL')) continue;

        const rowClass = same ? 'match-row' : 'diff-row';

        html += '<tr class="' + rowClass + '">'
          + '<td>' + rowNum + '</td>'
          + '<td>' + testName + '</td>'
          + '<td>' + testField + '</td>'
          + '<td>' + statusBadge(statusA) + '</td>'
          + '<td>' + statusBadge(statusB) + '</td>'
          + '<td>' + (errA ?? '') + '</td>'
          + '<td>' + (errB ?? '') + '</td>'
          + '</tr>';

        rowNum++;
      }
    });

    html += '</table>';
    return html;
  }

  $(document).ready(function() {
    compareRuns.forEach(r => {
      const rid = String(r.run_id);
      $('#runA, #runB').append('<option value="' + rid + '">' + rid + '</option>');
    });

    $('#compareBtn').on('click', function() {
      const runA = $('#runA').val();
      const runB = $('#runB').val();

      if (!runA || !runB || runA === runB) {
        alert('Select two different runs');
        return;
      }

      const payloadA = getRunPayload(runA);
      const payloadB = getRunPayload(runB);

      if (!payloadA || !payloadB) {
        alert('Unable to find one or both selected runs in compare data.');
        return;
      }

      const html = buildCompareTable(
        payloadA,
        payloadB,
        $('#onlyDiffs').is(':checked'),
        $('#onlyFailsBoth').is(':checked')
      );

      $('#compareResults').html(html);
    });
  });
</script>
</body>
</html>
"""
    html = html.replace("__COMPARE_RUNS_JSON__", compare_runs_json)
    html = html.replace("__TIMESTAMP__", timestamp)
    return html


def generate_dashboard_option1(output_dir: str = OUTPUT_DIR, max_compare_runs: int = 50):
    _ensure_dir(output_dir)
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")

    runs_pdf = get_runs().toPandas()

    runs_list = []
    compare_runs = []

    for _, run in runs_pdf.iterrows():
        run_id = run.get("run_id")
        if not run_id:
            continue

        run_id = str(run_id)
        run_dict = run.to_dict()

        for k in ["run_start_datetime", "run_end_datetime"]:
            if k in run_dict:
                run_dict[k] = _safe_dt_str(run_dict[k])

        results_pdf = get_results(run_id).toPandas()
        results_records = _json_records_from_pandas(results_pdf)
        results_json = json.dumps(results_records)

        totals = _compute_totals(results_records)
        run_summary = _build_run_summary(run_dict, totals)

        runs_list.append(run_summary)

        run_html = _run_page_html(run_summary, results_json, timestamp)
        run_html_path = os.path.join(output_dir, f"run_{run_id}.html")
        _write_text(run_html_path, run_html)

    # Sort runs newest first for index and compare
    runs_list.sort(key=lambda r: str(r.get("run_start_datetime", "")), reverse=True)

    # Build compare payload for most recent N runs only, to keep compare.html reasonable
    for run_summary in runs_list[:max_compare_runs]:
        run_id = str(run_summary.get("run_id"))
        results_pdf = get_results(run_id).toPandas()
        results_records = _json_records_from_pandas(results_pdf)

        compare_runs.append({
            "run_id": run_id,
            "run_user": run_summary.get("run_user"),
            "run_start_datetime": run_summary.get("run_start_datetime"),
            "run_status": run_summary.get("run_status"),
            "results": results_records,
        })

    index_html = _index_page_html(json.dumps(runs_list), timestamp)
    compare_html = _compare_page_html(json.dumps(compare_runs), timestamp)

    index_path = os.path.join(output_dir, "index.html")
    compare_path = os.path.join(output_dir, "compare.html")

    _write_text(index_path, index_html)
    _write_text(compare_path, compare_html)

    print(f"Dashboard index written: {index_path}")
    print(f"Compare page written: {compare_path}")
    print(f"Run pages written: {output_dir}/run_<run_id>.html")

    return index_path