from src.db_utils import get_runs, get_results
from datetime import datetime
import os
import json

OUTPUT_DIR = "/dbfs/tmp"

def generate_dashboard():
    """
    Generates main dashboard, drill-down pages, and compare page.
    Includes totals, correct DataTables mapping, pie chart, and compare row numbering.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")

    # =========================
    # MAIN DASHBOARD
    # =========================
    runs_df = get_runs().toPandas()

    runs_list = []
    all_results = {}
    for _, run in runs_df.iterrows():
        run_id = run["run_id"]
        results_df = get_results(run_id).toPandas()

        total_tests = len(results_df)
        total_passed = results_df[results_df["status"] == "PASS"].shape[0]
        total_failed = results_df[results_df["status"] == "FAIL"].shape[0]

        # Convert datetime to string for JSON
        run_dict = run.to_dict()
        if run_dict.get("run_start_datetime"):
            run_dict["run_start_datetime"] = run_dict["run_start_datetime"].strftime("%Y-%m-%d %H:%M:%S")
        if run_dict.get("run_end_datetime"):
            run_dict["run_end_datetime"] = run_dict["run_end_datetime"].strftime("%Y-%m-%d %H:%M:%S")

        run_dict.update({
            "total_tests": total_tests,
            "total_passed": total_passed,
            "total_failed": total_failed
        })
        runs_list.append(run_dict)

        # Store results for drill-down and compare
        all_results[run_id] = json.loads(results_df.to_json(orient="records", date_format="iso"))

    runs_json = json.dumps(runs_list)
    main_file = f"{OUTPUT_DIR}/test_dashboard-{timestamp}.html"
    compare_file = f"{OUTPUT_DIR}/compare_runs-{timestamp}.html"

    # --------------------
    # MAIN DASHBOARD HTML
    # --------------------
    main_html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>ARIA Test Runs Dashboard</title>

<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>

<style>
body {{ margin:0; font-family: Arial, sans-serif; }}
.banner {{
    background:#1f4e79;
    color:white;
    padding:20px;
    font-size:24px;
    font-weight:bold;
}}
.content {{ padding:20px; }}
a.compare-link {{ color:#1f4e79; font-weight:bold; text-decoration:none; }}
a.compare-link:hover {{ text-decoration:underline; }}
.pass {{ color:green; font-weight:bold; }}
.fail {{ color:red; font-weight:bold; }}
</style>
</head>
<body>

<div class="banner">
ARIA Test Runs Dashboard
</div>

<div class="content">
<h2>Test Runs Overview</h2>
<p>Generated: {timestamp}</p>

<p><a href="compare_runs-{timestamp}.html" class="compare-link" target="_blank">⬇ Compare Runs</a></p>

<table id="runsTable" class="display">
<thead>
<tr>
<th>Run ID</th>
<th>User</th>
<th>Start</th>
<th>End</th>
<th>Automation</th>
<th>Tag</th>
<th>Status</th>
<th>State Under Test</th>
<th>Total Passed</th>
<th>Total Failed</th>
<th>Total Tests</th>
</tr>
</thead>
<tbody></tbody>
</table>
</div>

<script>
let runsData = {runs_json};
$(document).ready(function() {{
    $('#runsTable').DataTable({{
        data: runsData,
        columns: [
            {{ data: 'run_id', render: d => '<a href="run_results_'+d+'.html">'+d+'</a>' }},
            {{ data: 'run_user' }},
            {{ data: 'run_start_datetime' }},
            {{ data: 'run_end_datetime' }},
            {{ data: 'run_by_automation_name' }},
            {{ data: 'run_tag' }},
            {{ data: 'run_status', render: d => d=='PASS'? '<span class="pass">'+d+'</span>':'<span class="fail">'+d+'</span>' }},
            {{ data: 'state_under_test' }},
            {{ data: 'total_passed' }},
            {{ data: 'total_failed' }},
            {{ data: 'total_tests' }}
        ],
        order: [[2,'desc']],
        pageLength: 15
    }});
}});
</script>
</body>
</html>
"""

    # --------------------
    # COMPARE PAGE HTML (with Row Number)
    # --------------------
    compare_html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Compare Test Runs</title>

<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<style>
body {{ font-family: Arial, sans-serif; margin:0; padding:20px; }}
h2 {{ margin-bottom:10px; }}
label {{ margin-right:8px; }}
select {{ margin-right:12px; padding:4px; }}
button {{ padding:6px 10px; cursor:pointer; }}
table {{ border-collapse: collapse; width: 100%; margin-top:20px; }}
th, td {{ border: 1px solid #ccc; padding:6px; text-align:left; }}
th {{ background:#f2f2f2; }}
.match-row {{ background-color:#d4edda; }}
.diff-row {{ background-color:#fff3cd; }}
.pass {{ color:green; font-weight:bold; }}
.fail {{ color:red; font-weight:bold; }}
</style>
</head>
<body>

<h2>Compare Runs</h2>
<label>Run A:</label>
<select id="runA"><option value="">--Select--</option></select>
<label>Run B:</label>
<select id="runB"><option value="">--Select--</option></select>
<button id="compareBtn">Compare</button>

<div id="compareResults"></div>

<script>
let allResults = {json.dumps(all_results)};

// populate dropdowns
Object.keys(allResults).forEach(rid => {{
    $('#runA, #runB').append(`<option value="${{rid}}">${{rid}}</option>`);
}});

$('#compareBtn').on('click', function() {{
    let runA = $('#runA').val();
    let runB = $('#runB').val();
    if(!runA || !runB || runA===runB){{
        alert("Select two different runs");
        return;
    }}

    let resultsA = allResults[runA];
    let resultsB = allResults[runB];

    let mapA = {{}}, mapB = {{}};
    resultsA.forEach(r => {{
        let key = r.test_name + "|" + r.test_field;
        if(!mapA[key]) mapA[key] = [];
        mapA[key].push(r);
    }});
    resultsB.forEach(r => {{
        let key = r.test_name + "|" + r.test_field;
        if(!mapB[key]) mapB[key] = [];
        mapB[key].push(r);
    }});

    let allKeys = new Set([...Object.keys(mapA), ...Object.keys(mapB)]);
    let html = "<table><tr><th>#</th><th>Test Name</th><th>Field</th><th>Run A Status</th><th>Run B Status</th></tr>";

    let rowNum = 1;

    allKeys.forEach(key => {{
        let rowsA = mapA[key] || [{{status:"MISSING"}}];
        let rowsB = mapB[key] || [{{status:"MISSING"}}];
        let maxLen = Math.max(rowsA.length, rowsB.length);

        for(let i=0;i<maxLen;i++){{
            let statusA = rowsA[i] ? rowsA[i].status : "MISSING";
            let statusB = rowsB[i] ? rowsB[i].status : "MISSING";

            let parts = key.split("|");
            let rowClass = (statusA===statusB) ? "match-row" : "diff-row";

            let classA = (statusA==="PASS") ? "pass" : (statusA==="FAIL" ? "fail" : "");
            let classB = (statusB==="PASS") ? "pass" : (statusB==="FAIL" ? "fail" : "");

            html += `<tr class="${{rowClass}}">
                        <td>${{rowNum}}</td>
                        <td>${{parts[0]}}</td>
                        <td>${{parts[1]}}</td>
                        <td class="${{classA}}">${{statusA}}</td>
                        <td class="${{classB}}">${{statusB}}</td>
                     </tr>`;
            rowNum++;
        }}
    }});

    html += "</table>";
    $('#compareResults').html(html);
}});
</script>
</body>
</html>
"""

    # --------------------
    # Write main + compare pages
    # --------------------
    with open(main_file, "w", encoding="utf-8") as f:
        f.write(main_html)
    with open(compare_file, "w", encoding="utf-8") as f:
        f.write(compare_html)

    print(f"✅ Main dashboard: {main_file}")
    print(f"✅ Compare runs page: {compare_file}")

    # =========================
    # Drill-down pages
    # =========================
    for _, run in runs_df.iterrows():
        run_id = run["run_id"]
        results_df = get_results(run_id).toPandas()
        run_file = f"{OUTPUT_DIR}/run_results_{run_id}.html"

        pie_data = results_df["status"].value_counts().to_dict()
        pie_labels = list(pie_data.keys())
        pie_values = list(pie_data.values())

        total_tests = len(results_df)
        total_passed = results_df[results_df["status"] == "PASS"].shape[0]
        total_failed = results_df[results_df["status"] == "FAIL"].shape[0]
        pass_percent = round((total_passed / total_tests) * 100, 1) if total_tests else 0

        results_json = results_df.to_json(orient="records", date_format="iso")
        pie_labels_json = json.dumps(pie_labels)
        pie_values_json = json.dumps(pie_values)

        run_html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Run {run_id} Results</title>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>

<style>
body {{ margin:0; font-family: Arial, sans-serif; }}
.banner {{ background:#1f4e79; color:white; padding:20px; font-size:24px; font-weight:bold; }}
.content {{ padding:20px; }}
.pass {{ color:green; font-weight:bold; }}
.fail {{ color:red; font-weight:bold; }}
button {{ padding:8px 12px; margin-bottom:10px; cursor:pointer; }}
.flex-container {{ display:flex; flex-wrap:wrap; gap:30px; margin-bottom:25px; }}
.flex-left {{ flex:1 1 250px; max-width:350px; }}
.summary-card {{ border:1px solid #ddd; padding:12px; border-radius:6px; background:#f9f9f9; }}
.summary-card span {{ display:block; margin-bottom:6px; font-weight:bold; overflow-wrap:break-word; }}
.flex-right {{ width:250px; height:250px; }}
</style>
</head>
<body>

<div class="banner">Run {run_id} Results</div>
<div class="content">
<p><a href="test_dashboard-{timestamp}.html">⬅ Back to Main Dashboard</a></p>

<div class="flex-container">
<div class="flex-left">
<div class="summary-card">
<span>Run ID: {run.run_id}</span>
<span>User: {run.run_user}</span>
<span>Start: {run.run_start_datetime}</span>
<span>End: {run.run_end_datetime}</span>
<span>Automation: {run.run_by_automation_name}</span>
<span>Tag: {run.run_tag}</span>
<span>Status: {run.run_status}</span>
<span>State Under Test: {run.state_under_test}</span>
<span>Total Passed: {total_passed}</span>
<span>Total Failed: {total_failed}</span>
<span>Total Tests: {total_tests}</span>
<span>Pass %: {pass_percent}%</span>
</div>
</div>

<div class="flex-right"><canvas id="pieChart"></canvas></div>
</div>

<button onclick="downloadExcel()">📥 Download Excel</button>

<table id="resultsTable" class="display">
<thead>
<tr><th>Result ID</th><th>Run ID</th><th>Test Name</th><th>Test Field</th><th>Test From State</th><th>Status</th><th>Message</th></tr>
</thead>
<tbody></tbody>
</table>
</div>

<script>
let resultsData = {results_json};

$(document).ready(function() {{
    $('#resultsTable').DataTable({{
        data: resultsData,
        columns: [
            {{ data: 'result_id' }},
            {{ data: 'run_id' }},
            {{ data: 'test_name' }},
            {{ data: 'test_field' }},
            {{ data: 'test_from_state' }},
            {{ data: 'status', render: d => d=='PASS'? '<span class="pass">'+d+'</span>':'<span class="fail">'+d+'</span>' }},
            {{ data: 'message' }}
        ],
        pageLength: 15
    }});
}});

new Chart(document.getElementById('pieChart'), {{
    type:'pie',
    data:{{ labels:{pie_labels_json}, datasets:[{{ data:{pie_values_json}, backgroundColor:['#4CAF50','#F44336']}}]}},
    options:{{ responsive:false, maintainAspectRatio:false, plugins:{{ legend:{{ position:'bottom'}}}}}}
}});

function downloadExcel() {{
    var wb = XLSX.utils.table_to_book(document.getElementById("resultsTable"), {{sheet:"Results"}});
    XLSX.writeFile(wb,"run_{run_id}_results.xlsx");
}}
</script>
</body>
</html>
"""
        with open(run_file, "w", encoding="utf-8") as f:
            f.write(run_html)

    print("✅ Drill-down pages generated for all runs")