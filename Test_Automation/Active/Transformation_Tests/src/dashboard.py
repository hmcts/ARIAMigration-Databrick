from src.db_utils import get_runs, get_results
from datetime import datetime
import os
import json

OUTPUT_DIR = "/dbfs/tmp"

def generate_dashboard():
    """
    Generates the main dashboard and drill-down pages for test runs.
    Fully reload-safe.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")

    # =========================
    # MAIN DASHBOARD
    # =========================
    runs_df = get_runs().toPandas()
    main_file = f"{OUTPUT_DIR}/test_dashboard-{timestamp}.html"
    runs_json = runs_df.to_json(orient="records", date_format="iso")

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
            {{ data: 'run_id',
               render: function(d) {{
                   return '<a href="run_results_'+d+'.html">'+d+'</a>';
               }}
            }},
            {{ data: 'run_user' }},
            {{ data: 'run_start_datetime' }},
            {{ data: 'run_end_datetime' }},
            {{ data: 'run_by_automation_name' }},
            {{ data: 'run_tag' }},
            {{ data: 'run_status',
               render: function(d) {{
                   return d=='PASS'
                       ? '<span class="pass">'+d+'</span>'
                       : '<span class="fail">'+d+'</span>';
               }}
            }},
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

    with open(main_file, "w") as f:
        f.write(main_html)

    print(f"✅ Main dashboard generated: {main_file}")

    # =========================
    # DRILL-DOWN PAGES
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

        run_html = f"""
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
body {{ margin:0; font-family: Arial, sans-serif; }}

.banner {{
    background:#1f4e79;
    color:white;
    padding:20px;
    font-size:24px;
    font-weight:bold;
}}

.content {{ padding:20px; }}

.pass {{ color:green; font-weight:bold; }}
.fail {{ color:red; font-weight:bold; }}
button {{ padding:8px 12px; margin-bottom:10px; cursor:pointer; }}

.flex-container {{
    display:flex;
    flex-wrap:wrap;
    gap:30px;
    margin-bottom:25px;
}}

.flex-left {{
    flex: 1 1 250px;
    max-width: 350px;
}}

.summary-card {{
    border:1px solid #ddd;
    padding:12px;
    border-radius:6px;
    background:#f9f9f9;
}}

.summary-card span {{
    display:block;
    margin-bottom:6px;
    font-weight:bold;
    overflow-wrap:break-word;
}}

.flex-right {{
    width:250px;
    height:250px;
}}
</style>
</head>
<body>

<div class="banner">
ARIA Test Runs Dashboard
</div>

<div class="content">

<h2>Results for Run {run_id}</h2>
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

    <div class="flex-right">
        <canvas id="pieChart"></canvas>
    </div>

</div>

<button onclick="downloadExcel()">📥 Download Excel</button>

<table id="resultsTable" class="display">
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
            {{ data: 'status',
               render: function(d) {{
                   return d=='PASS'
                       ? '<span class="pass">'+d+'</span>'
                       : '<span class="fail">'+d+'</span>';
               }}
            }},
            {{ data: 'message' }}
        ],
        order: [[5,'desc']],
        pageLength: 15
    }});
}});

new Chart(document.getElementById('pieChart'), {{
    type: 'pie',
    data: {{
        labels: {json.dumps(pie_labels)},
        datasets: [{{
            data: {json.dumps(pie_values)},
            backgroundColor: ['#4CAF50','#F44336']
        }}]
    }},
    options: {{
        responsive: false,
        maintainAspectRatio: false,
        plugins: {{ legend: {{ position: 'bottom' }} }}
    }}
}});

function downloadExcel() {{
    var wb = XLSX.utils.table_to_book(
        document.getElementById("resultsTable"),
        {{sheet:"Results"}}
    );
    XLSX.writeFile(
        wb,
        "run_{run.state_under_test}_{run_id}_results.xlsx"
    );
}}
</script>

</body>
</html>
"""

        with open(run_file, "w") as f:
            f.write(run_html)

        print(f"✅ Drill-down page generated for run {run_id}")
