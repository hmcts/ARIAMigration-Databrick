from collections import defaultdict, Counter
from datetime import datetime
import os

def generate_html_report(results, report_dir,timestamp):
# def generate_html_report(results, base_path="results"):
    """
    Generates a dynamic HTML report using DataTables.js with summary of pass/fail by test_type
    """
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # output_dir = f"{base_path}/{timestamp}"
    # os.makedirs(output_dir, exist_ok=True)

    output_file = f"{report_dir}/validation_report.html"

    # --- Prepare summary: pass/fail counts by test_type ---
    summary_counts = defaultdict(lambda: {"PASS": 0, "FAIL": 0})
    for r in results:
        summary_counts[r.test_type][r.status] += 1

    summary_html = ""
    for test_type, counts in summary_counts.items():
        summary_html += f"""
        <tr>
            <td>{test_type}</td>
            <td>{counts['PASS']}</td>
            <td>{counts['FAIL']}</td>
        </tr>
        """

    # --- Prepare detailed table rows ---
    rows_html = ""
    for r in results:
        status_class = "pass" if r.status == "PASS" else "fail"
        rows_html += f"""
        <tr>
            <td>{r.table_name}</td>
            <td>{r.test_type}</td>
            <td class="{status_class}">{r.status}</td>
            <td>{r.message}</td>
        </tr>
        """

    # --- HTML template ---
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>SQL vs Parquet Validation Report</title>

<!-- DataTables CSS -->
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css"/>

<style>
    body {{ font-family: Arial, sans-serif; padding: 20px; }}
    h1 {{ color: #333; }}
    .pass {{ color: green; font-weight: bold; }}
    .fail {{ color: red; font-weight: bold; }}
    table {{ margin-bottom: 30px; }}
</style>

<!-- jQuery and DataTables JS -->
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
</head>

<body>
<h1>SQL vs Parquet Validation Report</h1>
<p><b>Run timestamp:</b> {timestamp}</p>

<h2>Summary by Test Type</h2>
<table id="summaryTable" class="display" style="width:50%">
    <thead>
        <tr>
            <th>Test Type</th>
            <th>Passed</th>
            <th>Failed</th>
        </tr>
    </thead>
    <tbody>
        {summary_html}
    </tbody>
</table>

<h2>Detailed Results</h2>
<table id="resultsTable" class="display" style="width:100%">
    <thead>
        <tr>
            <th>Table Name</th>
            <th>Test Type</th>
            <th>Status</th>
            <th>Message</th>
        </tr>
    </thead>
    <tbody>
        {rows_html}
    </tbody>
</table>

<script>
$(document).ready(function() {{
    $('#resultsTable').DataTable({{
        "pageLength": 25,
        "order": [[0, "asc"]],
        "dom": 'Bfrtip',
        "initComplete": function () {{
            // Add a select filter for the Status column
            this.api().columns(2).every(function () {{
                var column = this;
                var select = $('<select><option value="">All Status</option></select>')
                    .appendTo($(column.header()).empty())
                    .on('change', function () {{
                        var val = $.fn.dataTable.util.escapeRegex($(this).val());
                        column.search(val ? '^'+val+'$' : '', true, false).draw();
                    }});
                column.data().unique().sort().each(function(d, j) {{
                    select.append('<option value="'+d+'">'+d+'</option>')
                }});
            }});
        }}
    }});
    $('#summaryTable').DataTable({{
        "paging": false,
        "searching": false,
        "info": false
    }});
}});
</script>

</body>
</html>
"""

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    return output_file

