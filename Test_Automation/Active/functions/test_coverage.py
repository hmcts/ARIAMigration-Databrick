from pyspark.sql import functions as F
from pyspark.sql.window import Window
from html import escape
import json


def generate_transformation_test_coverage_report(
    spark,
    runs_table,
    results_table,
    tests_by_state,
    state_display_order=None,
    output_file=None
):

    # -----------------------------------
    # Flatten tests
    # -----------------------------------

    test_records = []

    for state, tests in tests_by_state.items():
        for t in tests:
            test_records.append({
                "test_from_state": str(state),
                "test_name": str(t)
            })

    seen = set()
    ordered_test_records = []

    for r in test_records:
        key = (r["test_from_state"], r["test_name"])
        if key not in seen:
            seen.add(key)
            ordered_test_records.append(r)

    all_test_names = [r["test_name"] for r in ordered_test_records]
    filter_states = sorted({r["test_from_state"] for r in ordered_test_records})

    # -----------------------------------
    # Load tables
    # -----------------------------------

    runs_df = (
        spark.table(runs_table)
        .filter(F.col("run_by_automation_name") == "Transformation_Tests")
        .select(
            "run_id",
            "state_under_test",
            "run_start_datetime",
            "run_user",
            "run_tag",
            "run_status"
        )
    )

    results_df = (
        spark.table(results_table)
        .filter(F.col("test_name").isin(all_test_names))
        .select(
            "run_id",
            "test_name",
            "status",
            "message"
        )
    )

    joined_df = (
        results_df
        .join(runs_df, "run_id")
        .withColumn("status_norm", F.upper(F.trim(F.col("status"))))
    )

    # -----------------------------------
    # Discover states
    # -----------------------------------

    discovered_states = [
        r["state_under_test"]
        for r in (
            runs_df
            .select("state_under_test")
            .distinct()
            .orderBy("state_under_test")
            .collect()
        )
    ]

    if state_display_order:
        states = [s for s in state_display_order if s in discovered_states]
        states += [s for s in discovered_states if s not in states]
    else:
        states = discovered_states

    # -----------------------------------
    # Coverage (full history)
    # -----------------------------------

    coverage_all_df = (
        joined_df
        .groupBy("test_name", "state_under_test")
        .agg(
            F.sum(F.when(F.col("status_norm") == "PASS", 1).otherwise(0)).alias("pass_count"),
            F.sum(F.when(F.col("status_norm") == "FAIL", 1).otherwise(0)).alias("fail_count"),
            F.count("*").alias("total_count")
        )
    )

    lookup_all = {}

    for r in coverage_all_df.collect():
        lookup_all[(r["test_name"], r["state_under_test"])] = {
            "pass": int(r["pass_count"]),
            "fail": int(r["fail_count"]),
            "total": int(r["total_count"])
        }

    # -----------------------------------
    # Coverage (latest only)
    # -----------------------------------

    window = Window.partitionBy(
        "test_name",
        "state_under_test"
    ).orderBy(F.col("run_start_datetime").desc_nulls_last())

    latest_df = (
        joined_df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
    )

    coverage_latest_df = (
        latest_df
        .groupBy("test_name", "state_under_test")
        .agg(
            F.sum(F.when(F.col("status_norm") == "PASS", 1).otherwise(0)).alias("pass_count"),
            F.sum(F.when(F.col("status_norm") == "FAIL", 1).otherwise(0)).alias("fail_count"),
            F.count("*").alias("total_count")
        )
    )

    lookup_latest = {}

    for r in coverage_latest_df.collect():
        lookup_latest[(r["test_name"], r["state_under_test"])] = {
            "pass": int(r["pass_count"]),
            "fail": int(r["fail_count"]),
            "total": int(r["total_count"])
        }

    # -----------------------------------
    # Drilldown data
    # -----------------------------------

    drilldown_lookup = {}

    for r in joined_df.collect():

        key = str(r["test_name"]) + "|||" + str(r["state_under_test"])

        if key not in drilldown_lookup:
            drilldown_lookup[key] = []

        drilldown_lookup[key].append({
            "run_id": "" if r["run_id"] is None else str(r["run_id"]),
            "run_start_datetime": "" if r["run_start_datetime"] is None else str(r["run_start_datetime"]),
            "run_user": "" if r["run_user"] is None else str(r["run_user"]),
            "run_tag": "" if r["run_tag"] is None else str(r["run_tag"]),
            "run_status": "" if r["run_status"] is None else str(r["run_status"]),
            "result_status": "" if r["status"] is None else str(r["status"]),
            "message": "" if r["message"] is None else str(r["message"])
        })

    drilldown_json = json.dumps(drilldown_lookup)

    # -----------------------------------
    # Helper
    # -----------------------------------

    def colour(p, f, t):
        if t == 0:
            return "#f3f4f6"
        if f == 0 and p > 0:
            return "#d1fae5"
        if p == 0 and f > 0:
            return "#fee2e2"
        return "#fef3c7"

    # -----------------------------------
    # Build table rows
    # -----------------------------------

    def build_rows(lookup):

        rows = []

        total_tests = len(ordered_test_records)
        passed_some = 0
        passed_none = 0

        for i, rec in enumerate(ordered_test_records, start=1):

            test = rec["test_name"]
            from_state = rec["test_from_state"]

            total_pass = 0
            total_fail = 0
            total_runs = 0
            states_passing = 0
            states_covered = 0

            row = (
                f"<tr class='data-row' "
                f"data-states-passing='0' "
                f"data-test-from-state='{escape(from_state)}'>"
            )

            row += f"<td class='row-num-col'>{i}</td>"
            row += f"<td class='from-state-col'>{escape(from_state)}</td>"
            row += f"<td class='test-col'>{escape(test)}</td>"

            for s in states:

                item = lookup.get((test, s))

                if item:
                    p = item["pass"]
                    f = item["fail"]
                    t = item["total"]
                else:
                    p = f = t = 0

                if t > 0:
                    states_covered += 1

                if t > 0 and f == 0 and p > 0:
                    states_passing += 1

                total_pass += p
                total_fail += f
                total_runs += t

                row += (
                    f"<td class='state-col clickable-cell' "
                    f"data-test-name='{escape(test)}' "
                    f"data-state-under-test='{escape(s)}' "
                    f"data-test-from-state='{escape(from_state)}' "
                    f"style='background:{colour(p, f, t)}'>"
                    f"P:{p}<br>F:{f}<br>T:{t}</td>"
                )

            if states_passing > 0:
                passed_some += 1
            else:
                passed_none += 1

            pass_pct = (total_pass / total_runs * 100) if total_runs else 0

            row = row.replace(
                "data-states-passing='0'",
                f"data-states-passing='{states_passing}'"
            )

            row += f"<td>{total_pass}</td>"
            row += f"<td>{total_fail}</td>"
            row += f"<td>{total_runs}</td>"
            row += f"<td>{pass_pct:.1f}%</td>"
            row += f"<td>{states_passing}/{len(states)}</td>"
            row += f"<td>{states_covered}/{len(states)}</td>"
            row += "</tr>"

            rows.append(row)

        return rows, total_tests, passed_some, passed_none

    rows_all, total_tests, passed_some, passed_none = build_rows(lookup_all)
    rows_latest, _, _, _ = build_rows(lookup_latest)

    filter_options = "".join(
        f"<option value='{escape(s)}'>{escape(s)}</option>"
        for s in filter_states
    )

    total_cols = 3 + len(states) + 5 + 2

    # -----------------------------------
    # HTML
    # -----------------------------------

    html = f"""
<html>
<head>

<style>

body {{
font-family:Arial;
margin:20px;
color:#222;
}}

.summary-container {{
display:flex;
gap:15px;
margin-bottom:20px;
flex-wrap:wrap;
}}

.summary-box {{
border:1px solid #d1d5db;
border-radius:8px;
padding:14px 18px;
background:#f9fafb;
min-width:200px;
}}

.summary-label {{
font-size:12px;
color:#6b7280;
margin-bottom:4px;
}}

.summary-value {{
font-size:28px;
font-weight:bold;
}}

.controls-box {{
border:1px solid #d1d5db;
padding:12px;
border-radius:8px;
background:#f9fafb;
margin-bottom:20px;
}}

.controls-row {{
display:flex;
gap:20px;
align-items:center;
flex-wrap:wrap;
}}

.controls-row label {{
font-size:13px;
}}

.controls-row select {{
margin-left:6px;
}}

.table-wrap {{
overflow-x:auto;
margin-bottom:20px;
}}

table {{
border-collapse:collapse;
width:max-content;
}}

th,td {{
border:1px solid #ccc;
padding:4px;
font-size:12px;
text-align:center;
vertical-align:middle;
}}

.row-num-col {{
width:50px;
min-width:50px;
max-width:50px;
}}

.from-state-col {{
width:100px;
min-width:100px;
max-width:100px;
word-break:break-word;
}}

.test-col {{
width:150px;
min-width:150px;
max-width:150px;
word-break:break-word;
text-align:left;
}}

.state-col {{
width:50px;
min-width:50px;
max-width:50px;
}}

.state-header {{
width:50px;
min-width:50px;
max-width:50px;
height:150px;
padding:0;
vertical-align:bottom;
}}

.state-header div {{
writing-mode:vertical-rl;
transform:rotate(180deg);
padding:6px 2px;
}}

.clickable-cell:hover {{
outline:2px solid blue;
cursor:pointer;
}}

.drilldown-row td {{
background:#ffffff;
padding:0;
}}

.inline-drilldown {{
padding:16px;
background:#ffffff;
}}

.inline-drilldown-header {{
display:flex;
justify-content:space-between;
align-items:flex-start;
gap:16px;
margin-bottom:12px;
flex-wrap:wrap;
}}

.inline-drilldown-title {{
font-size:18px;
font-weight:bold;
margin-bottom:4px;
}}

.inline-drilldown-subtitle {{
font-size:13px;
color:#555;
}}

.inline-drilldown-note {{
font-size:12px;
color:#6b7280;
margin-top:4px;
}}

.inline-drilldown-close {{
border:1px solid #d1d5db;
background:#f9fafb;
padding:6px 10px;
cursor:pointer;
border-radius:6px;
}}

.drilldown-table {{
width:100%;
border-collapse:collapse;
}}

.drilldown-table th,
.drilldown-table td {{
border:1px solid #d1d5db;
padding:6px;
text-align:left;
vertical-align:top;
font-size:12px;
}}

.drilldown-table th {{
background:#f3f4f6;
}}

.small-muted {{
font-size:12px;
color:#6b7280;
}}

.selected-cell {{
outline:3px solid #1d4ed8 !important;
outline-offset:-3px;
}}

</style>

</head>

<body>

<h2>Transformation Tests Coverage Report</h2>

<div class="summary-container">

<div class="summary-box">
<div class="summary-label">Total Tests</div>
<div class="summary-value" id="summaryTotalTests">{total_tests}</div>
</div>

<div class="summary-box">
<div class="summary-label">Passed in One or More States</div>
<div class="summary-value" id="summaryPassedSome">{passed_some}</div>
</div>

<div class="summary-box">
<div class="summary-label">Not Passed in Any State</div>
<div class="summary-value" id="summaryPassedNone">{passed_none}</div>
</div>

</div>

<div class="controls-box">
<div class="controls-row">

<label>
Test From State
<select id="filterTestFromState">
<option value="">All</option>
{filter_options}
</select>
</label>

<label>
<input type="checkbox" id="latestOnlyToggle"> Only latest data
</label>

<label>
<input type="checkbox" id="filterNoCoverage"> Show where no Coverage
</label>

<label>
<input type="checkbox" id="filterCovered"> Show where is covered
</label>

</div>
</div>

<div class="table-wrap">
<table>

<thead>
<tr>
<th class="row-num-col">#</th>
<th class="from-state-col">Test From State</th>
<th class="test-col">Test</th>
{''.join(f"<th class='state-header'><div>{escape(s)}</div></th>" for s in states)}
<th>Total Pass</th>
<th>Total Fail</th>
<th>Total Runs</th>
<th>Pass %</th>
<th>States Passing</th>
<th>States Covered</th>
</tr>
</thead>

<tbody id="tbodyAll">
{''.join(rows_all)}
</tbody>

<tbody id="tbodyLatest" style="display:none;">
{''.join(rows_latest)}
</tbody>

</table>
</div>

<script>

const drilldownData = {drilldown_json};
const drilldownColspan = {total_cols};

function activeBody() {{
    return document.getElementById("latestOnlyToggle").checked
        ? document.getElementById("tbodyLatest")
        : document.getElementById("tbodyAll");
}}

function removeExistingDrilldown() {{
    const existing = document.querySelector(".drilldown-row");
    if (existing) existing.remove();

    document.querySelectorAll(".selected-cell").forEach(c => {{
        c.classList.remove("selected-cell");
    }});
}}

function updateSummary() {{

    let rows = Array.from(activeBody().querySelectorAll("tr.data-row"))
        .filter(r => r.style.display !== "none");

    let total = rows.length;
    let passedSome = 0;
    let passedNone = 0;

    rows.forEach(r => {{
        let p = parseInt(r.dataset.statesPassing || 0, 10);
        if (p > 0) passedSome++;
        else passedNone++;
    }});

    document.getElementById("summaryTotalTests").textContent = total;
    document.getElementById("summaryPassedSome").textContent = passedSome;
    document.getElementById("summaryPassedNone").textContent = passedNone;
}}

function applyFilters() {{

    removeExistingDrilldown();

    const showNoCoverage = document.getElementById("filterNoCoverage").checked;
    const showCovered = document.getElementById("filterCovered").checked;
    const state = document.getElementById("filterTestFromState").value;

    activeBody().querySelectorAll("tr.data-row").forEach(row => {{

        let passing = parseInt(row.dataset.statesPassing || 0, 10);
        let from = row.dataset.testFromState || "";

        let covered = passing > 0;
        let noCoverage = passing === 0;

        let show = true;

        if (showNoCoverage && !showCovered) show = noCoverage;
        if (showCovered && !showNoCoverage) show = covered;
        if (state && state !== from) show = false;

        row.style.display = show ? "" : "none";

    }});

    updateSummary();
}}

function escapeHtml(value) {{
    return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}}

function renderDrilldownTable(records, latestOnly) {{

    let displayRecords = records.slice();

    if (latestOnly) {{
        displayRecords = records.length > 0 ? [records[0]] : [];
    }}

    let html = "";

    html += "<div class='inline-drilldown'>";
    html += "<div class='inline-drilldown-header'>";
    html += "<div>";
    html += "<div class='inline-drilldown-title' id='inlineDrilldownTitle'></div>";
    html += "<div class='inline-drilldown-subtitle' id='inlineDrilldownSubtitle'></div>";
    html += "<div class='inline-drilldown-note' id='inlineDrilldownNote'></div>";
    html += "</div>";
    html += "<button class='inline-drilldown-close' onclick='closeInlineDrilldown()'>Close</button>";
    html += "</div>";

    if (displayRecords.length === 0) {{
        html += "<div class='small-muted'>No matching records.</div>";
        html += "</div>";
        return html;
    }}

    html += "<table class='drilldown-table'>";
    html += "<thead><tr>";
    html += "<th>Run Start</th>";
    html += "<th>Run ID</th>";
    html += "<th>User</th>";
    html += "<th>Tag</th>";
    html += "<th>Run Status</th>";
    html += "<th>Result Status</th>";
    html += "<th>Message</th>";
    html += "</tr></thead><tbody>";

    displayRecords.forEach(r => {{
        html += "<tr>";
        html += "<td>" + escapeHtml(r.run_start_datetime || "") + "</td>";
        html += "<td>" + escapeHtml(r.run_id || "") + "</td>";
        html += "<td>" + escapeHtml(r.run_user || "") + "</td>";
        html += "<td>" + escapeHtml(r.run_tag || "") + "</td>";
        html += "<td>" + escapeHtml(r.run_status || "") + "</td>";
        html += "<td>" + escapeHtml(r.result_status || "") + "</td>";
        html += "<td>" + escapeHtml(r.message || "") + "</td>";
        html += "</tr>";
    }});

    html += "</tbody></table>";
    html += "</div>";
    return html;
}}

function closeInlineDrilldown() {{
    removeExistingDrilldown();
}}

function openInlineDrilldown(cell) {{

    const row = cell.closest("tr");
    const test = cell.dataset.testName || "";
    const state = cell.dataset.stateUnderTest || "";
    const fromState = cell.dataset.testFromState || "";
    const latestOnly = document.getElementById("latestOnlyToggle").checked;

    const key = test + "|||" + state;
    const records = drilldownData[key] || [];

    const existing = document.querySelector(".drilldown-row");
    const sameRow = existing && existing.previousElementSibling === row;
    const sameCellSelected = cell.classList.contains("selected-cell");

    if (sameRow && sameCellSelected) {{
        removeExistingDrilldown();
        return;
    }}

    removeExistingDrilldown();
    cell.classList.add("selected-cell");

    const drillRow = document.createElement("tr");
    drillRow.className = "drilldown-row";

    const drillCell = document.createElement("td");
    drillCell.colSpan = drilldownColspan;
    drillCell.innerHTML = renderDrilldownTable(records, latestOnly);

    drillRow.appendChild(drillCell);
    row.parentNode.insertBefore(drillRow, row.nextSibling);

    document.getElementById("inlineDrilldownTitle").textContent = "Details for " + test;
    document.getElementById("inlineDrilldownSubtitle").textContent =
        "Test From State: " + fromState + " | State Under Test: " + state;
    document.getElementById("inlineDrilldownNote").textContent = latestOnly
        ? "Showing latest record only because Only latest data is enabled."
        : "Showing full history for this test and state.";

    drillRow.scrollIntoView({{ behavior: "smooth", block: "nearest" }});
}}

document.getElementById("latestOnlyToggle").addEventListener("change", function() {{

    removeExistingDrilldown();

    document.getElementById("tbodyAll").style.display = this.checked ? "none" : "";
    document.getElementById("tbodyLatest").style.display = this.checked ? "" : "none";

    applyFilters();

}});

document.getElementById("filterNoCoverage").addEventListener("change", applyFilters);
document.getElementById("filterCovered").addEventListener("change", applyFilters);
document.getElementById("filterTestFromState").addEventListener("change", applyFilters);

document.querySelectorAll(".clickable-cell").forEach(cell => {{
    cell.addEventListener("click", function() {{
        openInlineDrilldown(this);
    }});
}});

applyFilters();

</script>

</body>
</html>
"""

    if output_file:
        with open(output_file, "w", encoding="utf8") as f:
            f.write(html)

    return html