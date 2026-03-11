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

    discovered_states = [
        r["state_under_test"]
        for r in (
            runs_df
            .select("state_under_test")
            .where(F.col("state_under_test").isNotNull())
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

    total_states = len(states)

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

    latest_window = Window.partitionBy(
        "test_name",
        "state_under_test"
    ).orderBy(
        F.col("run_start_datetime").desc_nulls_last(),
        F.col("run_id").desc()
    )

    latest_rows_df = (
        joined_df
        .withColumn("rn_latest", F.row_number().over(latest_window))
        .filter(F.col("rn_latest") == 1)
    )

    coverage_latest_df = (
        latest_rows_df
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

    def colour(p, f, t):
        if t == 0:
            return "#f3f4f6"
        if f == 0 and p > 0:
            return "#d1fae5"
        if p == 0 and f > 0:
            return "#fee2e2"
        return "#fef3c7"

    def build_rows(lookup):
        rows = []
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
                f"data-test-from-state='{escape(from_state)}' "
                f"data-states-passing='{0}'>"
            )

            row += f"<td class='row-num-col sticky-col sticky-col-1'>{i}</td>"
            row += f"<td class='from-state-col sticky-col sticky-col-2'>{escape(from_state)}</td>"
            row += f"<td class='test-col sticky-col sticky-col-3'>{escape(test)}</td>"

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
                    f"style='background:{colour(p,f,t)}'>"
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

            row += f"<td class='metric-col-mid'>{states_passing} / {total_states}</td>"
            row += f"<td class='metric-col-mid'>{states_covered} / {total_states}</td>"
            row += f"<td class='metric-col-narrow'>{total_pass}</td>"
            row += f"<td class='metric-col-narrow'>{total_fail}</td>"
            row += f"<td class='metric-col-narrow'>{total_runs}</td>"
            row += f"<td class='metric-col'>{pass_pct:.1f}%</td>"

            row += "</tr>"

            rows.append(row)

        return rows, passed_some, passed_none

    rows_all, passed_some_all, passed_none_all = build_rows(lookup_all)
    rows_latest, passed_some_latest, passed_none_latest = build_rows(lookup_latest)

    total_tests = len(ordered_test_records)
    total_cols = 3 + len(states) + 6

    filter_options = "".join(
        f"<option value='{escape(s)}'>{escape(s)}</option>"
        for s in filter_states
    )

    html = f"""
<html>
<head>

<style>

body {{
    font-family: Arial, sans-serif;
    margin: 20px;
}}

.summary-box {{
    display: inline-block;
    padding: 15px;
    margin-right: 10px;
    margin-bottom: 10px;
    border: 1px solid #ccc;
    border-radius: 6px;
    background: #fafafa;
    min-width: 150px;
}}

.controls-box {{
    margin: 15px 0;
    padding: 10px;
    border: 1px solid #ccc;
    border-radius: 6px;
    background: #fafafa;
}}

.controls-box label {{
    margin-right: 18px;
    font-size: 13px;
}}

.table-wrap {{
    overflow-x: auto;
    border: 1px solid #ccc;
}}

table {{
    border-collapse: collapse;
    width: max-content;
}}

th, td {{
    border: 1px solid #ccc;
    padding: 4px;
    font-size: 12px;
    text-align: center;
    vertical-align: middle;
    background: white;
}}

thead th {{
    background: #f3f4f6;
}}

.row-num-col {{
    width: 50px;
    min-width: 50px;
    max-width: 50px;
}}

.from-state-col {{
    width: 100px;
    min-width: 100px;
    max-width: 100px;
    word-break: break-word;
}}

.test-col {{
    width: 150px;
    min-width: 150px;
    max-width: 150px;
    word-break: break-word;
    text-align: left;
}}

.state-col {{
    width: 50px;
    min-width: 50px;
    max-width: 50px;
}}

.metric-col-mid {{
    width: 50px;
    min-width: 50px;
    max-width: 50px;
    white-space: normal;
    font-size: 11px;
}}

.metric-col-narrow {{
    width: 55px;
    min-width: 55px;
    max-width: 55px;
    white-space: normal;
}}

.metric-col {{
    width: 60px;
    min-width: 60px;
    max-width: 60px;
    white-space: normal;
}}

.metric-header-wrap {{
    white-space: normal;
    word-break: break-word;
}}

.state-header {{
    width: 50px;
    min-width: 50px;
    max-width: 50px;
    height: 150px;
    padding: 0;
    vertical-align: bottom;
}}

.state-header div {{
    writing-mode: vertical-rl;
    transform: rotate(180deg);
    padding: 6px 2px;
}}

.sticky-col {{
    position: sticky;
    background: white;
    z-index: 2;
}}

.sticky-col-1 {{ left: 0; }}
.sticky-col-2 {{ left: 50px; }}
.sticky-col-3 {{ left: 150px; }}

thead .sticky-col {{
    background: #f3f4f6;
    z-index: 4;
}}

.clickable-cell:hover {{
    outline: 2px solid blue;
    cursor: pointer;
}}

.drilldown-row td {{
    background: #fff;
    padding: 10px;
}}

.drilldown-box {{
    text-align: left;
}}

.drilldown-header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
    gap: 12px;
}}

.drilldown-title {{
    font-weight: bold;
}}

.drilldown-close {{
    padding: 4px 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
    background: #f8f8f8;
    cursor: pointer;
}}

.drilldown-close:hover {{
    background: #efefef;
}}

.drilldown-table {{
    margin-top: 8px;
    width: 100%;
    border-collapse: collapse;
}}

.drilldown-table th,
.drilldown-table td {{
    border: 1px solid #ccc;
    padding: 4px;
    text-align: left;
}}

</style>

</head>

<body>

<h2>Transformation Tests Coverage Report</h2>

<div>
    <div class="summary-box">Total Tests<br><b id="totalTests">{total_tests}</b></div>
    <div class="summary-box">Passed in ≥1 State<br><b id="passSome">{passed_some_all}</b></div>
    <div class="summary-box">Passed in 0 States<br><b id="passNone">{passed_none_all}</b></div>
</div>

<div class="controls-box">

    <label>
        Test From State
        <select id="stateFilter">
            <option value="">All</option>
            {filter_options}
        </select>
    </label>

    <label><input type="checkbox" id="showNoCoverage"> Show where no Coverage</label>
    <label><input type="checkbox" id="showCovered"> Show where covered</label>
    <label><input type="checkbox" id="latestOnly"> Only latest data</label>

</div>

<div class="table-wrap">

<table>

<thead>

<tr>

<th class="row-num-col sticky-col sticky-col-1">#</th>
<th class="from-state-col sticky-col sticky-col-2">Test From State</th>
<th class="test-col sticky-col sticky-col-3">Test</th>

{''.join(f"<th class='state-header'><div>{escape(s)}</div></th>" for s in states)}

<th>States Passing</th>
<th>States Covered</th>
<th>Total Pass</th>
<th>Total Fail</th>
<th>Total Runs</th>
<th>Pass %</th>

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

window.closeDrilldown = function() {{
    let d = document.querySelector(".drilldown-row");
    if (d) d.remove();
}};

function activeBody() {{
    return document.getElementById("latestOnly").checked
        ? document.getElementById("tbodyLatest")
        : document.getElementById("tbodyAll");
}}

function removeExistingDrilldown() {{
    let d = document.querySelector(".drilldown-row");
    if (d) d.remove();
}}

function updateSummary() {{
    const rows = [...activeBody().querySelectorAll("tr.data-row")]
        .filter(r => r.style.display !== "none");

    let total = rows.length;
    let passSome = 0;
    let passNone = 0;

    rows.forEach(r => {{
        const passing = parseInt(r.dataset.statesPassing || "0", 10);
        if (passing > 0) passSome++;
        else passNone++;
    }});

    document.getElementById("totalTests").innerText = total;
    document.getElementById("passSome").innerText = passSome;
    document.getElementById("passNone").innerText = passNone;
}}

function applyFilters() {{
    removeExistingDrilldown();

    const selectedState = document.getElementById("stateFilter").value;
    const showNoCoverage = document.getElementById("showNoCoverage").checked;
    const showCovered = document.getElementById("showCovered").checked;
    const latestOnly = document.getElementById("latestOnly").checked;

    document.getElementById("tbodyAll").style.display = latestOnly ? "none" : "";
    document.getElementById("tbodyLatest").style.display = latestOnly ? "" : "none";

    activeBody().querySelectorAll("tr.data-row").forEach(row => {{
        const fromState = row.dataset.testFromState || "";
        const passing = parseInt(row.dataset.statesPassing || "0", 10);

        const matchesState = !selectedState || fromState === selectedState;
        const isNoCoverage = passing === 0;
        const isCovered = passing > 0;

        let show = true;

        if (showNoCoverage && !showCovered) show = isNoCoverage;
        if (showCovered && !showNoCoverage) show = isCovered;
        if (!matchesState) show = false;

        row.style.display = show ? "" : "none";
    }});

    updateSummary();
}}

function openInlineDrilldown(cell) {{

    removeExistingDrilldown();

    const row = cell.closest("tr");
    const test = cell.dataset.testName;
    const state = cell.dataset.stateUnderTest;

    const key = test + "|||" + state;
    const records = drilldownData[key] || [];

    let html = "<tr class='drilldown-row'><td colspan='" + drilldownColspan + "'>";
    html += "<div class='drilldown-box'>";
    html += "<div class='drilldown-header'>";
    html += "<div class='drilldown-title'>" + test + " | " + state + "</div>";
    html += "<button type='button' class='drilldown-close' onclick='window.closeDrilldown()'>Close</button>";
    html += "</div>";

    html += "<table class='drilldown-table'>";
    html += "<tr><th>Run Time</th><th>Status</th><th>User</th><th>Message</th></tr>";

    records.forEach(r => {{
        html += "<tr>";
        html += "<td>" + r.run_start_datetime + "</td>";
        html += "<td>" + r.result_status + "</td>";
        html += "<td>" + r.run_user + "</td>";
        html += "<td>" + r.message + "</td>";
        html += "</tr>";
    }});

    html += "</table></div></td></tr>";

    row.insertAdjacentHTML("afterend", html);
}}

document.querySelectorAll(".clickable-cell").forEach(cell => {{
    cell.addEventListener("click", function() {{
        openInlineDrilldown(this);
    }});
}});

document.getElementById("stateFilter").addEventListener("change", applyFilters);
document.getElementById("showNoCoverage").addEventListener("change", applyFilters);
document.getElementById("showCovered").addEventListener("change", applyFilters);
document.getElementById("latestOnly").addEventListener("change", applyFilters);

applyFilters();

</script>

</body>
</html>
"""

    if output_file:
        with open(output_file, "w", encoding="utf8") as f:
            f.write(html)

    return html