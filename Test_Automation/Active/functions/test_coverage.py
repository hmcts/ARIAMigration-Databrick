from pyspark.sql import functions as F
from html import escape


def generate_transformation_test_coverage_report(
    spark,
    runs_table,
    results_table,
    test_names,
    state_display_order=None,
    output_file=None
):

    # ---------------------------------------------------
    # LOAD DATA
    # ---------------------------------------------------

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
        .filter(F.col("test_name").isin(test_names))
        .select(
            "run_id",
            "test_name",
            "status",
            "message"
        )
    )

    joined_df = (
        results_df.alias("r")
        .join(runs_df.alias("ru"), on="run_id", how="inner")
        .select(
            "run_id",
            "test_name",
            F.col("status").alias("result_status"),
            "state_under_test"
        )
    )

    joined_df = joined_df.withColumn(
        "result_status_norm",
        F.upper(F.trim(F.col("result_status")))
    )

    # ---------------------------------------------------
    # STATE ORDERING
    # ---------------------------------------------------

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

    # ---------------------------------------------------
    # COVERAGE AGGREGATION
    # ---------------------------------------------------

    coverage_df = (
        joined_df.groupBy("test_name", "state_under_test")
        .agg(
            F.sum(F.when(F.col("result_status_norm") == "PASS", 1).otherwise(0)).alias("pass_count"),
            F.sum(F.when(F.col("result_status_norm") == "FAIL", 1).otherwise(0)).alias("fail_count"),
            F.count(F.lit(1)).alias("total_count")
        )
    )

    coverage_pd = coverage_df.toPandas()

    lookup = {}
    for _, r in coverage_pd.iterrows():
        lookup[(r["test_name"], r["state_under_test"])] = r

    # ---------------------------------------------------
    # CELL COLOUR
    # ---------------------------------------------------

    def colour(p, f, t):
        if t == 0:
            return "#f3f4f6"
        if f == 0 and p > 0:
            return "#d1fae5"
        if p == 0 and f > 0:
            return "#fee2e2"
        return "#fef3c7"

    rows = []

    # ---------------------------------------------------
    # SUMMARY COUNTS
    # ---------------------------------------------------

    total_tests_count = len(test_names)
    tests_with_any_states_passing = 0
    tests_with_no_states_passing = 0

    # ---------------------------------------------------
    # BUILD REPORT ROWS
    # ---------------------------------------------------

    for test in test_names:

        row = f"<tr class='data-row' data-states-passing='0'><td class='sticky test-col'>{escape(test)}</td>"

        total_pass = 0
        total_fail = 0
        total_exec = 0

        states_passing = 0
        states_covered = 0

        for state in states:

            item = lookup.get((test, state))

            if item is None:
                p = f = t = 0
            else:
                p = int(item.pass_count)
                f = int(item.fail_count)
                t = int(item.total_count)

            if t > 0:
                states_covered += 1

            if t > 0 and f == 0 and p > 0:
                states_passing += 1

            total_pass += p
            total_fail += f
            total_exec += t

            row += f"""
            <td class="state-col" style="background:{colour(p, f, t)}">
                <div>P:{p}</div>
                <div>F:{f}</div>
                <div>T:{t}</div>
            </td>
            """

        if states_passing > 0:
            tests_with_any_states_passing += 1
        else:
            tests_with_no_states_passing += 1

        pass_pct = (total_pass / total_exec * 100) if total_exec else 0

        pass_class = "states-pass-good" if states_passing == len(states) and len(states) > 0 else "states-pass-bad"

        row = row.replace(
            "class='data-row' data-states-passing='0'",
            f"class='data-row' data-states-passing='{states_passing}'"
        )

        row += f"""
        <td class="total-col">{total_pass}</td>
        <td class="total-col">{total_fail}</td>
        <td class="total-col">{total_exec}</td>
        <td class="total-col">{pass_pct:.1f}%</td>
        <td class="{pass_class}">{states_passing}/{len(states)}</td>
        <td class="total-col">{states_covered}/{len(states)}</td>
        </tr>
        """

        rows.append(row)

    # ---------------------------------------------------
    # HTML OUTPUT
    # ---------------------------------------------------

    html = f"""
<html>
<head>
<style>

body {{
    font-family: Arial, sans-serif;
    margin: 20px;
}}

.summary-cards {{
    display: flex;
    gap: 12px;
    margin-bottom: 16px;
    flex-wrap: wrap;
}}

.summary-card {{
    border: 1px solid #d1d5db;
    background: #f9fafb;
    padding: 12px 14px;
    min-width: 180px;
}}

.summary-card .label {{
    font-size: 12px;
    color: #555;
    margin-bottom: 4px;
}}

.summary-card .value {{
    font-size: 24px;
    font-weight: bold;
    color: #111827;
}}

.table-wrap {{
    overflow-x: auto;
}}

.controls {{
    margin-bottom: 12px;
    padding: 10px 12px;
    border: 1px solid #d1d5db;
    background: #f9fafb;
    display: inline-block;
}}

.controls label {{
    margin-right: 18px;
    font-size: 13px;
    user-select: none;
}}

table {{
    border-collapse: collapse;
}}

th, td {{
    border: 1px solid #ccc;
    padding: 4px;
    text-align: center;
    font-size: 12px;
}}

th {{
    background: #f2f2f2;
    white-space: normal;
    word-wrap: break-word;
    word-break: break-word;
}}

.state-col {{
    width: 50px;
    min-width: 50px;
    max-width: 50px;
    font-size: 11px;
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

.state-header > div {{
    writing-mode: vertical-rl;
    transform: rotate(180deg);
    white-space: normal;
    word-break: break-word;
    overflow-wrap: anywhere;
    margin: 0 auto;
    padding: 6px 2px;
    line-height: 1.1;
}}

.sticky {{
    position: sticky;
    left: 0;
    background: white;
    text-align: left;
    font-weight: bold;
    width: 150px;
    min-width: 150px;
    max-width: 150px;
    white-space: normal;
    word-wrap: break-word;
    z-index: 2;
}}

thead .sticky {{
    background: #f2f2f2;
    z-index: 3;
}}

.test-col {{
    white-space: normal;
    word-break: break-word;
}}

.states-pass-good {{
    background: #dcfce7;
    font-weight: bold;
    color: #166534;
}}

.states-pass-bad {{
    background: #fee2e2;
    font-weight: bold;
    color: #991b1b;
}}

.total-col {{
    white-space: normal;
    word-break: break-word;
}}

.hidden-row {{
    display: none;
}}

</style>
</head>

<body>

<h1>Transformation Tests Coverage Report</h1>

<div class="summary-cards">
    <div class="summary-card">
        <div class="label">Total Tests</div>
        <div class="value">{total_tests_count}</div>
    </div>
    <div class="summary-card">
        <div class="label">Passed in One or More States</div>
        <div class="value">{tests_with_any_states_passing}</div>
    </div>
    <div class="summary-card">
        <div class="label">Not Passed in Any State</div>
        <div class="value">{tests_with_no_states_passing}</div>
    </div>
</div>

<div class="controls">
    <label>
        <input type="checkbox" id="filterNoCoverage">
        Show where no Coverage
    </label>
    <label>
        <input type="checkbox" id="filterCovered">
        Show where is covered
    </label>
</div>

<div class="table-wrap">
<table>

<thead>
<tr>
    <th class="sticky">Test</th>
    {''.join(f"<th class='state-header'><div>{escape(str(s))}</div></th>" for s in states)}
    <th>Total Pass</th>
    <th>Total Fail</th>
    <th>Total Runs</th>
    <th>Pass %</th>
    <th>States Passing</th>
    <th>States Covered</th>
</tr>
</thead>

<tbody>
{''.join(rows)}
</tbody>

</table>
</div>

<script>
(function() {{
    const noCoverageCheckbox = document.getElementById("filterNoCoverage");
    const coveredCheckbox = document.getElementById("filterCovered");
    const rows = Array.from(document.querySelectorAll("tbody tr.data-row"));

    function applyFilters() {{
        const showNoCoverage = noCoverageCheckbox.checked;
        const showCovered = coveredCheckbox.checked;

        rows.forEach(row => {{
            const statesPassing = parseInt(row.getAttribute("data-states-passing") || "0", 10);

            const isNoCoverage = statesPassing === 0;
            const isCovered = statesPassing !== 0;

            let show = true;

            if (showNoCoverage && !showCovered) {{
                show = isNoCoverage;
            }} else if (!showNoCoverage && showCovered) {{
                show = isCovered;
            }} else {{
                show = true;
            }}

            row.style.display = show ? "" : "none";
        }});
    }}

    noCoverageCheckbox.addEventListener("change", applyFilters);
    coveredCheckbox.addEventListener("change", applyFilters);

    applyFilters();
}})();
</script>

</body>
</html>
"""

    if output_file:
        with open(output_file, "w", encoding="utf8") as f:
            f.write(html)

    return html