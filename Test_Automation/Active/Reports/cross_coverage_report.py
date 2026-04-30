"""
Cross-Coverage Report rendering logic. Called from the Cross_Coverage_Report
notebook. All tunables are passed in as arguments; nothing here is edited
for tuning.
"""


DEFAULT_CHAIN_ORDER = [
    "paymentPending",
    "appealSubmitted",
    "awaitingRespondentEvidence(a)",
    "awaitingRespondentEvidence(b)",
    "caseUnderReview",
    "reasonsForAppealSubmitted",
    "listing",
    "prepareForHearing",
    "decision",
    "decided(a)",
    "ftpaSubmitted(a)",
    "decided(b)",
    "ftpaSubmitted(b)",
    "ftpaDecided",
    "ended",
    "remitted",
]


def build_and_display(
    spark,
    displayHTML,
    runs_table,
    results_table,
    filter_states=None,
    chain_order=None,
    from_state_col_width_px=100,
    field_col_width_px=91,
    field_col_min_width_px=56,
    matrix_compact_font_px=10,
    heatmap_cell_min_width_px=18,
    report_version="v?",
):
    if chain_order is None:
        chain_order = DEFAULT_CHAIN_ORDER
    if filter_states is None:
        filter_states = []
    REPORT_VERSION = report_version


    from pyspark.sql.functions import col, desc
    from collections import defaultdict
    import pandas as pd

    active_states = filter_states if filter_states else chain_order

    runs_df = spark.table(runs_table)
    results_df = spark.table(results_table)

    trans_runs = runs_df.filter(col("run_by_automation_name") == "Transformation_Tests")
    all_runs_list = trans_runs.orderBy(desc("run_start_datetime")).collect()
    run_lookup = {r.run_id: r for r in all_runs_list}
    all_run_ids = [r.run_id for r in all_runs_list]

    if not all_run_ids:
        print(f"No runs found in {runs_table}")
    else:
        all_results = results_df.filter(col("run_id").isin(all_run_ids)).toPandas()
        run_to_state = {r.run_id: r.state_under_test for r in all_runs_list}
        all_results["run_state"] = all_results["run_id"].map(run_to_state)
        all_results["status_upper"] = all_results["status"].str.upper().str.strip()

        NO_DATA_PATTERNS = ["NO RECORDS TO TEST", "NO MATCHING TEST DATA", "DOES NOT EXIST IN THE",
                            "NO RECORDS FOUND", "NO DATA AVAILABLE", "NO DATA TO TEST",
                            "NO DATA EXISTS FOR", "UNRESOLVED_COLUMN"]
        ERROR_PATTERNS = ["IS NOT DEFINED", "TEST CRASHED:", "FAILED TO SETUP DATA"]

        def reclassify(row):
            status = row["status_upper"]
            if status == "PASS": return "PASS"
            msg = str(row.get("message", "") or "").upper()
            for p in NO_DATA_PATTERNS:
                if p in msg: return "NO_DATA"
            for p in ERROR_PATTERNS:
                if p in msg: return "ERROR"
            return status

        all_results["status_upper"] = all_results.apply(reclassify, axis=1)

        if all_results.empty:
            print("No results found")
        else:
            field_pair_runstate_best = defaultdict(dict)
            field_pair_runstate_latest = defaultdict(dict)
            field_pair_runstate_runs = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            priority = {"PASS": 4, "FAIL": 3, "ERROR": 2, "NO_DATA": 1, "": 0}

            all_results["__run_dt"] = all_results["run_id"].map(
                lambda rid: getattr(run_lookup.get(rid), "run_start_datetime", None)
            )
            all_results = all_results.sort_values("__run_dt", na_position="first")

            for _, row in all_results.iterrows():
                field = str(row.get("test_field", "") or "").strip()
                from_state = str(row.get("test_from_state", "") or "").strip()
                run_state = str(row.get("run_state", "") or "").strip()
                test_name = str(row.get("test_name", "") or "").strip()
                rs = row["status_upper"]
                run_id = row.get("run_id", "")
                if not field or not from_state or not run_state or not test_name: continue
                if from_state not in active_states: continue
                pair = (from_state, field, test_name)
                field_pair_runstate_runs[pair][run_state][run_id].append({
                    "status": rs,
                    "message": str(row.get("message", ""))[:300],
                })
                field_pair_runstate_latest[pair][run_state] = rs
                current = field_pair_runstate_best[pair].get(run_state, "")
                if priority.get(rs, 0) > priority.get(current, 0):
                    field_pair_runstate_best[pair][run_state] = rs

            states_in_data = sorted(
                set(rs for m in field_pair_runstate_latest.values() for rs in m.keys()),
                key=lambda s: chain_order.index(s) if s in chain_order else 999
            )

            pair_stats = {}
            for pair, run_state_latest_for_pair in field_pair_runstate_latest.items():
                latest_vals = list(run_state_latest_for_pair.values())
                total_p = total_f = total_e = total_nd = 0
                for run_state, runs_map in field_pair_runstate_runs[pair].items():
                    for rid, rlist in runs_map.items():
                        for r in rlist:
                            s = r["status"]
                            if s == "PASS": total_p += 1
                            elif s == "FAIL": total_f += 1
                            elif s == "ERROR": total_e += 1
                            elif s == "NO_DATA": total_nd += 1
                if "PASS" in latest_vals: v = "PASSED"
                elif "FAIL" in latest_vals: v = "FAILED"
                elif "ERROR" in latest_vals: v = "ERROR"
                else: v = "NEEDS DATA"
                pair_stats[pair] = {
                    "verdict": v,
                    "states_covered": len(latest_vals),
                    "pass_states": sum(1 for v in latest_vals if v == "PASS"),
                    "pass": total_p, "fail": total_f, "error": total_e, "nodata": total_nd,
                    "total": total_p + total_f + total_e + total_nd,
                }

            count_passed = sum(1 for v in pair_stats.values() if v["verdict"] == "PASSED")
            count_failed = sum(1 for v in pair_stats.values() if v["verdict"] == "FAILED")
            count_error = sum(1 for v in pair_stats.values() if v["verdict"] == "ERROR")
            count_needs_data = sum(1 for v in pair_stats.values() if v["verdict"] == "NEEDS DATA")
            count_total_pairs = len(pair_stats)

            pair_runstate_tooltip = {}
            def _attr_safe(s):
                # Escape double quotes AND newlines so the string is safe inside
                # an HTML attribute even when the whole HTML later gets wrapped
                # in a single-line JS string literal by the Databricks iframe.
                return (str(s)
                        .replace("&", "&amp;")
                        .replace('"', "&quot;")
                        .replace("<", "&lt;")
                        .replace(">", "&gt;")
                        .replace("\n", " | ")
                        .replace("\r", " ")
                        .replace("\t", " "))

            for pair, runs_map in field_pair_runstate_runs.items():
                for run_state, rid_map in runs_map.items():
                    items = []
                    for rid, rlist in rid_map.items():
                        for r in rlist:
                            st = r.get("status", "")
                            msg = (str(r.get("message", "") or ""))[:120]
                            items.append(f"[{st}] {msg}")
                    if len(items) > 4:
                        shown = items[:4] + [f"... +{len(items) - 4} more"]
                    else:
                        shown = items
                    pair_runstate_tooltip[(pair, run_state)] = _attr_safe(" | ".join(shown))


            # ---- Per-state totals ----
            state_totals = []
            for state in states_in_data:
                state_rows = all_results[all_results["run_state"] == state]
                if state_rows.empty: continue
                sp = (state_rows["status_upper"] == "PASS").sum()
                sf = (state_rows["status_upper"] == "FAIL").sum()
                se = (state_rows["status_upper"] == "ERROR").sum()
                sn = (state_rows["status_upper"] == "NO_DATA").sum()
                stotal = len(state_rows)
                sfields = state_rows["test_field"].nunique()
                run_pass = sp + sf
                pass_pct = round(sp / run_pass * 100, 1) if run_pass > 0 else 0.0
                state_totals.append({
                    "state": state, "total": stotal, "pass": sp, "fail": sf,
                    "error": se, "nodata": sn, "fields": sfields, "pass_pct": pass_pct,
                })

            state_totals_rows = ""
            for s in state_totals:
                state_totals_rows += (
                    f'<tr data-state="{s["state"]}" data-total="{s["total"]}" data-fields="{s["fields"]}" '
                    f'data-pass="{s["pass"]}" data-fail="{s["fail"]}" data-error="{s["error"]}" '
                    f'data-nodata="{s["nodata"]}" data-pct="{s["pass_pct"]}">'
                    f'<td><b>{s["state"]}</b></td>'
                    f'<td style="text-align:right">{s["total"]}</td>'
                    f'<td style="text-align:right">{s["fields"]}</td>'
                    f'<td class="pass" style="text-align:right">{s["pass"]}</td>'
                    f'<td class="fail" style="text-align:right">{s["fail"]}</td>'
                    f'<td class="error" style="text-align:right">{s["error"]}</td>'
                    f'<td class="nodata" style="text-align:right">{s["nodata"]}</td>'
                    f'<td style="text-align:right">{s["pass_pct"]}%</td>'
                    f'</tr>\n'
                )

            # ---- Matrix rows ----
            def pair_sort(pair):
                from_state, field, test_name = pair
                chain_idx = chain_order.index(from_state) if from_state in chain_order else 999
                order = {"FAILED": 0, "ERROR": 1, "NEEDS DATA": 2, "PASSED": 3}
                v_rank = order.get(pair_stats[pair]["verdict"], 3)
                return (chain_idx, field, v_rank, test_name)

            field_rows = ""
            row_idx = 0
            for pair in sorted(pair_stats.keys(), key=pair_sort):
                from_state, field, test_name = pair
                stats = pair_stats[pair]
                verdict = stats["verdict"]
                v_css = {"PASSED": "v-pass", "FAILED": "v-fail", "ERROR": "v-err", "NEEDS DATA": "v-nodata"}[verdict]
                run_state_latest_for_pair = field_pair_runstate_latest[pair]
                run_state_best_for_pair = field_pair_runstate_best[pair]
                row_run_states = []
                fails_in_states = []
                errors_in_states = []
                state_cells = ""
                for state in states_in_data:
                    val = run_state_latest_for_pair.get(state, "")
                    best = run_state_best_for_pair.get(state, "")
                    regressed = bool(val) and priority.get(best, 0) > priority.get(val, 0)
                    if val:
                        row_run_states.append(state)
                    if val == "FAIL":
                        fails_in_states.append(state)
                    elif val == "ERROR":
                        errors_in_states.append(state)
                    tip_parts = []
                    if regressed:
                        tip_parts.append(f"Regressed from {best}")
                    base_tip = pair_runstate_tooltip.get((pair, state), "")
                    if base_tip:
                        tip_parts.append(base_tip)
                    tip_attr = f' title="{" | ".join(tip_parts)}"' if tip_parts else ''
                    css_extra = f' data-col-state="{state}"' + tip_attr
                    regress_marker = '<span class="regress">&#x2193;</span>' if regressed else ''
                    if val == "PASS": state_cells += f'<td class="hm-pass"{css_extra}>&#10003;{regress_marker}</td>'
                    elif val == "FAIL": state_cells += f'<td class="hm-fail"{css_extra}>&#10007;{regress_marker}</td>'
                    elif val == "ERROR": state_cells += f'<td class="hm-err"{css_extra}>!{regress_marker}</td>'
                    elif val == "NO_DATA": state_cells += f'<td class="hm-nd"{css_extra}>&mdash;{regress_marker}</td>'
                    else: state_cells += f'<td class="hm-empty"{css_extra}></td>'

                state_data_attr = "|".join(row_run_states)
                is_crashed_chunk = 1 if field.startswith("chunk:") else 0
                is_partial = 1 if stats["pass_states"] < stats["states_covered"] else 0

                field_rows += (
                    f'<tr class="field-row" data-field="{field.lower()}" data-testname="{test_name.lower()}" data-verdict="{verdict}" '
                    f'data-states="{state_data_attr}" data-fromstate="{from_state}" '
                    f'data-passstates="{stats["pass_states"]}" data-statescov="{stats["states_covered"]}" '
                    f'data-crashed="{is_crashed_chunk}" data-partial="{is_partial}" '
                    f'data-fails-in="{"|".join(fails_in_states)}" data-errors-in="{"|".join(errors_in_states)}" '
                    f'data-pass-count="{stats["pass"]}" data-fail-count="{stats["fail"]}" '
                    f'data-error-count="{stats["error"]}" data-nodata-count="{stats["nodata"]}" '
                    f'onclick="toggleDetail({row_idx})" style="cursor:pointer" title="Click to expand">'
                    f'<td class="col-fromstate"><b>{from_state}</b></td>'
                    f'<td class="col-fieldgroup">{field}</td>'
                    f'<td class="col-test"><b>{test_name}</b></td>'
                    f'{state_cells}'
                    f'<td class="num">{stats["states_covered"]}</td>'
                    f'<td class="num pass">{stats["pass_states"]}</td>'
                    f'<td class="num pass">{stats["pass"]}</td>'
                    f'<td class="num fail">{stats["fail"]}</td>'
                    f'<td class="num error">{stats["error"]}</td>'
                    f'<td class="num nodata">{stats["nodata"]}</td>'
                    f'<td class="{v_css}">{verdict}</td>'
                    f'</tr>\n'
                )

                # drill-down
                detail_html = ""
                for state in states_in_data:
                    runs_for_state = field_pair_runstate_runs[pair].get(state, {})
                    if not runs_for_state: continue
                    for rid, results_list in sorted(runs_for_state.items(), key=lambda x: str(run_lookup.get(x[0], {}))):
                        run_info = run_lookup.get(rid)
                        run_date = str(run_info.run_start_datetime)[:16] if run_info else "?"
                        p = sum(1 for r in results_list if r["status"] == "PASS")
                        fc = sum(1 for r in results_list if r["status"] == "FAIL")
                        e = sum(1 for r in results_list if r["status"] == "ERROR")
                        nd = sum(1 for r in results_list if r["status"] == "NO_DATA")
                        total = len(results_list)
                        detail_html += (
                            f'<tr style="background:#e8f4fd">'
                            f'<td colspan="2"><b>{state}</b></td>'
                            f'<td>Run: {rid[:8]}...</td>'
                            f'<td>{run_date}</td>'
                            f'<td><span class="pass">{p}P</span> <span class="fail">{fc}F</span> '
                            f'<span class="error">{e}E</span> <span class="nodata">{nd}ND</span> = {total}</td>'
                            f'</tr>\n'
                        )
                        for r in results_list:
                            d_css = {"PASS": "pass", "FAIL": "fail", "ERROR": "error", "NO_DATA": "nodata"}.get(r["status"], "")
                            detail_html += (
                                f'<tr>'
                                f'<td></td>'
                                f'<td>{test_name}</td>'
                                f'<td class="{d_css}">{r["status"]}</td>'
                                f'<td colspan="2">{r["message"]}</td>'
                                f'</tr>\n'
                            )
                if detail_html:
                    field_rows += (
                        f'<tr class="detail-row" id="detail-{row_idx}" data-parent="{row_idx}" style="display:none">'
                        f'<td colspan="{10 + len(states_in_data)}">'
                        f'<table style="width:100%;margin:4px 0;font-size:11px;background:#fafafa">'
                        f'<thead><tr><th>State</th><th>Test</th><th>Run / Status</th><th>Date</th><th>Detail</th></tr></thead>'
                        f'<tbody>{detail_html}</tbody>'
                        f'</table></td></tr>\n'
                    )
                row_idx += 1

            state_header = "".join(
                f'<th class="state-col-header" data-col-state="{s}" '
                f'style="writing-mode:vertical-lr;transform:rotate(180deg);min-width:{heatmap_cell_min_width_px}px;'
                f'padding:3px;font-size:10px;cursor:pointer" title="Sort by state {s}">{s}</th>'
                for s in states_in_data
            )
            state_filter_options = "".join(f'<option value="{s}">{s}</option>' for s in states_in_data)
            run_compare_options = "".join(
                f'<option value="{r.run_id}">{str(r.run_start_datetime)[:16]} | {r.state_under_test} | {r.run_id[:8]}</option>'
                for r in all_runs_list
            )
            import json as _json
            run_pair_status = {r.run_id: {} for r in all_runs_list}
            for _, row in all_results.iterrows():
                rid = row.get("run_id", "")
                field = str(row.get("test_field", "") or "").strip()
                fs = str(row.get("test_from_state", "") or "").strip()
                tn = str(row.get("test_name", "") or "").strip()
                st = row.get("status_upper", "") or ""
                if not (rid and field and fs and tn and st):
                    continue
                key = f"{fs}|{field}|{tn}"
                cur = run_pair_status.get(rid, {}).get(key, "")
                order = {"PASS": 4, "FAIL": 3, "ERROR": 2, "NO_DATA": 1, "": 0}
                if order.get(st, 0) > order.get(cur, 0):
                    run_pair_status[rid][key] = st
            run_pair_status_json = _json.dumps(run_pair_status).replace("</", "<\\/")
            run_meta_json = _json.dumps({r.run_id: {
                "state": r.state_under_test,
                "started": str(r.run_start_datetime)[:19]
            } for r in all_runs_list}).replace("</", "<\\/")

            state_toggle_checks = "".join(
                f'<label class="state-toggle" data-col-state="{s}">'
                f'<input type="checkbox" checked onchange="toggleStateCol(\'{s}\',this.checked)"> {s}'
                f'</label>'
                for s in states_in_data
            )

            html = f"""
            <style>
                .csr {{font-family:Arial,sans-serif;padding:8px}}
                .csr h2 {{color:#222;border-bottom:2px solid #333;padding-bottom:4px;margin:18px 0 8px 0;font-size:17px}}
                .csr table {{border-collapse:collapse;width:100%;margin-bottom:10px;font-size:12px}}
                .csr th {{background:#2c3e50;color:white;padding:3px 5px;text-align:left;position:sticky;top:0;user-select:none}}
                .csr td {{border:1px solid #ddd;padding:2px 5px}}
                .csr tr:nth-child(even) {{background:#f9f9f9}}
                .csr td.num {{text-align:right;font-variant-numeric:tabular-nums}}
                .csr .pass {{color:#27ae60;font-weight:bold}}
                .csr .fail {{color:#e74c3c;font-weight:bold}}
                .csr .nodata {{color:#f39c12;font-weight:bold}}
                .csr .error {{color:#7f8c8d;font-weight:bold}}
                .csr .hm-pass {{background:#d5f5e3;color:#27ae60;text-align:center;font-weight:bold;font-size:11px}}
                .csr .hm-fail {{background:#fadbd8;color:#e74c3c;text-align:center;font-weight:bold;font-size:11px}}
                .csr .hm-err  {{background:#eee;color:#7f8c8d;text-align:center;font-weight:bold;font-size:11px}}
                .csr .hm-nd   {{background:#fef9e7;color:#f39c12;text-align:center;font-size:11px}}
                .csr .hm-empty {{background:#f8f9fa}}
                .csr .v-pass {{background:#d5f5e3;color:#155724;font-weight:bold;text-align:center}}
                .csr .v-fail {{background:#fadbd8;color:#721c24;font-weight:bold;text-align:center}}
                .csr .v-err  {{background:#fdecea;color:#922b21;font-weight:bold;text-align:center}}
                .csr .v-nodata {{background:#fef9e7;color:#856404;font-weight:bold;text-align:center}}
                .csr .box {{display:inline-block;padding:10px 16px;margin:3px;border-radius:4px;color:white;font-size:14px;text-align:center}}
                .csr .box-pass {{background:#27ae60}}
                .csr .box-fail {{background:#e74c3c}}
                .csr .box-nd   {{background:#f39c12}}
                .csr .box-err  {{background:#7f8c8d}}
                .csr .box-total {{background:#2c3e50}}
                .csr .tp {{background:#ecf0f1;padding:8px 12px;border-left:4px solid #2c3e50;margin:8px 0;font-size:12px;line-height:1.4}}
                .csr .field-row:hover {{background:#e8f4fd !important}}
                .csr .detail-row td {{padding:0}}
                .csr .detail-row table td {{border:1px solid #eee;padding:2px 6px;word-break:break-word;white-space:normal;max-width:500px}}
                .csr .filter-bar {{background:#ecf0f1;padding:8px 10px;border-radius:4px;margin:8px 0;display:flex;gap:8px;align-items:center;flex-wrap:wrap;font-size:12px}}
                .csr .filter-bar input, .csr .filter-bar select {{padding:4px 7px;border:1px solid #bdc3c7;border-radius:3px;font-size:12px}}
                .csr .filter-bar input.num {{width:55px;text-align:right}}
                .csr .filter-bar input.text {{min-width:180px}}
                .csr .filter-bar label {{font-weight:bold;color:#2c3e50;margin-right:2px}}
                .csr .filter-bar button {{padding:4px 10px;background:#2c3e50;color:white;border:none;border-radius:3px;cursor:pointer;font-size:12px}}
                .csr .filter-bar button.alt {{background:#27ae60}}
                .csr .filter-bar button.danger {{background:#c0392b}}
                .csr .filter-bar .group {{border-left:1px dashed #95a5a6;padding-left:8px;margin-left:4px;display:flex;gap:6px;align-items:center;flex-wrap:wrap}}
                .csr .hidden {{display:none !important}}
                .csr #match-count, .csr #states-match-count {{color:#7f8c8d;font-size:11px;margin-left:4px}}
                .csr .state-toggles {{background:#f4f6f7;padding:6px 8px;border-radius:4px;margin:4px 0;display:flex;flex-wrap:wrap;gap:4px 10px;font-size:11px;max-height:90px;overflow-y:auto}}
                .csr .state-toggles label {{white-space:nowrap;cursor:pointer}}
                .csr .state-toggles input {{margin-right:3px}}
                .csr th.sort-asc::after  {{content:" \u25B2"; font-size:9px; color:#bdc3c7}}
                .csr th.sort-desc::after {{content:" \u25BC"; font-size:9px; color:#bdc3c7}}
                .csr .pill {{display:inline-block;padding:1px 6px;border-radius:10px;font-size:10px;background:#bdc3c7;color:white;margin-right:4px}}
                .csr #field-matrix.solid-heat td.hm-pass,
                .csr #field-matrix.solid-heat td.hm-fail,
                .csr #field-matrix.solid-heat td.hm-err,
                .csr #field-matrix.solid-heat td.hm-nd {{color:transparent}}
                .csr #field-matrix.solid-heat td.hm-pass {{background:#27ae60}}
                .csr #field-matrix.solid-heat td.hm-fail {{background:#e74c3c}}
                .csr #field-matrix.solid-heat td.hm-err  {{background:#7f8c8d}}
                .csr #field-matrix.solid-heat td.hm-nd   {{background:#f39c12}}
                .csr tr.group-header td:hover {{background:#2c3e50}}
                .csr #cmp-out table {{margin-top:6px}}
                .csr .regress {{color:#c0392b;font-size:9px;margin-left:2px;font-weight:bold;vertical-align:super}}
                .csr #field-matrix th.col-fromstate, .csr #field-matrix td.col-fromstate {{width:110px;min-width:110px;max-width:110px;word-break:break-word;white-space:normal}}
                .csr #field-matrix th.col-fieldgroup, .csr #field-matrix td.col-fieldgroup {{width:140px;min-width:140px;max-width:140px;word-break:break-word;white-space:normal}}
                .csr #field-matrix th.col-test, .csr #field-matrix td.col-test {{width:210px;min-width:210px;max-width:210px;word-break:break-word;white-space:normal;font-size:11px}}
            </style>
            <div class="csr">
                <h2>Active Transformation and Default Mapping Test Coverage Report <span style="font-size:11px;color:#7f8c8d;font-weight:normal">{REPORT_VERSION}</span></h2>
                <p style="font-size:11px;color:#7f8c8d;margin-top:0">
                    <code>{runs_table}</code> / <code>{results_table}</code> &nbsp;|&nbsp;
                    {len(all_runs_list)} runs &nbsp;|&nbsp; {len(pair_stats)} (from_state, field) pairs &nbsp;|&nbsp; {len(states_in_data)} run-states
                </p>

                <div>
                    <div class="box box-pass">PASSED<br>(&ge;1 state)<br><b>{count_passed}</b> pairs</div>
                    <div class="box box-fail">FAILED<br><b>{count_failed}</b></div>
                    <div class="box box-err">ERROR<br><b>{count_error}</b></div>
                    <div class="box box-nd">NEEDS DATA<br><b>{count_needs_data}</b></div>
                    <div class="box box-total">TOTAL<br>pairs<br><b>{count_total_pairs}</b></div>
                </div>

                <div class="tp">
                    <b>Pass rule:</b> a (from_state, field) pair is PASSED if it passes in at least one run_state; other columns may still show FAIL / ERROR / NO_DATA.
                </div>

                <h2>Per-State Totals (by run_state)</h2>
                <div class="filter-bar">
                    <label>State contains:</label>
                    <input type="text" id="st-filter" class="text" placeholder="filter state name..." oninput="applyStateFilter()" />
                    <span class="group">
                        <label>Pass % &ge;</label>
                        <input type="number" class="num" id="st-minpct" min="0" max="100" value="" oninput="applyStateFilter()" />
                    </span>
                    <span class="group">
                        <label>Total &ge;</label>
                        <input type="number" class="num" id="st-mintotal" min="0" value="" oninput="applyStateFilter()" />
                    </span>
                    <button class="danger" onclick="clearStateFilter()">Clear</button>
                    <span id="states-match-count"></span>
                </div>
                <table id="state-totals">
                    <thead><tr>
                        <th data-sort-key="state">State (run_state)</th>
                        <th data-sort-key="total" style="text-align:right">Total tests</th>
                        <th data-sort-key="fields" style="text-align:right">Fields</th>
                        <th data-sort-key="pass" style="text-align:right">Pass</th>
                        <th data-sort-key="fail" style="text-align:right">Fail</th>
                        <th data-sort-key="error" style="text-align:right">Error</th>
                        <th data-sort-key="nodata" style="text-align:right">No Data</th>
                        <th data-sort-key="pct" style="text-align:right">Pass %</th>
                    </tr></thead>
                    <tbody>{state_totals_rows}</tbody>
                </table>

                <h2>From-State x Run-State Field Matrix</h2>

                <div class="filter-bar" style="background:#f4f6f7">
                    <label>Theme:</label>
                    <select id="tb-theme" onchange="applyTheme(this.value)">
                        <option value="ticks">Ticks &amp; Crosses</option>
                        <option value="solid">Solid Colour</option>
                    </select>
                    <span class="group">
                        <button class="alt" onclick="copyMarkdown()">Copy Matrix View to Clipboard</button>
                    </span>
                </div>

                <div class="filter-bar" style="background:#eef2f5">
                    <label>Show statuses:</label>
                    <label><input type="checkbox" class="status-filter-cb" data-status="PASS" checked onchange="applyFilters()"> Pass</label>
                    <label><input type="checkbox" class="status-filter-cb" data-status="FAIL" checked onchange="applyFilters()"> Fail</label>
                    <label><input type="checkbox" class="status-filter-cb" data-status="ERROR" checked onchange="applyFilters()"> Error</label>
                    <label><input type="checkbox" class="status-filter-cb" data-status="NO_DATA" checked onchange="applyFilters()"> No Data</label>
                    <span class="group">
                        <label>From state:</label>
                        <input type="text" id="filter-fromstate" class="text" placeholder="filter from_state..." oninput="applyFilters()" style="min-width:130px" />
                        <label>Field:</label>
                        <input type="text" id="filter-field" class="text" placeholder="filter field..." oninput="applyFilters()" style="min-width:130px" />
                        <label>Test:</label>
                        <input type="text" id="filter-test" class="text" placeholder="filter test..." oninput="applyFilters()" style="min-width:130px" />
                    </span>
                    <span class="group">
                        <button onclick="resetStatusFilters()" class="alt">Reset</button>
                    </span>
                    <span id="matrix-match-count" style="color:#7f8c8d;font-size:11px;margin-left:4px"></span>
                </div>


                <div class="state-toggles" id="state-toggles">
                    <span style="font-weight:bold;color:#2c3e50;margin-right:6px">Show tests from state:</span>
                    <button type="button" onclick="resetAllStateCols()" style="padding:2px 8px;font-size:11px;background:#2c3e50;color:white;border:none;border-radius:3px;cursor:pointer;margin-right:4px">Show all</button>
                    <button type="button" onclick="hideAllStateCols()" style="padding:2px 8px;font-size:11px;background:#7f8c8d;color:white;border:none;border-radius:3px;cursor:pointer;margin-right:8px">Hide all</button>
                    <button type="button" onclick="expandAllGroups()" style="padding:2px 8px;font-size:11px;background:#27ae60;color:white;border:none;border-radius:3px;cursor:pointer;margin-right:4px">Expand all groups</button>
                    <button type="button" onclick="collapseAllGroups()" style="padding:2px 8px;font-size:11px;background:#c0392b;color:white;border:none;border-radius:3px;cursor:pointer;margin-right:4px">Collapse all groups</button>
                    <button type="button" onclick="resetGroups()" style="padding:2px 8px;font-size:11px;background:#8e44ad;color:white;border:none;border-radius:3px;cursor:pointer;margin-right:10px">Reset Groups</button>
                    {state_toggle_checks}
                </div>

                <p style="font-size:11px;color:#7f8c8d;margin:4px 0">
                    Click any row to drill down. Click any column header to sort.
                    Symbols: &#10003; PASS &nbsp; &#10007; FAIL &nbsp; ! ERROR &nbsp; &mdash; NO_DATA.
                </p>

                <div style="overflow-x:auto;max-height:700px;overflow-y:auto">
                    <table id="field-matrix">
                        <thead><tr>
                            <th class="col-fromstate" data-sort-key="fromstate">From State</th>
                            <th class="col-fieldgroup" data-sort-key="field">FieldGroup</th>
                            <th class="col-test" data-sort-key="testname">Test</th>
                            {state_header}
                            <th data-sort-key="statescov">States</th>
                            <th data-sort-key="passstates">Pass states</th>
                            <th data-sort-key="pass-count">P</th>
                            <th data-sort-key="fail-count">F</th>
                            <th data-sort-key="error-count">E</th>
                            <th data-sort-key="nodata-count">ND</th>
                            <th data-sort-key="verdict">Verdict</th>
                        </tr></thead>
                        <tbody>{field_rows}</tbody>
                    </table>
                </div>

                <div style="margin:12px 0">
                    <button onclick="downloadReport()" style="background:#2c3e50;color:white;padding:8px 18px;border:none;border-radius:4px;cursor:pointer;font-size:13px;margin-right:6px">Download HTML Report</button>
                    <button onclick="downloadCSV()" style="background:#27ae60;color:white;padding:8px 18px;border:none;border-radius:4px;cursor:pointer;font-size:13px;margin-right:6px">Download CSV (Matrix)</button>
                    <button onclick="downloadFailuresCSV()" style="background:#c0392b;color:white;padding:8px 18px;border:none;border-radius:4px;cursor:pointer;font-size:13px">Download Failures + Errors Only</button>
                </div>

                <h2>Compare Two Runs (side-by-side)</h2>
                <div class="filter-bar" style="background:#fdf6e3">
                    <label>Run A:</label>
                    <select id="cmp-a"><option value="">-- select --</option>{run_compare_options}</select>
                    <label>Run B:</label>
                    <select id="cmp-b"><option value="">-- select --</option>{run_compare_options}</select>
                    <button class="alt" onclick="runCompare()">Compare</button>
                    <button class="danger" onclick="document.getElementById('cmp-out').innerHTML='';">Clear</button>
                    <span style="font-size:11px;color:#7f8c8d">Shows only pairs where the two runs differ. Does not affect the matrix above.</span>
                </div>
                <div id="cmp-out" style="font-size:11px"></div>

            </div>
            <script id="csr-script">
                var RUN_PAIR_STATUS = {run_pair_status_json};
                var RUN_META = {run_meta_json};
                var FIELD_MATRIX_COLS = {len(states_in_data) + 3};  /* 3 fixed (From/FieldGroup/Test) + state columns */

                function toggleDetail(idx) {{
                    var row = document.getElementById('detail-' + idx);
                    if (row) row.style.display = (row.style.display === 'none' ? 'table-row' : 'none');
                }}

                /* --- state column visibility --- */
                /* v12: toggles now filter ROWS by data-fromstate (tests FROM
                   that state), not columns. Name kept for compatibility with
                   existing onchange handlers and preset restore. */
                function toggleStateCol(stateName, visible) {{
                    var rows = document.querySelectorAll('#field-matrix tbody tr.field-row[data-fromstate="' + stateName + '"]');
                    rows.forEach(function(r) {{
                        if (visible) r.removeAttribute('data-hidden-by-fromstate');
                        else r.setAttribute('data-hidden-by-fromstate', '1');
                    }});
                    /* also hide/show the group header for this from_state */
                    var hdr = document.querySelector('tr.group-header[data-group-fromstate="' + stateName + '"]');
                    if (hdr) hdr.style.display = visible ? '' : 'none';
                    if (typeof applyFilters === 'function') applyFilters();
                }}
                function _setAllStateCols(visible) {{
                    var checks = document.querySelectorAll('#state-toggles input[type="checkbox"]');
                    checks.forEach(function(cb) {{
                        cb.checked = visible;
                        var m = (cb.getAttribute('onchange') || '').match(/'([^']+)'/);
                        if (m) toggleStateCol(m[1], visible);
                    }});
                }}
                function resetAllStateCols() {{ _setAllStateCols(true); }}
                function hideAllStateCols() {{ _setAllStateCols(false); }}

                /* --- matrix filters --- */
                function _getActiveStatuses() {{
                    var active = [];
                    document.querySelectorAll('.status-filter-cb').forEach(function(cb) {{
                        if (cb.checked) active.push(cb.getAttribute('data-status'));
                    }});
                    return active;
                }}
                function applyFilters() {{
                    var active = _getActiveStatuses();
                    var allChecked = active.length === 4;
                    var fsFilter = (document.getElementById('filter-fromstate') || {{}}).value || '';
                    fsFilter = fsFilter.trim().toLowerCase();
                    var fdFilter = (document.getElementById('filter-field') || {{}}).value || '';
                    fdFilter = fdFilter.trim().toLowerCase();
                    var tnFilter = (document.getElementById('filter-test') || {{}}).value || '';
                    tnFilter = tnFilter.trim().toLowerCase();
                    var statusClasses = {{'PASS':'hm-pass','FAIL':'hm-fail','ERROR':'hm-err','NO_DATA':'hm-nd'}};
                    var rows = document.querySelectorAll('#field-matrix tbody tr.field-row');
                    var shown = 0;
                    rows.forEach(function(row) {{
                        if (row.getAttribute('data-hidden-by-fromstate') === '1') {{
                            row.classList.add('hidden');
                            return;
                        }}
                        /* text filters on from_state and field */
                        var rowFs = (row.getAttribute('data-fromstate') || '').toLowerCase();
                        var rowFd = (row.getAttribute('data-field') || '').toLowerCase();
                        if (fsFilter && rowFs.indexOf(fsFilter) === -1) {{
                            row.classList.add('hidden');
                            return;
                        }}
                        if (fdFilter && rowFd.indexOf(fdFilter) === -1) {{
                            row.classList.add('hidden');
                            return;
                        }}
                        var rowTn = (row.getAttribute('data-testname') || '').toLowerCase();
                        if (tnFilter && rowTn.indexOf(tnFilter) === -1) {{
                            row.classList.add('hidden');
                            return;
                        }}
                        /* status filter: check actual cell classes in the row */
                        if (allChecked) {{
                            row.classList.remove('hidden');
                            shown++;
                            return;
                        }}
                        var statusToVerdict = {{'PASS':'PASSED','FAIL':'FAILED','ERROR':'ERROR','NO_DATA':'NEEDS DATA'}};
                        var rowVerdict = row.getAttribute('data-verdict') || '';
                        var hasMatch = false;
                        for (var i = 0; i < active.length; i++) {{
                            if (statusToVerdict[active[i]] === rowVerdict) {{ hasMatch = true; break; }}
                        }}
                        if (active.length === 0) hasMatch = false;
                        if (hasMatch) {{
                            row.classList.remove('hidden');
                            shown++;
                        }} else {{
                            row.classList.add('hidden');
                        }}
                    }});
                    /* hide detail rows for hidden parents */
                    document.querySelectorAll('#field-matrix tbody tr.detail-row').forEach(function(d) {{
                        var parentIdx = d.getAttribute('data-parent');
                        var parent = null;
                        rows.forEach(function(r) {{
                            if (r.getAttribute('onclick') && r.getAttribute('onclick').indexOf('(' + parentIdx + ')') !== -1) parent = r;
                        }});
                        if (parent && parent.classList.contains('hidden')) d.style.display = 'none';
                    }});
                    var mc = document.getElementById('matrix-match-count');
                    if (mc) mc.textContent = shown + ' of ' + rows.length + ' rows';
                }}
                function clearFilters() {{ resetStatusFilters(); }}
                function resetStatusFilters() {{
                    document.querySelectorAll('.status-filter-cb').forEach(function(cb) {{ cb.checked = true; }});
                    var ffs = document.getElementById('filter-fromstate'); if (ffs) ffs.value = '';
                    var ffd = document.getElementById('filter-field'); if (ffd) ffd.value = '';
                    var ftn = document.getElementById('filter-test'); if (ftn) ftn.value = '';
                    applyFilters();
                }}

                /* --- per-state totals filters --- */
                function applyStateFilter() {{
                    var text = (document.getElementById('st-filter').value || '').trim().toLowerCase();
                    var minPct = document.getElementById('st-minpct').value;
                    var minTotal = document.getElementById('st-mintotal').value;
                    var rows = document.querySelectorAll('#state-totals tbody tr');
                    var shown = 0;
                    rows.forEach(function(row) {{
                        var name = (row.getAttribute('data-state') || '').toLowerCase();
                        var pct  = parseFloat(row.getAttribute('data-pct') || '0');
                        var tot  = parseInt(row.getAttribute('data-total') || '0');
                        var ok = (!text || name.indexOf(text) !== -1)
                              && (minPct === '' || pct >= parseFloat(minPct))
                              && (minTotal === '' || tot >= parseInt(minTotal));
                        row.style.display = ok ? '' : 'none';
                        if (ok) shown++;
                    }});
                    document.getElementById('states-match-count').textContent = shown + ' of ' + rows.length + ' states';
                }}
                function clearStateFilter() {{
                    ['st-filter','st-minpct','st-mintotal'].forEach(function(id){{
                        document.getElementById(id).value = '';
                    }});
                    applyStateFilter();
                }}

                /* --- sortable headers --- */
                function sortTable(tableId, th, isNumeric) {{
                    var table = document.getElementById(tableId);
                    var tbody = table.querySelector('tbody');
                    var key = th.getAttribute('data-sort-key');
                    var current = th.getAttribute('data-sort-dir') || '';
                    var nextDir = current === 'asc' ? 'desc' : 'asc';
                    Array.from(table.querySelectorAll('th')).forEach(function(x){{
                        x.removeAttribute('data-sort-dir');
                        x.classList.remove('sort-asc','sort-desc');
                    }});
                    th.setAttribute('data-sort-dir', nextDir);
                    th.classList.add(nextDir === 'asc' ? 'sort-asc' : 'sort-desc');
                    var rows = Array.from(tbody.querySelectorAll('tr.field-row, tr[data-state]'));
                    rows.sort(function(a, b){{
                        var av = a.getAttribute('data-' + key);
                        var bv = b.getAttribute('data-' + key);
                        if (av === null) av = a.children[0] ? a.children[0].innerText : '';
                        if (bv === null) bv = b.children[0] ? b.children[0].innerText : '';
                        var an = parseFloat(av), bn = parseFloat(bv);
                        var numeric = !isNaN(an) && !isNaN(bn) && av !== '' && bv !== '';
                        var cmp = numeric ? (an - bn) : (String(av).localeCompare(String(bv)));
                        return nextDir === 'asc' ? cmp : -cmp;
                    }});
                    /* for matrix: keep detail rows attached to their parent */
                    if (tableId === 'field-matrix') {{
                        rows.forEach(function(r){{
                            var idx = r.getAttribute('onclick');
                            if (!idx) return;
                            idx = idx.match(/\\d+/)[0];
                            tbody.appendChild(r);
                            var det = document.getElementById('detail-' + idx);
                            if (det) tbody.appendChild(det);
                        }});
                    }} else {{
                        rows.forEach(function(r){{ tbody.appendChild(r); }});
                    }}
                }}
                document.querySelectorAll('#field-matrix thead th, #state-totals thead th').forEach(function(th){{
                    th.addEventListener('click', function(){{
                        var tableId = th.closest('table').id;
                        sortTable(tableId, th, true);
                    }});
                }});

                /* --- downloads --- */
                function downloadCSV() {{
                    var rows = document.querySelectorAll('#field-matrix thead tr, #field-matrix tbody tr.field-row');
                    var lines = [];
                    rows.forEach(function(r) {{
                        if (r.classList && r.classList.contains('hidden')) return;
                        var cells = r.querySelectorAll('th, td');
                        var vals = [];
                        cells.forEach(function(c) {{
                            if (c.style && c.style.display === 'none') return;
                            var txt = (c.innerText || c.textContent || '').replace(/\\s+/g,' ').trim();
                            if (txt === '\\u2713') txt = 'PASS';
                            else if (txt === '\\u2717') txt = 'FAIL';
                            else if (txt === '\\u2014') txt = 'NO_DATA';
                            else if (txt === '!') txt = 'ERROR';
                            if (txt.indexOf(',') !== -1 || txt.indexOf('"') !== -1 || txt.indexOf('\\n') !== -1) {{
                                txt = '"' + txt.replace(/"/g,'""') + '"';
                            }}
                            vals.push(txt);
                        }});
                        lines.push(vals.join(','));
                    }});
                    _saveCSV(lines.join('\\n'), 'cross_coverage_matrix.csv');
                }}
                function downloadFailuresCSV() {{
                    var rows = document.querySelectorAll('#field-matrix tbody tr.field-row');
                    var header = ['from_state','field_group','test_name','verdict','states_covered','pass_states','P','F','E','ND','failed_in','errored_in'];
                    var lines = [header.join(',')];
                    rows.forEach(function(r) {{
                        var v = r.getAttribute('data-verdict');
                        if (v === 'PASSED' || v === 'NEEDS DATA') return;
                        var vals = [
                            r.getAttribute('data-fromstate') || '',
                            r.getAttribute('data-field') || '',
                            r.getAttribute('data-testname') || '',
                            v,
                            r.getAttribute('data-statescov') || '0',
                            r.getAttribute('data-passstates') || '0',
                            r.getAttribute('data-pass-count') || '0',
                            r.getAttribute('data-fail-count') || '0',
                            r.getAttribute('data-error-count') || '0',
                            r.getAttribute('data-nodata-count') || '0',
                            r.getAttribute('data-fails-in') || '',
                            r.getAttribute('data-errors-in') || ''
                        ].map(function(x){{
                            if (x.indexOf(',') !== -1 || x.indexOf('"') !== -1) return '"' + x.replace(/"/g,'""') + '"';
                            return x;
                        }});
                        lines.push(vals.join(','));
                    }});
                    _saveCSV(lines.join('\\n'), 'cross_coverage_failures_errors.csv');
                }}
                function _saveCSV(csv, name) {{
                    var blob = new Blob([csv], {{type: 'text/csv;charset=utf-8'}});
                    var a = document.createElement('a');
                    a.href = URL.createObjectURL(blob);
                    a.download = name;
                    a.click();
                }}
                function downloadReport() {{
                    var content = document.querySelector('.csr').outerHTML;
                    var styles = document.querySelectorAll('style');
                    var styleText = '';
                    styles.forEach(function(s) {{ styleText += s.outerHTML; }});
                    var scriptEl = document.getElementById('csr-script');
                    var js = '<' + 'script id="csr-script">' +
                        scriptEl.innerHTML +
                        '<' + '/script>';
                    var full = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Active Transformation and Default Mapping Test Coverage Report</title>' + styleText + '</head><body>' + content + js + '</body></html>';
                    var blob = new Blob([full], {{type: 'text/html'}});
                    var a = document.createElement('a');
                    a.href = URL.createObjectURL(blob);
                    a.download = 'active_transformation_coverage_report_' + '{REPORT_VERSION}'.replace(/[^a-zA-Z0-9]+/g,'_') + '.html';
                    a.click();
                }}

                /* --- Group by from_state --- */
                function toggleGroupByFromState(on) {{
                    var tbody = document.querySelector('#field-matrix tbody');
                    if (!on) {{
                        tbody.querySelectorAll('tr.group-header').forEach(function(h){{ h.remove(); }});
                        tbody.querySelectorAll('tr.field-row, tr.detail-row').forEach(function(r){{ r.style.display = ''; }});
                        applyFilters();
                        return;
                    }}
                    tbody.querySelectorAll('tr.group-header').forEach(function(h){{ h.remove(); }});
                    var rows = Array.from(tbody.querySelectorAll('tr.field-row'));
                    var lastFs = null;
                    rows.forEach(function(r){{
                        var fs = r.getAttribute('data-fromstate') || '';
                        if (fs !== lastFs) {{
                            var hdr = document.createElement('tr');
                            hdr.className = 'group-header';
                            hdr.setAttribute('data-group-fromstate', fs);
                            var td = document.createElement('td');
                            td.colSpan = 999;
                            td.style.background = '#34495e';
                            td.style.color = 'white';
                            td.style.fontWeight = 'bold';
                            td.style.padding = '5px 8px';
                            td.style.cursor = 'pointer';
                            td.innerHTML = '&#x25BC; ' + fs;
                            td.onclick = (function(f){{ return function(){{ toggleGroup(f); }}; }})(fs);
                            hdr.appendChild(td);
                            tbody.insertBefore(hdr, r);
                            lastFs = fs;
                        }}
                    }});
                }}
                function toggleGroup(fs) {{
                    var hdr = document.querySelector('tr.group-header[data-group-fromstate="' + fs + '"]');
                    if (!hdr) return;
                    var td = hdr.querySelector('td');
                    var collapsed = td.innerHTML.indexOf('\u25BA') !== -1;
                    td.innerHTML = (collapsed ? '&#x25BC; ' : '&#x25BA; ') + fs;
                    var sib = hdr.nextElementSibling;
                    while (sib && !sib.classList.contains('group-header')) {{
                        if (sib.classList.contains('field-row') || sib.classList.contains('detail-row')) {{
                            sib.style.display = collapsed ? '' : 'none';
                        }}
                        sib = sib.nextElementSibling;
                    }}
                }}

                function expandAllGroups() {{
                    document.querySelectorAll('tr.group-header').forEach(function(hdr) {{
                        var td = hdr.querySelector('td');
                        var fs = hdr.getAttribute('data-group-fromstate');
                        td.innerHTML = '&#x25BC; ' + fs;
                        var sib = hdr.nextElementSibling;
                        while (sib && !sib.classList.contains('group-header')) {{
                            if (sib.classList.contains('field-row') || sib.classList.contains('detail-row')) {{
                                sib.style.display = '';
                            }}
                            sib = sib.nextElementSibling;
                        }}
                    }});
                    applyFilters();
                }}
                function collapseAllGroups() {{
                    document.querySelectorAll('tr.group-header').forEach(function(hdr) {{
                        var td = hdr.querySelector('td');
                        var fs = hdr.getAttribute('data-group-fromstate');
                        td.innerHTML = '&#x25BA; ' + fs;
                        var sib = hdr.nextElementSibling;
                        while (sib && !sib.classList.contains('group-header')) {{
                            if (sib.classList.contains('field-row') || sib.classList.contains('detail-row')) {{
                                sib.style.display = 'none';
                            }}
                            sib = sib.nextElementSibling;
                        }}
                    }});
                }}

                function resetGroups() {{
                    /* expand all groups, close all detail rows */
                    document.querySelectorAll('tr.group-header').forEach(function(hdr) {{
                        var td = hdr.querySelector('td');
                        var fs = hdr.getAttribute('data-group-fromstate');
                        td.innerHTML = '&#x25BC; ' + fs;
                        var sib = hdr.nextElementSibling;
                        while (sib && !sib.classList.contains('group-header')) {{
                            if (sib.classList.contains('field-row')) sib.style.display = '';
                            if (sib.classList.contains('detail-row')) sib.style.display = 'none';
                            sib = sib.nextElementSibling;
                        }}
                    }});
                    applyFilters();
                }}

                /* --- Solid heatmap style toggle --- */
                function applyTheme(theme) {{
                    var tbl = document.getElementById('field-matrix');
                    tbl.classList.remove('solid-heat','theme-ticks');
                    if (theme === 'solid') tbl.classList.add('solid-heat');
                    else tbl.classList.add('theme-ticks');
                }}

                /* --- Copy visible matrix as Markdown --- */
                function copyMarkdown() {{
                    var thead = document.querySelectorAll('#field-matrix thead th');
                    var headers = [];
                    thead.forEach(function(th){{
                        if (th.style && th.style.display === 'none') return;
                        headers.push((th.innerText || '').trim().replace(/\\|/g,'\\\\|'));
                    }});
                    var lines = [];
                    lines.push('| ' + headers.join(' | ') + ' |');
                    lines.push('|' + headers.map(function(){{return '---';}}).join('|') + '|');
                    var rows = document.querySelectorAll('#field-matrix tbody tr.field-row');
                    var rcount = 0;
                    rows.forEach(function(r){{
                        if (r.classList.contains('hidden')) return;
                        var cells = r.querySelectorAll('th, td');
                        var vals = [];
                        cells.forEach(function(c){{
                            if (c.style && c.style.display === 'none') return;
                            var t = (c.innerText || '').trim();
                            if (t === '\u2713') t = 'PASS';
                            else if (t === '\u2717') t = 'FAIL';
                            else if (t === '\u2014') t = 'NO_DATA';
                            else if (t === '!') t = 'ERROR';
                            vals.push(t.replace(/\\|/g,'\\\\|'));
                        }});
                        lines.push('| ' + vals.join(' | ') + ' |');
                        rcount++;
                    }});
                    var md = lines.join('\\n');
                    // Show in a visible textarea — Databricks sandboxed iframe
                    // usually blocks both clipboard API and alert(), so give the
                    // user something they can manually Ctrl-A / Ctrl-C from.
                    var existing = document.getElementById('md-out-box');
                    if (existing) existing.remove();
                    var box = document.createElement('div');
                    box.id = 'md-out-box';
                    box.style.cssText = 'margin:10px 0;padding:10px;border:2px solid #27ae60;background:#eafaf1;border-radius:4px;font-family:Arial';
                    var hint = document.createElement('div');
                    hint.style.cssText = 'font-size:12px;color:#2c3e50;margin-bottom:4px;font-weight:bold';
                    hint.textContent = 'Markdown for ' + rcount + ' visible rows (click textarea, Ctrl-A, Ctrl-C) -- or try the Copy button below:';
                    var ta = document.createElement('textarea');
                    ta.style.cssText = 'width:100%;height:200px;font-family:monospace;font-size:11px;padding:6px';
                    ta.value = md;
                    var btnrow = document.createElement('div');
                    btnrow.style.cssText = 'margin-top:6px;display:flex;gap:6px';
                    var copyBtn = document.createElement('button');
                    copyBtn.textContent = 'Copy (tries clipboard API)';
                    copyBtn.style.cssText = 'padding:6px 12px;background:#27ae60;color:white;border:none;border-radius:3px;cursor:pointer';
                    copyBtn.onclick = function() {{
                        ta.select();
                        var ok = false;
                        try {{ ok = document.execCommand('copy'); }} catch(e) {{}}
                        if (!ok && navigator.clipboard && navigator.clipboard.writeText) {{
                            navigator.clipboard.writeText(md).then(
                                function(){{ copyBtn.textContent = 'Copied ' + rcount + ' rows'; }},
                                function(){{ copyBtn.textContent = 'Select manually + Ctrl-C'; }}
                            );
                        }} else {{
                            copyBtn.textContent = ok ? ('Copied ' + rcount + ' rows') : 'Select manually + Ctrl-C';
                        }}
                    }};
                    var closeBtn = document.createElement('button');
                    closeBtn.textContent = 'Close';
                    closeBtn.style.cssText = 'padding:6px 12px;background:#7f8c8d;color:white;border:none;border-radius:3px;cursor:pointer';
                    closeBtn.onclick = function() {{ box.remove(); }};
                    btnrow.appendChild(copyBtn);
                    btnrow.appendChild(closeBtn);
                    box.appendChild(hint);
                    box.appendChild(ta);
                    box.appendChild(btnrow);
                    // Insert right after the toolbar (or at top of .csr)
                    var csr = document.querySelector('.csr');
                    csr.insertBefore(box, csr.firstChild.nextSibling);
                    ta.focus();
                    ta.select();
                }}
                function _fallbackCopy(txt, n) {{
                    var ta = document.createElement('textarea');
                    ta.value = txt; document.body.appendChild(ta);
                    ta.select(); document.execCommand('copy'); ta.remove();
                    alert('Copied ' + n + ' rows as markdown');
                }}

                /* --- Filter presets (localStorage) --- */
                var PRESET_KEY = 'atcr_presets_v1';
                function _getPresets() {{
                    try {{ return JSON.parse(localStorage.getItem(PRESET_KEY) || '{{}}'); }}
                    catch(e) {{ return {{}}; }}
                }}
                function _setPresets(p) {{
                    try {{ localStorage.setItem(PRESET_KEY, JSON.stringify(p)); }} catch(e) {{}}
                }}
                function _refreshPresetDropdown() {{
                    var sel = document.getElementById('preset-list');
                    if (!sel) return;
                    var p = _getPresets();
                    sel.innerHTML = '<option value="">-- load preset --</option>';
                    Object.keys(p).sort().forEach(function(n){{
                        var o = document.createElement('option');
                        o.value = n; o.textContent = n;
                        sel.appendChild(o);
                    }});
                }}
                function _snapshotFilters() {{
                    var snap = {{}};
                    snap['hidden-cols'] = Array.from(document.querySelectorAll('#state-toggles input[type="checkbox"]'))
                        .filter(function(cb){{ return !cb.checked; }})
                        .map(function(cb){{ var m = (cb.getAttribute('onchange')||'').match(/'([^']+)'/); return m?m[1]:''; }})
                        .filter(Boolean);
                    snap['theme'] = (document.getElementById('tb-theme') || {{value:'ticks'}}).value;
                    return snap;
                }}
                function _restoreFilters(snap) {{
                    if (!snap) return;
                    var hidden = snap['hidden-cols'] || [];
                    document.querySelectorAll('#state-toggles input[type="checkbox"]').forEach(function(cb){{
                        var m = (cb.getAttribute('onchange')||'').match(/'([^']+)'/);
                        if (!m) return;
                        var shouldShow = hidden.indexOf(m[1]) === -1;
                        cb.checked = shouldShow;
                        toggleStateCol(m[1], shouldShow);
                    }});
                    if (snap['theme']) {{
                        var sel = document.getElementById('tb-theme');
                        if (sel) {{ sel.value = snap['theme']; applyTheme(snap['theme']); }}
                    }}
                    toggleGroupByFromState(true);
                    applyFilters();
                }}
                function savePreset() {{
                    var name = (document.getElementById('preset-name').value || '').trim();
                    if (!name) {{ alert('Enter a name for this preset'); return; }}
                    var p = _getPresets();
                    p[name] = _snapshotFilters();
                    _setPresets(p);
                    _refreshPresetDropdown();
                    alert('Saved preset: ' + name);
                }}
                function loadPreset(name) {{
                    if (!name) return;
                    var p = _getPresets();
                    if (p[name]) _restoreFilters(p[name]);
                }}
                function deletePreset() {{
                    var sel = document.getElementById('preset-list');
                    var n = sel.value;
                    if (!n) {{ alert('Pick a preset from the dropdown first'); return; }}
                    var p = _getPresets();
                    delete p[n];
                    _setPresets(p);
                    _refreshPresetDropdown();
                }}

                /* --- Compare two runs --- */
                function runCompare() {{
                    var a = document.getElementById('cmp-a').value;
                    var b = document.getElementById('cmp-b').value;
                    var out = document.getElementById('cmp-out');
                    if (!a || !b) {{ out.innerHTML = '<p style="color:#c0392b">Pick both Run A and Run B.</p>'; return; }}
                    if (a === b) {{ out.innerHTML = '<p style="color:#c0392b">Run A and Run B are the same run.</p>'; return; }}
                    var ma = RUN_PAIR_STATUS[a] || {{}};
                    var mb = RUN_PAIR_STATUS[b] || {{}};
                    var keys = {{}};
                    Object.keys(ma).forEach(function(k){{ keys[k] = true; }});
                    Object.keys(mb).forEach(function(k){{ keys[k] = true; }});
                    var rows = [];
                    Object.keys(keys).sort().forEach(function(k){{
                        var sa = ma[k] || '';
                        var sb = mb[k] || '';
                        if (sa === sb) return;
                        var parts = k.split('|');
                        rows.push({{ from: parts[0], field: parts[1], test: parts[2], a: sa, b: sb }});
                    }});
                    var metaA = RUN_META[a] || {{}};
                    var metaB = RUN_META[b] || {{}};
                    if (rows.length === 0) {{
                        out.innerHTML = '<p style="color:#27ae60">Runs are equivalent for every (from_state, field) pair.</p>';
                        return;
                    }}
                    var html = '<p><b>' + rows.length + '</b> differing pairs. ' +
                        'A = ' + (metaA.state||'?') + ' @ ' + (metaA.started||'?') + '. ' +
                        'B = ' + (metaB.state||'?') + ' @ ' + (metaB.started||'?') + '.</p>';
                    html += '<table style="font-size:11px"><thead><tr><th>From</th><th>FieldGroup</th><th>Test</th><th>Run A</th><th>Run B</th><th>Delta</th></tr></thead><tbody>';
                    function _cls(s){{ return ({{PASS:'pass',FAIL:'fail',ERROR:'error',NO_DATA:'nodata'}})[s] || ''; }}
                    rows.forEach(function(r){{
                        var delta;
                        if (r.a === 'PASS' && r.b !== 'PASS') delta = 'regressed';
                        else if (r.a !== 'PASS' && r.b === 'PASS') delta = 'fixed';
                        else delta = 'changed';
                        html += '<tr><td>' + r.from + '</td><td>' + r.field + '</td><td><b>' + r.test + '</b></td>' +
                            '<td class="' + _cls(r.a) + '">' + (r.a||'-') + '</td>' +
                            '<td class="' + _cls(r.b) + '">' + (r.b||'-') + '</td>' +
                            '<td>' + delta + '</td></tr>';
                    }});
                    html += '</tbody></table>';
                    out.innerHTML = html;
                }}

                applyTheme('ticks');
                toggleGroupByFromState(true);
                applyFilters();
                applyStateFilter();
            </script>
            """
            displayHTML(html)
