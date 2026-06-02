import os
import re
import csv
import json
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

USERNAME = os.environ.get("IDAM_USERNAME")
PASSWORD = os.environ.get("IDAM_PASSWORD")
BASE = os.environ.get("MANAGE_CASE_BASE", "https://manage-case.aat.platform.hmcts.net").rstrip("/")
HEADED = os.environ.get("HEADED", "true").strip().lower() == "true"
DEBUG = os.environ.get("DEBUG", "").strip().lower() == "true"
_debug_done = set()

HERE = os.path.dirname(os.path.abspath(__file__))
EXPECTED_FILE = os.path.join(HERE, "expected_links.json")
EVIDENCE_DIR = os.path.join(HERE, "evidence")
RESULTS_DIR = os.path.join(HERE, "results")

CCD_REF_RE = re.compile(r"\b\d{16}\b")


def fmt_ref(ref):
    return re.sub(r"\D", "", str(ref))


def login(page):
    page.goto(BASE, wait_until="load")
    page.fill("#username", USERNAME)
    page.fill("#password", PASSWORD)
    submit = page.locator("input[type=submit], button[type=submit]").first
    if submit.count():
        submit.click()
    else:
        page.get_by_role("button", name=re.compile(r"sign in|continue|submit", re.I)).click()
    page.wait_for_url(re.compile(r"manage-case\."), timeout=60000)
    print("Logged in.")


def open_case(page, ref):
    page.goto(BASE, wait_until="domcontentloaded")
    box = page.locator("#exuiCaseReferenceSearch")
    box.wait_for(state="visible", timeout=30000)
    box.fill(ref)
    page.get_by_role("button", name="Find").click()
    page.get_by_role("tab", name=re.compile(r"linked cases", re.I)).first.wait_for(
        state="visible", timeout=45000)


TABLE_SELECTORS = {
    "to": [
        "ccd-linked-cases-to-table table",
        'table[aria-describedby*="cases linked TO" i]',
    ],
    "from": [
        "ccd-linked-cases-from-table table",
        'table[aria-describedby*="cases linked FROM" i]',
        'table[aria-describedby*="linked FROM" i]',
    ],
}


def _resolve_table(page, direction, wait_ms):
    for sel in TABLE_SELECTORS[direction]:
        loc = page.locator(sel).first
        try:
            loc.wait_for(state="visible", timeout=wait_ms)
            return loc
        except Exception:
            continue
    return None


def _ensure_revealed(page):
    toggles = page.locator("#show-hide-link")
    for i in range(toggles.count()):
        t = toggles.nth(i)
        try:
            if t.is_visible() and t.inner_text().strip().lower().startswith("show"):
                t.click()
                page.wait_for_timeout(400)
        except Exception:
            pass


def _debug_dump(page):
    print("    ---- DEBUG: Linked Cases tab ----")
    try:
        print("    [debug] tab names:", page.get_by_role("tab").all_inner_texts())
    except Exception as e:
        print("    [debug] tab names error:", e)
    try:
        dby = page.locator("table[aria-describedby]").evaluate_all(
            "els => els.map(e => e.getAttribute('aria-describedby'))")
        print("    [debug] table aria-describedby:", dby)
    except Exception as e:
        print("    [debug] aria-describedby error:", e)
    for tag in ("ccd-linked-cases-to-table", "ccd-linked-cases-from-table"):
        loc = page.locator(tag)
        n = loc.count()
        vis = loc.first.is_visible() if n else False
        print(f"    [debug] {tag}: count={n} visible={vis}")
        if n:
            try:
                rows = loc.locator("table tbody tr").count()
                txt = loc.first.inner_text()[:300].replace("\n", " | ")
                print(f"    [debug] {tag}: tbody rows={rows} text='{txt}'")
            except Exception as e:
                print(f"    [debug] {tag} read error:", e)
    print("    ---------------------------------")


def scrape_linked_rows(page, direction="to", wait_ms=12000):
    global _debug_done
    page.get_by_role("tab", name=re.compile(r"linked\s*cases", re.I)).first.click()
    try:
        page.locator("ccd-linked-cases-to-table, ccd-linked-cases-from-table").first.wait_for(
            state="attached", timeout=wait_ms)
    except Exception:
        pass
    page.wait_for_timeout(800)

    _ensure_revealed(page)

    if DEBUG and direction not in _debug_done:
        _debug_done.add(direction)
        _debug_dump(page)

    table = _resolve_table(page, direction, wait_ms)
    if table is None:
        return []

    try:
        table.locator("tbody tr td a").first.wait_for(state="attached", timeout=wait_ms)
    except Exception:
        pass

    rows = []
    tr = table.locator("tbody tr.govuk-table__row")
    for i in range(tr.count()):
        row = tr.nth(i)
        link = row.locator("td a")
        href = (link.first.get_attribute("href") or "") if link.count() else ""
        m = re.search(r"(\d{16})", href)
        if m:
            ref = m.group(1)
        else:
            t = re.search(r"\d{4}-\d{4}-\d{4}-\d{4}", row.inner_text())
            ref = fmt_ref(t.group(0)) if t else ""
        if not ref:
            continue
        cells = row.locator("td")
        reason = cells.nth(cells.count() - 1).inner_text().strip()
        rows.append({"ref": ref, "reason": reason})
    return rows


def _capture_evidence(page, path):
    try:
        panel = page.locator(".mat-tab-body-active").first
        if panel.count() and panel.is_visible():
            panel.screenshot(path=path)
            return
    except Exception:
        pass
    page.screenshot(path=path, full_page=True)


def check_case(page, case):
    ref = fmt_ref(case["ccd_ref"])
    aria = case.get("aria_case_no")
    kind = case.get("kind", "positive")
    direction = case.get("direction", "from" if kind == "incoming" else "to")

    exp_reasons = {}
    for l in case.get("expected_links", []):
        r = fmt_ref(l["to_ccd_ref"])
        exp_reasons.setdefault(r, set())
        if l.get("reason_label"):
            exp_reasons[r].add(l["reason_label"].strip())
    expected = set(exp_reasons)
    base = {"ccd_ref": ref, "aria_case_no": aria, "kind": kind,
            "direction": direction, "expected": len(expected)}

    try:
        open_case(page, ref)
    except Exception as e:
        return {**base, "status": "ERROR", "shown": 0, "missing": sorted(expected),
                "extra": [], "reason_issues": [], "message": f"could not open case: {e}"}

    try:
        rows = scrape_linked_rows(page, direction)
    except Exception as e:
        return {**base, "status": "ERROR", "shown": 0, "missing": sorted(expected),
                "extra": [], "reason_issues": [], "message": f"scrape failed: {e}"}

    shown_reasons = {}
    for row in rows:
        shown_reasons[row["ref"]] = (shown_reasons.get(row["ref"], "") + " " + row["reason"]).strip()
    shown = set(shown_reasons)

    missing = sorted(expected - shown)
    extra = sorted(shown - expected)

    reason_issues = []
    for r in sorted(expected & shown):
        ui_text = shown_reasons[r].lower()
        for label in sorted(exp_reasons[r]):
            if label and label.lower() not in ui_text:
                reason_issues.append(f"{r}:'{label}' not shown (ui='{shown_reasons[r]}')")

    ok = not missing and not extra and not reason_issues
    status = "PASS" if ok else "FAIL"

    os.makedirs(EVIDENCE_DIR, exist_ok=True)
    evidence_file = f"{ref}_{kind}_{status}.png"
    try:
        _capture_evidence(page, os.path.join(EVIDENCE_DIR, evidence_file))
    except Exception:
        evidence_file = ""

    parts = []
    if missing:       parts.append(f"missing={missing}")
    if extra:         parts.append(f"extra={extra}")
    if reason_issues: parts.append(f"reasons={reason_issues}")

    if ok and kind == "negative_422":
        message = "no links shown - 422 rejection confirmed"
    elif ok and kind == "incoming":
        message = "incoming link(s) confirmed on the linked-to case"
    elif ok:
        message = ""
    else:
        message = "; ".join(parts)

    return {**base, "status": status, "shown": len(shown),
            "missing": missing, "extra": extra, "reason_issues": reason_issues,
            "evidence": evidence_file, "message": message}


def main():
    if not USERNAME or not PASSWORD:
        sys.exit("IDAM_USERNAME / IDAM_PASSWORD not set - copy .env.example to .env and fill them in.")
    if not os.path.exists(EXPECTED_FILE):
        sys.exit(f"{EXPECTED_FILE} not found - run build_expected_links.ipynb and drop the JSON here.")

    with open(EXPECTED_FILE, encoding="utf-8") as f:
        expected = json.load(f)
    cases = expected["cases"]
    print(f"Loaded {len(cases)} cases from expected_links.json (env={expected.get('env')})")

    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADED)
        ctx = browser.new_context()
        page = ctx.new_page()
        login(page)
        for case in cases:
            r = check_case(page, case)
            flag = {"PASS": "PASS", "FAIL": "FAIL", "ERROR": "ERR "}[r["status"]]
            print(f"  [{flag}] {r['ccd_ref']} ({r.get('kind','positive')}): "
                  f"expected={r['expected']} shown={r['shown']} {r['message']}")
            results.append(r)
        browser.close()

    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    errored = sum(1 for r in results if r["status"] == "ERROR")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    out_csv = os.path.join(RESULTS_DIR, f"xui_link_check_{ts}.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ccd_ref", "aria_case_no", "kind", "direction", "status",
                    "expected", "shown", "missing", "extra", "reason_issues",
                    "evidence", "message"])
        for r in results:
            w.writerow([r["ccd_ref"], r.get("aria_case_no", ""), r.get("kind", "positive"),
                        r.get("direction", "to"), r["status"], r["expected"], r["shown"],
                        "|".join(r["missing"]), "|".join(r["extra"]),
                        "|".join(r.get("reason_issues", [])), r.get("evidence", ""),
                        r["message"]])

    summary = {
        "run_ts": ts,
        "run_status": "FAILED" if (failed or errored) else "PASSED",
        "env": expected.get("env"),
        "base": BASE,
        "total": len(results),
        "passed": passed, "failed": failed, "errored": errored,
        "results": results,
    }
    out_json = os.path.join(RESULTS_DIR, "xui_link_results.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(f"\nDone. PASS={passed} FAIL={failed} ERROR={errored}")
    print(f"  CSV     : {out_csv}")
    print(f"  JSON    : {out_json}  (upload this for load_xui_results.ipynb)")
    print(f"  Evidence: {EVIDENCE_DIR}  ({len(results)} screenshots)")


if __name__ == "__main__":
    main()
