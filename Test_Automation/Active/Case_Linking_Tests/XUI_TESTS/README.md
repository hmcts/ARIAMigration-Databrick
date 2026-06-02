# XUI / CCD front-end case-link checks

Local Playwright checks that confirm the case links we created are actually showing
in the Manage Case (XUI / ExpertUI) front end. Run on your own machine against AAT.
Results round-trip back into the central audit tables for evidence.

## Flow

```
build_expected_links.ipynb        check_case_links.py            load_xui_results.ipynb
   (Databricks)                       (local)                       (Databricks)
   picks positive / 422 /     ->   logs in, opens each case,  ->   reads xui_link_results.json
   incoming cases + their          scrapes Linked Cases tab,       -> test_automation_runs2
   expected linked refs            compares to expected,           -> test_automation_results2
   -> expected_links.json          writes CSV + JSON               -> case_link_xui_evidence
```

## What it proves

| kind | direction | proves |
|------|-----------|--------|
| **positive**     | outgoing | the case's links ("This case is linked to") are all shown with the right reasons |
| **negative_422** | outgoing | a 422-rejected case shows **no** links - the failure is real, visible, not just an audit row |
| **incoming**     | reverse  | a linked-to case shows the link in "Cases that link to this case" - the one-directional link is visible on the other side |

## One-time setup

```
pip install -r requirements.txt
python -m playwright install chromium
cp .env.example .env          # then edit .env with your IDAM email + password
```

`.env` is git-ignored. Credentials are read at runtime only — never hardcoded or committed.

## 1. Generate the expected file (Databricks)

1. Run `build_expected_links.ipynb` (same Spark config as the other case-linking notebooks).
2. It picks positive / negative_422 / incoming cases, prints and writes `expected_links.json`.
3. Download it and drop it in this folder.

## 2. Run the checks (local)

```
python check_case_links.py
```

- Opens a real browser (set `HEADED=false` in `.env` for headless).
- Logs into Manage Case via IDAM (no 2FA).
- For each case: searches the ref in the top search box, opens it, clicks the Linked Cases
  tab, scrapes the relevant table, compares to expected.
- Writes `results/xui_link_check_<ts>.csv` (human) + `results/xui_link_results.json`
  (for the loader).
- Saves an **evidence screenshot of the Linked Cases tab for every case** (pass or fail)
  in `evidence/`, named `<ref>_<kind>_<status>.png`. The filename is recorded against each
  case in the CSV/JSON so it's traceable in the audit.

## 3. Load results back for evidence (Databricks)

1. Upload `results/xui_link_results.json` to your Workspace Results folder
   (`/Workspace/Users/<you>/Results/Case_Linking_Tests/`).
2. Run `load_xui_results.ipynb`. It writes:
   - a run row to `test_automation_runs2` (`state_under_test = caseLinking_xui`),
   - one result row per case to `test_automation_results2`,
   - a detailed `case_link_xui_evidence` table (expected/shown, missing/extra, reasons).

## Status of selectors

Confirmed from the real DOM (all green against AAT/stg):
- **IDAM login** — `#username`, `#password`.
- **Open case** — `#exuiCaseReferenceSearch` search box + the **Find** button.
- **Linked Cases tab** — Angular Material tab (`role=tab`, text "Linked Cases").
- **Outgoing table** — `ccd-linked-cases-to-table table`; ref from each row's `<a href=.../<16 digits>>`, reason from the last cell.
- **Incoming table** — `ccd-linked-cases-from-table` (with an `aria-describedby*="FROM"` fallback).
- **Show/Hide toggle** — `#show-hide-link`; the "Cases linked from" section is collapsed by
  default, so the scrape clicks it to expand (only when it reads "Show").
- **Async populate** — the scrape waits for the first `tbody td a` before reading, so it
  doesn't read an empty table shell.

Still inferred:
- **Login submit button** — covered by an `input/button[type=submit]` + "Sign in" fallback.

## What it checks

- Every expected linked CCD reference is shown on the case (missing / extra flagged).
- The reason label per linked case (e.g. "Shared evidence").
- Negative 422 cases show no links; incoming cases show the link on the linked-to side.
