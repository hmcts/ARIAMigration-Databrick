from models.test_result import TestResult
from collections import defaultdict

# Patterns that mean "no data to test" — reclassify FAIL → NO_DATA
NO_DATA_PATTERNS = [
    "NO RECORDS TO TEST",
    "No matching test data",
    "does not exist in the payload",
    "does not exist in the columns",
    "does not exist in the",
    "No records found",
    "no data available",
    "No data to test",
    "no data exists for",
    "UNRESOLVED_COLUMN",
    "Failed to Setup Data",
]

# Patterns that mean "code error" — reclassify FAIL → ERROR
ERROR_PATTERNS = [
    "is not defined",
    "Test crashed:",
]

# Tests that produce variable results (1 per case) — aggregate into 1 summary
# Add test_name here if it returns a variable number of results
AGGREGATE_TESTS = [
    "hearing_centre_field_test",
]


def classify_result(result):
    """Reclassify a TestResult based on its message."""
    if not hasattr(result, 'status') or not hasattr(result, 'message'):
        return result
    status = str(result.status or "").upper().strip()
    message = str(result.message or "")
    if status == "PASS":
        return result
    msg_upper = message.upper()
    for pattern in NO_DATA_PATTERNS:
        if pattern.upper() in msg_upper:
            result.status = "NO_DATA"
            return result
    for pattern in ERROR_PATTERNS:
        if pattern.upper() in msg_upper:
            result.status = "ERROR"
            return result
    return result


def _aggregate_group(results, test_name):
    """Collapse a list of per-case results into one summary result."""
    if not results:
        return results

    passes = [r for r in results if r.status.upper() == "PASS"]
    fails = [r for r in results if r.status.upper() == "FAIL"]
    total = len(results)
    p = len(passes)
    f = len(fails)

    # Use the first result's metadata
    first = results[0]
    field = first.test_field
    state = first.test_from_state

    if f == 0 and p > 0:
        status = "PASS"
        message = f"{p} of {total} cases passed."
    elif f > 0:
        fail_msgs = [str(r.message)[:80] for r in fails[:5]]
        more = f" (+{f - 5} more)" if f > 5 else ""
        message = f"{f} of {total} cases failed, {p} passed.{more} Failures: " + " | ".join(fail_msgs)
        status = "FAIL"
    else:
        status = "NO_DATA"
        message = f"{total} results, none passed or failed"

    return [TestResult(field, status, message, state, test_name)]


def classify_all(results):
    """Classify results AND aggregate variable-length test groups into single results.

    1. Classify each result (FAIL → NO_DATA/ERROR based on message)
    2. Group results by test_name
    3. For tests in AGGREGATE_TESTS, collapse N per-case results into 1 summary
    4. Return the final list
    """
    # Step 1: classify
    classified = [classify_result(r) for r in results]

    # Step 2: separate into aggregate vs keep-as-is
    to_aggregate = defaultdict(list)
    keep = []

    for r in classified:
        name = getattr(r, 'test_name', '')
        if name in AGGREGATE_TESTS:
            to_aggregate[name].append(r)
        else:
            keep.append(r)

    # Step 3: aggregate variable-length groups
    for test_name, group in to_aggregate.items():
        keep.extend(_aggregate_group(group, test_name))

    return keep
