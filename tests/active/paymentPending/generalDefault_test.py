import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import generalDefault
import uuid

@pytest.fixture(scope="session")
def generalDefault_outputs(spark):

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True),
        T.StructField("lu_appealType", T.StringType(), True),
    ])

    m1_data = [
        ("EA/01001/2025", "LR", "EA"),
        ("HU/01004/2025", "LR", "HU"),
        ("XX/01005/2025", None, "EA"),  # should be filtered out
        ("YY/01006/2025", "LR", None),  # should be filtered out
    ]

    silver_m1 = spark.createDataFrame(m1_data, m1_schema)

    generalDefault_content = generalDefault(silver_m1)

    results = {row["CaseNo"]: row.asDict() for row in generalDefault_content.collect()}
    return results

def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k} expected {v} but got {row.get(k)}"

def test_generalDefault_filters_rows(generalDefault_outputs):
    """Rows with dv_representation not in ['LR','AIP'] or null lu_appealType are excluded."""
    assert "XX/01005/2025" not in generalDefault_outputs
    assert "YY/01006/2025" not in generalDefault_outputs
    assert "EA/01001/2025" in generalDefault_outputs
    assert "HU/01004/2025" in generalDefault_outputs

def test_generalDefault_notificationsSent_is_empty_array(generalDefault_outputs):
    for row in generalDefault_outputs.values():
        assert "notificationsSent" in row
        assert row["notificationsSent"] == []

def test_generalDefault_string_fields(generalDefault_outputs):
    expected_values = {
        "submitNotificationStatus": "",
        "isFeePaymentEnabled": "Yes",
        "isRemissionsEnabled": "Yes",
        "isOutOfCountryEnabled": "Yes",
        "isIntegrated": "No",
        "isNabaEnabled": "No",
        "isNabaAdaEnabled": "Yes",
        "isNabaEnabledOoc": "No",
        "isCaseUsingLocationRefData": "Yes",
        "hasAddedLegalRepDetails": "Yes",
        "autoHearingRequestEnabled": "No",
        "isDlrmFeeRemissionEnabled": "Yes",
        "isDlrmFeeRefundEnabled": "Yes",
        "sendDirectionActionAvailable": "Yes",
        "changeDirectionDueDateActionAvailable": "No",
        "markEvidenceAsReviewedActionAvailable": "No",
        "uploadAddendumEvidenceActionAvailable": "No",
        "uploadAdditionalEvidenceActionAvailable": "No",
        "displayMarkAsPaidEventForPartialRemission": "No",
        "haveHearingAttendeesAndDurationBeenRecorded": "No",
        "markAddendumEvidenceAsReviewedActionAvailable": "No",
        "uploadAddendumEvidenceLegalRepActionAvailable": "No",
        "uploadAddendumEvidenceHomeOfficeActionAvailable": "No",
        "uploadAddendumEvidenceAdminOfficerActionAvailable": "No",
        "uploadAdditionalEvidenceHomeOfficeActionAvailable": "No"
    }

    for row in generalDefault_outputs.values():
        assert_equals(row, **expected_values)