from Databricks.ACTIVE.APPEALS.Active_Functions.paymentPending import paymentType
from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("PaymentPendingTests")
        .getOrCreate()
    )

##### Testing the PaymentType field grouping function #####
@pytest.fixture
def paymentType_inputs(spark):
    data = [
        ("CASE001", "EA", 1),
        ("CASE002", "EU", 2),
        ("CASE003", "HU", 3),
        ("CASE004", "HI", 1),  # should be filtered out
    ]
    columns = ["CaseNo", "dv_CCDAppealType", "VisitVisatype"]
    return spark.createDataFrame(data, columns)


## helper function to get payment content as a dictionary
def get_payment_content(df):
    return {row["CaseNo"]: row.asDict() for row in df.collect()}

def test_feeAmountGbp(spark,paymentType_inputs):

    payment_content, _ = paymentType(paymentType_inputs)
    results = get_payment_content(payment_content)

    assert results["CASE001"]["feeAmountGbp"] == 8000
    assert results["CASE002"]["feeAmountGbp"] == 14000
    assert results["CASE003"]["feeAmountGbp"] == None
    assert "CASE004" not in  results


def test_feeDescription(spark,paymentType_inputs):

    payment_content, _ = paymentType(paymentType_inputs)
    results = get_payment_content(payment_content)

    assert results["CASE001"]["feeDescription"] == "Notice of Appeal - appellant consents without hearing A"
    assert results["CASE002"]["feeDescription"] == "Appeal determined with a hearing"
    assert results["CASE003"]["feeDescription"] is None
    assert "CASE004" not in  results

def test_feeWithHearing(spark, paymentType_inputs):
    payment_content, _ = paymentType(paymentType_inputs)
    results = get_payment_content(payment_content)
    assert results["CASE001"]["feeWithHearing"] is None
    assert results["CASE002"]["feeWithHearing"] == 140
    assert results["CASE003"]["feeWithHearing"] is None
    assert "CASE004" not in  results

def test_feeWithoutHearing(spark, paymentType_inputs):
    payment_content, _ = paymentType(paymentType_inputs)
    results = get_payment_content(payment_content)
    assert results["CASE001"]["feeWithoutHearing"] == 80
    assert results["CASE002"]["feeWithoutHearing"] is None
    assert results["CASE003"]["feeWithoutHearing"] is None
    assert "CASE004" not in  results

def test_paymentDescription(spark, paymentType_inputs):
    payment_content, _ = paymentType(paymentType_inputs)
    results = get_payment_content(payment_content)
    assert results["CASE001"]["paymentDescription"] == "Appeal determined without a hearing"
    assert results["CASE002"]["paymentDescription"] == "Appeal determined with a hearing"
    assert results["CASE003"]["paymentDescription"] is None
    assert "CASE004" not in  results

def test_feePaymentAppealType(spark, paymentType_inputs):
    payment_content, _ = paymentType(paymentType_inputs)
    results = get_payment_content(payment_content)
    for case in ["CASE001", "CASE002", "CASE003"]:
        assert results[case]["feePaymentAppealType"] == "Yes"
    assert "CASE004" not in results

def test_paymentStatus(spark, paymentType_inputs):
    payment_content, _ = paymentType(paymentType_inputs)
    results = get_payment_content(payment_content)
    for case in ["CASE001", "CASE002", "CASE003"]:
        assert results[case]["paymentStatus"] == "Payment Pending"
    assert "CASE004" not in results


def test_feeVersion(spark, paymentType_inputs):
    payment_content, _ = paymentType(paymentType_inputs)
    results = get_payment_content(payment_content)
    for case in ["CASE001", "CASE002", "CASE003"]:
        assert results[case]["feeVersion"] == 2
    assert "CASE004" not in results

def test_decisionHearingFeeOption(spark, paymentType_inputs):
    payment_content, _ = paymentType(paymentType_inputs)
    results = get_payment_content(payment_content)
    assert results["CASE001"]["decisionHearingFeeOption"] == "decisionWithoutHearing"
    assert results["CASE002"]["decisionHearingFeeOption"] == "decisionWithHearing"
    assert results["CASE003"]["decisionHearingFeeOption"] is None
    assert "CASE004" not in results

def test_hasServiceRequestAlready(spark, paymentType_inputs):
    payment_content, _ = paymentType(paymentType_inputs)
    results = get_payment_content(payment_content)
    for case in ["CASE001", "CASE002", "CASE003"]:
        assert results[case]["hasServiceRequestAlready"] == "No"
    assert "CASE004" not in results




