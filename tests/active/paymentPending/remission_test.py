import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import remissionTypes

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("remissionTypesTests")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def remissionTypes_outputs(spark):

    m1_schema = T.StructType([
            T.StructField("CaseNo", T.StringType()),
            T.StructField("dv_CCDAppealType", T.StringType()),
            T.StructField("dv_representation", T.StringType()),
            T.StructField("lu_appealType", T.StringType()),
            T.StructField("PaymentRemissionRequested", T.IntegerType()),
            T.StructField("PaymentRemissionReason", T.IntegerType()),
            T.StructField("ASFReferenceNo", T.StringType()),
            T.StructField("LSCReference", T.StringType()),
            T.StructField("PaymentRemissionReasonNote", T.StringType()),
        ])

    m1_data = [
    ("EA/01001/2025", "EA", "AIP", "refusalOfHumanRights", 1, 1, "ASF-999", None, None),
    ("EA/01002/2025", "EA", "LR", "refusalOfHumanRights", 1, 3, None, "LSC-123", None),
    ("EA/01003/2025", "EA", "AIP", "refusalOfHumanRights", 1, 9, None, None, "HWF-456"),
    ("HU/01004/2025", "HU", "LR", "refusalOfHumanRights", 1, 2, None, None, None),
    ("PA/01005/2025", "PA", "AIP", "refusalOfHumanRights", 1, 1, None, None, None)
]
    
    bronze_schema = T.StructType([
        T.StructField("PaymentRemissionRequested", T.IntegerType()),
        T.StructField("PaymentRemissionReason", T.IntegerType()),
        T.StructField("remissionType", T.StringType()),
        T.StructField("remissionClaim", T.StringType()),
        T.StructField("feeRemissionType", T.StringType()),
        T.StructField("exceptionalCircumstances", T.StringType()),
        T.StructField("legalAidAccountNumber", T.StringType()),
        T.StructField("asylumSupportReference", T.StringType()),
        T.StructField("helpWithFeesReferenceNumber", T.StringType()),
    ])

    bronze_data = [(2, 0, "noRemission", "OMIT", "OMIT", "OMIT", "OMIT", "OMIT", "OMIT"),
        (1, 2, "exceptionalCircumstancesRemission", "OMIT", "OMIT",
         "This is a migrated ARIA case. The remission reason was Oral Hearing Direction. Please see the documents for further information.",
         "OMIT", "OMIT", "OMIT"),
        (1, 1, "hoWaiverRemission", "asylumSupport", "Asylum Support", "OMIT",
         "OMIT", "M1.ASFReferenceNo ELSE IF NULL 'Unknown'", "OMIT"),
        (1, 10, "NO MAPPING REQUIRED", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED",
         "NO MAPPING REQUIRED", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED"),
        (0, 0, "noRemission", "OMIT", "OMIT", "OMIT", "OMIT", "OMIT", "OMIT"),
        (1, 5, "NO MAPPING REQUIRED", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED",
         "NO MAPPING REQUIRED", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED", "NO MAPPING REQUIRED"),
        (1, 3, "hoWaiverRemission", "legalAid", "Legal Aid", "OMIT",
         "M1.LSCReference; ELSE IF NULL 'Unknown'", "OMIT", "OMIT"),
        (1, 7, "hoWaiverRemission", "section20", "Section 20", "OMIT", "OMIT", "OMIT", "OMIT"),
        (1, 9, "helpWithFees", "OMIT", "OMIT", "OMIT", "OMIT", "OMIT",
         "M1.PaymentRemissionReasonNote; ELSE IF NULL 'Unknown'"),
        (1, 8, "hoWaiverRemission", "homeOfficeWaiver", "Home Office fee waiver", "OMIT",
         "OMIT", "OMIT", "OMIT"),
        (1, 4, "hoWaiverRemission", "section17", "Section 17", "OMIT", "OMIT", "OMIT", "OMIT"),
        (1, 6, "exceptionalCircumstancesRemission", "OMIT", "OMIT",
         "This is a migrated ARIA case. The remission reason was Other. Please see the documents for further information.",
         "OMIT", "OMIT", "OMIT"),]

    silver_m4_schema = T.StructType([T.StructField("CaseNo", T.StringType())])

    silver_m4_data = [    
    ("EA/01001/2025",),
    ("EA/01002/2025",),
    ("EA/01003/2025",),
    ("HU/01004/2025",),
    ("PA/01005/2025",)]

    silver_m1 =  spark.createDataFrame(m1_data, m1_schema)
    silver_m4 = spark.createDataFrame(silver_m4_data, silver_m4_schema)
    bronze_remission_lookup_df = spark.createDataFrame(bronze_data, bronze_schema)

    # Call the function under test
    remissionTypes_content, _ = remissionTypes(silver_m1, bronze_remission_lookup_df, silver_m4)

    # Convert to dictionary keyed by CaseNo
    results = {row["CaseNo"]: row.asDict() for row in remissionTypes_content.collect()}
    return results

def assert_remission(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k}: expected {v} but got {row.get(k)}"


def test_asylum_support_remission(remissionTypes_outputs):
    row = remissionTypes_outputs["EA/01001/2025"]

    assert_remission(
        row,
        remissionType="hoWaiverRemission",
        remissionClaim="asylumSupport",
        feeRemissionType="Asylum Support",
        exceptionalCircumstances=None,
        legalAidAccountNumber=None,
        asylumSupportReference="ASF-999",
        helpWithFeesReferenceNumber=None,
    )


def test_legal_aid_remission_lr(remissionTypes_outputs):
    row = remissionTypes_outputs["EA/01002/2025"]

    assert_remission(
        row,
        remissionType="hoWaiverRemission",
        remissionClaim="legalAid",
        feeRemissionType="Legal Aid",
        exceptionalCircumstances=None,
        legalAidAccountNumber="LSC-123",
        asylumSupportReference=None,
        helpWithFeesReferenceNumber=None,
    )


def test_help_with_fees_remission(remissionTypes_outputs):
    row = remissionTypes_outputs["EA/01003/2025"]

    assert_remission(
        row,
        remissionType="helpWithFees",
        remissionClaim=None,
        feeRemissionType=None,
        exceptionalCircumstances=None,
        legalAidAccountNumber=None,
        asylumSupportReference=None,
        helpWithFeesReferenceNumber="HWF-456",
    )


def test_exceptional_circumstances_remission(remissionTypes_outputs):
    row = remissionTypes_outputs["HU/01004/2025"]

    assert_remission(
        row,
        remissionType="exceptionalCircumstancesRemission",
        remissionClaim=None,
        feeRemissionType=None,
        exceptionalCircumstances=(
            "This is a migrated ARIA case. The remission reason was Oral Hearing Direction. "
            "Please see the documents for further information."
        ),
        legalAidAccountNumber=None,
        asylumSupportReference=None,
        helpWithFeesReferenceNumber=None,
    )


def test_asylum_support_unknown_reference(remissionTypes_outputs):
    row = remissionTypes_outputs["PA/01005/2025"]

    assert_remission(
        row,
        remissionType="hoWaiverRemission",
        remissionClaim="asylumSupport",
        feeRemissionType="Asylum Support",
        exceptionalCircumstances=None,
        legalAidAccountNumber=None,
        asylumSupportReference="Unknown",
        helpWithFeesReferenceNumber=None,
    )
