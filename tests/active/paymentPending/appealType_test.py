import pytest
from pyspark.sql import SparkSession, types as T
from Databricks.ACTIVE.APPEALS.shared_functions.paymentPending import appealType

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("appealTypeTests")
        .getOrCreate()
    )

def ccd_dynamic_list(code: str, label: str):
    return {
        "value": {"code": code, "label": label},
        "list_items": [{"code": code, "label": label}]
    }

@pytest.fixture(scope="session")
def appealType_outputs(spark):

    ccd_dynamic_list_schema = T.StructType([
    T.StructField(
        "value", T.StructType([
            T.StructField("code", T.StringType(), True),
            T.StructField("label", T.StringType(), True),
        ]), True),

    T.StructField(
        "list_items", T.ArrayType(T.StructType([
                T.StructField("code", T.StringType(), True),
                T.StructField("label", T.StringType(), True),
            ])), True)])

    m1_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CasePrefix", T.StringType(), True),
        T.StructField("dv_representation", T.StringType(), True), 
        T.StructField("lu_appealType", T.StringType(), True),
        T.StructField("lu_hmctsCaseCategory", T.StringType(), True),
        T.StructField("lu_appealTypeDescription", T.StringType(), True),
        T.StructField("lu_caseManagementCategory", ccd_dynamic_list_schema, True)
    ])

    m1_data = [
    # DA -> refusalOfEu
    ("DA/00001/2020", "DA", "LR", "refusalOfEu", "EEA",
     "Refusal of application under the EEA regulations",
     ccd_dynamic_list("refusalOfEu", "Refusal of application under the EEA regulations")),

    # DC -> deprivation
    ("DC/00002/2020", "DC", "LR", "deprivation", "DoC",
     "Deprivation of citizenship",
     ccd_dynamic_list("deprivation", "Deprivation of citizenship")),

    # EA (CategoryId NOT IN [47,48,49]) -> refusalOfEu
    ("EA/00003/2020", "EA", "LR", "refusalOfEu", "EEA",
     "Refusal of application under the EEA regulations",
     ccd_dynamic_list("refusalOfEu", "Refusal of application under the EEA regulations")),

    # EA (CategoryId IN [47,48,49]) -> euSettlementScheme (AIP)
    ("EA/00004/2020", "EA", "AIP", "euSettlementScheme", "EU Settlement Scheme",
     "EU Settlement Scheme", None),

    ("EA/00005/2020", "EA", "AIP", "euSettlementScheme", "EU Settlement Scheme",
     "EU Settlement Scheme", None),

    ("EA/00006/2020", "EA", "AIP", "euSettlementScheme", "EU Settlement Scheme",
     "EU Settlement Scheme", None),

    # HU -> refusalOfHumanRights
    ("HU/00007/2020", "HU", "LR", "refusalOfHumanRights", "Human rights",
     "Refusal of a human rights claim",
     ccd_dynamic_list("refusalOfHumanRights", "Refusal of a human rights claim")),

    # IA -> refusalOfHumanRights
    ("IA/00008/2020", "IA", "LR", "refusalOfHumanRights", "Human rights",
     "Refusal of a human rights claim",
     ccd_dynamic_list("refusalOfHumanRights", "Refusal of a human rights claim")),

    # LD -> deprivation
    ("LD/00009/2020", "LD", "LR", "deprivation", "DoC",
     "Deprivation of citizenship",
     ccd_dynamic_list("deprivation", "Deprivation of citizenship")),

    # LE -> refusalOfEu
    ("LE/00010/2020", "LE", "LR", "refusalOfEu", "EEA",
     "Refusal of application under the EEA regulations",
     ccd_dynamic_list("refusalOfEu", "Refusal of application under the EEA regulations")),

    # LH -> refusalOfHumanRights
    ("LH/00011/2020", "LH", "LR", "refusalOfHumanRights", "Human rights",
     "Refusal of a human rights claim",
     ccd_dynamic_list("refusalOfHumanRights", "Refusal of a human rights claim")),

    # LP -> protection (AIP)
    ("LP/00012/2020", "LP", "AIP", "protection", "Protection",
     "Refusal of protection claim", None),

    # LR -> revocationOfProtection
    ("LR/00013/2020", "LR", "LR", "revocationOfProtection", "Revocation",
     "Revocation of a protection status",
     ccd_dynamic_list("revocationOfProtection", "Revocation of a protection status")),

    # PA -> protection (AIP)
    ("PA/00014/2020", "PA", "AIP", "protection", "Protection",
     "Refusal of protection claim", None),

    # RP -> revocationOfProtection
    ("RP/00015/2020", "RP", "LR", "revocationOfProtection", "Revocation",
     "Revocation of a protection status",
     ccd_dynamic_list("revocationOfProtection", "Revocation of a protection status")),
    ]

    silver_c_schema = T.StructType([
        T.StructField("CaseNo", T.StringType(), True),
        T.StructField("CategoryId", T.IntegerType(), True)
    ])

    silver_c_data = [
        # DA
        ("DA/00001/2020", None),

        # DC
        ("DC/00002/2020", None),

        # EA (CategoryId drives the rule)
        ("EA/00003/2020", 10),   # NOT IN [47,48,49] => refusalOfEu
        ("EA/00004/2020", 47),   # IN [47,48,49] => euSettlementScheme
        ("EA/00005/2020", 48),   # IN [47,48,49] => euSettlementScheme
        ("EA/00006/2020", 49),   # IN [47,48,49] => euSettlementScheme

        # HU
        ("HU/00007/2020", None),

        # IA
        ("IA/00008/2020", None),

        # LD
        ("LD/00009/2020", None),

        # LE
        ("LE/00010/2020", None),

        # LH
        ("LH/00011/2020", None),

        # LP
        ("LP/00012/2020", None),

        # LR
        ("LR/00013/2020", None),

        # PA
        ("PA/00014/2020", None),

        # RP
        ("RP/00015/2020", None)
    ]

    silver_m1 =  spark.createDataFrame(m1_data, m1_schema)
    silver_c =  spark.createDataFrame(silver_c_data, silver_c_schema)

    # Call the function under test
    appealType_content, _ = appealType(silver_m1, silver_c)

    # Convert to dictionary keyed by CaseNo
    results = {row["CaseNo"]: row.asDict() for row in appealType_content.collect()}
    return results

def assert_all_null(row, *fields):
    for f in fields:
        assert row.get(f) is None, f"{f} expected None but got {row.get(f)}"


def assert_equals(row, **expected):
    for k, v in expected.items():
        assert row.get(k) == v, f"{k} expected {v} but got {row.get(k)}"

def test_all_expected_cases_present(appealType_outputs):
    expected_case_nos = {
        "DA/00001/2020",
        "DC/00002/2020",
        "EA/00003/2020",
        "EA/00004/2020",
        "EA/00005/2020",
        "EA/00006/2020",
        "HU/00007/2020",
        "IA/00008/2020",
        "LD/00009/2020",
        "LE/00010/2020",
        "LH/00011/2020",
        "LP/00012/2020",
        "LR/00013/2020",
        "PA/00014/2020",
        "RP/00015/2020",
    }

    assert set(appealType_outputs.keys()) == expected_case_nos

def test_core_fields_populated_when_conditions_pass(appealType_outputs):
    for case_no, row in appealType_outputs.items():
        assert row["appealReferenceNumber"] == case_no
        assert row["ccdReferenceNumberForDisplay"] == ""

        assert row["appealType"] is not None
        assert row["hmctsCaseCategory"] is not None
        assert row["appealTypeDescription"] is not None

def test_lr_cases_have_case_management_category(appealType_outputs):
    lr_cases = [
        "DA/00001/2020",
        "DC/00002/2020",
        "EA/00003/2020",
        "HU/00007/2020",
        "IA/00008/2020",
        "LD/00009/2020",
        "LE/00010/2020",
        "LH/00011/2020",
        "LR/00013/2020",
        "RP/00015/2020",
    ]

    for case_no in lr_cases:
        row = appealType_outputs[case_no]
        assert row["caseManagementCategory"] is not None

        # Struct quality check:
        assert row["caseManagementCategory"]["value"]["code"] == row["appealType"]

def test_aip_cases_have_is_appeal_reference_available_yes(appealType_outputs):
    aip_cases = [
        "EA/00004/2020",
        "EA/00005/2020",
        "EA/00006/2020",
        "LP/00012/2020",
        "PA/00014/2020",
    ]

    for case_no in aip_cases:
        row = appealType_outputs[case_no]
        assert row["isAppealReferenceNumberAvailable"] == "Yes"

def test_aip_cases_have_null_case_management_category(appealType_outputs):
    aip_cases = [
        "EA/00004/2020",
        "EA/00005/2020",
        "EA/00006/2020",
        "LP/00012/2020",
        "PA/00014/2020",
    ]

    for case_no in aip_cases:
        row = appealType_outputs[case_no]
        assert row["caseManagementCategory"] is None

def test_lr_cases_have_null_is_appeal_reference_available(appealType_outputs):
    lr_cases = [
        "DA/00001/2020",
        "DC/00002/2020",
        "EA/00003/2020",
        "HU/00007/2020",
        "IA/00008/2020",
        "LD/00009/2020",
        "LE/00010/2020",
        "LH/00011/2020",
        "LR/00013/2020",
        "RP/00015/2020",
    ]

    for case_no in lr_cases:
        row = appealType_outputs[case_no]
        assert row["isAppealReferenceNumberAvailable"] is None

def test_da_mapping(appealType_outputs):
    row = appealType_outputs["DA/00001/2020"]
    assert row["appealType"] == "refusalOfEu"
    assert row["hmctsCaseCategory"] == "EEA"
    assert row["appealTypeDescription"] == "Refusal of application under the EEA regulations"

def test_dc_mapping(appealType_outputs):
    row = appealType_outputs["DC/00002/2020"]
    assert row["appealType"] == "deprivation"
    assert row["hmctsCaseCategory"] == "DoC"
    assert row["appealTypeDescription"] == "Deprivation of citizenship"

def test_ea_not_in_47_48_49_mapping(appealType_outputs):
    row = appealType_outputs["EA/00003/2020"]
    assert row["appealType"] == "refusalOfEu"
    assert row["hmctsCaseCategory"] == "EEA"
    assert row["appealTypeDescription"] == "Refusal of application under the EEA regulations"

def test_ea_in_47_48_49_mapping(appealType_outputs):
    for case_no in ["EA/00004/2020", "EA/00005/2020", "EA/00006/2020"]:
        row = appealType_outputs[case_no]
        assert row["appealType"] == "euSettlementScheme"
        assert row["hmctsCaseCategory"] == "EU Settlement Scheme"
        assert row["appealTypeDescription"] == "EU Settlement Scheme"

def test_hu_mapping(appealType_outputs):
    row = appealType_outputs["HU/00007/2020"]
    assert row["appealType"] == "refusalOfHumanRights"
    assert row["hmctsCaseCategory"] == "Human rights"
    assert row["appealTypeDescription"] == "Refusal of a human rights claim"

def test_ia_mapping(appealType_outputs):
    row = appealType_outputs["IA/00008/2020"]
    assert row["appealType"] == "refusalOfHumanRights"
    assert row["hmctsCaseCategory"] == "Human rights"
    assert row["appealTypeDescription"] == "Refusal of a human rights claim"

def test_ld_mapping(appealType_outputs):
    row = appealType_outputs["LD/00009/2020"]
    assert row["appealType"] == "deprivation"
    assert row["hmctsCaseCategory"] == "DoC"
    assert row["appealTypeDescription"] == "Deprivation of citizenship"

def test_le_mapping(appealType_outputs):
    row = appealType_outputs["LE/00010/2020"]
    assert row["appealType"] == "refusalOfEu"
    assert row["hmctsCaseCategory"] == "EEA"
    assert row["appealTypeDescription"] == "Refusal of application under the EEA regulations"

def test_lh_mapping(appealType_outputs):
    row = appealType_outputs["LH/00011/2020"]
    assert row["appealType"] == "refusalOfHumanRights"
    assert row["hmctsCaseCategory"] == "Human rights"
    assert row["appealTypeDescription"] == "Refusal of a human rights claim"

def test_lp_mapping(appealType_outputs):
    row = appealType_outputs["LP/00012/2020"]
    assert row["appealType"] == "protection"
    assert row["hmctsCaseCategory"] == "Protection"
    assert row["appealTypeDescription"] == "Refusal of protection claim"

def test_lr_mapping(appealType_outputs):
    row = appealType_outputs["LR/00013/2020"]
    assert row["appealType"] == "revocationOfProtection"
    assert row["hmctsCaseCategory"] == "Revocation"
    assert row["appealTypeDescription"] == "Revocation of a protection status"

def test_pa_mapping(appealType_outputs):
    row = appealType_outputs["PA/00014/2020"]
    assert row["appealType"] == "protection"
    assert row["hmctsCaseCategory"] == "Protection"
    assert row["appealTypeDescription"] == "Refusal of protection claim"

def test_rp_mapping(appealType_outputs):
    row = appealType_outputs["RP/00015/2020"]
    assert row["appealType"] == "revocationOfProtection"
    assert row["hmctsCaseCategory"] == "Revocation"
    assert row["appealTypeDescription"] == "Revocation of a protection status"

