from Databricks.ACTIVE.APPEALS.shared_functions import listing

from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

import pytest

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
            .appName("listing")
            .getOrCreate()
    )

@pytest.fixture(scope="session")
def silver_m1_test_data(spark):
    columns = ["CaseNo", "dv_representation", "lu_appealType", "Interpreter", "CourtPreference", "InCamera", "LanguageId"]
    data = [
        ("1", "AIP", "FT", 0, 0, 0, 0),            # AIP Case
        ("2", "LR", "FT", 0, 0, 0, 0),             # LR Case
        ("3", "AIP", "FT", 1, 0, 0, 0),            # Interpreter Case
        ("4", "LR", "FT", 0, 1, 0, 0),             # Court Preference All Male
        ("5", "AIP", "FT", 0, 2, 0, 0),            # Court Preference All Female
        ("6", "LR", "FT", 0, 0, 1, 0),             # In Camera
        ("7", None, None, None, None, None, None), # All None
        ("8", "AIP", "FT", 1, 1, 1, 1),            # All 1
        ("9", "NotValid", "FT", 0, 0, 0, 0),       # Not AIP or LR
        ("10", "LR", "FT", 2, 0, 0, 0),            # Interpreter is not 1
        ("11", "AIP", "FT", 0, 3, 0, 0),           # Court Preference is not 1 or 2
        ("12", "LR", "FT", 0, 0, 2, 0),            # In Camera is not 1
        ("13", "AIP", "FTPA", 0, 0, 0, 1),         # LanguageCode 1 - Spoken Language
        ("14", "AIP", "FTPA", 0, 0, 0, 5),         # LanguageCode 5 - Spoken Language Manual Entry
        ("15", "AIP", "FTPA", 0, 0, 0, 6),         # LanguageCode 6 - Sign Language
        ("16", "AIP", "FTPA", 0, 0, 0, 7),         # LanguageCode 7 - Sign Language Manual Entry
        ("17", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 37 AND Outcome = 0
        ("18", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 37 AND Outcome = 27
        ("19", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 37 AND Outcome = 37
        ("20", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 37 AND Outcome = 39
        ("21", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 37 AND Outcome = 40
        ("22", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 37 AND Outcome = 50
        ("23", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 38 AND Outcome = 0
        ("24", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 38 AND Outcome = 27
        ("25", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 38 AND Outcome = 37
        ("26", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 38 AND Outcome = 39
        ("27", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 38 AND Outcome = 40
        ("28", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 38 AND Outcome = 50
        ("29", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 26 AND Outcome = 40
        ("30", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - CaseStatus = 26 AND Outcome = 52
        ("31", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - Not Matching CaseStatus: CaseStatus = 39 AND Outcome = 0
        ("32", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - Not Matching Outcome: CaseStatus = 37 AND Outcome = 52
        ("33", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - Not Matching Outcome: CaseStatus = 26 AND Outcome = 0
        ("34", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - Not Matching Outcome: CaseStatus = 0 AND Outcome = 1
        ("35", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - StatusId Check (Many Statuses)
        ("36", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - StatusId Check (CaseStatus and Outcome)
        ("37", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - Additional Language Spoken + Spoken (+ Latest StatusId Check)
        ("38", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - Additional Language Spoken + Spoken Manual
        ("39", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - Additional Language Spoken + Sign
        ("40", "AIP", "FT", 0, 0, 0, 1),           # For m3 conditional tests - Additional Language Spoken + Sign Manual
        ("41", "AIP", "FT", 0, 0, 0, 6),           # For m3 conditional tests - Additional Language Sign + Sign
        ("42", "AIP", "FT", 0, 0, 0, 6),           # For m3 conditional tests - Additional Language Sign + Spoken Manual
        ("43", "AIP", "FT", 0, 0, 0, 6)            # For m3 conditional tests - Additional Language Sign + Sign Manual
    ]

    df = spark.createDataFrame(data, columns)

    return df

@pytest.fixture(scope="session")
def silver_m3_test_data(spark):
    columns = ["CaseNo", "StatusId", "CaseStatus", "Outcome", "AdditionalLanguageId"]
    data = [
        ("17", 1, 37, 0, 0),  # CaseStatus = 37 AND Outcome = 0
        ("18", 1, 37, 27, 0), # CaseStatus = 37 AND Outcome = 27
        ("19", 1, 37, 37, 0), # CaseStatus = 37 AND Outcome = 37
        ("20", 1, 37, 39, 0), # CaseStatus = 37 AND Outcome = 39
        ("21", 1, 37, 40, 0), # CaseStatus = 37 AND Outcome = 40
        ("22", 1, 37, 50, 0), # CaseStatus = 37 AND Outcome = 50
        ("23", 1, 38, 0, 0),  # CaseStatus = 38 AND Outcome = 0
        ("24", 1, 38, 27, 0), # CaseStatus = 38 AND Outcome = 27
        ("25", 1, 38, 37, 0), # CaseStatus = 38 AND Outcome = 37
        ("26", 1, 38, 39, 0), # CaseStatus = 38 AND Outcome = 39
        ("27", 1, 38, 40, 0), # CaseStatus = 38 AND Outcome = 40
        ("28", 1, 38, 50, 0), # CaseStatus = 38 AND Outcome = 50
        ("29", 1, 26, 37, 0), # CaseStatus = 26 AND Outcome = 40
        ("30", 1, 26, 52, 0), # CaseStatus = 26 AND Outcome = 52
        ("31", 1, 39, 0, 0),  # Not Matching CaseStatus: CaseStatus = 39 AND Outcome = 0
        ("32", 1, 37, 52, 0), # Not Matching Outcome: CaseStatus = 37 AND Outcome = 52
        ("33", 1, 26, 0, 0),  # Not Matching Outcome: CaseStatus = 26 AND Outcome = 0
        ("34", 1, 0, 1, 0),   # Not Matching Outcome: CaseStatus = 0 AND Outcome = 1
        ("35", 1, 0, 0, 0),   # StatusId 1 Does Not Match
        ("35", 2, 37, 0, 0),  # StatusId 2 CaseStatus = 37 AND Outcome = 0
        ("35", 3, 0, 50, 0),  # StatusId 3 Does Not Match
        ("35", 4, 37, 50, 0), # StatusId 4 Updated CaseStatus = 37 AND Outcome = 50
        ("35", 5, 50, 50, 0), # StatusId 5 Does Not Match
        ("36", 1, 38, 0, 0),  # CaseStatus and Outcome update test: StatusId 1 CaseStatus = 38 AND Outcome = 0
        ("36", 2, 37, 50, 0), # CaseStatus and Outcome update test: StatusId 2 CaseStatus = 37 AND Outcome = 50
        ("36", 3, 26, 40, 0), # CaseStatus and Outcome update test: StatusId 3 CaseStatus = 26 AND Outcome = 40
        ("37", 1, 37, 0, 2),  # StatusId 1 Unused Additional Spoken Language Only
        ("37", 2, 37, 0, 3),  # StatusId 2 First Additional Spoken Language Only (to Spoken + Spoken)
        ("38", 1, 37, 0, 5),  # Additional Manual Language Entry (to Spoken + Spoken Manual)
        ("39", 1, 37, 0, 6),  # Additional Sign Language (to Spoken + Sign)
        ("40", 1, 37, 0, 7),  # Additional Sign Manual Language (to Spoken + Sign Manual)
        ("41", 1, 37, 0, 6),  # Additional Sign Language (to Sign + Sign Manual)
        ("42", 1, 37, 0, 5),  # Additional Manual Language Entry (to Sign + Spoken Manual)
        ("43", 1, 37, 0, 7),  # Additional Sign Manual Language (to Sign + Sign Manual)
        # Current m3 condition prevents more than 1 AdditionalLanguageId per CaseNo. Below should be removed after clarification if this is the case.
        # ("37", 2, 37, 0, 4),  # StatusId 2 Second Additional Spoken Language Only
        # ("44", 1, 37, 0, 2),  # Additional Spoken Language (to Spoken + Sign + Spoken Manual)
        # ("44", 1, 37, 0, 5),  # Additional Manual Language (to Spoken + Sign + Spoken Manual)
        # ("44", 1, 37, 0, 6),  # Additional Sign Language (to Spoken + Sign + Spoken Manual)
        # ("45", 1, 37, 0, 2),  # Additional Spoken Language (to Spoken + Sign + Spoken Manual + Sign Manual)
        # ("45", 1, 37, 0, 5),  # Additional Manual Language (to Spoken + Sign + Spoken Manual + Sign Manual)
        # ("45", 1, 37, 0, 6),  # Additional Sign Language (to Spoken + Sign + Spoken Manual + Sign Manual)
        # ("45", 1, 37, 0, 7)   # Additional Sign Language (to Spoken + Sign + Spoken Manual + Sign Manual)
    ]

    df = spark.createDataFrame(data, columns)

    return df

@pytest.fixture(scope="session")
def silver_c_test_data(spark):
    columns = ["CaseNo", "CategoryId"]
    data = [
        ("1", 37), # Category 37 for isEvidenceFromOutsideUkOoc
        ("2", 38), # Category 38 for isEvidenceFromOutsideUkInCountry
        ("3", 39), # Category 39 with no match
        ("3", 40), # Category 40 with no match
        ("4", 37), # Category 37 AND 38
        ("4", 38), # Category 37 AND 38
        ("5", 38), # Category 38 matches
        ("5", 39), # Category 38 matches
        ("5", 40)  # Category 38 matches
    ]

    df = spark.createDataFrame(data, columns)

    return df

@pytest.fixture(scope="session")
def bronze_interpreter_languages_test_data(spark):
    columns = ["LanguageId", "Description", "appellantInterpreterLanguageCategory", "LanguageCode", "LanguageLabel", "manualEntry", "manualEntryDescription"]
    data = [
        (1, "British English", "spokenLanguageInterpreter", "en", "English", "[]", None),               # Spoken Language
        (2, "French", "spokenLanguageInterpreter", "fr", "French", "[]", None),                         # Spoken Language Secondary
        (3, "German", "spokenLanguageInterpreter", "deu", "German", "[]", None),                        # Spoken Language Tertiary
        (4, "Spanish European", "spokenLanguageInterpreter", "spa", "Spanish", "[]", None),             # Spoken Language Quaternary
        (5, "Manual", "spokenLanguageInterpreter", None, None, "Yes", "Manual Entry"),                  # Spoken Language Manual Entry
        (6, "Sign (BSL)", "signLanguageInterpreter", "bfi", "British Sign Language (BSL)", "[]", None), # Sign Language
        (7, "Sign (Other)", "signLanguageInterpreter", None, None, "Yes", "Sign Language (Other)")      # Sign Language Manual Entry
    ]

    df = spark.createDataFrame(data, columns)

    return df


class TestListingState():

    def test_hearing_requirements(self, spark, silver_m1_test_data, silver_m3_test_data, silver_c_test_data, bronze_interpreter_languages_test_data):
        df, df_audit = listing.hearingRequirements(silver_m1_test_data, silver_m3_test_data, silver_c_test_data, bronze_interpreter_languages_test_data)

        expected_output_df = spark.read.schema(df.schema).option("multiLine", True).json("tests/active/listing/resources/output_df.json")
        expected_audit_output_df = spark.read.schema(df_audit.schema).option("multiLine", True).json("tests/active/listing/resources/audit_df.json")

        assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
        assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)
