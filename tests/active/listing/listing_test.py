from Databricks.ACTIVE.APPEALS.shared_functions import listing

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
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
def bronze_interpreter_languages_test_data(spark):
    columns = ["LanguageId", "Description", "appellantInterpreterLanguageCategory", "LanguageCode", "LanguageLabel", "manualEntry", "manualEntryDescription"]
    data = [
        (1, "British English", "spokenLanguageInterpreter", "en", "English", "[]", None),                # Spoken Language
        (2, "French", "spokenLanguageInterpreter", "fr", "French", "[]", None),                          # Spoken Language Secondary
        (3, "German", "spokenLanguageInterpreter", "deu", "German", "[]", None),                         # Spoken Language Tertiary
        (4, "Spanish European", "spokenLanguageInterpreter", "spa", "Spanish", "[]", None),              # Spoken Language Quaternary
        (5, "Manual", "spokenLanguageInterpreter", None, None, "Yes", "Manual Entry"),                   # Spoken Language Manual Entry
        (6, "Sign (BSL)", "signLanguageInterpreter", "bfi", "British Sign Language (BSL)", "[]", None),  # Sign Language
        (7, "Sign (Other)", "signLanguageInterpreter", None, None, "Yes", "Sign Language (Other)")       # Sign Language Manual Entry
    ]

    df = spark.createDataFrame(data, columns)

    return df


class TestListingState():

    M1_COLUMNS = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_representation", StringType()),
        StructField("lu_appealType", StringType()),
        StructField("Interpreter", IntegerType()),
        StructField("CourtPreference", IntegerType()),
        StructField("InCamera", IntegerType()),
        StructField("LanguageId", IntegerType())
    ])
    M3_COLUMNS = StructType([
        StructField("CaseNo", StringType()),
        StructField("StatusId", IntegerType()),
        StructField("CaseStatus", IntegerType()),
        StructField("Outcome", IntegerType()),
        StructField("AdditionalLanguageId", IntegerType())
    ])
    C_COLUMNS = StructType([
        StructField("CaseNo", StringType()),
        StructField("CategoryId", IntegerType())
    ])

    def test_hearing_requirements_yes_no_fields(self, spark, bronze_interpreter_languages_test_data):
        m1_data = [
            ("1", "AIP", "FT", 0, 0, 0, 0),             # AIP Case
            ("2", "LR", "FT", 0, 0, 0, 0),              # LR Case
            ("3", "AIP", "FT", 1, 0, 0, 0),             # Interpreter Case 1
            ("4", "AIP", "FT", 2, 0, 0, 0),             # Interpreter Case 2
            ("5", "LR", "FT", 0, 1, 0, 0),              # Court Preference All Male
            ("6", "AIP", "FT", 0, 2, 0, 0),             # Court Preference All Female
            ("7", "LR", "FT", 0, 0, 1, 0),              # In Camera
            ("8", None, None, None, None, None, None),  # All None
            ("9", "AIP", "FT", 1, 1, 1, 1),             # All 1
            ("10", "NotValid", "FT", 0, 0, 0, 0),       # Not AIP or LR
            ("11", "LR", "FT", 3, 0, 0, 0),             # Interpreter is not 1 or 2
            ("12", "AIP", "FT", 0, 3, 0, 0),            # Court Preference is not 1 or 2
            ("13", "LR", "FT", 0, 0, 2, 0),             # In Camera is not 1
        ]

        silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)
        silver_m3_test_data = spark.createDataFrame([], self.M3_COLUMNS)
        silver_c_test_data = spark.createDataFrame([], self.C_COLUMNS)

        df, df_audit = listing.hearingRequirements(silver_m1_test_data, silver_m3_test_data, silver_c_test_data, bronze_interpreter_languages_test_data)

        expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/yes_no_output.jsonl")
        expected_audit_output_df = spark.read.schema(df_audit.schema).json("tests/active/listing/resources/yes_no_audit_output.jsonl")

        assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
        assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)

    def test_hearing_requirements_interpreter_language_fields(self, spark, bronze_interpreter_languages_test_data):
        m1_data = [
            ("1", "AIP", "FTPA", 0, 0, 0, 1),  # LanguageCode 1 - Spoken Language
            ("2", "AIP", "FTPA", 0, 0, 0, 5),  # LanguageCode 5 - Spoken Language Manual Entry
            ("3", "AIP", "FTPA", 0, 0, 0, 6),  # LanguageCode 6 - Sign Language
            ("4", "AIP", "FTPA", 0, 0, 0, 7),  # LanguageCode 7 - Sign Language Manual Entry
            ("5", "AIP", "FT", 0, 0, 0, 1),    # For m3 conditional tests - Additional Language Spoken + Spoken (+ Latest StatusId Check)
            ("6", "AIP", "FT", 0, 0, 0, 1),    # For m3 conditional tests - Additional Language Spoken + Spoken Manual
            ("7", "AIP", "FT", 0, 0, 0, 1),    # For m3 conditional tests - Additional Language Spoken + Sign
            ("8", "AIP", "FT", 0, 0, 0, 1),    # For m3 conditional tests - Additional Language Spoken + Sign Manual
            ("9", "AIP", "FT", 0, 0, 0, 6),    # For m3 conditional tests - Additional Language Sign + Sign
            ("10", "AIP", "FT", 0, 0, 0, 6),   # For m3 conditional tests - Additional Language Sign + Spoken Manual
            ("11", "AIP", "FT", 0, 0, 0, 6)    # For m3 conditional tests - Additional Language Sign + Sign Manual
        ]

        m3_data = [
            ("5", 1, 37, 0, 2),   # StatusId 1 Unused Additional Spoken Language Only
            ("5", 2, 37, 0, 3),   # StatusId 2 First Additional Spoken Language Only (to Spoken + Spoken)
            ("6", 1, 37, 0, 5),   # Additional Manual Language Entry (to Spoken + Spoken Manual)
            ("7", 1, 37, 0, 6),   # Additional Sign Language (to Spoken + Sign)
            ("8", 1, 37, 0, 7),   # Additional Sign Manual Language (to Spoken + Sign Manual)
            ("9", 1, 37, 0, 6),   # Additional Sign Language (to Sign + Sign Manual)
            ("10", 1, 37, 0, 5),  # Additional Manual Language Entry (to Sign + Spoken Manual)
            ("11", 1, 37, 0, 7)   # Additional Sign Manual Language (to Sign + Sign Manual)
        ]

        silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)
        silver_m3_test_data = spark.createDataFrame(m3_data, self.M3_COLUMNS)
        silver_c_test_data = spark.createDataFrame([], self.C_COLUMNS)

        df, df_audit = listing.hearingRequirements(silver_m1_test_data, silver_m3_test_data, silver_c_test_data, bronze_interpreter_languages_test_data)

        expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/interpreter_languages_output.jsonl")
        expected_audit_output_df = spark.read.schema(df_audit.schema).json("tests/active/listing/resources/interpreter_languages_audit_output.jsonl")

        assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
        assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)

    def test_hearing_requirements_m3_conditional_fields(self, spark, bronze_interpreter_languages_test_data):
        m1_data = [
            ("1", "AIP", "FT", 0, 0, 0, 1),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 0
            ("2", "AIP", "FT", 0, 0, 0, 1),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 27
            ("3", "AIP", "FT", 0, 0, 0, 1),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 37
            ("4", "AIP", "FT", 0, 0, 0, 1),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 39
            ("5", "AIP", "FT", 0, 0, 0, 1),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 40
            ("6", "AIP", "FT", 0, 0, 0, 1),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 50
            ("7", "AIP", "FT", 0, 0, 0, 1),   # For m3 conditional tests - CaseStatus = 38 AND Outcome = 0
            ("8", "AIP", "FT", 0, 0, 0, 1),   # For m3 conditional tests - CaseStatus = 38 AND Outcome = 27
            ("9", "AIP", "FT", 0, 0, 0, 1),   # For m3 conditional tests - CaseStatus = 38 AND Outcome = 37
            ("10", "AIP", "FT", 0, 0, 0, 1),  # For m3 conditional tests - CaseStatus = 38 AND Outcome = 39
            ("11", "AIP", "FT", 0, 0, 0, 1),  # For m3 conditional tests - CaseStatus = 38 AND Outcome = 40
            ("12", "AIP", "FT", 0, 0, 0, 1),  # For m3 conditional tests - CaseStatus = 38 AND Outcome = 50
            ("13", "AIP", "FT", 0, 0, 0, 1),  # For m3 conditional tests - CaseStatus = 26 AND Outcome = 40
            ("14", "AIP", "FT", 0, 0, 0, 1),  # For m3 conditional tests - CaseStatus = 26 AND Outcome = 52
            ("15", "AIP", "FT", 0, 0, 0, 1),  # For m3 conditional tests - Not Matching CaseStatus: CaseStatus = 39 AND Outcome = 0
            ("16", "AIP", "FT", 0, 0, 0, 1),  # For m3 conditional tests - Not Matching Outcome: CaseStatus = 37 AND Outcome = 52
            ("17", "AIP", "FT", 0, 0, 0, 1),  # For m3 conditional tests - Not Matching Outcome: CaseStatus = 26 AND Outcome = 0
            ("18", "AIP", "FT", 0, 0, 0, 1),  # For m3 conditional tests - Not Matching Outcome: CaseStatus = 0 AND Outcome = 1
            ("19", "AIP", "FT", 0, 0, 0, 1),  # For m3 conditional tests - StatusId Check (Many Statuses)
            ("20", "AIP", "FT", 0, 0, 0, 1)   # For m3 conditional tests - StatusId Check (CaseStatus and Outcome)
        ]

        m3_data = [
            ("1", 1, 37, 0, 0),    # CaseStatus = 37 AND Outcome = 0
            ("2", 1, 37, 27, 0),   # CaseStatus = 37 AND Outcome = 27
            ("3", 1, 37, 37, 0),   # CaseStatus = 37 AND Outcome = 37
            ("4", 1, 37, 39, 0),   # CaseStatus = 37 AND Outcome = 39
            ("5", 1, 37, 40, 0),   # CaseStatus = 37 AND Outcome = 40
            ("6", 1, 37, 50, 0),   # CaseStatus = 37 AND Outcome = 50
            ("7", 1, 38, 0, 0),    # CaseStatus = 38 AND Outcome = 0
            ("8", 1, 38, 27, 0),   # CaseStatus = 38 AND Outcome = 27
            ("9", 1, 38, 37, 0),   # CaseStatus = 38 AND Outcome = 37
            ("10", 1, 38, 39, 0),  # CaseStatus = 38 AND Outcome = 39
            ("11", 1, 38, 40, 0),  # CaseStatus = 38 AND Outcome = 40
            ("12", 1, 38, 50, 0),  # CaseStatus = 38 AND Outcome = 50
            ("13", 1, 26, 37, 0),  # CaseStatus = 26 AND Outcome = 40
            ("14", 1, 26, 52, 0),  # CaseStatus = 26 AND Outcome = 52
            ("15", 1, 39, 0, 0),   # Not Matching CaseStatus: CaseStatus = 39 AND Outcome = 0
            ("16", 1, 37, 52, 0),  # Not Matching Outcome: CaseStatus = 37 AND Outcome = 52
            ("17", 1, 26, 0, 0),   # Not Matching Outcome: CaseStatus = 26 AND Outcome = 0
            ("18", 1, 0, 1, 0),    # Not Matching Outcome: CaseStatus = 0 AND Outcome = 1
            ("19", 1, 0, 0, 0),    # StatusId 1 Does Not Match
            ("19", 2, 37, 0, 0),   # StatusId 2 CaseStatus = 37 AND Outcome = 0
            ("19", 3, 0, 50, 0),   # StatusId 3 Does Not Match
            ("19", 4, 37, 50, 0),  # StatusId 4 Updated CaseStatus = 37 AND Outcome = 50
            ("19", 5, 50, 50, 0),  # StatusId 5 Does Not Match
            ("20", 1, 38, 0, 0),   # CaseStatus and Outcome update test: StatusId 1 CaseStatus = 38 AND Outcome = 0
            ("20", 2, 37, 50, 0),  # CaseStatus and Outcome update test: StatusId 2 CaseStatus = 37 AND Outcome = 50
            ("20", 3, 26, 40, 0),  # CaseStatus and Outcome update test: StatusId 3 CaseStatus = 26 AND Outcome = 40
        ]

        silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)
        silver_m3_test_data = spark.createDataFrame(m3_data, self.M3_COLUMNS)
        silver_c_test_data = spark.createDataFrame([], self.C_COLUMNS)

        df, df_audit = listing.hearingRequirements(silver_m1_test_data, silver_m3_test_data, silver_c_test_data, bronze_interpreter_languages_test_data)

        expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/m3_conditional_output.jsonl")
        expected_audit_output_df = spark.read.schema(df_audit.schema).json("tests/active/listing/resources/m3_conditional_audit_output.jsonl")

        assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
        assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)

    def test_hearing_requirements_category_fields(self, spark, bronze_interpreter_languages_test_data):
        m1_data = [
            ("1", "AIP", "FT", 0, 0, 0, 0),  # Category isEvidenceFromOutsideUkOoc
            ("2", "AIP", "FT", 0, 0, 0, 0),  # Category isEvidenceFromOutsideUkInCountry
            ("3", "AIP", "FT", 0, 0, 0, 0),  # No matching category
            ("4", "AIP", "FT", 0, 0, 0, 0),  # Both isEvidenceFromOutsideUkOoc and isEvidenceFromOutsideUkInCountry
            ("5", "AIP", "FT", 0, 0, 0, 0)   # Multiple categories including isEvidenceFromOutsideUkInCountry
        ]

        c_data = [
            ("1", 37),  # Category 37 for isEvidenceFromOutsideUkOoc
            ("2", 38),  # Category 38 for isEvidenceFromOutsideUkInCountry
            ("3", 39),  # Category 39 with no match
            ("3", 40),  # Category 40 with no match
            ("4", 37),  # Category 37 AND 38
            ("4", 38),  # Category 37 AND 38
            ("5", 38),  # Category 38 matches
            ("5", 39),  # Category 38 matches
            ("5", 40)   # Category 38 matches
        ]

        silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)
        silver_m3_test_data = spark.createDataFrame([], self.M3_COLUMNS)
        silver_c_test_data = spark.createDataFrame(c_data, self.C_COLUMNS)

        df, df_audit = listing.hearingRequirements(silver_m1_test_data, silver_m3_test_data, silver_c_test_data, bronze_interpreter_languages_test_data)

        expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/category_output.jsonl")
        expected_audit_output_df = spark.read.schema(df_audit.schema).json("tests/active/listing/resources/category_audit_output.jsonl")

        assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
        assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)
