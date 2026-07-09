from Databricks.ACTIVE.APPEALS.shared_functions import listing

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import ArrayType, IntegerType, StringType, BooleanType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual
from unittest.mock import patch

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
        (7, "Sign (ASL)", "signLanguageInterpreter", "bfi", "Added Sign Language (ASL)", "[]", None),    # Sign Language Secondary
        (8, "Sign (Other)", "signLanguageInterpreter", None, None, "Yes", "Sign Language (Other)"),      # Sign Language Manual Entry
        (9, "Manual", "spokenLanguageInterpreter", None, None, "Yes", "Additional Manual Entry"),        # Spoken Language Additional Manual Entry
        (10, "Manual", "signLanguageInterpreter", None, None, "Yes", "Additional Manual Sign Entry")     # Sign Language Additional Manual Entry

    ]

    df = spark.createDataFrame(data, columns)

    return df


@pytest.fixture(scope="session")
def bronze_hearing_centres_test_data(spark):
    columns = StructType([
        StructField("Id", StringType())
    ])
    data = []

    df = spark.createDataFrame(data, columns)

    return df


@pytest.fixture(scope="session")
def bronze_derive_hearing_centres_test_data(spark):
    columns = StructType([
        StructField("Id", StringType())
    ])
    data = []

    df = spark.createDataFrame(data, columns)

    return df

@pytest.fixture(scope="session")
def bronze_detention_centres_test_data(spark):
    columns = StructType([
        StructField("Id", StringType())
    ])
    data = []

    df = spark.createDataFrame(data, columns)

    return df

class TestListingState():

    CASE_NO_COLUMNS = StructType([
        StructField("CaseNo", StringType())
    ])
    M1_COLUMNS = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_representation", StringType()),
        StructField("lu_appealType", StringType()),
        StructField("Sponsor_Name", StringType()),
        StructField("Interpreter", IntegerType()),
        StructField("CourtPreference", IntegerType()),
        StructField("InCamera", BooleanType()),
        StructField("LanguageId", IntegerType())
    ])
    M2_COLUMNS = StructType([
        StructField("CaseNo", StringType()),
        StructField("Detained", IntegerType()),
        StructField("AppellantCountryId", IntegerType()),
        StructField("Appellant_Postcode", StringType()),
        StructField("Appellant_Address1", StringType()),
        StructField("Appellant_Address2", StringType()),
        StructField("Appellant_Address3", StringType()),
        StructField("Appellant_Address4", StringType()),
        StructField("Appellant_Address5", StringType()),
        StructField("lu_countryGovUkOocAdminJ", StringType()) 
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
            ("1", "AIP", "FT", None, 0, 0, False, 0),             # AIP Case
            ("2", "LR", "FT", None, 0, 0, False, 0),              # LR Case
            ("3", "AIP", "FT", None, 1, 0, False, 0),             # Interpreter Case 1
            ("4", "AIP", "FT", None, 2, 0, False, 0),             # Interpreter Case 2
            ("5", "LR", "FT", None, 0, 1, False, 0),              # Court Preference All Male
            ("6", "AIP", "FT", None, 0, 2, False, 0),             # Court Preference All Female
            ("7", "LR", "FT", None, 0, 0, True, 0),              # In Camera
            ("8", None, None, None, None, None, None, None),  # All None
            ("9", "AIP", "FT", None, 1, 1, True, 1),             # All 1
            ("10", "NotValid", "FT", None, 0, 0, False, 0),       # Not AIP or LR
            ("11", "LR", "FT", None, 3, 0, False, 0),             # Interpreter is not 1 or 2
            ("12", "AIP", "FT", None, 0, 3, False, 0),            # Court Preference is not 1 or 2
            ("13", "LR", "FT", None, 0, 0, False, 0),             # In Camera is not 1
        ]

        silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)
        silver_m2_test_data = spark.createDataFrame([], self.M2_COLUMNS)
        silver_m3_test_data = spark.createDataFrame([], self.M3_COLUMNS)
        silver_c_test_data = spark.createDataFrame([], self.C_COLUMNS)

        df, df_audit = listing.hearingRequirements(silver_m1_test_data, silver_m2_test_data, silver_m3_test_data, silver_c_test_data, bronze_interpreter_languages_test_data)

        expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/hearing_requirements/yes_no_output.jsonl")
        # expected_audit_output_df = spark.read.schema(df_audit.schema).json("tests/active/listing/resources/hearing_requirements/yes_no_audit_output.jsonl")

        assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
        # assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)

    def test_hearing_requirements_interpreter_language_fields(self, spark, bronze_interpreter_languages_test_data):
        m1_data = [
            ("1", "AIP", "FTPA", None, 1, 0, False, 1),    # LanguageCode 1 - Spoken Language
            ("2", "AIP", "FTPA", None, 1, 0, False, 5),    # LanguageCode 5 - Spoken Language Manual Entry
            ("3", "AIP", "FTPA", None, 1, 0, False, 6),    # LanguageCode 6 - Sign Language
            ("4", "AIP", "FTPA", None, 1, 0, False, 8),    # LanguageCode 7 - Sign Language Manual Entry
            ("5", "AIP", "FT", None, 1, 0, False, 1),      # For m3 conditional tests - Additional Language Spoken + Spoken (+ Latest StatusId Check)
            ("6", "AIP", "FT", None, 1, 0, False, 1),      # For m3 conditional tests - Additional Language Spoken + Spoken Manual
            ("7", "AIP", "FT", None, 1, 0, False, 1),      # For m3 conditional tests - Additional Language Spoken + Sign
            ("8", "AIP", "FT", None, 1, 0, False, 1),      # For m3 conditional tests - Additional Language Spoken + Sign Manual
            ("9", "AIP", "FT", None, 1, 0, False, 6),      # For m3 conditional tests - Additional Language Sign + Sign
            ("10", "AIP", "FT", None, 1, 0, False, 6),     # For m3 conditional tests - Additional Language Sign + Spoken Manual
            ("11", "AIP", "FT", None, 1, 0, False, 6),     # For m3 conditional tests - Additional Language Sign + Sign Manual
            ("12", "AIP", "FT", None, 1, 0, False, 5),     # For m3 conditional tests - Additional Language Spoken Manual + Spoken Manual
            ("13", "AIP", "FT", None, 1, 0, False, 8),     # For m3 conditional tests - Additional Language Sign Manual + Spoken Manual
            ("14", "AIP", "FT", None, 1, 0, False, 8),     # For m3 conditional tests - Additional Language Sign Manual + Sign Manual
            ("15", "AIP", "FT", None, 1, 0, False, 1),     # For m3 conditional tests - Two of the same spoken language
            ("16", "AIP", "FT", None, 1, 0, False, 6),     # For m3 conditional tests - Two of the same sign language
            ("17", "AIP", "FT", None, 1, 0, False, 5),     # For m3 conditional tests - Two of the same manual spoken language
            ("18", "AIP", "FT", None, 1, 0, False, 8),     # For m3 conditional tests - Two of the same manual sign language
            ("19", "AIP", "FT", None, 1, 0, False, None),  # For m3 conditional tests - Additional spoken language only
            ("20", "AIP", "FT", None, 1, 0, False, 0),     # For m3 conditional tests - Additional manual spoken language only
            ("21", "AIP", "FT", None, 1, 0, False, 0),     # For m3 conditional tests - Additional sign language only
            ("22", "AIP", "FT", None, 1, 0, False, None)   # For m3 conditional tests - Additional manual sign language only
        ]

        m2_data = [
            ("1", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("2", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("3", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("4", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("5", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("6", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("7", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("8", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("9", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("10", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("11", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("12", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("13", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("14", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("15", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("16", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("17", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("18", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("19", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("20", 1, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("21", 1, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("22", 1, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
        ]

        m3_data = [
            ("5", 1, 37, 0, 2),    # StatusId 1 Unused Additional Spoken Language Only
            ("5", 2, 37, 0, 3),    # StatusId 2 First Additional Spoken Language Only (to Spoken + Spoken)
            ("6", 1, 37, 0, 5),    # Additional Manual Language Entry (to Spoken + Spoken Manual)
            ("7", 1, 37, 0, 6),    # Additional Sign Language (to Spoken + Sign)
            ("8", 1, 37, 0, 8),    # Additional Sign Manual Language (to Spoken + Sign Manual)
            ("9", 1, 37, 0, 7),    # Additional Sign Language (to Sign + Sign)
            ("10", 1, 37, 0, 5),   # Additional Manual Language Entry (to Sign + Spoken Manual)
            ("11", 1, 37, 0, 8),   # Additional Sign Manual Language (to Sign + Sign Manual)
            ("12", 1, 37, 0, 9),   # Additional Spoken Manual Language (to Spoken Manual + Spoken Manual)
            ("13", 1, 37, 0, 9),   # Additional Spoken Manual Language (to Sign Manual + Spoken Manual)
            ("14", 1, 37, 0, 10),  # Additional Sign Manual Language (to Sign Manual + Sign Manual)
            ("15", 1, 37, 0, 1),   # Additional same spoken language
            ("16", 1, 37, 0, 6),   # Additional same sign language
            ("17", 1, 37, 0, 5),   # Additional same manual spoken language
            ("18", 1, 37, 0, 8),   # Additional same manual sign language
            ("19", 1, 37, 0, 1),   # Additional only spoken language
            ("20", 1, 37, 0, 5),   # Additional only manual spoken language
            ("21", 1, 37, 0, 6),   # Additional only sign language
            ("22", 1, 37, 0, 8)    # Additional only manual sign language
        ]

        silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)
        silver_m2_test_data = spark.createDataFrame(m2_data, self.M2_COLUMNS)
        silver_m3_test_data = spark.createDataFrame(m3_data, self.M3_COLUMNS)
        silver_c_test_data = spark.createDataFrame([], self.C_COLUMNS)

        df, df_audit = listing.hearingRequirements(silver_m1_test_data, silver_m2_test_data, silver_m3_test_data, silver_c_test_data, bronze_interpreter_languages_test_data)

        expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/hearing_requirements/interpreter_languages_output.jsonl")
        # expected_audit_output_df = spark.read.schema(df_audit.schema).json("tests/active/listing/resources/hearing_requirements/interpreter_languages_audit_output.jsonl")

        assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
        # assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)

    def test_hearing_requirements_m3_conditional_fields(self, spark, bronze_interpreter_languages_test_data):
        m1_data = [
            ("1", "AIP", "FT", None, 1, 0, False, 0),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 0
            ("2", "AIP", "FT", None, 1, 0, False, 0),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 27
            ("3", "AIP", "FT", None, 1, 0, False, 0),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 37
            ("4", "AIP", "FT", None, 1, 0, False, 0),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 39
            ("5", "AIP", "FT", None, 1, 0, False, 0),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 40
            ("6", "AIP", "FT", None, 1, 0, False, 0),   # For m3 conditional tests - CaseStatus = 37 AND Outcome = 50
            ("7", "AIP", "FT", None, 1, 0, False, 0),   # For m3 conditional tests - CaseStatus = 38 AND Outcome = 0
            ("8", "AIP", "FT", None, 1, 0, False, 0),   # For m3 conditional tests - CaseStatus = 38 AND Outcome = 27
            ("9", "AIP", "FT", None, 1, 0, False, 0),   # For m3 conditional tests - CaseStatus = 38 AND Outcome = 37
            ("10", "AIP", "FT", None, 1, 0, False, 0),  # For m3 conditional tests - CaseStatus = 38 AND Outcome = 39
            ("11", "AIP", "FT", None, 1, 0, False, 0),  # For m3 conditional tests - CaseStatus = 38 AND Outcome = 40
            ("12", "AIP", "FT", None, 1, 0, False, 0),  # For m3 conditional tests - CaseStatus = 38 AND Outcome = 50
            ("13", "AIP", "FT", None, 1, 0, False, 0),  # For m3 conditional tests - CaseStatus = 26 AND Outcome = 40
            ("14", "AIP", "FT", None, 1, 0, False, 0),  # For m3 conditional tests - CaseStatus = 26 AND Outcome = 52
            ("15", "AIP", "FT", None, 1, 0, False, 0),  # For m3 conditional tests - Not Matching CaseStatus: CaseStatus = 39 AND Outcome = 0
            ("16", "AIP", "FT", None, 1, 0, False, 0),  # For m3 conditional tests - Not Matching Outcome: CaseStatus = 37 AND Outcome = 52
            ("17", "AIP", "FT", None, 1, 0, False, 0),  # For m3 conditional tests - Not Matching Outcome: CaseStatus = 26 AND Outcome = 0
            ("18", "AIP", "FT", None, 1, 0, False, 0),  # For m3 conditional tests - Not Matching Outcome: CaseStatus = 0 AND Outcome = 1
            ("19", "AIP", "FT", None, 1, 0, False, 0),  # For m3 conditional tests - StatusId Check (Many Statuses)
            ("20", "AIP", "FT", None, 1, 0, False, 0)   # For m3 conditional tests - StatusId Check (CaseStatus and Outcome)
        ]

        m2_data = [
            ("1", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("2", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("3", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("4", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("5", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("6", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("7", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("8", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("9", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("10", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("11", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("12", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("13", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("14", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("15", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("16", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("17", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("18", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("19", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("20", 1, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
        ]

        m3_data = [
            ("1", 1, 37, 0, 1),    # CaseStatus = 37 AND Outcome = 0
            ("2", 1, 37, 27, 1),   # CaseStatus = 37 AND Outcome = 27
            ("3", 1, 37, 37, 1),   # CaseStatus = 37 AND Outcome = 37
            ("4", 1, 37, 39, 1),   # CaseStatus = 37 AND Outcome = 39
            ("5", 1, 37, 40, 1),   # CaseStatus = 37 AND Outcome = 40
            ("6", 1, 37, 50, 1),   # CaseStatus = 37 AND Outcome = 50
            ("7", 1, 38, 0, 1),    # CaseStatus = 38 AND Outcome = 0
            ("8", 1, 38, 27, 1),   # CaseStatus = 38 AND Outcome = 27
            ("9", 1, 38, 37, 1),   # CaseStatus = 38 AND Outcome = 37
            ("10", 1, 38, 39, 1),  # CaseStatus = 38 AND Outcome = 39
            ("11", 1, 38, 40, 1),  # CaseStatus = 38 AND Outcome = 40
            ("12", 1, 38, 50, 1),  # CaseStatus = 38 AND Outcome = 50
            ("13", 1, 26, 37, 1),  # CaseStatus = 26 AND Outcome = 40
            ("14", 1, 26, 52, 1),  # CaseStatus = 26 AND Outcome = 52
            ("15", 1, 39, 0, 1),   # Not Matching CaseStatus: CaseStatus = 39 AND Outcome = 0
            ("16", 1, 37, 52, 1),  # Not Matching Outcome: CaseStatus = 37 AND Outcome = 52
            ("17", 1, 26, 0, 1),   # Not Matching Outcome: CaseStatus = 26 AND Outcome = 0
            ("18", 1, 0, 1, 1),    # Not Matching Outcome: CaseStatus = 0 AND Outcome = 1
            ("19", 1, 0, 0, 1),    # StatusId 1 Does Not Match
            ("19", 2, 37, 0, 2),   # StatusId 2 CaseStatus = 37 AND Outcome = 0
            ("19", 3, 0, 50, 3),   # StatusId 3 Does Not Match
            ("19", 4, 37, 50, 4),  # StatusId 4 Updated CaseStatus = 37 AND Outcome = 50
            ("19", 5, 50, 50, 5),  # StatusId 5 Does Not Match
            ("20", 1, 38, 0, 1),   # CaseStatus and Outcome update test: StatusId 1 CaseStatus = 38 AND Outcome = 0
            ("20", 2, 37, 50, 2),  # CaseStatus and Outcome update test: StatusId 2 CaseStatus = 37 AND Outcome = 50
            ("20", 3, 26, 40, 3),  # CaseStatus and Outcome update test: StatusId 3 CaseStatus = 26 AND Outcome = 40
        ]

        silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)
        silver_m2_test_data = spark.createDataFrame(m2_data, self.M2_COLUMNS)
        silver_m3_test_data = spark.createDataFrame(m3_data, self.M3_COLUMNS)
        silver_c_test_data = spark.createDataFrame([], self.C_COLUMNS)

        df, df_audit = listing.hearingRequirements(silver_m1_test_data, silver_m2_test_data, silver_m3_test_data, silver_c_test_data, bronze_interpreter_languages_test_data)

        expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/hearing_requirements/m3_conditional_output.jsonl")
        # expected_audit_output_df = spark.read.schema(df_audit.schema).json("tests/active/listing/resources/hearing_requirements/m3_conditional_audit_output.jsonl")

        assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
        # assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)

    def test_hearing_requirements_category_fields(self, spark, bronze_interpreter_languages_test_data):
        m1_data = [
            ("1", "AIP", "FT", "Sponsor", 0, 0, False, 0),  # Category isEvidenceFromOutsideUkOoc and has sponsor
            ("2", "AIP", "FT", "Sponsor", 0, 0, False, 0),  # Category isEvidenceFromOutsideUkInCountry and has sponsor
            ("3", "AIP", "FT", None, 0, 0, False, 0),       # Category isEvidenceFromOutsideUkOoc but no sponsor
            ("4", "AIP", "FT", None, 0, 0, False, 0),       # Category isEvidenceFromOutsideUkInCountry but no sponsor
            ("5", "AIP", "FT", "Sponsor", 0, 0, False, 0),  # No matching category
            ("6", "AIP", "FT", "Sponsor", 0, 0, False, 0),  # Both isEvidenceFromOutsideUkOoc and isEvidenceFromOutsideUkInCountry and has sponsor
            ("7", "AIP", "FT", "SPonsor", 0, 0, False, 0)   # Multiple categories including isEvidenceFromOutsideUkInCountry and has sponsor
        ]

        m2_data = [
            ("1", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("2", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("3", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("4", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("5", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("6", 3, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None),
            ("7", 1, 1, "SW1A 1AA", "1 Test St", None, None, None, None, None)
        ]

        c_data = [
            ("1", 37),  # Category 37 for isEvidenceFromOutsideUkOoc
            ("2", 38),  # Category 38 for isEvidenceFromOutsideUkInCountry
            ("3", 37),  # Category 37 for isEvidenceFromOutsideUkOoc
            ("4", 38),  # Category 38 for isEvidenceFromOutsideUkInCountry
            ("5", 39),  # Category 39 with no match
            ("5", 40),  # Category 40 with no match
            ("6", 37),  # Category 37 AND 38
            ("6", 38),  # Category 37 AND 38
            ("7", 38),  # Category 38 matches
            ("7", 39),  # Category 38 matches
            ("7", 40)   # Category 38 matches
        ]

        silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)
        silver_m2_test_data = spark.createDataFrame(m2_data, self.M2_COLUMNS)
        silver_m3_test_data = spark.createDataFrame([], self.M3_COLUMNS)
        silver_c_test_data = spark.createDataFrame(c_data, self.C_COLUMNS)

        df, df_audit = listing.hearingRequirements(silver_m1_test_data, silver_m2_test_data, silver_m3_test_data, silver_c_test_data, bronze_interpreter_languages_test_data)

        expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/hearing_requirements/category_output.jsonl")
        # expected_audit_output_df = spark.read.schema(df_audit.schema).json("tests/active/listing/resources/hearing_requirements/category_audit_output.jsonl")

        assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
        # assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)

    def test_general_fields(self, spark, bronze_hearing_centres_test_data, bronze_derive_hearing_centres_test_data, bronze_detention_centres_test_data):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.listing.PPD') as PP:
            paymentPendingCaseOutput = [("1",), ("2",), ("3",), ("4",), ("5",), ("6",)]  # listing joins left on the paymentPendingOutput, trailing comma for tuple type
            PP.general.return_value = spark.createDataFrame(paymentPendingCaseOutput, self.CASE_NO_COLUMNS), spark.createDataFrame(paymentPendingCaseOutput, self.CASE_NO_COLUMNS)
            m1_data = [
                ("1", "AIP", "FT", None, 0, 0, False, 0),  # AIP Case
                ("2", "LR", "FT", None, 0, 0, False, 0),   # LR Case
                ("3", "AIP", None, None, 0, 0, False, 0),  # AIP Case no appealType
                ("4", "LR", None, None, 0, 0, False, 0),   # LR Case no appealType
                ("5", None, "FT", None, 0, 0, False, 0),   # No representation
                ("6", "UN", "FT", None, 0, 0, False, 0)    # Not AIP or LR Case
            ]

            dcs_schema = StructType([
            StructField("Detained", IntegerType(), True),
            StructField("DetentionCentreId", IntegerType(), True),
            StructField("DetentionCentre", StringType(), True),
            StructField("prisonName", StringType(), True),
            StructField("ircName", StringType(), True),
            StructField("detentionBuilding", StringType(), True),
            StructField("detentionAddressLines", StringType(), True),
            StructField("detentionPostcode", StringType(), True),
            StructField("hearingCentre", StringType(), True),
            StructField("staffLocation", StringType(), True),

            # caseManagementLocation as STRUCT
            StructField(
                "caseManagementLocation",
                StructType([
                    StructField("region", StringType(), True),
                    StructField("baseLocation", StringType(), True),
                ]),
                True
            ),

            StructField("locationCode", StringType(), True),
            StructField("locationLabel", StringType(), True),
            StructField("selectedHearingCentreRefData", StringType(), True),
            StructField("applicationChangeDesignatedHearingCentre", StringType(), True),
            ])

            silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)
            silver_m2_test_data = spark.createDataFrame([], self.M2_COLUMNS)
            silver_m3_test_data = spark.createDataFrame([], self.M3_COLUMNS)
            silver_h_test_data = spark.createDataFrame([], self.CASE_NO_COLUMNS)


            df, df_audit = listing.general(silver_m1_test_data, silver_m2_test_data, silver_m3_test_data, silver_h_test_data, bronze_hearing_centres_test_data, bronze_derive_hearing_centres_test_data,bronze_detention_centres_test_data)

            expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/general/general_output.jsonl")
            # expected_audit_output_df = spark.read.schema(df_audit.schema).json("tests/active/listing/resources/general/general_audit_output.jsonl")

            assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
            # assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)

    def test_general__default_fields(self, spark):
        m1_data = [
            ("1", "AIP", "FT", None, 0, 0, False, 0),  # Defaults 1
            ("2", "LR", "FT", None, 0, 0, False, 0)    # Defaults 2
        ]

        silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)

        df = listing.generalDefault(silver_m1_test_data)

        expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/general_default/general_default_output.jsonl")

        assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)

    def test_documents_fields(self, spark):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.listing.AERb') as AERb:
            aerBCaseOutput = [("1",), ("2",), ("3",), ("4",), ("5",), ("6",)]  # listing joins left on the AERb output, trailing comma for tuple type
            AERb.documents.return_value = spark.createDataFrame(aerBCaseOutput, self.CASE_NO_COLUMNS), spark.createDataFrame(aerBCaseOutput, self.CASE_NO_COLUMNS)
            m1_data = [
                ("1", "AIP", "FT", None, 0, 0, False, 0),  # Defaults 1
                ("2", "LR", "FT", None, 0, 0, False, 0),   # Defaults 2
                ("3", "LR", "FT", None, 0, 0, False, 0),   # Defaults 2
                ("4", "LR", "FT", None, 0, 0, False, 0),   # Defaults 2
                ("5", "LR", "FT", None, 0, 0, False, 0),   # Defaults 2
                ("6", "LR", "FT", None, 0, 0, False, 0),   # Defaults 2
                
            ]

            silver_m1_test_data = spark.createDataFrame(m1_data, self.M1_COLUMNS)

            df, df_audit = listing.documents(silver_m1_test_data)

            expected_output_df = spark.read.schema(df.schema).json("tests/active/listing/resources/documents/documents_output.jsonl")
            expected_audit_output_df = spark.read.schema(df_audit.schema).json("tests/active/listing/resources/documents/documents_audit_output.jsonl")

            assertDataFrameEqual(df, expected_output_df, showOnlyDiff=True)
            # assertDataFrameEqual(df_audit, expected_audit_output_df, showOnlyDiff=True)

    def test_flags_labels(self, spark, bronze_interpreter_languages_test_data):
        with patch('Databricks.ACTIVE.APPEALS.shared_functions.listing.PP') as mock_PP:
            path_item_schema = StructType([
                StructField("id", StringType(), True),
                StructField("value", StringType(), True),
            ])
            flag_value_schema = StructType([
                StructField("name", StringType(), True),
                StructField("path", ArrayType(path_item_schema), True),
                StructField("status", StringType(), True),
                StructField("flagCode", StringType(), True),
                StructField("flagComment", StringType(), True),
                StructField("subTypeKey", StringType(), True),
                StructField("subTypeValue", StringType(), True),
                StructField("dateTimeCreated", StringType(), True),
                StructField("hearingRelevant", StringType(), True),
            ])
            flag_detail_schema = StructType([
                StructField("id", StringType(), True),
                StructField("value", flag_value_schema, True),
            ])
            appellant_flags_schema = StructType([
                StructField("details", ArrayType(flag_detail_schema), True),
                StructField("partyName", StringType(), True),
                StructField("roleOnCase", StringType(), True),
            ])
            base_df_schema = StructType([
                StructField("CaseNo", StringType(), True),
                StructField("appellantLevelFlags", appellant_flags_schema, True),
            ])

            base_data = [
                Row(CaseNo="CASE1", appellantLevelFlags=None),
                Row(CaseNo="CASE2", appellantLevelFlags=None),
                Row(CaseNo="CASE3", appellantLevelFlags=Row(
                    details=[Row(
                        id="existing-id",
                        value=Row(
                            name="Prior Flag",
                            path=[Row(id="path-id", value="Party")],
                            status="Active",
                            flagCode="PF0012",
                            flagComment=None,
                            subTypeKey=None,
                            subTypeValue=None,
                            dateTimeCreated="2024-01-01T00:00:00Z",
                            hearingRelevant="Yes"
                        )
                    )],
                    partyName="Jane Doe",
                    roleOnCase="Appellant"
                )),
                Row(CaseNo="CASE4", appellantLevelFlags=None),
                Row(CaseNo="CASE5", appellantLevelFlags=None),
            ]

            mock_base_df = spark.createDataFrame(base_data, base_df_schema)
            mock_PP.flagsLabels.return_value = (mock_base_df, mock_base_df)

            m1_schema = StructType([
                StructField("CaseNo", StringType(), True),
                StructField("AppellantForenames", StringType(), True),
                StructField("AppellantName", StringType(), True),
                StructField("LanguageId", IntegerType(), True),
            ])
            silver_m1 = spark.createDataFrame([
                ("CASE1", "Alice", "Smith", None),   # no interpreter language
                ("CASE2", "Bob", "Jones", 1),         # spoken English (non-manual)
                ("CASE3", "Jane", "Doe", 6),          # sign BSL (non-manual)
                ("CASE4", "Tom", "Brown", 5),         # spoken manual entry
                ("CASE5", "Mary", "White", 8),        # sign manual entry
            ], m1_schema)

            m2_schema = StructType([
                StructField("CaseNo", StringType(), True),
                StructField("Relationship", StringType(), True),
                StructField("Appellant_Forenames", StringType(), True),
                StructField("Appellant_Name", StringType(), True),
            ])
            silver_m2 = spark.createDataFrame([
                ("CASE1", None, "Alice", "Smith"),
                ("CASE2", None, "Bob", "Jones"),
                ("CASE3", None, "Jane", "Doe"),
                ("CASE4", None, "Tom", "Brown"),
                ("CASE5", None, "Mary", "White"),
            ], m2_schema)

            silver_c = spark.createDataFrame([
                ("CASE2", 41),
                ("CASE3", 41),
                ("CASE4", 41),
                ("CASE5", 41),
            ], self.C_COLUMNS)

            silver_m3 = spark.createDataFrame([], self.M3_COLUMNS)

            df, _ = listing.flagsLabels(silver_m1, silver_m2, silver_c, silver_m3, bronze_interpreter_languages_test_data)
            results = {row["CaseNo"]: row.asDict() for row in df.collect()}

            # CASE1: no existing flags, no interpreter → gets 3 static flags
            case1_flags = results["CASE1"]["appellantLevelFlags"]
            assert case1_flags is not None
            case1_codes = [d["value"]["flagCode"] for d in case1_flags["details"]]
            assert case1_codes == ["RA0019", "RA0043", "PF0014"]
            assert case1_flags["partyName"] == "Alice Smith"
            assert case1_flags["roleOnCase"] == "Appellant"

            # CASE2: spoken English → 3 static flags + Language Interpreter (PF0015)
            case2_flags = results["CASE2"]["appellantLevelFlags"]
            assert case2_flags is not None
            case2_codes = [d["value"]["flagCode"] for d in case2_flags["details"]]
            assert case2_codes == ["RA0019", "RA0043", "PF0014", "PF0015"]
            assert case2_flags["partyName"] == "Bob Jones"
            assert case2_flags["roleOnCase"] == "Appellant"
            case2_pf0015 = next(d for d in case2_flags["details"] if d["value"]["flagCode"] == "PF0015")
            assert case2_pf0015["value"]["subTypeKey"] == "en"
            assert case2_pf0015["value"]["subTypeValue"] == "English"

            # CASE3: existing flags, sign BSL → prior flag + 3 static + Sign Language Interpreter (RA0042)
            case3_flags = results["CASE3"]["appellantLevelFlags"]
            assert case3_flags is not None
            case3_codes = [d["value"]["flagCode"] for d in case3_flags["details"]]
            assert case3_codes == ["PF0012", "RA0019", "RA0043", "PF0014", "RA0042"]
            assert len(case3_flags["details"]) == 5
            case3_ra0042 = next(d for d in case3_flags["details"] if d["value"]["flagCode"] == "RA0042")
            assert case3_ra0042["value"]["subTypeKey"] == "bfi"
            assert case3_ra0042["value"]["subTypeValue"] == "British Sign Language (BSL)"

            # CASE4: has category, spoken manual entry → 3 static + Language Interpreter (PF0015, no subTypeKey)
            case4_flags = results["CASE4"]["appellantLevelFlags"]
            assert case4_flags is not None
            case4_codes = [d["value"]["flagCode"] for d in case4_flags["details"]]
            assert case4_codes == ["RA0019", "RA0043", "PF0014", "PF0015"]
            case4_pf0015 = next(d for d in case4_flags["details"] if d["value"]["flagCode"] == "PF0015")
            assert case4_pf0015["value"]["subTypeKey"] is None
            assert case4_pf0015["value"]["subTypeValue"] == "Manual Entry"

            # CASE5: has category, sign manual entry → 3 static + Sign Language Interpreter (RA0042, no subTypeKey)
            case5_flags = results["CASE5"]["appellantLevelFlags"]
            assert case5_flags is not None
            case5_codes = [d["value"]["flagCode"] for d in case5_flags["details"]]
            assert case5_codes == ["RA0019", "RA0043", "PF0014", "RA0042"]
            case5_ra0042 = next(d for d in case5_flags["details"] if d["value"]["flagCode"] == "RA0042")
            assert case5_ra0042["value"]["subTypeKey"] is None
            assert case5_ra0042["value"]["subTypeValue"] == "Sign Language (Other)"
