from Databricks.ACTIVE.APPEALS.shared_functions import listing_dq_rules

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

import pytest


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
            .appName("listing")
            .getOrCreate()
    )

@pytest.fixture(scope="session")
def dq_checks():
    return listing_dq_rules.add_checks({})


class TestListingDQChecks():

    LANGUAGE_CATEGORY_COLUMNS = StructType([
        StructField("CaseNo", StringType()),
        StructField("LanguageId", IntegerType()),
        StructField("AdditionalLanguageId", IntegerType()),
        StructField("appellantInterpreterLanguageCategory", ArrayType(StringType())),
        StructField("valid_languageCategory", StringType()),
        StructField("valid_additionalLanguageCategory", StringType())
    ])

    LANGUAGE_MAP_TYPE = StructType([StructField("code", StringType()), StructField("label", StringType())])

    LANGUAGE_REF_DATA_TYPE = StructType([StructField("value", LANGUAGE_MAP_TYPE)])

    APPELLANT_INTERPRETER_LANGUAGE_COLUMNS = StructType([
        StructField("CaseNo", StringType()),
        StructField("LanguageId", IntegerType()),
        StructField("AdditionalLanguageId", IntegerType()),
        StructField("appellantInterpreterSpokenLanguage", StructType([
            StructField("languageRefData", LANGUAGE_REF_DATA_TYPE),
            StructField("list_items", ArrayType(LANGUAGE_MAP_TYPE)),
            StructField("languageManualEntry", ArrayType(StringType())),

            StructField("languageManualEntryDescription", StringType())

        ])),
        StructField("appellantInterpreterSignLanguage", StructType([
            StructField("languageRefData", LANGUAGE_REF_DATA_TYPE),
            StructField("list_items", ArrayType(LANGUAGE_MAP_TYPE)),
            StructField("languageManualEntry", ArrayType(StringType())),

            StructField("languageManualEntryDescription", StringType())

        ])),
        StructField("valid_languageCategory", StringType()),
        StructField("valid_languageCode", StringType()),
        StructField("valid_languageLabel", StringType()),
        StructField("valid_manualEntry", StringType()),
        StructField("valid_manualEntryDescription", StringType()),
        StructField("valid_additionalLanguageCategory", StringType()),
        StructField("valid_additionalLanguageCode", StringType()),
        StructField("valid_additionalLanguageLabel", StringType()),
        StructField("valid_additionalManualEntry", StringType()),
        StructField("valid_additionalManualEntryDescription", StringType())
    ])

    def test_valid_appellantInterpreterLanguageCategory_check(self, spark, dq_checks):
        df = spark.createDataFrame([
            ("1", 0, 0, [], None, None),           # No languages - valid
            ("2", None, None, [], None, None),     # No languages with nulls - valid
            ("3", 1, 0, ["a"], "a", None),         # 1 language, no additional - valid
            ("4", 1, None, ["b"], "b", None),      # 1 language, additional is null - valid
            ("5", 0, 2, ["c"], None, "c"),         # Additional language only - valid
            ("6", None, 2, ["c"], None, "c"),      # Additional language only, primary null - valid
            ("7", 0, 0, ["d"], "d", "d"),          # Category is set despite no languages, with valid inputs set - invalid
            ("8", None, None, ["e"], None, None),  # Category is set with null languages - invalid
            ("9", 0, 0, [], "f", "f"),             # No languages, even with valid inputs set - valid (as valid inputs are dropped from final_df)
            ("10", 3, 4, ["g", "h"], "g", "h"),    # Both language and additional, set as expected - valid
            ("11", 3, 4, [], "g", "h"),            # Both languages but no categories set - invalid
            ("12", 3, 4, ["g", "h"], None, None),  # Both languages with categories set, but unable to confirm due to missing valid inputs - invalid NULL
            ("13", 5, 6, ["i", "j"], "k", "l"),    # Both languages with categories set but differing valid inputs - invalid
            ("14", 5, 6, ["i"], "i", "i"),         # Both languages but 1 category entry when duplicate in valid inputs - valid
            ("15", 5, 0, ["i"], "j", None),        # 1 language, but mismatch in valid inputs value - invalid
            ("16", 5, 0, ["i"], None, "i"),        # 1 language, but mismatch of valid inputs between primary and additional - invalid
            ("17", 0, 6, ["j"], "j", None),        # 1 additional language, but mismatch in valid inputs between primary and additional - invalid
            ("18", 0, 6, ["j"], None, "i")         # 1 additional language, but mismatch of valid inputs value - invalid
        ], self.LANGUAGE_CATEGORY_COLUMNS)

        checks_to_run = dq_checks["valid_appellantInterpreterLanguageCategory"]

        df = df.withColumn("is_valid", expr(checks_to_run)).sort(col("CaseNo").cast("int"))

        df_checks = df.select("is_valid").collect()

        assert df_checks[0][0] is True and df_checks[1][0] is True and df_checks[2][0] is True and df_checks[3][0] is True and df_checks[4][0] is True and df_checks[5][0] is True
        assert df_checks[6][0] is not True and df_checks[7][0] is not True
        assert df_checks[8][0] is True and df_checks[9][0] is True
        assert df_checks[10][0] is not True and df_checks[11][0] is not True and df_checks[12][0] is not True
        assert df_checks[13][0] is True
        assert df_checks[14][0] is not True and df_checks[15][0] is not True and df_checks[16][0] is not True and df_checks[17][0] is not True

    def test_valid_appellantInterpreterSpokenLanguage(self, spark, dq_checks):
        df = spark.createDataFrame([

            ("1", 1, 2, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "b, d"}, None, "spokenLanguageInterpreter", "a", "b", None, None, "spokenLanguageInterpreter", "c", "d", None, None),                                                                   # Two languages - valid
            ("2", 1, 3, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "b, desc"}, None, "spokenLanguageInterpreter", "a", "b", None, None, "spokenLanguageInterpreter", None, None, "Yes", "desc"),                                                           # One lanuage, 1 manual language - valid
            ("3", 4, 3, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "desc, desc_two"}, None, "spokenLanguageInterpreter", None, None, "Yes", "desc", "spokenLanguageInterpreter", None, None, "Yes", "desc_two"),                                           # Two manual languages - valid
            ("4", 1, 0, {"languageRefData": {"value": {"code": "a", "label": "b"}}, "list_items": [{"code": "a", "label": "b"}, {"code": "c", "label": "d"}], "languageManualEntry": []}, None, "spokenLanguageInterpreter", "a", "b", None, None, None, None, None, None, None),  # One language only - valid
            ("5", 3, 0, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "desc"}, None, "spokenLanguageInterpreter", None, None, "Yes", "desc", None, None, None, None, None),                                                                                   # One manual anguage only - valid
            ("6", 0, 0, None, None, None, None, None, None, None, None, None, None, None, None),                                                                                                                                                                                   # No languages - valid
            ("7", None, None, None, None, None, None, None, None, None, None, None, None, None, None),                                                                                                                                                                             # No languages since all null - valid
            ("8", 0, 0, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "a, desc"}, None, "spokenLanguageInterpreter", "a", "b", None, None, "spokenLanguageInterpreter", None, None, "Yes", "desc"),                                                           # No languages, but languageRefData entry - invalid
            ("9", 5, 0, None, {"languageRefData": {"value": {"code": "a", "label": "b"}}, "list_items": [{"code": "a", "label": "b"}, {"code": "c", "label": "d"}], "languageManualEntry": []}, "signLanguageInterpreter", "a", "b", None, None, None, None, None, None, None),    # LanguageIds set but no spoken languages - valid
            ("10", 5, 6, None, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "b, d"}, "signLanguageInterpreter", "a", "b", None, None, "signLanguageInterpreter", "c", "d", None, None),                                                                      # Two LanguageId set but no spoken languages - valid
            ("11", 5, 0, {"languageRefData": {"value": {"code": "a", "label": "b"}}, "list_items": [{"code": "a", "label": "b"}, {"code": "c", "label": "d"}], "languageManualEntry": []}, None, "signLanguageInterpreter", "a", "b", None, None, None, None, None, None, None),   # Sign languageId set but has spoken language - invalid

        ], self.APPELLANT_INTERPRETER_LANGUAGE_COLUMNS)

        checks_to_run = dq_checks["valid_appellantInterpreterSpokenLanguage"]

        df = df.withColumn("is_valid", expr(checks_to_run)).sort(col("CaseNo").cast("int"))

        df_checks = df.select("is_valid").collect()

        assert df_checks[0][0] is True and df_checks[1][0] is True and df_checks[2][0] is True and df_checks[3][0] is True
        assert df_checks[4][0] is True and df_checks[5][0] is True and df_checks[6][0] is True
        assert df_checks[7][0] is not True
        assert df_checks[8][0] is True and df_checks[9][0] is True
        assert df_checks[10][0] is not True

    def test_valid_appellantInterpreterSignLanguage(self, spark, dq_checks):
        df = spark.createDataFrame([
            ("1", 1, 2, None, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "b, d"}, "signLanguageInterpreter", "a", "b", None, None, "signLanguageInterpreter", "c", "d", None, None),                                                                        # Two sign languages - valid
            ("2", 1, 3, None, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "b, desc"}, "signLanguageInterpreter", "a", "b", None, None, "signLanguageInterpreter", None, None, "Yes", "desc"),                                                                # One sign lanuage, 1 manual sign language - valid
            ("3", 4, 3, None, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "desc, desc_two"}, "signLanguageInterpreter", None, None, "Yes", "desc", "signLanguageInterpreter", None, None, "Yes", "desc_two"),                                                # Two manual sign languages - valid
            ("4", 1, 0, None, {"languageRefData": {"value": {"code": "a", "label": "b"}}, "list_items": [{"code": "a", "label": "b"}, {"code": "c", "label": "d"}], "languageManualEntry": []}, "signLanguageInterpreter", "a", "b", None, None, None, None, None, None, None),     # One sign language only - valid
            ("5", 3, 0, None, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "desc"}, "signLanguageInterpreter", None, None, "Yes", "desc", None, None, None, None, None),                                                                                      # One manual sign language only - valid
            ("6", 0, 0, None, None, None, None, None, None, None, None, None, None, None, None),                                                                                                                                                                                    # No languages - valid
            ("7", None, None, None, None, None, None, None, None, None, None, None, None, None, None),                                                                                                                                                                              # No languages since all null - valid
            ("8", 0, 0, None, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "b, desc"}, "signLanguageInterpreter", "a", "b", None, None, "signLanguageInterpreter", None, None, "Yes", "b, desc"),                                                             # No languages, but languageRefData entry - invalid
            ("9", 5, 0, {"languageRefData": {"value": {"code": "a", "label": "b"}}, "list_items": [{"code": "a", "label": "b"}, {"code": "c", "label": "d"}], "languageManualEntry": []}, None, "spokenLanguageInterpreter", "a", "b", None, None, None, None, None, None, None),   # LanguageIds set but no sign languages - valid
            ("10", 5, 6, {"languageManualEntry": ["Yes"], "languageManualEntryDescription": "b, d"}, None, "spokenLanguageInterpreter", "a", "b", None, None, "spokenLanguageInterpreter", "c", "d", None, None),                                                                   # Two LanguageId set but no sign languages - valid
            ("11", 5, 0, None, {"languageRefData": {"value": {"code": "a", "label": "b"}}, "list_items": [{"code": "a", "label": "b"}, {"code": "c", "label": "d"}], "languageManualEntry": []}, "spokenLanguageInterpreter", "a", "b", None, None, None, None, None, None, None),  # Sign languageId set but has spoken language - invalid

        ], self.APPELLANT_INTERPRETER_LANGUAGE_COLUMNS)

        checks_to_run = dq_checks["valid_appellantInterpreterSignLanguage"]

        df = df.withColumn("is_valid", expr(checks_to_run)).sort(col("CaseNo").cast("int"))

        df_checks = df.select("is_valid").collect()

        assert df_checks[0][0] is True and df_checks[1][0] is True and df_checks[2][0] is True and df_checks[3][0] is True
        assert df_checks[4][0] is True and df_checks[5][0] is True and df_checks[6][0] is True
        assert df_checks[7][0] is not True
        assert df_checks[8][0] is True and df_checks[9][0] is True
        assert df_checks[10][0] is not True
