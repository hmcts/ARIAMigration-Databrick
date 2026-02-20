from Databricks.ACTIVE.APPEALS.shared_functions.reasonsForAppealSubmitted import hearingResponse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from textwrap import dedent

import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("reasonsForAppealSubmitted_hearingResponse") \
        .getOrCreate()


class TestReasonForAppealSubmittedHearingResponse:
    SILVER_M1_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("dv_representation", StringType()),
        StructField("dv_CCDAppealType", StringType())
    ])

    SILVER_M3_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("StatusId", IntegerType()),
        StructField("CaseStatus", IntegerType()),
        StructField("Outcome", IntegerType()),
        StructField("ListedCentre", StringType()),
        StructField("KeyDate", StringType()),
        StructField("HearingType", StringType()),
        StructField("CourtName", StringType()),
        StructField("ListType", StringType()),
        StructField("StartTime", StringType()),
        StructField("TimeEstimate", StringType()),
        StructField("Notes", StringType()),
        StructField("Judge1FT_Surname", StringType()),
        StructField("Judge1FT_Forenames", StringType()),
        StructField("Judge1FT_Title", StringType()),
        StructField("Judge2FT_Surname", StringType()),
        StructField("Judge2FT_Forenames", StringType()),
        StructField("Judge2FT_Title", StringType()),
        StructField("Judge3FT_Surname", StringType()),
        StructField("Judge3FT_Forenames", StringType()),
        StructField("Judge3FT_Title", StringType()),
        StructField("CourtClerk_Surname", StringType()),
        StructField("CourtClerk_Forenames", StringType()),
        StructField("CourtClerk_Title", StringType())
    ])

    SILVER_M6_SCHEMA = StructType([
        StructField("CaseNo", StringType()),
        StructField("Required", StringType()),
        StructField("Judge_Surname", StringType()),
        StructField("Judge_Forenames", StringType()),
        StructField("Judge_Title", StringType())
    ])

    def test_additionalInstructionsTribunalResponse(self, spark):
        silver_m1 = spark.createDataFrame([
            ("1", "AIP", "appeal"),
            ("2", "AIP", "appeal"),
            ("3", "AIP", "appeal"),
            ("4", "AIP", "appeal"),
            ("5", "AIP", "appeal"),
            ("6", "AIP", "appeal"),
            ("7", "AIP", "appeal"),
            ("8", "LR", "appeal")
        ], self.SILVER_M1_SCHEMA)

        silver_m3 = spark.createDataFrame([
            (  # CaseStatus 26 and Outcome == 0 - valid
                "1", 1, 26, 0, "HearingCentre", "2000-01-01", "Type", "Court",
                "List", "10:00", "10", "Notes", None, None, None, None, None,
                None, None, None, None, None, None, None
            ),
            (  # CaseStatus 37 and StatusId 1 - skipped for condition CaseStatus in 37 or 38 and max StatusId
                "2", 1, 37, 1, "HearingCentre", "2000-01-01", "Type", "Court",
                "List", "10:00", "10", "Notes", None, None, None, None, None,
                None, None, None, None, None, None, None
            ),
            (  # CaseStatus 38 and StatusId 2 with no judicial details - valid replaces above
                "2", 2, 38, 1, "HearingCentre2", "2010-01-01", "Type2", "Court2",
                "List2", "11:00", "20", "Notes2", None, None, None, None, None,
                None, None, None, None, None, None, None
            ),
            (  # CaseStatus 37 with Judicial details - valid
                "3", 1, 37, 0, "HearingCentre", "2000-01-01", "Type", "Court",
                "List", "10:00", "10", "Notes",
                "Judge1Last", "Judge1First", "Mr",
                "Judge2Last", "Judge2First", "Mrs",
                "Judge3Last", "Judge3First", "Ms",
                "ClerkLast", "ClerkFirst", "Miss"
            ),
            (  # CaseStatus 39 - skipped for above case as not in CaseStatus 37 or 38
                "3", 2, 39, 0, "HearingCentre2", "2010-01-01", "Type2", "Court2",
                "Lis2t", "11:00", "20", "Notes2",
                "Judge1Last2", "Judge1First2", "Mr2",
                "Judge2Last2", "Judge2First2", "Mrs2",
                "Judge3Last2", "Judge3First2", "Ms2",
                "ClerkLast2", "ClerkFirst2", "Miss2"
            ),
            (  # CaseStatus 26 and Outcome == 0 but no values
                "4", 1, 26, 0, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None
            ),
            (  # CaseStatus 26 and Outcome == 1 - skipped as invalid
                "5", 1, 26, 1, "HearingCentre", "2000-01-01", "Type", "Court",
                "List", "10:00", "10", "Notes", None, None, None, None, None,
                None, None, None, None, None, None, None
            ),
            (  # CaseStatus 26 and Outcome == 0 - valid - for m6 entry single required judge
                "6", 1, 26, 0, "HearingCentre", "2000-01-01", "Type", "Court",
                "List", "10:00", "10", "Notes", None, None, None, None, None,
                None, None, None, None, None, None, None
            ),
            (  # CaseStatus 26 and Outcome == 0 - valid - for m6 entry multiple judges
                "7", 1, 26, 0, "HearingCentre", "2000-01-01", "Type", "Court",
                "List", "10:00", "10", "Notes", None, None, None, None, None,
                None, None, None, None, None, None, None
            ),
            (  # CaseStatus 26 and Outcome == 0 - skipped for m1 is AIP type not LR
                "8", 1, 26, 0, "HearingCentre", "2000-01-01", "Type", "Court",
                "List", "10:00", "10", "Notes", None, None, None, None, None,
                None, None, None, None, None, None, None
            )
        ], self.SILVER_M3_SCHEMA)

        silver_m6 = spark.createDataFrame([
            ("6", "1", "JudgeLastName", "JudgeFirstName", "Judge"),
            ("7", "0", "JudgeLastName", "JudgeFirstName", "Judge"),
            ("7", "1", "JudgeLastName2", "JudgeFirstName2", "Judge2"),
            ("7", "0", "JudgeLastName3", "JudgeFirstName3", "Judge3")
        ], self.SILVER_M6_SCHEMA)

        df, df_audit = hearingResponse(silver_m1, silver_m3, silver_m6)

        resultList = df.orderBy(col("CaseNo").cast("int")).select("additionalInstructionsTribunalResponse").collect()

        assert resultList[0][0] == dedent("""\
            Listed details from ARIA: 
            Hearing Centre: HearingCentre
            Hearing Date: 2000-01-01
            Hearing Type: Type
            Court: Court
            List Type: List
            List Start Time: 10:00
            Judge First Tier: 
            Court Clerk / Usher: N/A
            Start Time: 10:00
            Estimated Duration: 10
            Required/Incompatible Judicial Officers: 
            Notes: Notes\
        """).strip()
        assert resultList[1][0] == dedent("""\
            Listed details from ARIA: 
            Hearing Centre: HearingCentre2
            Hearing Date: 2010-01-01
            Hearing Type: Type2
            Court: Court2
            List Type: List2
            List Start Time: 11:00
            Judge First Tier: 
            Court Clerk / Usher: N/A
            Start Time: 11:00
            Estimated Duration: 20
            Required/Incompatible Judicial Officers: 
            Notes: Notes2
        """).strip()
        assert resultList[2][0] == dedent("""\
            Listed details from ARIA: 
            Hearing Centre: HearingCentre
            Hearing Date: 2000-01-01
            Hearing Type: Type
            Court: Court
            List Type: List
            List Start Time: 10:00
            Judge First Tier: Judge1Last Judge1First (Mr) Judge2Last Judge2First (Mrs) Judge3Last Judge3First (Ms)
            Court Clerk / Usher: ClerkLast ClerkFirst (Miss)
            Start Time: 10:00
            Estimated Duration: 10
            Required/Incompatible Judicial Officers: 
            Notes: Notes\
        """).strip()
        assert resultList[3][0] == dedent("""\
            Listed details from ARIA: 
            Hearing Centre: N/A
            Hearing Date: N/A
            Hearing Type: N/A
            Court: N/A
            List Type: N/A
            List Start Time: N/A
            Judge First Tier: 
            Court Clerk / Usher: N/A
            Start Time: N/A
            Estimated Duration: N/A
            Required/Incompatible Judicial Officers: 
            Notes: N/A\
        """).strip()
        assert resultList[4][0] == dedent("""\
            Listed details from ARIA: 
            Hearing Centre: HearingCentre
            Hearing Date: 2000-01-01
            Hearing Type: Type
            Court: Court
            List Type: List
            List Start Time: 10:00
            Judge First Tier: 
            Court Clerk / Usher: N/A
            Start Time: 10:00
            Estimated Duration: 10
            Required/Incompatible Judicial Officers: 
            JudgeLastName JudgeFirstName ( Judge ) : Required
            Notes: Notes\
        """).strip()
        assert resultList[5][0] == dedent("""\
            Listed details from ARIA: 
            Hearing Centre: HearingCentre
            Hearing Date: 2000-01-01
            Hearing Type: Type
            Court: Court
            List Type: List
            List Start Time: 10:00
            Judge First Tier: 
            Court Clerk / Usher: N/A
            Start Time: 10:00
            Estimated Duration: 10
            Required/Incompatible Judicial Officers: 
            JudgeLastName JudgeFirstName ( Judge ) : Not Required
            JudgeLastName2 JudgeFirstName2 ( Judge2 ) : Required
            JudgeLastName3 JudgeFirstName3 ( Judge3 ) : Not Required
            Notes: Notes\
        """).strip()
        assert len(resultList) == 6  # 2 non-matching conditions of the 8 cases provided, leaves 6.
