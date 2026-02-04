from datetime import datetime
import re
import string
import pycountry
import pandas as pd
import json

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from . import AwaitingEvidenceRespondant_b as AERb
from . import listing as L
from . import prepareForHearing as PFH
from . import decision as D
from . import decided_a as DA
from . import ftpa_submitted_a as FSA

from pyspark.sql.functions import (
    col, when, lit, array, struct, collect_list, 
    max as spark_max, date_format, row_number, expr, regexp_replace,
    size, udf, coalesce, concat_ws, concat, trim, year, split, datediff,
    collect_set, current_timestamp,transform, first, array_contains,rank,create_map, map_from_entries, map_from_arrays
)

################################################################
##########              ftpa          ###########
################################################################

def ftpa(silver_m3,silver_c):

    ftpa_df,ftpa_audit = FSA.ftpa(silver_m3,silver_c)

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    silver_m3_filtered_casestatus = silver_m3.filter(col("CaseStatus").isin(39))
    silver_m3_ranked = silver_m3_filtered_casestatus.withColumn("row_number", row_number().over(window_spec))
    silver_m3_max_statusid = silver_m3_ranked.filter(col("row_number") == 1).drop("row_number")


    silver_m3_content = (
        silver_m3_max_statusid
            # Appellant fields
            .withColumn("judgeAllocationExists",lit("Yes"))
            .withColumn("allocatedJudge",concat(col("Adj_Title"),lit(" "),col("Adj_Forenames"),lit(" "),col("Adj_Surname")))
            .withColumn("allocatedJudgeEdit",concat(col("Adj_Title"),lit(" "),col("Adj_Forenames"),lit(" "),col("Adj_Surname")))
            .select(
                col("CaseNo"),
                col("judgeAllocationExists"),
                col("allocatedJudge"),
                col("allocatedJudgeEdit"),
            )
    )

    ftpa_df = (
        ftpa_df.alias("ftpa")
            .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
            .select(
                "ftpa.*",
                col("judgeAllocationExists"),
                col("allocatedJudge"),
                col("allocatedJudgeEdit"),
            )
    )

    # Build the audit DataFrame
    ftpa_audit = (
        ftpa_audit.alias("audit")
            .join(ftpa_df.alias("ftpa"), on=["CaseNo"], how="left")
            .join(silver_m3_max_statusid.alias("m3"), on=["CaseNo"], how="left")
            .select(
                "audit.*",
                array(struct(lit("judgeAllocationExists"))).alias("judgeAllocationExists_inputFields"),
                array(struct(lit("Null"))).alias("judgeAllocationExists_inputValues"),
                col("judgeAllocationExists").alias("judgeAllocationExists_value"),
                lit("Yes").alias("judgeAllocationExists_Transformation"),

                array(struct(lit("Adj_Title"),lit("Adj_Forenames"),lit("Adj_Surname"))).alias("allocatedJudge_inputFields"),
                array(struct(col("Adj_Title"),col("Adj_Forenames"),col("Adj_Surname"))).alias("allocatedJudge_inputValues"),
                col("allocatedJudge").alias("allocatedJudge_value"),
                lit("Yes").alias("allocatedJudge_Transformation"),

                array(struct(lit("Adj_Title"),lit("Adj_Forenames"),lit("Adj_Surname"))).alias("allocatedJudgeEdit_inputFields"),
                array(struct(col("Adj_Title"),col("Adj_Forenames"),col("Adj_Surname"))).alias("allocatedJudgeEdit_inputValues"),
                col("allocatedJudgeEdit").alias("allocatedJudgeEdite_value"),
                lit("Yes").alias("allocatedJudgeEdit_Transformation"),


            )
    )
       
    return ftpa_df, ftpa_audit
################################################################

################################################################   

if __name__ == "__main__":
    pass