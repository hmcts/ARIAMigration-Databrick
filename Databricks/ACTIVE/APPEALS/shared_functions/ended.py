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
##########              ended          ###########
################################################################

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def ended(silver_m3, bronze_ended_states):
    # 1) Normalize numeric types explicitly (in-place casts)
    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    # 2) Build "exists in the same case" flag for sa.CaseStatus IN (10,51,52)
    has_10_51_52 = (
        df.groupBy("CaseNo")
          .agg(
              F.max(
                  F.when(F.col("CaseStatus").isin(10, 51, 52), F.lit(1)).otherwise(F.lit(0))
              ).alias("has_10_51_52_in_case")
          )
    )

    df_with_flag = df.join(has_10_51_52, on="CaseNo", how="left")

    # 3) Apply the full filter equivalent to your SQL WHERE
    cond = (
        (
            (F.col("CaseStatus") == 10) &
            F.col("Outcome").isin(80, 122, 25, 120, 2, 105, 13)
        ) |
        (
            # t.CaseStatus = 46 AND t.Outcome = 31 AND sa.CaseStatus IN (10,51,52)
            (F.col("CaseStatus") == 46) &
            (F.col("Outcome") == 31) &
            (F.col("has_10_51_52_in_case") == 1)
        ) |
        (
            (F.col("CaseStatus") == 26) &
            F.col("Outcome").isin(80, 13, 25)
        ) |
        (
            F.col("CaseStatus").isin(37, 38) &
            F.col("Outcome").isin(80, 13, 25, 72, 125)
        ) |
        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 25)
        ) |
        (
            (F.col("CaseStatus") == 51) &
            F.col("Outcome").isin(0, 94, 93)
        ) |
        (
            (F.col("CaseStatus") == 52) &
            F.col("Outcome").isin(91, 95)
        ) |
        (
            (F.col("CaseStatus") == 36) &
            F.col("Outcome").isin(1, 2, 25)
        )
    )

    filtered = df_with_flag.filter(cond)

    # 4) For each CaseNo, take the row with the MAX(StatusId) among the filtered rows
    w = Window.partitionBy("CaseNo").orderBy(F.col("StatusId").desc())
    m3_net_df = (
        filtered
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "has_10_51_52_in_case")   # drop helper cols
    )

    # 5) Build decision_ts robustly and format end date
    silver_with_decision_ts = m3_net_df.withColumn(
        "decision_ts",
        F.coalesce(
            F.to_timestamp(F.col("DecisionDate")),  # already-ISO timestamps parse fine
            F.to_timestamp(F.col("DecisionDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp(F.col("DecisionDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")
        )
    )

    # 6) Join with ended state and build final fields
    #    Assumes bronze_ended_states has int CaseStatus and Outcome too.
    ended_df = (
        silver_with_decision_ts.alias("m3")
        .join(bronze_ended_states.alias("es"), on=["CaseStatus", "Outcome"], how="left")
        .withColumn(
            "endAppealApproverType",
            F.when(F.col("CaseStatus") == 46, F.lit("Judge")).otherwise(F.lit("Case Worker"))
        )
        .withColumn(
            "endAppealApproverName",
            F.when(
                F.col("CaseStatus") == 46,
                F.concat(
                    F.col("Adj_Determination_Title"), F.lit(" "),
                    F.col("Adj_Determination_Forenames"), F.lit(" "),
                    F.col("Adj_Determination_Surname")
                )
            ).otherwise(F.lit("This is a migrated ARIA case"))
        )
        .withColumn("endAppealDate", F.date_format(F.col("decision_ts"), "dd/MM/yyyy"))
        .select(
            F.col("CaseNo"),
            F.col("es.endAppealOutcome"),
            F.col("es.endAppealOutcomeReason"),
            F.col("endAppealApproverType"),
            F.col("endAppealApproverName"),
            F.col("endAppealDate"),
            F.col("es.stateBeforeEndAppeal"),
        )
    )

    ended_audit = (
        ended_df.alias("content")
            .join(silver_with_decision_ts.alias("m3"), on="CaseNo", how="left")
            .join(bronze_ended_states.alias("es"), on=["CaseStatus", "Outcome"], how="left")
            .select(
                "content.CaseNo",
                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("endAppealOutcome"))).alias("endAppealOutcome_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),col("es.endAppealOutcome"))).alias("endAppealOutcome_inputValues"),
                col("content.endAppealOutcome").alias("endAppealOutcome_value"),
                lit("Yes").alias("endAppealOutcome_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("endAppealOutcomeReason"))).alias("endAppealOutcomeReason_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),col("es.endAppealOutcomeReason"))).alias("endAppealOutcomeReason_inputValues"),
                col("content.endAppealOutcomeReason").alias("endAppealOutcomeReason_value"),
                lit("Yes").alias("endAppealOutcomeReason_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("endAppealApproverType"))).alias("endAppealApproverType_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),lit("Null"))).alias("endAppealApproverType_inputValues"),
                col("endAppealApproverType").alias("endAppealApproverType_value"),
                lit("Yes").alias("endAppealApproverType_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("Adj_Determination_Title"),lit("Adj_Determination_Forenames"),lit("Adj_Determination_Surname"))).alias("endAppealApproverName_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),col("Adj_Determination_Title"),col("Adj_Determination_Forenames"),col("Adj_Determination_Surname"))).alias("endAppealApproverName_inputValues"),
                col("endAppealApproverName").alias("endAppealApproverName_value"),
                lit("Yes").alias("endAppealApproverName_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("DecisionDate"))).alias("endAppealDate_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),col("m3.DecisionDate"))).alias("endAppealDate_inputValues"),
                col("endAppealDate").alias("endAppealDate_value"),
                lit("Yes").alias("endAppealDate_Transformed"),

                array(struct(lit("CaseStatus"),lit("StatusId"),lit("Outcome"),lit("stateBeforeEndAppeal"))).alias("stateBeforeEndAppeal_inputFields"),
                array(struct(col("m3.CaseStatus"),col("m3.StatusId"),col("m3.Outcome"),col("es.stateBeforeEndAppeal"))).alias("stateBeforeEndAppeal_inputValues"),
                col("content.stateBeforeEndAppeal").alias("stateBeforeEndAppeal_value"),
                lit("Yes").alias("stateBeforeEndAppeal_Transformed"),
            )
    )

    return ended_df,ended_audit

################################################################
##########              documents          ###########
################################################################

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def documents(silver_m1,silver_m3):

    documents_df, documents_audit = FSA.documents(silver_m1,silver_m3)

    df = (
        silver_m3
        .withColumn("CaseStatus", F.col("CaseStatus").cast("int"))
        .withColumn("Outcome", F.col("Outcome").cast("int"))
        .withColumn("StatusId", F.col("StatusId").cast("long"))
    )

    cond_state_2_3_4 = (
        (
            (F.col("CaseStatus") == 26) &
            (F.col("Outcome").isin(80, 25,13))
        ) |
        (
            # t.CaseStatus = 46 AND t.Outcome = 31 AND sa.CaseStatus IN (10,51,52)
            (F.col("CaseStatus").isin(37, 38)) &
            (F.col("Outcome").isin(80,13,25))
        ) |
        (
            (F.col("CaseStatus") == 38) &
            (F.col("Outcome") == 72)
        ) |

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 72)
        )
    )

    cond_state_3_4 = (
        (
            # t.CaseStatus = 46 AND t.Outcome = 31 AND sa.CaseStatus IN (10,51,52)
            (F.col("CaseStatus").isin(37, 38)) &
            (F.col("Outcome").isin(80,13,25))
        ) |
        (
            (F.col("CaseStatus") == 38) &
            (F.col("Outcome") == 72)
        ) |

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 72)
        )
    )

    cond_state_4 = (

        (
            (F.col("CaseStatus") == 39) &
            (F.col("Outcome") == 72)
        )
    )









################################################################   

if __name__ == "__main__":
    pass