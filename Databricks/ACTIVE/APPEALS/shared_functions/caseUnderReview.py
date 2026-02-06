from pyspark.sql.functions import col, struct, lit, concat, array, trim, concat_ws, when, desc, coalesce, nullif, collect_list
from pyspark.sql import functions as F
from pyspark.sql import Window
from . import AwaitingEvidenceRespondant_b as AERb


def generalDefault(silver_m1):
    df = AERb.generalDefault(silver_m1)

    df = (
        df.join(silver_m1.alias("m1").select("CaseNo", "dv_representation"), on="CaseNo", how="left")
            .filter(col("dv_representation") == "LR")
            .drop(col("dv_representation"))
    )

    df = df.select(
        "*",
        lit("Yes").alias("caseArgumentAvailable")
    )

    return df


def hearingResponse(silver_m1, silver_m3, silver_m6):

    window = Window.partitionBy("CaseNo").orderBy(desc("StatusId"))
    df_stg = silver_m3.filter((F.col("CaseStatus").isin([37,38])) | (F.col("CaseStatus") == 26) & (F.col("Outcome") == 0)).withColumn("rn",F.row_number().over(window)).filter(F.col("rn") == 1).drop(F.col("rn"))

    m3_df = df_stg.withColumn("CourtClerkFull",
        when((col("CourtClerk_Surname").isNotNull()) & (col("CourtClerk_Surname") != ""), concat_ws(" ", col("CourtClerk_Surname"), col("CourtClerk_Forenames"),
        when((col("CourtClerk_Title").isNotNull()) & (col("CourtClerk_Title") != ""),
            concat(lit("("), col("CourtClerk_Title"), lit(")"))).otherwise(lit(None))))
        )

    stg_m6 = (
        silver_m6
            .withColumn("Transformed_Required", when(F.col("Required") == '0', lit('Not Required')).when(F.col("Required") == '1', lit('Required')))
            .withColumn("ConcatJudgeDetails", concat_ws(
                " ",
                col("Judge_Surname"),
                col("Judge_Forenames"),
                when(col("Judge_Title").isNotNull(), "("),
                col("Judge_Title"),
                when(col("Judge_Title").isNotNull(), ")"),
                when(col("Transformed_Required").isNotNull(), ":"), col("Transformed_Required")
            ))
            .groupBy("CaseNo")
                .agg(
                    concat_ws("\n", collect_list(col("ConcatJudgeDetails"))).alias("ConcatJudgeDetails_List"),
                )
    )

    final_df = m3_df.join(silver_m1, ["CaseNo"], "left").join(stg_m6, ["CaseNo"], "left").withColumn("CaseNo", trim(col("CaseNo"))
                    ).withColumn("Hearing Centre",
                                when(col("ListedCentre").isNull(), "N/A").otherwise(col("ListedCentre"))
                    ).withColumn("Hearing Date",
                                when(col("KeyDate").isNull(), "N/A").otherwise(col("KeyDate"))
                    ).withColumn("Hearing Type",
                                when(col("HearingType").isNull(), "N/A").otherwise(col("HearingType"))
                    ).withColumn("Court",
                                when(col("CourtName").isNull(), "N/A").otherwise(col("CourtName"))
                    ).withColumn("List Type",
                                when(col("ListType").isNull(), "N/A").otherwise(col("ListType"))
                    ).withColumn("List Start Time",
                                when(col("StartTime").isNull(), "N/A").otherwise(col("StartTime"))
                    ).withColumn("Judge First Tier", 
                            when(coalesce(col("Judge1FT_Surname"), col("Judge2FT_Surname"), col("Judge3FT_Surname")).isNotNull(),
                            trim(concat_ws(" ",
                            when(col("Judge1FT_Surname").isNotNull(),
                                concat_ws(" ", col("Judge1FT_Surname"), col("Judge1FT_Forenames"),
                                when(col("Judge1FT_Title").isNotNull() & (col("Judge1FT_Title") != ""),
                                    concat(lit("("), col("Judge1FT_Title"), lit(")"))).otherwise(lit("")))).otherwise(lit("")),

                            when(col("Judge2FT_Surname").isNotNull(),
                                concat_ws(" ", col("Judge2FT_Surname"), col("Judge2FT_Forenames"),
                                    when(col("Judge2FT_Title").isNotNull() & (col("Judge2FT_Title") != ""),
                                        concat(lit("("), col("Judge2FT_Title"), lit(")"))).otherwise(lit("")))).otherwise(lit("")),

                            when(col("Judge3FT_Surname").isNotNull(),
                                concat_ws(" ", col("Judge3FT_Surname"), col("Judge3FT_Forenames"),
                                    when(col("Judge3FT_Title").isNotNull() & (col("Judge3FT_Title") != ""),
                                        concat(lit("("), col("Judge3FT_Title"), lit(")"))).otherwise(lit("")))
                                ).otherwise(lit(""))))

                            ).otherwise(lit(None))
                    ).withColumn("Start Time",
                                when(col("StartTime").isNull(), "N/A").otherwise(col("StartTime"))
                    ).withColumn("Estimated Duration",
                                when(col("TimeEstimate").isNull(), "N/A").otherwise(col("TimeEstimate"))
                    ).withColumn("Required/Incompatible Judicial Officers",
                                when(col("ConcatJudgeDetails_List").isNotNull(), concat(lit("\n"), col("ConcatJudgeDetails_List")))
                    ).withColumn("Notes",
                                when(col("Notes").isNull(), "N/A").otherwise(col("Notes"))
                    ).withColumn("additionalInstructionsTribunalResponse",
                                concat(
                                    lit("Listed details from ARIA: "),
                                    lit("\nHearing Centre: "), coalesce(col("Hearing Centre"), lit("N/A")),
                                    lit("\nHearing Date: "), coalesce(col("Hearing Date"), lit("N/A")),
                                    lit("\nHearing Type: "), coalesce(col("Hearing Type"), lit("N/A")),
                                    lit("\nCourt: "), coalesce(col("Court"), lit("N/A")),
                                    lit("\nList Type: "), coalesce(col("ListType"), lit("N/A")),
                                    lit("\nList Start Time: "), coalesce(col("List Start Time"), lit("N/A")),
                                    lit("\nJudge First Tier: "), coalesce(col("Judge First Tier"), lit("")),
                                    lit("\nCourt Clerk / Usher: "), coalesce(nullif(concat_ws(", ", col("CourtClerkFull")), lit("")), lit("N/A")),
                                    lit("\nStart Time: "), coalesce(col("Start Time"), lit("N/A")),
                                    lit("\nEstimated Duration: "), coalesce(col("Estimated Duration"), lit("N/A")),
                                    lit("\nRequired/Incompatible Judicial Officers: "),
                                    coalesce(col("Required/Incompatible Judicial Officers"), lit("")),
                                    lit("\nNotes: "), coalesce(col("Notes"), lit("N/A"))
                                )          
                    )

    additionalInstructionsTribunalResponse_schema_dict = {
        "Hearing Centre": ["ListedCentre"],
        "Hearing Date": ["KeyDate"],
        "Hearing Type": ["HearingType"],
        "Court": ["CourtName"],
        "List Type": ["ListType"],
        "List Start Time": ["StartTime"],
        "Judge First Tier": [
            "Judge1FT_Surname", "Judge1FT_Forenames", "Judge1FT_Title",
            "Judge2FT_Surname", "Judge2FT_Forenames", "Judge2FT_Title",
            "Judge3FT_Surname", "Judge3FT_Forenames", "Judge3FT_Title"
        ],
        "Start Time": ["StartTime"],
        "Estimated Duration": ["TimeEstimate"],
        "Required/Incompatible Judicial Officers": [
            "Judge_Surname", "Judge_Forenames", "Judge_Title", "Transformed_Required"
        ],
        "Notes": ["Notes"],
        "Court Clerk / Usher": [
            "CourtClerk_Surname", "CourtClerk_Forenames", "CourtClerk_Title"
        ]
    }

    content_df = final_df.select(
        col("CaseNo"),
        col("additionalInstructionsTribunalResponse")).where(col('dv_representation') == 'LR')

    df_audit = final_df.alias("f").join(silver_m1.alias("m1"), col("m1.CaseNo") == col("f.CaseNo"), "left").select(
        col("f.CaseNo"),
        col("f.additionalInstructionsTribunalResponse"),
        array(
            struct(
                lit("Hearing Centre").alias("field"),
                array(lit("HearingCentre")).alias("source_columns")
            ),
            struct(
                lit("Hearing Date").alias("field"),
                array(lit("HearingDate")).alias("source_columns")
            ),
            struct(
                lit("Hearing Type").alias("field"),
                array(lit("HearingType")).alias("source_columns")
            ),
            struct(
                lit("Court").alias("field"),
                array(lit("CourtName")).alias("source_columns")
            ),
            struct(
                lit("List Type").alias("field"),
                array(lit("ListType")).alias("source_columns")
            ),
            struct(
                lit("List Start Time").alias("field"),
                array(lit("StartTime")).alias("source_columns")
            ),
            struct(
                lit("Judge First Tier").alias("field"),
                array(
                    lit("Judge1FT_Surname"), lit("Judge1FT_Forenames"), lit("Judge1FT_Title"),
                    lit("Judge2FT_Surname"), lit("Judge2FT_Forenames"), lit("Judge2FT_Title"),
                    lit("Judge3FT_Surname"), lit("Judge3FT_Forenames"), lit("Judge3FT_Title")
                ).alias("source_columns")
            ),
            struct(
                lit("Start Time").alias("field"),
                array(lit("StartTime")).alias("source_columns")
            ),
            struct(
                lit("Estimated Duration").alias("field"),
                array(lit("TimeEstimate")).alias("source_columns")
            ),
            struct(
                lit("Required/Incompatible Judicial Officers").alias("field"),
                array(lit("Judge_Surname"), lit("Judge_Forenames"), lit("Judge_Title"), lit("Transformed_Required")).alias("source_columns")
            ),
            struct(
                lit("Notes").alias("field"),
                array(lit("Notes")).alias("source_columns")
            ),
            struct(
                lit("Court Clerk / Usher").alias("field"),
                array(lit("CourtClerk_Surname"), lit("CourtClerk_Forenames"), lit("CourtClerk_Title")).alias("source_columns")
            ),
            struct(
                lit("dv_representation").alias("field"),
                array(lit("dv_representation")).alias("source_columns")
            ),
            struct(
                lit("dv_CCDAppealType").alias("field"),
                array(lit("dv_CCDAppealType")).alias("source_columns")
            )
        ).alias("additionalInstructionsTribunalResponse_inputFields"),
        array(
            struct(lit("Hearing Centre").alias("field"), col("f.`Hearing Centre`").cast("string").alias("value")),
            struct(lit("Hearing Date").alias("field"), col("f.`Hearing Date`").cast("string").alias("value")),
            struct(lit("Hearing Type").alias("field"), col("f.`Hearing Type`").cast("string").alias("value")),
            struct(lit("Court").alias("field"), col("f.`Court`").cast("string").alias("value")),
            struct(lit("List Type").alias("field"), col("f.`List Type`").cast("string").alias("value")),
            struct(lit("List Start Time").alias("field"), col("f.`List Start Time`").cast("string").alias("value")),
            struct(lit("Judge First Tier").alias("field"), col("f.`Judge First Tier`").cast("string").alias("value")),
            struct(lit("Start Time").alias("field"), col("f.`Start Time`").cast("string").alias("value")),
            struct(lit("Estimated Duration").alias("field"), col("f.`Estimated Duration`").cast("string").alias("value")),
            struct(lit("Required/Incompatible Judicial Officers").alias("field"), col("f.`Required/Incompatible Judicial Officers`").cast("string").alias("value")),
            struct(lit("Notes").alias("field"), col("f.`Notes`").cast("string").alias("value")),
            struct(lit("Court Clerk / Usher").alias("field"), col("f.CourtClerkFull").cast("string").alias("value")),
            struct(lit("dv_representation").alias("field"), col("m1.dv_representation").cast("string").alias("value")),
            struct(lit("dv_CCDAppealType").alias("field"), col("m1.dv_CCDAppealType").cast("string").alias("value"))
        ).alias("additionalInstructionsTribunalResponse_inputValues")
    )
    return content_df, df_audit

if __name__ == "__main__":
    pass
    
