def ftpa(silver_m3, silver_c):

    # ------------------------------------------------------------
    # IMPORTANT:
    # Do NOT mutate DateReceived / DecisionDate BEFORE base function call.
    # Base logic (DA/FSB) relies on original schema/types.
    # ------------------------------------------------------------

    # Base ftpa fields (judge allocation etc.)
    ftpa_df, ftpa_audit = FSB.ftpa(silver_m3, silver_c)

    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Latest row per CaseNo (keep/remove CaseStatus filter based on business)
    silver_m3_max_statusid = (
        silver_m3
        .withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number")
    )

    # submitted_a style parsing, but into a NEW column (do not overwrite)
    decisiondate_ts = coalesce(
        F.to_timestamp(col("DecisionDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),  # +00:00
        F.to_timestamp(col("DecisionDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),    # Z or +01
        col("DecisionDate").cast("timestamp")                                  # if already timestamp
    )

    # Outcome mapping
    outcome_type = (
        when(col("Outcome") == 30, lit("granted"))
        .when(col("Outcome") == 31, lit("refused"))
        .when(col("Outcome") == 14, lit("notAdmitted"))
        .otherwise(lit(None))
    )

    outcome_display = (
        when(col("Outcome") == 30, lit("Granted"))
        .when(col("Outcome") == 31, lit("Refused"))
        .when(col("Outcome") == 14, lit("Not admitted"))
        .otherwise(lit(None))
    )

    silver_m3_content = (
        silver_m3_max_statusid
        .withColumn(
            "ftpaApplicantType",
            when(col("Party") == 1, lit("appellant"))
            .when(col("Party") == 2, lit("respondent"))
            .otherwise(lit(None))
        )
        .withColumn("ftpaFirstDecision", outcome_type)
        .withColumn(
            "ftpaAppellantDecisionDate",
            when(col("Party") == 1, date_format(decisiondate_ts, "dd/MM/yyyy")).otherwise(lit(None))
        )
        .withColumn(
            "ftpaRespondentDecisionDate",
            when(col("Party") == 2, date_format(decisiondate_ts, "dd/MM/yyyy")).otherwise(lit(None))
        )
        .withColumn("ftpaFinalDecisionForDisplay", outcome_display)
        .withColumn(
            "ftpaAppellantRjDecisionOutcomeType",
            when(col("Party") == 1, outcome_type).otherwise(lit(None))
        )
        .withColumn(
            "ftpaRespondentRjDecisionOutcomeType",
            when(col("Party") == 2, outcome_type).otherwise(lit(None))
        )
        .withColumn(
            "isFtpaAppellantNoticeOfDecisionSetAside",
            when(col("Party") == 1, lit("No")).otherwise(lit(None))
        )
        .withColumn(
            "isFtpaRespondentNoticeOfDecisionSetAside",
            when(col("Party") == 2, lit("No")).otherwise(lit(None))
        )
        .select(
            col("CaseNo"),
            col("ftpaApplicantType"),
            col("ftpaFirstDecision"),
            col("ftpaAppellantDecisionDate"),
            col("ftpaRespondentDecisionDate"),
            col("ftpaFinalDecisionForDisplay"),
            col("ftpaAppellantRjDecisionOutcomeType"),
            col("ftpaRespondentRjDecisionOutcomeType"),
            col("isFtpaAppellantNoticeOfDecisionSetAside"),
            col("isFtpaRespondentNoticeOfDecisionSetAside"),
        )
    )

    # Join decided fields onto base ftpa_df
    ftpa_df = (
        ftpa_df.alias("ftpa")
        .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
        .select(
            col("ftpa.*"),
            col("m3.ftpaApplicantType").alias("ftpaApplicantType"),
            col("m3.ftpaFirstDecision").alias("ftpaFirstDecision"),
            col("m3.ftpaAppellantDecisionDate").alias("ftpaAppellantDecisionDate"),
            col("m3.ftpaRespondentDecisionDate").alias("ftpaRespondentDecisionDate"),
            col("m3.ftpaFinalDecisionForDisplay").alias("ftpaFinalDecisionForDisplay"),
            col("m3.ftpaAppellantRjDecisionOutcomeType").alias("ftpaAppellantRjDecisionOutcomeType"),
            col("m3.ftpaRespondentRjDecisionOutcomeType").alias("ftpaRespondentRjDecisionOutcomeType"),
            col("m3.isFtpaAppellantNoticeOfDecisionSetAside").alias("isFtpaAppellantNoticeOfDecisionSetAside"),
            col("m3.isFtpaRespondentNoticeOfDecisionSetAside").alias("isFtpaRespondentNoticeOfDecisionSetAside"),
        )
    )

    # Audit for decided-only fields
    ftpa_audit = (
        ftpa_audit.alias("audit")
        .join(ftpa_df.alias("ftpa"), on=["CaseNo"], how="left")
        .join(silver_m3_max_statusid.alias("m3base"), on=["CaseNo"], how="left")
        .join(silver_m3_content.alias("m3"), on=["CaseNo"], how="left")
        .select(
            col("audit.*"),

            array(struct(lit("Party"))).alias("ftpaApplicantType_inputFields"),
            array(struct(col("m3base.Party"))).alias("ftpaApplicantType_inputValues"),
            col("m3.ftpaApplicantType").alias("ftpaApplicantType_value"),
            lit("Derived").alias("ftpaApplicantType_Transformation"),

            array(struct(lit("Outcome"))).alias("ftpaFirstDecision_inputFields"),
            array(struct(col("m3base.Outcome"))).alias("ftpaFirstDecision_inputValues"),
            col("m3.ftpaFirstDecision").alias("ftpaFirstDecision_value"),
            lit("Derived").alias("ftpaFirstDecision_Transformation"),

            array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaAppellantDecisionDate_inputFields"),
            array(struct(col("m3base.DecisionDate"), col("m3base.Party"))).alias("ftpaAppellantDecisionDate_inputValues"),
            col("m3.ftpaAppellantDecisionDate").alias("ftpaAppellantDecisionDate_value"),
            lit("Derived").alias("ftpaAppellantDecisionDate_Transformation"),

            array(struct(lit("DecisionDate"), lit("Party"))).alias("ftpaRespondentDecisionDate_inputFields"),
            array(struct(col("m3base.DecisionDate"), col("m3base.Party"))).alias("ftpaRespondentDecisionDate_inputValues"),
            col("m3.ftpaRespondentDecisionDate").alias("ftpaRespondentDecisionDate_value"),
            lit("Derived").alias("ftpaRespondentDecisionDate_Transformation"),

            array(struct(lit("Outcome"))).alias("ftpaFinalDecisionForDisplay_inputFields"),
            array(struct(col("m3base.Outcome"))).alias("ftpaFinalDecisionForDisplay_inputValues"),
            col("m3.ftpaFinalDecisionForDisplay").alias("ftpaFinalDecisionForDisplay_value"),
            lit("Derived").alias("ftpaFinalDecisionForDisplay_Transformation"),

            array(struct(lit("Outcome"), lit("Party"))).alias("ftpaAppellantRjDecisionOutcomeType_inputFields"),
            array(struct(col("m3base.Outcome"), col("m3base.Party"))).alias("ftpaAppellantRjDecisionOutcomeType_inputValues"),
            col("m3.ftpaAppellantRjDecisionOutcomeType").alias("ftpaAppellantRjDecisionOutcomeType_value"),
            lit("Derived").alias("ftpaAppellantRjDecisionOutcomeType_Transformation"),

            array(struct(lit("Outcome"), lit("Party"))).alias("ftpaRespondentRjDecisionOutcomeType_inputFields"),
            array(struct(col("m3base.Outcome"), col("m3base.Party"))).alias("ftpaRespondentRjDecisionOutcomeType_inputValues"),
            col("m3.ftpaRespondentRjDecisionOutcomeType").alias("ftpaRespondentRjDecisionOutcomeType_value"),
            lit("Derived").alias("ftpaRespondentRjDecisionOutcomeType_Transformation"),

            array(struct(lit("Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputFields"),
            array(struct(col("m3base.Party"))).alias("isFtpaAppellantNoticeOfDecisionSetAside_inputValues"),
            col("m3.isFtpaAppellantNoticeOfDecisionSetAside").alias("isFtpaAppellantNoticeOfDecisionSetAside_value"),
            lit("Derived").alias("isFtpaAppellantNoticeOfDecisionSetAside_Transformation"),

            array(struct(lit("Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputFields"),
            array(struct(col("m3base.Party"))).alias("isFtpaRespondentNoticeOfDecisionSetAside_inputValues"),
            col("m3.isFtpaRespondentNoticeOfDecisionSetAside").alias("isFtpaRespondentNoticeOfDecisionSetAside_value"),
            lit("Derived").alias("isFtpaRespondentNoticeOfDecisionSetAside_Transformation"),
        )
    )

    return ftpa_df, ftpa_audit
