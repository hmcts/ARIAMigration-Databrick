from pyspark.sql.functions import (
    array, array_compact, array_contains, array_distinct, array_size, col, collect_list, concat_ws, expr, lit, row_number, struct, when
)
from pyspark.sql.window import Window

from . import paymentPending as PP
from . import AwaitingEvidenceRespondant_b as AERb


def hearingRequirements(silver_m1, silver_m3, silver_c, bronze_interpreter_languages):
    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_ranked = silver_m3.withColumn("row_num", row_number().over(window_spec))

    silver_m3_filtered = silver_m3_ranked.filter(
        (col("row_num") == 1) & (
            (
                (col("CaseStatus").isin(37, 38)) & (col("Outcome").isin(0, 27, 37, 39, 40, 50))
            ) | (
                (col("CaseStatus") == 26) & (col("Outcome").isin(40, 52))
            )
        )
    )

    silver_c_grouped = silver_c.groupBy("CaseNo").agg(collect_list(col("CategoryId")).alias("CategoryIdList"))

    spokenLanguageCategory = "spokenLanguageInterpreter"
    signLanguageCategory = "signLanguageInterpreter"

    # List items for all Spoken Languages
    spoken_languages_list = bronze_interpreter_languages.filter(
        (col("appellantInterpreterLanguageCategory") == lit(spokenLanguageCategory)) & (col("manualEntry") != lit("Yes"))
    ).select(col("languageCode").alias("code"), col("languageLabel").alias("label")).collect()

    spoken_languages_list_literal = array([
        struct(lit(row.code).alias("code"), lit(row.label).alias("label"))
        for row in spoken_languages_list
    ])

    # Language Ref Data condition is:
    # Only 1 of language or additional language must match the spoken category and neither language matching spoken category is a manualEntry one.
    spoken_language_ref_data_condition = (
        (
            (~(col("il.appellantInterpreterLanguageCategory").eqNullSafe(spokenLanguageCategory)))
            | (~(col("ail.appellantInterpreterLanguageCategory").eqNullSafe(spokenLanguageCategory)))
        ) & (
            ~((col("il.appellantInterpreterLanguageCategory").eqNullSafe(spokenLanguageCategory)) & (col("il.manualEntry").eqNullSafe("Yes")))
            & ~((col("ail.appellantInterpreterLanguageCategory").eqNullSafe(spokenLanguageCategory)) & (col("ail.manualEntry").eqNullSafe("Yes")))
        )
    )

    # List items for all Sign Languages
    sign_languages_list = bronze_interpreter_languages.filter(
        (col("appellantInterpreterLanguageCategory") == lit(signLanguageCategory)) & (col("manualEntry") != lit("Yes"))
    ).select(col("languageCode").alias("code"), col("languageLabel").alias("label")).collect()

    sign_languages_list_literal = array([
        struct(lit(row.code).alias("code"), lit(row.label).alias("label"))
        for row in sign_languages_list
    ])

    # Language Ref Data condition is:
    # Only 1 of language or additional language must match the sign category and neither language matching sign category is a manualEntry one.
    sign_language_ref_data_condition = (
        (
            (~(col("il.appellantInterpreterLanguageCategory").eqNullSafe(signLanguageCategory)))
            | (~(col("ail.appellantInterpreterLanguageCategory").eqNullSafe(signLanguageCategory)))
        ) & (
            ~((col("il.appellantInterpreterLanguageCategory").eqNullSafe(signLanguageCategory)) & (col("il.manualEntry").eqNullSafe("Yes")))
            & ~((col("ail.appellantInterpreterLanguageCategory").eqNullSafe(signLanguageCategory)) & (col("ail.manualEntry").eqNullSafe("Yes")))
        )
    )

    # silver_m3_filtered returns at most 1 m3 record per CaseNo, so there can only be at most 1 LanguageId and 1 AdditionalLanguageId
    # therefore no aggregation of AdditionalLanguageIds needed (as no duplicate CaseNo) unless the condition for the M3 is changed. To be clarified.
    interpreter_languages_lookup = (
        silver_m1.alias("m1")
            .join(silver_m3_filtered.alias("m3"), on="CaseNo", how="left")
            .join(bronze_interpreter_languages.alias("il"), on=(col("m1.LanguageId") == col("il.LanguageId")), how="left")
            .join(bronze_interpreter_languages.alias("ail"), on=((col("m3.AdditionalLanguageId") == col("ail.LanguageId")) & (col("m1.LanguageId") != col("m3.AdditionalLanguageId"))), how="left")
            .withColumn("lu_appellantInterpreterLanguageCategory", array_distinct(array_compact(array(
                col("il.appellantInterpreterLanguageCategory"), col("ail.appellantInterpreterLanguageCategory")
            ))))
            .withColumn("lu_appellantInterpreterSpokenLanguageRefData",
                struct(
                    when((col("il.appellantInterpreterLanguageCategory") == spokenLanguageCategory),
                        struct(col("il.languageCode").alias("code"), col("il.languageLabel").alias("label"))
                    ).when((col("ail.appellantInterpreterLanguageCategory") == spokenLanguageCategory),
                        struct(col("ail.languageCode").alias("code"), col("ail.languageLabel").alias("label"))
                    ).alias("value")
                )
            )
            .withColumn("lu_appellantInterpreterSignLanguageRefData",
                struct(
                    when((col("il.appellantInterpreterLanguageCategory") == signLanguageCategory),
                        struct(col("il.languageCode").alias("code"), col("il.languageLabel").alias("label"))
                    ).when((col("ail.appellantInterpreterLanguageCategory") == signLanguageCategory),
                        struct(col("ail.languageCode").alias("code"), col("ail.languageLabel").alias("label"))
                    ).alias("value")
                )
            )
            .withColumn("lu_spokenManualEntry",
                array_distinct(array_compact(array(
                    when(((col("il.appellantInterpreterLanguageCategory") == spokenLanguageCategory) & (col("il.manualEntry") == "Yes")),
                        col("il.manualEntry")
                    ),
                    when(((col("ail.appellantInterpreterLanguageCategory") == spokenLanguageCategory) & (col("ail.manualEntry") == "Yes")),
                        col("ail.manualEntry")
                    ),
                    when(((col("il.appellantInterpreterLanguageCategory") == spokenLanguageCategory) & (col("ail.appellantInterpreterLanguageCategory") == spokenLanguageCategory)),
                        lit("Yes")
                    )
                )))
            )
            .withColumn("lu_signManualEntry",
                array_distinct(array_compact(array(
                    when(((col("il.appellantInterpreterLanguageCategory") == signLanguageCategory) & (col("il.manualEntry") == "Yes")),
                        col("il.manualEntry")
                    ),
                    when(((col("ail.appellantInterpreterLanguageCategory") == signLanguageCategory) & (col("ail.manualEntry") == "Yes")),
                        col("ail.manualEntry")
                    ),
                    when(((col("il.appellantInterpreterLanguageCategory") == signLanguageCategory) & (col("ail.appellantInterpreterLanguageCategory") == signLanguageCategory)),
                        lit("Yes")
                    )
                )))
            )
            .withColumn("lu_spokenManualEntryDescription",
                concat_ws(", ",
                    when(((col("il.appellantInterpreterLanguageCategory") == spokenLanguageCategory) & (col("il.manualEntry") == "Yes")),
                        col("il.manualEntryDescription")
                    ).when(((col("il.appellantInterpreterLanguageCategory") == spokenLanguageCategory) & (col("ail.appellantInterpreterLanguageCategory") == spokenLanguageCategory)),
                        col("il.languageLabel")
                    ),
                    when(((col("ail.appellantInterpreterLanguageCategory") == spokenLanguageCategory) & (col("ail.manualEntry") == "Yes")),
                        col("ail.manualEntryDescription")
                    ).when(((col("ail.appellantInterpreterLanguageCategory") == spokenLanguageCategory) & (col("il.appellantInterpreterLanguageCategory") == spokenLanguageCategory)),
                        col("ail.languageLabel")
                    )
                )
            )
            .withColumn("lu_signManualEntryDescription",
                concat_ws(", ",
                    when(((col("il.appellantInterpreterLanguageCategory") == signLanguageCategory) & (col("il.manualEntry") == "Yes")),
                        col("il.manualEntryDescription")
                    ).when(((col("il.appellantInterpreterLanguageCategory") == signLanguageCategory) & (col("ail.appellantInterpreterLanguageCategory") == signLanguageCategory)),
                        col("il.languageLabel")
                    ),
                    when(((col("ail.appellantInterpreterLanguageCategory") == signLanguageCategory) & (col("ail.manualEntry") == "Yes")),
                        col("ail.manualEntryDescription")
                    ).when(((col("ail.appellantInterpreterLanguageCategory") == signLanguageCategory) & (col("il.appellantInterpreterLanguageCategory") == signLanguageCategory)),
                        col("ail.languageLabel")
                    )
                )
            )
            .withColumn("lu_appellantInterpreterSpokenLanguage",
                when((array_contains(col("lu_appellantInterpreterLanguageCategory"), spokenLanguageCategory)),
                    struct(
                        when(spoken_language_ref_data_condition,
                            col("lu_appellantInterpreterSpokenLanguageRefData")
                        ).alias("languageRefData"),
                        when(spoken_language_ref_data_condition,
                            spoken_languages_list_literal
                        ).alias("list_items"),
                        col("lu_spokenManualEntry").alias("languageManualEntry"),
                        when((array_size(col("lu_spokenManualEntry")) > 0),
                            col("lu_spokenManualEntryDescription")
                        ).alias("languageManualEntryDescription")
                    )
                )
            )
            .withColumn("lu_appellantInterpreterSignLanguage",
                when((array_contains(col("lu_appellantInterpreterLanguageCategory"), signLanguageCategory)),
                    struct(
                        when(sign_language_ref_data_condition,
                            col("lu_appellantInterpreterSignLanguageRefData")
                        ).alias("languageRefData"),
                        when(sign_language_ref_data_condition,
                            sign_languages_list_literal
                        ).alias("list_items"),
                        col("lu_signManualEntry").alias("languageManualEntry"),
                        when((array_size(col("lu_signManualEntry")) > 0),
                            col("lu_signManualEntryDescription")
                        ).alias("languageManualEntryDescription")
                    )
                )
            )
            .select(
                "CaseNo",
                "lu_appellantInterpreterLanguageCategory",
                "lu_appellantInterpreterSpokenLanguage",
                "lu_appellantInterpreterSignLanguage"
            )
    )

    df_hearingRequirements = (
        silver_m1.alias("m1")
            .join(silver_m3_filtered.alias("m3"), on="CaseNo", how="left")
            .join(silver_c_grouped.alias("c"), on="CaseNo", how="left")
            .join(interpreter_languages_lookup.alias("ilu"), on="CaseNo", how="left")
            .withColumn("isAppellantAttendingTheHearing", lit("Yes"))
            .withColumn("isAppellantGivingOralEvidence", lit("Yes"))
            .withColumn("isWitnessesAttending", lit("No"))
            .withColumn("isEvidenceFromOutsideUkOoc", (
                when(expr("array_contains(c.CategoryIdList, 38)"),
                    when(col("m1.Sponsor_Name").isNotNull(), lit("Yes"))
                    .otherwise(lit("No")))
                .otherwise(lit(None))
            ))
            .withColumn("isEvidenceFromOutsideUkInCountry", (
                when(expr("array_contains(c.CategoryIdList, 37)"),
                    when(col("m1.Sponsor_Name").isNotNull(), lit("Yes"))
                    .otherwise(lit("No")))
                .otherwise(lit(None))
            ))
            .withColumn("isInterpreterServicesNeeded", (
                when((col("m1.Interpreter") == 1), lit("Yes"))
                .when((col("m1.Interpreter") == 2), lit("No"))
                .otherwise(lit("No"))
            ))
            .withColumn("appellantInterpreterLanguageCategory", col("ilu.lu_appellantInterpreterLanguageCategory"))
            .withColumn("appellantInterpreterSpokenLanguage", col("ilu.lu_appellantInterpreterSpokenLanguage"))
            .withColumn("appellantInterpreterSignLanguage", col("ilu.lu_appellantInterpreterSignLanguage"))
            .withColumn("isHearingRoomNeeded", lit("Yes"))
            .withColumn("isHearingLoopNeeded", lit("Yes"))
            .withColumn("remoteVideoCall", lit("Yes"))
            .withColumn("remoteVideoCallDescription", lit("This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form."))
            .withColumn("physicalOrMentalHealthIssues", lit("Yes"))
            .withColumn("physicalOrMentalHealthIssuesDescription", lit("This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form."))
            .withColumn("pastExperiences", lit("Yes"))
            .withColumn("pastExperiencesDescription", lit("This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form."))
            .withColumn("multimediaEvidence", lit("Yes"))
            .withColumn("multimediaEvidenceDescription", lit("This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form."))
            .withColumn("singleSexCourt", (
                when((col("m1.CourtPreference") == 0), lit("No"))
                .when(((col("m1.courtPreference") == 1) | (col("m1.courtPreference") == 2)), lit("Yes"))
                .otherwise(lit("No"))
            ))
            .withColumn("singleSexCourtType", (
                when((col("m1.CourtPreference") == 1), lit("All male"))
                .when((col("m1.courtPreference") == 2), lit("All female"))
                .otherwise(lit(None))
            ))
            .withColumn("singleSexCourtTypeDescription", (
                when(((col("m1.courtPreference") == 1) | (col("m1.courtPreference") == 2)), lit("This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the single sex court."))
                .otherwise(lit(None))
            ))
            .withColumn("inCameraCourt", (
                when((col("m1.InCamera") == 1), lit("Yes"))
                .when((col("m1.InCamera") == 0), lit("No"))
                .otherwise(lit("No"))
            ))
            .withColumn("inCameraCourtDescription", (
                when((col("m1.InCamera") == 1), lit("This is an ARIA migrated case. Please refer to the hearing requirements in the appeal form for further details on the appellants need for an in camera court."))
                .otherwise(lit(None))
            ))
            .withColumn("additionalRequests", lit("Yes"))
            .withColumn("additionalRequestsDescription", lit("This is an ARIA Migrated Case. Please refer to the hearing requirements in the appeal form."))
            .withColumn("datesToAvoidYesNo", lit("No"))
            .select(
                "CaseNo",
                "isAppellantAttendingTheHearing",
                "isAppellantGivingOralEvidence",
                "isWitnessesAttending",
                "isEvidenceFromOutsideUkOoc",
                "isEvidenceFromOutsideUkInCountry",
                "isInterpreterServicesNeeded",
                "appellantInterpreterLanguageCategory",
                "appellantInterpreterSpokenLanguage",
                "appellantInterpreterSignLanguage",
                "isHearingRoomNeeded",
                "isHearingLoopNeeded",
                "remoteVideoCall",
                "remoteVideoCallDescription",
                "physicalOrMentalHealthIssues",
                "physicalOrMentalHealthIssuesDescription",
                "pastExperiences",
                "pastExperiencesDescription",
                "multimediaEvidence",
                "multimediaEvidenceDescription",
                "singleSexCourt",
                "singleSexCourtType",
                "singleSexCourtTypeDescription",
                "inCameraCourt",
                "inCameraCourtDescription",
                "additionalRequests",
                "additionalRequestsDescription",
                "datesToAvoidYesNo"
            )
    )

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("m1.dv_representation"), col("m1.lu_appealType")]

    df_audit_hearingRequirements = (
        df_hearingRequirements.alias("hr")
            .join(silver_m1.alias("m1"), on="CaseNo", how="left")
            .join(silver_m3_filtered.alias("m3"), on="CaseNo", how="left")
            .join(silver_c_grouped.alias("c"), on="CaseNo", how="left")
            .join(bronze_interpreter_languages.alias("il"), on=(col("m1.LanguageId") == col("il.LanguageId")), how="left")
            .join(bronze_interpreter_languages.alias("ail"), on=(col("m3.AdditionalLanguageId") == col("ail.LanguageId")), how="left")
            .select(
                "hr.CaseNo",
                # isAppellantAttendingTheHearing
                array(struct(*common_inputFields)).alias("isAppellantAttendingTheHearing_inputFields"),
                array(struct(*common_inputValues)).alias("isAppellantAttendingTheHearing_inputValues"),
                col("hr.isAppellantAttendingTheHearing").alias("isAppellantAttendingTheHearing_value"),
                lit("Yes").alias("isAppellantAttendingTheHearing_Transformed"),
                # isAppellantGivingOralEvidence
                array(struct(*common_inputFields)).alias("isAppellantGivingOralEvidence_inputFields"),
                array(struct(*common_inputValues)).alias("isAppellantGivingOralEvidence_inputValues"),
                col("hr.isAppellantGivingOralEvidence").alias("isAppellantGivingOralEvidence_value"),
                lit("Yes").alias("isAppellantGivingOralEvidence_Transformed"),
                # isWitnessesAttending
                array(struct(*common_inputFields)).alias("isWitnessesAttending_inputFields"),
                array(struct(*common_inputValues)).alias("isWitnessesAttending_inputValues"),
                col("hr.isWitnessesAttending").alias("isWitnessesAttending_value"),
                lit("Yes").alias("isWitnessesAttending_Transformed"),
                # isEvidenceFromOutsideUkOoc
                array(struct(*common_inputFields, lit("Sponsor_Name"), lit("CategoryIdList"))).alias("isEvidenceFromOutsideUkOoc_inputFields"),
                array(struct(*common_inputValues, col("m1.Sponsor_Name"), col("CategoryIdList"))).alias("isEvidenceFromOutsideUkOoc_inputValues"),
                col("hr.isEvidenceFromOutsideUkOoc").alias("isEvidenceFromOutsideUkOoc_value"),
                lit("Yes").alias("isEvidenceFromOutsideUkOoc_Transformed"),
                # isEvidenceFromOutsideUkInCountry
                array(struct(*common_inputFields, lit("Sponsor_Name"), lit("CategoryIdList"))).alias("isEvidenceFromOutsideUkInCountry_inputFields"),
                array(struct(*common_inputValues, col("m1.Sponsor_Name"), col("CategoryIdList"))).alias("isEvidenceFromOutsideUkInCountry_inputValues"),
                col("hr.isEvidenceFromOutsideUkInCountry").alias("isEvidenceFromOutsideUkInCountry_value"),
                lit("Yes").alias("isEvidenceFromOutsideUkInCountry_Transformed"),
                # isInterpreterServicesNeeded
                array(struct(*common_inputFields, lit("Interpreter"))).alias("isInterpreterServicesNeeded_inputFields"),
                array(struct(*common_inputValues, col("m1.Interpreter"))).alias("isInterpreterServicesNeeded_inputValues"),
                col("hr.isInterpreterServicesNeeded").alias("isInterpreterServicesNeeded_value"),
                lit("Yes").alias("isInterpreterServicesNeeded_Transformed"),
                # appellantInterpreterLanguageCategory
                array(struct(*common_inputFields, lit("LanguageId"), lit("AdditionalLanguageId"), lit("LanguageCategory"), lit("AdditionalLanguageCategory"))).alias("appellantInterpreterLanguageCategory_inputFields"),
                array(struct(*common_inputValues, col("m1.LanguageId"), col("m3.AdditionalLanguageId"), col("il.appellantInterpreterLanguageCategory"), col("ail.appellantInterpreterLanguageCategory").alias("additionalAppellantInterpreterLanguageCategory"))).alias("appellantInterpreterLanguageCategory_inputValues"),
                col("hr.appellantInterpreterLanguageCategory").alias("appellantInterpreterLanguageCategory_value"),
                lit("Yes").alias("appellantInterpreterLanguageCategory_Transformed"),
                # appellantInterpreterSpokenLanguage
                array(struct(*common_inputFields, lit("LanguageId"), lit("AdditionalLanguageId"), lit("LanguageCategory"), lit("AdditionalLanguageCategory"), lit("LanguageCode"), lit("AdditionalLanguageCode"), lit("LanguageLabel"), lit("AdditionalLanguageLabel"), lit("ManualEntry"), lit("AdditionalManualEntry"), lit("ManualEntryDescription"), lit("AdditionalManualEntryDescription"))).alias("appellantInterpreterSpokenLanguage_inputFields"),
                array(struct(*common_inputValues, col("m1.LanguageId"), col("m3.AdditionalLanguageId"), col("il.appellantInterpreterLanguageCategory"), col("ail.appellantInterpreterLanguageCategory").alias("additionalAppellantInterpreterLanguageCategory"), col("il.languageCode"), col("ail.languageCode").alias("AdditionalLanguageCode"), col("il.languageLabel"), col("ail.languageLabel").alias("AdditionalLanguageLabel"), col("il.manualEntry"), col("ail.manualEntry").alias("AdditionalManualEntry"), col("il.manualEntryDescription"), col("ail.manualEntryDescription").alias("AdditionalManualEntryDescription"))).alias("appellantInterpreterSpokenLanguage_inputValues"),
                col("hr.appellantInterpreterSpokenLanguage").alias("appellantInterpreterSpokenLanguage_value"),
                lit("Yes").alias("appellantInterpreterSpokenLanguage_Transformed"),
                # appellantInterpreterSignLanguage
                array(struct(*common_inputFields, lit("LanguageId"), lit("AdditionalLanguageId"), lit("LanguageCategory"), lit("AdditionalLanguageCategory"), lit("LanguageCode"), lit("AdditionalLanguageCode"), lit("LanguageLabel"), lit("AdditionalLanguageLabel"), lit("ManualEntry"), lit("AdditionalManualEntry"), lit("ManualEntryDescription"), lit("AdditionalManualEntryDescription"))).alias("appellantInterpreterSignLanguage_inputFields"),
                array(struct(*common_inputValues, col("m1.LanguageId"), col("m3.AdditionalLanguageId"), col("il.appellantInterpreterLanguageCategory"), col("ail.appellantInterpreterLanguageCategory").alias("additionalAppellantInterpreterLanguageCategory"), col("il.languageCode"), col("ail.languageCode").alias("AdditionalLanguageCode"), col("il.languageLabel"), col("ail.languageLabel").alias("AdditionalLanguageLabel"), col("il.manualEntry"), col("ail.manualEntry").alias("AdditionalManualEntry"), col("il.manualEntryDescription"), col("ail.manualEntryDescription").alias("AdditionalManualEntryDescription"))).alias("appellantInterpreterSignLanguage_inputValues"),
                col("hr.appellantInterpreterSignLanguage").alias("appellantInterpreterSignLanguage_value"),
                lit("Yes").alias("appellantInterpreterSignLanguage_Transformed"),
                # isHearingRoomNeeded
                array(struct(*common_inputFields)).alias("isHearingRoomNeeded_inputFields"),
                array(struct(*common_inputValues)).alias("isHearingRoomNeeded_inputValues"),
                col("hr.isHearingRoomNeeded").alias("isHearingRoomNeeded_value"),
                lit("Yes").alias("isHearingRoomNeeded_Transformed"),
                # isHearingLoopNeeded
                array(struct(*common_inputFields)).alias("isHearingLoopNeeded_inputFields"),
                array(struct(*common_inputValues)).alias("isHearingLoopNeeded_inputValues"),
                col("hr.isHearingLoopNeeded").alias("isHearingLoopNeeded_value"),
                lit("Yes").alias("isHearingLoopNeeded_Transformed"),
                # remoteVideoCall
                array(struct(*common_inputFields)).alias("remoteVideoCall_inputFields"),
                array(struct(*common_inputValues)).alias("remoteVideoCall_inputValues"),
                col("hr.remoteVideoCall").alias("remoteVideoCall_value"),
                lit("Yes").alias("remoteVideoCall_Transformed"),
                # remoteVideoCallDescription
                array(struct(*common_inputFields)).alias("remoteVideoCallDescription_inputFields"),
                array(struct(*common_inputValues)).alias("remoteVideoCallDescription_inputValues"),
                col("hr.remoteVideoCallDescription").alias("remoteVideoCallDescription_value"),
                lit("Yes").alias("remoteVideoCallDescription_Transformed"),
                # physicalOrMentalHealthIssues
                array(struct(*common_inputFields)).alias("physicalOrMentalHealthIssues_inputFields"),
                array(struct(*common_inputValues)).alias("physicalOrMentalHealthIssues_inputValues"),
                col("hr.physicalOrMentalHealthIssues").alias("physicalOrMentalHealthIssues_value"),
                lit("Yes").alias("physicalOrMentalHealthIssues_Transformed"),
                # physicalOrMentalHealthIssuesDescription
                array(struct(*common_inputFields)).alias("physicalOrMentalHealthIssuesDescription_inputFields"),
                array(struct(*common_inputValues)).alias("physicalOrMentalHealthIssuesDescription_inputValues"),
                col("hr.physicalOrMentalHealthIssuesDescription").alias("physicalOrMentalHealthIssuesDescription_value"),
                lit("Yes").alias("physicalOrMentalHealthIssuesDescription_Transformed"),
                # pastExperiences
                array(struct(*common_inputFields)).alias("pastExperiences_inputFields"),
                array(struct(*common_inputValues)).alias("pastExperiences_inputValues"),
                col("hr.pastExperiences").alias("pastExperiences_value"),
                lit("Yes").alias("pastExperiences_Transformed"),
                # pastExperiencesDescription
                array(struct(*common_inputFields)).alias("pastExperiencesDescription_inputFields"),
                array(struct(*common_inputValues)).alias("pastExperiencesDescription_inputValues"),
                col("hr.pastExperiencesDescription").alias("pastExperiencesDescription_value"),
                lit("Yes").alias("pastExperiencesDescription_Transformed"),
                # multimediaEvidence
                array(struct(*common_inputFields)).alias("multimediaEvidence_inputFields"),
                array(struct(*common_inputValues)).alias("multimediaEvidence_inputValues"),
                col("hr.multimediaEvidence").alias("multimediaEvidence_value"),
                lit("Yes").alias("multimediaEvidence_Transformed"),
                # multimediaEvidenceDescription
                array(struct(*common_inputFields)).alias("multimediaEvidenceDescription_inputFields"),
                array(struct(*common_inputValues)).alias("multimediaEvidenceDescription_inputValues"),
                col("hr.multimediaEvidenceDescription").alias("multimediaEvidenceDescription_value"),
                lit("Yes").alias("multimediaEvidenceDescription_Transformed"),
                # singleSexCourt
                array(struct(*common_inputFields, lit("CourtPreference"))).alias("singleSexCourt_inputFields"),
                array(struct(*common_inputValues, col("CourtPreference"))).alias("singleSexCourt_inputValues"),
                col("hr.singleSexCourt").alias("singleSexCourt_value"),
                lit("Yes").alias("singleSexCourt_Transformed"),
                # singleSexCourtType
                array(struct(*common_inputFields, lit("CourtPreference"))).alias("singleSexCourtType_inputFields"),
                array(struct(*common_inputValues, col("CourtPreference"))).alias("singleSexCourtType_inputValues"),
                col("hr.singleSexCourtType").alias("singleSexCourtType_value"),
                lit("Yes").alias("singleSexCourtType_Transformed"),
                # singleSexCourtTypeDescription
                array(struct(*common_inputFields, lit("CourtPreference"))).alias("singleSexCourtTypeDescription_inputFields"),
                array(struct(*common_inputValues, col("CourtPreference"))).alias("singleSexCourtTypeDescription_inputValues"),
                col("hr.singleSexCourtTypeDescription").alias("singleSexCourtTypeDescription_value"),
                lit("Yes").alias("singleSexCourtTypeDescription_Transformed"),
                # inCameraCourt
                array(struct(*common_inputFields, lit("InCamera"))).alias("inCameraCourt_inputFields"),
                array(struct(*common_inputValues, col("InCamera"))).alias("inCameraCourt_inputValues"),
                col("hr.inCameraCourt").alias("inCameraCourt_value"),
                lit("Yes").alias("inCameraCourt_Transformed"),
                # inCameraCourtDescription
                array(struct(*common_inputFields, lit("InCamera"))).alias("inCameraCourtDescription_inputFields"),
                array(struct(*common_inputValues, col("InCamera"))).alias("inCameraCourtDescription_inputValues"),
                col("hr.inCameraCourtDescription").alias("inCameraCourtDescription_value"),
                lit("Yes").alias("inCameraCourtDescription_Transformed"),
                # additionalRequests
                array(struct(*common_inputFields)).alias("additionalRequests_inputFields"),
                array(struct(*common_inputValues)).alias("additionalRequests_inputValues"),
                col("hr.additionalRequests").alias("additionalRequests_value"),
                lit("Yes").alias("additionalRequests_Transformed"),
                # additionalRequestsDescription
                array(struct(*common_inputFields)).alias("additionalRequestsDescription_inputFields"),
                array(struct(*common_inputValues)).alias("additionalRequestsDescription_inputValues"),
                col("hr.additionalRequestsDescription").alias("additionalRequestsDescription_value"),
                lit("Yes").alias("additionalRequestsDescription_Transformed"),
                # datesToAvoidYesNo
                array(struct(*common_inputFields)).alias("datesToAvoidYesNo_inputFields"),
                array(struct(*common_inputValues)).alias("datesToAvoidYesNo_inputValues"),
                col("hr.datesToAvoidYesNo").alias("datesToAvoidYesNo_value"),
                lit("Yes").alias("datesToAvoidYesNo_Transformed")
            )
    )

    return df_hearingRequirements, df_audit_hearingRequirements


def general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres):
    df, df_audit = PP.general(silver_m1, silver_m2, silver_m3, silver_h, bronze_hearing_centres, bronze_derive_hearing_centres)
    df_representation = silver_m1.select("CaseNo", "dv_representation", "lu_appealType")

    df = df.join(df_representation, on="CaseNo", how="left")

    aip_conditions_generalDefault = (col("dv_representation") == "AIP") & (col("lu_appealType").isNotNull())
    lr_conditions_generalDefault = (col("dv_representation") == "LR") & (col("lu_appealType").isNotNull())

    df = (
        df
            .withColumn("caseArgumentAvailable", (when(lr_conditions_generalDefault, lit("Yes")).otherwise(lit(None))))
            .withColumn("reasonsForAppealDecision", (when(aip_conditions_generalDefault, lit("This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal.")).otherwise(lit(None))))
    )

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("general.dv_representation"), col("general.lu_appealType")]

    df_audit = (
        df_audit.alias("audit")
            .join(df.alias("general"), on="CaseNo", how="left")
            .select(
                "audit.*",
                # caseArgumentAvailable
                array(struct(*common_inputFields)).alias("caseArgumentAvailable_inputFields"),
                array(struct(*common_inputValues)).alias("caseArgumentAvailable_inputValues"),
                col("general.caseArgumentAvailable").alias("caseArgumentAvailable_value"),
                lit("Yes").alias("caseArgumentAvailable_Transformed"),
                # reasonForAppealDecision
                array(struct(*common_inputFields)).alias("reasonsForAppealDecision_inputFields"),
                array(struct(*common_inputValues)).alias("reasonsForAppealDecision_inputValues"),
                col("general.reasonsForAppealDecision").alias("reasonsForAppealDecision_value"),
                lit("Yes").alias("reasonsForAppealDecision_Transformed")
            )
    )

    df = df.drop("dv_representation", "lu_appealType")

    return df, df_audit


def generalDefault(silver_m1):
    df_generalDefault = (
        silver_m1
            .withColumn("notificationsSent", lit([]).cast("array<string>"))
            .withColumn("submitNotificationStatus", lit(""))
            .withColumn("isFeePaymentEnabled", lit("Yes"))
            .withColumn("isRemissionsEnabled", lit("Yes"))
            .withColumn("isOutOfCountryEnabled", lit("Yes"))
            .withColumn("isIntegrated", lit("No"))
            .withColumn("isNabaEnabled", lit("No"))
            .withColumn("isNabaAdaEnabled", lit("Yes"))
            .withColumn("isNabaEnabledOoc", lit("No"))
            .withColumn("isCaseUsingLocationRefData", lit("Yes"))
            .withColumn("hasAddedLegalRepDetails", lit("Yes"))
            .withColumn("autoHearingRequestEnabled", lit("No"))
            .withColumn("isDlrmFeeRemissionEnabled", lit("Yes"))
            .withColumn("isDlrmFeeRefundEnabled", lit("Yes"))
            .withColumn("sendDirectionActionAvailable", lit("Yes"))
            .withColumn("changeDirectionDueDateActionAvailable", lit("Yes"))
            .withColumn("markEvidenceAsReviewedActionAvailable", lit("Yes"))
            .withColumn("uploadAddendumEvidenceActionAvailable", lit("No"))
            .withColumn("uploadAdditionalEvidenceActionAvailable", lit("Yes"))
            .withColumn("displayMarkAsPaidEventForPartialRemission", lit("No"))
            .withColumn("haveHearingAttendeesAndDurationBeenRecorded", lit("No"))
            .withColumn("markAddendumEvidenceAsReviewedActionAvailable", lit("No"))
            .withColumn("uploadAddendumEvidenceLegalRepActionAvailable", lit("No"))
            .withColumn("uploadAddendumEvidenceHomeOfficeActionAvailable", lit("No"))
            .withColumn("uploadAddendumEvidenceAdminOfficerActionAvailable", lit("No"))
            .withColumn("uploadAdditionalEvidenceHomeOfficeActionAvailable", lit("Yes"))
            .withColumn("directions", lit([]).cast("array<string>"))
            .withColumn("uploadHomeOfficeBundleAvailable", lit("No"))
            .withColumn("uploadHomeOfficeBundleActionAvailable", lit("No"))
            .withColumn("appealReviewOutcome", lit("decisionMaintained"))
            .withColumn("appealResponseAvailable", lit("Yes"))
            .withColumn("reviewedHearingRequirements", lit("No"))
            .withColumn("amendResponseActionAvailable", lit("Yes"))
            .withColumn("currentHearingDetailsVisible", lit("Yes"))
            .withColumn("reviewResponseActionAvailable", lit("No"))
            .withColumn("reviewHomeOfficeResponseByLegalRep", lit("Yes"))
            .withColumn("submitHearingRequirementsAvailable", lit("Yes"))
            .withColumn("uploadHomeOfficeAppealResponseActionAvailable", lit("No"))
            .select(
                "CaseNo",
                "notificationsSent",
                "submitNotificationStatus",
                "isFeePaymentEnabled",
                "isRemissionsEnabled",
                "isOutOfCountryEnabled",
                "isIntegrated",
                "isNabaEnabled",
                "isNabaAdaEnabled",
                "isNabaEnabledOoc",
                "isCaseUsingLocationRefData",
                "haveHearingAttendeesAndDurationBeenRecorded",
                "hasAddedLegalRepDetails",
                "autoHearingRequestEnabled",
                "isDlrmFeeRemissionEnabled",
                "isDlrmFeeRefundEnabled",
                "sendDirectionActionAvailable",
                "changeDirectionDueDateActionAvailable",
                "markEvidenceAsReviewedActionAvailable",
                "uploadAddendumEvidenceActionAvailable",
                "uploadAdditionalEvidenceActionAvailable",
                "displayMarkAsPaidEventForPartialRemission",
                "markAddendumEvidenceAsReviewedActionAvailable",
                "uploadAddendumEvidenceLegalRepActionAvailable",
                "uploadAddendumEvidenceHomeOfficeActionAvailable",
                "uploadAddendumEvidenceAdminOfficerActionAvailable",
                "uploadAdditionalEvidenceHomeOfficeActionAvailable",
                "directions",
                "uploadHomeOfficeBundleAvailable",
                "uploadHomeOfficeBundleActionAvailable",
                "appealReviewOutcome",
                "appealResponseAvailable",
                "reviewedHearingRequirements",
                "amendResponseActionAvailable",
                "currentHearingDetailsVisible",
                "reviewResponseActionAvailable",
                "reviewHomeOfficeResponseByLegalRep",
                "submitHearingRequirementsAvailable",
                "uploadHomeOfficeAppealResponseActionAvailable"
            )
    )

    return df_generalDefault


def documents(silver_m1):
    df_documents, df_audit_documents = AERb.documents(silver_m1)

    df_documents = (
        df_documents
            .withColumn("hearingRequirements", lit([]).cast("array<string>"))
    )

    common_inputFields = [lit("dv_representation"), lit("lu_appealType")]
    common_inputValues = [col("m1.dv_representation"), col("m1.lu_appealType")]

    df_audit_documents = (
        df_audit_documents.alias("audit")
            .join(df_documents.alias("documents"), on="CaseNo", how="left")
            .join(silver_m1.alias("m1"), on="CaseNo", how="left")
            .select(
                "audit.*",
                # hearingRequirements
                array(struct(*common_inputFields)).alias("hearingRequirements_inputFields"),
                array(struct(*common_inputValues)).alias("hearingRequirements_inputValues"),
                col("documents.hearingRequirements").alias("hearingRequirements_value"),
                lit("Yes").alias("hearingRequirements_Transformed")
            )
    )

    return df_documents, df_audit_documents


if __name__ == "__main__":
    pass
