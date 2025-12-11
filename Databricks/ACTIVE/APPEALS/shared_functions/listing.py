from pyspark.sql.functions import (
    col, collect_list, desc, expr, lit, row_number, when
)
from pyspark.sql.window import Window
from . import AwaitingEvidenceRespondant_b as AERb


def hearingRequirements(silver_m1, silver_m3, silver_c):
    window_spec = Window.partitionBy("CaseNo").orderBy(col("StatusId").desc())

    # Add row_number to get the row with the highest StatusId per CaseNo
    silver_m3_ranked = silver_m3.withColumn("row_num", row_number().over(window_spec))

    silver_m3_filtered = silver_m3_ranked.filter(
        (col("row_num") == 1) 
        & (
            ((col("CaseStatus").isin(37, 38)) & (col("Outcome").isin(0, 27, 37, 39, 40, 50)))
            | ((col("CaseStatus") == 26) & (col("Outcome").isin(40, 52)))
        )
    )

    silver_c_grouped = silver_c.groupBy("CaseNo").agg(collect_list(col("CategoryId")).alias("CategoryIdList"))

    df_hearingRequirements = (
        silver_m1.alias("m1")
            .join(silver_m3_filtered.alias("m3"), on="CaseNo", how="left")
            .join(silver_c_grouped.alias("c"), on="CaseNo", how="left")
            .withColumn("isAppellantAttendingTheHearing", lit("Yes"))
            .withColumn("isAppellantGivingOralEvidence", lit("Yes"))
            .withColumn("isWitnessesAttending", lit("No"))
            .withColumn("isEvidenceFromOutsideUkOoc", (
                when(expr("array_contains(CategoryIdList, 38)"), lit("Yes"))
                .otherwise(lit(None))
            ))
            .withColumn("isEvidenceFromOutsideUkInCountry", (
                when(expr("array_contains(CategoryIdList, 37)"), lit("Yes"))
                .otherwise(lit(None))
            ))
            .withColumn("isInterpreterServicesNeeded", (
                when((col("m1.Interpreter") == 1), lit("Yes"))
                .when((col("m1.Interpreter") == 0), lit("No"))
                .otherwise(lit(None))
            ))
            .withColumn("appellantInterpreterLanguageCategory", lit("PLACEHOLDER TODO"))
            .withColumn("appellantInterpreterSpokenLanguage", lit("PLACEHOLDER TODO"))
            .withColumn("appellantInterpreterSignLanguage", lit("PLACEHOLDER TODO"))
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
                .otherwise(lit(None))
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
                .otherwise(lit(None))
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
                "datesToAvoidYesNo",
            )
    )

    df_audit_hearingRequirements = (
        df_hearingRequirements.alias("hr")
            .join(silver_m1.alias("m1"), on="CaseNo", how="left")
            .join(silver_m3_filtered.alias("m3"), on="CaseNo", how="left")
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
                "datesToAvoidYesNo",
            )
    )
    
    return df_hearingRequirements, df_audit_hearingRequirements

def generalDefault(silver_m1): 
    df_generalDefault = AERb.generalDefault(silver_m1)
    df_representation = silver_m1.select("CaseNo", "dv_representation", "lu_appealType")

    df_generalDefault = df_generalDefault.join(df_representation, on="CaseNo", how="left")

    aip_conditions_generalDefault = (col("dv_representation") == "AIP") & (col("lu_appealType").isNotNull())
    lr_conditions_generalDefault = (col("dv_representation") == "LR") & (col("lu_appealType").isNotNull())

    df_generalDefault = (
        df_generalDefault
            .withColumn("caseArgumentAvailable", (when(lr_conditions_generalDefault, lit("Yes")).otherwise(lit(None))))
            .withColumn("reasonsForAppealDecision", (when(aip_conditions_generalDefault, lit("This is a migrated ARIA case. Please see the documents provided as part of the notice of appeal.")).otherwise(lit(None))))
            .withColumn("appealReviewOutcome", lit("decisionMaintained"))
            .withColumn("appealResponseAvailable", lit("Yes"))
            .withColumn("reviewedHearingRequirements", lit("No"))
            .withColumn("amendResponseActionAvailable", lit("Yes"))
            .withColumn("currentHearingDetailsVisible", lit("Yes"))
            .withColumn("reviewResponseActionAvailable", lit("No"))
            .withColumn("reviewHomeOfficeResponseByLegalRep", lit("Yes"))
            .withColumn("submitHearingRequirementsAvailable", lit("Yes"))
            .withColumn("uploadHomeOfficeAppealResponseActionAvailable", lit("No"))
    )

    df_generalDefault = df_generalDefault.drop("dv_representation", "lu_appealType")

    return df_generalDefault

def documents(silver_m1): 
    df_documents, df_audit_documents = AERb.documents(silver_m1)

    df_documents = (
        df_documents
            .withColumn("hearingRequirements", lit([]).cast("array<string>"))
    )
    
    return df_documents, df_audit_documents

if __name__ == "__main__":
    pass
