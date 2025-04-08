# Databricks notebook source
tables = [
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_ca_apt_country_detc",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_cl_ht_list_lt_hc_c_ls_adj",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_bfdiary_bftype",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_history_users",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_link_linkdetail",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_appealcatagory_catagory",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at",
    "hive_metastore.ariadm_arm_appeals.bronze_status_decisiontype",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_t_tt_ts_tm",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_ahr_hr",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_anm_nm",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_dr_rd",
    "hive_metastore.ariadm_arm_appeals.bronze_appealcase_rsd_sd",
    "hive_metastore.ariadm_arm_appeals.bronze_review_specific_direction",
    "hive_metastore.ariadm_arm_appeals.bronze_cost_award",
    "hive_metastore.ariadm_arm_appeals.bronze_cost_award_linked",
    "hive_metastore.ariadm_arm_appeals.bronze_costorder",
    "hive_metastore.ariadm_arm_appeals.bronze_hearing_points_change_reason",
    "hive_metastore.ariadm_arm_appeals.bronze_hearing_points_history",
    "hive_metastore.ariadm_arm_appeals.bronze_appeal_type_category",
    "hive_metastore.ariadm_arm_appeals.bronze_appeal_grounds",
    "hive_metastore.ariadm_arm_appeals.bronze_required_incompatible_adjudicator",
    "hive_metastore.ariadm_arm_appeals.bronze_case_adjudicator"
]

for table in tables:
    df = spark.read.table(table)
    duplicate_cases = df.groupBy("CaseNo").count().filter("count > 1")
    if duplicate_cases.count() > 0:
        print(f"Table {table} has duplicate CaseNo")

# COMMAND ----------

tables = spark.catalog.listTables("hive_metastore.ariadm_arm_appeals")
for table in tables:
    # if table.name.startswith("bronze_") or table.name.startswith("silver_"):
    if table.name.startswith("silver_"):
         table_name = f"hive_metastore.ariadm_arm_appeals.{table.name}"
         print(table_name)


# COMMAND ----------

tables = spark.catalog.listTables("hive_metastore.ariadm_arm_appeals")
for table in tables:
    # if table.name.startswith("bronze_") or table.name.startswith("silver_"):
    if table.name.startswith("silver_"):
        table_name = f"hive_metastore.ariadm_arm_appeals.{table.name}"
        df = spark.read.table(table_name)
        if "CaseNo" in df.columns:
            duplicate_cases = df.groupBy("CaseNo").count().filter("count > 1")
            if duplicate_cases.count() > 0:
                print(f"Table {table_name} has duplicate CaseNo")
            else:
                print(f"Table {table_name} has unique CaseNo")

# COMMAND ----------

spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealgrounds_detail").columns

# COMMAND ----------







# df_statusdecisiontype = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_statusdecisiontype_detail")
# print("Columns in hive_metastore.ariadm_arm_appeals.silver_statusdecisiontype_detail:", df_statusdecisiontype.columns)

df_transaction = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_transaction_detail")
print("Columns in hive_metastore.ariadm_arm_appeals.silver_transaction_detail:", df_transaction.columns)

# COMMAND ----------

# df_statusdecisiontype = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_statusdecisiontype_detail").groupBy("CaseNo").agg(
#     collect_list(struct('DecisionTypeDescription', 'DeterminationRequired', 'DoNotUse', 'State', 'BailRefusal', 'BailHOConsent')).alias("StatusDecisionTypes")
# )

# df_transaction = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_transaction_detail").groupBy("CaseNo").agg(
#     collect_list(struct('TransactionId', 'TransactionTypeId', 'TransactionMethodId', 'TransactionDate', 'Amount', 'ClearedDate', 'TransactionStatusId', 'OriginalPaymentReference', 'PaymentReference', 'AggregatedPaymentURN', 'PayerForename', 'PayerSurname', 'LiberataNotifiedDate', 'LiberataNotifiedAggregatedPaymentDate', 'BarclaycardTransactionId', 'Last4DigitsCard', 'TransactionNotes', 'ExpectedDate', 'ReferringTransactionId', 'CreateUserId', 'LastEditUserId', 'TransactionDescription', 'InterfaceDescription', 'AllowIfNew', 'DoNotUse', 'SumFeeAdjustment', 'SumPayAdjustment', 'SumTotalFee', 'SumTotalPay', 'SumBalance', 'GridFeeColumn', 'GridPayColumn', 'IsReversal', 'TransactionStatusDesc', 'TransactionStatusIntDesc', 'DoNotUseTransactionStatus', 'TransactionMethodDesc', 'TransactionMethodIntDesc', 'DoNotUseTransactionMethod', 'AmountDue', 'AmountPaid', 'FirstTierFee', 'TotalFeeAdjustments', 'TotalFeeDue', 'TotalPaymentsReceived', 'TotalPaymentAdjustments', 'BalanceDue')).alias("Transactions")
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC select CaseNo, count(*) from hive_metastore.ariadm_arm_td.bronze_ac_ca_ant_fl_dt_hc 
# MAGIC group by all
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select CaseNo,Forenames,Name, count(*) from hive_metastore.ariadm_arm_td.bronze_ac_ca_ant_fl_dt_hc 
# MAGIC group by all
# MAGIC having count(*) > 1

# COMMAND ----------

from pyspark.sql.functions import collect_list, struct

# Read unique CaseNo tables
df_appealcase = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcase_detail")
df_applicant = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_applicant_detail")
df_case_detail = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_case_detail")
df_costorder = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_costorder_detail")
df_dfdairy = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_dfdairy_detail")
df_hearingpointschange = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearingpointschange_detail")
df_newmatter = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_newmatter_detail")
df_reviewspecificdirection = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_reviewspecificdirection_detail")

# Read duplicate CaseNo tables and aggregate them
df_appealcategory = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealcategory_detail").groupBy("CaseNo").agg(
     collect_list(
        struct( 'CategoryDescription', 'Flag')
    ).alias("AppealCategory")
)

df_appealgrounds = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealgrounds_detail").groupBy("CaseNo").agg(
    collect_list(struct( 'AppealTypeId', 'AppealTypeDescription')).alias("AppealGrounds")
)

df_appealtypecategory = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_appealtypecategory_detail").groupBy("CaseNo").agg(
    collect_list(struct('AppealTypeCategoryId', 'AppealTypeId', 'CategoryId', 'FeeExempt')).alias("AppealTypeCategories")
)

df_case_adjudicator = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_case_adjudicator").groupBy("CaseNo").agg(
    collect_list(struct( 'Required', 'JudgeSurname', 'JudgeForenames', 'JudgeTitle')).alias("CaseAdjudicators")
)

df_costaward = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_costaward_detail").groupBy("CaseNo").agg(
    collect_list(struct('CostAwardId', 'Name', 'Forenames', 'Title', 'DateOfApplication', 'TypeOfCostAward', 'ApplyingParty', 'PayingParty', 'MindedToAward', 'ObjectionToMindedToAward', 'CostsAwardDecision', 'DateOfDecision', 'CostsAmount', 'OutcomeOfAppeal', 'AppealStage', 'AppealStageDescription')).alias("CostAwards")
)

df_dependent = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_dependent_detail").groupBy("CaseNo").agg(
    collect_list(struct('AppellantId', 'CaseAppellantRelationship', 'PortReference', 'AppellantName', 'AppellantForenames', 'AppellantTitle', 'AppellantBirthDate', 'AppellantAddress1', 'AppellantAddress2', 'AppellantAddress3', 'AppellantAddress4', 'AppellantAddress5', 'AppellantPostcode', 'AppellantTelephone', 'AppellantFax', 'Detained', 'AppellantEmail', 'FCONumber', 'PrisonRef', 'DetentionCentre', 'CentreTitle', 'DetentionCentreType', 'DCAddress1', 'DCAddress2', 'DCAddress3', 'DCAddress4', 'DCAddress5', 'DCPostcode', 'DCFax', 'DCSdx', 'Country', 'Nationality', 'Code', 'DoNotUseCountry', 'CountrySdx', 'DoNotUseNationality')).alias("Dependents")
)

df_documents = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_documents_detail").groupBy("CaseNo").agg(
    collect_list(struct( 'ReceivedDocumentId', 'DateRequested', 'DateRequired', 'DateReceived', 'NoLongerRequired', 'RepresentativeDate', 'POUDate', 'DocumentDescription', 'DoNotUse', 'Auditable')).alias("Documents")
)

df_hearingpointshistory = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_hearingpointshistory_detail").groupBy("CaseNo").agg(
    collect_list(struct('CaseNo', 'StatusId', 'HearingPointsHistoryId', 'HistDate', 'HistType', 'UserId', 'DefaultPoints', 'InitialPoints', 'FinalPoints')).alias("HearingPointHistory")
)

df_history = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_history_detail").groupBy("CaseNo").agg(
    collect_list(struct('HistoryId', 'CaseNo', 'HistDate', 'fileLocation', 'lastDocument', 'HistType', 'HistoryComment', 'StatusId', 'UserName', 'UserType', 'Fullname', 'Extension', 'DoNotUse', 'HistTypeDescription')).alias("History")
)

df_humanright = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_humanright_detail").groupBy("CaseNo").agg(
    collect_list(struct('HumanRightId', 'HumanRightDescription', 'DoNotShow', 'Priority')).alias("HumanRights")
)

df_link = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_link_detail").groupBy("CaseNo").agg(
    collect_list(struct( 'LinkNo', 'LinkDetailComment', 'LinkName', 'LinkForeNames', 'LinkTitle')).alias("LinkedCases")
)

df_linkedcostaward = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_linkedcostaward_detail").groupBy("CaseNo").agg(
    collect_list(struct('CostAwardId', 'CaseNo', 'LinkNo', 'Name', 'Forenames', 'Title', 'DateOfApplication', 'TypeOfCostAward', 'ApplyingParty', 'PayingParty', 'MindedToAward', 'ObjectionToMindedToAward', 'CostsAwardDecision', 'DateOfDecision', 'CostsAmount', 'OutcomeOfAppeal', 'AppealStage', 'AppealStageDescription')).alias("LinkedCostAwards")
)

df_list_detail = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_list_detail").groupBy("CaseNo").agg(
    collect_list(struct('Outcome', 'CaseStatus', 'StatusId', 'TimeEstimate', 'ListNumber', 'HearingDuration', 'StartTime', 'HearingTypeDesc', 'HearingTypeEst', 'DoNotUse', 'ListAdjudicatorId', 'ListAdjudicatorSurname', 'ListAdjudicatorForenames', 'ListAdjudicatorNote', 'ListAdjudicatorTitle', 'ListName', 'ListStartTime', 'ListTypeDesc', 'ListType', 'DoNotUseListType', 'CourtName', 'DoNotUseCourt', 'HearingCentreDesc')).alias("Lists")
)

df_required_incompatible_adjudicator = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_required_incompatible_adjudicator").groupBy("CaseNo").agg(
    collect_list(struct('Required', 'JudgeSurname', 'JudgeForenames', 'JudgeTitle')).alias("RequiredIncompatibleAdjudicators")
)

df_status = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_status_detail").groupBy("CaseNo").agg(
    collect_list(struct('StatusId', 'CaseNo', 'CaseStatus', 'DateReceived', 'StatusDetailAdjudicatorId', 'Allegation', 'KeyDate', 'MiscDate1', 'Notes1', 'Party', 'InTime', 'MiscDate2', 'MiscDate3', 'Notes2', 'DecisionDate', 'Outcome', 'Promulgated', 'InterpreterRequired', 'AdminCourtReference', 'UKAITNo', 'FC', 'VideoLink', 'Process', 'COAReferenceNumber', 'HighCourtReference', 'OutOfTime', 'ReconsiderationHearing', 'DecisionSentToHO', 'DecisionSentToHODate', 'MethodOfTyping', 'CourtSelection', 'DecidingCentre', 'Tier', 'RemittalOutcome', 'UpperTribunalAppellant', 'ListRequirementTypeId', 'UpperTribunalHearingDirectionId', 'ApplicationType', 'NoCertAwardDate', 'CertRevokedDate', 'WrittenOffFileDate', 'ReferredEnforceDate', 'Letter1Date', 'Letter2Date', 'Letter3Date', 'ReferredFinanceDate', 'WrittenOffDate', 'CourtActionAuthDate', 'BalancePaidDate', 'WrittenReasonsRequestedDate', 'TypistSentDate', 'TypistReceivedDate', 'WrittenReasonsSentDate', 'ExtemporeMethodOfTyping', 'Extempore', 'DecisionByTCW', 'InitialHearingPoints', 'FinalHearingPoints', 'HearingPointsChangeReasonId', 'OtherCondition', 'OutcomeReasons', 'AdditionalLanguageId', 'CostOrderAppliedFor', 'HearingCourt', 'CaseStatusDescription', 'DoNotUseCaseStatus', 'CaseStatusHearingPoints', 'ContactStatus', 'SCCourtName', 'SCAddress1', 'SCAddress2', 'SCAddress3', 'SCAddress4', 'SCAddress5', 'SCPostcode', 'SCTelephone', 'SCForenames', 'SCTitle', 'ReasonAdjourn', 'DoNotUseReason', 'LanguageDescription', 'DoNotUseLanguage', 'DecisionTypeDescription', 'DeterminationRequired', 'DoNotUse', 'State', 'BailRefusal', 'BailHOConsent', 'StatusDetailAdjudicatorSurname', 'StatusDetailAdjudicatorForenames', 'StatusDetailAdjudicatorTitle', 'StatusDetailAdjudicatorNote', 'DeterminationByJudgeSurname', 'DeterminationByJudgeForenames', 'DeterminationByJudgeTitle', 'CurrentStatus', 'AdjournmentParentStatusId')).alias("Statuses")
)

df_statusdecisiontype = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_statusdecisiontype_detail").groupBy("CaseNo").agg(
    collect_list(struct('DecisionTypeDescription', 'DeterminationRequired', 'DoNotUse', 'State', 'BailRefusal', 'BailHOConsent')).alias("StatusDecisionTypes")
)

df_transaction = spark.read.table("hive_metastore.ariadm_arm_appeals.silver_transaction_detail").groupBy("CaseNo").agg(
    collect_list(struct('TransactionId', 'TransactionTypeId', 'TransactionMethodId', 'TransactionDate', 'Amount', 'ClearedDate', 'TransactionStatusId', 'OriginalPaymentReference', 'PaymentReference', 'AggregatedPaymentURN', 'PayerForename', 'PayerSurname', 'LiberataNotifiedDate', 'LiberataNotifiedAggregatedPaymentDate', 'BarclaycardTransactionId', 'Last4DigitsCard', 'TransactionNotes', 'ExpectedDate', 'ReferringTransactionId', 'CreateUserId', 'LastEditUserId', 'TransactionDescription', 'InterfaceDescription', 'AllowIfNew', 'DoNotUse', 'SumFeeAdjustment', 'SumPayAdjustment', 'SumTotalFee', 'SumTotalPay', 'SumBalance', 'GridFeeColumn', 'GridPayColumn', 'IsReversal', 'TransactionStatusDesc', 'TransactionStatusIntDesc', 'DoNotUseTransactionStatus', 'TransactionMethodDesc', 'TransactionMethodIntDesc', 'DoNotUseTransactionMethod', 'AmountDue', 'AmountPaid', 'FirstTierFee', 'TotalFeeAdjustments', 'TotalFeeDue', 'TotalPaymentsReceived', 'TotalPaymentAdjustments', 'BalanceDue')).alias("Transactions")
)

# Join all tables
df_combined = (
    df_appealcase
    .join(df_applicant, "CaseNo", "left")
    .join(df_case_detail, "CaseNo", "left")
    .join(df_costorder, "CaseNo", "left")
    .join(df_dfdairy, "CaseNo", "left")
    .join(df_hearingpointschange, "CaseNo", "left")
    .join(df_newmatter, "CaseNo", "left")
    .join(df_reviewspecificdirection, "CaseNo", "left")
    .join(df_appealcategory, "CaseNo", "left")
    .join(df_appealgrounds, "CaseNo", "left")
    .join(df_appealtypecategory, "CaseNo", "left")
    .join(df_case_adjudicator, "CaseNo", "left")
    .join(df_costaward, "CaseNo", "left")
    .join(df_dependent, "CaseNo", "left")
    .join(df_documents, "CaseNo", "left")
    .join(df_hearingpointshistory, "CaseNo", "left")
    .join(df_history, "CaseNo", "left")
    .join(df_humanright, "CaseNo", "left")
    .join(df_link, "CaseNo", "left")
    .join(df_linkedcostaward, "CaseNo", "left")
    .join(df_list_detail, "CaseNo", "left")
    .join(df_required_incompatible_adjudicator, "CaseNo", "left")
    .join(df_status, "CaseNo", "left")
    .join(df_statusdecisiontype, "CaseNo", "left")
    .join(df_transaction, "CaseNo", "left")
)

display(df_combined)


# COMMAND ----------

df_appealcase.count()

# COMMAND ----------

from pyspark.sql.functions import collect_list, struct

df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

# Aggregate Other Centres
grouped_centres = df_other_centres.groupBy("AdjudicatorId").agg(
    collect_list("OtherCentres").alias("OtherCentres")
)

# Aggregate Roles
grouped_roles = df_roles.groupBy("AdjudicatorId").agg(
    collect_list(
        struct("Role", "DateOfAppointment", "EndDateOfAppointment")
    ).alias("Roles")
).cache()

# Aggregate History
grouped_history = df_history.groupBy("AdjudicatorId").agg(
    collect_list(
        struct("HistDate", "HistType", "UserName", "Comment")
    ).alias("History")
)

# Join all aggregated data with JudicialOfficerDetails
df_combined = (
    df_judicial_officer_details
    .join(grouped_centres, "AdjudicatorId", "left")
    .join(grouped_roles, "AdjudicatorId", "left")
    .join(grouped_history, "AdjudicatorId", "left")
)

display(df_combined)

# COMMAND ----------

# DBTITLE 1,M1 values- AppealCaseDetail
# MAGIC %sql
# MAGIC -- done check for AdditionalGrounds multiple recods 
# MAGIC with cte as (
# MAGIC select CaseNo, hoRef,CCDAppealNum,CaseRepDXNo1,CaseRepDXNo2,AppealTypeId,DateApplicationLodged,DateOfApplicationDecision,DateLodged,AdditionalGrounds,AppealCategories,MREmbassy,NationalityId,CountryId,PortId,HumanRights,DateOfIssue,fileLocationNote,DocumentsReceived,TransferOutDate,RemovalDate,DeportationDate,ProvisionalDestructionDate,AppealReceivedBy,CRRespondent,RepresentativeRef,Language,HOInterpreter,CourtPreference,CertifiedDate,CertifiedRecordedDate,NoticeSentDate,DateReceived,ReferredToJudgeDate,RespondentName,MRPOU,RespondentName,RespondentAddress1,RespondentAddress2,RespondentAddress3,RespondentAddress4,RespondentAddress5,RespondentPostcode,RespondentTelephone,RespondentFax,RespondentEmail,CRReference,CRContact,CaseRepName,RespondentAddress1,RespondentAddress2,RespondentAddress3,RespondentAddress4,RespondentAddress5,CaseRepPostcode,CaseRepTelephone,CaseRepFax,CaseRepEmail,LSCCommission,CRReference,CRContact,RepTelephone,RespondentFax,RespondentEmail,StatutoryClosureDate,ThirdCountryId,SubmissionURN, DateReinstated,CaseOutcomeId
# MAGIC from hive_metastore.ariadm_arm_appeals.bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1
# MAGIC
# MAGIC -- %sql
# MAGIC -- select CaseNo, AdditionalGrounds
# MAGIC -- from hive_metastore.ariadm_arm_appeals.bronze_appealcase_cr_cs_ca_fl_cres_mr_res_lang

# COMMAND ----------

# DBTITLE 1,M2 values
# MAGIC %sql
# MAGIC -- done. but review
# MAGIC with cte as (
# MAGIC select AppellantName,AppellantForenames,AppellantTitle,DetentionCentre,AppellantBirthDate,AppellantId,PortReference,FCONumber,Detained,AppellantAddress1,AppellantAddress2,AppellantAddress3,AppellantAddress4,AppellantAddress5,Country,DCPostcode,AppellantTelephone,AppellantEmail,PrisonRef,CaseNo
# MAGIC from hive_metastore.ariadm_arm_appeals.bronze_appealcase_ca_apt_country_detc
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC -- where CaseAppellantRelationship is null
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M3 values
# MAGIC %sql
# MAGIC -- multiple values to handel in hearing details(under status)
# MAGIC with cte as (
# MAGIC select CourtName,ListName,ListType,HearingTypeDesc,ListStartTime,HearingTypeEst,Outcome,ListAdjudicatorNote,CaseNo   
# MAGIC from hive_metastore.ariadm_arm_appeals.bronze_appealcase_cl_ht_list_lt_hc_c_ls_adj
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M4 values
# MAGIC %sql
# MAGIC --done B/F Diary
# MAGIC with cte as (
# MAGIC select EntryDate,BFTypeDescription, Entry,DateCompleted,CaseNo from hive_metastore.ariadm_arm_appeals.bronze_appealcase_bfdiary_bftype
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1
# MAGIC  

# COMMAND ----------

# DBTITLE 1,M5 values
# MAGIC %sql
# MAGIC --done History
# MAGIC with cte as (
# MAGIC select HistDate,HistType,UserName,HistoryComment,CaseNo from hive_metastore.ariadm_arm_appeals.bronze_appealcase_history_users
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select LinkNo,LinkDetailComment,a.CaseNo from hive_metastore.ariadm_arm_appeals.bronze_appealcase_link_linkdetail a join hive_metastore.ariadm_arm_appeals.stg_appeals_filtered b on a.CaseNo = b.CaseNo

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from hive_metastore.ariadm_arm_appeals.bronze_appealcase_link_linkdetail

# COMMAND ----------

# DBTITLE 1,M6: linked Cost Award
# MAGIC %sql 
# MAGIC select * from  hive_metastore.ariadm_arm_appeals.bronze_cost_award
# MAGIC where CostAwardId in (
# MAGIC select max(CostAwardId) from  hive_metastore.ariadm_arm_appeals.bronze_cost_award
# MAGIC where linkno = '43' and CaseNo != 'IA/00009/2014'
# MAGIC group by CaseNo)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ariadm_arm_appeals.bronze_cost_award
# MAGIC where CaseNo = 'HR/00040/2008'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ariadm_arm_appeals.bronze_appealcase_link_linkdetail
# MAGIC  where CaseNO = 'IA/00009/2014'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ariadm_arm_appeals.bronze_appealcase_link_linkdetail a join hive_metastore.ariadm_arm_appeals.bronze_cost_award b on a.CaseNo = b.CaseNo 

# COMMAND ----------

# DBTITLE 1,m6 values
# MAGIC %sql
# MAGIC -- need confirmation Linked Files missing- copy(disabled), filenumber
# MAGIC with cte as (
# MAGIC select LinkNo,LinkDetailComment,CaseNo from hive_metastore.ariadm_arm_appeals.bronze_appealcase_link_linkdetail
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 values
# MAGIC %sql
# MAGIC -- Status table done and dynamic pages need to be sorted 
# MAGIC with cte as (
# MAGIC select ApplicationType,DateReceived,CaseStatus,KeyDate,MiscDate1,InterpreterRequired,RemittalOutcome,MiscDate2,UpperTribunalAppellant,TypistSentDate,InitialHearingPoints,FinalHearingPoints,HearingPointsChangeReasonId,Tier,
# MAGIC DecisionDate,InTime,Party,Notes1,Letter1Date,DecisionByTCW,MethodOfTyping,Outcome,Promulgated,UKAITNo,WrittenReasonsRequestedDate,process,TypistSentDate,ExtemporeMethodOfTyping,WrittenReasonsSentDate,CaseNo,DecisionSentToHO,Allegation,DecidingCentre,DecisionSentToHODate,Extempore,OutOfTime,NoCertAwardDate,CertRevokedDate,WrittenOffFileDate,ReferredEnforceDate,Letter1Date,Letter2Date,Letter3Date,ReferredFinanceDate,CourtActionAuthDate,ListRequirementTypeId,judicialOfficer,
# MAGIC UpperTribunalHearingDirectionId,CourtSelection,HighCourtReference
# MAGIC from hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1
# MAGIC
# MAGIC -- %sql
# MAGIC -- select CaseStatus, a.CaseNo, count(*) from hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs a join hive_metastore.ariadm_arm_appeals.stg_appeals_filtered b on a.CaseNo = b.CaseNo
# MAGIC -- group by CaseStatus, a.CaseNo
# MAGIC -- having count(*) > 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC CaseNo,
# MAGIC CaseStatus, KeyDate, InterpreterRequired, 
# MAGIC -- AdjudicatorId,
# MAGIC  MiscDate2, TypistSentDate,
# MAGIC InitialHearingPoints,
# MAGIC FinalHearingPoints,
# MAGIC HearingPointsChangeReasonId,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC Extempore,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC TypistSentDate,
# MAGIC ExtemporeMethodOfTyping,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC WrittenReasonsSentDate
# MAGIC -- AdjudicatorSurname,
# MAGIC -- AdjudicatorForenames,
# MAGIC -- AdjudicatorTitle
# MAGIC from hive_metastore.ariadm_arm_appeals.silver_status_detail
# MAGIC where --CaseStatus = 37 
# MAGIC --and 
# MAGIC CaseNO = 'TH/00137/2003'
# MAGIC
# MAGIC
# MAGIC -- %sql
# MAGIC -- select 
# MAGIC -- CaseNo,
# MAGIC -- CaseStatus,
# MAGIC -- AdjudicatorId,
# MAGIC -- AdjudicatorSurname,
# MAGIC -- AdjudicatorForenames,
# MAGIC -- AdjudicatorTitle
# MAGIC -- from hive_metastore.ariadm_arm_appeals.bronze_appealcase_cl_ht_list_lt_hc_c_ls_adj -- M3
# MAGIC -- where --CaseStatus = 37 --and 
# MAGIC -- CaseNO = 'HX/00050/2003'

# COMMAND ----------

# DBTITLE 1,m7First Tier - Hearing
# MAGIC %sql
# MAGIC with cte as (
# MAGIC select 
# MAGIC CaseNo,
# MAGIC CaseStatus, KeyDate, InterpreterRequired, 
# MAGIC -- AdjudicatorId,
# MAGIC  MiscDate2, TypistSentDate,
# MAGIC InitialHearingPoints,
# MAGIC FinalHearingPoints,
# MAGIC HearingPointsChangeReasonId,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC Extempore,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC TypistSentDate,
# MAGIC ExtemporeMethodOfTyping,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC WrittenReasonsSentDate
# MAGIC -- AdjudicatorSurname,
# MAGIC -- AdjudicatorForenames,
# MAGIC -- AdjudicatorTitle
# MAGIC from hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1
# MAGIC

# COMMAND ----------

# DBTITLE 1,m7-First Tier - Paper
# MAGIC %sql
# MAGIC with cte as (
# MAGIC select 
# MAGIC CaseStatus,
# MAGIC KeyDate,
# MAGIC InterpreterRequired,
# MAGIC AdjudicatorId,
# MAGIC MiscDate2,
# MAGIC RemittalOutcome,
# MAGIC UpperTribunalAppellant,
# MAGIC TypistSentDate,
# MAGIC InitialHearingPoints,
# MAGIC FinalHearingPoints,
# MAGIC HearingPointsChangeReasonId,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC CaseStatus,
# MAGIC DateReceived,
# MAGIC Party,
# MAGIC AdjudicatorId,
# MAGIC OutOfTime,
# MAGIC AdjudicatorId,
# MAGIC MiscDate1,
# MAGIC DecisionSentToHO,
# MAGIC FinalHearingPoints,
# MAGIC InitialHearingPoints,
# MAGIC HearingPointsChangeReasonId,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC Extempore,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC TypistSentDate,
# MAGIC ExtemporeMethodOfTyping,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC WrittenReasonsSentDate,
# MAGIC AdjudicatorSurname,
# MAGIC AdjudicatorForenames,
# MAGIC AdjudicatorTitle
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1
# MAGIC

# COMMAND ----------

# DBTITLE 1,m7 First Tier Permission Application
# MAGIC %sql
# MAGIC with cte as (
# MAGIC SELECT 
# MAGIC CaseStatus,
# MAGIC DateReceived,
# MAGIC Party,
# MAGIC AdjudicatorId,
# MAGIC MiscDate1,
# MAGIC DecisionSentToHO,
# MAGIC InitialHearingPoints,
# MAGIC FinalHearingPoints,
# MAGIC HearingPointsChangeReasonId,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC TypistSentDate,
# MAGIC ExtemporeMethodOfTyping,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC WrittenReasonsSentDate
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1
# MAGIC

# COMMAND ----------

# DBTITLE 1,m7 Preliminary Issue
# MAGIC %sql
# MAGIC with cte as (
# MAGIC SELECT 
# MAGIC CaseStatus,
# MAGIC KeyDate,
# MAGIC InterpreterRequired,
# MAGIC Allegation,
# MAGIC MiscDate1,
# MAGIC AdjudicatorId,
# MAGIC MiscDate2,
# MAGIC DecidingCentre,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC Extempore,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC TypistSentDate,
# MAGIC ExtemporeMethodOfTyping,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC WrittenReasonsSentDate
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1
# MAGIC

# COMMAND ----------

# DBTITLE 1,M7 Case Management Review
# MAGIC %sql
# MAGIC with cte as (
# MAGIC SELECT 
# MAGIC CaseStatus,
# MAGIC process,
# MAGIC Tier,
# MAGIC KeyDate,
# MAGIC InterpreterRequired,
# MAGIC MiscDate2,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC TypistSentDate,
# MAGIC ExtemporeMethodOfTyping,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC WrittenReasonsSentDate
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1
# MAGIC

# COMMAND ----------

# DBTITLE 1,m7 Set Aside Application
# MAGIC %sql
# MAGIC SELECT
# MAGIC CaseStatus,
# MAGIC process,
# MAGIC DateReceived,
# MAGIC MiscDate1,
# MAGIC AdjudicatorId,
# MAGIC Party,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,m7  Closed - Fee Not Paid
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT
# MAGIC CaseStatus,
# MAGIC NoCertAwardDate,
# MAGIC CertRevokedDate,
# MAGIC WrittenOffFileDate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,m7 Review of Cost Order
# MAGIC %sql
# MAGIC with cte as (
# MAGIC SELECT
# MAGIC CaseStatus,
# MAGIC process,
# MAGIC KeyDate,
# MAGIC InterpreterRequired,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo
# MAGIC
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs

# COMMAND ----------

# DBTITLE 1,M7 Case Closed Fee Outstanding
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT
# MAGIC CaseStatus,
# MAGIC ReferredEnforceDate,
# MAGIC Letter1Date,
# MAGIC Letter2Date,
# MAGIC Letter3Date,
# MAGIC ReferredFinanceDate,
# MAGIC WrittenOffDate,
# MAGIC CourtActionAuthDate,
# MAGIC BalancePaidDate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M71. On Hold - Chargeback Taken                                 2. Upper Trib Case on Hold - Fee Not Paid
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   select
# MAGIC CaseStatus,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,m7 1. Immigration Judge - Hearing                                 2. Immigration Judge - Paper
# MAGIC %sql
# MAGIC with cte as (
# MAGIC SELECT
# MAGIC CaseStatus,
# MAGIC KeyDate,
# MAGIC InterpreterRequired,
# MAGIC AdjudicatorSurname,
# MAGIC AdjudicatorForenames,
# MAGIC AdjudicatorTitle,
# MAGIC MiscDate2,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,m7 1. Panel Heaing (Legal)           2. Panel Heaing (Legal / Non Legal)
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   select
# MAGIC KeyDate,
# MAGIC InterpreterRequired,
# MAGIC AdjudicatorId,
# MAGIC MiscDate2,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 Upper Tribunal Hearing
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC CaseStatus,
# MAGIC KeyDate,
# MAGIC InterpreterRequired,
# MAGIC AdjudicatorSurname,
# MAGIC AdjudicatorForenames,
# MAGIC AdjudicatorTitle,
# MAGIC MiscDate2,
# MAGIC Party,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC TypistSentDate,
# MAGIC ExtemporeMethodOfTyping,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC WrittenReasonsSentDate
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 Upper Tribunal Hearing - Continuance
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC CaseStatus,
# MAGIC KeyDate,
# MAGIC InterpreterRequired,
# MAGIC AdjudicatorSurname,
# MAGIC AdjudicatorForenames,
# MAGIC AdjudicatorTitle,
# MAGIC MiscDate2,
# MAGIC Party,
# MAGIC UpperTribunalHearingDirectionId,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC TypistSentDate,
# MAGIC ExtemporeMethodOfTyping,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC WrittenReasonsSentDate
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 Upper Tribunal Permission Application
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC CaseStatus,
# MAGIC DateReceived,
# MAGIC Party,
# MAGIC AdjudicatorId,
# MAGIC MiscDate1,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC ListRequirementTypeId
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 Upper Tribunal Oral Permission Hearing
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC CaseStatus,
# MAGIC KeyDate,
# MAGIC InterpreterRequired,
# MAGIC AdjudicatorSurname,
# MAGIC AdjudicatorForenames,
# MAGIC AdjudicatorTitle,
# MAGIC MiscDate2,
# MAGIC Party,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC TypistSentDate,
# MAGIC ExtemporeMethodOfTyping,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC WrittenReasonsSentDate
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 Appellate Court
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC CaseStatus,
# MAGIC CourtSelection,
# MAGIC DateReceived,
# MAGIC KeyDate,
# MAGIC Party,
# MAGIC COAReferenceNumber,
# MAGIC Notes2,
# MAGIC DecisionDate,
# MAGIC Outcome,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1
# MAGIC
# MAGIC -- %sql
# MAGIC -- with cte as (
# MAGIC --   SELECT 
# MAGIC -- max(StatusId) as StatusId, CaseNo
# MAGIC -- FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs
# MAGIC -- group by caseNo)
# MAGIC -- select a.CaseStatus as CurrentStatus, a.CaseNo from hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs a
# MAGIC -- join cte b on a.CaseNo = b.CaseNo and a.StatusId = b.StatusId

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC max(StatusId) as StatusId, CaseNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs
# MAGIC group by caseNo)
# MAGIC select a.CaseStatus as CurrentStatus, a.CaseNo from hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs a
# MAGIC join cte b on a.CaseNo = b.CaseNo and a.StatusId = b.StatusId
# MAGIC where a.CaseNo = 'AA/00001/2014'

# COMMAND ----------

# DBTITLE 1,M7 PTA Direct to Appellate Court
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC CourtSelection,
# MAGIC Party,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 Permission to Appeal
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC CaseStatus,
# MAGIC CourtSelection,
# MAGIC Party,
# MAGIC MiscDate1,
# MAGIC AdjudicatorId,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 High Court Review
CaseStatus
DateReceived
HighCourtReference
Party
DecisionDate
Outcome
Promulgated
UKAITNo
FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
select CaseNo, count(*) from cte
group by CaseNo
having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 High Court Review (Filter)
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC CaseStatus,
# MAGIC DateReceived,
# MAGIC HighCourtReference,
# MAGIC Party,
# MAGIC AdjudicatorId,
# MAGIC MiscDate1,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 1. Judicial Review Hearing                                 2. Judicial Review Oral Permission Hearing
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC CaseStatus,
# MAGIC KeyDate,
# MAGIC InterpreterRequired,
# MAGIC Party,
# MAGIC AdjudicatorSurname,
# MAGIC AdjudicatorForenames,
# MAGIC AdjudicatorTitle,
# MAGIC MiscDate2,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC DecisionByTCW,
# MAGIC MethodOfTyping,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC UKAITNo,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC TypistSentDate,
# MAGIC ExtemporeMethodOfTyping,
# MAGIC CaseNo,
# MAGIC WrittenReasonsRequestedDate,
# MAGIC WrittenReasonsSentDate
# MAGIC
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M7 Judicial Review Permission Application
# MAGIC %sql
# MAGIC with cte as (
# MAGIC   SELECT 
# MAGIC CaseStatus,
# MAGIC ApplicationType,
# MAGIC DateReceived,
# MAGIC Party,
# MAGIC DecisionSentToHODate,
# MAGIC DecisionDate,
# MAGIC Outcome,
# MAGIC Promulgated,
# MAGIC CaseNo,
# MAGIC UKAITNo
# MAGIC FROM hive_metastore.ariadm_arm_appeals.bronze_appealcase_status_sc_ra_cs)
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,M10-payment summary
appeal_case = spark.read.table("hive_metastore.ariadm_arm_appeals.raw_appealcase").alias("ac")
case_fee_summary = spark.read.table("hive_metastore.ariadm_arm_appeals.raw_casefeesummary").alias("cfs")
fee_satisfaction = spark.read.table("hive_metastore.ariadm_arm_appeals.raw_feesatisfaction").alias("fs")
payment_remission_reason = spark.read.table("hive_metastore.ariadm_arm_appeals.raw_paymentremissionreason").alias("prr")
port = spark.read.table("hive_metastore.ariadm_arm_appeals.raw_port").alias("p")
embassy = spark.read.table("hive_metastore.ariadm_arm_appeals.raw_embassy").alias("e")
hearing_centre = spark.read.table("hive_metastore.ariadm_arm_appeals.raw_hearingcentre").alias("hc")
case_sponsor = spark.read.table("hive_metastore.ariadm_arm_appeals.raw_casesponsor").alias("cs")
appeal_grounds = spark.read.table("hive_metastore.ariadm_arm_appeals.raw_appealgrounds").alias("ag")
appeal_type = spark.read.table("hive_metastore.ariadm_arm_appeals.raw_appealtype").alias("at")

# max(CaseFeeSummaryId)

# COMMAND ----------

appeal_case.select('CaseNo').distinct().count()

# COMMAND ----------

from pyspark.sql.functions import col

appeal_case.alias("ac")\
.join(case_fee_summary.alias("cfs"), (col("ac.CaseNo") == col("cfs.CaseNo")), "left_outer")\
.join(fee_satisfaction.alias("fs"), col("ac.FeeSatisfactionId") == col("fs.FeeSatisfactionId"), "left_outer")\
.join(payment_remission_reason.alias("prr"), col("cfs.PaymentRemissionReason") == col("prr.PaymentRemissionReasonId"), "left_outer")\
.join(port.alias("p"), col("ac.PortId") == col("p.PortId"), "left_outer")\
.join(embassy.alias("e"), col("ac.VVEmbassyId") == col("e.EmbassyId"), "left_outer")\
.join(hearing_centre.alias("hc"), col("ac.CentreId") == col("hc.CentreId"), "left_outer")\
.join(case_sponsor.alias("cs"), col("ac.CaseNo") == col("cs.CaseNo"), "left_outer")\
.join(appeal_grounds.alias("ag"), col("ac.CaseNo") == col("ag.CaseNo"), "left_outer")\
.join(appeal_type.alias("at"), col("ag.AppealTypeId") == col("at.AppealTypeId"), "left_outer")\
.select(col('ac.CaseNo')).count()

# COMMAND ----------

from pyspark.sql.functions import col

appeal_case.alias("ac")\
.join(case_fee_summary.alias("cfs"), (col("ac.CaseNo") == col("cfs.CaseNo")), "left_outer")\
.join(fee_satisfaction.alias("fs"), col("ac.FeeSatisfactionId") == col("fs.FeeSatisfactionId"), "left_outer")\
.join(payment_remission_reason.alias("prr"), col("cfs.PaymentRemissionReason") == col("prr.PaymentRemissionReasonId"), "left_outer")\
.join(port.alias("p"), col("ac.PortId") == col("p.PortId"), "left_outer")\
.join(embassy.alias("e"), col("ac.VVEmbassyId") == col("e.EmbassyId"), "left_outer")\
.join(hearing_centre.alias("hc"), col("ac.CentreId") == col("hc.CentreId"), "left_outer")\
.join(case_sponsor.alias("cs"), col("ac.CaseNo") == col("cs.CaseNo"), "left_outer")\
.select(col('ac.CaseNo')).count()

# COMMAND ----------

appeal_case
        .join(case_fee_summary, col("ac.CaseNo") == col("cfs.CaseNo"), "left_outer")
        .join(fee_satisfaction, col("ac.FeeSatisfactionId") == col("fs.FeeSatisfactionId"), "left_outer")
        .join(payment_remission_reason, col("cfs.PaymentRemissionReason") == col("prr.PaymentRemissionReasonId"), "left_outer")
        .join(port, col("ac.PortId") == col("p.PortId"), "left_outer")
        .join(embassy, col("ac.VVEmbassyId") == col("e.EmbassyId"), "left_outer")
        .join(hearing_centre, col("ac.CentreId") == col("hc.CentreId"), "left_outer")
        .join(case_sponsor, col("ac.CaseNo") == col("cs.CaseNo"), "left_outer")
        .join(appeal_grounds, col("ac.CaseNo") == col("ag.CaseNo"), "left_outer")
        .join(appeal_type, col("ag.AppealTypeId") == col("at.AppealTypeId"), "left_outer")
        .select(
            trim(col("ac.CaseNo")).alias('CaseNo'),
            col("cfs.CaseFeeSummaryId"),
            col("cfs.DatePosting1stTier"),
            col("cfs.DatePostingUpperTier"),
            col("cfs.DateCorrectFeeReceived"),
            col("cfs.DateCorrectFeeDeemedReceived"),
            col("cfs.PaymentRemissionrequested"),
            col("cfs.PaymentRemissionGranted"),
            col("cfs.PaymentRemissionReason"),
            col("cfs.PaymentRemissionReasonNote"),
            col("cfs.ASFReferenceNo"),
            col("cfs.ASFReferenceNoStatus"),
            col("cfs.LSCReference"),
            col("cfs.LSCStatus"),
            col("cfs.LCPRequested"),
            col("cfs.LCPOutcome"),
            col("cfs.S17Reference"),
            col("cfs.S17ReferenceStatus"),
            col("cfs.SubmissionURNCopied"),
            col("cfs.S20Reference"),
            col("cfs.S20ReferenceStatus"),
            col("cfs.HomeOfficeWaiverStatus"),
            col("prr.Description").alias("PaymentRemissionReasonDescription"),
            col("prr.DoNotUse").alias("PaymentRemissionReasonDoNotUse"),
            col("p.PortName").alias("POUPortName"),
            col("p.Address1").alias("PortAddress1"),
            col("p.Address2").alias("PortAddress2"),
            col("p.Address3").alias("PortAddress3"),
            col("p.Address4").alias("PortAddress4"),
            col("p.Address5").alias("PortAddress5"),
            col("p.Postcode").alias("PortPostcode"),
            col("p.Telephone").alias("PortTelephone"),
            col("p.Sdx").alias("PortSdx"),
            col("e.Location").alias("EmbassyLocation"),
            col("e.Embassy"),
            col("e.Surname").alias("Surname"),
            col("e.Forename").alias("Forename"),
            col("e.Title"),
            col("e.OfficialTitle"),
            col("e.Address1").alias("EmbassyAddress1"),
            col("e.Address2").alias("EmbassyAddress2"),
            col("e.Address3").alias("EmbassyAddress3"),
            col("e.Address4").alias("EmbassyAddress4"),
            col("e.Address5").alias("EmbassyAddress5"),
            col("e.Postcode").alias("EmbassyPostcode"),
            col("e.Telephone").alias("EmbassyTelephone"),
            col("e.Fax").alias("EmbassyFax"),
            col("e.Email").alias("EmbassyEmail"),
            col("e.DoNotUse").alias("DoNotUseEmbassy"),
            col("hc.Description"),
            col("hc.Prefix"),
            col("hc.CourtType"),
            col("hc.Address1").alias("HearingCentreAddress1"),
            col("hc.Address2").alias("HearingCentreAddress2"),
            col("hc.Address3").alias("HearingCentreAddress3"),
            col("hc.Address4").alias("HearingCentreAddress4"),
            col("hc.Address5").alias("HearingCentreAddress5"),
            col("hc.Postcode").alias("HearingCentrePostcode"),
            col("hc.Telephone").alias("HearingCentreTelephone"),
            col("hc.Fax").alias("HearingCentreFax"),
            col("hc.Email").alias("HearingCentreEmail"),
            col("hc.Sdx").alias("HearingCentreSdx"),
            col("hc.STLReportPath"),
            col("hc.STLHelpPath"),
            col("hc.LocalPath"),
            col("hc.GlobalPath"),
            col("hc.PouId"),
            col("hc.MainLondonCentre"),
            col("hc.DoNotUse"),
            col("hc.CentreLocation"),
            col("hc.OrganisationId"),
            col("cs.Name").alias("CaseSponsorName"),
            col("cs.Forenames").alias("CaseSponsorForenames"),
            col("cs.Title").alias("CaseSponsorTitle"),
            col("cs.Address1").alias("CaseSponsorAddress1"),
            col("cs.Address2").alias("CaseSponsorAddress2"),
            col("cs.Address3").alias("CaseSponsorAddress3"),
            col("cs.Address4").alias("CaseSponsorAddress4"),
            col("cs.Address5").alias("CaseSponsorAddress5"),
            col("cs.Postcode").alias("CaseSponsorPostcode"),
            col("cs.Telephone").alias("CaseSponsorTelephone"),
            col("cs.Email").alias("CaseSponsorEmail"),
            col("cs.Authorised"),
            col("ag.AppealTypeId"),
            #this hads been alias as there had been multiple appeal columns
            col("at.Description").alias("AppealTypeDescription"),
            col("at.Prefix").alias("AppealTypePrefix"),
            col("at.Number").alias("AppealTypeNumber"),
            col("at.FullName").alias("AppealTypeFullName"),
            col("at.Category").alias("AppealTypeCategory"),
            col("at.AppealType"),
            col("at.DoNotUse").alias("AppealTypeDoNotUse"),
            col("at.DateStart").alias("AppealTypeDateStart"),
            col("at.DateEnd").alias("AppealTypeDateEnd")
        )

# COMMAND ----------

appeal_case.createOrReplaceTempView("tv_appeal_case")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tv_appeal_case

# COMMAND ----------

# MAGIC %sql
# MAGIC     select a.* 
# MAGIC     from hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at as a 
# MAGIC  where CaseNo = 'CC/00022/2003'

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select a.* 
# MAGIC     from hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at as a 
# MAGIC     join tv_appeal_case  b 
# MAGIC     on a.CaseNo = b.CaseNo)
# MAGIC     select   CaseNo, count(*)
# MAGIC  from cte
# MAGIC  group by all
# MAGIC  having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- with cte as (
# MAGIC -- select CaseSponsorName,CaseSponsorForenames,CaseSponsorTitle,CaseSponsorAddress1,CaseSponsorAddress2,CaseSponsorAddress3,CaseSponsorAddress4,CaseSponsorAddress5,CaseSponsorPostcode,CaseSponsorTelephone,CaseSponsorEmail,PaymentRemissionReason,s17Reference,
# MAGIC -- --FeeSatisfactionId,
# MAGIC -- Forename,Surname,S20Reference,LSCReference,PaymentRemissionReasonNote,DateCorrectFeeReceived,DateCorrectFeeDeemedReceived,PaymentRemissionRequested,ASFReferenceNo,PaymentRemissionGranted,ASFReferenceNoStatus,CaseNo
# MAGIC -- from hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at
# MAGIC
# MAGIC -- )
# MAGIC -- select CaseNo, count(*) from cte
# MAGIC -- group by CaseNo
# MAGIC -- having count(*) > 1
# MAGIC
# MAGIC
# MAGIC select * from hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at
# MAGIC
# MAGIC where CaseNo = 'CC/00022/2003'
# MAGIC

# COMMAND ----------

# DBTITLE 1,-M10 values
 # CasePaymentdetails
%sql
# --Sponcer (need to handel multiple values)

with cte as (
select CaseSponsorName,CaseSponsorForenames,CaseSponsorTitle,CaseSponsorAddress1,CaseSponsorAddress2,CaseSponsorAddress3,CaseSponsorAddress4,CaseSponsorAddress5,CaseSponsorPostcode,CaseSponsorTelephone,CaseSponsorEmail,PaymentRemissionReason,s17Reference,
--FeeSatisfactionId,
Forename,Surname,S20Reference,LSCReference,PaymentRemissionReasonNote,DateCorrectFeeReceived,DateCorrectFeeDeemedReceived,PaymentRemissionRequested,ASFReferenceNo,PaymentRemissionGranted,ASFReferenceNoStatus,CaseNo
from hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at

)
select CaseNo, count(*) from cte
group by CaseNo
having count(*) > 1


-- %sql
-- select CaseSponsorName, caseNo, count(*) from hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at
--  group by CaseSponsorName, caseNo 
--  having count(*) > 1
--  order by caseNo

-- %sql
-- with cte as (
-- select LSCReference,CaseNo
-- from hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at AS table_name
-- group by LSCReference,CaseNo
-- )
-- select LSCReference, CaseNo,count(*) from cte
-- group by LSCReference, CaseNo
-- having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.caseNo, TransactionDate,TransactionTypeId,TransactionStatusId,Amount,SumTotalPay,ClearedDate,PaymentReference,AggregatedPaymentURN
# MAGIC from hive_metastore.ariadm_arm_appeals.bronze_appealcase_t_tt_ts_tm a join hive_metastore.ariadm_arm_appeals.stg_appeals_filtered b on a.CaseNo = b.CaseNo
# MAGIC -- IA/00014/2011

# COMMAND ----------

# DBTITLE 1,M12 values
# MAGIC %sql
# MAGIC -- handel multiple values(Payment details)
# MAGIC with cte as (
# MAGIC select TransactionTypeId,SumTotalFee,SumFeeAdjustment,SumPayAdjustment,SumBalance,TransactionDate,LiberataNotifiedDate,TransactionId,PaymentReference,OriginalPaymentReference,TransactionDescription,TransactionStatusId,SumTotalPay,ClearedDate,CaseNo,
# MAGIC AggregatedPaymentURN,payerSurname,ExpectedDate,clearedDate,Last4DigitsCard,payerForename,Amount,TransactionNotes
# MAGIC from hive_metastore.ariadm_arm_appeals.bronze_appealcase_t_tt_ts_tm
# MAGIC
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC
# MAGIC -- %sql
# MAGIC -- select TransactionTypeId,SumTotalFee,SumFeeAdjustment,SumPayAdjustment,SumBalance,TransactionDate,LiberataNotifiedDate,TransactionId,PaymentReference,OriginalPaymentReference,TransactionDescription,TransactionStatusId,SumTotalPay,ClearedDate,CaseNo,
# MAGIC -- AggregatedPaymentURN,payerSurname,ExpectedDate,clearedDate,Last4DigitsCard,payerForename,Amount,TransactionNotes
# MAGIC -- from hive_metastore.ariadm_arm_appeals.bronze_appealcase_t_tt_ts_tm
# MAGIC -- where CaseNo in ('AA/00032/2011')

# COMMAND ----------

# DBTITLE 1,M13 Values
# MAGIC %sql
# MAGIC -- done
# MAGIC with cte as (
# MAGIC select HumanRightDescription,CaseNo
# MAGIC from hive_metastore.ariadm_arm_appeals.bronze_appealcase_ahr_hr
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo

# COMMAND ----------

# DBTITLE 1,m14 values
# MAGIC %sql
# MAGIC -- done
# MAGIC
# MAGIC with cte as (
# MAGIC select NewMatterDescription,AppealNewMatterNotes,DateReceived,DateReferredToHO,HODecision,DateHODecision,CaseNo
# MAGIC From hive_metastore.ariadm_arm_appeals.bronze_appealcase_anm_nm
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo

# COMMAND ----------

# DBTITLE 1,m15 multiple values
# MAGIC %sql
# MAGIC -- done
# MAGIC with cte as (
# MAGIC Select DateRequested,DateRequired,DateReceived,RepresentativeDate,POUDate,NoLongerRequired,ReceivedDocumentId,CaseNo
# MAGIC from  hive_metastore.ariadm_arm_appeals.bronze_appealcase_dr_rd
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo

# COMMAND ----------

# DBTITLE 1,maintain cost award
# MAGIC %sql
# MAGIC -- MaintainCostAwards done linked is pending
# MAGIC
# MAGIC with cte as (
# MAGIC select CaseNo,AppealStage,DateOfApplication,TypeOfCostAward,ApplyingParty,PayingParty,MindedToAward,ObjectionToMindedToAward,CostsAwardDecision,DateOfDecision,CostsAmount
# MAGIC from hive_metastore.ariadm_arm_appeals.raw_costaward
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC  

# COMMAND ----------

# DBTITLE 1,M16
# MAGIC %sql
# MAGIC -- Review Directions- check if ReviewStandardDirectionId or StandardDirectionId
# MAGIC select a.CaseNo, ReviewStandardDirectionId,DateRequiredIND,DateRequiredAppellantRep,DateReceivedIND,DateReceivedAppellantRep from hive_metastore.ariadm_arm_appeals.bronze_appealcase_rsd_sd a join hive_metastore.ariadm_arm_appeals.stg_appeals_filtered b on a.CaseNo = b.CaseNo

# COMMAND ----------

# DBTITLE 1,M17
# MAGIC %sql
# MAGIC select a.CaseNo, ReviewSpecificDirectionId,DateRequiredIND,DateRequiredAppellantRep,DateReceivedIND,DateReceivedAppellantRep from hive_metastore.ariadm_arm_appeals.bronze_review_specific_direction a

# COMMAND ----------

# MAGIC %sql
# MAGIC select AppealStageWhenApplicationMade,DateOfApplication,AppealStageWhenDecisionMade,OutcomeOfAppealWhereDecisionMade,DateOfDecision,CostOrderDecision,ApplyingRepresentativeId, a.caseNo from ariadm_arm_appeals.bronze_cost_order a join ariadm_arm_appeals.stg_appeals_filtered b on a.caseNo = b.CaseNo

# COMMAND ----------

PaymentDetailstemplate = """
<!--payment copy01 start-->
                                <div id="paymentDetails">
                                    <br>
                                    <br>
                                    <table id="table13">
                                        <tbody>
                                        <tr>
                                            <th style="vertical-align: top; text-align: left; padding-left: 5px;">Payment Details</th>
                                        </tr>
                                        <tr>
                                        <!-- left side -->
                                        <td style="vertical-align: top;">
                                            <table id="table12" style="padding-left: 100px;">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="eventDate">Event Date : </label></td>
                                                    <td><input type="date" id="eventDate" name="eventDate" value="{{TransactionDate}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="eventType">Event Type : </label></td>
                                                    <td><select id="eventType" disabled style="width:150px;">
                                                        <option></option>
                                                    </select></td>
                                                </tr> 
                                                <tr>
                                                    <td id="labels"><label for="eventStatus">Event Status : </label></td>
                                                    <td><select id="eventStatus" disabled style="width:150px;">
                                                        <option></option>
                                                    </select></td>
                                                </tr> 
                                                <tr>
                                                    <td id="labels"><label for="eventNotifiedDate">Event Notified Date : </label></td>
                                                    <td><input type="date" id="eventNotifiedDate" value="{{LiberataNotifiedDate}}" readonly></td>    
                                                </tr>
                                            </tbody></table>
                                            <table id="table12" style="padding-left: 45px;">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="barclayTransId">Barclaycard Transaction ID : </label></td>
                                                    <td><input type="text" id="barclayTransId" value="{{TransactionId}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="paymentRef">Payment Reference : </label></td>
                                                    <td><input type="text" id="paymentRef" value="{{PaymentReference}}" readonly></td>
                                                </tr> 
                                                <tr>
                                                    <td id="labels"><label for="originalPayRef">Original Payment Reference : </label></td>
                                                    <td><input type="text" id="originalPayRef" value="{{OriginalPaymentReference}}" readonly></td>
                                                </tr> 
                                                <tr>
                                                    <td id="labels"><label for="aggPaymentUrn">Aggregated Payment URN : </label></td>
                                                    <td><input type="text" id="aggPaymentUrn" value="{{AggregatedPaymentURN}}" readonly></td>   
                                                </tr>
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td style="vertical-align: top;">
                                            <table id="table12" style="padding-right: 75px">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="expectedPaymentDate">Expected Payment Date : </label></td>
                                                    <td><input type="date" id="expectedPaymentDate" value="{{ExpectedDate}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="clearedDate">Cleared Date : </label></td>
                                                    <td><input type="date" id="clearedDate" value="{{clearedDate}}" readonly></td>
                                                </tr> 
                                                <tr>
                                                    <td id="labels"><label for="eventAmount">Event Amount(£) : </label></td>
                                                    <td><input type="text" id="eventAmount" value="{{Amount}}" readonly></td>
                                                </tr> 
                                                <tr>
                                                    <td id="labels"><label for="eventMethod">Event Method : </label></td>
                                                    <td><select id="eventMethod" disabled style="width:148px;">
                                                        <option></option>
                                                    </select></td>                                  
                                            </tbody></table>
                                            <table id="table12" style="padding-right: 180px">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="lastCardDigit">Last 4 Digits from Card : </label></td>
                                                    <td><input type="text" id="lastCardDigit" size="6" value="{{Last4DigitsCard}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="createUserId">Create User ID : </label></td>
                                                </tr> 
                                                <tr>
                                                    <td id="labels" style="padding-bottom: 30px;"><label for="editUserId">Last Edit User ID : </label></td>
                                                </tr>                              
                                            </tbody></table>
                                        </td>
                                        </tr>
                                        <tr>
                                            <td style="vertical-align: top;">
                                                <label for="payerSurname">Payer Surname :</label>
                                                <input type="text" id="payerSurname" style="width:300px" value="{{payerSurname}}" readonly>
                                            </td>
                                            <td style="vertical-align: top;">
                                                <label for="payerForename">Payer Forenames :</label>
                                                <input type="text" id="payerForename" style="width:300px" value="{{payerForename}}" readonly>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td colspan="2" style="vertical-align: top;">
                                                <label for="notes" style="display: block;">Notes:</label>
                                                <textarea id="notes" rows="7" style="width:850px;">{{TransactionNotes}}</textarea>
                                            </td>
                                        </tr>
                                        </tbody>
                                    </table>
                                </div>
                                <!-- payment copy-01 end-->

"""
displayHTML(PaymentDetailstemplate)

# COMMAND ----------

# DBTITLE 1,FirstTier_Hearing template
FirstTier_Hearing = """                      
                            <!-- This tab is to be used for the following status(es):
                            1. First Tier - Hearing
                            -->
                            <div id="firstTierHearing" >
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}" readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc"> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq" value ="{{InterpreterRequired}}" size="3"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judgeFT">Judge First-Tier : </label></td>
                                                    <td colspan="2"><input type="text" id="judgeFT" name="judgeFT" value ="{{AdjudicatorSurname}},{{AdjudicatorForenames}} {{AdjudicatorTitle}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateReserved">Date Reserved : </label></td>
                                                    <td colspan="2"><input type="date" id="dateReserved" name="dateReserved" value ="{{MiscDate2}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="videoLink">Video link : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="videoLink" name="videoLink"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="remittalOutcome">Remittal Outcome : </label></td>
                                                    <td colspan="2"><select id="remittalOutcome" name="remittalOutcome">
                                                        <option value="" selected="">{{RemittalOutcome}}</option>
                                                        <option value="yes">Yes</option>
                                                        <option value="no">No</option>
                                                    </select></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="UTAppellant">Upper tribunal appellant : </label></td>
                                                    <td colspan="2"><select id="UTAppellant" name="UTAppellant">
                                                        <option value="" selected="">{{UpperTribunalAppellant}}</option>
                                                        <option value="appellant">Appellant</option>
                                                        <option value="respondent">Respondent</option>
                                                    </select></td>
                                                </tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><select id="decisionToHO" name="decisionToHO">
                                                        <option value="" selected=""></option>
                                                        <option value="yes">Yes</option>
                                                        <option value="no">No</option>
                                                    </select></td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" value ="{{TypistSentDate}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="initalHearingPoints">Initial hearing points : </label></td>
                                                    <td colspan="2"><input type="text" id="initalHearingPoints" name="initalHearingPoints" value ="{{InitialHearingPoints}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="finalHearingPoints">Final Hearing Points : </label></td>
                                                    <td colspan="2"><input type="text" id="finalHearingPoints" name="finalHearingPoints" value ="{{FinalHearingPoints}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="reasonPointsChange">Reason for points change : </label></td>
                                                    <td colspan="2"><input type="text" id="reasonPointsChange" name="reasonPointsChange" value ="{{HearingPointsChangeReasonId}}"></td>
                                                </tr>
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder"></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision"></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}"></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extempore">Extempore : </label></td>
                                                                <td colspan="2"><input type="checkbox" id="extempore" name="extempore" value ="{{Extempore}}"></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsRequest">Written reasons request : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsRequest" name="writtenReasonsRequest" value ="{{WrittenReasonsRequestedDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="sentForTyping">Sent for typing : </label></td>
                                                                <td colspan="2"><input type="date" id="sentForTyping" name="sentForTyping" value ="{{TypistSentDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extemMethodOfTyping">Extem. method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="extemMethodOfTyping" name="extemMethodOfTyping" value ="{{ExtemporeMethodOfTyping}}"></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="typingReasonsReceived">Typing reasons received : </label></td>
                                                                <td colspan="2"><input type="date" id="typingReasonsReceived" name="typingReasonsReceived" value ="{{WrittenReasonsRequestedDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsSent">Written reasons sent : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsSent" name="writtenReasonsReceived" value ="{{WrittenReasonsSentDate}}" readonly></td>
                                                            </tr>                        
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(FirstTier_Hearing)

# COMMAND ----------

# DBTITLE 1,firstTierPaper template
FirstTier_Paper = """
                            <!-- This tab is to be used for the following status(es):
                                1. First Tier - Paper
                            -->
                            <div id="firstTierPaper" style="display: block;" >
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" value= "{{CaseStatus}} "name="statusOfCase"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing"  value= "{{KeyDate}}" readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc"> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq" value="{{InterpreterRequired}}" size="3"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judgeFT">Judge First-Tier : </label></td>
                                                    <td colspan="2"><input type="text" id="judgeFT" name="judgeFT" value ="{{AdjudicatorSurname}},{{AdjudicatorForenames}} {{AdjudicatorTitle}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateReserved">Date Reserved : </label></td>
                                                    <td colspan="2"><input type="date" id="dateReserved" name="dateReserved" value="{{MiscDate2}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="videoLink">Video link : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="videoLink" name="videoLink"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="remittalOutcome">Remittal Outcome : </label></td>
                                                    <td colspan="2"><select id="remittalOutcome" name="remittalOutcome">
                                                        <option value="{{RemittalOutcome}}" selected=""></option>
                                                        <option value="yes">Yes</option>
                                                        <option value="no">No</option>
                                                    </select></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="UTAppellant">Upper tribunal appellant : </label></td>
                                                    <td colspan="2"><input id="UTAppellant" name="UTAppellant" type="text" value="{{UpperTribunalAppellant}}"readonly>
                                                        </td>
                                                </tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text"readonly></td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" value="{{TypistSentDate}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="initalHearingPoints">Initial hearing points : </label></td>
                                                    <td colspan="2"><input type="text" id="initalHearingPoints" name="initalHearingPoints" value="{{InitialHearingPoints}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="finalHearingPoints">Final Hearing Points : </label></td>
                                                    <td colspan="2"><input type="text" id="finalHearingPoints" name="finalHearingPoints" value="{{FinalHearingPoints}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="reasonPointsChange">Reason for points change : </label></td>
                                                    <td colspan="2"><input type="text" id="reasonPointsChange" name="reasonPointsChange" value="{{HearingPointsChangeReasonId}}" readonly></td>
                                                </tr>
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy"  value="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value="{{Outcome}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}" readonly></td>
                                                            </tr>      
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(FirstTier_Paper)                          

# COMMAND ----------

# DBTITLE 1,First Tier Permission Application Template
FirstTierPermission_Application = """
                            <div id="ftpa" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfApplication">  Date of application : </label></td>
                                                    <td><input type="date" id="dateOfApplication" name="dateOfApplication" value="{{DateReceived}}" readonly></td>
                                                    
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyMakingApp">Party making application : </label></td>
                                                    <td colspan="2"><input type="text" id="partyMakingApp" name="partyMakingApp" value="{{Party}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="appOOT">Application out of time: </label></td>
                                                    <td colspan="2"><input type="checkbox" id="appOOT" name="appOOT" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judicialOfficer">Judicial officer : </label></td>
                                                    <td colspan="2"><input type="text" id="judicialOfficer" name="judicialOfficer" value="{{AdjudicatorId}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateToJudge">Date to judicial officer : </label></td>
                                                    <td colspan="2"><input type="date" id="dateToJudge" name="dateToJudge" value="{{MiscDate1}}"readonly></td>
                                                </tr>
                                            
                                                <tr>
                                                    <td style="padding: 10px;"></td>
                                                    <td colspan="2"></td>
                                                </tr>
                                                <tr>
                                                    <td style="padding: 10px;"></td>
                                                    <td colspan="2"></td>
                                                </tr>
                                                <tr>
                                                    <td style="padding: 10px;"></td>
                                                    <td colspan="2"></td>
                                                </tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" value ="{{DecisionSentToHO}}"type="text" readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="initalHearingPoints">Initial hearing points : </label></td>
                                                    <td colspan="2"><input type="text" id="initalHearingPoints" name="initalHearingPoints" value="{{InitialHearingPoints}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="finalHearingPoints">Final Hearing Points : </label></td>
                                                    <td colspan="2"><input type="text" id="finalHearingPoints" name="finalHearingPoints" value="{{FinalHearingPoints}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="reasonPointsChange">Reason for points change : </label></td>
                                                    <td colspan="2"><input type="text" id="reasonPointsChange" name="reasonPointsChange" value="{{HearingPointsChangeReasonId}}" readonly></td>
                                                </tr>
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value="{{DecisionByTCW}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value="{{Outcome}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value="{{UKAITNo}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extempore">Extempore : </label></td>
                                                                <td colspan="2"><input type="checkbox" id="extempore" name="extempore" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsRequest">Written reasons request : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsRequest" name="writtenReasonsRequest" value="{{WrittenReasonsRequestedDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="sentForTyping">Sent for typing : </label></td>
                                                                <td colspan="2"><input type="date" id="sentForTyping" name="sentForTyping" value ="{{TypistSentDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extemMethodOfTyping">Extem. method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="extemMethodOfTyping" name="extemMethodOfTyping"  value="{{ExtemporeMethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="typingReasonsReceived">Typing reasons received : </label></td>
                                                                <td colspan="2"><input type="date" id="typingReasonsReceived" name="typingReasonsReceived" value="{{WrittenReasonsRequestedDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsSent">Written reasons sent : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsSent" name="writtenReasonsReceived" value ="{{WrittenReasonsSentDate}}"readonly></td>
                                                            </tr>                        
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
 """
displayHTML(FirstTierPermission_Application)

# COMMAND ----------

# DBTITLE 1,Preliminary Issue template
Preliminary_Issue = """
                            <div id="preliminaryIssue" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}"readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc" readonly> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq" value ="{{InterpreterRequired}}" size="3" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="natureOfAllegation">Nature of allegation : </label></td>
                                                    <td colspan="2"><input type="text" id="natureOfAllegation" name="natureOfAllegation" value="{{Allegation}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateToJO">Date to judicial officer : </label></td>
                                                    <td colspan="2"><input type="date" id="dateToJO" name="dateToJO" value="{{MiscDate1}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="nameOfJO">Name of judicial officer : </label></td>
                                                    <td colspan="2"><input type="text" id="nameOfJO" name="nameOfJO" value="{{AdjudicatorId}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateReserved">Date Reserved : </label></td>
                                                    <td colspan="2"><input type="date" id="dateReserved" name="dateReserved" value ="{{MiscDate2}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="videoLink">Video link : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="videoLink" name="videoLink" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="decidingCentre">Deciding centre: </label></td>
                                                    <td colspan="2"><input type="text" id="decidingCentre" name="decidingCentre" value="{{DecidingCentre}}" readonly></td>
                                                </tr>

                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" value="{{DecisionSentToHODate}}"type="text"readonly></td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value="{{DecisionByTCW}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value="{{MethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value="{{Outcome}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extempore">Extempore : </label></td>
                                                                <td colspan="2"><input type="checkbox" id="extempore" name="extempore"  readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsRequest">Written reasons request : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsRequest" name="writtenReasonsRequest" value="{{WrittenReasonsRequestedDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="sentForTyping">Sent for typing : </label></td>
                                                                <td colspan="2"><input type="date" id="sentForTyping" name="sentForTyping" value ="{{TypistSentDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extemMethodOfTyping">Extem. method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="extemMethodOfTyping" name="extemMethodOfTyping" value ="{{ExtemporeMethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="typingReasonsReceived">Typing reasons received : </label></td>
                                                                <td colspan="2"><input type="date" id="typingReasonsReceived" name="typingReasonsReceived" value ="{{WrittenReasonsRequestedDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsSent">Written reasons sent : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsSent" name="writtenReasonsReceived" value ="{{WrittenReasonsSentDate}}" readonly></td>
                                                            </tr>                        
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(Preliminary_Issue)                            

# COMMAND ----------

# DBTITLE 1,Case Management Review template
CaseManagement_Review = """                          
                            <div id="cmr" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="process">Process : </label></td>
                                                    <td colspan="2"><input type="text" id="process" name="process" value="{{process}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="tier">Tier : </label></td>
                                                    <td colspan="2"><input type="text" id="tier" name="tier" value="{{Tier}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}" readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc" readonly> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq" value="{{InterpreterRequired}}" size="3" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="cmrOrders">CMR orders : </label></td>
                                                    <td colspan="2"><input type="text" id="cmrOrders" name="cmrOrders" value="{{}}" readonly></td>
                                                </tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="dateReserved">Date Reserved : </label></td>
                                                    <td colspan="2"><input type="date" id="dateReserved" name="dateReserved" value ="{{MiscDate2}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="videoLink">Video link : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="videoLink" name="videoLink" readonly></td>
                                                </tr>
                                                

                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}" readonly></td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" value="{{DecisionSentToHODate}}"readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;"></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extempore">Extempore : </label></td>
                                                                <td colspan="2"><input type="checkbox" id="extempore" name="extempore"  readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsRequest">Written reasons request : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsRequest" name="writtenReasonsRequest" value ="{{WrittenReasonsRequestedDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="sentForTyping">Sent for typing : </label></td>
                                                                <td colspan="2"><input type="date" id="sentForTyping" name="sentForTyping" value ="{{TypistSentDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extemMethodOfTyping">Extem. method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="extemMethodOfTyping" name="extemMethodOfTyping" value ="{{ExtemporeMethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="typingReasonsReceived">Typing reasons received : </label></td>
                                                                <td colspan="2"><input type="date" id="typingReasonsReceived" name="typingReasonsReceived" value ="{{WrittenReasonsRequestedDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsSent">Written reasons sent : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsSent" name="writtenReasonsReceived"  value ="{{WrittenReasonsSentDate}}"readonly></td>
                                                            </tr>                        
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(CaseManagement_Review)                            

# COMMAND ----------

# DBTITLE 1,Set Aside Application Template
SetAsideApplication = """
                            <div id="setAside" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="process">Process : </label></td>
                                                    <td colspan="2"><input type="text" id="process" name="process" value="{{process}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfApplication">Date of application : </label></td>
                                                    <td colspan="2"><input type="date" id="dateOfApplication" name="dateOfApplication"  value="{{DateReceived}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateToJO">Date to judicial officer : </label></td>
                                                    <td colspan="2"><input type="date" id="dateToJO" name="dateToJO" value="{{MiscDate1}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="nameOfJO">Judicial officer name : </label></td>
                                                    <td colspan="2"><input type="text" id="nameOfJO" name="nameOfJO" value="{{AdjudicatorId}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyMakingApp">Party making application : </label></td>
                                                    <td colspan="2"><input type="text" id="partyMakingApp" name="partyMakingApp" value="{{Party}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="appOOT">Application out of time : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="appOOT" name="appOOT" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>

                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly></td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;"></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService"  value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}" readonly></td>
                                                            </tr>
                                                                    
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(SetAsideApplication)

# COMMAND ----------

# DBTITLE 1,Closed - Fee Not Paid Template
Closed_FeeNotPaid = """
                            <div id="closedFeeNotPaid" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="noCertAwarded">No Certificate awarded - sent to filing : </label></td>
                                                    <td colspan="2"><input type="date" id="noCertAwarded" name="noCertAwarded" value="{{NoCertAwardDate}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfWriteOff">Cert revoked/appeal struck out- date of write off : </label></td>
                                                    <td colspan="2"><input type="date" id="dateOfWriteOff" name="dateOfWriteOff" value="{{CertRevokedDate}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="writtenOffSentFiling">Written off - sent for filing : </label></td>
                                                    <td colspan="2"><input type="date" id="writtenOffSentFiling" name="writtenOffSentFiling" value="{{WrittenOffFileDate}}"readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                            
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}"readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                                        
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(Closed_FeeNotPaid)

# COMMAND ----------

# DBTITLE 1,Review of Cost Order Template
ReviewofCostOrder = """
                            <div id="reviewOfCostOrder" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="process">Process : </label></td>
                                                    <td colspan="2"><input type="text" id="process" name="process" value="{{process}}"></td>
                                                </tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}" readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc" readonly> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq" size="3" value ="{{InterpreterRequired}}" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                            
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"></td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;"></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome"  value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService"  value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"   value ="{{UKAITNo}}" readonly></td>
                                                            </tr>
                                                                            
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(ReviewofCostOrder)                            

# COMMAND ----------

# DBTITLE 1,Case Closed Fee Outstanding Template
CaseClosedFeeOutstanding = """
                            <div id="caseClosedFeeOutstanding" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="referredForEnforcement">Referred for Enforcement Action : </label></td>
                                                    <td colspan="2"><input type="date" id="referredForEnforcement" name="referredForEnforcement" value="{{ReferredEnforceDate}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="letter1">Letter 1 : </label></td>
                                                    <td colspan="2"><input type="date" id="letter1" name="letter1" value="{{Letter1Date}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="letter2">Letter 2 : </label></td>
                                                    <td colspan="2"><input type="date" id="letter2" name="letter2" value="{{Letter2Date}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="letter3">Letter 3 : </label></td>
                                                    <td colspan="2"><input type="date" id="letter3" name="letter3" value="{{Letter3Date}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="refToFinance">Referred to Finance : </label></td>
                                                    <td colspan="2"><input type="date" id="refToFinance" name="refToFinance" value="{{ReferredFinanceDate}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="writtenOff">Written Off : </label></td>
                                                    <td colspan="2"><input type="date" id="writtenOff" name="writtenOff" value="{{WrittenOffDate}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="courtActionAuth">Court action authorised : </label></td>
                                                    <td colspan="2"><input type="date" id="courtActionAuth" name="courtActionAuth" value="{{CourtActionAuthDate}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="balancePaid">Balance paid : </label></td>
                                                    <td colspan="2"><input type="date" id="balancePaid" name="balancePaid" value="{{BalancePaidDate}}"readonly></td>
                                                </tr>
                                                
                                                
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;"></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}"readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService"  value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"   value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                                            
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(CaseClosedFeeOutstanding)                            

# COMMAND ----------

# DBTITLE 1,On Hold - Chargeback Taken, Upper Trib Case on Hold - Fee Not Paid
 OnHold_ChargebackTaken = """
                            <div id="onHoldChargeback" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                            
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}"readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome"  value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr><td style="padding:10px"></td></tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                                        
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(OnHold_ChargebackTaken)

# COMMAND ----------

# DBTITLE 1,Immigration Judge - Hearing, Immigration Judge - Paper
ImmigrationJudge = """   
                            <div id="immigrationJudge" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}"  readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc"> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq" value ="{{InterpreterRequired}}" size="3"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judgeFT">Judge First-Tier : </label></td>
                                                    <td colspan="2"><input type="text" id="judgeFT" name="judgeFT" value ="{{AdjudicatorSurname}},{{AdjudicatorForenames}} {{AdjudicatorTitle}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateReserved">Date Reserved : </label></td>
                                                    <td colspan="2"><input type="date" id="dateReserved" name="dateReserved" value ="{{MiscDate2}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="videoLink">Video link : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="videoLink" name="videoLink"></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly></td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Reconsideration Hearing : <input type="checkbox" id="reconsiderationHearing" name="reconsiderationHearing" readonly> Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                    
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy"  value ="{{DecisionByTCW}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome"   value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"   value ="{{UKAITNo}}"readonly></td>
                                                            </tr>      
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(ImmigrationJudge)

# COMMAND ----------

# DBTITLE 1,Panel Heaing (Legal, Panel Heaing (Legal / Non Legal)
Panel_Heaing = """
                            <div id="panelHearingLegal" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}" readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc"> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq" value ="{{InterpreterRequired}}" size="3"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judicialOfficer">Judicial Officer : </label></td>
                                                    <td colspan="2"><input type="text" id="judicialOfficer" name="judicialOfficer" value="{{AdjudicatorId}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateReserved">Date Reserved : </label></td>
                                                    <td colspan="2"><input type="date" id="dateReserved" name="dateReserved" value ="{{MiscDate2}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="videoLink">Video link : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="videoLink" name="videoLink"></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly></td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Reconsideration Hearing : <input type="checkbox" id="reconsiderationHearing" name="reconsiderationHearing" readonly> Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                    
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}" readonly></td>
                                                            </tr>      
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(Panel_Heaing)                            

# COMMAND ----------

# DBTITLE 1,Upper Tribunal Hearing
 UpperTribunalHearing = """
                            <div id="upperTribunalHearing" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}"readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc"> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq" value ="{{InterpreterRequired}}"size="3"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judgeFT">Judge First-Tier : </label></td>
                                                    <td colspan="2"><input type="text" id="judgeFT" name="judgeFT" value ="{{AdjudicatorSurname}},{{AdjudicatorForenames}} {{AdjudicatorTitle}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateReserved">Date Reserved : </label></td>
                                                    <td colspan="2"><input type="date" id="dateReserved" name="dateReserved" value ="{{MiscDate2}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="videoLink">Video link : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="videoLink" name="videoLink"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyMakingAppeal">Party making appeal : </label></td>
                                                    <td colspan="2"><input type="text" id="partyMakingAppeal" name="partyMakingAppeal" value="{{Party}}"readonly></td>
                                                </tr>
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" value="{{DecisionSentToHODate}}"readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}"readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extempore">Extempore : </label></td>
                                                                <td colspan="2"><input type="checkbox" id="extempore" name="extempore" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsRequest">Written reasons request : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsRequest" name="writtenReasonsRequest" value ="{{WrittenReasonsRequestedDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="sentForTyping">Sent for typing : </label></td>
                                                                <td colspan="2"><input type="date" id="sentForTyping" name="sentForTyping" value ="{{TypistSentDate}}"  readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extemMethodOfTyping">Extem. method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="extemMethodOfTyping" name="extemMethodOfTyping" value ="{{ExtemporeMethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="typingReasonsReceived">Typing reasons received : </label></td>
                                                                <td colspan="2"><input type="date" id="typingReasonsReceived" name="typingReasonsReceived" value ="{{WrittenReasonsRequestedDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsSent">Written reasons sent : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsSent" name="writtenReasonsReceived" value ="{{WrittenReasonsSentDate}}"readonly></td>
                                                            </tr>                            
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(UpperTribunalHearing)                            

# COMMAND ----------

# DBTITLE 1,Upper Tribunal Hearing - Continuance template
UpperTribunalHearing_Continuance = """
                            <div id="upperTribunalHearingCont" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}"readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc"> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq" value ="{{InterpreterRequired}}"size="3"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judgeFT">Judge First-Tier : </label></td>
                                                    <td colspan="2"><input type="text" id="judgeFT" name="judgeFT" value ="{{AdjudicatorSurname}},{{AdjudicatorForenames}} {{AdjudicatorTitle}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateReserved">Date Reserved : </label></td>
                                                    <td colspan="2"><input type="date" id="dateReserved" name="dateReserved" value ="{{MiscDate2}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="videoLink">Video link : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="videoLink" name="videoLink"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyMakingAppeal">Party making appeal : </label></td>
                                                    <td colspan="2"><input type="text" id="partyMakingAppeal" name="partyMakingAppeal" value="{{Party}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="utHearingDirection">Upper tribunal hearing direction : </label></td>
                                                    <td colspan="2"><input type="text" id="utHearingDirection" name="utHearingDirection" value="{{UpperTribunalHearingDirectionId}}"readonly></td>
                                                </tr>
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                                
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}" readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extempore">Extempore : </label></td>
                                                                <td colspan="2"><input type="checkbox" id="extempore" name="extempore" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsRequest">Written reasons request : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsRequest" name="writtenReasonsRequest" value ="{{WrittenReasonsRequestedDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="sentForTyping">Sent for typing : </label></td>
                                                                <td colspan="2"><input type="date" id="sentForTyping" name="sentForTyping" value ="{{TypistSentDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extemMethodOfTyping">Extem. method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="extemMethodOfTyping" name="extemMethodOfTyping" value ="{{ExtemporeMethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="typingReasonsReceived">Typing reasons received : </label></td>
                                                                <td colspan="2"><input type="date" id="typingReasonsReceived" name="typingReasonsReceived" value ="{{WrittenReasonsRequestedDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsSent">Written reasons sent : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsSent" name="writtenReasonsReceived" value ="{{WrittenReasonsSentDate}}"readonly></td>
                                                            </tr>                            
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(UpperTribunalHearing_Continuance)                            

# COMMAND ----------

# DBTITLE 1,Upper Tribunal Permission Application
UpperTribunalPermissionApplication = """  
                            <div id="utpa" style="display: block;" >
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfApplication">  Date of application : </label></td>
                                                    <td><input type="date" id="dateOfApplication" name="dateOfApplication" value="{{DateReceived}}"></td>
                                                    
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyMakingApp">Party making application : </label></td>
                                                    <td colspan="2"><input type="text" id="partyMakingApp" name="partyMakingApp" value="{{Party}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="appOOT">Application out of time: </label></td>
                                                    <td colspan="2"><input type="checkbox" id="appOOT" name="appOOT" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judicialOfficer">Judicial officer : </label></td>
                                                    <td colspan="2"><input type="text" id="judicialOfficer" name="judicialOfficer" value="{{AdjudicatorId}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateToJudge">Date to judicial officer : </label></td>
                                                    <td colspan="2"><input type="date" id="dateToJudge" name="dateToJudge" value="{{MiscDate1}}"readonly></td>
                                                </tr>
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for Personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}"readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="listReqType">List requirement type : </label></td>
                                                                <td colspan="2"><input type="text" id="listReqType" name="listReqType" value="{{ListRequirementTypeId}}"></td>
                                                            </tr>
                                                                            
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(UpperTribunalPermissionApplication)

# COMMAND ----------

# DBTITLE 1,Upper Tribunal Oral Permission Application
UpperTribunalOralPermissionApplication = """
                            <div id="utopa"  style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfApplication">  Date of application : </label></td>
                                                    <td><input type="date" id="dateOfApplication" name="dateOfApplication" value="{{DateReceived}}"readonly></td>
                                                    
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyMakingApp">Party making application : </label></td>
                                                    <td colspan="2"><input type="text" id="partyMakingApp" name="partyMakingApp" value="{{Party}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="appOOT">Application out of time: </label></td>
                                                    <td colspan="2"><input type="checkbox" id="appOOT" name="appOOT" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judicialOfficer">Judicial officer : </label></td>
                                                    <td colspan="2"><input type="text" id="judicialOfficer" name="judicialOfficer" value="{{AdjudicatorId}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateToJudge">Date to judicial officer : </label></td>
                                                    <td colspan="2"><input type="date" id="dateToJudge" name="dateToJudge" value="{{MiscDate1}}"readonly></td>
                                                </tr>
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" value="{{DecisionSentToHODate}}"readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="listReqType">List requirement type : </label></td>
                                                                <td colspan="2"><input type="text" id="listReqType" name="listReqType" value="{{ListRequirementTypeId}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extempore">Extempore : </label></td>
                                                                <td colspan="2"><input type="checkbox" id="extempore" name="extempore" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsRequest">Written reasons request : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsRequest" name="writtenReasonsRequest" value ="{{WrittenReasonsRequestedDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="sentForTyping">Sent for typing : </label></td>
                                                                <td colspan="2"><input type="date" id="sentForTyping" name="sentForTyping" value ="{{TypistSentDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extemMethodOfTyping">Extem. method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="extemMethodOfTyping" name="extemMethodOfTyping" value ="{{ExtemporeMethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="typingReasonsReceived">Typing reasons received : </label></td>
                                                                <td colspan="2"><input type="date" id="typingReasonsReceived" name="typingReasonsReceived"value ="{{WrittenReasonsRequestedDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsSent">Written reasons sent : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsSent" name="writtenReasonsReceived" value ="{{WrittenReasonsSentDate}}"readonly></td>
                                                            </tr>                            
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(UpperTribunalOralPermissionApplication)

# COMMAND ----------

# DBTITLE 1,Upper Tribunal Oral Permission Hearing
UpperTribunalOralPermissionHearing = """
                            <div id="upperTribunalOralPermissionHearing" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}" readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc"> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq"value ="{{InterpreterRequired}}"  size="3"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judgeFT">Judge First-Tier : </label></td>
                                                    <td colspan="2"><input type="text" id="judgeFT" name="judgeFT"value ="{{AdjudicatorSurname}},{{AdjudicatorForenames}} {{AdjudicatorTitle}}"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateReserved">Date Reserved : </label></td>
                                                    <td colspan="2"><input type="date" id="dateReserved" name="dateReserved" value ="{{MiscDate2}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="videoLink">Video link : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="videoLink" name="videoLink"></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyMakingApp">Party making application : </label></td>
                                                    <td colspan="2"><input type="text" id="partyMakingAppeal" name="partyMakingApp" value="{{Party}}"readonly></td>
                                                </tr>
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}"readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extempore">Extempore : </label></td>
                                                                <td colspan="2"><input type="checkbox" id="extempore" name="extempore" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsRequest">Written reasons request : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsRequest" name="writtenReasonsRequest" value ="{{WrittenReasonsRequestedDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="sentForTyping">Sent for typing : </label></td>
                                                                <td colspan="2"><input type="date" id="sentForTyping" name="sentForTyping" value ="{{TypistSentDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extemMethodOfTyping">Extem. method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="extemMethodOfTyping" name="extemMethodOfTyping" value ="{{ExtemporeMethodOfTyping}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="typingReasonsReceived">Typing reasons received : </label></td>
                                                                <td colspan="2"><input type="date" id="typingReasonsReceived" name="typingReasonsReceived" value ="{{WrittenReasonsRequestedDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsSent">Written reasons sent : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsSent" name="writtenReasonsReceived" value ="{{WrittenReasonsSentDate}}"readonly></td>
                                                            </tr>                            
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(UpperTribunalOralPermissionHearing)

# COMMAND ----------

# DBTITLE 1,Appellate Court template
AppellateCourt = """
                            <div id="appellateCourt" style="display: block;" >
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="courtSelection">Court selection : </label></td>
                                                    <td colspan="2"><input type="text" id="courtSelection" name="courtSelection" value="{{CourtSelection}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfAppeal">  Date of appeal : </label></td>
                                                    <td><input type="text" id="dateOfAppeal" name="dateOfAppeal" value="{{DateReceived}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateAppRecd">  Date Application received : </label></td>
                                                    <td><input type="text" id="dateAppRecd" name="dateAppRecd" value="{{}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyBringingAppeal">Party bringing appeal : </label></td>
                                                    <td colspan="2"><input type="text" id="partyBringingAppeal" name="partyBringingAppeal" value="{{Party}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="coaRef">COA reference number : </label></td>
                                                    <td colspan="2"><input type="text" id="coaRef" name="coaRef" value="{{COAReferenceNumber}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;" id="labels"><label for="notes">Notes :{{Notes2}}</label></td>
                                                    <td colspan="2"><textarea id="outcome" cols="30" ></textarea></td>
                                                </tr>
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                                
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}" readonly></td>
                                                            </tr>
                                                            
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                                                
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(AppellateCourt)

# COMMAND ----------

# DBTITLE 1,PTA Direct to Appellate Court
PTADirecttoAppellateCourt = """
                            <div id="ptaDirectToAppellateCourt" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="courtSelection">Court selection : </label></td>
                                                    <td colspan="2"><input type="text" id="courtSelection" name="courtSelection" value="{{CourtSelection}}" readonly></td>
                                                </tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="dateAppRecd">  Date Application received : </label></td>
                                                    <td><input type="date" id="dateAppRecd" name="dateAppRecd" value="{{}}"readonly></td>
                                                </tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="partyMakingApp">Party making application : </label></td>
                                                    <td colspan="2"><input type="text" id="partyBringingAppeal" name="partyBringingAppeal" value="{{Party}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="oot">Out of time : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="coaRef" name="coaRef" readonly></td>
                                                </tr>
                                                
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" readonly></td>
                                                </tr>
                                                
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                                                
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(PTADirecttoAppellateCourt)

# COMMAND ----------

# DBTITLE 1,Permission to Appeal
PermissiontoAppeal = """
                            <div id="permissionToAppeal" style="display: block;" >
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="courtSelection">Court selection : </label></td>
                                                    <td colspan="2"><input type="text" id="courtSelection" name="courtSelection" value="{{CourtSelection}}" readonly></td>
                                                </tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="dateAppRecd">  Date Application received : </label></td>
                                                    <td><input type="date" id="dateAppRecd" name="dateAppRecd" readonly></td>
                                                </tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="partyMakingApp">Party making application : </label></td>
                                                    <td colspan="2"><input type="text" id="partyBringingAppeal" name="partyBringingAppeal" value="{{Party}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="oot">Out of time : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="coaRef" name="coaRef"  readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateToJO">  Date to judicial officer : </label></td>
                                                    <td><input type="date" id="dateToJO" name="dateToJO" value="{{MiscDate1}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="joReceivingApp">JO receiving application : </label></td>
                                                    <td colspan="2"><input type="text" id="joReceivingApp" name="joReceivingApp" value="{{AdjudicatorId}}" readonly></td>
                                                </tr>
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}" readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" value="{{DecisionSentToHODate}}"readonly></td>
                                                </tr>
                                                
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo" value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                                                
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(PermissiontoAppeal)

# COMMAND ----------

# DBTITLE 1,High Court Review template
HighCourtReview = """
                            <div id="highCourtReview" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfApp">  Date of application : </label></td>
                                                    <td><input type="date" id="dateOfApp" name="dateOfApp" value="{{DateReceived}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="highCourtRef">High court reference : </label></td>
                                                    <td colspan="2"><input type="text" id="highCourtRef" name="highCourtRef" value="{{HighCourtReference}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="adminCourtRef">Admin court reference : </label></td>
                                                    <td colspan="2"><input type="text" id="adminCourtRef" name="adminCourtRef" value="{{AdminCourtReference}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyMakingApp">Party making application : </label></td>
                                                    <td colspan="2"><input type="text" id="partyBringingAppeal" name="partyBringingAppeal" value="{{Party}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="appOOT">Application out of time : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="appOOT" name="appOOT" readonly></td>
                                                </tr>
                                                
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                                
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}" readonly></td>
                                                            </tr>
                                                                                
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>

"""
displayHTML(HighCourtReview)

# COMMAND ----------

# DBTITLE 1,High Court Review (Filter) template
HighCourtReviewFilter = """
                            <div id="highCourtReviewFilter" style="display: block;">
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody>
                                                <tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfApp">  Date of application : </label></td>
                                                    <td><input type="date" id="dateOfApp" name="dateOfApp" value="{{DateReceived}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="highCourtRef">High court reference : </label></td>
                                                    <td colspan="2"><input type="text" id="highCourtRef" name="highCourtRef" value="{{HighCourtReference}}"readonly></td>
                                                </tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="partyMakingApp">Party making application : </label></td>
                                                    <td colspan="2"><input type="text" id="partyBringingAppeal" name="partyBringingAppeal" value="{{Party}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="appOOT">Application out of time : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="appOOT" name="appOOT" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judicialOfficer">Judicial Officer : </label></td>
                                                    <td colspan="2"><input type="text" id="judicialOfficer" name="judicialOfficer" value="{{AdjudicatorId}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateToJO">  Date to judicial officer : </label></td>
                                                    <td><input type="date" id="dateToJO" name="dateToJO" value="{{MiscDate1}}"readonly></td>
                                                </tr>
                                                
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" value="{{DecisionSentToHODate}}"readonly></td>
                                                </tr>
                                                
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                                                
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(HighCourtReviewFilter)

# COMMAND ----------

# DBTITLE 1,Judicial Review Hearing, Judicial Review Oral Permission Hearing
JudicialReviewHearing = """
                            <div id="judicialReviewHearing" style="display: block;" >
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="hearingCourt">Hearing Court : </label></td>
                                                    <td colspan="2"><input type="text" id="hearingCourt" name="hearingCourt" value="{{HearingCourt}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfHearing">  Date of hearing : </label></td>
                                                    <td><input type="date" id="dateOfHearing" name="dateOfHearing" value="{{KeyDate}}" readonly></td>
                                                    <td><input type="checkbox" id="fc" name="fc" readonly> F.C. </td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="interpreterReq">Interpreter(s) required : </label></td>
                                                    <td colspan="2"><input type="text" id="interpreterReq" name="interpreterReq" size="3" value ="{{InterpreterRequired}}" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyMakingAppeal">Party making appeal : </label></td>
                                                    <td colspan="2"><input type="text" id="partyMakingAppeal" name="partyMakingAppeal" value="{{Party}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="judgeFT">Judge First-Tier : </label></td>
                                                    <td colspan="2"><input type="text" id="judgeFT" name="judgeFT" value ="{{AdjudicatorSurname}},{{AdjudicatorForenames}} {{AdjudicatorTitle}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateReserved">Date Reserved : </label></td>
                                                    <td colspan="2"><input type="date" id="dateReserved" name="dateReserved" value ="{{MiscDate2}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="videoLink">Video link : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="videoLink" name="videoLink" readonly></td>
                                                </tr>
                                                
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" value="{{DecisionSentToHODate}}"readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}"readonly></td>
                                                                <td style="text-align: right;"><label for="tcwDecision">TCW Decision : </label><input type="checkbox" id="tcwDecision" name="tcwDecision" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="decisionBy">Decision by : </label></td>
                                                                <td colspan="2"><input type="text" id="decisionBy" name="decisionBy" value ="{{DecisionByTCW}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="methodOfTyping">Method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="methodOfTyping" name="methodOfTyping" value ="{{MethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome"  value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extempore">Extempore : </label></td>
                                                                <td colspan="2"><input type="checkbox" id="extempore" name="extempore"  readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsRequest">Written reasons request : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsRequest" name="writtenReasonsRequest" value ="{{WrittenReasonsRequestedDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="sentForTyping">Sent for typing : </label></td>
                                                                <td colspan="2"><input type="date" id="sentForTyping" name="sentForTyping" value ="{{TypistSentDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="extemMethodOfTyping">Extem. method of typing : </label></td>
                                                                <td colspan="2"><input type="text" id="extemMethodOfTyping" name="extemMethodOfTyping" value ="{{ExtemporeMethodOfTyping}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="typingReasonsReceived">Typing reasons received : </label></td>
                                                                <td colspan="2"><input type="date" id="typingReasonsReceived" name="typingReasonsReceived"  value ="{{WrittenReasonsRequestedDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="writtenReasonsSent">Written reasons sent : </label></td>
                                                                <td colspan="2"><input type="date" id="writtenReasonsSent" name="writtenReasonsReceived" value ="{{WrittenReasonsSentDate}}"readonly></td>
                                                            </tr>                            
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>
"""
displayHTML(JudicialReviewHearing)

# COMMAND ----------

# DBTITLE 1,Judicial Review Permission Application template
JudicialReviewPermissionApplication = """
                            <div id="jrpa" style="display: block;" >
                                <br>
                                <br>
                                <table id="table10">
                                    <tbody><tr>
                                        <th style="vertical-align: top; text-align: left; padding-left:5px">Status Details</th>
                                    </tr>
                                    <tr>
                                        <!-- left side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td id="labels"><label for="statusOfCase">Status of case : </label></td>
                                                    <td colspan="2"><input type="text" id="statusOfCase" name="statusOfCase" value="{{CaseStatus}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="applicationType">Application type : </label></td>
                                                    <td colspan="2"><input type="text" id="applicationType" name="applicationType" value="{{ApplicationType}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="dateOfApp">Date of application : </label></td>
                                                    <td colspan="2"><input type="date" id="dateOfApp" name="dateOfApp" value="{{DateReceived}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="partyMakingApp">Party making application : </label></td>
                                                    <td colspan="2"><input type="text" id="partyMakingAppeal" name="partyMakingApp" value="{{Party}}"readonly></td>
                                                </tr>
                                                <tr>
                                                    <td id="labels"><label for="appOOT">Application out of time : </label></td>
                                                    <td colspan="2"><input type="checkbox" id="appOOT" name="appOOT" readonly></td>
                                                </tr>
                                                
                                            
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                                <tr>
                                                    <td id="labels"><label for="decisionToHO">Decision sent to HO for personal service : </label></td>
                                                    <td><input id="decisionToHO" name="decisionToHO" type="text" value="{{DecisionSentToHODate}}"readonly>
                                                        </td>
                                                    <td>Date sent : <input type="date" id="dateSent" name="dateSent" value="{{DecisionSentToHODate}}"readonly></td>
                                                </tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                <tr><td style="padding:10px"></td></tr>
                                                
                                            </tbody></table>
                                        </td>
                                        <!-- right side -->
                                        <td>
                                            <table id="table3">
                                                <tbody><tr>
                                                    <td style="padding:10px"> </td>
                                                </tr>
                                                <tr>
                                                    <td style="text-align: right; vertical-align: top;">Cost order applied for : <input type="checkbox" id="costOrder" name="costOrder" readonly></td>
                                                </tr>
                                                <tr>
                                                    <td style="vertical-align: top;">
                                                        <table id="table4">
                                                            <tbody><tr>
                                                                <th colspan="3" style="text-align: left;">Outcome</th>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfDecision">Date of decision : </label></td>
                                                                <td><input type="date" id="dateOfDecision" name="dateOfDecision" value="{{DecisionDate}}" readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="outcome">Outcome : </label></td>
                                                                <td colspan="2"><input type="text" id="outcome" name="outcome" value ="{{Outcome}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="dateOfService">Date of Service : </label></td>
                                                                <td colspan="2"><input type="date" id="dateOfService" name="dateOfService" value ="{{Promulgated}}"readonly></td>
                                                            </tr>
                                                            <tr>
                                                                <td id="labels"><label for="UKAITNo">UKAIT number : </label></td>
                                                                <td colspan="2"><input type="text" id="UKAITNo" name="UKAITNo"  value ="{{UKAITNo}}"readonly></td>
                                                            </tr>
                                                        </tbody></table>
                                                    </td>
                                                </tr>               
                                            </tbody></table>
                                        </td>
                                    </tr>
                                </tbody></table>
                            </div>

                        </div>
                    </div>
"""
displayHTML(JudicialReviewPermissionApplication)

# COMMAND ----------

dbutils.secrets.listScopes()

