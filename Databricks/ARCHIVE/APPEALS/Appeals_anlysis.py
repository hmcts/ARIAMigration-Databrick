# Databricks notebook source
# DBTITLE 1,M1 values- AppealCaseDetail
# MAGIC %sql
# MAGIC -- done check for AdditionalGrounds multiple recods 
# MAGIC with cte as (
# MAGIC select CaseNo, hoRef,CCDAppealNum,CaseRepDXNo1,CaseRepDXNo2,AppealTypeId,DateApplicationLodged,DateOfApplicationDecision,DateLodged,AdditionalGrounds,AppealCategories,MREmbassy,NationalityId,CountryId,PortId,HumanRights,DateOfIssue,fileLocationNote,DocumentsReceived,TransferOutDate,RemovalDate,DeportationDate,ProvisionalDestructionDate,AppealReceivedBy,CRRespondent,RepresentativeRef,Language,HOInterpreter,CourtPreference,CertifiedDate,CertifiedRecordedDate,NoticeSentDate,DateReceived,ReferredToJudgeDate,RespondentName,MRPOU,RespondentName,RespondentAddress1,RespondentAddress2,RespondentAddress3,RespondentAddress4,RespondentAddress5,RespondentPostcode,RespondentTelephone,RespondentFax,RespondentEmail,CRReference,CRContact,CaseRepName,RespondentAddress1,RespondentAddress2,RespondentAddress3,RespondentAddress4,RespondentAddress5,CaseRepPostcode,CaseRepTelephone,CaseRepFax,CaseRepEmail,LSCCommission,CRReference,CRContact,RepTelephone,RespondentFax,RespondentEmail,StatutoryClosureDate,ThirdCountryId,SubmissionURN, DateReinstated,CaseOutcomeId,COAReferenceNumber,Notes2,
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
# MAGIC select CourtName,ListName,ListType,HearingTypeDesc,ListStartTime,HearingTypeEst,Outcome,AdjudicatorNote,CaseNo   
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
# MAGIC select AdjudicatorId,ApplicationType,DateReceived,CaseStatus,KeyDate,MiscDate1,InterpreterRequired,RemittalOutcome,MiscDate2,UpperTribunalAppellant,TypistSentDate,InitialHearingPoints,FinalHearingPoints,HearingPointsChangeReasonId,Tier,
# MAGIC DecisionDate,InTime,Party,Notes1,Letter1Date,DecisionByTCW,MethodOfTyping,Outcome,Promulgated,UKAITNo,WrittenReasonsRequestedDate,process,TypistSentDate,ExtemporeMethodOfTyping,WrittenReasonsSentDate,CaseNo,DecisionSentToHO,Allegation,DecidingCentre,DecisionSentToHODate,Extempore,OutOfTime,NoCertAwardDate,CertRevokedDate,WrittenOffFileDate,ReferredEnforceDate,Letter1Date,Letter2Date,Letter3Date,ReferredFinanceDate,CourtActionAuthDate,ListRequirementTypeId
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

# DBTITLE 1,m7First Tier - Hearing
# MAGIC %sql
# MAGIC with cte as (
# MAGIC select 
# MAGIC CaseStatus, KeyDate, InterpreterRequired, AdjudicatorId, MiscDate2, TypistSentDate,
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
# MAGIC WrittenReasonsSentDate,
# MAGIC AdjudicatorSurname,
# MAGIC AdjudicatorForenames,
# MAGIC AdjudicatorTitle
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

# DBTITLE 1,-M10 values
# MAGIC %sql
# MAGIC --Sponcer (need to handel multiple values)
# MAGIC
# MAGIC with cte as (
# MAGIC select CaseSponsorName,CaseSponsorForenames,CaseSponsorTitle,CaseSponsorAddress1,CaseSponsorAddress2,CaseSponsorAddress3,CaseSponsorAddress4,CaseSponsorAddress5,CaseSponsorPostcode,CaseSponsorTelephone,CaseSponsorEmail,PaymentRemissionReason,s17Reference,FeeSatisfactionId,Forename,Surname,S20Reference,LSCReference,PaymentRemissionReasonNote,DateCorrectFeeReceived,DateCorrectFeeDeemedReceived,PaymentRemissionRequested,ASFReferenceNo,PaymentRemissionGranted,ASFReferenceNoStatus,CaseNo
# MAGIC from hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at
# MAGIC )
# MAGIC select CaseNo, count(*) from cte
# MAGIC group by CaseNo
# MAGIC having count(*) > 1
# MAGIC
# MAGIC
# MAGIC -- %sql
# MAGIC -- select CaseSponsorName, caseNo, count(*) from hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at
# MAGIC --  group by CaseSponsorName, caseNo 
# MAGIC --  having count(*) > 1
# MAGIC --  order by caseNo
# MAGIC
# MAGIC -- %sql
# MAGIC -- with cte as (
# MAGIC -- select LSCReference,CaseNo
# MAGIC -- from hive_metastore.ariadm_arm_appeals.bronze_appealcase_p_e_cfs_prr_fs_cs_hc_ag_at AS table_name
# MAGIC -- group by LSCReference,CaseNo
# MAGIC -- )
# MAGIC -- select LSCReference, CaseNo,count(*) from cte
# MAGIC -- group by LSCReference, CaseNo
# MAGIC -- having count(*) > 1

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
