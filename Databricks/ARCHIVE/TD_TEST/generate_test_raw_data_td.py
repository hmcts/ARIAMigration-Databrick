import random
import re
from datetime import datetime, timedelta
import os

import pandas as pd
from faker import Faker

import csv

# Setting variables for use in subsequent cells
# raw_mnt = "/mnt/ingest01rawsboxraw/ARIADM/ARM/TD/test"
# landing_mnt = "/mnt/ingest01landingsboxlanding/test/"
# bronze_mnt = "/mnt/ingest01curatedsboxbronze/ARIADM/ARM/TD/test"
# silver_mnt = "/mnt/ingest01curatedsboxsilver/ARIADM/ARM/TD/test"
# gold_mnt = "/mnt/ingest01curatedsboxgold/ARIADM/ARM/TD/test"
raw_mnt = "/mnt/raw/"
landing_mnt = "/mnt/landing/"
bronze_mnt = "/mnt/bronze/"
silver_mnt = "/mnt/silver/"
gold_mnt = "/mnt/gold/"

# Variable to control the percentage of CaseNo present in all tables
case_no_presence_percentage = 80


def generate_appeal_case_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Appeal Case table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    case_nos = set()
    for _ in range(num_records):
        case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
        case_serial = fake.unique.random_number(digits=6)
        case_year = fake.random_int(min=1990, max=datetime.now().year)
        case_no = f"{case_prefix}{case_serial}/{case_year}"
        case_nos.add(case_no)
        
        case_type = random.randint(1, 6)
        appeal_type_id = fake.random_number(digits=3)  
        bail_type = random.randint(1, 2)
        visit_visa_type = random.randint(1, 2)
        date_logged = fake.date_between(start_date="-5y", end_date="today")
        date_received = fake.date_between(start_date="-5y", end_date="today")
        clearance_officer = fake.random_number(digits=3) 
        port_id = fake.random_number(digits=3) 
        ho_ref = fake.unique.bothify(text='??#########')
        vv_embassy_id = fake.random_number(digits=3)
        date_served = fake.date_between(start_date="-5y", end_date="today")
        centre_id = fake.random_number(digits=3)
        language_id = fake.random_number(digits=3)
        notes = fake.paragraph(nb_sentences=3)
        nationality_id = fake.random_number(digits=3)
        interpreter = fake.boolean()
        country_id = fake.random_number(digits=3)
        third_country_id = fake.random_number(digits=3)
        date_of_issue = fake.date_between(start_date="-5y", end_date="today")
        family_case = fake.boolean()
        oakington_case = fake.boolean()
        ho_interpreter = fake.name()
        additional_grounds = random.randint(0, 1)
        
        data.append([
            case_no, case_prefix, case_serial, case_year, case_type, appeal_type_id,
            bail_type, visit_visa_type, date_logged, date_received, clearance_officer,
            port_id, ho_ref, vv_embassy_id, date_served, centre_id, language_id, 
            notes, nationality_id, interpreter, country_id, third_country_id,
            date_of_issue, family_case, oakington_case, ho_interpreter,
            additional_grounds
        ])
        
    columns = [
        "CaseNo", "CasePrefix", "CaseSerial", "CaseYear", "CaseType", "AppealTypeId",
        "BailType", "VisitVisaType", "DateLogged", "DateReceived", "ClearanceOfficer",
        "PortId", "HoRef", "VVEmbassyId", "DateServed", "CentreId", "LanguageId",
        "Notes", "NationalityId", "Interpreter", "CountryId", "ThirdCountryId",
        "DateOfIssue", "FamilyCase", "OakingtonCase", "HoInterpreter", 
        "AdditionalGrounds"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, case_nos


def generate_status_data(num_records: int, case_nos: set) -> pd.DataFrame:
    """
    Generate sample data for the Status table.

    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    status_ids = set()
    for _ in range(num_records):
        status_id = fake.unique.random_number(digits=4)
        status_ids.add(status_id)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
        
        case_status = random.randint(1, 10)
        date_received = fake.date_between(start_date="-5y", end_date="today")
        allegation = fake.random_element(elements=(1, 2))
        reason = fake.random_element(elements=(0, 1))
        key_date = fake.date_between(start_date="-5y", end_date="today")
        adjudicator_id = fake.random_number(digits=3)
        misc_date1 = fake.date_between(start_date="-5y", end_date="today")
        notes1 = fake.sentence(nb_words=10)
        nature_id = fake.random_number(digits=3)
        party = fake.random_element(elements=(0, 1))
        in_time = fake.random_element(elements=(0, 1))
        further_grounds = fake.boolean()
        misc_date2 = fake.date_between(start_date="-5y", end_date="today")
        centre_id = fake.random_number(digits=3)
        misc_date3 = fake.date_between(start_date="-5y", end_date="today")
        bracket_number = fake.random_number(digits=3)
        chairman = fake.random_number(digits=3)
        lay_member1 = fake.random_number(digits=3)
        lay_member2 = fake.random_number(digits=3)
        recognizance = fake.random_number(digits=3)
        security = fake.random_number(digits=3)
        notes2 = fake.sentence(nb_words=10)
        decision_date = fake.date_between(start_date="-5y", end_date="today")
        outcome = fake.random_element(elements=(0, 1, 2, 3))
        promulgated = fake.random_element(elements=(0, 1))
        typist_id = fake.random_number(digits=3)
        reason_adjourn_id = fake.random_number(digits=3)
        residence_order = fake.random_element(elements=(0, 1))
        reporting_order = fake.random_element(elements=(0, 1))
        bailed_time_place = fake.time()
        bailed_date_hearing = fake.date_between(start_date="-5y", end_date="today")
        interpreter_required = fake.random_element(elements=(0, 1))
        decision_reserved = fake.random_element(elements=(0, 1))
        batch_number = fake.random_number(digits=3)
        admin_court_reference = fake.bothify(text='??#########')
        adjourned_list_type_id = fake.random_number(digits=3)
        certificate_of_no_merit = fake.boolean()
        bail_conditions = fake.sentence(nb_words=10)
        lives_and_sleeps_at = fake.address()
        appear_before = fake.date_between(start_date="-5y", end_date="today")
        report_to = fake.company()
        ukait_no = fake.bothify(text='??#########')
        fc = fake.boolean()
        adjournment_parent_status_id = fake.random_number(digits=3)
        process = fake.random_element(elements=('Process 1', 'Process 2', 'Process 3'))
        coa_reference_number = fake.bothify(text='??#########')
        high_court_reference = fake.bothify(text='??#########')
        appeal_route = fake.random_element(elements=(0, 1))
        application_for_costs = fake.random_element(elements=(0, 1))
        out_of_time = fake.boolean()
        listed_centre = fake.city()
        adjourned_hearing_type_id = fake.random_number(digits=3)
        adjourned_centre_id = fake.random_number(digits=3)
        cost_order = fake.random_element(elements=(0, 1))
        iris_status_of_case = fake.random_element(elements=('Status 1', 'Status 2', 'Status 3'))
        list_type_id = fake.random_number(digits=3)
        hearing_type_id = fake.random_number(digits=3)
        reconsideration_hearing = fake.random_element(elements=(0, 1))
        decision_sent_to_ho = fake.random_element(elements=(0, 1))
        decision_sent_to_ho_date = fake.date_between(start_date="-5y", end_date="today")
        determination_by = fake.random_element(elements=(0, 1))
        method_of_typing = fake.random_element(elements=(0, 1))
        court_selection = fake.random_element(elements=(0, 1))
        cost_order_applied_for = fake.random_element(elements=(0, 1))
        cost_order_applied_for_date = fake.date_between(start_date="-5y", end_date="today")
        cost_order_date = fake.date_between(start_date="-5y", end_date="today")
        additional_language_id = fake.random_number(digits=3)
        video_link = fake.random_element(elements=(0, 1))
        deciding_centre = fake.city()
        tier = fake.random_element(elements=(0, 1))
        remittal_outcome = fake.random_element(elements=(0, 1))
        upper_tribunal_appellant = fake.random_element(elements=(0, 1))
        list_requirement_type_id = fake.random_number(digits=3)
        upper_tribunal_hearing_direction_id = fake.random_number(digits=3)
        application_type = fake.random_element(elements=('Type 1', 'Type 2', 'Type 3'))
        hearing_court = fake.city()
        entered_warned_list = fake.date_between(start_date="-5y", end_date="today")
        no_cert_award_date = fake.date_between(start_date="-5y", end_date="today")
        cert_revoked_date = fake.date_between(start_date="-5y", end_date="today")
        written_off_file_date = fake.date_between(start_date="-5y", end_date="today")
        referred_enforce_date = fake.date_between(start_date="-5y", end_date="today")
        letter1_date = fake.date_between(start_date="-5y", end_date="today")
        letter2_date = fake.date_between(start_date="-5y", end_date="today")
        letter3_date = fake.date_between(start_date="-5y", end_date="today")
        referred_finance_date = fake.date_between(start_date="-5y", end_date="today")
        written_off_date = fake.date_between(start_date="-5y", end_date="today")
        court_action_auth_date = fake.date_between(start_date="-5y", end_date="today")
        balance_paid_date = fake.date_between(start_date="-5y", end_date="today")
        written_reasons_requested_date = fake.date_between(start_date="-5y", end_date="today")
        typist_sent_date = fake.date_between(start_date="-5y", end_date="today")
        typist_received_date = fake.date_between(start_date="-5y", end_date="today")
        written_reasons_sent_date = fake.date_between(start_date="-5y", end_date="today")
        extempore_method_of_typing = fake.random_element(elements=(0, 1))
        extempore = fake.random_element(elements=(0, 1))
        decision_by_tcw = fake.random_element(elements=(0, 1))
        initial_hearing_points = fake.random_number(digits=3)
        final_hearing_points = fake.random_number(digits=3)
        hearing_points_change_reason_id = fake.random_number(digits=3)
        work_and_study_restriction = fake.sentence(nb_words=10)
        tagging = fake.sentence(nb_words=10)
        other_condition = fake.sentence(nb_words=10)
        outcome_reasons = fake.sentence(nb_words=10)
        
        data.append([
            status_id, case_no, case_status, date_received, allegation, reason, key_date,
            adjudicator_id, misc_date1, notes1, nature_id, party, in_time, further_grounds,
            misc_date2, centre_id, misc_date3, bracket_number, chairman, lay_member1,
            lay_member2, recognizance, security, notes2, decision_date, outcome,
            promulgated, typist_id, reason_adjourn_id, residence_order, reporting_order,
            bailed_time_place, bailed_date_hearing, interpreter_required, decision_reserved,
            batch_number, admin_court_reference, adjourned_list_type_id, certificate_of_no_merit,
            bail_conditions, lives_and_sleeps_at, appear_before, report_to, ukait_no, fc,
            adjournment_parent_status_id, process, coa_reference_number, high_court_reference,
            appeal_route, application_for_costs, out_of_time, listed_centre, adjourned_hearing_type_id,
            adjourned_centre_id, cost_order, iris_status_of_case, list_type_id, hearing_type_id,
            reconsideration_hearing, decision_sent_to_ho, decision_sent_to_ho_date, determination_by,
            method_of_typing, court_selection, cost_order_applied_for, cost_order_applied_for_date,
            cost_order_date, additional_language_id, video_link, deciding_centre, tier,
            remittal_outcome, upper_tribunal_appellant, list_requirement_type_id,
            upper_tribunal_hearing_direction_id, application_type, hearing_court, entered_warned_list,
            no_cert_award_date, cert_revoked_date, written_off_file_date, referred_enforce_date,
            letter1_date, letter2_date, letter3_date, referred_finance_date, written_off_date,
            court_action_auth_date, balance_paid_date, written_reasons_requested_date, typist_sent_date,
            typist_received_date, written_reasons_sent_date, extempore_method_of_typing, extempore,
            decision_by_tcw, initial_hearing_points, final_hearing_points, hearing_points_change_reason_id,
            work_and_study_restriction, tagging, other_condition, outcome_reasons
        ])
        
    columns = [
        "StatusId", "CaseNo", "CaseStatus", "DateReceived", "Allegation", "Reason", "KeyDate",
        "AdjudicatorId", "MiscDate1", "Notes1", "NatureId", "Party", "InTime", "FurtherGrounds",
        "MiscDate2", "CentreId", "MiscDate3", "BracketNumber", "Chairman", "LayMember1",
        "LayMember2", "Recognizance", "Security", "Notes2", "DecisionDate", "Outcome",
        "Promulgated", "TypistId", "ReasonAdjournId", "ResidenceOrder", "ReportingOrder",
        "BailedTimePlace", "BailedDateHearing", "InterpreterRequired", "DecisionReserved",
        "BatchNumber", "AdminCourtReference", "AdjournedListTypeId", "CertificateOfNoMerit",
        "BailConditions", "LivesAndSleepsAt", "AppearBefore", "ReportTo", "UKAITNo", "FC",
        "AdjournmentParentStatusId", "Process", "COAReferenceNumber", "HighCourtReference",
        "AppealRoute", "ApplicationForCosts", "OutOfTime", "ListedCentre", "AdjournedHearingTypeId",
        "AdjournedCentreId", "CostOrder", "IRISStatusOfCase", "ListTypeId", "HearingTypeId",
        "ReconsiderationHearing", "DecisionSentToHO", "DecisionSentToHODate", "DeterminationBy",
        "MethodOfTyping", "CourtSelection", "CostOrderAppliedFor", "CostOrderAppliedForDate",
        "CostOrderDate", "AdditionalLanguageId", "VideoLink", "DecidingCentre", "Tier",
        "RemittalOutcome", "UpperTribunalAppellant", "ListRequirementTypeId",
        "UpperTribunalHearingDirectionId", "ApplicationType", "HearingCourt", "EnteredWarnedList",
        "NoCertAwardDate", "CertRevokedDate", "WrittenOffFileDate", "ReferredEnforceDate",
        "Letter1Date", "Letter2Date", "Letter3Date", "ReferredFinanceDate", "WrittenOffDate",
        "CourtActionAuthDate", "BalancePaidDate", "WrittenReasonsRequestedDate", "TypistSentDate",
        "TypistReceivedDate", "WrittenReasonsSentDate", "ExtemporeMethodOfTyping", "Extempore",
        "DecisionByTCW", "InitialHearingPoints", "FinalHearingPoints", "HearingPointsChangeReasonId",
        "WorkAndStudyRestriction", "Tagging", "OtherCondition", "OutcomeReasons"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, status_ids


def generate_file_location_data(num_records: int, case_nos: set, dept_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the File Location table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        dept_ids (set): Set of department IDs from the Department table.
        
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    for _ in range(num_records):
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
        
        if random.random() < case_no_presence_percentage / 100:  
            dept_id = random.choice(list(dept_ids))
        else:
            dept_id = fake.unique.random_number(digits=3)
        
        note = fake.sentence(nb_words=5)
        transfer_date = fake.date_between(start_date="-5y", end_date="today")
        from_dept_id = fake.random_number(digits=3)
        
        data.append([case_no, dept_id, note, transfer_date, from_dept_id])
        
    columns = ["CaseNo", "DeptId", "Note", "TransferDate", "FromDeptId"]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_case_appellant_data(num_records: int, case_nos: set, appellant_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Case Appellant table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        appellant_ids (set): Set of appellant IDs from the Appellant table.
        
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    for _ in range(num_records):
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
        
        if random.random() < case_no_presence_percentage / 100:
            appellant_id = random.choice(list(appellant_ids))
        else:
            appellant_id = fake.unique.random_number(digits=7)
        
        relationship = fake.random_element(elements=('Main', 'Partner', 'Child'))
        
        data.append([appellant_id, case_no, relationship])
        
    columns = ["AppellantId", "CaseNo", "Relationship"]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_appellant_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Appellant table.
    
    Args:
        num_records (int): The number of records to generate.
        
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    appellant_ids = set()
    for _ in range(num_records):
        appellant_id = fake.unique.random_number(digits=7)
        appellant_ids.add(appellant_id)
        
        port_reference = fake.ean(length=13)
        name = fake.last_name()
        forenames = fake.first_name()
        title = fake.prefix()
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=100)
        address1 = fake.street_address()
        address2 = fake.secondary_address()
        address3 = fake.city()
        address4 = fake.county()
        address5 = fake.country()
        country_id = fake.random_number(digits=3)
        postcode = fake.postcode()
        telephone = fake.phone_number()
        email = fake.email()
        name_sdx = fake.lexify(text="????")
        forename_sdx = fake.lexify(text="????")
        detained = random.randint(1, 4)
        date_detained = fake.date_between(start_date="-5y", end_date="today") if detained != 3 else None
        detention_centre_id = fake.random_number(digits=3) if detained != 3 else None
        fco_number = fake.bothify(text="??###-######")
        prison_ref = fake.bothify(text="?###-######")
        
        data.append([
            appellant_id, port_reference, name, forenames, title, birth_date,
            address1, address2, address3, address4, address5, country_id, postcode,
            telephone, email, name_sdx, forename_sdx, detained, date_detained,
            detention_centre_id, fco_number, prison_ref
        ])
        
    columns = [
        "AppellantId", "PortReference", "Name", "Forenames", "Title", "BirthDate",
        "Address1", "Address2", "Address3", "Address4", "Address5", "CountryId",
        "Postcode", "Telephone", "Email", "NameSdx", "ForenameSdx", "Detained",
        "DateDetained", "DetentionCentreId", "FCONumber", "PrisonRef"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, appellant_ids


def generate_department_data(num_records: int, centre_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Department table.
    
    Args:
        num_records (int): The number of records to generate.
        centre_ids (set): Set of centre IDs from the Hearing Centre table.
        
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    dept_ids = set()
    for _ in range(num_records):
        dept_id = fake.unique.random_number(digits=3)
        dept_ids.add(dept_id)
        
        if random.random() < case_no_presence_percentage / 100:
            centre_id = random.choice(list(centre_ids)) 
        else:
            centre_id = fake.unique.random_number(digits=3)
        
        description = fake.catch_phrase()
        location = fake.boolean()
        do_not_use = fake.boolean()
        
        data.append([dept_id, centre_id, description, location, do_not_use])
        
    columns = ["DeptId", "CentreId", "Description", "Location", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, dept_ids


def generate_hearing_centre_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Hearing Centre table.
    
    Args:
        num_records (int): The number of records to generate.
        
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    centre_ids = set()
    for _ in range(num_records):
        centre_id = fake.unique.random_number(digits=3)
        centre_ids.add(centre_id)
        
        description = fake.company()
        prefix = fake.lexify(text="??")
        bail_number = fake.random_number(digits=3)
        court_type = random.randint(1, 3)
        address1 = fake.street_address()
        address2 = fake.secondary_address()
        address3 = fake.city()
        address4 = fake.county()
        address5 = fake.country()
        postcode = fake.postcode()
        telephone = fake.phone_number()
        fax = fake.phone_number()
        email = fake.email()
        sdx = fake.lexify(text="????")
        local_path = fake.file_path(depth=3, extension=None)
        stl_report_path = fake.file_path(depth=3, extension=None)
        stl_help_path = fake.file_path(depth=3, extension=None)
        pou_id = fake.random_number(digits=3)
        global_path = fake.file_path(depth=3, extension=None)
        main_london_centre = fake.boolean()
        do_not_use = fake.boolean()
        centre_location = random.randint(1, 4)
        organisation_id = fake.random_number(digits=3)
        
        data.append([
            centre_id, description, prefix, bail_number, court_type, address1,
            address2, address3, address4, address5, postcode, telephone, fax,
            email, sdx, local_path, stl_report_path, stl_help_path, pou_id,
            global_path, main_london_centre, do_not_use, centre_location,
            organisation_id
        ])
        
    columns = [
        "CentreId", "Description", "Prefix", "BailNumber", "CourtType", "Address1", 
        "Address2", "Address3", "Address4", "Address5", "Postcode", "Telephone",
        "Fax", "Email", "Sdx", "LocalPath", "STLReportPath", "STLHelpPath",
        "PouId", "GlobalPath", "MainLondonCentre", "DoNotUse", "CentreLocation",
        "OrganisationId"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, centre_ids


def generate_csv_file(appeal_case_data, appellant_data, hearing_centre_data, department_data, file_location_data):
    csv_data = []
    for i in range(len(appeal_case_data)):
        row = [
            appeal_case_data.iloc[i]["CaseNo"],
            appellant_data.iloc[i]["Forenames"],
            appellant_data.iloc[i]["Name"],
            appellant_data.iloc[i]["BirthDate"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            appeal_case_data.iloc[i]["DateOfIssue"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            appeal_case_data.iloc[i]["HoRef"],
            appellant_data.iloc[i]["PortReference"],
            hearing_centre_data.iloc[i % len(hearing_centre_data)]["Description"],
            department_data.iloc[i % len(department_data)]["Description"],
            file_location_data.iloc[i % len(file_location_data)]["Note"]
        ]
        csv_data.append(row)

    column_names = [
        "AppCaseNo", "Fornames", "Name", "BirthDate", "DestructionDate",
        "HORef", "PortReference", "File_Location", "Description", "Note"
    ]

    csv_file_path = "/mnt/landing/SQL_Server/Sales/IRIS/csv/Example_IRIS_tribunal_decisions_data_file.csv"

    dbutils.fs.mkdirs("/mnt/landing/SQL_Server/Sales/IRIS/csv/")

    local_temp_file = "/tmp/temp_example.csv"
    df = pd.DataFrame(csv_data, columns=column_names)
    df.to_csv(local_temp_file, index=False)

    dbutils.fs.cp(f"file://{local_temp_file}", csv_file_path)

# Generate sample data for each table
num_appeal_case_records = 100
num_status_records = 200
num_file_location_records = 150
num_case_appellant_records = 120
num_appellant_records = 200
num_department_records = 30
num_hearing_centre_records = 20

appeal_case_data, case_nos = generate_appeal_case_data(num_appeal_case_records)
status_data, status_ids = generate_status_data(num_status_records, case_nos)
appellant_data, appellant_ids = generate_appellant_data(num_appellant_records)
hearing_centre_data, centre_ids = generate_hearing_centre_data(num_hearing_centre_records)  
department_data, dept_ids = generate_department_data(num_department_records, centre_ids)
file_location_data = generate_file_location_data(num_file_location_records, case_nos, dept_ids)
case_appellant_data = generate_case_appellant_data(num_case_appellant_records, case_nos, appellant_ids)

# Update Appeal Case table with referential integrity
appeal_case_data["CentreId"] = appeal_case_data["CentreId"].apply(lambda x: random.choice(list(centre_ids)))

# Update Status table with referential integrity
status_data["CentreId"] = status_data["CentreId"].apply(lambda x: random.choice(list(centre_ids)))

# Generate the CSV file
generate_csv_file(appeal_case_data, appellant_data, hearing_centre_data, department_data, file_location_data)

# Save the generated data as Parquet files for local test
appeal_case_data.to_parquet("AppealCase.parquet", index=False)
status_data.to_parquet("Status.parquet", index=False)
file_location_data.to_parquet("FileLocation.parquet", index=False)  
case_appellant_data.to_parquet("CaseAppellant.parquet", index=False)
appellant_data.to_parquet("Appellant.parquet", index=False)
department_data.to_parquet("Department.parquet", index=False)
hearing_centre_data.to_parquet("HearingCentre.parquet", index=False)

spark_appeal_case_data = spark.createDataFrame(appeal_case_data)
spark_status_data = spark.createDataFrame(status_data)
spark_file_location_data = spark.createDataFrame(file_location_data)
spark_case_appellant_data = spark.createDataFrame(case_appellant_data)  
spark_appellant_data = spark.createDataFrame(appellant_data)
spark_department_data = spark.createDataFrame(department_data)
spark_hearing_centre_data = spark.createDataFrame(hearing_centre_data)

# Generate timestamp for unique file naming
datesnap = spark.sql("select date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

# Appeal Case
appeal_case_temp_output_path = f"/mnt/landing/test/AppealCase/temp_{datesnap}"  
spark_appeal_case_data.coalesce(1).write.format("parquet").mode("overwrite").save(appeal_case_temp_output_path)

appeal_case_files = dbutils.fs.ls(appeal_case_temp_output_path)
appeal_case_parquet_file = [file.path for file in appeal_case_files if file.path.endswith(".parquet")][0]

appeal_case_final_output_path = f"/mnt/landing/test/AppealCase/full/SQLServer_TD_dbo_appeal_case_{datesnap}.parquet"
dbutils.fs.mv(appeal_case_parquet_file, appeal_case_final_output_path)
dbutils.fs.rm(appeal_case_temp_output_path, True)

# Status
status_temp_output_path = f"/mnt/landing/test/Status/temp_{datesnap}"  
spark_status_data.coalesce(1).write.format("parquet").mode("overwrite").save(status_temp_output_path)

status_files = dbutils.fs.ls(status_temp_output_path)
status_parquet_file = [file.path for file in status_files if file.path.endswith(".parquet")][0]

status_final_output_path = f"/mnt/landing/test/Status/full/SQLServer_TD_dbo_status_{datesnap}.parquet"
dbutils.fs.mv(status_parquet_file, status_final_output_path)
dbutils.fs.rm(status_temp_output_path, True)

# File Location  
file_location_temp_output_path = f"/mnt/landing/test/FileLocation/temp_{datesnap}"
spark_file_location_data.coalesce(1).write.format("parquet").mode("overwrite").save(file_location_temp_output_path)

file_location_files = dbutils.fs.ls(file_location_temp_output_path)  
file_location_parquet_file = [file.path for file in file_location_files if file.path.endswith(".parquet")][0]

file_location_final_output_path = f"/mnt/landing/test/FileLocation/full/SQLServer_TD_dbo_file_location_{datesnap}.parquet" 
dbutils.fs.mv(file_location_parquet_file, file_location_final_output_path)
dbutils.fs.rm(file_location_temp_output_path, True)

# Case Appellant
case_appellant_temp_output_path = f"/mnt/landing/test/CaseAppellant/temp_{datesnap}"
spark_case_appellant_data.coalesce(1).write.format("parquet").mode("overwrite").save(case_appellant_temp_output_path)

case_appellant_files = dbutils.fs.ls(case_appellant_temp_output_path)
case_appellant_parquet_file = [file.path for file in case_appellant_files if file.path.endswith(".parquet")][0]

case_appellant_final_output_path = f"/mnt/landing/test/CaseAppellant/full/SQLServer_TD_dbo_case_appellant_{datesnap}.parquet"
dbutils.fs.mv(case_appellant_parquet_file, case_appellant_final_output_path) 
dbutils.fs.rm(case_appellant_temp_output_path, True)

# Appellant
appellant_temp_output_path = f"/mnt/landing/test/Appellant/temp_{datesnap}"
spark_appellant_data.coalesce(1).write.format("parquet").mode("overwrite").save(appellant_temp_output_path)

appellant_files = dbutils.fs.ls(appellant_temp_output_path)
appellant_parquet_file = [file.path for file in appellant_files if file.path.endswith(".parquet")][0]

appellant_final_output_path = f"/mnt/landing/test/Appellant/full/SQLServer_TD_dbo_appellant_{datesnap}.parquet"
dbutils.fs.mv(appellant_parquet_file, appellant_final_output_path)
dbutils.fs.rm(appellant_temp_output_path, True)

# Department  
department_temp_output_path = f"/mnt/landing/test/Department/temp_{datesnap}"
spark_department_data.coalesce(1).write.format("parquet").mode("overwrite").save(department_temp_output_path)

department_files = dbutils.fs.ls(department_temp_output_path)
department_parquet_file = [file.path for file in department_files if file.path.endswith(".parquet")][0]  

department_final_output_path = f"/mnt/landing/test/Department/full/SQLServer_TD_dbo_department_{datesnap}.parquet"
dbutils.fs.mv(department_parquet_file, department_final_output_path)
dbutils.fs.rm(department_temp_output_path, True)

# Hearing Centre
hearing_centre_temp_output_path = f"/mnt/landing/test/HearingCentre/temp_{datesnap}"  
spark_hearing_centre_data.coalesce(1).write.format("parquet").mode("overwrite").save(hearing_centre_temp_output_path)

hearing_centre_files = dbutils.fs.ls(hearing_centre_temp_output_path)
hearing_centre_parquet_file = [file.path for file in hearing_centre_files if file.path.endswith(".parquet")][0]

hearing_centre_final_output_path = f"/mnt/landing/test/HearingCentre/full/SQLServer_TD_dbo_hearing_centre_{datesnap}.parquet"
dbutils.fs.mv(hearing_centre_parquet_file, hearing_centre_final_output_path)  
dbutils.fs.rm(hearing_centre_temp_output_path, True)
 
# Read and display schema to confirm the file output
output_paths = {
    "AppealCase": appeal_case_final_output_path,
    "Status": status_final_output_path,
    "FileLocation": file_location_final_output_path,
    "CaseAppellant": case_appellant_final_output_path,
    "Appellant": appellant_final_output_path,  
    "Department": department_final_output_path,
    "HearingCentre": hearing_centre_final_output_path
}

for output_path in output_paths.values():
    df = spark.read.format("parquet").load(output_path)
    df.printSchema()
    display(df)
