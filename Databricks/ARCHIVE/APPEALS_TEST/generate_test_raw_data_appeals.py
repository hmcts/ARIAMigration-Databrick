from typing import Tuple
import random

import re
from datetime import datetime, timedelta
import os

import pandas as pd
from faker import Faker
from pyspark.sql import functions as F

import csv

fake = Faker("en_GB")

# Setting variables for use in subsequent cells
raw_mnt = "/mnt/raw/"
landing_mnt = "/mnt/landing/"
bronze_mnt = "/mnt/bronze/"
silver_mnt = "/mnt/silver/"
gold_mnt = "/mnt/gold/"

# Variable to control the percentage of CaseNo present in all tables
case_no_presence_percentage = 90


def generate_appeal_case_data(num_records: int, language_ids: set, port_ids: set, embassy_ids: set, 
                            appeal_type_ids: set, centre_ids: set, country_ids: set, user_ids: set) -> Tuple[pd.DataFrame, set]:
    """
    Generate sample data for the Appeal Case table with proper referential integrity.
    
    Args:
        num_records (int): Number of records to generate
        language_ids (set): Valid language IDs from Language table
        port_ids (set): Valid port IDs from Port table
        embassy_ids (set): Valid embassy IDs from Embassy table
        appeal_type_ids (set): Valid appeal type IDs from AppealType table
        centre_ids (set): Valid centre IDs from HearingCentre table
        country_ids (set): Valid country IDs from Country table
        user_ids (set): Valid user IDs from Users table
    
    Returns:
        Tuple[pd.DataFrame, set]: Generated DataFrame and set of generated case numbers
    """
    fake = Faker("en_GB")
    
    data = []
    case_nos = set()
    
    # Validation of input sets
    if not all([language_ids, port_ids, embassy_ids, appeal_type_ids, centre_ids, country_ids, user_ids]):
        raise ValueError("All foreign key sets must contain valid IDs")

    for _ in range(num_records):
        # Generate unique case number
        while True:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
            if case_no not in case_nos:
                case_nos.add(case_no)
                break

        # Ensure referential integrity by using valid foreign keys
        language_id = random.choice(list(language_ids))
        port_id = random.choice(list(port_ids))
        vv_embassy_id = random.choice(list(embassy_ids))
        appeal_type_id = random.choice(list(appeal_type_ids))
        centre_id = random.choice(list(centre_ids))
        country_id = random.choice(list(country_ids))
        nationality_id = random.choice(list(country_ids))  # Using country_ids for nationality
        user_id = random.choice(list(user_ids))

        # Generate other fields
        case_type = random.randint(1, 6)
        bail_type = random.randint(1, 2) if case_type == 2 else None  # Only for bail cases
        visit_visa_type = random.randint(1, 2) if case_type == 5 else None  # Only for visit visa cases
        
        # Generate dates ensuring logical order
        date_lodged = fake.date_between(start_date="-5y", end_date="-1y")
        date_received = fake.date_between(start_date=date_lodged, end_date=date_lodged + timedelta(days=30))
        date_served = fake.date_between(start_date=date_received, end_date=date_received + timedelta(days=30))
        
        # Generate other fields with business logic
        ho_ref = fake.unique.bothify(text='??#########')
        interpreter = fake.boolean()
        third_country_id = random.choice(list(country_ids)) if random.random() < 0.3 else None
        date_of_issue = fake.date_between(start_date=date_received, end_date="today")
        
        # Generated fields with dependencies
        additional_grounds = random.randint(0, 1)
        appeal_categories = random.randint(0, 1)
        statutory_closure = fake.boolean()
        statutory_closure_date = fake.date_between(start_date="+1y", end_date="+2y") if statutory_closure else None
        
        # More date fields with logical progression
        provisional_destruction_date = fake.date_between(start_date="+1y", end_date="+5y")
        destruction_date = fake.date_between(start_date=provisional_destruction_date, end_date="+6y")
        
        data.append([
            case_no, case_prefix, case_serial, case_year, case_type, appeal_type_id, bail_type,
            visit_visa_type, date_lodged, date_received, None, port_id, ho_ref,
            vv_embassy_id, date_served, centre_id, language_id, fake.text(max_nb_chars=200),
            nationality_id, interpreter, country_id, third_country_id, date_of_issue,
            False, False, fake.name() if interpreter else None,
            additional_grounds, appeal_categories, statutory_closure, statutory_closure_date,
            fake.boolean(), fake.boolean(), random.randint(1, 2),
            provisional_destruction_date, destruction_date, statutory_closure,
            fake.boolean(), random.randint(1, 2), fake.boolean(), fake.boolean(),
            fake.boolean(), fake.date_time_this_year() if fake.boolean() else None,
            fake.date_time_this_year() if fake.boolean() else None,
            random.randint(1, 4), fake.boolean(),
            fake.date_time_this_year() if fake.boolean() else None,
            user_id, fake.date_time_this_year() if fake.boolean() else None,
            fake.date_time_this_year() if fake.boolean() else None,
            fake.boolean(), fake.bothify(text='??##-######'),
            random.randint(0, 1), fake.date_time_this_year() if fake.boolean() else None,
            fake.date_time_this_year() if fake.boolean() else None,
            fake.date_time_this_year() if fake.boolean() else None,
            fake.date_time_this_year() if fake.boolean() else None,
            fake.date_time_this_year() if fake.boolean() else None,
            fake.date_time_this_year() if fake.boolean() else None
        ])

    columns = [
        "CaseNo", "CasePrefix", "CaseSerial", "CaseYear", "CaseType", "AppealTypeId", "BailType",
        "VisitVisaType", "DateLodged", "DateReceived", "ClearanceOfficer", "PortId", "HoRef",
        "VVEmbassyId", "DateServed", "CentreId", "LanguageId", "Notes", "NationalityId",
        "Interpreter", "CountryId", "ThirdCountryId", "DateOfIssue", "FamilyCase", "OakingtonCase",
        "HoInterpreter", "AdditionalGrounds", "AppealCategories", "StatutoryClosure",
        "StatutoryClosureDate", "NonStandardSCPeriod", "PubliclyFunded", "CourtPreference",
        "ProvisionalDestructionDate", "DestructionDate", "FileInStatutoryClosure",
        "DateOfNextListedHearing", "DocumentsReceived", "OutOfTimeIssues", "ValidityIssues",
        "ReceivedFromRespondent", "IndDateAppealReceived", "RemovalDate", "AppealReceivedBy",
        "InCamera", "DateOfApplicationDecision", "UserId", "DateReinstated", "DeportationDate",
        "SecureCourtRequired", "HOANRef", "HumanRights", "TransferOutDate", "CertifiedDate",
        "CertifiedRecordedDate", "NoticeSentDate", "AddressRecordedDate", "ReferredToJudgeDate"
    ]

    df = pd.DataFrame(data, columns=columns)
    return df, case_nos


def generate_case_respondent_data(num_records: int, case_nos: set, main_respondent_ids: set, respondent_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Case Respondent table.

    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        main_respondent_ids (set): Set of main respondent IDs from the Main Respondent table.
        respondent_ids (set): Set of respondent IDs from the Respondent table.

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
        
        respondent = random.randint(1, 3)
        
        if respondent == 1 and respondent_ids:
            respondent_id = random.choice(list(respondent_ids))
        else:
            respondent_id = None
        
        if main_respondent_ids:
            main_respondent_id = random.choice(list(main_respondent_ids))
        else:
            main_respondent_id = None
        
        reference = fake.bothify(text='??#########')
        contact = fake.name()
        
        data.append([case_no, respondent, respondent_id, main_respondent_id, reference, contact])
        
    columns = ["CaseNo", "Respondent", "RespondentId", "MainRespondentId", "Reference", "Contact"]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_main_respondent_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Main Respondent table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    main_respondent_ids = set()
    for _ in range(num_records):
        main_respondent_id = fake.unique.random_number(digits=3)
        main_respondent_ids.add(main_respondent_id)
        
        name = fake.company()
        embassy = fake.boolean()
        pou = fake.boolean()
        respondent = fake.boolean()
        
        data.append([main_respondent_id, name, embassy, pou, respondent])
        
    columns = ["MainRespondentId", "Name", "Embassy", "POU", "Respondent"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, main_respondent_ids


def generate_respondent_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Respondent table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    respondent_ids = set()
    for _ in range(num_records):
        respondent_id = fake.unique.random_number(digits=3)
        respondent_ids.add(respondent_id)
        
        short_name = fake.company()
        postal_name = fake.company()
        department = fake.catch_phrase()
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
        
        data.append([
            respondent_id, short_name, postal_name, department, address1, address2, address3,
            address4, address5, postcode, telephone, fax, email, sdx
        ])
        
    columns = [
        "RespondentId", "ShortName", "PostalName", "Department", "Address1", "Address2", "Address3",
        "Address4", "Address5", "Postcode", "Telephone", "Fax", "Email", "Sdx"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, respondent_ids


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


def generate_case_representative_data(num_records: int, case_nos: set, 
                                    representative_ids: set, ensure_latest_only: bool = True) -> pd.DataFrame:
    """
    Generate sample data for the Case Representative table with proper referential integrity.
    
    Args:
        num_records (int): Number of records to generate
        case_nos (set): Valid case numbers from AppealCase table
        representative_ids (set): Valid representative IDs from Representative table
        ensure_latest_only (bool): Ensure only one active representative per case
    
    Returns:
        pd.DataFrame: Generated DataFrame
    """
    fake = Faker("en_GB")
    
    if not all([case_nos, representative_ids]):
        raise ValueError("Both case_nos and representative_ids must contain valid IDs")

    data = []
    case_rep_map = {}  # Track latest representative per case
    
    for _ in range(num_records):
        # Select valid foreign keys
        case_no = random.choice(list(case_nos))
        representative_id = random.choice(list(representative_ids))
        
        if ensure_latest_only:
            # Update or add the latest representative for this case
            case_rep_map[case_no] = {
                'representative_id': representative_id,
                'effective_date': fake.date_time_this_year(),
                'end_date': None
            }
        else:
            # Add the record directly
            data.append([
                case_no,
                representative_id,
                fake.date_time_this_year(),
                None
            ])
    
    # If ensuring latest only, process the map to generate final records
    if ensure_latest_only:
        for case_no, rep_info in case_rep_map.items():
            data.append([
                case_no,
                rep_info['representative_id'],
                rep_info['effective_date'],
                rep_info['end_date']
            ])
    
    columns = [
        "CaseNo",
        "RepresentativeId",
        "EffectiveDate",
        "EndDate"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    
    # Sort by CaseNo and EffectiveDate to maintain clear history
    df = df.sort_values(['CaseNo', 'EffectiveDate'])
    
    return df


def generate_case_surety_data(num_records: int, case_nos: set) -> pd.DataFrame:
    """
    Generate sample data for the Case Surety table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    for _ in range(num_records):
        surety_id = fake.unique.random_number(digits=4) 
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
            
        name = fake.name()
        forenames = fake.first_name()  
        title = fake.prefix()
        address1 = fake.street_address()
        address2 = fake.secondary_address()
        address3 = fake.city()
        address4 = fake.county()
        address5 = fake.country()
        postcode = fake.postcode()
        recognizance = fake.pyfloat(left_digits=4, right_digits=2, positive=True)
        security = fake.pyfloat(left_digits=4, right_digits=2, positive=True)
        date_lodged = fake.date_between(start_date="-5y", end_date="today")  
        location = fake.city()
        solicitor = random.randint(1, 2)
        telephone = fake.phone_number()
        
        data.append([
            surety_id, case_no, name, forenames, title, address1, address2, address3,
            address4, address5, postcode, recognizance, security, date_lodged, location,
            solicitor, telephone
        ])
        
    columns = [
        "SuretyId", "CaseNo", "Name", "Forenames", "Title", "Address1", "Address2",
        "Address3", "Address4", "Address5", "Postcode", "Recognizance", "Security",
        "DateLodged", "Location", "Solicitor", "Telephone" 
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_cost_award_data(num_records: int, case_nos: set) -> pd.DataFrame:
    """
    Generate sample data for the Cost Award table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    for _ in range(num_records):
        cost_award_id = fake.unique.random_number(digits=4)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
            
        date_of_application = fake.date_between(start_date="-5y", end_date="today")
        type_of_cost_award = random.randint(1, 4)  
        applying_party = random.randint(1, 3)
        paying_party = random.randint(1, 6)
        minded_to_award = fake.date_between(start_date="-5y", end_date="today")
        objection_to_minded_to_award = fake.date_between(start_date="-5y", end_date="today")
        costs_award_decision = random.randint(1, 3)
        date_of_decision = fake.date_between(start_date="-5y", end_date="today")
        costs_amount = fake.pyfloat(left_digits=4, right_digits=2, positive=True)
        outcome_of_appeal = random.randint(0, 2)
        appeal_stage = random.randint(0, 1)
        
        data.append([
            cost_award_id, case_no, date_of_application, type_of_cost_award, applying_party, paying_party,
            minded_to_award, objection_to_minded_to_award, costs_award_decision, date_of_decision,
            costs_amount, outcome_of_appeal, appeal_stage
        ])
        
    columns = [
        "CostAwardId", "CaseNo", "DateOfApplication", "TypeOfCostAward", "ApplyingParty", "PayingParty",
        "MindedToAward", "ObjectionToMindedToAward", "CostsAwardDecision", "DateOfDecision",
        "CostsAmount", "OutcomeOfAppeal", "AppealStage"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_appeal_category_data(num_records: int, case_nos: set, category_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Appeal Category table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        category_ids (set): Set of category IDs from the Category table.
    
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
            
        if random.random() < case_no_presence_percentage / 100 and category_ids:
            category_id = random.choice(list(category_ids))
        else:
            category_id = fake.unique.random_number(digits=4)
            
        data.append([case_no, category_id])
        
    columns = ["CaseNo", "CategoryId"]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_link_data(num_records: int, case_nos: set) -> pd.DataFrame:
    """
    Generate sample data for the Link table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    
    data = []
    link_nos = set()
    for _ in range(num_records):
        link_no = fake.unique.random_number(digits=4)
        link_nos.add(link_no)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
            
        data.append([link_no, case_no])
        
    columns = ["LinkNo", "CaseNo"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, link_nos


def generate_appeal_grounds_data(num_records: int, case_nos: set, appeal_type_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Appeal Grounds table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        appeal_type_ids (set): Set of appeal type IDs from the Appeal Type table.
    
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
            appeal_type_id = random.choice(list(appeal_type_ids))  
        else:
            appeal_type_id = fake.unique.random_number(digits=4)
            
        data.append([case_no, appeal_type_id])
        
    columns = ["CaseNo", "AppealTypeId"]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_status_data(num_records: int, case_nos: set, centre_ids: set, language_ids: set,
                        reason_adjourn_ids: set, case_status_ids: set, decision_type_ids: set,
                        ) -> Tuple[pd.DataFrame, set]:
    """
    Generate sample data for the Status table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        centre_ids (set): Set of centre IDs from the Hearing Centre table.
        language_ids (set): Set of language IDs from the Language table.
        reason_adjourn_ids (set): Set of reason adjourn IDs from the Reason Adjourn table.
        case_status_ids (set): Set of case status IDs from the Case Status table.
        decision_type_ids (set): Set of decision type IDs from the Decision Type table.
    
    Returns:
        Tuple[pd.DataFrame, set]: The generated sample data as a DataFrame and set of generated status IDs.
    """
    fake = Faker("en_GB")
    case_no_presence_percentage = 80  # Percentage of records that will use existing case numbers
    
    if not all([case_nos, centre_ids, language_ids, reason_adjourn_ids, case_status_ids, 
                decision_type_ids]):
        raise ValueError("All reference ID sets must contain values")

    data = []
    status_ids = set()

    for _ in range(num_records):
        status_id = fake.unique.random_number(digits=4)
        status_ids.add(status_id)
        
        # Use existing case number or generate new one
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
        
        # Reference data
        case_status_id = random.choice(list(case_status_ids))
        centre_id = random.choice(list(centre_ids))
        outcome = random.choice(list(decision_type_ids))
        reason_adjourn_id = random.choice(list(reason_adjourn_ids))
        additional_language_id = random.choice(list(language_ids))

        # Dates
        date_received = fake.date_between(start_date="-5y", end_date="today")
        key_date = fake.date_between(start_date="-5y", end_date="today")
        misc_date1 = fake.date_between(start_date="-5y", end_date="today")
        misc_date2 = fake.date_between(start_date="-5y", end_date="today")
        misc_date3 = fake.date_between(start_date="-5y", end_date="today")
        decision_date = fake.date_between(start_date="-5y", end_date="today")
        bailed_date_hearing = fake.date_between(start_date="-5y", end_date="today")
        appear_before = fake.date_between(start_date="-5y", end_date="today")
        entered_warned_list = fake.date_between(start_date="-5y", end_date="today")
        decision_sent_to_ho_date = fake.date_between(start_date="-5y", end_date="today")
        
        # Document dates
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

        # IDs and numbers
        adjudicator_id = fake.random_number(digits=3)
        nature_id = fake.random_number(digits=3)
        bracket_number = fake.random_number(digits=3)
        chairman = fake.random_number(digits=3)
        lay_member1 = fake.random_number(digits=3)
        lay_member2 = fake.random_number(digits=3)
        recognizance = fake.random_number(digits=3)
        security = fake.random_number(digits=3)
        typist_id = fake.random_number(digits=3)
        batch_number = fake.random_number(digits=3)
        adjourned_list_type_id = fake.random_number(digits=3)
        adjournment_parent_status_id = fake.random_number(digits=3)
        adjourned_hearing_type_id = fake.random_number(digits=3)
        adjourned_centre_id = fake.random_number(digits=3)
        list_type_id = fake.random_number(digits=3)
        hearing_type_id = fake.random_number(digits=3)
        list_requirement_type_id = fake.random_number(digits=3)
        upper_tribunal_hearing_direction_id = fake.random_number(digits=3)

        # Text fields
        notes1 = fake.sentence(nb_words=10)
        notes2 = fake.sentence(nb_words=10)
        admin_court_reference = fake.bothify(text='??#########')
        bail_conditions = fake.sentence(nb_words=10)
        lives_and_sleeps_at = fake.address()
        report_to = fake.company()
        ukait_no = fake.bothify(text='??#########')
        process = fake.random_element(elements=('Process 1', 'Process 2', 'Process 3'))
        coa_reference_number = fake.bothify(text='??#########')
        high_court_reference = fake.bothify(text='??#########')
        listed_centre = fake.city()
        iris_status_of_case = fake.random_element(elements=('Status 1', 'Status 2', 'Status 3'))
        deciding_centre = fake.city()
        application_type = fake.random_element(elements=('Type 1', 'Type 2', 'Type 3'))
        bailed_time_place = fake.time()

        # Boolean and small integer fields
        allegation = fake.random_element(elements=(1, 2))
        reason = fake.random_element(elements=(0, 1))
        party = fake.random_element(elements=(0, 1))
        in_time = fake.random_element(elements=(0, 1))
        further_grounds = fake.boolean()
        promulgated = fake.random_element(elements=(0, 1))
        residence_order = fake.random_element(elements=(0, 1))
        reporting_order = fake.random_element(elements=(0, 1))
        interpreter_required = fake.random_element(elements=(0, 1))
        decision_reserved = fake.random_element(elements=(0, 1))
        certificate_of_no_merit = fake.boolean()
        fc = fake.boolean()
        appeal_route = fake.random_element(elements=(0, 1))
        application_for_costs = fake.random_element(elements=(0, 1))
        out_of_time = fake.boolean()
        cost_order = fake.random_element(elements=(0, 1))
        reconsideration_hearing = fake.random_element(elements=(0, 1))
        decision_sent_to_ho = fake.random_element(elements=(0, 1))
        determination_by = fake.random_element(elements=(0, 1))
        method_of_typing = fake.random_element(elements=(0, 1))
        court_selection = fake.random_element(elements=(0, 1))
        court_order_received = fake.boolean()
        video_link = fake.random_element(elements=(0, 1))
        tier = fake.random_element(elements=(0, 1))
        remittal_outcome = fake.random_element(elements=(0, 1))
        upper_tribunal_appellant = fake.random_element(elements=(0, 1))
        hearing_court = fake.random_element(elements=(1, 2))
        extempore_method_of_typing = fake.random_element(elements=(0, 1))
        extempore = fake.random_element(elements=(0, 1))

        data.append([
            status_id, case_no, case_status_id, date_received, allegation, reason, key_date,
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
            method_of_typing, court_selection, court_order_received, additional_language_id,
            video_link, deciding_centre, tier, remittal_outcome, upper_tribunal_appellant,
            list_requirement_type_id, upper_tribunal_hearing_direction_id, application_type,
            hearing_court, entered_warned_list, no_cert_award_date, cert_revoked_date,
            written_off_file_date, referred_enforce_date, letter1_date, letter2_date, letter3_date,
            referred_finance_date, written_off_date, court_action_auth_date, balance_paid_date,
            written_reasons_requested_date, typist_sent_date, typist_received_date,
            written_reasons_sent_date, extempore_method_of_typing, extempore
        ])
    
    columns = [
        "StatusId", "CaseNo", "CaseStatusId", "DateReceived", "Allegation", "Reason", "KeyDate",
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
        "MethodOfTyping", "CourtSelection", "CourtOrderReceived", "AdditionalLanguageId",
        "VideoLink", "DecidingCentre", "Tier", "RemittalOutcome", "UpperTribunalAppellant",
        "ListRequirementTypeId", "UpperTribunalHearingDirectionId", "ApplicationType",
        "HearingCourt", "EnteredWarnedList", "NoCertAwardDate", "CertRevokedDate",
        "WrittenOffFileDate", "ReferredEnforceDate", "Letter1Date", "Letter2Date", "Letter3Date",
        "ReferredFinanceDate", "WrittenOffDate", "CourtActionAuthDate", "BalancePaidDate",
        "WrittenReasonsRequestedDate", "TypistSentDate", "TypistReceivedDate",
        "WrittenReasonsSentDate", "ExtemporeMethodOfTyping", "Extempore"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, status_ids


def generate_document_received_data(num_records: int, case_nos: set, received_document_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Document Received table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        received_document_ids (set): Set of received document IDs from the Received Document table.
    
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
            document_received_id = random.choice(list(received_document_ids))
        else:
            document_received_id = fake.unique.random_number(digits=4)
        
        date_requested = fake.date_between(start_date="-5y", end_date="today")
        date_required = fake.date_between(start_date="-5y", end_date="today")
        date_received = fake.date_between(start_date="-5y", end_date="today")
        no_longer_required = fake.boolean()
        representative_date = fake.date_between(start_date="-5y", end_date="today")
        pou_date = fake.date_between(start_date="-5y", end_date="today")
        
        data.append([
            case_no, document_received_id, date_requested, date_required, date_received,
            no_longer_required, representative_date, pou_date
        ])
        
    columns = [
        "CaseNo", "DocumentReceivedId", "DateRequested", "DateRequired", "DateReceived",
        "NoLongerRequired", "RepresentativeDate", "POUDate"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_appeal_new_matter_data(num_records: int, case_nos: set, new_matter_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Appeal New Matter table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        new_matter_ids (set): Set of new matter IDs from the New Matter table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    appeal_new_matter_ids = set()
    for _ in range(num_records):
        appeal_new_matter_id = fake.unique.random_number(digits=4)
        appeal_new_matter_ids.add(appeal_new_matter_id)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
            
        if random.random() < case_no_presence_percentage / 100:
            new_matter_id = random.choice(list(new_matter_ids))
        else:
            new_matter_id = fake.unique.random_number(digits=4)
        
        notes = fake.sentence(nb_words=10)
        date_received = fake.date_between(start_date="-5y", end_date="today")
        date_referred_to_ho = fake.date_between(start_date="-5y", end_date="today")
        ho_decision = fake.random_element(elements=(1, 2))
        date_ho_decision = fake.date_between(start_date="-5y", end_date="today")
        
        data.append([
            appeal_new_matter_id, case_no, new_matter_id, notes, date_received,
            date_referred_to_ho, ho_decision, date_ho_decision
        ])
        
    columns = [
        "AppealNewMatterId", "CaseNo", "NewMatterId", "Notes", "DateReceived",
        "DateReferredToHO", "HODecision", "DateHODecision"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, appeal_new_matter_ids


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


def generate_review_standard_direction_data(num_records: int, case_nos: set, status_ids: set, standard_direction_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Review Standard Direction table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        status_ids (set): Set of status IDs from the Status table.
        standard_direction_ids (set): Set of standard direction IDs from the Standard Direction table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    review_standard_direction_ids = set()
    for _ in range(num_records):
        review_standard_direction_id = fake.unique.random_number(digits=4)
        review_standard_direction_ids.add(review_standard_direction_id)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
            
        if random.random() < case_no_presence_percentage / 100:
            status_id = random.choice(list(status_ids))
        else:
            status_id = fake.unique.random_number(digits=4)
            
        if random.random() < case_no_presence_percentage / 100:
            standard_direction_id = random.choice(list(standard_direction_ids))
        else:
            standard_direction_id = fake.unique.random_number(digits=4)
        
        date_required_ind = fake.date_between(start_date="-5y", end_date="today")
        date_required_appellant_rep = fake.date_between(start_date="-5y", end_date="today")
        date_received_ind = fake.date_between(start_date="-5y", end_date="today")
        date_received_appellant_rep = fake.date_between(start_date="-5y", end_date="today")
        
        data.append([
            review_standard_direction_id, case_no, status_id, standard_direction_id,
            date_required_ind, date_required_appellant_rep, date_received_ind,
            date_received_appellant_rep
        ])
        
    columns = [
        "ReviewStandardDirectionId", "CaseNo", "StatusId", "StandardDirectionId",
        "DateRequiredIND", "DateRequiredAppellantRep", "DateReceivedIND",
        "DateReceivedAppellantRep"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, review_standard_direction_ids


def generate_review_specific_direction_data(num_records: int, case_nos: set, status_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Review Specific Direction table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        status_ids (set): Set of status IDs from the Status table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    review_specific_direction_ids = set()
    for _ in range(num_records):
        review_specific_direction_id = fake.unique.random_number(digits=4)
        review_specific_direction_ids.add(review_specific_direction_id)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
            
        if random.random() < case_no_presence_percentage / 100:
            status_id = random.choice(list(status_ids))
        else:
            status_id = fake.unique.random_number(digits=4)
        
        specific_direction = fake.sentence(nb_words=5)
        date_required_ind = fake.date_between(start_date="-5y", end_date="today")
        date_required_appellant_rep = fake.date_between(start_date="-5y", end_date="today")
        date_received_ind = fake.date_between(start_date="-5y", end_date="today")
        date_received_appellant_rep = fake.date_between(start_date="-5y", end_date="today")
        
        data.append([
            review_specific_direction_id, case_no, status_id, specific_direction,
            date_required_ind, date_required_appellant_rep, date_received_ind,
            date_received_appellant_rep
        ])
        
    columns = [
        "ReviewSpecificDirectionId", "CaseNo", "StatusId", "SpecificDirection",
        "DateRequiredIND", "DateRequiredAppellantRep", "DateReceivedIND",
        "DateReceivedAppellantRep"  
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, review_specific_direction_ids


def generate_language_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Language table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    language_ids = set()
    for _ in range(num_records):
        language_id = fake.unique.random_number(digits=3)
        language_ids.add(language_id)
        
        description = fake.language_name()
        do_not_use = fake.boolean()
        
        data.append([language_id, description, do_not_use])
        
    columns = ["LanguageId", "Description", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, language_ids


def generate_received_document_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Received Document table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    received_document_ids = set()
    for _ in range(num_records):
        received_document_id = fake.unique.random_number(digits=4)
        received_document_ids.add(received_document_id)
        
        description = fake.sentence(nb_words=5)
        do_not_use = fake.boolean()
        auditable = fake.boolean()
        
        data.append([received_document_id, description, do_not_use, auditable])
        
    columns = ["ReceivedDocumentId", "Description", "DoNotUse", "Auditable"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, received_document_ids


def generate_appeal_type_category_data(num_records: int, appeal_type_ids: set, category_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Appeal Type Category table.
    
    Args:
        num_records (int): The number of records to generate.
        appeal_type_ids (set): Set of appeal type IDs from the Appeal Type table.
        category_ids (set): Set of category IDs from the Category table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    appeal_type_category_ids = set()
    for _ in range(num_records):
        appeal_type_category_id = fake.unique.random_number(digits=4)
        appeal_type_category_ids.add(appeal_type_category_id)
        
        if random.random() < case_no_presence_percentage / 100:
            appeal_type_id = random.choice(list(appeal_type_ids))
        else:
            appeal_type_id = fake.unique.random_number(digits=4)
            
        if random.random() < case_no_presence_percentage / 100:
            category_id = random.choice(list(category_ids))
        else:
            category_id = fake.unique.random_number(digits=4)
        
        fee_exempt = fake.boolean()
        
        data.append([appeal_type_category_id, appeal_type_id, category_id, fee_exempt])
        
    columns = ["AppealTypeCategoryId", "AppealTypeId", "CategoryId", "FeeExempt"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, appeal_type_category_ids


def generate_link_detail_data(num_records: int, link_nos: set, reason_link_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Link Detail table.
    
    Args:
        num_records (int): The number of records to generate.
        link_nos (set): Set of link numbers from the Link table.
        reason_link_ids (set): Set of reason link IDs from the Reason Link table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    for _ in range(num_records):
        if random.random() < case_no_presence_percentage / 100:
            link_no = random.choice(list(link_nos))
        else:
            link_no = fake.unique.random_number(digits=4)
        
        comment = fake.sentence(nb_words=10)
        
        if random.random() < case_no_presence_percentage / 100:
            reason_link_id = random.choice(list(reason_link_ids))
        else:
            reason_link_id = fake.unique.random_number(digits=4)
        
        data.append([link_no, comment, reason_link_id])
        
    columns = ["LinkNo", "Comment", "ReasonLinkId"]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_user_data(num_records: int, dept_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Users table.
    
    Args:
        num_records (int): The number of records to generate.
        dept_ids (set): Set of department IDs from the Department table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    user_ids = set()
    for _ in range(num_records):
        user_id = fake.unique.random_number(digits=4)
        user_ids.add(user_id)
        
        name = fake.user_name()
        user_type = fake.random_element(elements=('A', 'B', 'C'))
        full_name = fake.name()
        suspended = fake.boolean()
        
        if random.random() < case_no_presence_percentage / 100:
            dept_id = random.choice(list(dept_ids))
        else:
            dept_id = fake.unique.random_number(digits=4)
        
        extension = fake.bothify(text='###')
        do_not_use = fake.boolean()
        
        data.append([user_id, name, user_type, full_name, suspended, dept_id, extension, do_not_use])
        
    columns = ["UserId", "Name", "UserType", "FullName", "Suspended", "DeptId", "Extension", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, user_ids


def generate_decision_type_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Decision Type table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    decision_type_ids = set()
    for _ in range(num_records):
        decision_type_id = fake.unique.random_number(digits=4)
        decision_type_ids.add(decision_type_id)
        
        description = fake.sentence(nb_words=5)
        determination_required = fake.boolean()
        state = random.randint(0, 2)
        bail_refusal = fake.boolean()
        bail_ho_consent = random.randint(0, 2)
        do_not_use = fake.boolean()
        
        data.append([decision_type_id, description, determination_required, state, bail_refusal, bail_ho_consent, do_not_use])
        
    columns = ["DecisionTypeId", "Description", "DeterminationRequired", "State", "BailRefusal", "BailHOConsent", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, decision_type_ids


def generate_transaction_data(num_records: int, case_nos: set, transaction_type_ids: set, 
                              transaction_method_ids: set, transaction_status_ids: set,
                              user_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Transaction table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        transaction_type_ids (set): Set of transaction type IDs from the Transaction Type table.
        transaction_method_ids (set): Set of transaction method IDs from the Transaction Method table.
        transaction_status_ids (set): Set of transaction status IDs from the Transaction Status table.
        user_ids (set): Set of user IDs from the Users table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    transaction_ids = set()
    for _ in range(num_records):
        transaction_id = fake.unique.random_number(digits=4)
        transaction_ids.add(transaction_id)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
        
        if random.random() < case_no_presence_percentage / 100:
            transaction_type_id = random.choice(list(transaction_type_ids))
        else:
            transaction_type_id = fake.unique.random_number(digits=4)
        
        if random.random() < case_no_presence_percentage / 100:
            transaction_method_id = random.choice(list(transaction_method_ids))
        else:
            transaction_method_id = fake.unique.random_number(digits=4)
        
        transaction_date = fake.date_between(start_date="-5y", end_date="today")
        amount = fake.pyfloat(left_digits=4, right_digits=2, positive=True)
        cleared_date = fake.date_between(start_date="-5y", end_date="today")
        
        if random.random() < case_no_presence_percentage / 100:
            status = random.choice(list(transaction_status_ids))
        else:
            status = fake.unique.random_number(digits=4)
        
        original_payment_reference = fake.bothify(text='???-########')
        payment_reference = fake.bothify(text='???-########')
        aggregated_payment_urn = fake.bothify(text='???-########')
        payer_forename = fake.first_name()
        payer_surname = fake.last_name()
        liberata_notified_date = fake.date_between(start_date="-5y", end_date="today")
        liberata_notified_aggregated_payment_date = fake.date_between(start_date="-5y", end_date="today")
        barclaycard_transaction_id = fake.uuid4()
        last_4_digits_card = fake.credit_card_number()[-4:]
        notes = fake.sentence(nb_words=10)
        expected_date = fake.date_between(start_date="-5y", end_date="today")
        referring_transaction_id = fake.random_element(elements=list(transaction_ids)) if transaction_ids else None
        
        if random.random() < case_no_presence_percentage / 100:
            create_user_id = random.choice(list(user_ids))
        else:
            create_user_id = fake.unique.random_number(digits=4)
        
        if random.random() < case_no_presence_percentage / 100:
            last_edit_user_id = random.choice(list(user_ids))
        else:
            last_edit_user_id = fake.unique.random_number(digits=4)
        
        data.append([
            transaction_id, case_no, transaction_type_id, transaction_method_id, transaction_date,
            amount, cleared_date, status, original_payment_reference, payment_reference,
            aggregated_payment_urn, payer_forename, payer_surname, liberata_notified_date,
            liberata_notified_aggregated_payment_date, barclaycard_transaction_id, last_4_digits_card,
            notes, expected_date, referring_transaction_id, create_user_id, last_edit_user_id
        ])
        
    columns = [
        "TransactionId", "CaseNo", "TransactionTypeId", "TransactionMethodId", "TransactionDate",
        "Amount", "ClearedDate", "Status", "OriginalPaymentReference", "PaymentReference",
        "AggregatedPaymentURN", "PayerForename", "PayerSurname", "LiberataNotifiedDate",
        "LiberataNotifiedAggregatedPaymentDate", "BarclayCardTransactionID", "Last4DigitsCard",
        "Notes", "ExpectedDate", "ReferringTransactionId", "CreateUserId", "LastEditUserId"  
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, transaction_ids


def generate_transaction_status_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Transaction Status table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    transaction_status_ids = set()
    for _ in range(num_records):
        transaction_status_id = fake.unique.random_number(digits=4)
        transaction_status_ids.add(transaction_status_id)
        
        description = fake.sentence(nb_words=5)
        interface_description = fake.sentence(nb_words=5)
        do_not_use = fake.boolean()
        
        data.append([transaction_status_id, description, interface_description, do_not_use])
        
    columns = ["TransactionStatusId", "Description", "InterfaceDescription", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, transaction_status_ids


def generate_cost_order_data(num_records: int, case_nos: set, decision_type_ids: set, representative_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Cost Order table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        decision_type_ids (set): Set of decision type IDs from the Decision Type table.
        representative_ids (set): Set of representative IDs from the Representative table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    cost_order_ids = set()
    for _ in range(num_records):
        cost_order_id = fake.unique.random_number(digits=4)
        cost_order_ids.add(cost_order_id)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
        
        appeal_stage_when_application_made = random.randint(0, 7)
        date_of_application = fake.date_between(start_date="-5y", end_date="today")
        appeal_stage_when_decision_made = random.randint(0, 7)
        
        if random.random() < case_no_presence_percentage / 100:
            outcome_of_appeal_where_decision_made = random.choice(list(decision_type_ids))
        else:
            outcome_of_appeal_where_decision_made =fake.unique.random_number(digits=4)
        
        date_of_decision = fake.date_between(start_date="-5y", end_date="today")
        cost_order_decision = random.randint(0, 4)
        
        if random.random() < case_no_presence_percentage / 100:
            applying_representative_id = random.choice(list(representative_ids))
        else:
            applying_representative_id = fake.unique.random_number(digits=4)
        
        applying_representative_name = fake.name()
        
        data.append([
            cost_order_id, case_no, appeal_stage_when_application_made, date_of_application,
            appeal_stage_when_decision_made, outcome_of_appeal_where_decision_made,
            date_of_decision, cost_order_decision, applying_representative_id,
            applying_representative_name
        ])
        
    columns = [
        "CostOrderId", "CaseNo", "AppealStageWhenApplicationMade", "DateOfApplication",
        "AppealStageWhenDecisionMade", "OutcomeOfAppealWhereDecisionMade",
        "DateOfDecision", "CostOrderDecision", "ApplyingRepresentativeId",
        "ApplyingRepresentativeName"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, cost_order_ids


def generate_list_type_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the List Type table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    list_type_ids = set()
    for _ in range(num_records):
        list_type_id = fake.unique.random_number(digits=4)
        list_type_ids.add(list_type_id)
        
        list_type = random.randint(1, 3)
        description = fake.sentence(nb_words=5)
        do_not_use = fake.boolean()
        
        data.append([list_type_id, list_type, description, do_not_use])
        
    columns = ["ListTypeId", "ListType", "Description", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, list_type_ids


def generate_list_sitting_data(num_records: int, list_ids: set, adjudicator_ids: set, user_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the List Sitting table.
    
    Args:
        num_records (int): The number of records to generate.
        list_ids (set): Set of list IDs from the List table.
        adjudicator_ids (set): Set of adjudicator IDs from the Adjudicator table.
        user_ids (set): Set of user IDs from the Users table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    list_sitting_ids = set()
    for _ in range(num_records):
        list_sitting_id = fake.unique.random_number(digits=4)
        list_sitting_ids.add(list_sitting_id)
        
        if random.random() < case_no_presence_percentage / 100:
            list_id = random.choice(list(list_ids))
        else:
            list_id = fake.unique.random_number(digits=4)
        
        position = random.randint(1, 6)
        
        if random.random() < case_no_presence_percentage / 100:
            adjudicator_id = random.choice(list(adjudicator_ids))
        else:
            adjudicator_id = fake.unique.random_number(digits=4)
        
        date_booked = fake.date_between(start_date="-5y", end_date="today")
        letter_date = fake.date_between(start_date="-5y", end_date="today")
        cancelled = fake.boolean()
        
        if random.random() < case_no_presence_percentage / 100:
            user_id = random.choice(list(user_ids))
        else:
            user_id = fake.unique.random_number(digits=4)
        
        chairman = fake.boolean()
        
        data.append([
            list_sitting_id, list_id, position, adjudicator_id, date_booked,
            letter_date, cancelled, user_id, chairman
        ])
        
    columns = [
        "ListSittingId", "ListId", "Position", "AdjudicatorId", "DateBooked",
        "LetterDate", "Cancelled", "UserId", "Chairman"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, list_sitting_ids


def generate_court_data(num_records: int, centre_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Court table.
    
    Args:
        num_records (int): The number of records to generate.
        centre_ids (set): Set of centre IDs from the Hearing Centre table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    court_ids = set()
    for _ in range(num_records):
        court_id = fake.unique.random_number(digits=4)
        court_ids.add(court_id)
        
        if random.random() < case_no_presence_percentage / 100:
            centre_id = random.choice(list(centre_ids))
        else:
            centre_id = fake.unique.random_number(digits=4)
        
        court_name = fake.lexify(text='Court ???')
        do_not_use = fake.boolean()
        
        data.append([court_id, centre_id, court_name, do_not_use])
        
    columns = ["CourtId", "CentreId", "CourtName", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, court_ids


def generate_list_data(num_records: int, centre_ids: set, court_ids: set, list_type_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the List table.
    
    Args:
        num_records (int): The number of records to generate.
        centre_ids (set): Set of centre IDs from the Hearing Centre table.
        court_ids (set): Set of court IDs from the Court table.
        list_type_ids (set): Set of list type IDs from the List Type table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    list_ids = set()
    for _ in range(num_records):
        list_id = fake.unique.random_number(digits=4)
        list_ids.add(list_id)
        
        if random.random() < case_no_presence_percentage / 100:
            centre_id = random.choice(list(centre_ids))
        else:
            centre_id = fake.unique.random_number(digits=4)
        
        if random.random() < case_no_presence_percentage / 100:
            court_id = random.choice(list(court_ids))
        else:
            court_id = fake.unique.random_number(digits=4)
        
        list_name = fake.lexify(text='List ???')
        start_date = fake.date_between(start_date="-5y", end_date="today")
        end_date = fake.date_between(start_date=start_date, end_date="+5y")
        
        if random.random() < case_no_presence_percentage / 100:
            list_type_id = random.choice(list(list_type_ids))
        else:
            list_type_id = fake.unique.random_number(digits=4)
        
        max_mins = fake.random_number(digits=3)
        start_time = fake.time()
        end_time = fake.time()
        lunchtime = fake.random_number(digits=2)
        closed = fake.boolean()
        am_pm = random.randint(1, 3)
        num_req_chair = fake.random_number(digits=1)
        num_req_adj = fake.random_number(digits=1)
        num_req_qual_mem = fake.random_number(digits=1)
        num_req_lay_mem = fake.random_number(digits=1)
        num_req_senior_immigration_judge = fake.random_number(digits=1)
        num_req_designated_immigration_judge = fake.random_number(digits=1)
        num_req_immigration_judge = fake.random_number(digits=1)
        num_req_non_legal_member = fake.random_number(digits=1)
        
        data.append([
            list_id, centre_id, court_id, list_name, start_date, end_date, list_type_id,
            max_mins, start_time, end_time, lunchtime, closed, am_pm, num_req_chair,
            num_req_adj, num_req_qual_mem, num_req_lay_mem, num_req_senior_immigration_judge,
            num_req_designated_immigration_judge, num_req_immigration_judge, num_req_non_legal_member
        ])
        
    columns = [
        "ListId", "CentreId", "CourtId", "ListName", "StartDate", "EndDate", "ListTypeId",
        "MaxMins", "StartTime", "EndTime", "Lunchtime", "Closed", "AMPM", "NumReqChair",
        "NumReqAdj", "NumReqQualMem", "NumReqLayMem", "NumReqSeniorImmigrationJudge",
        "NumReqDesignatedImmigrationJudge", "NumReqImmigrationJudge", "NumReqNonLegalMember"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, list_ids


def generate_new_matter_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the New Matter table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    new_matter_ids = set()
    for _ in range(num_records):
        new_matter_id = fake.unique.random_number(digits=4)
        new_matter_ids.add(new_matter_id)
        
        description = fake.sentence(nb_words=10)
        notes_required = fake.boolean()
        do_not_use = fake.boolean()
        
        data.append([new_matter_id, description, notes_required, do_not_use])
        
    columns = ["NewMatterId", "Description", "NotesRequired", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, new_matter_ids


def generate_documents_received_data(num_records: int, case_nos: set, received_document_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Documents Received table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        received_document_ids (set): Set of received document IDs from the Received Document table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    # Ensure we don't exceed possible unique combinations
    max_possible_records = len(case_nos) * len(received_document_ids)
    num_records = min(num_records, max_possible_records)
    
    data = []
    used_combinations = set()
    
    while len(data) < num_records:
        # Only use existing case numbers and document IDs
        case_no = random.choice(list(case_nos))
        document_received_id = random.choice(list(received_document_ids))
        
        # Ensure unique combination
        combination = (case_no, document_received_id)
        if combination in used_combinations:
            continue
        used_combinations.add(combination)
        
        # Generate dates in chronological order
        date_requested = fake.date_between(start_date="-2y", end_date="today")
        date_required = fake.date_between(start_date=date_requested, end_date="+6m")
        
        # 80% chance of document being received
        if random.random() < 0.8:
            date_received = fake.date_between(start_date=date_requested, end_date=date_required)
            no_longer_required = random.choice([True, False])
            
            # Only set representative and POU dates if document was received
            representative_date = fake.date_between(start_date=date_received, end_date="+1m") if random.random() < 0.7 else None
            pou_date = fake.date_between(start_date=date_received, end_date="+1m") if random.random() < 0.7 else None
        else:
            date_received = None
            no_longer_required = False
            representative_date = None
            pou_date = None
        
        data.append([
            case_no, 
            document_received_id, 
            date_requested, 
            date_required, 
            date_received, 
            no_longer_required, 
            representative_date, 
            pou_date
        ])
        
    columns = [
        "CaseNo", 
        "DocumentReceivedId", 
        "DateRequested", 
        "DateRequired", 
        "DateReceived", 
        "NoLongerRequired", 
        "RepresentativeDate", 
        "POUDate"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_representative_data(num_records: int, user_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Representative table.
    
    Args:
        num_records (int): The number of records to generate.
        user_ids (set): Set of user IDs from the Users table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    representative_ids = set()
    for _ in range(num_records):
        representative_id = fake.unique.random_number(digits=4)
        representative_ids.add(representative_id)
        
        name = fake.name()
        title = fake.prefix()
        forenames = fake.first_name()
        address1 = fake.street_address()
        address2 = fake.secondary_address()
        address3 = fake.city()
        address4 = fake.county()
        address5 = fake.country()
        postcode = fake.postcode()
        telephone = fake.phone_number()
        fax = fake.phone_number()
        email = fake.email()
        prevent_certificate = fake.boolean()
        
        if random.random() < case_no_presence_percentage / 100:
            user_id = random.choice(list(user_ids))
        else:
            user_id = fake.unique.random_number(digits=4)
        
        ban_date = fake.date_between(start_date="-5y", end_date="today")
        do_not_use = fake.boolean()
        sdx = fake.lexify(text="????")
        dx_no1 = fake.bothify(text="??-###")
        dx_no2 = fake.bothify(text="??-###")
        
        data.append([
            representative_id, name, title, forenames, address1, address2, address3, 
            address4, address5, postcode, telephone, fax, email, prevent_certificate, 
            user_id, ban_date, do_not_use, sdx, dx_no1, dx_no2
        ])
        
    columns = [
        "RepresentativeId", "Name", "Title", "Forenames", "Address1", "Address2", 
        "Address3", "Address4", "Address5", "Postcode", "Telephone", "Fax", 
        "Email", "PreventCertificate", "UserId", "BanDate", "DoNotUse", "Sdx", 
        "DXNo1", "DXNo2"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, representative_ids


def generate_case_fee_summary_data(num_records: int, case_nos: set, payment_remission_reason_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Case Fee Summary table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        payment_remission_reason_ids (set): Set of payment remission reason IDs from the Payment Remission Reason table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    case_fee_summary_ids = set()
    for _ in range(num_records):
        case_fee_summary_id = fake.unique.random_number(digits=4)
        case_fee_summary_ids.add(case_fee_summary_id)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
            
        date_correct_fee_received = fake.date_between(start_date="-5y", end_date="today")
        date_correct_fee_deemed_received = fake.date_between(start_date="-5y", end_date="today")
        payment_remission_requested = fake.random_element(elements=(1, 2))
        payment_remission_granted = fake.random_element(elements=(1, 2))
        
        if random.random() < case_no_presence_percentage / 100:
            payment_remission_reason = random.choice(list(payment_remission_reason_ids))
        else:
            payment_remission_reason = fake.unique.random_number(digits=4)
        
        payment_remission_reason_note = fake.sentence(nb_words=10)
        asf_reference_no = fake.bothify(text="ASF-????-####")
        asf_reference_no_status = fake.random_element(elements=(1, 2, 3))
        lsc_reference = fake.bothify(text="LSC-????-####")
        lsc_status = fake.random_element(elements=(1, 2, 3))
        lcp_requested = fake.random_element(elements=(1, 2))
        lcp_outcome = fake.random_element(elements=(1, 2, 3, 4, 5))
        s17_reference = fake.bothify(text="S17-????-####")
        s17_reference_status = fake.random_element(elements=(1, 2, 3))
        
        data.append([
            case_fee_summary_id, case_no, date_correct_fee_received, date_correct_fee_deemed_received,
            payment_remission_requested, payment_remission_granted, payment_remission_reason,
            payment_remission_reason_note, asf_reference_no, asf_reference_no_status, lsc_reference,
            lsc_status, lcp_requested, lcp_outcome, s17_reference, s17_reference_status
        ])
        
    columns = [
        "CaseFeeSummaryId", "CaseNo", "DateCorrectFeeReceived", "DateCorrectFeeDeemedReceived",
        "PaymentRemissionRequested", "PaymentRemissionGranted", "PaymentRemissionReason",
        "PaymentRemissionReasonNote", "ASFReferenceNo", "ASFReferenceNoStatus", "LSCReference",
        "LSCStatus", "LCPRequested", "LCPOutcome", "S17Reference", "S17ReferenceStatus"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, case_fee_summary_ids


def generate_human_right_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Human Right table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    human_right_ids = set()
    for _ in range(num_records):
        human_right_id = fake.unique.random_number(digits=4)
        human_right_ids.add(human_right_id)
        
        description = fake.sentence(nb_words=10)
        do_not_show = fake.boolean()
        priority = fake.random_int(min=1, max=10)
        
        data.append([human_right_id, description, do_not_show, priority])
        
    columns = ["HumanRightId", "Description", "DoNotShow", "Priority"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, human_right_ids


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
            appellant_id = fake.unique.random_number(digits=4)
        
        relationship = fake.random_element(elements=("Spouse", "Child", "Parent", "Sibling", None))
        
        data.append([appellant_id, case_no, relationship])
        
    columns = ["AppellantId", "CaseNo", "Relationship"]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_bf_diary_data(num_records: int, case_nos: set, bf_type_ids: set, user_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the BF Diary table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        bf_type_ids (set): Set of BF type IDs from the BF Type table.
        user_ids (set): Set of user IDs from the Users table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    bf_ids = set()
    for _ in range(num_records):
        bf_id = fake.unique.random_number(digits=4)
        bf_ids.add(bf_id)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
            
        bf_date = fake.date_between(start_date="-5y", end_date="today")
        
        if random.random() < case_no_presence_percentage / 100:
            bf_type_id = random.choice(list(bf_type_ids))
        else:
            bf_type_id = fake.unique.random_number(digits=4)
        
        entry = fake.sentence(nb_words=10)
        entry_date = fake.date_between(start_date="-5y", end_date="today")
        date_completed = fake.date_between(start_date="-5y", end_date="today") if fake.boolean() else None
        
        if random.random() < case_no_presence_percentage / 100:
            completed_by = random.choice(list(user_ids))
        else:
            completed_by = fake.unique.random_number(digits=4)
        
        reason = fake.sentence(nb_words=10) if fake.boolean() else None
        
        data.append([
            bf_id, case_no, bf_date, bf_type_id, entry, entry_date, date_completed,
            completed_by, reason
        ])
        
    columns = [
        "BfId", "CaseNo", "BFDate", "BFTypeId", "Entry", "Entrydate", "DateCompleted",
        "CompletedBy", "Reason"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, bf_ids


def generate_appeal_human_right_data(num_records: int, case_nos: set, human_right_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Appeal Human Right table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        human_right_ids (set): Set of human right IDs from the Human Right table.
    
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
            human_right_id = random.choice(list(human_right_ids))
        else:
            human_right_id = fake.unique.random_number(digits=4)
        
        data.append([case_no, human_right_id])
        
    columns = ["CaseNo", "HumanRightId"]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_transaction_method_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Transaction Method table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    transaction_method_ids = set()
    for _ in range(num_records):
        transaction_method_id = fake.unique.random_number(digits=4)
        transaction_method_ids.add(transaction_method_id)
        
        description = fake.sentence(nb_words=3)
        interface_description = fake.sentence(nb_words=3)
        do_not_use = fake.boolean()
        
        data.append([transaction_method_id, description, interface_description, do_not_use])
        
    columns = ["TransactionMethodId", "Description", "InterfaceDescription", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, transaction_method_ids


def generate_transaction_type_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Transaction Type table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    transaction_type_ids = set()
    for _ in range(num_records):
        transaction_type_id = fake.unique.random_number(digits=4)
        transaction_type_ids.add(transaction_type_id)
        
        description = fake.sentence(nb_words=3)
        interface_description = fake.sentence(nb_words=3)
        allow_in_new = fake.boolean()
        do_not_use = fake.boolean()
        sum_fee_adjustment = fake.boolean()
        sum_pay_adjustment = fake.boolean()
        sum_total_fee = fake.boolean()
        sum_total_pay = fake.boolean()
        sum_balance = fake.boolean()
        grid_fee_column = fake.boolean()
        grid_pay_column = fake.boolean()
        is_reversal = fake.boolean()
        
        data.append([
            transaction_type_id, description, interface_description, allow_in_new, 
            do_not_use, sum_fee_adjustment, sum_pay_adjustment, sum_total_fee, 
            sum_total_pay, sum_balance, grid_fee_column, grid_pay_column, is_reversal
        ])
        
    columns = [
        "TransactionTypeId", "Description", "InterfaceDescription", "AllowInNew", 
        "DoNotUse", "SumFeeAdjustment", "SumPayAdjustment", "SumTotalFee", 
        "SumTotalPay", "SumBalance", "GridFeeColumn", "GridPayColumn", "IsReversal"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, transaction_type_ids


def generate_country_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Country table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    country_ids = set()
    for _ in range(num_records):
        country_id = fake.unique.random_number(digits=4)
        country_ids.add(country_id)
        
        country = fake.country()
        nationality = fake.sentence(nb_words=2)
        code = fake.lexify(text="???")
        do_not_use = fake.boolean()
        do_not_use_nationality = fake.boolean()
        sdx = fake.lexify(text="????")
        
        data.append([country_id, country, nationality, code, do_not_use, do_not_use_nationality, sdx])
        
    columns = ["CountryId", "Country", "Nationality", "Code", "DoNotUse", "DoNotUseNationality", "Sdx"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, country_ids


def generate_status_contact_data(num_records: int, status_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Status Contact table.
    
    Args:
        num_records (int): The number of records to generate.
        status_ids (set): Set of status IDs from the Status table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    for _ in range(num_records):
        if random.random() < case_no_presence_percentage / 100:
            status_id = random.choice(list(status_ids))
        else:
            status_id = fake.unique.random_number(digits=4)
        
        contact = fake.name()
        court_name = fake.company()
        address1 = fake.street_address()
        address2 = fake.secondary_address()
        address3 = fake.city()
        address4 = fake.county()
        address5 = fake.country()
        postcode = fake.postcode()
        telephone = fake.phone_number()
        forenames = fake.first_name()
        title = fake.prefix()
        
        data.append([
            status_id, contact, court_name, address1, address2, address3, 
            address4, address5, postcode, telephone, forenames, title
        ])
        
    columns = [
        "StatusId", "Contact", "CourtName", "Address1", "Address2", "Address3", 
        "Address4", "Address5", "Postcode", "Telephone", "Forenames", "Title"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_fee_satisfaction_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Fee Satisfaction table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    fee_satisfaction_ids = set()
    for _ in range(num_records):
        fee_satisfaction_id = fake.unique.random_number(digits=4)
        fee_satisfaction_ids.add(fee_satisfaction_id)
        
        description = fake.sentence(nb_words=3)
        do_not_use = fake.boolean()
        
        data.append([fee_satisfaction_id, description, do_not_use])
        
    columns = ["FeeSatisfactionId", "Description", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, fee_satisfaction_ids


def generate_case_sponsor_data(num_records: int, case_nos: set) -> pd.DataFrame:
    """
    Generate sample data for the Case Sponsor table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
    
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
            
        name = fake.name()
        forenames = fake.first_name()
        title = fake.prefix()
        address1 = fake.street_address()
        address2 = fake.secondary_address()
        address3 = fake.city()
        address4 = fake.county()
        address5 = fake.country()
        postcode = fake.postcode()
        telephone = fake.phone_number()
        email = fake.email()
        authorised = fake.boolean()
        
        data.append([
            case_no, name, forenames, title, address1, address2, address3, 
            address4, address5, postcode, telephone, email, authorised
        ])
        
    columns = [
        "CaseNo", "Name", "Forenames", "Title", "Address1", "Address2", "Address3", 
        "Address4", "Address5", "Postcode", "Telephone", "Email", "Authorised"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_appellant_data(num_records: int, country_ids: set, detention_centre_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Appellant table.
    
    Args:
        num_records (int): The number of records to generate.
        country_ids (set): Set of country IDs from the Country table.
        detention_centre_ids (set): Set of detention centre IDs from the Detention Centre table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    appellant_ids = set()
    for _ in range(num_records):
        appellant_id = fake.unique.random_number(digits=4)
        appellant_ids.add(appellant_id)
        
        port_reference = fake.bothify(text="????-####")
        name = fake.name()
        forenames = fake.first_name()
        title = fake.prefix()
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=100)
        address1 = fake.street_address()
        address2 = fake.secondary_address()
        address3 = fake.city()
        address4 = fake.county()
        address5 = fake.country()
        
        if random.random() < case_no_presence_percentage / 100:
            country_id = random.choice(list(country_ids))
        else:
            country_id = fake.unique.random_number(digits=4)
        
        postcode = fake.postcode()
        telephone = fake.phone_number()
        email = fake.email()
        name_sdx = fake.lexify(text="????")
        forename_sdx = fake.lexify(text="????")
        detained = fake.random_element(elements=(1, 2, 3, 4))
        
        if random.random() < case_no_presence_percentage / 100:
            detention_centre_id = random.choice(list(detention_centre_ids))
        else:
            detention_centre_id = fake.unique.random_number(digits=4)
        
        fco_number = fake.bothify(text="FCO-????-####")
        prison_ref = fake.bothify(text="PRIS-????-####")
        
        data.append([
            appellant_id, port_reference, name, forenames, title, birth_date, 
            address1, address2, address3, address4, address5, country_id, 
            postcode, telephone, email, name_sdx, forename_sdx, detained, 
            detention_centre_id, fco_number, prison_ref
        ])
        
    columns = [
        "AppellantId", "PortReference", "Name", "Forenames", "Title", "BirthDate", 
        "Address1", "Address2", "Address3", "Address4", "Address5", "CountryId", 
        "Postcode", "Telephone", "Email", "NameSdx", "ForenameSdx", "Detained", 
        "DetentionCentreId", "FCONumber", "PrisonRef"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, appellant_ids


def generate_detention_centre_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Detention Centre table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    detention_centre_ids = set()
    for _ in range(num_records):
        detention_centre_id = fake.unique.random_number(digits=4)
        detention_centre_ids.add(detention_centre_id)
        
        centre = fake.company()
        centre_title = fake.sentence(nb_words=3)
        detention_centre_type = fake.random_element(elements=(1, 2, 4))
        address1 = fake.street_address()
        address2 = fake.secondary_address()
        address3 = fake.city()
        address4 = fake.county()
        address5 = fake.country()
        postcode = fake.postcode()
        fax = fake.phone_number()
        sdx = fake.lexify(text="????")
        
        data.append([
            detention_centre_id, centre, centre_title, detention_centre_type, 
            address1, address2, address3, address4, address5, postcode, fax, sdx
        ])
        
    columns = [
        "DetentionCentreId", "Centre", "CentreTitle", "DetentionCentreType", 
        "Address1", "Address2", "Address3", "Address4", "Address5", "Postcode", 
        "Fax", "Sdx"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, detention_centre_ids


def generate_hearing_type_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Hearing Type table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    hearing_type_ids = set()
    for _ in range(num_records):
        hearing_type_id = fake.unique.random_number(digits=4)
        hearing_type_ids.add(hearing_type_id)
        
        description = fake.sentence(nb_words=3)
        time_estimate = fake.random_int(min=1, max=100)
        do_not_use = fake.boolean()
        
        data.append([hearing_type_id, description, time_estimate, do_not_use])
        
    columns = ["HearingTypeId", "Description", "TimeEstimate", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, hearing_type_ids


def generate_embassy_data(num_records: int, country_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Embassy table.
    
    Args:
        num_records (int): The number of records to generate.
        country_ids (set): Set of country IDs from the Country table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    embassy_ids = set()
    for _ in range(num_records):
        embassy_id = fake.unique.random_number(digits=4)
        embassy_ids.add(embassy_id)
        
        if random.random() < case_no_presence_percentage / 100:
            country_id = random.choice(list(country_ids))
        else:
            country_id = fake.unique.random_number(digits=4)
        
        location = fake.city()
        embassy = fake.company()
        surname = fake.last_name()
        forenames = fake.first_name()
        title = fake.prefix()
        official_title = fake.job()
        address1 = fake.street_address()
        address2 = fake.secondary_address()
        address3 = fake.city()
        address4 = fake.county()
        address5 = fake.country()
        postcode = fake.postcode()
        telephone = fake.phone_number()
        fax = fake.phone_number()
        email = fake.email()
        do_not_use = fake.boolean()
        
        data.append([
            embassy_id, country_id, location, embassy, surname, forenames, title, 
            official_title, address1, address2, address3, address4, address5, 
            postcode, telephone, fax, email, do_not_use
        ])
        
    columns = [
        "EmbassyId", "CountryId", "Location", "Embassy", "Surname", "Forenames", 
        "Title", "OfficialTitle", "Address1", "Address2", "Address3", "Address4", 
        "Address5", "Postcode", "Telephone", "Fax", "Email", "DoNotUse"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, embassy_ids


def generate_port_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Port table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    port_ids = set()
    for _ in range(num_records):
        port_id = fake.unique.random_number(digits=4)
        port_ids.add(port_id)
        
        port_name = fake.city()
        address_name = fake.street_name()
        address1 = fake.street_address()
        address2 = fake.secondary_address()
        address3 = fake.city()
        address4 = fake.county()
        address5 = fake.country()
        postcode = fake.postcode()
        telephone = fake.phone_number()
        sdx = fake.lexify(text="????")
        
        data.append([
            port_id, port_name, address_name, address1, address2, address3, 
            address4, address5, postcode, telephone, sdx
        ])
        
    columns = [
        "PortId", "PortName", "AddressName", "Address1", "Address2", "Address3", 
        "Address4", "Address5", "Postcode", "Telephone", "Sdx"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, port_ids


def generate_bf_type_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the BF Type table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    bf_type_ids = set()
    for _ in range(num_records):
        bf_type_id = fake.unique.random_number(digits=4)
        bf_type_ids.add(bf_type_id)
        
        description = fake.sentence(nb_words=3)
        do_not_use = fake.boolean()
        
        data.append([bf_type_id, description, do_not_use])
        
    columns = ["BFTypeId", "Description", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, bf_type_ids


def generate_payment_remission_reason_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Payment Remission Reason table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    payment_remission_reason_ids = set()
    for _ in range(num_records):
        payment_remission_reason_id = fake.unique.random_number(digits=4)
        payment_remission_reason_ids.add(payment_remission_reason_id)
        
        description = fake.sentence(nb_words=3)
        do_not_use = fake.boolean()
        
        data.append([payment_remission_reason_id, description, do_not_use])
        
    columns = ["PaymentRemissionReasonId", "Description", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, payment_remission_reason_ids


def generate_reason_adjourn_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Reason Adjourn table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    reason_adjourn_ids = set()
    for _ in range(num_records):
        reason_adjourn_id = fake.unique.random_number(digits=4)
        reason_adjourn_ids.add(reason_adjourn_id)
        
        reason = fake.sentence(nb_words=3)
        do_not_use = fake.boolean()
        
        data.append([reason_adjourn_id, reason, do_not_use])
        
    columns = ["ReasonAdjournId", "Reason", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, reason_adjourn_ids


def generate_case_status_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Case Status table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
        set: Set of generated case status IDs.
    """
    fake = Faker("en_GB")
    
    data = []
    case_status_ids = set()
    for _ in range(num_records):
        case_status_id = fake.unique.random_number(digits=4)
        case_status_ids.add(case_status_id)
        
        description = fake.sentence(nb_words=3)
        do_not_use = fake.boolean(chance_of_getting_true=10)
        hearing_points = fake.boolean(chance_of_getting_true=20)
        
        data.append([case_status_id, description, do_not_use, hearing_points])
        
    columns = ["CaseStatusId", "Description", "DoNotUse", "HearingPoints"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, case_status_ids


def generate_hearing_point_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Hearing Point table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    for _ in range(num_records):
        hearing_point = fake.sentence(nb_words=3)
        
        data.append([hearing_point])
        
    columns = ["HearingPoint"]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_category_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Category table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    category_ids = set()
    for _ in range(num_records):
        category_id = fake.unique.random_number(digits=4)
        category_ids.add(category_id)
        
        description = fake.sentence(nb_words=3)
        priority = fake.random_int(min=1, max=10)
        flag = fake.lexify(text="???")
        on_screen = fake.boolean()
        file_label = fake.boolean()
        in_case = fake.boolean()
        in_bail = fake.boolean()
        in_visit_visa = fake.boolean()
        do_not_show = fake.boolean()
        fee_exemption = fake.boolean()
        
        data.append([
            category_id, description, priority, flag, on_screen, file_label, 
            in_case, in_bail, in_visit_visa, do_not_show, fee_exemption
        ])
        
    columns = [
        "CategoryId", "Description", "Priority", "Flag", "OnScreen", "FileLabel", 
        "InCase", "InBail", "InVisitVisa", "DoNotShow", "FeeExemption"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, category_ids


def generate_standard_direction_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Standard Direction table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    standard_direction_ids = set()
    for _ in range(num_records):
        standard_direction_id = fake.unique.random_number(digits=4)
        standard_direction_ids.add(standard_direction_id)
        
        description = fake.sentence(nb_words=3)
        do_not_use = fake.boolean()
        
        data.append([standard_direction_id, description, do_not_use])
        
    columns = ["StandardDirectionId", "Description", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, standard_direction_ids


def generate_history_data(num_records: int, case_nos: set, user_ids: set, status_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the History table.
    
    Args:
        num_records (int): The number of records to generate.
        case_nos (set): Set of case numbers from the Appeal Case table.
        user_ids (set): Set of user IDs from the Users table.
        status_ids (set): Set of status IDs from the Status table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    history_ids = set()
    for _ in range(num_records):
        history_id = fake.unique.random_number(digits=4)
        history_ids.add(history_id)
        
        if random.random() < case_no_presence_percentage / 100:
            case_no = random.choice(list(case_nos))
        else:
            case_prefix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            case_serial = fake.unique.random_number(digits=6)
            case_year = fake.random_int(min=1990, max=datetime.now().year)
            case_no = f"{case_prefix}{case_serial}/{case_year}"
            
        hist_date = fake.date_between(start_date="-5y", end_date="today")
        hist_type = fake.random_element(elements=list(range(1, 51)))
        
        if random.random() < case_no_presence_percentage / 100:
            user_id = random.choice(list(user_ids))
        else:
            user_id = fake.unique.random_number(digits=4)
        
        comment = fake.sentence(nb_words=10)
        deleted_by = fake.unique.random_number(digits=4) if fake.boolean() else None
        
        if random.random() < case_no_presence_percentage / 100:
            status_id = random.choice(list(status_ids))
        else:
            status_id = fake.unique.random_number(digits=4)
        
        data.append([
            history_id, case_no, hist_date, hist_type, user_id, comment, deleted_by, status_id
        ])
        
    columns = [
        "HistoryId", "CaseNo", "HistDate", "HistType", "UserId", "Comment", "DeletedBy", "StatusId"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, history_ids


def generate_case_list_data(num_records: int, status_ids: set, hearing_type_ids: set, 
                           list_ids: set, adjudicator_ids: set, hearing_centre_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Case List table with proper referential integrity.
    
    Args:
        num_records (int): Number of records to generate
        status_ids (set): Valid status IDs from Status table
        hearing_type_ids (set): Valid hearing type IDs from HearingType table
        list_ids (set): Valid list IDs from List table
        adjudicator_ids (set): Valid adjudicator IDs from Adjudicator table
        hearing_centre_ids (set): Valid hearing centre IDs from HearingCentre table
    
    Returns:
        pd.DataFrame: Generated DataFrame
    """
    fake = Faker("en_GB")
    
    if not all([status_ids, hearing_type_ids, list_ids, adjudicator_ids, hearing_centre_ids]):
        raise ValueError("All foreign key sets must contain valid IDs")

    data = []
    status_list_map = {}  # Track active lists per status
    
    for _ in range(num_records):
        # Generate primary key
        list_id = fake.unique.random_number(digits=6)
        
        # Select valid foreign keys
        status_id = random.choice(list(status_ids))
        hearing_type = random.choice(list(hearing_type_ids))
        adjudicator_id = random.choice(list(adjudicator_ids))
        
        # Ensure only one active list per status
        if status_id in status_list_map:
            list_id = status_list_map[status_id]
        else:
            status_list_map[status_id] = list_id
            
        # Generate time estimates based on hearing type
        time_estimate = random.randint(30, 240)  # 30 mins to 4 hours
        hearing_duration = time_estimate  # Initially same as estimate
        
        # Generate list details
        list_number = fake.random_int(min=1, max=100)
        
        # Generate start time during business hours
        start_time = fake.date_time_between(
            start_date=datetime.now(),
            end_date=datetime.now() + timedelta(days=180),
            tzinfo=None
        )
        start_time = start_time.replace(
            hour=random.randint(9, 16),
            minute=random.choice([0, 15, 30, 45])
        )
        
        data.append([
            list_id,
            status_id,
            time_estimate,
            hearing_type,
            list_number,
            adjudicator_id,
            hearing_duration,
            start_time
        ])
    
    columns = [
        "ListId",
        "StatusId",
        "TimeEstimate",
        "HearingType",
        "ListNumber",
        "AdjudicatorId",
        "HearingDuration",
        "StartTime"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df


def generate_appeal_type_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Appeal Type table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    appeal_type_ids = set()
    for _ in range(num_records):
        appeal_type_id = fake.unique.random_number(digits=4)
        appeal_type_ids.add(appeal_type_id)
        
        description = fake.sentence(nb_words=3)
        prefix = fake.lexify(text="??")
        number = fake.random_int(min=1000, max=9999)
        full_name = fake.sentence(nb_words=5)
        category = fake.random_int(min=1, max=10)
        appeal_type = fake.random_element(elements=(1, 2, 3))
        do_not_use = fake.boolean()
        date_start = fake.date_between(start_date="-5y", end_date="today")
        date_end = fake.date_between(start_date=date_start, end_date="+5y")
        
        data.append([
            appeal_type_id, description, prefix, number, full_name, category, appeal_type, 
            do_not_use, date_start, date_end
        ])
        
    columns = [
        "AppealTypeId", "Description", "Prefix", "Number", "FullName", "Category", "AppealType", 
        "DoNotUse", "DateStart", "Dateend"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, appeal_type_ids


def generate_department_data(num_records: int) -> pd.DataFrame:
    fake = Faker("en_GB")
    
    data = []
    dept_ids = set()
    for _ in range(num_records):
        dept_id = fake.unique.random_number(digits=4)
        dept_ids.add(dept_id)
        
        name = fake.company()
        
        data.append([dept_id, name])
        
    columns = ["DeptId", "Name"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, dept_ids


def generate_reason_link_data(num_records: int) -> pd.DataFrame:
    fake = Faker("en_GB")
    
    data = []
    reason_link_ids = set()
    for _ in range(num_records):
        reason_link_id = fake.unique.random_number(digits=4)
        reason_link_ids.add(reason_link_id)
        
        description = fake.sentence(nb_words=4)
        
        data.append([reason_link_id, description])
        
    columns = ["ReasonLinkId", "Description"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, reason_link_ids


def generate_employment_term_data(num_records: int) -> pd.DataFrame:
    fake = Faker("en_GB")
    
    data = []
    employment_term_ids = set()
    for _ in range(num_records):
        employment_term_id = fake.unique.random_number(digits=4)
        employment_term_ids.add(employment_term_id)
        
        description = fake.sentence(nb_words=3)
        
        data.append([employment_term_id, description])
        
    columns = ["EmploymentTermId", "Description"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, employment_term_ids


def generate_do_not_use_reason_data(num_records: int) -> pd.DataFrame:
    fake = Faker("en_GB")
    
    data = []
    do_not_use_reason_ids = set()
    for _ in range(num_records):
        do_not_use_reason_id = fake.unique.random_number(digits=4)
        do_not_use_reason_ids.add(do_not_use_reason_id)
        
        reason = fake.sentence(nb_words=4)
        
        data.append([do_not_use_reason_id, reason])
        
    columns = ["DoNotUseReasonId", "Reason"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, do_not_use_reason_ids


def generate_adjudicator_data(num_records: int, centre_ids: set, employment_term_ids: set, do_not_use_reason_ids: set) -> pd.DataFrame:
    fake = Faker("en_GB")
    
    data = []
    adjudicator_ids = set()
    for _ in range(num_records):
        adjudicator_id = fake.unique.random_number(digits=4)
        adjudicator_ids.add(adjudicator_id)
        
        surname = fake.last_name()
        forenames = fake.first_name()
        title = fake.prefix()
        
        if random.random() < 0.8:  # 80% chance of having a valid CentreId
            centre_id = random.choice(list(centre_ids))
        else:
            centre_id = None
        
        address1 = fake.street_address()
        address2 = fake.secondary_address() if fake.boolean(chance_of_getting_true=30) else None
        address3 = fake.city() if fake.boolean(chance_of_getting_true=20) else None
        address4 = fake.county() if fake.boolean(chance_of_getting_true=10) else None
        address5 = fake.country() if fake.boolean(chance_of_getting_true=5) else None
        postcode = fake.postcode()
        full_time = fake.boolean()
        sdx = fake.lexify(text="????", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        do_not_list = fake.boolean(chance_of_getting_true=10)
        date_of_birth = fake.date_of_birth(minimum_age=30, maximum_age=70)
        correspondence_address = fake.random_element(elements=(1, 2))
        contact_telephone = fake.random_element(elements=(1, 2, 3))
        contact_details = fake.phone_number() if fake.boolean(chance_of_getting_true=50) else None
        available_at_short_notice = fake.boolean(chance_of_getting_true=30)
        
        if random.random() < 0.9:  # 90% chance of having a valid EmploymentTermId
            employment_terms = random.choice(list(employment_term_ids))
        else:
            employment_terms = None
        
        identity_number = fake.numerify(text="############") if fake.boolean(chance_of_getting_true=80) else None
        date_of_retirement = fake.future_date(end_date="+5y") if fake.boolean(chance_of_getting_true=20) else None
        contract_end_date = fake.future_date(end_date="+2y") if fake.boolean(chance_of_getting_true=30) else None
        contract_renewal_date = fake.future_date(end_date="+1y") if fake.boolean(chance_of_getting_true=40) else None
        
        if random.random() < 0.1:  # 10% chance of having a valid DoNotUseReasonId
            do_not_use_reason = random.choice(list(do_not_use_reason_ids))
        else:
            do_not_use_reason = None
        
        telephone = fake.phone_number()
        mobile = fake.phone_number()
        email = fake.email()
        business_address1 = fake.street_address()
        business_address2 = fake.secondary_address() if fake.boolean(chance_of_getting_true=30) else None
        business_address3 = fake.city() if fake.boolean(chance_of_getting_true=20) else None
        business_address4 = fake.county() if fake.boolean(chance_of_getting_true=10) else None
        business_address5 = fake.country() if fake.boolean(chance_of_getting_true=5) else None
        business_postcode = fake.postcode()
        business_telephone = fake.phone_number()
        business_fax = fake.phone_number()
        business_email = fake.company_email()
        judicial_instructions = fake.paragraph(nb_sentences=3) if fake.boolean(chance_of_getting_true=20) else None
        judicial_instructions_date = fake.date_between(start_date="-1y", end_date="today") if judicial_instructions else None
        notes = fake.paragraph(nb_sentences=2) if fake.boolean(chance_of_getting_true=30) else None
        judicial_status = fake.random_element(elements=(1, 2, 3, 4, 5, 6, 7, 21, 22, 23, 25, 26, 28, 29))
        
        data.append([
            adjudicator_id, surname, forenames, title, centre_id,
            address1, address2, address3, address4, address5,
            postcode, full_time, sdx, do_not_list, date_of_birth,
            correspondence_address, contact_telephone, contact_details, available_at_short_notice, employment_terms,
            identity_number, date_of_retirement, contract_end_date, contract_renewal_date, do_not_use_reason,
            telephone, mobile, email, business_address1, business_address2,
            business_address3, business_address4, business_address5, business_postcode, business_telephone,
            business_fax, business_email, judicial_instructions, judicial_instructions_date, notes,
            judicial_status
        ])
        
    columns = [
        "AdjudicatorId", "Surname", "Forenames", "Title", "CentreId",
        "Address1", "Address2", "Address3", "Address4", "Address5",
        "Postcode", "FullTime", "Sdx", "DoNotList", "DateOfBirth",
        "CorrespondenceAddress", "ContactTelephone", "ContactDetails", "AvailableAtShortNotice", "EmploymentTerms",
        "IdentityNumber", "DateOfRetirement", "ContractEndDate", "ContractRenewalDate", "DoNotUseReason",
        "Telephone", "Mobile", "Email", "BusinessAddress1", "BusinessAddress2",
        "BusinessAddress3", "BusinessAddress4", "BusinessAddress5", "BusinessPostcode", "BusinessTelephone",
        "BusinessFax", "BusinessEmail", "JudicialInstructions", "JudicialInstructionsDate", "Notes",
        "JudicialStatus"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df, adjudicator_ids

def generate_hearing_point_change_reason_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Hearing Point Change Reason table.
    
    Args:
        num_records (int): The number of records to generate.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    hearing_point_change_reason_ids = set()
    for _ in range(num_records):
        hearing_point_change_reason_id = fake.unique.random_number(digits=4)
        hearing_point_change_reason_ids.add(hearing_point_change_reason_id)
        
        description = fake.sentence(nb_words=4)
        do_not_use = fake.boolean(chance_of_getting_true=10)
        
        data.append([hearing_point_change_reason_id, description, do_not_use])
        
    columns = ["HearingPointsChangeReasonId", "Description", "DoNotUse"]
    
    df = pd.DataFrame(data, columns=columns)
    return df, hearing_point_change_reason_ids


def generate_hearing_point_history_data(num_records: int, status_ids: set, user_ids: set, hearing_point_change_reason_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the Hearing Point History table.
    
    Args:
        num_records (int): The number of records to generate.
        status_ids (set): Set of status IDs from the Status table.
        user_ids (set): Set of user IDs from the User table.
        hearing_point_change_reason_ids (set): Set of hearing point change reason IDs from the Hearing Point Change Reason table.
    
    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")
    
    data = []
    for _ in range(len(num_records)):
        hearing_point_history_id = fake.unique.random_number(digits=4)
        
        if random.random() < 0.8:  # 80% chance of having a valid StatusId
            status_id = random.choice(list(status_ids))
        else:
            status_id = None
        
        hist_date = fake.date_between(start_date="-5y", end_date="today")
        hist_type = fake.random_element(elements=(1, 2))
        
        if random.random() < 0.9:  # 90% chance of having a valid UserId
            user_id = random.choice(list(user_ids))
        else:
            user_id = None
        
        default_points = fake.random_int(min=0, max=5)
        initial_points = fake.random_int(min=0, max=5)
        final_points = fake.random_int(min=0, max=5)
        
        if random.random() < 0.7:  # 70% chance of having a valid HearingPointsChangeReasonId
            hearing_point_change_reason_id = random.choice(list(hearing_point_change_reason_ids))
        else:
            hearing_point_change_reason_id = None
        
        data.append([
            hearing_point_history_id, status_id, hist_date, hist_type, user_id,
            default_points, initial_points, final_points, hearing_point_change_reason_id
        ])
        
    columns = [
        "HearingPointsHistoryId", "StatusId", "HistDate", "HistType", "UserId",
        "DefaultPoints", "InitialPoints", "FinalPoints", "HearingPointsChangeReasonId"
    ]
    
    df = pd.DataFrame(data, columns=columns)
    return df


# Function to clean DataFrame before Spark conversion
def prepare_for_spark(df):
    # Convert all columns to basic Python types
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].astype(str)
        elif df[col].dtype == 'bool':
            df[col] = df[col].astype(int)
    return df


# Generate sample data for each table
num_appeal_case_records = 100
num_case_respondent_records = 120
num_main_respondent_records = 20
num_respondent_records = 30
num_file_location_records = 150
num_case_representative_records = 80
num_case_surety_records = 90
num_cost_award_records = 70
num_appeal_category_records = 110
num_link_records = 50
num_appeal_grounds_records = 130
num_status_records = 200
num_document_received_records = 140
num_appeal_new_matter_records = 60
num_hearing_centre_records = 20
num_review_standard_direction_records = 40
num_review_specific_direction_records = 30
num_language_records = 15
num_received_document_records = 50
num_appeal_type_category_records = 30
num_link_detail_records = 40
num_user_records = 100
num_decision_type_records = 20
num_transaction_records = 200
num_transaction_status_records = 5
num_cost_order_records = 60
num_list_type_records = 10
num_list_sitting_records = 150
num_court_records = 15
num_list_records = 80
num_new_matter_records = 50
num_documents_received_records = 100
num_representative_records = 60
num_case_fee_summary_records = 120
num_human_right_records = 30
num_case_appellant_records = 110
num_bf_diary_records = 90
num_appeal_human_right_records = 80
num_transaction_method_records = 10
num_transaction_type_records = 20
num_country_records = 50
num_status_contact_records = 150
num_fee_satisfaction_records = 15
num_case_sponsor_records = 70
num_appellant_records = 100
num_detention_centre_records = 10
num_hearing_type_records = 20
num_embassy_records = 30
num_port_records = 15
num_bf_type_records = 10
num_payment_remission_reason_records = 8
num_reason_adjourn_records = 20
num_case_status_records = 180
num_hearing_point_history_records = 120
num_hearing_point_change_reason_records = 100
num_hearing_point_records = 80
num_category_records = 25
num_standard_direction_records = 30
num_case_list_records = 160
num_history_records = 300
num_appeal_type_records = 25
num_dept_records = 20
num_reason_link_records = 30
num_employment_term_records = 15
num_do_not_use_reason_records = 10
num_detention_centre_records = 5
num_court_records = 10
num_adjudicator_records = 50
num_hearing_point_history = 100,
num_hearing_point_change_reason = 10


# Part 1: Base Lookup Tables
language_data, language_ids = generate_language_data(num_language_records)
port_data, port_ids = generate_port_data(num_port_records)
country_data, country_ids = generate_country_data(num_country_records)
embassy_data, embassy_ids = generate_embassy_data(num_embassy_records, country_ids)
appeal_type_data, appeal_type_ids = generate_appeal_type_data(num_appeal_type_records)
hearing_centre_data, centre_ids = generate_hearing_centre_data(num_hearing_centre_records)
dept_data, dept_ids = generate_department_data(num_dept_records)

# Part 2: User and Security
user_data, user_ids = generate_user_data(num_user_records, dept_ids)

# Part 3: Basic Reference Data
reason_link_data, reason_link_ids = generate_reason_link_data(num_reason_link_records)
employment_term_data, employment_term_ids = generate_employment_term_data(num_employment_term_records)
do_not_use_reason_data, do_not_use_reason_ids = generate_do_not_use_reason_data(num_do_not_use_reason_records)
detention_centre_data, detention_centre_ids = generate_detention_centre_data(num_detention_centre_records)

# Part 4: Appeal Case and Direct Dependencies
appeal_case_data, case_nos = generate_appeal_case_data(num_records=num_appeal_case_records, language_ids=language_ids, port_ids=port_ids, embassy_ids=embassy_ids, appeal_type_ids=appeal_type_ids, centre_ids=centre_ids, country_ids=country_ids, user_ids=user_ids)

# Part 5: File Location
file_location_data = generate_file_location_data(num_file_location_records, case_nos, dept_ids)

# Part 6: Respondent Chain
main_respondent_data, main_respondent_ids = generate_main_respondent_data(num_main_respondent_records)
respondent_data, respondent_ids = generate_respondent_data(num_respondent_records)
case_respondent_data = generate_case_respondent_data(num_case_respondent_records, case_nos, main_respondent_ids, respondent_ids)

# Part 7: Representative Chain
representative_data, representative_ids = generate_representative_data(num_representative_records, user_ids)
case_representative_data = generate_case_representative_data(num_case_representative_records, case_nos, representative_ids)

# Part 8: Case Related Data
case_surety_data = generate_case_surety_data(num_case_surety_records, case_nos)
cost_award_data = generate_cost_award_data(num_cost_award_records, case_nos)

# Part 9: Categories and Appeals
category_data, category_ids = generate_category_data(num_category_records)
appeal_category_data = generate_appeal_category_data(num_appeal_category_records, case_nos, category_ids)
link_data, link_nos = generate_link_data(num_link_records, case_nos)
appeal_grounds_data = generate_appeal_grounds_data(num_appeal_grounds_records, case_nos, appeal_type_ids)

# Part 10: Status Related
reason_adjourn_data, reason_adjourn_ids = generate_reason_adjourn_data(num_reason_adjourn_records)
decision_type_data, decision_type_ids = generate_decision_type_data(num_decision_type_records)
case_status_data, case_status_ids = generate_case_status_data(num_case_status_records)

# Part 11: Document Management
received_document_data, received_document_ids = generate_received_document_data(num_received_document_records)
document_received_data = generate_document_received_data(num_document_received_records, case_nos, received_document_ids)
received_document_data, received_document_ids = generate_received_document_data(num_received_document_records)
documents_received_data = generate_documents_received_data(num_documents_received_records, case_nos, received_document_ids)

# Part 12: New Matter Management
new_matter_data, new_matter_ids = generate_new_matter_data(num_new_matter_records)
appeal_new_matter_data, appeal_new_matter_ids = generate_appeal_new_matter_data(num_appeal_new_matter_records, case_nos, new_matter_ids)

# Part 13: Hearing Points
hearing_point_data = generate_hearing_point_data(num_hearing_point_records)
hearing_point_change_reason_data, hearing_point_change_reason_ids = generate_hearing_point_change_reason_data(
    num_hearing_point_change_reason_records
)

# Part 14: Status
status_data, status_ids = generate_status_data(num_records=num_status_records, case_nos=case_nos, centre_ids=centre_ids, language_ids=language_ids, reason_adjourn_ids=reason_adjourn_ids, case_status_ids=case_status_ids, decision_type_ids=decision_type_ids)

# Part 15: Directions
standard_direction_data, standard_direction_ids = generate_standard_direction_data(num_standard_direction_records)
review_standard_direction_data, review_standard_direction_data_ids = generate_review_standard_direction_data(num_review_standard_direction_records, case_nos, status_ids, standard_direction_ids)
review_specific_direction_data, review_specific_direction_data_ids = generate_review_specific_direction_data(num_review_specific_direction_records, case_nos, status_ids)

# Part 16: Appeal Type Categories
appeal_type_category_data = generate_appeal_type_category_data(num_appeal_type_category_records, appeal_type_ids, category_ids)

# Part 17: Link Details
link_detail_data = generate_link_detail_data(num_link_detail_records, link_nos, reason_link_ids)

# Part 18: Transactions
transaction_method_data, transaction_method_ids = generate_transaction_method_data(num_transaction_method_records)
transaction_type_data, transaction_type_ids = generate_transaction_type_data(num_transaction_type_records)
transaction_status_data, transaction_status_ids = generate_transaction_status_data(num_transaction_status_records)
transaction_data, transaction_ids = generate_transaction_data(num_transaction_records, case_nos, transaction_type_ids, transaction_method_ids, transaction_status_ids, user_ids)

# Part 19: Cost Orders
cost_order_data, cost_order_data_ids = generate_cost_order_data(num_cost_order_records, case_nos, decision_type_ids, representative_ids)

# Part 20: Hearing Management
list_type_data, list_type_ids = generate_list_type_data(num_list_type_records)
adjudicator_data, adjudicator_ids = generate_adjudicator_data(num_adjudicator_records, centre_ids, employment_term_ids, do_not_use_reason_ids)
court_data, court_ids = generate_court_data(num_court_records, centre_ids)
list_data, list_ids = generate_list_data(num_list_records, centre_ids, court_ids, list_type_ids)
list_sitting_data, listing_sitting_data_ids = generate_list_sitting_data(num_list_sitting_records, list_ids, adjudicator_ids, user_ids)

# Part 21: Case Management
hearing_type_data, hearing_type_ids = generate_hearing_type_data(num_hearing_type_records)
case_list_data = generate_case_list_data(num_case_list_records, status_ids, hearing_type_ids, list_ids, adjudicator_ids, centre_ids)

# Part 22: Human Rights
human_right_data, human_right_ids = generate_human_right_data(num_human_right_records)
appeal_human_right_data = generate_appeal_human_right_data(num_appeal_human_right_records, case_nos, human_right_ids)

# Part 23: BF Management
bf_type_data, bf_type_ids = generate_bf_type_data(num_bf_type_records)
bf_diary_data, bf_diary_data_ids = generate_bf_diary_data(num_bf_diary_records, case_nos, bf_type_ids, user_ids)

# Part 24: Status Contacts
status_contact_data = generate_status_contact_data(num_status_contact_records, status_ids)

# Part 25: Fee Management
fee_satisfaction_data, fee_satisfaction_ids = generate_fee_satisfaction_data(num_fee_satisfaction_records)
payment_remission_reason_data, payment_remission_reason_ids = generate_payment_remission_reason_data(num_payment_remission_reason_records)
case_fee_summary_data, case_fee_summary_data_ids = generate_case_fee_summary_data(num_case_fee_summary_records, case_nos, payment_remission_reason_ids)

# Part 26: Appellant Management
appellant_data, appellant_ids = generate_appellant_data(num_appellant_records, country_ids, detention_centre_ids)
case_appellant_data = generate_case_appellant_data(num_case_appellant_records, case_nos, appellant_ids)

# Part 27: Case Sponsor
case_sponsor_data = generate_case_sponsor_data(num_case_sponsor_records, case_nos)

# Part 28: History and Hearing Points
hearing_point_history_data = generate_hearing_point_history_data(num_hearing_point_history, status_ids, user_ids, hearing_point_change_reason_ids)
history_data, history_data_ids = generate_history_data(num_history_records, case_nos, user_ids, status_ids)

# Update Appeal Case table with referential integrity
appeal_case_data["AppealTypeId"] = appeal_case_data["AppealTypeId"].apply(lambda x: random.choice(list(appeal_type_ids)) if pd.notnull(x) else None)
appeal_case_data["CentreId"] = appeal_case_data["CentreId"].apply(lambda x: random.choice(list(centre_ids)))
appeal_case_data["LanguageId"] = appeal_case_data["LanguageId"].apply(lambda x: random.choice(list(language_ids)))
appeal_case_data["PortId"] = appeal_case_data["PortId"].apply(lambda x: random.choice(list(port_ids)) if pd.notnull(x) else None)
appeal_case_data["VVEmbassyId"] = appeal_case_data["VVEmbassyId"].apply(lambda x: random.choice(list(embassy_ids)) if pd.notnull(x) else None)
appeal_case_data["CountryId"] = appeal_case_data["CountryId"].apply(lambda x: random.choice(list(country_ids)) if pd.notnull(x) else None)
appeal_case_data["ThirdCountryId"] = appeal_case_data["ThirdCountryId"].apply(lambda x: random.choice(list(country_ids)) if pd.notnull(x) else None)
# appeal_case_data["HumanRights"] = appeal_case_data["CaseNo"].apply(lambda x: 1 if x in appeal_human_right_data["CaseNo"].values else 0)
cases_with_human_rights = set(appeal_human_right_data["CaseNo"].unique())
appeal_case_data.loc[appeal_case_data["CaseNo"].isin(cases_with_human_rights), "HumanRights"] = 1

# Update Status table with referential integrity
status_data["CentreId"] = status_data["CentreId"].apply(lambda x: random.choice(list(centre_ids)))
status_data["AdjudicatorId"] = status_data["AdjudicatorId"].apply(lambda x: random.choice(list(adjudicator_ids)) if pd.notnull(x) else None)
status_data["AdditionalLanguageId"] = status_data["AdditionalLanguageId"].apply(lambda x: random.choice(list(language_ids)) if pd.notnull(x) else None)
status_data["Chairman"] = status_data["Chairman"].apply(lambda x: random.choice(list(adjudicator_ids)) if pd.notnull(x) else None)
status_data["LayMember1"] = status_data["LayMember1"].apply(lambda x: random.choice(list(adjudicator_ids)) if pd.notnull(x) else None)
status_data["LayMember2"] = status_data["LayMember2"].apply(lambda x: random.choice(list(adjudicator_ids)) if pd.notnull(x) else None)
status_data["AdjournedListTypeId"] = status_data["AdjournedListTypeId"].apply(lambda x: random.choice(list(list_type_ids)) if pd.notnull(x) else None)
status_data["AdjournedHearingTypeId"] = status_data["AdjournedHearingTypeId"].apply(lambda x: random.choice(list(hearing_type_ids)) if pd.notnull(x) else None)
status_data["AdjournedCentreId"] = status_data["AdjournedCentreId"].apply(lambda x: random.choice(list(centre_ids)) if pd.notnull(x) else None)
status_data["ListTypeId"] = status_data["ListTypeId"].apply(lambda x: random.choice(list(list_type_ids)) if pd.notnull(x) else None)
status_data["HearingTypeId"] = status_data["HearingTypeId"].apply(lambda x: random.choice(list(hearing_type_ids)) if pd.notnull(x) else None)
status_data["ListRequirementTypeId"] = status_data["ListRequirementTypeId"].apply(lambda x: random.choice(list(hearing_type_ids)) if pd.notnull(x) else None)
status_data["UpperTribunalHearingDirectionId"] = status_data["UpperTribunalHearingDirectionId"].apply(lambda x: random.choice(list(appeal_type_ids)) if pd.notnull(x) else None)
adjudicator_data["EmploymentTerms"] = [random.choice(list(employment_term_ids)) for _ in range(len(adjudicator_data))]

# Update tables with referential integrity
data = []
for _ in range(len(appeal_type_category_data)):
    data.append({
        "AppealTypeId": random.choice(list(appeal_type_ids)),
        "CategoryId": random.choice(list(category_ids)),
        "FeeExempt": random.choice([True, False])
    })
appeal_type_category_data = pd.DataFrame(data)
# appeal_new_matter_data["Notes"] = [
#     fake.sentence() if new_matter_data.loc[new_matter_data["NewMatterId"] == row["NewMatterId"], "NotesRequired"].iloc[0] else None 
#     for _, row in appeal_new_matter_data.iterrows()
# ]
link_detail_data["LinkNo"] = link_detail_data["LinkNo"].apply(lambda x: random.choice(list(link_nos)))
link_detail_data["ReasonLinkId"] = link_detail_data["ReasonLinkId"].apply(lambda x: random.choice(list(reason_link_ids)) if pd.notnull(x) else None)
user_data["DeptId"] = user_data["DeptId"].apply(lambda x: random.choice(list(dept_ids)) if pd.notnull(x) else None)
transaction_data["TransactionTypeId"] = transaction_data["TransactionTypeId"].apply(lambda x: random.choice(list(transaction_type_ids)))
transaction_data["TransactionMethodId"] = transaction_data["TransactionMethodId"].apply(lambda x: random.choice(list(transaction_method_ids)) if pd.notnull(x) else None)
transaction_data["Status"] = transaction_data["Status"].apply(lambda x: random.choice(list(transaction_status_ids)) if pd.notnull(x) else None)
transaction_data["CreateUserId"] = transaction_data["CreateUserId"].apply(lambda x: random.choice(list(user_ids)) if pd.notnull(x) else None)
transaction_data["LastEditUserId"] = transaction_data["LastEditUserId"].apply(lambda x: random.choice(list(user_ids)) if pd.notnull(x) else None)
transaction_data["ReferringTransactionId"] = transaction_data["TransactionId"].apply(lambda x: random.choice(list(transaction_ids)) if random.random() < 0.3 else None)
cost_order_data["OutcomeOfAppealWhereDecisionMade"] = [
    random.choice(list(decision_type_ids)) if random.random() < 0.8 else None 
    for _ in range(len(cost_order_data))
]
cost_order_data["ApplyingRepresentativeId"] = [
    random.choice(list(representative_ids)) if random.random() < 0.8 else None 
    for _ in range(len(cost_order_data))
]
list_sitting_data["Position"] = [random.randint(1, 6) for _ in range(len(list_sitting_data))]
list_sitting_data["AdjudicatorId"] = [
    random.choice(list(adjudicator_ids)) 
    for _ in range(len(list_sitting_data))
]

list_sitting_data["UserId"] = [
    random.choice(list(user_ids)) if random.random() < 0.8 else None 
    for _ in range(len(list_sitting_data))
]
court_data["CentreId"] = court_data["CentreId"].apply(lambda x: random.choice(list(centre_ids)))
list_data["CentreId"] = list_data["CentreId"].apply(lambda x: random.choice(list(centre_ids)))
list_data["CourtId"] = list_data["CourtId"].apply(lambda x: random.choice(list(court_ids)))
list_data["ListTypeId"] = list_data["ListTypeId"].apply(lambda x: random.choice(list(list_type_ids)) if pd.notnull(x) else None)
documents_received_data["DocumentReceivedId"] = documents_received_data["DocumentReceivedId"].apply(lambda x: random.choice(list(received_document_ids)))
case_fee_summary_data["PaymentRemissionReason"] = [
    random.choice(list(payment_remission_reason_ids)) if random.random() < 0.8 else None 
    for _ in range(len(case_fee_summary_data))
]
case_appellant_data["AppellantId"] = case_appellant_data["AppellantId"].apply(lambda x: random.choice(list(appellant_ids)))
appeal_human_right_data["HumanRightId"] = appeal_human_right_data["HumanRightId"].apply(lambda x: random.choice(list(human_right_ids)))
embassy_data["CountryId"] = embassy_data["CountryId"].apply(lambda x: random.choice(list(country_ids)))
appellant_data["CountryId"] = appellant_data["CountryId"].apply(lambda x: random.choice(list(country_ids)) if pd.notnull(x) else None)
appellant_data["DetentionCentreId"] = appellant_data["DetentionCentreId"].apply(lambda x: random.choice(list(detention_centre_ids)) if pd.notnull(x) else None)


# Save the generated data as Parquet files for local test example, it is too many tables to save locally
appeal_case_data.to_parquet("AppealCase.parquet", index=False)
case_respondent_data.to_parquet("CaseRespondent.parquet", index=False)


spark_appeal_case_data = spark.createDataFrame(appeal_case_data)
spark_case_respondent_data = spark.createDataFrame(case_respondent_data)
spark_main_respondent_data = spark.createDataFrame(main_respondent_data)
spark_respondent_data = spark.createDataFrame(respondent_data)
spark_file_location_data = spark.createDataFrame(file_location_data)
spark_representative_data = spark.createDataFrame(representative_data)
spark_case_representative_data = spark.createDataFrame(case_representative_data)
spark_case_surety_data = spark.createDataFrame(case_surety_data)
spark_cost_award_data = spark.createDataFrame(cost_award_data)
spark_category_data = spark.createDataFrame(category_data)
spark_appeal_category_data = spark.createDataFrame(appeal_category_data)
spark_link_data = spark.createDataFrame(link_data)
spark_appeal_type_data = spark.createDataFrame(appeal_type_data)
spark_appeal_grounds_data = spark.createDataFrame(appeal_grounds_data)
spark_language_data = spark.createDataFrame(language_data)
spark_reason_adjourn_data = spark.createDataFrame(reason_adjourn_data)
spark_case_status_data = spark.createDataFrame(case_status_data)
spark_decision_type_data = spark.createDataFrame(decision_type_data)
spark_status_data = spark.createDataFrame(status_data)
spark_received_document_data = spark.createDataFrame(received_document_data)
spark_document_received_data = spark.createDataFrame(document_received_data)
spark_new_matter_data = spark.createDataFrame(new_matter_data)
spark_appeal_new_matter_data = spark.createDataFrame(prepare_for_spark(appeal_new_matter_data))
spark_hearing_centre_data = spark.createDataFrame(hearing_centre_data)
spark_standard_direction_data = spark.createDataFrame(standard_direction_data)
spark_review_standard_direction_data = spark.createDataFrame(prepare_for_spark(review_standard_direction_data))
spark_review_specific_direction_data = spark.createDataFrame(prepare_for_spark(review_specific_direction_data))
spark_appeal_type_category_data = spark.createDataFrame(prepare_for_spark(appeal_type_category_data))
spark_reason_link_data = spark.createDataFrame(reason_link_data)
spark_link_detail_data = spark.createDataFrame(link_detail_data)
spark_dept_data = spark.createDataFrame(dept_data)
spark_user_data = spark.createDataFrame(user_data)
spark_transaction_method_data = spark.createDataFrame(transaction_method_data)
spark_transaction_type_data = spark.createDataFrame(transaction_type_data)
spark_transaction_status_data = spark.createDataFrame(transaction_status_data)
spark_transaction_data = spark.createDataFrame(transaction_data)
spark_cost_order_data = spark.createDataFrame(prepare_for_spark(cost_order_data))
spark_list_type_data = spark.createDataFrame(list_type_data)
spark_adjudicator_data = spark.createDataFrame(adjudicator_data)
spark_list_sitting_data = spark.createDataFrame(prepare_for_spark(list_sitting_data))
spark_court_data = spark.createDataFrame(court_data)
spark_list_data = spark.createDataFrame(list_data)
spark_documents_received_data = spark.createDataFrame(documents_received_data)
spark_case_fee_summary_data = spark.createDataFrame(prepare_for_spark(case_fee_summary_data))
spark_human_right_data = spark.createDataFrame(human_right_data)
spark_case_appellant_data = spark.createDataFrame(case_appellant_data)
spark_bf_type_data = spark.createDataFrame(bf_type_data)
spark_bf_diary_data = spark.createDataFrame(prepare_for_spark(bf_diary_data))
spark_appeal_human_right_data = spark.createDataFrame(appeal_human_right_data)
spark_country_data = spark.createDataFrame(country_data)
spark_status_contact_data = spark.createDataFrame(status_contact_data)
spark_fee_satisfaction_data = spark.createDataFrame(fee_satisfaction_data)
spark_case_sponsor_data = spark.createDataFrame(case_sponsor_data)
spark_appellant_data = spark.createDataFrame(appellant_data)
spark_detention_centre_data = spark.createDataFrame(detention_centre_data)
spark_hearing_type_data = spark.createDataFrame(hearing_type_data)
spark_embassy_data = spark.createDataFrame(embassy_data)
spark_port_data = spark.createDataFrame(port_data)
spark_payment_remission_reason_data = spark.createDataFrame(payment_remission_reason_data)
spark_hearing_point_history_data = spark.createDataFrame(hearing_point_history_data)
spark_hearing_point_change_reason_data = spark.createDataFrame(hearing_point_change_reason_data)
spark_hearing_point_data = spark.createDataFrame(hearing_point_data)
spark_history_data = spark.createDataFrame(prepare_for_spark(history_data))
spark_case_list_data = spark.createDataFrame(case_list_data)

# Generate timestamp for unique file naming
datesnap = spark.sql("select date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

# Appeal Case
appeal_case_temp_output_path = f"/mnt/landing/test/AppealCase/temp_{datesnap}"
for column in spark_appeal_case_data.columns:
    if spark_appeal_case_data.schema[column].dataType.typeName() in ["void", "binary"]:
        spark_appeal_case_data = spark_appeal_case_data.withColumn(
            column,
            F.when(F.col(column).isNull(), None)
            .otherwise(F.col(column).cast("string"))
        )  
spark_appeal_case_data.coalesce(1).write.format("parquet").mode("overwrite").save(appeal_case_temp_output_path)

appeal_case_files = dbutils.fs.ls(appeal_case_temp_output_path)
appeal_case_parquet_file = [file.path for file in appeal_case_files if file.path.endswith(".parquet")][0]

appeal_case_final_output_path = f"/mnt/landing/test/AppealCase/full/SQLServer_TD_dbo_appeal_case_{datesnap}.parquet"
dbutils.fs.mv(appeal_case_parquet_file, appeal_case_final_output_path)
dbutils.fs.rm(appeal_case_temp_output_path, True)

# Case Respondent
case_respondent_temp_output_path = f"/mnt/landing/test/CaseRespondent/temp_{datesnap}"
spark_case_respondent_data.coalesce(1).write.format("parquet").mode("overwrite").save(case_respondent_temp_output_path)

case_respondent_files = dbutils.fs.ls(case_respondent_temp_output_path)
case_respondent_parquet_file = [file.path for file in case_respondent_files if file.path.endswith(".parquet")][0]

case_respondent_final_output_path = f"/mnt/landing/test/CaseRespondent/full/SQLServer_TD_dbo_case_respondent_{datesnap}.parquet"
dbutils.fs.mv(case_respondent_parquet_file, case_respondent_final_output_path)
dbutils.fs.rm(case_respondent_temp_output_path, True)

# Main Respondent
main_respondent_temp_output_path = f"/mnt/landing/test/MainRespondent/temp_{datesnap}"
spark_main_respondent_data.coalesce(1).write.format("parquet").mode("overwrite").save(main_respondent_temp_output_path)

main_respondent_files = dbutils.fs.ls(main_respondent_temp_output_path)
main_respondent_parquet_file = [file.path for file in main_respondent_files if file.path.endswith(".parquet")][0]

main_respondent_final_output_path = f"/mnt/landing/test/MainRespondent/full/SQLServer_TD_dbo_main_respondent_{datesnap}.parquet"
dbutils.fs.mv(main_respondent_parquet_file, main_respondent_final_output_path)
dbutils.fs.rm(main_respondent_temp_output_path, True)

# Respondent
respondent_temp_output_path = f"/mnt/landing/test/Respondent/temp_{datesnap}"
spark_respondent_data.coalesce(1).write.format("parquet").mode("overwrite").save(respondent_temp_output_path)

respondent_files = dbutils.fs.ls(respondent_temp_output_path)
respondent_parquet_file = [file.path for file in respondent_files if file.path.endswith(".parquet")][0]

respondent_final_output_path = f"/mnt/landing/test/Respondent/full/SQLServer_TD_dbo_respondent_{datesnap}.parquet"
dbutils.fs.mv(respondent_parquet_file, respondent_final_output_path)
dbutils.fs.rm(respondent_temp_output_path, True)

# File Location  
file_location_temp_output_path = f"/mnt/landing/test/FileLocation/temp_{datesnap}"
spark_file_location_data.coalesce(1).write.format("parquet").mode("overwrite").save(file_location_temp_output_path)

file_location_files = dbutils.fs.ls(file_location_temp_output_path)  
file_location_parquet_file = [file.path for file in file_location_files if file.path.endswith(".parquet")][0]

file_location_final_output_path = f"/mnt/landing/test/FileLocation/full/SQLServer_TD_dbo_file_location_{datesnap}.parquet" 
dbutils.fs.mv(file_location_parquet_file, file_location_final_output_path)
dbutils.fs.rm(file_location_temp_output_path, True)

# Representative
representative_temp_output_path = f"/mnt/landing/test/Representative/temp_{datesnap}"
spark_representative_data.coalesce(1).write.format("parquet").mode("overwrite").save(representative_temp_output_path)

representative_files = dbutils.fs.ls(representative_temp_output_path)
representative_parquet_file = [file.path for file in representative_files if file.path.endswith(".parquet")][0]

representative_final_output_path = f"/mnt/landing/test/Representative/full/SQLServer_TD_dbo_representative_{datesnap}.parquet"
dbutils.fs.mv(representative_parquet_file, representative_final_output_path)
dbutils.fs.rm(representative_temp_output_path, True)

# Case Representative
case_representative_temp_output_path = f"/mnt/landing/test/CaseRepresentative/temp_{datesnap}"
for column in spark_case_representative_data.columns:
    if spark_case_representative_data.schema[column].dataType.typeName() in ["void", "binary"]:
        spark_case_representative_data = spark_case_representative_data.withColumn(
            column,
            F.when(F.col(column).isNull(), None)
            .otherwise(F.col(column).cast("string"))
        )  
spark_case_representative_data.coalesce(1).write.format("parquet").mode("overwrite").save(case_representative_temp_output_path)

case_representative_files = dbutils.fs.ls(case_representative_temp_output_path)
case_representative_parquet_file = [file.path for file in case_representative_files if file.path.endswith(".parquet")][0]

case_representative_final_output_path = f"/mnt/landing/test/CaseRepresentative/full/SQLServer_TD_dbo_case_representative_{datesnap}.parquet"
dbutils.fs.mv(case_representative_parquet_file, case_representative_final_output_path)
dbutils.fs.rm(case_representative_temp_output_path, True)

# Case Surety
case_surety_temp_output_path = f"/mnt/landing/test/CaseSurety/temp_{datesnap}"
spark_case_surety_data.coalesce(1).write.format("parquet").mode("overwrite").save(case_surety_temp_output_path)

case_surety_files = dbutils.fs.ls(case_surety_temp_output_path)
case_surety_parquet_file = [file.path for file in case_surety_files if file.path.endswith(".parquet")][0]

case_surety_final_output_path = f"/mnt/landing/test/CaseSurety/full/SQLServer_TD_dbo_case_surety_{datesnap}.parquet"
dbutils.fs.mv(case_surety_parquet_file, case_surety_final_output_path)
dbutils.fs.rm(case_surety_temp_output_path, True)

# Cost Award
cost_award_temp_output_path = f"/mnt/landing/test/CostAward/temp_{datesnap}"
spark_cost_award_data.coalesce(1).write.format("parquet").mode("overwrite").save(cost_award_temp_output_path)

cost_award_files = dbutils.fs.ls(cost_award_temp_output_path)
cost_award_parquet_file = [file.path for file in cost_award_files if file.path.endswith(".parquet")][0]

cost_award_final_output_path = f"/mnt/landing/test/CostAward/full/SQLServer_TD_dbo_cost_award_{datesnap}.parquet"
dbutils.fs.mv(cost_award_parquet_file, cost_award_final_output_path)
dbutils.fs.rm(cost_award_temp_output_path, True)

# Category
category_temp_output_path = f"/mnt/landing/test/Category/temp_{datesnap}"
spark_category_data.coalesce(1).write.format("parquet").mode("overwrite").save(category_temp_output_path)

category_files = dbutils.fs.ls(category_temp_output_path)
category_parquet_file = [file.path for file in category_files if file.path.endswith(".parquet")][0]

category_final_output_path = f"/mnt/landing/test/Category/full/SQLServer_TD_dbo_category_{datesnap}.parquet"
dbutils.fs.mv(category_parquet_file, category_final_output_path)
dbutils.fs.rm(category_temp_output_path, True)

# Appeal Category
appeal_category_temp_output_path = f"/mnt/landing/test/AppealCategory/temp_{datesnap}"
spark_appeal_category_data.coalesce(1).write.format("parquet").mode("overwrite").save(appeal_category_temp_output_path)

appeal_category_files = dbutils.fs.ls(appeal_category_temp_output_path)
appeal_category_parquet_file = [file.path for file in appeal_category_files if file.path.endswith(".parquet")][0]

appeal_category_final_output_path = f"/mnt/landing/test/AppealCategory/full/SQLServer_TD_dbo_appeal_category_{datesnap}.parquet"
dbutils.fs.mv(appeal_category_parquet_file, appeal_category_final_output_path)
dbutils.fs.rm(appeal_category_temp_output_path, True)

# Link
link_temp_output_path = f"/mnt/landing/test/Link/temp_{datesnap}"
spark_link_data.coalesce(1).write.format("parquet").mode("overwrite").save(link_temp_output_path)

link_files = dbutils.fs.ls(link_temp_output_path)
link_parquet_file = [file.path for file in link_files if file.path.endswith(".parquet")][0]

link_final_output_path = f"/mnt/landing/test/Link/full/SQLServer_TD_dbo_link_{datesnap}.parquet"
dbutils.fs.mv(link_parquet_file, link_final_output_path)
dbutils.fs.rm(link_temp_output_path, True)

# Appeal Type
appeal_type_temp_output_path = f"/mnt/landing/test/AppealType/temp_{datesnap}"
spark_appeal_type_data.coalesce(1).write.format("parquet").mode("overwrite").save(appeal_type_temp_output_path)

appeal_type_files = dbutils.fs.ls(appeal_type_temp_output_path)
appeal_type_parquet_file = [file.path for file in appeal_type_files if file.path.endswith(".parquet")][0]

appeal_type_final_output_path = f"/mnt/landing/test/AppealType/full/SQLServer_TD_dbo_appeal_type_{datesnap}.parquet"
dbutils.fs.mv(appeal_type_parquet_file, appeal_type_final_output_path)
dbutils.fs.rm(appeal_type_temp_output_path, True)

# Appeal Grounds
appeal_grounds_temp_output_path = f"/mnt/landing/test/AppealGrounds/temp_{datesnap}"
spark_appeal_grounds_data.coalesce(1).write.format("parquet").mode("overwrite").save(appeal_grounds_temp_output_path)

appeal_grounds_files = dbutils.fs.ls(appeal_grounds_temp_output_path)
appeal_grounds_parquet_file = [file.path for file in appeal_grounds_files if file.path.endswith(".parquet")][0]

appeal_grounds_final_output_path = f"/mnt/landing/test/AppealGrounds/full/SQLServer_TD_dbo_appeal_grounds_{datesnap}.parquet"
dbutils.fs.mv(appeal_grounds_parquet_file, appeal_grounds_final_output_path)
dbutils.fs.rm(appeal_grounds_temp_output_path, True)

# Language
language_temp_output_path = f"/mnt/landing/test/Language/temp_{datesnap}"
spark_language_data.coalesce(1).write.format("parquet").mode("overwrite").save(language_temp_output_path)

language_files = dbutils.fs.ls(language_temp_output_path)
language_parquet_file = [file.path for file in language_files if file.path.endswith(".parquet")][0]

language_final_output_path = f"/mnt/landing/test/Language/full/SQLServer_TD_dbo_language_{datesnap}.parquet"
dbutils.fs.mv(language_parquet_file, language_final_output_path)
dbutils.fs.rm(language_temp_output_path, True)

# Reason Adjourn
reason_adjourn_temp_output_path = f"/mnt/landing/test/ReasonAdjourn/temp_{datesnap}"
spark_reason_adjourn_data.coalesce(1).write.format("parquet").mode("overwrite").save(reason_adjourn_temp_output_path)

reason_adjourn_files = dbutils.fs.ls(reason_adjourn_temp_output_path)
reason_adjourn_parquet_file = [file.path for file in reason_adjourn_files if file.path.endswith(".parquet")][0]

reason_adjourn_final_output_path = f"/mnt/landing/test/ReasonAdjourn/full/SQLServer_TD_dbo_reason_adjourn_{datesnap}.parquet"
dbutils.fs.mv(reason_adjourn_parquet_file, reason_adjourn_final_output_path)
dbutils.fs.rm(reason_adjourn_temp_output_path, True)

# Case Status
case_status_temp_output_path = f"/mnt/landing/test/CaseStatus/temp_{datesnap}"
spark_case_status_data.coalesce(1).write.format("parquet").mode("overwrite").save(case_status_temp_output_path)

case_status_files = dbutils.fs.ls(case_status_temp_output_path)
case_status_parquet_file = [file.path for file in case_status_files if file.path.endswith(".parquet")][0]

case_status_final_output_path = f"/mnt/landing/test/CaseStatus/full/SQLServer_TD_dbo_case_status_{datesnap}.parquet"
dbutils.fs.mv(case_status_parquet_file, case_status_final_output_path)
dbutils.fs.rm(case_status_temp_output_path, True)

# Decision Type
decision_type_temp_output_path = f"/mnt/landing/test/DecisionType/temp_{datesnap}"
spark_decision_type_data.coalesce(1).write.format("parquet").mode("overwrite").save(decision_type_temp_output_path)

decision_type_files = dbutils.fs.ls(decision_type_temp_output_path)
decision_type_parquet_file = [file.path for file in decision_type_files if file.path.endswith(".parquet")][0]

decision_type_final_output_path = f"/mnt/landing/test/DecisionType/full/SQLServer_TD_dbo_decision_type_{datesnap}.parquet"
dbutils.fs.mv(decision_type_parquet_file, decision_type_final_output_path)
dbutils.fs.rm(decision_type_temp_output_path, True)

# Status
status_temp_output_path = f"/mnt/landing/test/Status/temp_{datesnap}"  
spark_status_data.coalesce(1).write.format("parquet").mode("overwrite").save(status_temp_output_path)

status_files = dbutils.fs.ls(status_temp_output_path)
status_parquet_file = [file.path for file in status_files if file.path.endswith(".parquet")][0]

status_final_output_path = f"/mnt/landing/test/Status/full/SQLServer_TD_dbo_status_{datesnap}.parquet"
dbutils.fs.mv(status_parquet_file, status_final_output_path)
dbutils.fs.rm(status_temp_output_path, True)

# Received Document
received_document_temp_output_path = f"/mnt/landing/test/ReceivedDocument/temp_{datesnap}"
spark_received_document_data.coalesce(1).write.format("parquet").mode("overwrite").save(received_document_temp_output_path)

received_document_files = dbutils.fs.ls(received_document_temp_output_path)
received_document_parquet_file = [file.path for file in received_document_files if file.path.endswith(".parquet")][0]

received_document_final_output_path = f"/mnt/landing/test/ReceivedDocument/full/SQLServer_TD_dbo_received_document_{datesnap}.parquet"
dbutils.fs.mv(received_document_parquet_file, received_document_final_output_path)
dbutils.fs.rm(received_document_temp_output_path, True)

# Document Received
document_received_temp_output_path = f"/mnt/landing/test/DocumentReceived/temp_{datesnap}"
spark_document_received_data.coalesce(1).write.format("parquet").mode("overwrite").save(document_received_temp_output_path)

document_received_files = dbutils.fs.ls(document_received_temp_output_path)
document_received_parquet_file = [file.path for file in document_received_files if file.path.endswith(".parquet")][0]

document_received_final_output_path = f"/mnt/landing/test/DocumentReceived/full/SQLServer_TD_dbo_document_received_{datesnap}.parquet"
dbutils.fs.mv(document_received_parquet_file, document_received_final_output_path)
dbutils.fs.rm(document_received_temp_output_path, True)

# New Matter
new_matter_temp_output_path = f"/mnt/landing/test/NewMatter/temp_{datesnap}"
spark_new_matter_data.coalesce(1).write.format("parquet").mode("overwrite").save(new_matter_temp_output_path)

new_matter_files = dbutils.fs.ls(new_matter_temp_output_path)
new_matter_parquet_file = [file.path for file in new_matter_files if file.path.endswith(".parquet")][0]

new_matter_final_output_path = f"/mnt/landing/test/NewMatter/full/SQLServer_TD_dbo_new_matter_{datesnap}.parquet"
dbutils.fs.mv(new_matter_parquet_file, new_matter_final_output_path)
dbutils.fs.rm(new_matter_temp_output_path, True)

# Appeal New Matter
appeal_new_matter_temp_output_path = f"/mnt/landing/test/AppealNewMatter/temp_{datesnap}"
spark_appeal_new_matter_data.coalesce(1).write.format("parquet").mode("overwrite").save(appeal_new_matter_temp_output_path)

appeal_new_matter_files = dbutils.fs.ls(appeal_new_matter_temp_output_path)
appeal_new_matter_parquet_file = [file.path for file in appeal_new_matter_files if file.path.endswith(".parquet")][0]

appeal_new_matter_final_output_path = f"/mnt/landing/test/AppealNewMatter/full/SQLServer_TD_dbo_appeal_new_matter_{datesnap}.parquet"
dbutils.fs.mv(appeal_new_matter_parquet_file, appeal_new_matter_final_output_path)
dbutils.fs.rm(appeal_new_matter_temp_output_path, True)

# Hearing Centre
hearing_centre_temp_output_path = f"/mnt/landing/test/HearingCentre/temp_{datesnap}"
spark_hearing_centre_data.coalesce(1).write.format("parquet").mode("overwrite").save(hearing_centre_temp_output_path)

hearing_centre_files = dbutils.fs.ls(hearing_centre_temp_output_path)
hearing_centre_parquet_file = [file.path for file in hearing_centre_files if file.path.endswith(".parquet")][0]

hearing_centre_final_output_path = f"/mnt/landing/test/HearingCentre/full/SQLServer_TD_dbo_hearing_centre_{datesnap}.parquet"
dbutils.fs.mv(hearing_centre_parquet_file, hearing_centre_final_output_path)
dbutils.fs.rm(hearing_centre_temp_output_path, True)

# Standard Direction
standard_direction_temp_output_path = f"/mnt/landing/test/StandardDirection/temp_{datesnap}"
spark_standard_direction_data.coalesce(1).write.format("parquet").mode("overwrite").save(standard_direction_temp_output_path)

standard_direction_files = dbutils.fs.ls(standard_direction_temp_output_path)
standard_direction_parquet_file = [file.path for file in standard_direction_files if file.path.endswith(".parquet")][0]

standard_direction_final_output_path = f"/mnt/landing/test/StandardDirection/full/SQLServer_TD_dbo_standard_direction_{datesnap}.parquet"
dbutils.fs.mv(standard_direction_parquet_file, standard_direction_final_output_path)
dbutils.fs.rm(standard_direction_temp_output_path, True)

# Review Standard Direction
review_standard_direction_temp_output_path = f"/mnt/landing/test/ReviewStandardDirection/temp_{datesnap}"
spark_review_standard_direction_data.coalesce(1).write.format("parquet").mode("overwrite").save(review_standard_direction_temp_output_path)

review_standard_direction_files = dbutils.fs.ls(review_standard_direction_temp_output_path)
review_standard_direction_parquet_file = [file.path for file in review_standard_direction_files if file.path.endswith(".parquet")][0]

review_standard_direction_final_output_path = f"/mnt/landing/test/ReviewStandardDirection/full/SQLServer_TD_dbo_review_standard_direction_{datesnap}.parquet"
dbutils.fs.mv(review_standard_direction_parquet_file, review_standard_direction_final_output_path)
dbutils.fs.rm(review_standard_direction_temp_output_path, True)

# Review Specific Direction
review_specific_direction_temp_output_path = f"/mnt/landing/test/ReviewSpecificDirection/temp_{datesnap}"
spark_review_specific_direction_data.coalesce(1).write.format("parquet").mode("overwrite").save(review_specific_direction_temp_output_path)

review_specific_direction_files = dbutils.fs.ls(review_specific_direction_temp_output_path)
review_specific_direction_parquet_file = [file.path for file in review_specific_direction_files if file.path.endswith(".parquet")][0]

review_specific_direction_final_output_path = f"/mnt/landing/test/ReviewSpecificDirection/full/SQLServer_TD_dbo_review_specific_direction_{datesnap}.parquet"
dbutils.fs.mv(review_specific_direction_parquet_file, review_specific_direction_final_output_path)
dbutils.fs.rm(review_specific_direction_temp_output_path, True)

# Appeal Type Category
appeal_type_category_temp_output_path = f"/mnt/landing/test/AppealTypeCategory/temp_{datesnap}"
spark_appeal_type_category_data.coalesce(1).write.format("parquet").mode("overwrite").save(appeal_type_category_temp_output_path)

appeal_type_category_files = dbutils.fs.ls(appeal_type_category_temp_output_path)
appeal_type_category_parquet_file = [file.path for file in appeal_type_category_files if file.path.endswith(".parquet")][0]

appeal_type_category_final_output_path = f"/mnt/landing/test/AppealTypeCategory/full/SQLServer_TD_dbo_appeal_type_category_{datesnap}.parquet"
dbutils.fs.mv(appeal_type_category_parquet_file, appeal_type_category_final_output_path)
dbutils.fs.rm(appeal_type_category_temp_output_path, True)

# Reason Link
reason_link_temp_output_path = f"/mnt/landing/test/ReasonLink/temp_{datesnap}"
spark_reason_link_data.coalesce(1).write.format("parquet").mode("overwrite").save(reason_link_temp_output_path)

reason_link_files = dbutils.fs.ls(reason_link_temp_output_path)
reason_link_parquet_file = [file.path for file in reason_link_files if file.path.endswith(".parquet")][0]

reason_link_final_output_path = f"/mnt/landing/test/ReasonLink/full/SQLServer_TD_dbo_reason_link_{datesnap}.parquet"
dbutils.fs.mv(reason_link_parquet_file, reason_link_final_output_path)
dbutils.fs.rm(reason_link_temp_output_path, True)

# Link Detail
link_detail_temp_output_path = f"/mnt/landing/test/LinkDetail/temp_{datesnap}"
spark_link_detail_data.coalesce(1).write.format("parquet").mode("overwrite").save(link_detail_temp_output_path)

link_detail_files = dbutils.fs.ls(link_detail_temp_output_path)
link_detail_parquet_file = [file.path for file in link_detail_files if file.path.endswith(".parquet")][0]

link_detail_final_output_path = f"/mnt/landing/test/LinkDetail/full/SQLServer_TD_dbo_link_detail_{datesnap}.parquet"
dbutils.fs.mv(link_detail_parquet_file, link_detail_final_output_path)
dbutils.fs.rm(link_detail_temp_output_path, True)

# Department
dept_temp_output_path = f"/mnt/landing/test/Department/temp_{datesnap}"
spark_dept_data.coalesce(1).write.format("parquet").mode("overwrite").save(dept_temp_output_path)

dept_files = dbutils.fs.ls(dept_temp_output_path)
dept_parquet_file = [file.path for file in dept_files if file.path.endswith(".parquet")][0]

dept_final_output_path = f"/mnt/landing/test/Department/full/SQLServer_TD_dbo_department_{datesnap}.parquet"
dbutils.fs.mv(dept_parquet_file, dept_final_output_path)
dbutils.fs.rm(dept_temp_output_path, True)

# User
user_temp_output_path = f"/mnt/landing/test/User/temp_{datesnap}"
spark_user_data.coalesce(1).write.format("parquet").mode("overwrite").save(user_temp_output_path)

user_files = dbutils.fs.ls(user_temp_output_path)
user_parquet_file = [file.path for file in user_files if file.path.endswith(".parquet")][0]

user_final_output_path = f"/mnt/landing/test/User/full/SQLServer_TD_dbo_user_{datesnap}.parquet"
dbutils.fs.mv(user_parquet_file, user_final_output_path)
dbutils.fs.rm(user_temp_output_path, True)

# Transaction Method
transaction_method_temp_output_path = f"/mnt/landing/test/TransactionMethod/temp_{datesnap}"
spark_transaction_method_data.coalesce(1).write.format("parquet").mode("overwrite").save(transaction_method_temp_output_path)

transaction_method_files = dbutils.fs.ls(transaction_method_temp_output_path)
transaction_method_parquet_file = [file.path for file in transaction_method_files if file.path.endswith(".parquet")][0]

transaction_method_final_output_path = f"/mnt/landing/test/TransactionMethod/full/SQLServer_TD_dbo_transaction_method_{datesnap}.parquet"
dbutils.fs.mv(transaction_method_parquet_file, transaction_method_final_output_path)
dbutils.fs.rm(transaction_method_temp_output_path, True)

# Transaction Type  
transaction_type_temp_output_path = f"/mnt/landing/test/TransactionType/temp_{datesnap}"
spark_transaction_type_data.coalesce(1).write.format("parquet").mode("overwrite").save(transaction_type_temp_output_path)

transaction_type_files = dbutils.fs.ls(transaction_type_temp_output_path)
transaction_type_parquet_file = [file.path for file in transaction_type_files if file.path.endswith(".parquet")][0]

transaction_type_final_output_path = f"/mnt/landing/test/TransactionType/full/SQLServer_TD_dbo_transaction_type_{datesnap}.parquet"
dbutils.fs.mv(transaction_type_parquet_file, transaction_type_final_output_path)
dbutils.fs.rm(transaction_type_temp_output_path, True)

# Transaction Status
transaction_status_temp_output_path = f"/mnt/landing/test/TransactionStatus/temp_{datesnap}"  
spark_transaction_status_data.coalesce(1).write.format("parquet").mode("overwrite").save(transaction_status_temp_output_path)

transaction_status_files = dbutils.fs.ls(transaction_status_temp_output_path)
transaction_status_parquet_file = [file.path for file in transaction_status_files if file.path.endswith(".parquet")][0]

transaction_status_final_output_path = f"/mnt/landing/test/TransactionStatus/full/SQLServer_TD_dbo_transaction_status_{datesnap}.parquet"
dbutils.fs.mv(transaction_status_parquet_file, transaction_status_final_output_path) 
dbutils.fs.rm(transaction_status_temp_output_path, True)

# Transaction
transaction_temp_output_path = f"/mnt/landing/test/Transaction/temp_{datesnap}"
spark_transaction_data.coalesce(1).write.format("parquet").mode("overwrite").save(transaction_temp_output_path)

transaction_files = dbutils.fs.ls(transaction_temp_output_path)  
transaction_parquet_file = [file.path for file in transaction_files if file.path.endswith(".parquet")][0]

transaction_final_output_path = f"/mnt/landing/test/Transaction/full/SQLServer_TD_dbo_transaction_{datesnap}.parquet"
dbutils.fs.mv(transaction_parquet_file, transaction_final_output_path)
dbutils.fs.rm(transaction_temp_output_path, True)

# Cost Order
cost_order_temp_output_path = f"/mnt/landing/test/CostOrder/temp_{datesnap}"
spark_cost_order_data.coalesce(1).write.format("parquet").mode("overwrite").save(cost_order_temp_output_path)

cost_order_files = dbutils.fs.ls(cost_order_temp_output_path)
cost_order_parquet_file = [file.path for file in cost_order_files if file.path.endswith(".parquet")][0]

cost_order_final_output_path = f"/mnt/landing/test/CostOrder/full/SQLServer_TD_dbo_cost_order_{datesnap}.parquet"
dbutils.fs.mv(cost_order_parquet_file, cost_order_final_output_path)
dbutils.fs.rm(cost_order_temp_output_path, True)

# List Type
list_type_temp_output_path = f"/mnt/landing/test/ListType/temp_{datesnap}"
spark_list_type_data.coalesce(1).write.format("parquet").mode("overwrite").save(list_type_temp_output_path)

list_type_files = dbutils.fs.ls(list_type_temp_output_path)
list_type_parquet_file = [file.path for file in list_type_files if file.path.endswith(".parquet")][0]

list_type_final_output_path = f"/mnt/landing/test/ListType/full/SQLServer_TD_dbo_list_type_{datesnap}.parquet"
dbutils.fs.mv(list_type_parquet_file, list_type_final_output_path)
dbutils.fs.rm(list_type_temp_output_path, True)

# Adjudicator
adjudicator_temp_output_path = f"/mnt/landing/test/Adjudicator/temp_{datesnap}"
spark_adjudicator_data.coalesce(1).write.format("parquet").mode("overwrite").save(adjudicator_temp_output_path)

adjudicator_files = dbutils.fs.ls(adjudicator_temp_output_path)
adjudicator_parquet_file = [file.path for file in adjudicator_files if file.path.endswith(".parquet")][0]

adjudicator_final_output_path = f"/mnt/landing/test/Adjudicator/full/SQLServer_TD_dbo_adjudicator_{datesnap}.parquet"
dbutils.fs.mv(adjudicator_parquet_file, adjudicator_final_output_path)
dbutils.fs.rm(adjudicator_temp_output_path, True)

# List Sitting
list_sitting_temp_output_path = f"/mnt/landing/test/ListSitting/temp_{datesnap}"
spark_list_sitting_data.coalesce(1).write.format("parquet").mode("overwrite").save(list_sitting_temp_output_path)

list_sitting_files = dbutils.fs.ls(list_sitting_temp_output_path)
list_sitting_parquet_file = [file.path for file in list_sitting_files if file.path.endswith(".parquet")][0]

list_sitting_final_output_path = f"/mnt/landing/test/ListSitting/full/SQLServer_TD_dbo_list_sitting_{datesnap}.parquet"
dbutils.fs.mv(list_sitting_parquet_file, list_sitting_final_output_path)
dbutils.fs.rm(list_sitting_temp_output_path, True)

# Court
court_temp_output_path = f"/mnt/landing/test/Court/temp_{datesnap}"
spark_court_data.coalesce(1).write.format("parquet").mode("overwrite").save(court_temp_output_path)

court_files = dbutils.fs.ls(court_temp_output_path)
court_parquet_file = [file.path for file in court_files if file.path.endswith(".parquet")][0]

court_final_output_path = f"/mnt/landing/test/Court/full/SQLServer_TD_dbo_court_{datesnap}.parquet"
dbutils.fs.mv(court_parquet_file, court_final_output_path)
dbutils.fs.rm(court_temp_output_path, True)

# List
list_temp_output_path = f"/mnt/landing/test/List/temp_{datesnap}"
spark_list_data.coalesce(1).write.format("parquet").mode("overwrite").save(list_temp_output_path)

list_files = dbutils.fs.ls(list_temp_output_path)
list_parquet_file = [file.path for file in list_files if file.path.endswith(".parquet")][0]

list_final_output_path = f"/mnt/landing/test/List/full/SQLServer_TD_dbo_list_{datesnap}.parquet"
dbutils.fs.mv(list_parquet_file, list_final_output_path)
dbutils.fs.rm(list_temp_output_path, True)

# Documents Received
documents_received_temp_output_path = f"/mnt/landing/test/DocumentsReceived/temp_{datesnap}"
spark_documents_received_data.coalesce(1).write.format("parquet").mode("overwrite").save(documents_received_temp_output_path)

documents_received_files = dbutils.fs.ls(documents_received_temp_output_path)
documents_received_parquet_file = [file.path for file in documents_received_files if file.path.endswith(".parquet")][0]

documents_received_final_output_path = f"/mnt/landing/test/DocumentsReceived/full/SQLServer_TD_dbo_documents_received_{datesnap}.parquet"
dbutils.fs.mv(documents_received_parquet_file, documents_received_final_output_path)
dbutils.fs.rm(documents_received_temp_output_path, True)

# Case Fee Summary
case_fee_summary_temp_output_path = f"/mnt/landing/test/CaseFeeSummary/temp_{datesnap}"
spark_case_fee_summary_data.coalesce(1).write.format("parquet").mode("overwrite").save(case_fee_summary_temp_output_path)

case_fee_summary_files = dbutils.fs.ls(case_fee_summary_temp_output_path)
case_fee_summary_parquet_file = [file.path for file in case_fee_summary_files if file.path.endswith(".parquet")][0]

case_fee_summary_final_output_path = f"/mnt/landing/test/CaseFeeSummary/full/SQLServer_TD_dbo_case_fee_summary_{datesnap}.parquet"
dbutils.fs.mv(case_fee_summary_parquet_file, case_fee_summary_final_output_path)
dbutils.fs.rm(case_fee_summary_temp_output_path, True)

# Human Right
human_right_temp_output_path = f"/mnt/landing/test/HumanRight/temp_{datesnap}"
spark_human_right_data.coalesce(1).write.format("parquet").mode("overwrite").save(human_right_temp_output_path)

human_right_files = dbutils.fs.ls(human_right_temp_output_path)
human_right_parquet_file = [file.path for file in human_right_files if file.path.endswith(".parquet")][0]

human_right_final_output_path = f"/mnt/landing/test/HumanRight/full/SQLServer_TD_dbo_human_right_{datesnap}.parquet"
dbutils.fs.mv(human_right_parquet_file, human_right_final_output_path)
dbutils.fs.rm(human_right_temp_output_path, True)

# Case Appellant
case_appellant_temp_output_path = f"/mnt/landing/test/CaseAppellant/temp_{datesnap}"
spark_case_appellant_data.coalesce(1).write.format("parquet").mode("overwrite").save(case_appellant_temp_output_path)

case_appellant_files = dbutils.fs.ls(case_appellant_temp_output_path)
case_appellant_parquet_file = [file.path for file in case_appellant_files if file.path.endswith(".parquet")][0]

case_appellant_final_output_path = f"/mnt/landing/test/CaseAppellant/full/SQLServer_TD_dbo_case_appellant_{datesnap}.parquet"
dbutils.fs.mv(case_appellant_parquet_file, case_appellant_final_output_path)
dbutils.fs.rm(case_appellant_temp_output_path, True)

# BF Type
bf_type_temp_output_path = f"/mnt/landing/test/BFType/temp_{datesnap}"
spark_bf_type_data.coalesce(1).write.format("parquet").mode("overwrite").save(bf_type_temp_output_path)

bf_type_files = dbutils.fs.ls(bf_type_temp_output_path)
bf_type_parquet_file = [file.path for file in bf_type_files if file.path.endswith(".parquet")][0]

bf_type_final_output_path = f"/mnt/landing/test/BFType/full/SQLServer_TD_dbo_bf_type_{datesnap}.parquet"
dbutils.fs.mv(bf_type_parquet_file, bf_type_final_output_path)
dbutils.fs.rm(bf_type_temp_output_path, True)

# BF Diary
bf_diary_temp_output_path = f"/mnt/landing/test/BFDiary/temp_{datesnap}"
spark_bf_diary_data.coalesce(1).write.format("parquet").mode("overwrite").save(bf_diary_temp_output_path)

bf_diary_files = dbutils.fs.ls(bf_diary_temp_output_path)
bf_diary_parquet_file = [file.path for file in bf_diary_files if file.path.endswith(".parquet")][0]

bf_diary_final_output_path = f"/mnt/landing/test/BFDiary/full/SQLServer_TD_dbo_bf_diary_{datesnap}.parquet"
dbutils.fs.mv(bf_diary_parquet_file, bf_diary_final_output_path)
dbutils.fs.rm(bf_diary_temp_output_path, True)

# Appeal Human Right
appeal_human_right_temp_output_path = f"/mnt/landing/test/AppealHumanRight/temp_{datesnap}"
spark_appeal_human_right_data.coalesce(1).write.format("parquet").mode("overwrite").save(appeal_human_right_temp_output_path)

appeal_human_right_files = dbutils.fs.ls(appeal_human_right_temp_output_path)
appeal_human_right_parquet_file = [file.path for file in appeal_human_right_files if file.path.endswith(".parquet")][0]

appeal_human_right_final_output_path = f"/mnt/landing/test/AppealHumanRight/full/SQLServer_TD_dbo_appeal_human_right_{datesnap}.parquet"
dbutils.fs.mv(appeal_human_right_parquet_file, appeal_human_right_final_output_path)
dbutils.fs.rm(appeal_human_right_temp_output_path, True)

# Country
country_temp_output_path = f"/mnt/landing/test/Country/temp_{datesnap}"
spark_country_data.coalesce(1).write.format("parquet").mode("overwrite").save(country_temp_output_path)

country_files = dbutils.fs.ls(country_temp_output_path)
country_parquet_file = [file.path for file in country_files if file.path.endswith(".parquet")][0]

country_final_output_path = f"/mnt/landing/test/Country/full/SQLServer_TD_dbo_country_{datesnap}.parquet"
dbutils.fs.mv(country_parquet_file, country_final_output_path)
dbutils.fs.rm(country_temp_output_path, True)

# Status Contact
status_contact_temp_output_path = f"/mnt/landing/test/StatusContact/temp_{datesnap}"
spark_status_contact_data.coalesce(1).write.format("parquet").mode("overwrite").save(status_contact_temp_output_path)

status_contact_files = dbutils.fs.ls(status_contact_temp_output_path)
status_contact_parquet_file = [file.path for file in status_contact_files if file.path.endswith(".parquet")][0]

status_contact_final_output_path = f"/mnt/landing/test/StatusContact/full/SQLServer_TD_dbo_status_contact_{datesnap}.parquet"
dbutils.fs.mv(status_contact_parquet_file, status_contact_final_output_path)
dbutils.fs.rm(status_contact_temp_output_path, True)

# Fee Satisfaction
fee_satisfaction_temp_output_path = f"/mnt/landing/test/FeeSatisfaction/temp_{datesnap}"
spark_fee_satisfaction_data.coalesce(1).write.format("parquet").mode("overwrite").save(fee_satisfaction_temp_output_path)

fee_satisfaction_files = dbutils.fs.ls(fee_satisfaction_temp_output_path)
fee_satisfaction_parquet_file = [file.path for file in fee_satisfaction_files if file.path.endswith(".parquet")][0]

fee_satisfaction_final_output_path = f"/mnt/landing/test/FeeSatisfaction/full/SQLServer_TD_dbo_fee_satisfaction_{datesnap}.parquet"
dbutils.fs.mv(fee_satisfaction_parquet_file, fee_satisfaction_final_output_path)
dbutils.fs.rm(fee_satisfaction_temp_output_path, True)

# Case Sponsor
case_sponsor_temp_output_path = f"/mnt/landing/test/CaseSponsor/temp_{datesnap}"
spark_case_sponsor_data.coalesce(1).write.format("parquet").mode("overwrite").save(case_sponsor_temp_output_path)

case_sponsor_files = dbutils.fs.ls(case_sponsor_temp_output_path)
case_sponsor_parquet_file = [file.path for file in case_sponsor_files if file.path.endswith(".parquet")][0]

case_sponsor_final_output_path = f"/mnt/landing/test/CaseSponsor/full/SQLServer_TD_dbo_case_sponsor_{datesnap}.parquet"
dbutils.fs.mv(case_sponsor_parquet_file, case_sponsor_final_output_path)
dbutils.fs.rm(case_sponsor_temp_output_path, True)

# Appellant
appellant_temp_output_path = f"/mnt/landing/test/Appellant/temp_{datesnap}"
spark_appellant_data.coalesce(1).write.format("parquet").mode("overwrite").save(appellant_temp_output_path)

appellant_files = dbutils.fs.ls(appellant_temp_output_path)
appellant_parquet_file = [file.path for file in appellant_files if file.path.endswith(".parquet")][0]

appellant_final_output_path = f"/mnt/landing/test/Appellant/full/SQLServer_TD_dbo_appellant_{datesnap}.parquet"
dbutils.fs.mv(appellant_parquet_file, appellant_final_output_path)
dbutils.fs.rm(appellant_temp_output_path, True)

# Detention Centre
detention_centre_temp_output_path = f"/mnt/landing/test/DetentionCentre/temp_{datesnap}"
spark_detention_centre_data.coalesce(1).write.format("parquet").mode("overwrite").save(detention_centre_temp_output_path)

detention_centre_files = dbutils.fs.ls(detention_centre_temp_output_path)
detention_centre_parquet_file = [file.path for file in detention_centre_files if file.path.endswith(".parquet")][0]

detention_centre_final_output_path = f"/mnt/landing/test/DetentionCentre/full/SQLServer_TD_dbo_detention_centre_{datesnap}.parquet"
dbutils.fs.mv(detention_centre_parquet_file, detention_centre_final_output_path)
dbutils.fs.rm(detention_centre_temp_output_path, True)

# Hearing Type
hearing_type_temp_output_path = f"/mnt/landing/test/HearingType/temp_{datesnap}"
spark_hearing_type_data.coalesce(1).write.format("parquet").mode("overwrite").save(hearing_type_temp_output_path)

hearing_type_files = dbutils.fs.ls(hearing_type_temp_output_path)
hearing_type_parquet_file = [file.path for file in hearing_type_files if file.path.endswith(".parquet")][0]

hearing_type_final_output_path = f"/mnt/landing/test/HearingType/full/SQLServer_TD_dbo_hearing_type_{datesnap}.parquet"
dbutils.fs.mv(hearing_type_parquet_file, hearing_type_final_output_path)
dbutils.fs.rm(hearing_type_temp_output_path, True)

# Embassy
embassy_temp_output_path = f"/mnt/landing/test/Embassy/temp_{datesnap}"
spark_embassy_data.coalesce(1).write.format("parquet").mode("overwrite").save(embassy_temp_output_path)

embassy_files = dbutils.fs.ls(embassy_temp_output_path)
embassy_parquet_file = [file.path for file in embassy_files if file.path.endswith(".parquet")][0]

embassy_final_output_path = f"/mnt/landing/test/Embassy/full/SQLServer_TD_dbo_embassy_{datesnap}.parquet"
dbutils.fs.mv(embassy_parquet_file, embassy_final_output_path)
dbutils.fs.rm(embassy_temp_output_path, True)

# Port
port_temp_output_path = f"/mnt/landing/test/Port/temp_{datesnap}"
spark_port_data.coalesce(1).write.format("parquet").mode("overwrite").save(port_temp_output_path)

port_files = dbutils.fs.ls(port_temp_output_path)
port_parquet_file = [file.path for file in port_files if file.path.endswith(".parquet")][0]

port_final_output_path = f"/mnt/landing/test/Port/full/SQLServer_TD_dbo_port_{datesnap}.parquet"
dbutils.fs.mv(port_parquet_file, port_final_output_path)
dbutils.fs.rm(port_temp_output_path, True)

# Payment Remission Reason
payment_remission_reason_temp_output_path = f"/mnt/landing/test/PaymentRemissionReason/temp_{datesnap}"
spark_payment_remission_reason_data.coalesce(1).write.format("parquet").mode("overwrite").save(payment_remission_reason_temp_output_path)

payment_remission_reason_files = dbutils.fs.ls(payment_remission_reason_temp_output_path)
payment_remission_reason_parquet_file = [file.path for file in payment_remission_reason_files if file.path.endswith(".parquet")][0]

payment_remission_reason_final_output_path = f"/mnt/landing/test/PaymentRemissionReason/full/SQLServer_TD_dbo_payment_remission_reason_{datesnap}.parquet"
dbutils.fs.mv(payment_remission_reason_parquet_file, payment_remission_reason_final_output_path)
dbutils.fs.rm(payment_remission_reason_temp_output_path, True)

# Hearing Point History
hearing_point_history_temp_output_path = f"/mnt/landing/test/HearingPointHistory/temp_{datesnap}"
spark_hearing_point_history_data.coalesce(1).write.format("parquet").mode("overwrite").save(hearing_point_history_temp_output_path)

hearing_point_history_files = dbutils.fs.ls(hearing_point_history_temp_output_path)
hearing_point_history_parquet_file = [file.path for file in hearing_point_history_files if file.path.endswith(".parquet")][0]

hearing_point_history_final_output_path = f"/mnt/landing/test/HearingPointHistory/full/SQLServer_TD_dbo_hearing_point_history_{datesnap}.parquet"
dbutils.fs.mv(hearing_point_history_parquet_file, hearing_point_history_final_output_path)
dbutils.fs.rm(hearing_point_history_temp_output_path, True)

# Hearing Point Change Reason
hearing_point_change_reason_temp_output_path = f"/mnt/landing/test/HearingPointChangeReason/temp_{datesnap}"
spark_hearing_point_change_reason_data.coalesce(1).write.format("parquet").mode("overwrite").save(hearing_point_change_reason_temp_output_path)

hearing_point_change_reason_files = dbutils.fs.ls(hearing_point_change_reason_temp_output_path)
hearing_point_change_reason_parquet_file = [file.path for file in hearing_point_change_reason_files if file.path.endswith(".parquet")][0]

hearing_point_change_reason_final_output_path = f"/mnt/landing/test/HearingPointChangeReason/full/SQLServer_TD_dbo_hearing_point_change_reason_{datesnap}.parquet"
dbutils.fs.mv(hearing_point_change_reason_parquet_file, hearing_point_change_reason_final_output_path)
dbutils.fs.rm(hearing_point_change_reason_temp_output_path, True)

# Hearing Point
hearing_point_temp_output_path = f"/mnt/landing/test/HearingPoint/temp_{datesnap}"
spark_hearing_point_data.coalesce(1).write.format("parquet").mode("overwrite").save(hearing_point_temp_output_path)

hearing_point_files = dbutils.fs.ls(hearing_point_temp_output_path)
hearing_point_parquet_file = [file.path for file in hearing_point_files if file.path.endswith(".parquet")][0]

hearing_point_final_output_path = f"/mnt/landing/test/HearingPoint/full/SQLServer_TD_dbo_hearing_point_{datesnap}.parquet"
dbutils.fs.mv(hearing_point_parquet_file, hearing_point_final_output_path)
dbutils.fs.rm(hearing_point_temp_output_path, True)

# History
history_temp_output_path = f"/mnt/landing/test/History/temp_{datesnap}"
spark_history_data.coalesce(1).write.format("parquet").mode("overwrite").save(history_temp_output_path)

history_files = dbutils.fs.ls(history_temp_output_path)
history_parquet_file = [file.path for file in history_files if file.path.endswith(".parquet")][0]

history_final_output_path = f"/mnt/landing/test/History/full/SQLServer_TD_dbo_history_{datesnap}.parquet"
dbutils.fs.mv(history_parquet_file, history_final_output_path)
dbutils.fs.rm(history_temp_output_path, True)

# Case List
case_list_temp_output_path = f"/mnt/landing/test/CaseList/temp_{datesnap}"
spark_case_list_data.coalesce(1).write.format("parquet").mode("overwrite").save(case_list_temp_output_path)

case_list_files = dbutils.fs.ls(case_list_temp_output_path)
case_list_parquet_file = [file.path for file in case_list_files if file.path.endswith(".parquet")][0]

case_list_final_output_path = f"/mnt/landing/test/CaseList/full/SQLServer_TD_dbo_case_list_{datesnap}.parquet"
dbutils.fs.mv(case_list_parquet_file, case_list_final_output_path)
dbutils.fs.rm(case_list_temp_output_path, True)

# Read and display schema to confirm the file output
# Commenting out as they are too many to display
"""
output_paths = {
    "AppealCase": appeal_case_final_output_path,
    "CaseRespondent": case_respondent_final_output_path,
    "MainRespondent": main_respondent_final_output_path,
    "Respondent": respondent_final_output_path,
    "FileLocation": file_location_final_output_path,
    "Representative": representative_final_output_path,
    "CaseRepresentative": case_representative_final_output_path,
    "CaseSurety": case_surety_final_output_path,
    "CostAward": cost_award_final_output_path,
    "Category": category_final_output_path,
    "AppealCategory": appeal_category_final_output_path,
    "Link": link_final_output_path,
    "AppealType": appeal_type_final_output_path,
    "AppealGrounds": appeal_grounds_final_output_path,
    "Language": language_final_output_path,
    "ReasonAdjourn": reason_adjourn_final_output_path,
    "CaseStatus": case_status_final_output_path,
    "DecisionType": decision_type_final_output_path,
    "Status": status_final_output_path,
    "ReceivedDocument": received_document_final_output_path,
    "DocumentReceived": document_received_final_output_path,
    "NewMatter": new_matter_final_output_path,
    "AppealNewMatter": appeal_new_matter_final_output_path,
    "HearingCentre": hearing_centre_final_output_path,
    "StandardDirection": standard_direction_final_output_path,
    "ReviewStandardDirection": review_standard_direction_final_output_path,
    "ReviewSpecificDirection": review_specific_direction_final_output_path,
    "AppealTypeCategory": appeal_type_category_final_output_path,
    "ReasonLink": reason_link_final_output_path,
    "LinkDetail": link_detail_final_output_path,
    "Department": dept_final_output_path,
    "User": user_final_output_path,
    "TransactionMethod": transaction_method_final_output_path,
    "TransactionType": transaction_type_final_output_path,
    "TransactionStatus": transaction_status_final_output_path,
    "Transaction": transaction_final_output_path,
    "CostOrder": cost_order_final_output_path,
    "ListType": list_type_final_output_path,
    "Adjudicator": adjudicator_final_output_path,
    "ListSitting": list_sitting_final_output_path,
    "Court": court_final_output_path,
    "List": list_final_output_path,
    "DocumentsReceived": documents_received_final_output_path,
    "CaseFeeSummary": case_fee_summary_final_output_path,
    "HumanRight": human_right_final_output_path,
    "CaseAppellant": case_appellant_final_output_path,
    "BFType": bf_type_final_output_path,
    "BFDiary": bf_diary_final_output_path,
    "AppealHumanRight": appeal_human_right_final_output_path,
    "Country": country_final_output_path,
    "StatusContact": status_contact_final_output_path,
    "FeeSatisfaction": fee_satisfaction_final_output_path,
    "CaseSponsor": case_sponsor_final_output_path,
    "Appellant": appellant_final_output_path,
    "DetentionCentre": detention_centre_final_output_path,
    "HearingType": hearing_type_final_output_path,
    "Embassy": embassy_final_output_path,
    "Port": port_final_output_path,
    "PaymentRemissionReason": payment_remission_reason_final_output_path,
    "HearingPointHistory": hearing_point_history_final_output_path,
    "HearingPointChangeReason": hearing_point_change_reason_final_output_path,
    "HearingPoint": hearing_point_final_output_path,
    "History": history_final_output_path,
    "CaseList": case_list_final_output_path
}

for output_path in output_paths.values():
    df = spark.read.format("parquet").load(output_path)
    df.printSchema()
    display(df)

"""
