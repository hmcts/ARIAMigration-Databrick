%pip install azure-storage-blob pandas faker python-docx

import random

import pandas as pd
from faker import Faker


# Setting variables for use in subsequent cells
raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/JOH/test"
landing_mnt = "/mnt/ingest00landingsboxlanding/test"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/JOH/test"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/JOH/test"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/JOH/test"


def generate_adjudicator_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Adjudicator table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        adjudicator_id = fake.random_number(digits=4)
        surname = fake.last_name()
        forenames = fake.first_name()
        title = random.choice(["Mr", "Mrs", "Miss", "Ms", "Dr"])
        full_time = random.choice([1, 2])
        centre_id = fake.random_number(digits=3)
        do_not_list = random.choice([True, False])
        date_of_birth = fake.date_between(start_date="-80y", end_date="-30y").strftime(
            "%Y-%m-%d"
        )
        correspondence_address = random.choice([1, 2])
        contact_telephone = fake.random_number(digits=10)
        contact_details = fake.phone_number()
        available_at_short_notice = random.choice([True, False])
        employment_terms = fake.random_number(digits=2)
        identity_number = fake.random_number(digits=9)
        date_of_retirement = fake.date_between(
            start_date="+5y", end_date="+30y"
        ).strftime("%Y-%m-%d")
        contract_end_date = fake.date_between(
            start_date="+1y", end_date="+5y"
        ).strftime("%Y-%m-%d")
        contract_renewal_date = fake.date_between(
            start_date="+1y", end_date="+5y"
        ).strftime("%Y-%m-%d")
        do_not_use_reason = fake.random_number(digits=1)
        address_1 = fake.street_address()
        address_2 = fake.secondary_address()
        address_3 = fake.city()
        address_4 = fake.state()
        address_5 = fake.country()
        postcode = fake.postcode()
        telephone = fake.phone_number()
        mobile = fake.phone_number()
        email = fake.email()
        business_address_1 = fake.street_address()
        business_address_2 = fake.secondary_address()
        business_address_3 = fake.city()
        business_address_4 = fake.state()
        business_address_5 = fake.country()
        business_postcode = fake.postcode()
        business_telephone = fake.phone_number()
        business_fax = fake.phone_number()
        business_email = fake.email()
        judicial_instructions = fake.paragraph(nb_sentences=2)
        judicial_instructions_date = fake.date_between(
            start_date="-1y", end_date="today"
        ).strftime("%Y-%m-%d")
        notes = fake.paragraph(nb_sentences=1)
        position = fake.random_number(digits=1)
        extension = fake.random_number(digits=3)
        sdx = f"{surname[0]}{fake.random_number(digits=3)}"
        judicial_status = fake.random_number(digits=2)

        data.append(
            [
                adjudicator_id,
                surname,
                forenames,
                title,
                full_time,
                centre_id,
                do_not_list,
                date_of_birth,
                correspondence_address,
                contact_telephone,
                contact_details,
                available_at_short_notice,
                employment_terms,
                identity_number,
                date_of_retirement,
                contract_end_date,
                contract_renewal_date,
                do_not_use_reason,
                address_1,
                address_2,
                address_3,
                address_4,
                address_5,
                postcode,
                telephone,
                mobile,
                email,
                business_address_1,
                business_address_2,
                business_address_3,
                business_address_4,
                business_address_5,
                business_postcode,
                business_telephone,
                business_fax,
                business_email,
                judicial_instructions,
                judicial_instructions_date,
                notes,
                position,
                extension,
                sdx,
                judicial_status,
            ]
        )

    columns = [
        "AdjudicatorId",
        "Surname",
        "Forenames",
        "Title",
        "FullTime",
        "CentreId",
        "DoNotList",
        "DateOfBirth",
        "CorrespondenceAddress",
        "ContactTelephone",
        "ContactDetails",
        "AvailableAtShortNotice",
        "EmploymentTerms",
        "IdentityNumber",
        "DateOfRetirement",
        "ContractEndDate",
        "ContractRenewalDate",
        "DoNotUseReason",
        "Address1",
        "Address2",
        "Address3",
        "Address4",
        "Address5",
        "Postcode",
        "Telephone",
        "Mobile",
        "Email",
        "BusinessAddress1",
        "BusinessAddress2",
        "BusinessAddress3",
        "BusinessAddress4",
        "BusinessAddress5",
        "BusinessPostcode",
        "BusinessTelephone",
        "BusinessFax",
        "BusinessEmail",
        "JudicialInstructions",
        "JudicialInstructionsDate",
        "Notes",
        "Position",
        "Extension",
        "Sdx",
        "JudicialStatus",
    ]

    df = pd.DataFrame(data, columns=columns)
    return df


def generate_adjudicator_role_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the AdjudicatorRole table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        adjudicator_id = fake.random_number(digits=4)
        role = fake.random_number(digits=1)
        date_of_appointment = fake.date_between(
            start_date="-10y", end_date="today"
        ).strftime("%Y-%m-%d")
        end_date_of_appointment = fake.date_between(
            start_date="today", end_date="+10y"
        ).strftime("%Y-%m-%d")

        data.append(
            [adjudicator_id, role, date_of_appointment, end_date_of_appointment]
        )

    columns = ["AdjudicatorId", "Role", "DateOfAppointment", "EndDateOfAppointment"]

    df = pd.DataFrame(data, columns=columns)
    return df


def generate_employment_term_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the EmploymentTerm table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        employment_term_id = fake.random_number(digits=3)
        description = fake.sentence(nb_words=3)
        do_not_use = random.choice([True, False])

        data.append([employment_term_id, description, do_not_use])

    columns = ["EmploymentTermId", "Description", "DoNotUse"]

    df = pd.DataFrame(data, columns=columns)
    return df


def generate_do_not_use_reason_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the DoNotUseReason table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        do_not_use_reason_id: int = fake.random_number(digits=3)
        description: str = fake.sentence(nb_words=3)
        do_not_use: bool = random.choice([True, False])

        data.append([do_not_use_reason_id, description, do_not_use])

    columns = ["DoNotUseReasonId", "Description", "DoNotUse"]

    df = pd.DataFrame(data, columns=columns)
    return df


def generate_jo_history_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the JoHistory table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        jo_history_id: int = fake.random_number(digits=4)
        adjudicator_id: int = fake.random_number(digits=4)
        hist_date: str = fake.date_between(start_date="-1y", end_date="today").strftime(
            "%Y-%m-%d"
        )
        hist_type: int = fake.random_number(digits=2)
        user_id: int = fake.random_number(digits=3)
        comment: str = fake.paragraph(nb_sentences=1)
        deleted_by: int = fake.random_number(digits=1)

        data.append(
            [
                jo_history_id,
                adjudicator_id,
                hist_date,
                hist_type,
                user_id,
                comment,
                deleted_by,
            ]
        )

    columns = [
        "JoHistoryId",
        "AdjudicatorId",
        "HistDate",
        "HistType",
        "UserId",
        "Comment",
        "DeletedBy",
    ]

    df = pd.DataFrame(data, columns=columns)
    return df


def generate_users_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the Users table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        user_id: int = fake.random_number(digits=3)
        name: str = fake.user_name()
        user_type: str = random.choice(["U", "G"])
        full_name: str = fake.name()
        suspended: bool = random.choice([True, False])
        dept_id: int = fake.random_number(digits=3)
        extension: str = (
            fake.random_number(digits=3) if random.random() < 0.5 else "\\N"
        )
        do_not_use: bool = (
            random.choice([True, False]) if random.random() < 0.5 else None
        )

        data.append(
            [
                user_id,
                name,
                user_type,
                full_name,
                suspended,
                dept_id,
                extension,
                do_not_use,
            ]
        )

    columns = [
        "UserId",
        "Name",
        "UserType",
        "FullName",
        "Suspended",
        "DeptId",
        "Extension",
        "DoNotUse",
    ]

    df = pd.DataFrame(data, columns=columns)
    return df


def generate_hearing_centre_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the HearingCentre table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        centre_id: int = fake.random_number(digits=3)
        description: str = fake.company()
        prefix: str = fake.lexify(text="??")
        bail_number: int = fake.random_number(digits=4)
        court_type: int = fake.random_number(digits=1)
        address_1: str = fake.street_address()
        address_2: str = fake.secondary_address() if random.random() < 0.5 else "\\N"
        address_3: str = fake.city() if random.random() < 0.5 else "\\N"
        address_4: str = fake.state() if random.random() < 0.5 else "\\N"
        address_5: str = fake.country() if random.random() < 0.5 else "\\N"
        postcode: str = fake.postcode()
        telephone: str = fake.phone_number()
        fax: str = fake.phone_number() if random.random() < 0.5 else "\\N"
        email: str = fake.email() if random.random() < 0.5 else "\\N"
        sdx: str = fake.lexify(text="????")
        stl_report_path: str = (
            fake.file_path(depth=3, extension="txt") if random.random() < 0.5 else "\\N"
        )
        stl_help_path: str = (
            fake.file_path(depth=3, extension="txt") if random.random() < 0.5 else "\\N"
        )
        local_path: str = (
            fake.file_path(depth=3, extension="txt") if random.random() < 0.5 else "\\N"
        )
        global_path: str = (
            fake.file_path(depth=3, extension="txt") if random.random() < 0.5 else "\\N"
        )
        pou_id: int = fake.random_number(digits=2)
        main_london_centre: bool = random.choice([True, False])
        do_not_use: bool = random.choice([True, False])
        centre_location: int = fake.random_number(digits=1)
        organisation_id: int = fake.random_number(digits=1)

        data.append(
            [
                centre_id,
                description,
                prefix,
                bail_number,
                court_type,
                address_1,
                address_2,
                address_3,
                address_4,
                address_5,
                postcode,
                telephone,
                fax,
                email,
                sdx,
                stl_report_path,
                stl_help_path,
                local_path,
                global_path,
                pou_id,
                main_london_centre,
                do_not_use,
                centre_location,
                organisation_id,
            ]
        )

    columns = [
        "CentreId",
        "Description",
        "Prefix",
        "BailNumber",
        "CourtType",
        "Address1",
        "Address2",
        "Address3",
        "Address4",
        "Address5",
        "Postcode",
        "Telephone",
        "Fax",
        "Email",
        "Sdx",
        "STLReportPath",
        "STLHelpPath",
        "LocalPath",
        "GlobalPath",
        "PouId",
        "MainLondonCentre",
        "DoNotUse",
        "CentreLocation",
        "OrganisationId",
    ]

    df = pd.DataFrame(data, columns=columns)
    return df


def generate_other_centre_data(num_records: int) -> pd.DataFrame:
    """
    Generate sample data for the OtherCentre table.

    Args:
        num_records (int): The number of records to generate.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        other_centre_id: int = fake.random_number(digits=3)
        adjudicator_id: int = fake.random_number(digits=4)
        centre_id: int = fake.random_number(digits=3)

        data.append([other_centre_id, adjudicator_id, centre_id])

    columns = ["OtherCentreId", "AdjudicatorId", "CentreId"]

    df = pd.DataFrame(data, columns=columns)
    return df


# Generate sample data for each table
num_adjudicator_records: int = 100
num_adjudicator_role_records: int = 100
num_employment_term_records: int = 20
num_do_not_use_reason_records: int = 10
num_jo_history_records: int = 200
num_users_records: int = 50
num_hearing_centre_records: int = 30
num_other_centre_records: int = 100

adjudicator_data: pd.DataFrame = generate_adjudicator_data(num_adjudicator_records)
adjudicator_role_data: pd.DataFrame = generate_adjudicator_role_data(
    num_adjudicator_role_records
)
employment_term_data: pd.DataFrame = generate_employment_term_data(
    num_employment_term_records
)
do_not_use_reason_data: pd.DataFrame = generate_do_not_use_reason_data(
    num_do_not_use_reason_records
)
jo_history_data: pd.DataFrame = generate_jo_history_data(num_jo_history_records)
users_data: pd.DataFrame = generate_users_data(num_users_records)
hearing_centre_data: pd.DataFrame = generate_hearing_centre_data(
    num_hearing_centre_records
)
other_centre_data: pd.DataFrame = generate_other_centre_data(num_other_centre_records)

# Save the generated data as Parquet files for local test
adjudicator_data.to_parquet("Adjudicator.parquet", index=False)
adjudicator_role_data.to_parquet("AdjudicatorRole.parquet", index=False)
employment_term_data.to_parquet("EmploymentTerm.parquet", index=False)
do_not_use_reason_data.to_parquet("DoNotUseReason.parquet", index=False)
jo_history_data.to_parquet("JoHistory.parquet", index=False)
users_data.to_parquet("Users.parquet", index=False)
hearing_centre_data.to_parquet("HearingCentre.parquet", index=False)
other_centre_data.to_parquet("OtherCentre.parquet", index=False)


# Save the generated data as Parquet files in the landing path
adjudicator_data.to_parquet(f"{landing_mnt}/Adjudicator/full/Adjudicator.parquet", index=False)
adjudicator_role_data.to_parquet(f"{landing_mnt}/AdjudicatorRole/full/AdjudicatorRole.parquet", index=False)
employment_term_data.to_parquet(f"{landing_mnt}/EmploymentTerm/full/EmploymentTerm.parquet", index=False)
do_not_use_reason_data.to_parquet(f"{landing_mnt}/DoNotUseReason/full/DoNotUseReason.parquet", index=False)
jo_history_data.to_parquet(f"{landing_mnt}/JoHistory/full/JoHistory.parquet", index=False)
users_data.to_parquet(f"{landing_mnt}/Users/full/Users.parquet", index=False)
hearing_centre_data.to_parquet(f"{landing_mnt}/HearingCentre/full/HearingCentre.parquet", index=False)
other_centre_data.to_parquet(f"{landing_mnt}/OtherCentre/full/OtherCentre.parquet", index=False)
