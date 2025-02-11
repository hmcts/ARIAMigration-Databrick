# %pip install azure-storage-blob pandas faker python-docx

import random
import re

import pandas as pd
from faker import Faker


# Setting variables for use in subsequent cells
raw_mnt = "/mnt/raw/ARIADM/ARM/JOH"
landing_mnt = "/mnt/landing/test/"
bronze_mnt = "/mnt/bronze/ARIADM/ARM/JOH"
silver_mnt = "/mnt/silver/ARIADM/ARM/JOH"
gold_mnt = "/mnt/gold/ARIADM/ARM/JOH"

# Variable to control the percentage of adjudicator IDs present in all tables
adjudicator_id_presence_percentage = 80


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
    adjudicator_ids = set()
    for _ in range(num_records):
        adjudicator_id = fake.unique.random_number(digits=4)
        adjudicator_ids.add(adjudicator_id)
        surname = fake.last_name()
        forenames = fake.first_name()
        title = random.choice(["Mr", "Mrs", "Miss", "Ms", "Dr"])
        full_time = random.choice([1, 2])
        centre_id = fake.random_number(digits=3)
        do_not_list = random.choice([True, False])
        date_of_birth = fake.date_between(start_date="-80y", end_date="-30y")
        correspondence_address = random.choice([1, 2])
        contact_telephone = fake.random_number(digits=10)
        contact_details = fake.phone_number()
        available_at_short_notice = random.choice([True, False])
        employment_terms = fake.random_number(digits=2)
        identity_number = fake.random_number(digits=9)
        date_of_retirement = fake.date_between(
            start_date="+5y", end_date="+30y")
        contract_end_date = fake.date_between(
            start_date="+1y", end_date="+5y")
        contract_renewal_date = fake.date_between(
            start_date="+1y", end_date="+5y")
        do_not_use_reason = fake.random_number(digits=1)
        address_1 = fake.street_address()
        address_2 = fake.secondary_address()
        address_3 = fake.city()
        address_4 = fake.county()
        address_5 = fake.country()
        postcode = fake.postcode()
        telephone = fake.phone_number()
        mobile = fake.phone_number()
        email = fake.email()
        business_address_1 = fake.street_address()
        business_address_2 = fake.secondary_address()
        business_address_3 = fake.city()
        business_address_4 = fake.county()
        business_address_5 = fake.country()
        business_postcode = fake.postcode()
        business_telephone = fake.phone_number()
        business_fax = fake.phone_number()
        business_email = fake.email()
        judicial_instructions = fake.paragraph(nb_sentences=2)
        judicial_instructions_date = fake.date_between(
            start_date="-1y", end_date="-1m"
        )
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
    return df, adjudicator_ids


def generate_adjudicator_role_data(num_records: int, adjudicator_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the AdjudicatorRole table.

    Args:
        num_records (int): The number of records to generate.
        adjudicator_ids (set): Set of adjudicator IDs from the Adjudicator table.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        if random.random() < adjudicator_id_presence_percentage / 100:
            adjudicator_id = random.choice(list(adjudicator_ids))
        else:
            adjudicator_id = fake.random_number(digits=4)
        role = fake.random_number(digits=1)
        date_of_appointment = fake.date_between(
            start_date="-10y", end_date="-1m"
        )
        end_date_of_appointment = fake.date_between(
            start_date="+1m", end_date="+10y"
        )

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
    employment_term_ids = set()
    for _ in range(num_records):
        employment_term_id = fake.unique.random_number(digits=3)
        employment_term_ids.add(employment_term_id)
        description = fake.sentence(nb_words=3)
        do_not_use = random.choice([True, False])

        data.append([employment_term_id, description, do_not_use])

    columns = ["EmploymentTermId", "Description", "DoNotUse"]

    df = pd.DataFrame(data, columns=columns)
    return df, employment_term_ids


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
    do_not_use_reason_ids = set()
    for _ in range(num_records):
        do_not_use_reason_id: int = fake.unique.random_number(digits=3)
        do_not_use_reason_ids.add(do_not_use_reason_id)
        description: str = fake.sentence(nb_words=3)
        do_not_use: bool = random.choice([True, False])

        data.append([do_not_use_reason_id, description, do_not_use])

    columns = ["DoNotUseReasonId", "Description", "DoNotUse"]

    df = pd.DataFrame(data, columns=columns)
    return df, do_not_use_reason_ids


def generate_jo_history_data(num_records: int, adjudicator_ids: set, user_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the JoHistory table.

    Args:
        num_records (int): The number of records to generate.
        adjudicator_ids (set): Set of adjudicator IDs from the Adjudicator table.
        user_ids (set): Set of user IDs from the Users table.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        jo_history_id: int = fake.random_number(digits=4)
        if random.random() < adjudicator_id_presence_percentage / 100:
            adjudicator_id: int = random.choice(list(adjudicator_ids))
        else:
            adjudicator_id: int = fake.random_number(digits=4)
        hist_date: str = fake.date_between(start_date="-1y", end_date="-1m")
        hist_type: int = fake.random_number(digits=2)
        if random.random() < adjudicator_id_presence_percentage / 100:
            user_id: int = random.choice(list(user_ids))
        else:
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
    user_ids = set()
    for _ in range(num_records):
        user_id: int = fake.unique.random_number(digits=3)
        user_ids.add(user_id)
        name: str = fake.user_name()
        user_type: str = random.choice(["U", "G"])
        full_name: str = fake.name()
        suspended: bool = random.choice([True, False])
        dept_id: int = fake.random_number(digits=3)
        extension: str = (
            fake.random_number(digits=3) if random.random() < 0.5 else None
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
    return df, user_ids


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
    centre_ids = set()
    for _ in range(num_records):
        centre_id: int = fake.unique.random_number(digits=3)
        centre_ids.add(centre_id)
        description: str = fake.company()
        prefix: str = fake.lexify(text="??")
        bail_number: int = fake.random_number(digits=4)
        court_type: int = fake.random_number(digits=1)
        address_1: str = fake.street_address()
        address_2: str = fake.secondary_address() if random.random() < 0.5 else "\\N"
        address_3: str = fake.city() if random.random() < 0.5 else "\\N"
        address_4: str = fake.county() if random.random() < 0.5 else "\\N"
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
    return df, centre_ids


def generate_other_centre_data(num_records: int, adjudicator_ids: set, centre_ids: set) -> pd.DataFrame:
    """
    Generate sample data for the OtherCentre table.

    Args:
        num_records (int): The number of records to generate.
        adjudicator_ids (set): Set of adjudicator IDs from the Adjudicator table.
        centre_ids (set): Set of centre IDs from the HearingCentre table.

    Returns:
        pd.DataFrame: The generated sample data as a DataFrame.
    """
    fake = Faker("en_GB")

    data = []
    for _ in range(num_records):
        other_centre_id: int = fake.random_number(digits=3)
        if random.random() < adjudicator_id_presence_percentage / 100:
            adjudicator_id: int = random.choice(list(adjudicator_ids))
        else:
            adjudicator_id: int = fake.random_number(digits=4)
        if random.random() < adjudicator_id_presence_percentage / 100:
            centre_id: int = random.choice(list(centre_ids))
        else:
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

adjudicator_data, adjudicator_ids = generate_adjudicator_data(num_adjudicator_records)
adjudicator_role_data = generate_adjudicator_role_data(num_adjudicator_role_records, adjudicator_ids)
employment_term_data, employment_term_ids = generate_employment_term_data(num_employment_term_records)
do_not_use_reason_data, do_not_use_reason_ids = generate_do_not_use_reason_data(num_do_not_use_reason_records)
users_data, user_ids = generate_users_data(num_users_records)
jo_history_data = generate_jo_history_data(num_jo_history_records, adjudicator_ids, user_ids)
hearing_centre_data, centre_ids = generate_hearing_centre_data(num_hearing_centre_records)
other_centre_data = generate_other_centre_data(num_other_centre_records, adjudicator_ids, centre_ids)

# Update Adjudicator table with referential integrity
adjudicator_data["EmploymentTerms"] = adjudicator_data["EmploymentTerms"].apply(lambda x: random.choice(list(employment_term_ids)))
adjudicator_data["DoNotUseReason"] = adjudicator_data["DoNotUseReason"].apply(lambda x: random.choice(list(do_not_use_reason_ids)))
adjudicator_data["CentreId"] = adjudicator_data["CentreId"].apply(lambda x: random.choice(list(centre_ids)))

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
# adjudicator_data.to_parquet(f"{landing_mnt}/Adjudicator/full/Adjudicator.parquet", index=False)
# adjudicator_role_data.to_parquet(f"{landing_mnt}/AdjudicatorRole/full/AdjudicatorRole.parquet", index=False)
# employment_term_data.to_parquet(f"{landing_mnt}/EmploymentTerm/full/EmploymentTerm.parquet", index=False)
# do_not_use_reason_data.to_parquet(f"{landing_mnt}/DoNotUseReason/full/DoNotUseReason.parquet", index=False)
# jo_history_data.to_parquet(f"{landing_mnt}/JoHistory/full/JoHistory.parquet", index=False)
# users_data.to_parquet(f"{landing_mnt}/Users/full/Users.parquet", index=False)
# hearing_centre_data.to_parquet(f"{landing_mnt}/HearingCentre/full/HearingCentre.parquet", index=False)
# other_centre_data.to_parquet(f"{landing_mnt}/OtherCentre/full/OtherCentre.parquet", index=False)

spark_adjudicator_data = spark.createDataFrame(adjudicator_data)
spark_adjudicator_role_data = spark.createDataFrame(adjudicator_role_data)
spark_employment_term_data = spark.createDataFrame(employment_term_data)
spark_do_not_use_reason_data = spark.createDataFrame(do_not_use_reason_data)
spark_jo_history_data = spark.createDataFrame(jo_history_data)
spark_users_data = spark.createDataFrame(users_data)
spark_hearing_centre_data = spark.createDataFrame(hearing_centre_data)
spark_other_centre_data = spark.createDataFrame(other_centre_data)
 
# Generate timestamp for unique file naming
datesnap = spark.sql("select date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

# Generate timestamp for unique file naming
datesnap = spark.sql("select date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

# Adjudicator
adjudicator_temp_output_path = f"/mnt/landing/test/Adjudicator/temp_{datesnap}"
spark_adjudicator_data.coalesce(1).write.format("parquet").mode("overwrite").save(adjudicator_temp_output_path)

adjudicator_files = dbutils.fs.ls(adjudicator_temp_output_path)
adjudicator_parquet_file = [file.path for file in adjudicator_files if re.match(r".*\.parquet$", file.path)][0]

adjudicator_final_output_path = f"/mnt/landing/test/Adjudicator/full/SQLServer_Sales_IRIS_dbo_adjudicator_{datesnap}.parquet"
dbutils.fs.mv(adjudicator_parquet_file, adjudicator_final_output_path)
dbutils.fs.rm(adjudicator_temp_output_path, True)

# AdjudicatorRole
adjudicator_role_temp_output_path = f"/mnt/landing/test/AdjudicatorRole/temp_{datesnap}"
spark_adjudicator_role_data.coalesce(1).write.format("parquet").mode("overwrite").save(adjudicator_role_temp_output_path)

adjudicator_role_files = dbutils.fs.ls(adjudicator_role_temp_output_path)
adjudicator_role_parquet_file = [file.path for file in adjudicator_role_files if re.match(r".*\.parquet$", file.path)][0]

adjudicator_role_final_output_path = f"/mnt/landing/test/AdjudicatorRole/full/SQLServer_Sales_IRIS_dbo_adjudicator_role_{datesnap}.parquet"
dbutils.fs.mv(adjudicator_role_parquet_file, adjudicator_role_final_output_path)
dbutils.fs.rm(adjudicator_role_temp_output_path, True)

# ARIAEmploymentTerm
employment_term_temp_output_path = f"/mnt/landing/test/ARIAEmploymentTerm/temp_{datesnap}"
spark_employment_term_data.coalesce(1).write.format("parquet").mode("overwrite").save(employment_term_temp_output_path)

employment_term_files = dbutils.fs.ls(employment_term_temp_output_path)
employment_term_parquet_file = [file.path for file in employment_term_files if re.match(r".*\.parquet$", file.path)][0]

employment_term_final_output_path = f"/mnt/landing/test/ARIAEmploymentTerm/full/SQLServer_Sales_IRIS_dbo_employment_term_{datesnap}.parquet"
dbutils.fs.mv(employment_term_parquet_file, employment_term_final_output_path)
dbutils.fs.rm(employment_term_temp_output_path, True)

# DoNotUseReason
do_not_use_reason_temp_output_path = f"/mnt/landing/test/DoNotUseReason/temp_{datesnap}"
spark_do_not_use_reason_data.coalesce(1).write.format("parquet").mode("overwrite").save(do_not_use_reason_temp_output_path)

do_not_use_reason_files = dbutils.fs.ls(do_not_use_reason_temp_output_path)
do_not_use_reason_parquet_file = [file.path for file in do_not_use_reason_files if re.match(r".*\.parquet$", file.path)][0]

do_not_use_reason_final_output_path = f"/mnt/landing/test/DoNotUseReason/full/SQLServer_Sales_IRIS_dbo_do_not_use_reason_{datesnap}.parquet"
dbutils.fs.mv(do_not_use_reason_parquet_file, do_not_use_reason_final_output_path)
dbutils.fs.rm(do_not_use_reason_temp_output_path, True)

# JoHistory
jo_history_temp_output_path = f"/mnt/landing/test/JoHistory/temp_{datesnap}"
spark_jo_history_data.coalesce(1).write.format("parquet").mode("overwrite").save(jo_history_temp_output_path)

jo_history_files = dbutils.fs.ls(jo_history_temp_output_path)
jo_history_parquet_file = [file.path for file in jo_history_files if re.match(r".*\.parquet$", file.path)][0]

jo_history_final_output_path = f"/mnt/landing/test/JoHistory/full/SQLServer_Sales_IRIS_dbo_jo_history_{datesnap}.parquet"
dbutils.fs.mv(jo_history_parquet_file, jo_history_final_output_path)
dbutils.fs.rm(jo_history_temp_output_path, True)

# Users
users_temp_output_path = f"/mnt/landing/test/Users/temp_{datesnap}"
spark_users_data.coalesce(1).write.format("parquet").mode("overwrite").save(users_temp_output_path)

users_files = dbutils.fs.ls(users_temp_output_path)
users_parquet_file = [file.path for file in users_files if re.match(r".*\.parquet$", file.path)][0]

users_final_output_path = f"/mnt/landing/test/Users/full/SQLServer_Sales_IRIS_dbo_users_{datesnap}.parquet"
dbutils.fs.mv(users_parquet_file, users_final_output_path)
dbutils.fs.rm(users_temp_output_path, True)

# ARIAHearingCentre
hearing_centre_temp_output_path = f"/mnt/landing/test/ARIAHearingCentre/temp_{datesnap}"
spark_hearing_centre_data.coalesce(1).write.format("parquet").mode("overwrite").save(hearing_centre_temp_output_path)

hearing_centre_files = dbutils.fs.ls(hearing_centre_temp_output_path)
hearing_centre_parquet_file = [file.path for file in hearing_centre_files if re.match(r".*\.parquet$", file.path)][0]

hearing_centre_final_output_path = f"/mnt/landing/test/ARIAHearingCentre/full/SQLServer_Sales_IRIS_dbo_hearing_centre_{datesnap}.parquet"
dbutils.fs.mv(hearing_centre_parquet_file, hearing_centre_final_output_path)
dbutils.fs.rm(hearing_centre_temp_output_path, True)

# OtherCentre
other_centre_temp_output_path = f"/mnt/landing/test/OtherCentre/temp_{datesnap}"
spark_other_centre_data.coalesce(1).write.format("parquet").mode("overwrite").save(other_centre_temp_output_path)

other_centre_files = dbutils.fs.ls(other_centre_temp_output_path)
other_centre_parquet_file = [file.path for file in other_centre_files if re.match(r".*\.parquet$", file.path)][0]

other_centre_final_output_path = f"/mnt/landing/test/OtherCentre/full/SQLServer_Sales_IRIS_dbo_other_centre_{datesnap}.parquet"
dbutils.fs.mv(other_centre_parquet_file, other_centre_final_output_path)
dbutils.fs.rm(other_centre_temp_output_path, True)
 
# Read and display schema to confirm the file output
output_paths = {
    "Adjudicator": adjudicator_final_output_path,
    "AdjudicatorRole": adjudicator_role_final_output_path,
    "EmploymentTerm": employment_term_final_output_path,
    "DoNotUseReason": do_not_use_reason_final_output_path,
    "JoHistory": jo_history_final_output_path,
    "Users": users_final_output_path,
    "HearingCentre": hearing_centre_final_output_path,
    "OtherCentre": other_centre_final_output_path
    }

for output_path in output_paths.values():
    df = spark.read.format("parquet").load(output_path)
    df.printSchema()
    display(df)
