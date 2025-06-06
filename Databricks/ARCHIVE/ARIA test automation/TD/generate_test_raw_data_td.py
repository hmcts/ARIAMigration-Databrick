import random
import re
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

import csv
from pyspark.sql import functions
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

spark_session = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Setting variables for use in subsequent cells
# raw_mnt = "/mnt/ingest01rawsboxraw/ARIADM/ARM/TD/test"
# landing_mnt = "/mnt/ingest01landingsboxlanding/test/"
# bronze_mnt = "/mnt/ingest01curatedsboxbronze/ARIADM/ARM/TD/test"
# silver_mnt = "/mnt/ingest01curatedsboxsilver/ARIADM/ARM/TD/test"
# gold_mnt = "/mnt/ingest01curatedsboxgold/ARIADM/ARM/TD/test"
raw_mnt = "/mnt/raw/ARIADM/ARM/TD/test"
landing_mnt = "/mnt/landing/dbo/dbo/"
bronze_mnt = "/mnt/bronze/ARIADM/ARM/TD"
silver_mnt = "/mnt/silver/ARIADM/ARM/TD"
gold_mnt = "/mnt/gold/ARIADM/ARM/TD"

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
        # centre_id = fake.random_number(digits=3)
        centre_id: int = int(fake.unique.random_number(digits=3))  # Force Python int
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

    df = spark_session.createDataFrame(df)
    df = df.withColumn("CentreId", df["CentreId"].cast(IntegerType()))
    df = df.toPandas()

    return df, case_nos


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

    df = spark_session.createDataFrame(df)
    df = df.withColumn("CentreId", df["CentreId"].cast(IntegerType()))
    df = df.toPandas()

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
        # centre_id = fake.unique.random_number(digits=3)
        # centre_ids.add(centre_id)
        centre_id: int = int(fake.unique.random_number(digits=3))  # Force Python int
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

    df = spark_session.createDataFrame(df)
    df = df.withColumn("CentreId", df["CentreId"].cast(IntegerType()))
    df = df.toPandas()

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

    datesnap = datetime.today().strftime('%d-%m-%y_%H:%M')

    # csv_file_path = f"/mnt/ingest01landingsboxlanding/test/IRIS_data_{datesnap}.csv"
    # # html_template_list = spark.read.text("/mnt/html-template/html-template/TD-Details-no-js-v1.html").collect()
    # with open(csv_file_path, "w", newline="") as csvfile:
    #     writer = csv.writer(csvfile)
    #     writer.writerow(column_names)
    #     writer.writerows(csv_data)

# Generate sample data for each table
num_appeal_case_records = 100
num_file_location_records = 150
num_case_appellant_records = 120
num_appellant_records = 200
num_department_records = 30
num_hearing_centre_records = 20

appeal_case_data, case_nos = generate_appeal_case_data(num_appeal_case_records)
appellant_data, appellant_ids = generate_appellant_data(num_appellant_records)
hearing_centre_data, centre_ids = generate_hearing_centre_data(num_hearing_centre_records)  
department_data, dept_ids = generate_department_data(num_department_records, centre_ids)
file_location_data = generate_file_location_data(num_file_location_records, case_nos, dept_ids)
case_appellant_data = generate_case_appellant_data(num_case_appellant_records, case_nos, appellant_ids)

# Update Appeal Case table with referential integrity
# appeal_case_data["CentreId"] = appeal_case_data["CentreId"].apply(lambda x: random.choice(list(centre_ids)))
# appeal_case_data["CentreId"] = appeal_case_data["CentreId"].astype(int)
# df = df.withColumn("CentreId", df["CentreId"].cast(IntegerType()))

# Generate the CSV file
generate_csv_file(appeal_case_data, appellant_data, hearing_centre_data, department_data, file_location_data)

# Save the generated data as Parquet files for local test
appeal_case_data.to_parquet("AppealCase.parquet", index=False)
file_location_data.to_parquet("FileLocation.parquet", index=False)  
case_appellant_data.to_parquet("CaseAppellant.parquet", index=False)
appellant_data.to_parquet("Appellant.parquet", index=False)
department_data.to_parquet("Department.parquet", index=False)
hearing_centre_data.to_parquet("HearingCentre.parquet", index=False)

spark_appeal_case_data = spark.createDataFrame(appeal_case_data)
spark_file_location_data = spark.createDataFrame(file_location_data)
spark_case_appellant_data = spark.createDataFrame(case_appellant_data)  
spark_appellant_data = spark.createDataFrame(appellant_data)
spark_department_data = spark.createDataFrame(department_data)
spark_hearing_centre_data = spark.createDataFrame(hearing_centre_data)

# Generate timestamp for unique file naming
datesnap = spark.sql("select date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

# Appeal Case
appeal_case_temp_output_path = f"/mnt/ingest01landingsboxlanding/test/AppealCase/temp_{datesnap}"  
spark_appeal_case_data.coalesce(1).write.format("parquet").mode("overwrite").save(appeal_case_temp_output_path)

appeal_case_files = dbutils.fs.ls(appeal_case_temp_output_path)
appeal_case_parquet_file = [file.path for file in appeal_case_files if file.path.endswith(".parquet")][0]

appeal_case_final_output_path = f"/mnt/ingest01landingsboxlanding/test/AppealCase/full/SQLServer_TD_dbo_appeal_case_{datesnap}.parquet"
dbutils.fs.mv(appeal_case_parquet_file, appeal_case_final_output_path)
dbutils.fs.rm(appeal_case_temp_output_path, True)

# File Location  
file_location_temp_output_path = f"/mnt/ingest01landingsboxlanding/test/FileLocation/temp_{datesnap}"
spark_file_location_data.coalesce(1).write.format("parquet").mode("overwrite").save(file_location_temp_output_path)

file_location_files = dbutils.fs.ls(file_location_temp_output_path)  
file_location_parquet_file = [file.path for file in file_location_files if file.path.endswith(".parquet")][0]

file_location_final_output_path = f"/mnt/ingest01landingsboxlanding/test/FileLocation/full/SQLServer_TD_dbo_file_location_{datesnap}.parquet" 
dbutils.fs.mv(file_location_parquet_file, file_location_final_output_path)
dbutils.fs.rm(file_location_temp_output_path, True)

# Case Appellant
case_appellant_temp_output_path = f"/mnt/ingest01landingsboxlanding/test/CaseAppellant/temp_{datesnap}"
spark_case_appellant_data.coalesce(1).write.format("parquet").mode("overwrite").save(case_appellant_temp_output_path)

case_appellant_files = dbutils.fs.ls(case_appellant_temp_output_path)
case_appellant_parquet_file = [file.path for file in case_appellant_files if file.path.endswith(".parquet")][0]

case_appellant_final_output_path = f"/mnt/ingest01landingsboxlanding/test/CaseAppellant/full/SQLServer_TD_dbo_case_appellant_{datesnap}.parquet"
dbutils.fs.mv(case_appellant_parquet_file, case_appellant_final_output_path) 
dbutils.fs.rm(case_appellant_temp_output_path, True)

# Appellant
appellant_temp_output_path = f"/mnt/ingest01landingsboxlanding/test/Appellant/temp_{datesnap}"
spark_appellant_data.coalesce(1).write.format("parquet").mode("overwrite").save(appellant_temp_output_path)

appellant_files = dbutils.fs.ls(appellant_temp_output_path)
appellant_parquet_file = [file.path for file in appellant_files if file.path.endswith(".parquet")][0]

appellant_final_output_path = f"/mnt/ingest01landingsboxlanding/test/Appellant/full/SQLServer_TD_dbo_appellant_{datesnap}.parquet"
dbutils.fs.mv(appellant_parquet_file, appellant_final_output_path)
dbutils.fs.rm(appellant_temp_output_path, True)

# Department  
department_temp_output_path = f"/mnt/ingest01landingsboxlanding/test/Department/temp_{datesnap}"
spark_department_data.coalesce(1).write.format("parquet").mode("overwrite").save(department_temp_output_path)

department_files = dbutils.fs.ls(department_temp_output_path)
department_parquet_file = [file.path for file in department_files if file.path.endswith(".parquet")][0]  

department_final_output_path = f"/mnt/ingest01landingsboxlanding/test/Department/full/SQLServer_TD_dbo_department_{datesnap}.parquet"
dbutils.fs.mv(department_parquet_file, department_final_output_path)
dbutils.fs.rm(department_temp_output_path, True)

# Hearing Centre
hearing_centre_temp_output_path = f"/mnt/ingest01landingsboxlanding/test/HearingCentre/temp_{datesnap}"  
spark_hearing_centre_data.coalesce(1).write.format("parquet").mode("overwrite").save(hearing_centre_temp_output_path)

hearing_centre_files = dbutils.fs.ls(hearing_centre_temp_output_path)
hearing_centre_parquet_file = [file.path for file in hearing_centre_files if file.path.endswith(".parquet")][0]

hearing_centre_final_output_path = f"/mnt/ingest01landingsboxlanding/test/HearingCentre/full/SQLServer_TD_dbo_hearing_centre_{datesnap}.parquet"
dbutils.fs.mv(hearing_centre_parquet_file, hearing_centre_final_output_path)  
dbutils.fs.rm(hearing_centre_temp_output_path, True)
 
# Read and display schema to confirm the file output
output_paths = {
    "AppealCase": appeal_case_final_output_path,
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
