#installing request library
!pip install pyodbc
!pip install requests
!pip install pymssql

# importing all libraries
import json
import pymssql
import requests
import pyodbc
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET

# Define the API endpoint and necessary headers
api_url = "https://clsmf.legalserver.org/modules/report/api_export.php?load=5527&api_key=c57ca18e-c1f5-4950-9af9-5a965c2f5272"
api_key = "c57ca18e-c1f5-4950-9af9-5a965c2f5272"
username = "API_Cisco"
password = "ZhCLVDF2cw2z"

# Send the GET request with Basic Authentication
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',

}

# Send the GET request with Basic Authentication
response = requests.get(api_url, auth=HTTPBasicAuth(username, password), headers=headers)

# Check the response status
if response.status_code == 200:
    print("Data fetched successfully.")

    # Try to parse the XML response
    try:
        # Parse the XML data
        root = ET.fromstring(response.content)

        # Function to convert XML to Python dict
        def xml_to_dict(element):
            data = {}
            # If the element has children, recurse into them
            if len(element):
                for child in element:
                    child_data = xml_to_dict(child)
                    if child.tag not in data:
                        data[child.tag] = child_data
                    else:
                        if type(data[child.tag]) is list:
                            data[child.tag].append(child_data)
                        else:
                            data[child.tag] = [data[child.tag], child_data]
            else:
                data[element.tag] = element.text
            return data

        # Convert XML to dict and then to JSON
        xml_dict = xml_to_dict(root)
        json_data = json.dumps(xml_dict, indent=4)

        # Print the JSON result
        print(json_data)
    except Exception as e:
        print("Error parsing XML:", e)
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
    print(response.text)

#Check Data
response_data = response.json()
print(json.dumps(response_data, indent=4))

 # Define SQL Server connection details
server = 'clsmf.database.windows.net'
database = 'clsmf'
username = 'clsmf'
password = 'RBDfdc123orlando!'

# Parse the JSON response
data_dict = json.loads(response.text)

# Establish connection to SQL Server
conn = pymssql.connect(server='clsmf.database.windows.net',
                       user='clsmf',
                       password='RBDfdc123orlando!',
                       database='clsmf')
cursor = conn.cursor()

# Returns True if record exists, False if not
def record_exists(cursor, MatterID):
    check_query = "SELECT COUNT(1) FROM casedata WHERE MatterID = %s"
    cursor.execute(check_query, (MatterID,))
    return cursor.fetchone()[0] > 0  
def is_connection_open(conn):
    try:
        # Try executing a simple query to check if the connection is still open
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        return True
    except Exception:
        return False

try:
    # Process and insert the fetched data into SQL Server
    if 'data' in data_dict:
        for item in data_dict['data']:
            # Extract the necessary data from the item (handle None/Null values)
            MatterID = item.get('identification_number', None)

            if MatterID:
                # Check if the connection is still open
                if not is_connection_open(conn):
                    print("Connection is closed. Reopening connection...")
                    # Reconnect logic here if necessary
                    # conn = pymssql.connect(...)

                # Create a new cursor for each record check to ensure it is open
                with conn.cursor() as cursor:  # Use a with statement for automatic cursor closure
                    # Check if this record already exists in the database
                    if not record_exists(cursor, MatterID):
                        # Only insert if the record doesn't exist
                        AssignedOffice = item.get('office_name', None)
                        AssignedProgram = item.get('program_name', None)
                        DatabaseID = item.get('id', None)
                        CountyofResidence = item.get('address_builtin_lookup_county_county_expn', None)
                        DateClosed = item.get('close_date', None)
                        DateOpened = item.get('date_open', None)
                        FormulaicClientID = item.get('formula_4', None)
                        Disposition = item.get('matter_builtin_lookup_case_disposition_case_disposition_expn', None)
                        FullName = item.get('full_name_last_first', None)
                        FundingSource = item.get('source', None)
                        IntakeMonth = item.get('intake_date', None)
                        LegalProblemCode = item.get('matter_builtin_lookup_problem_code_legal_problem_code_expn', None)
                        NumberofDaysPrescreentoIntake = item.get('formula_365_9237', None)
                        NumberofDaysPrescreentoOpened = item.get('formula_365_9383', None)
                        NumberofPeople18andOver = item.get('family_over_18', None)
                        NumberofPeopleunder18 = item.get('family_under_18', None)
                        PrimaryAdvocate = item.get('caseworker_name', None)
                        RecordCount = item.get('formula_10', None)
                        SecondaryFundingCode = item.get('matter_clsmf_secondary_funding_code_builtin_lookup_clsmf_secondary_funding_code_clsmf_secondary_funding_code_expn', None)
                        SeniorCharacteristics = item.get('senior_chars', None)
                        TotalHouseholdSize = item.get('total_household_size', None)
                        TotalNumberHelped = item.get('formula_7', None)

                        # Insert data into table with placeholders
                        insert_query = """
                        INSERT INTO casedata (
                            MatterID, AssignedOffice, AssignedProgram, CaseIDCount, CountyofResidence,
                            DateClosed, DateOpened, FormulaicClientID, Disposition, FullName, FundingSource,
                            IntakeMonth, LegalProblemCode, NumberofDaysPrescreentoIntake, NumberofDaysPrescreentoOpened,
                            NumberofPeople18andOver, NumberofPeopleunder18, PrimaryAdvocate, RecordCount,
                            SecondaryFundingCode, SeniorCharacteristics, TotalHouseholdSize, TotalNumberHelped
                        ) 
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """
                        # Insert the data into the database
                        cursor.execute(insert_query, (
                            MatterID,
                            AssignedOffice,
                            AssignedProgram,
                            CaseIDCount,
                            CountyofResidence,
                            DateClosed,
                            DateOpened,
                            FormulaicClientID,
                            Disposition,
                            FullName,
                            FundingSource,
                            IntakeMonth,
                            LegalProblemCode,
                            NumberofDaysPrescreentoIntake,
                            NumberofDaysPrescreentoOpened,
                            NumberofPeople18andOver,
                            NumberofPeopleunder18,
                            PrimaryAdvocate,
                            RecordCount,
                            SecondaryFundingCode,
                            SeniorCharacteristics,
                            TotalHouseholdSize,
                            TotalNumberHelped,
                        ))
                    else:
                        print(f"Record with identification_number {MatterID} already exists. Skipping insert.")
            else:
                print("No identification_number found. Skipping this record.")
        
        # Commit the transaction after all data is processed
        conn.commit()

    else:
        print("No valid data found in 'data'.")
    
finally:
    # Ensure that the connection is always closed, even if an error occurs
    try:
        if is_connection_open(conn):
            conn.close()
    except Exception as e:
        print(f"Error while closing connection: {e}")

