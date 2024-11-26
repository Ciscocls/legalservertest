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
api_url = "https://clsmf.legalserver.org/modules/report/api_export.php?load=5461&api_key=68ea3da9-1e6d-4d7c-9ac5-abd5aa98d019"
api_key = "68ea3da9-1e6d-4d7c-9ac5-abd5aa98d019"
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

 # Define SQL Server connection details
server = 'clsmf.database.windows.net'  
database = 'clsmf'  
username = 'clsmf'  
password = 'RBDfdc123orlando!' 


# Establish connection to SQL Server 
conn = pymssql.connect(server='clsmf.database.windows.net',
                       user='clsmf',
                       password='RBDfdc123orlando!',
                       database='clsmf')                    
cursor = conn.cursor()


# Parse the JSON response
data_dict = json.loads(response.text)

insert_query = """
INSERT INTO casedatatest (
    office_name,
    program_name,
    address_builtin_lookup_county_county_expn,
    close_date,
    date_open,
    matter_builtin_lookup_case_disposition_case_disposition_expn,
    full_name_last_first,
    formula_4,
    source,
    intake_date,
    matter_builtin_lookup_problem_code_legal_problem_code_expn,
    identification_number,
    formula_365_9237,
    formula_365_9383,
    family_over_18,
    family_under_18,
    caseworker_name,
    formula_10,
    matter_clsmf_secondary_funding_code_builtin_lookup_clsmf_secondary_funding_code_clsmf_secondary_funding_code_expn,
    senior_chars,
    total_household_size,
    formula_7
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Establish connection to SQL Server 
conn = pymssql.connect(server='clsmf.database.windows.net',
                       user='clsmf',
                       password='RBDfdc123orlando!',
                       database='clsmf')                    
cursor = conn.cursor()

try:
  
    # Process and insert the fetched data into SQL Server
    if 'data' in data_dict:
        for item in data_dict['data']:
            # Extract the necessary data from the item (handle None/Null values)
            office_name = item.get('office_name', None)
            program_name = item.get('program_name', None)
            address_builtin_lookup_county_county_expn = item.get('address_builtin_lookup_county_county_expn', None)
            close_date = item.get('close_date', None)
            date_open = item.get('date_open', None)
            matter_builtin_lookup_case_disposition_case_disposition_expn = item.get('matter_builtin_lookup_case_disposition_case_disposition_expn', None)
            full_name_last_first = item.get('full_name_last_first', None)
            formula_4 = item.get('formula_4', None)
            source = item.get('source', None)
            intake_date = item.get('intake_date', None)
            matter_builtin_lookup_problem_code_legal_problem_code_expn = item.get('matter_builtin_lookup_problem_code_legal_problem_code_expn', None)
            identification_number = item.get('identification_number', None)
            formula_365_9237 = item.get('formula_365_9237', None)
            formula_365_9383 = item.get('formula_365_9383', None)
            family_over_18 = item.get('family_over_18', None)
            family_under_18 = item.get('family_under_18', None)
            caseworker_name = item.get('caseworker_name', None)
            formula_10 = item.get('formula_10', None)
            matter_clsmf_secondary_funding_code_builtin_lookup_clsmf_secondary_funding_code_clsmf_secondary_funding_code_expn = item.get('matter_clsmf_secondary_funding_code_builtin_lookup_clsmf_secondary_funding_code_clsmf_secondary_funding_code_expn', None)
            senior_chars = item.get('senior_chars', None)
            total_household_size = item.get('total_household_size', None)
            formula_7 = item.get('formula_7', None)

            # Insert the data into the database
            cursor.execute(insert_query, (
                office_name, program_name, address_builtin_lookup_county_county_expn,
                close_date, date_open, matter_builtin_lookup_case_disposition_case_disposition_expn,
                full_name_last_first, formula_4, source, intake_date,
                matter_builtin_lookup_problem_code_legal_problem_code_expn, identification_number,
                formula_365_9237, formula_365_9383, family_over_18, family_under_18,
                caseworker_name, formula_10, matter_clsmf_secondary_funding_code_builtin_lookup_clsmf_secondary_funding_code_clsmf_secondary_funding_code_expn,
                senior_chars, total_household_size, formula_7
            ))

        # Commit the transaction
        conn.commit()

    else:
        print("No valid data found in 'data'.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the cursor and connection at the end, even if an error occurs
    cursor.close()
    conn.close()

    print("Data inserted successfully into the SQL Server database.")



