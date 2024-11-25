#installing request library
!pip install pyodbc
!pip install requests
!pip install pymssql

# importing all libraries
import json
import pymssql
import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET

# Define the API endpoint and necessary headers
api_url = "https://clsmf.legalserver.org/report/display?report_session_id=Gd3tkuYwIZU896xx&xml_http=1&extraHtml=0&display_part=66"
api_key = "281c2442-82c7-482f-9fe3-14de9464a28f"
username = "API_Cisco"
password = "ZhCLVDF2cw2z"

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

# Define the SQL query to insert data (Does not work)
insert_query = """
INSERT INTO casedatatest (Database_ID, Assigned_Office, Assigned_Program, CaseID_Count, County_of_Residence, Date_Closed, Date_Opened, Disposition, Full_Name, Formulaic_Client_ID, Funding_Source, Intake_Month, Legal_Problem_Code, Matter_Case_ID, Number_of_Days_Prescreen_to_Intake, Number_of_Days_Prescreen_to_Opened, Number_of_People_18_and_Over, Number_of_People_under_18, Primary_Advocate, Row_Count, Secondary_Funding_Code, Senior_Characteristics, Total_Household_Size, Total_Number_Helped)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
data_dict = json.loads(json_data)
json_data = response.text

# Process and insert the fetched data into SQL Server
if 'ROWSET' in data_dict and 'ROW' in data_dict['ROWSET']:
    for item in data_dict['ROWSET']['ROW']:
        # Extract the necessary data from the item
        Database_ID = item.get('Database_ID')
        Assigned_Office = item.get('Assigned_Office')
        Assigned_Program = item.get('Assigned_Program')
        CaseID_Count = item.get('CaseID_Count')
        County_of_Residence = item.get('County_of_Residence')
        Date_Closed = item.get('Date_Closed')
        Date_Opened = item.get('Date_Opened')
        Disposition = item.get('Disposition')
        Full_Name = item.get('Full_Name')
        Formulaic_Client_ID = item.get('Formulaic_Client_ID')
        Funding_Source = item.get('Funding_Source')
        Intake_Month = item.get('Intake_Month')
        Legal_Problem_Code = item.get('Legal_Problem_Code')
        Matter_Case_ID = item.get('Matter_Case_ID')
        Number_of_Days_Prescreen_to_Intake = item.get('Number_of_Days_Prescreen_to_Intake')
        Number_of_Days_Prescreen_to_Opened = item.get('Number_of_Days_Prescreen_to_Opened')
        Number_of_People_18_and_Over = item.get('Number_of_People_18_and_Over')
        Number_of_People_under_18 = item.get('Number_of_People_under_18')
        Primary_Advocate = item.get('Primary_Advocate')
        Row_Count = item.get('Row_Count')
        Secondary_Funding_Code = item.get('Secondary_Funding_Code')
        Senior_Characteristics = item.get('Senior_Characteristics')
        Total_Household_Size = item.get('Total_Household_Size')
        Total_Number_Helped = item.get('Total_Number_Helped')

       # Insert the data into the database
    cursor.execute(insert_query, (Database_ID, Assigned_Office, Assigned_Program, CaseID_Count, County_of_Residence, Date_Closed, Date_Opened, Disposition, Full_Name, Formulaic_Client_ID, Funding_Source, Intake_Month, Legal_Problem_Code, Matter_Case_ID, Number_of_Days_Prescreen_to_Intake, Number_of_Days_Prescreen_to_Opened, Number_of_People_18_and_Over, Number_of_People_under_18, Primary_Advocate, Row_Count, Secondary_Funding_Code, Senior_Characteristics, Total_Household_Size, Total_Number_Helped))

# Commit the transaction
conn.commit()

# Close the database connection
cursor.close()
conn.close()

print("Data inserted successfully into the SQL Server database.")

