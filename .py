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
response = requests.get(api_url, auth=HTTPBasicAuth(username, password))

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

# Establish connection to SQL Server ( does not work)
conn = pymssql.connect(server='clsmf.database.windows.net',
                       user='clsmf',
                       password='RBDfdc123orlando!',
                       database='clsmf')                    
cursor = conn.cursor()
