from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
import logging, traceback
import utils.Utility as util
ul = util.Utility()

def extract_data(master_file_path):
    """
    Extract Patient Consultation Data from the Master Data file.

    Args:
        master_file_path (str): File path of the master data.

    Returns:
        List: List of patients along with their consultation details.
    """
    customerData = list()
    try:
        with open(master_file_path, 'r') as data:
            lines = data.readlines()

        for line in lines:
            tempDict = dict()
            if line.startswith('|D|'):
                fields = line.strip().split('|')
                tempDict['customer_name'] = fields[2]
                tempDict['customer_id'] = fields[3]
                tempDict['open_date'] = datetime.strptime(fields[4], "%Y%m%d")
                tempDict['last_consulted_date'] = datetime.strptime(fields[5], "%Y%m%d")
                tempDict['vaccination_id'] = fields[6]
                tempDict['doctor_name'] = fields[7]
                tempDict['state'] = fields[8]
                tempDict['country'] = fields[9]
                tempDict['dob'] = datetime.strptime(fields[10], "%d%m%Y")
                tempDict['is_active'] = fields[11]
                customerData.append(tempDict)
    except Exception as e:
        logging.error(f"{''.join(traceback.format_exception(e.__class__,e,e.__traceback__))}")
    return customerData

def transform(extracted_data):
    """
    Extracted patient data goes through business logic to produce staged data.

    Args:
        extracted_data (list): List of patients along with their consultation details.

    Returns:
        dict: Country specific tables along with the corresponding patient data.
    """
    stagedData = list()
    countryTables = dict()
    try:
        for customer in extracted_data:
            stagedDataField = dict()
            stagedDataField['Name'] = customer['customer_name']
            stagedDataField['Cust_ID'] = customer['customer_id']
            stagedDataField['Open_Dt'] = customer['open_date']
            stagedDataField['Consul_Dt'] = customer['last_consulted_date']
            stagedDataField['Days_Since_Consul'] = ul.days_since_last_consulted(customer['last_consulted_date'])
            stagedDataField['VAC_ID'] = customer['vaccination_id']
            stagedDataField['DR_Name'] = customer['doctor_name']
            stagedDataField['State'] = customer['state']
            stagedDataField['Country'] = customer['country']
            stagedDataField['DOB'] = customer['dob']
            stagedDataField['Age'] = ul.calculate_age(customer['dob'])
            stagedDataField['FLAG'] = customer['is_active']
            stagedData.append(stagedDataField)
        stagedDataDf = pd.DataFrame(stagedData)
        stagedDataDf = stagedDataDf.sort_values('Consul_Dt').groupby(['Country','Cust_ID'],as_index=False).last()
        countryTables = {f"{country}_table": data for country, data in stagedDataDf.groupby('Country')}
    except Exception as e:
        logging.error(f"{''.join(traceback.format_exception(e.__class__,e,e.__traceback__))}")
    return countryTables

def load(transformed_data):
    """
    Loads the country specific data to respective tables.

    Args:
        transformed_data (dict): Country specific tables along with the corresponding patient data.

    Returns:
        str: Status of data load.
    """
    try:
        for countryTableName, countryData in transformed_data.items():
            countryData.to_csv(f"output/{countryTableName}.csv",index=False)
        return "Data loaded or saved successfully."
    except Exception as e:
        logging.error(f"{''.join(traceback.format_exception(e.__class__,e,e.__traceback__))}")
        return "Data loading unsuccessful."

master_file_path = 'data/sample_customer_data.txt'
extracted_data = extract_data(master_file_path)
transformed_data = transform(extracted_data)
output = load(transformed_data)
print(output)
