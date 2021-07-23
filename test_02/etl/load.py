import pandas as pd
import numpy as np
import openpyxl
from carta_interview import Datasets, get_data_file
from DBConnector import DBConnector

class DataLoader(object):

    dt_format = '%m/%d/%Y %I:%M %p'
    
    """Load data into postgres"""
    
    def load_data(self):
        patient_extract1 = get_data_file(Datasets.PATIENT_EXTRACT1)
        patient_extract2 = get_data_file(Datasets.PATIENT_EXTRACT2)
        ## Implement load into postgres
        # 1. Each file represents an extract of a data system, each made a month apart. Each file includes patients discharged during the last two months, so the data will have intersecting data points, but the union of the two is really what you want.
        # -- Need to use latest record (sorted by "Update D/T" if MRN and EncounterID are the same for a record)
        # 

        # Map python data type for certain columns
        c_dict = {
            'MRN' : str,
            'Encounter ID' : str
        }
        
        dt_format_from_xls = '%m/%d/%Y %I:%M %p'

        df1 = pd.read_excel(patient_extract1, engine='openpyxl',converters=c_dict)
        df2 = pd.read_excel(patient_extract2, engine='openpyxl',converters=c_dict)
        union = pd.concat([df1, df2])
        
        union['Update D/T'] = pd.to_datetime(union['Update D/T'], format=dt_format_from_xls)
        union['Admission D/T'] = pd.to_datetime(union['Admission D/T'], format=dt_format_from_xls)
        union['Discharge D/T'] = pd.to_datetime(union['Discharge D/T'], format=dt_format_from_xls)
        union['Birth Date'] = pd.to_datetime(union['Birth Date'], format='%m/%d/%Y')
        
        # We keep the last (MRN + encounterID), which will be the latest, since we've sorted by update D/T
        union = union.sort_values(by='Update D/T', ascending=False) # We first sort by the latest updated before dropping duplicates
        union = union.drop_duplicates(subset=['MRN', 'Encounter ID', 'First Name', 'Last Name', 'Birth Date', 'Admission D/T', 'Discharge D/T'], keep='first').dropna(how='all') # We drop duplicates, and (NaN-filled) rows from our df
        #union = union.drop_duplicates(subset=['MRN'])
        # TODO dbg #print(union)
        
        uniqueGivenNames = union.groupby('MRN')['First Name'].unique()

        union = union.drop_duplicates(subset=['MRN']) # We may drop off duplicates based on MRN, now that we account for multiple "given" / First names
        print(union)

        dbc = DBConnector()
        dbc.execute_drop_tables()
        dbc.execute_init_tables()
        dbc.execute_define_prepared_statements()

        # 1. Create HumanName, Period objs, load into the DB. Use these IDs to then create Patient and Encounter objs, load those into the DB.
        for row in union.itertuples(index=True, name='Pandas'):
            # Although more elegant, this would be less maintainable
            # mrn, encounterid, firstname, lastname, birthdate, admission_dt, discharge_dt, _updated_ts = row[1:]
            # displayname = '{} {}'.format(firstname, lastname)

            mrn = row[1]
            encounterid = row[2]
            firstname = row[3]
            lastname = row[4]
            displayname = '{} {}'.format(row[3], row[4])
            birthdate = row[5]
            admission_dt = row[6]
            discharge_dt = row[7]
            update_ts = row[8]

            # Insert Patient entry
            patientEntry = dbc.execute_planPatientInsert_AndGetRow((mrn, birthdate))
            insertedID_patient = patientEntry[0]

            givenNames = sorted(list(set(uniqueGivenNames[mrn]))) # Sorted for consistency

            for givenName in givenNames:
                # Insert HumanName entry for each given name
                displayname = '{} {}'.format(givenName, lastname)
                humanNameEntry = dbc.execute_planHumanNameInsert_AndGetRow((insertedID_patient, givenName, lastname, displayname))
                insertedID_humanname = humanNameEntry[0]

            # Insert Period entry
            periodEntry = dbc.execute_planPeriodInsert_AndGetRow((admission_dt, discharge_dt))
            insertedID_period = periodEntry[0]

            # Insert Encounter entry
            encounterEntry = dbc.execute_planEncounterInsert_AndGetRow((encounterid, insertedID_patient, insertedID_period))
            insertedID_encounter = encounterEntry[0]