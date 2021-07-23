import fhir.resources
import json
import pandas as pd
from DBConnector import DBConnector
import datetime

class FHIRDataTransformer(object):
    """Transform data in postgres into Patient/Encounter resources"""
    conn = None
    
    def get_patient_resources(self):
        ## Query data in postgres, produce array of Patient FHIR resources
        #
        patientsArr = []

        dbc = DBConnector()
        
        conn = dbc.conn
        cur = conn.cursor()
        
        cur.execute('SELECT * FROM patient JOIN humanname ON humanname.patient_id = patient.patient_id;')
        res = cur.fetchall()
        df = pd.DataFrame(res)
        colNames = [desc[0] for desc in cur.description]
        print('colNames', colNames)
        df.columns = colNames
        print(df)
        uniqueGivenNames = df.groupby('mrn_id')['givenname'].unique()
        #print("unique names", uniqueGivenNames['002'])
        df = df.drop_duplicates(subset=['mrn_id'])

        birthdateFormat = ''
        
        for row in df.itertuples(index=True, name='Pandas'):
            print(row)
            (patient_id, mrn_id, humanname_id, birthdate, humanname_id2, patient_id2, givenname, familyname, displayedname) = row[1:]

            givenname, familyname, displayedname
            d = {
                    "resourceType" : "Patient",
                    "name" : [],
                    "birthdate" : birthdate.strftime('%Y/%m/%d')
            }
            uniqueGivenNamesForMe = uniqueGivenNames[mrn_id]
            for uniqueGivenName in uniqueGivenNamesForMe:
                d['name'].append(
                    {
                        "given" : uniqueGivenName,
                        "family" : familyname,
                        "text" : displayedname
                    }
                )
            
            patientsArr.append(d)
        # # For each row, prepare a Patient RHIF resource
        print('patients arr', patientsArr)
        return patientsArr

    def get_encounter_resources(self):
        ## Query data in postgres, produce array of Encounter FHIR resources
        #
        
        
        # Convert time format to %m/%d/%Y %l:%M %p for "Admission D/T" and "Discharge D/T"
        '''
        # Convert time format to FHIR preferred, which is 
        dateTime: A date, date-time or partial date (e.g. just year or year + month) as used in human communication.
        The format is YYYY, YYYY-MM, YYYY-MM-DD or YYYY-MM-DDThh:mm:ss+zz:zz,
        e.g. 2018, 1973-06, 1905-08-23, 2015-02-07T13:28:17-05:00 or 2017-01-01T00:00:00.000Z.
        If hours and minutes are specified, a time zone SHALL be populated. Seconds must be provided due to schema type
        constraints but may be zero-filled and may be ignored at receiver discretion. Dates SHALL be valid dates.
        '''
        ## Query data in postgres, produce array of Encounter FHIR resources
        #
        encountersArr = []

        dbc = DBConnector()
        
        conn = dbc.conn
        cur = conn.cursor()
        
        cur.execute('SELECT * FROM encounter LEFT OUTER JOIN period ON encounter.period_id = period.period_id RIGHT OUTER JOIN patient ON encounter.patient_id = patient.patient_id JOIN humanname ON humanname.patient_id = patient.patient_id;')
        res = cur.fetchall()
        conn.commit()

        df = pd.DataFrame(res)
        colNames = [desc[0] for desc in cur.description]
        print('colNames', colNames)
        df.columns = colNames
        print(df)
        uniqueGivenNames = df.groupby('mrn_id')['givenname'].unique()
        df = df.drop_duplicates(subset=['encounter_id_code'])

        iOfColname = {} # Index of column name. Index due to df occupies first column

        for i in range(len(colNames)):
            iOfColname[colNames[i]] = i + 1
        print(iOfColname)

        for row in df.itertuples(index=True, name='Pandas'):
            print(row)
            givenNames = list(uniqueGivenNames[row[iOfColname['mrn_id']]])

            d = {
                    "resourceType" : "Encounter",
                    "identifier" : {
                        "value" : row[iOfColname['encounter_id_code']]
                    },
                    "period" : {
                        "start_date" : row[iOfColname['start_date']],
                        "end_date" : row[iOfColname['end_date']]
                    },
                    "subject" : {
                        "resourceType" : "Patient",
                        "name" : {
                            "given": givenNames[0],
                            "family" : row[iOfColname['familyname']],
                            "text" : row[iOfColname['displayedname']]
                        }
                    }
            }
            encountersArr.append(d)
        
        print('encounters arr', encountersArr)

        return encountersArr