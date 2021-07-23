import sqlalchemy
import pandas as pd
import pytest
import openpyxl
from test_02.etl.load import DataLoader
from test_02.etl.fhir import FHIRDataTransformer


class TestDataLoading(object):

    @pytest.fixture(scope="session", autouse=True)
    def setup_once(self):
        # Setup the database
        pass

    def test_data_loaded(self):
        # Given
        ## Setup
        loader = DataLoader()
        transformer = FHIRDataTransformer()

        # When
        loader.load_data()
        patients = transformer.get_patient_resources()
        encounters = transformer.get_encounter_resources()

        # Then
        assert len(patients) == 4
        assert len(encounters) == 4
        names = set()
        print("The patients:", patients)
        print(type(patients[0]['name']))
        
        for patient in patients:
            print(patient, type(patient))
            
            for name in patient["name"]:
                names.add((name["given"], name["family"]))
        
        assert ("John", "Doe") in names
        # add additional assertions (optional)
        assert ("Cosmia", "Lee") in names
        assert ("Annabelle", "Jones") in names
        assert ("Jerry", "Jones") in names
        assert ("Joanne", "Lee") in names # Previous "given" name
        
        # assertions for encounters
        '''
        MRN_ID  encounter_id, First Name, Last Name, Birth Date, Admission D/T, Discharge D/T
        001	    1234	John	Doe	01/02/1999	04/12/2002 5:00 PM	04/13/2002 10:00 PM
        002	    2345	Joanne	Lee	04/19/2002	04/19/2002 7:22 PM	04/20/2002 6:22 AM
        003	    3456	Annabelle	Jones	01/02/2001	04/21/2002 5:00 PM	04/23/2002 2:53 AM
                                
        020	    0987	Jerry	Jones	01/02/1988	05/01/2002 2:00 PM	05/02/2002 9:00 PM
        002	    2345	Cosmia	Lee	04/19/2002	04/19/2002 7:22 PM	04/20/2002 6:22 AM
        003	    3456	Annabelle	Jones	01/02/2001	04/21/2002 5:00 PM	04/23/2002 2:53 AM

        '''
        print(encounters)
        periods = []
        encounter_id_codes = []
        subjects = []

        expected_periods = [{'start_date': '2002-05-01 14:00:00', 'end_date': '2002-05-02 21:00:00'}, {'start_date': '2002-04-19 19:22:00', 'end_date': '2002-04-20 06:22:00'}, {'start_date': '2002-04-21 17:00:00', 'end_date': '2002-04-23 02:53:00'}, {'start_date': '2002-04-12 17:00:00', 'end_date': '2002-04-13 22:00:00'}]
        expected_encounter_id_codes = ['0987', '2345', '3456', '1234']
        expected_subjects = [{'resourceType': 'Patient', 'name': {'given': 'Jerry', 'family': 'Jones', 'text': 'Jerry Jones'}}, {'resourceType': 'Patient', 'name': {'given': 'Cosmia', 'family': 'Lee', 'text': 'Cosmia Lee'}}, {'resourceType': 'Patient', 'name': {'given': 'Annabelle', 'family': 'Jones', 'text': 'Annabelle Jones'}}, {'resourceType': 'Patient', 'name': {'given': 'John', 'family': 'Doe', 'text': 'John Doe'}}]

        for encounter in encounters:
            curPeriod = encounter['period']
            curEncounterCode = encounter['identifier']['value']
            curSubject = encounter['subject']

            periods.append(curPeriod)
            encounter_id_codes.append(curEncounterCode)
            subjects.append(curSubject)
        
        # Check that we have each period present
        for expected_period in expected_periods:
            assert expected_period in periods
        
        # Check that we have seen all of the expected ID codes
        for expected_encounter_id_code in expected_encounter_id_codes:
            assert expected_encounter_id_code in encounter_id_codes
        
        # Check that we have each subject (patient) present
        for expected_subject in expected_subjects:
            assert expected_subject in subjects
        